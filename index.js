'use strict';

const express      = require('express');
const session      = require('express-session');
const fetch        = require('node-fetch');
const path         = require('path');

// ─── Config from environment variables ───────────────────────────────────────
const {
  TWITCH_CLIENT_ID,
  TWITCH_TOKEN,        // raw OAuth token (without "Bearer")
  TWITCH_CHANNEL,      // broadcaster username
  TWITCH_MOD_LOGIN,    // mod account username (whose token this is)
  DISCORD_WEBHOOK_URL,
  DASHBOARD_PASSWORD,
  SESSION_SECRET = 'changeme-random-string',
  PORT           = 3000,
  SCAN_INTERVAL_SECONDS = 120,   // chatters rescanned this often
  FOLLOWER_INTERVAL_SECONDS = 600 // followers rescanned every 10 min
} = process.env;

const BOT_PATTERN = /^[a-z]{5}\d{2}$/i;

function isSuspect(login, createdAt) {
  const d = new Date(createdAt);
  return BOT_PATTERN.test(login) && d.getFullYear() === 2024 && d.getMonth() === 4;
}

// ─── Shared state ─────────────────────────────────────────────────────────────
const state = {
  accounts:      new Map(),   // login → account object
  alerted:       new Set(),   // logins already Discord-alerted
  lastScan:      null,
  lastFollowerScan: null,
  scanCount:     0,
  running:       false,
  log:           [],          // last 100 log lines
  error:         null
};

function addLog(msg, type = 'info') {
  const entry = { time: new Date().toISOString(), msg, type };
  state.log.unshift(entry);
  if (state.log.length > 100) state.log.pop();
  console.log(`[${entry.time}] [${type.toUpperCase()}] ${msg}`);
}

// ─── Twitch helpers ───────────────────────────────────────────────────────────
const twitchHeaders = () => ({
  'Client-ID': TWITCH_CLIENT_ID,
  'Authorization': `Bearer ${TWITCH_TOKEN}`
});

async function resolveUserIds(logins) {
  const chunks = [];
  for (let i = 0; i < logins.length; i += 100) chunks.push(logins.slice(i, i + 100));
  const results = [];
  for (const chunk of chunks) {
    const qs = chunk.map(l => `login=${encodeURIComponent(l)}`).join('&');
    const r = await fetch(`https://api.twitch.tv/helix/users?${qs}`, { headers: twitchHeaders() });
    const d = await r.json();
    if (!r.ok) throw new Error(d.message || 'User lookup failed');
    results.push(...d.data);
  }
  return results;
}

async function fetchChatters(broadcasterId, modId) {
  let cursor = null, chatters = [];
  do {
    let url = `https://api.twitch.tv/helix/chat/chatters?broadcaster_id=${broadcasterId}&moderator_id=${modId}&first=1000`;
    if (cursor) url += `&after=${cursor}`;
    const r = await fetch(url, { headers: twitchHeaders() });
    const d = await r.json();
    if (!r.ok) throw new Error(d.message || 'Chatters fetch failed — check moderator:read:chatters scope');
    chatters = chatters.concat(d.data);
    cursor = d.pagination?.cursor;
  } while (cursor);
  return chatters;
}

async function fetchFollowers(broadcasterId, modId) {
  let cursor = null, followers = [];
  do {
    let url = `https://api.twitch.tv/helix/channels/followers?broadcaster_id=${broadcasterId}&moderator_id=${modId}&first=100`;
    if (cursor) url += `&after=${cursor}`;
    const r = await fetch(url, { headers: twitchHeaders() });
    const d = await r.json();
    if (!r.ok) throw new Error(d.message || 'Followers fetch failed — check moderator:read:followers scope');
    followers = followers.concat(d.data);
    cursor = d.pagination?.cursor;
  } while (cursor);
  addLog(`Follower scan complete — ${followers.length.toLocaleString()} followers fetched`);
  return followers;
}

// ─── Discord ──────────────────────────────────────────────────────────────────
async function sendDiscordAlert(newBots) {
  if (!DISCORD_WEBHOOK_URL) return;
  const inChat     = newBots.filter(v => v.inChat);
  const followOnly = newBots.filter(v => !v.inChat);
  const summary    = inChat.length && followOnly.length
    ? `${inChat.length} in chat, ${followOnly.length} follower-only`
    : inChat.length ? 'all in chat' : 'all follower-only';
  const lines = newBots.map(v => {
    const src     = v.inChat ? '💬 in chat' : '👤 follower only';
    const created = new Date(v.created).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    return `• **[${v.login}](https://twitch.tv/${v.login})** — created ${created} · ${src}`;
  }).join('\n');

  const payload = {
    username:   'Twitch Bot Monitor',
    avatar_url: 'https://static.twitchusercontent.com/assets/favicon-32-e29e246c157142c94346.png',
    embeds: [{
      title:       `⚠️ ${newBots.length} suspected bot${newBots.length !== 1 ? 's' : ''} detected in ${TWITCH_CHANNEL}`,
      description: `Pattern: 5 letters + 2 numbers, created May 2024 · ${summary}\n\n${lines}`,
      color:       0xf59e0b,
      timestamp:   new Date().toISOString(),
      footer:      { text: 'Twitch Bot Monitor · render.com' }
    }]
  };

  try {
    const r = await fetch(DISCORD_WEBHOOK_URL, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload)
    });
    if (!r.ok) addLog(`Discord alert failed: HTTP ${r.status}`, 'warn');
    else       addLog(`Discord alert sent for ${newBots.length} bot${newBots.length !== 1 ? 's' : ''}.`);
  } catch (e) {
    addLog(`Discord error: ${e.message}`, 'warn');
  }
}

// ─── Main scan loop ───────────────────────────────────────────────────────────
async function runScan() {
  if (state.running) return;
  state.running = true;
  state.error   = null;

  try {
    // Resolve IDs
    const users    = await resolveUserIds([TWITCH_CHANNEL, TWITCH_MOD_LOGIN].filter(Boolean));
    const broadcaster = users.find(u => u.login.toLowerCase() === TWITCH_CHANNEL.toLowerCase());
    const mod         = users.find(u => u.login.toLowerCase() === (TWITCH_MOD_LOGIN || TWITCH_CHANNEL).toLowerCase());
    if (!broadcaster) throw new Error(`Channel "${TWITCH_CHANNEL}" not found`);
    const broadcasterId = broadcaster.id;
    const modId         = mod ? mod.id : broadcasterId;

    // Chatters
    const chatters      = await fetchChatters(broadcasterId, modId);
    const chatterLogins = new Set(chatters.map(c => c.user_login));

    // Followers (every FOLLOWER_INTERVAL_SECONDS)
    const now         = Date.now();
    const doFollowers = !state.lastFollowerScan ||
      (now - state.lastFollowerScan) > FOLLOWER_INTERVAL_SECONDS * 1000;

    let followerLogins = new Set();
    if (doFollowers) {
      const followers = await fetchFollowers(broadcasterId, modId);
      followers.forEach(f => followerLogins.add(f.user_login));
      state.lastFollowerScan = now;
    } else {
      for (const [login, acc] of state.accounts) {
        if (acc.isFollower) followerLogins.add(login);
      }
    }

    // Combined unique logins — only fetch details for ones we haven't seen
    const allLogins = new Set([...chatterLogins, ...followerLogins]);
    const toFetch   = [...allLogins].filter(l => !state.accounts.has(l));

    if (toFetch.length > 0) {
      const details = await resolveUserIds(toFetch);
      const nowMs   = Date.now();
      details.forEach(u => {
        state.accounts.set(u.login, {
          login:      u.login,
          created:    u.created_at,
          days:       Math.floor((nowMs - new Date(u.created_at)) / 86400000),
          suspect:    isSuspect(u.login, u.created_at),
          inChat:     chatterLogins.has(u.login),
          isFollower: followerLogins.has(u.login),
          firstSeen:  new Date().toISOString()
        });
      });
    }

    // Update status for existing accounts
    for (const [login, acc] of state.accounts) {
      if (!toFetch.includes(login)) {
        acc.inChat = chatterLogins.has(login);
        if (doFollowers) acc.isFollower = followerLogins.has(login);
      }
    }

    // Remove accounts no longer in either list
    if (doFollowers) {
      for (const [login] of state.accounts) {
        if (!allLogins.has(login)) state.accounts.delete(login);
      }
    } else {
      for (const [login, acc] of state.accounts) {
        if (!chatterLogins.has(login) && !acc.isFollower) state.accounts.delete(login);
      }
    }

    // Find new bots not yet alerted
    const newBots = [...state.accounts.values()]
      .filter(v => v.suspect && toFetch.includes(v.login) && !state.alerted.has(v.login));
    newBots.forEach(v => state.alerted.add(v.login));

    state.scanCount++;
    state.lastScan = new Date().toISOString();

    const suspects = [...state.accounts.values()].filter(v => v.suspect).length;
    if (newBots.length > 0) {
      addLog(`⚠ ${newBots.length} new bot${newBots.length !== 1 ? 's' : ''} detected: ${newBots.map(v => v.login).join(', ')}`, 'warn');
      await sendDiscordAlert(newBots);
    } else {
      addLog(`Scan #${state.scanCount} — ${chatterLogins.size} chatters, ${followerLogins.size.toLocaleString()} followers, ${suspects} suspected bots total`);
    }

  } catch (e) {
    state.error = e.message;
    addLog(`Scan error: ${e.message}`, 'error');
  }

  state.running = false;
}

// Start scan loop
function startLoop() {
  addLog('Monitor starting…');
  runScan();
  setInterval(runScan, SCAN_INTERVAL_SECONDS * 1000);
}

// ─── Express dashboard ────────────────────────────────────────────────────────
const app = express();

app.use(express.urlencoded({ extended: false }));
app.use(express.json());
app.use(session({
  secret:            SESSION_SECRET,
  resave:            false,
  saveUninitialized: false,
  cookie:            { maxAge: 24 * 60 * 60 * 1000 } // 24h
}));
app.use(express.static(path.join(__dirname, 'public')));

function requireAuth(req, res, next) {
  if (req.session.authed) return next();
  res.redirect('/login');
}

// Login page
app.get('/login', (req, res) => {
  res.send(loginPage());
});
app.post('/login', (req, res) => {
  if (req.body.password === DASHBOARD_PASSWORD) {
    req.session.authed = true;
    res.redirect('/');
  } else {
    res.send(loginPage('Incorrect password.'));
  }
});
app.get('/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/login');
});

// Dashboard
app.get('/', requireAuth, (req, res) => {
  res.send(dashboardPage());
});

// API — returns current state as JSON for the dashboard to poll
app.get('/api/state', requireAuth, (req, res) => {
  const accounts = [...state.accounts.values()].sort((a, b) => {
    if (a.suspect && !b.suspect) return -1;
    if (!a.suspect && b.suspect) return 1;
    if (a.inChat && !b.inChat) return -1;
    if (!a.inChat && b.inChat) return 1;
    return new Date(a.created) - new Date(b.created);
  });
  const suspects = accounts.filter(v => v.suspect);
  res.json({
    scanCount:    state.scanCount,
    lastScan:     state.lastScan,
    running:      state.running,
    error:        state.error,
    log:          state.log.slice(0, 30),
    stats: {
      total:          accounts.length,
      chatters:       accounts.filter(v => v.inChat).length,
      followers:      accounts.filter(v => v.isFollower).length,
      suspects:       suspects.length,
      suspectsChat:   suspects.filter(v => v.inChat).length,
      suspectsFollow: suspects.filter(v => !v.inChat).length,
      newAccounts:    accounts.filter(v => v.days < 30).length,
      established:    accounts.filter(v => v.days >= 365).length
    },
    accounts
  });
});

// ─── HTML pages (inline) ──────────────────────────────────────────────────────
function loginPage(error = '') {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Bot Monitor — Login</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:#0e0e10;color:#efeff1;font-family:'Inter',system-ui,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center}
.box{background:#18181b;border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:2.5rem;width:100%;max-width:360px}
.logo{display:flex;align-items:center;gap:10px;margin-bottom:2rem}
.logo svg{color:#9147ff}
.logo-title{font-size:16px;font-weight:600}
.logo-sub{font-size:12px;color:#6b6b7a}
label{font-size:12px;color:#adadb8;font-weight:500;display:block;margin-bottom:5px}
input{width:100%;background:#1f1f23;border:1px solid rgba(255,255,255,0.14);border-radius:8px;color:#efeff1;font-family:inherit;font-size:14px;padding:9px 12px;outline:none;transition:border-color .15s}
input:focus{border-color:#9147ff;box-shadow:0 0 0 3px rgba(145,71,255,0.12)}
.btn{width:100%;background:#9147ff;color:#fff;border:none;border-radius:8px;padding:10px;font-family:inherit;font-size:14px;font-weight:500;cursor:pointer;margin-top:1.25rem}
.btn:hover{background:#772ce8}
.error{background:rgba(248,113,113,0.12);border:1px solid rgba(248,113,113,0.25);border-radius:8px;color:#f87171;font-size:13px;padding:9px 12px;margin-bottom:1rem}
</style>
</head>
<body>
<div class="box">
  <div class="logo">
    <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="currentColor"><path d="M11.571 4.714h1.715v5.143H11.57zm4.715 0H18v5.143h-1.714zM6 0L1.714 4.286v15.428h5.143V24l4.286-4.286h3.428L22.286 12V0zm14.571 11.143l-3.428 3.428h-3.429l-3 3v-3H6.857V1.714h13.714z"/></svg>
    <div><div class="logo-title">Bot Monitor</div><div class="logo-sub">Dashboard login</div></div>
  </div>
  ${error ? `<div class="error">${error}</div>` : ''}
  <form method="POST" action="/login">
    <label for="pw">Password</label>
    <input type="password" id="pw" name="password" autofocus placeholder="Enter dashboard password" />
    <button class="btn" type="submit">Sign in</button>
  </form>
</div>
</body>
</html>`;
}

function dashboardPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Twitch Bot Monitor</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0e0e10;--surface:#18181b;--surface2:#1f1f23;--surface3:#26262c;
  --border:rgba(255,255,255,0.08);--border2:rgba(255,255,255,0.14);
  --purple:#9147ff;--purple-d:#772ce8;--purple-faint:rgba(145,71,255,0.12);
  --text:#efeff1;--muted:#adadb8;--faint:#6b6b7a;
  --amber:#f59e0b;--amber-bg:rgba(245,158,11,0.12);
  --green:#22c55e;--green-bg:rgba(34,197,94,0.12);
  --red:#f87171;--red-bg:rgba(248,113,113,0.12);
  --blue:#60a5fa;--blue-bg:rgba(96,165,250,0.12);
  --radius:8px;--radius-lg:12px;
}
body{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.5;min-height:100vh;padding:0 0 4rem}
.header{background:var(--surface);border-bottom:1px solid var(--border);padding:0 2rem;display:flex;align-items:center;gap:12px;height:56px;position:sticky;top:0;z-index:10}
.header svg{color:var(--purple)}
.header-title{font-size:15px;font-weight:600}
.header-sub{font-size:12px;color:var(--muted)}
.header-spacer{flex:1}
.pill{display:flex;align-items:center;gap:8px;background:var(--surface2);border:1px solid var(--border2);border-radius:100px;padding:5px 14px;font-size:12px;color:var(--muted)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--faint)}
.dot.live{background:var(--green);box-shadow:0 0 0 3px rgba(34,197,94,0.25);animation:pulse 1.8s ease-in-out infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 3px rgba(34,197,94,0.25)}50%{box-shadow:0 0 0 6px rgba(34,197,94,0.08)}}
.logout{font-size:12px;color:var(--faint);text-decoration:none;margin-left:10px}
.logout:hover{color:var(--red)}
.page{max-width:960px;margin:0 auto;padding:2rem}
.error-banner{background:var(--red-bg);border:1px solid rgba(248,113,113,0.25);border-radius:var(--radius);color:var(--red);font-size:13px;padding:10px 14px;margin-bottom:1rem;display:none}
.stats-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:10px}
.stats-grid2{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:1.5rem}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px}
.stat-label{font-size:11px;color:var(--faint);margin-bottom:4px}
.stat-val{font-size:22px;font-weight:600}
.purple{color:var(--purple)}.blue{color:var(--blue)}.orange{color:var(--amber)}.red{color:var(--red)}.green{color:var(--green)}
.section-title{font-size:11px;font-weight:600;color:var(--faint);text-transform:uppercase;letter-spacing:.06em;margin-bottom:.75rem;padding-bottom:6px;border-bottom:1px solid var(--border)}
.filter-row{display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap}
.chip{font-size:12px;padding:4px 12px;border-radius:100px;border:1px solid var(--border2);background:transparent;color:var(--muted);cursor:pointer;font-family:inherit;transition:all .12s}
.chip:hover{border-color:var(--purple);color:var(--text)}
.chip.active{background:var(--purple);color:#fff;border-color:var(--purple)}
.search-wrap{position:relative;margin-bottom:12px}
.search-icon{position:absolute;left:11px;top:50%;transform:translateY(-50%);color:var(--faint);pointer-events:none}
.search-wrap input{padding-left:34px;width:100%;background:var(--surface2);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text);font-family:inherit;font-size:13px;padding:8px 12px 8px 34px;outline:none}
.search-wrap input:focus{border-color:var(--purple);box-shadow:0 0 0 3px var(--purple-faint)}
.table-wrap{border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.table-scroll{max-height:420px;overflow-y:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{background:var(--surface2);color:var(--faint);font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;padding:9px 14px;text-align:left;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:1}
tbody td{padding:9px 14px;border-bottom:1px solid var(--border);vertical-align:middle}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover td{background:var(--surface2)}
tbody tr.suspected td{background:rgba(245,158,11,0.05)}
tbody tr.suspected:hover td{background:rgba(245,158,11,0.1)}
.account-link{color:var(--blue);text-decoration:none;font-weight:500}
.account-link:hover{text-decoration:underline}
.account-link.bot{color:var(--amber)}
.badge{display:inline-flex;align-items:center;gap:3px;font-size:11px;font-weight:500;border-radius:100px;padding:2px 8px;margin-right:3px;white-space:nowrap}
.badge-bot{background:var(--amber-bg);color:var(--amber);border:1px solid rgba(245,158,11,0.3)}
.badge-chat{background:var(--purple-faint);color:#c4a0ff;border:1px solid rgba(145,71,255,0.25)}
.badge-follow{background:var(--surface3);color:var(--muted);border:1px solid var(--border2)}
.badge-new{background:var(--red-bg);color:var(--red)}
.badge-young{background:var(--amber-bg);color:var(--amber)}
.badge-est{background:var(--green-bg);color:var(--green)}
.log-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.log-title{font-size:11px;font-weight:600;color:var(--faint);text-transform:uppercase;letter-spacing:.06em}
.log-list{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:8px 12px;max-height:160px;overflow-y:auto;font-size:12px;font-family:'Menlo','Consolas',monospace}
.log-entry{padding:2px 0;border-bottom:1px solid var(--border);color:var(--muted)}
.log-entry:last-child{border-bottom:none}
.log-entry.warn,.log-entry.error{color:var(--amber)}
.log-entry.error{color:var(--red)}
.empty{text-align:center;color:var(--faint);padding:2.5rem;font-size:13px}
@media(max-width:600px){.stats-grid,.stats-grid2{grid-template-columns:repeat(2,1fr)}.page{padding:1rem}.header{padding:0 1rem}}
</style>
</head>
<body>
<header class="header">
  <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M11.571 4.714h1.715v5.143H11.57zm4.715 0H18v5.143h-1.714zM6 0L1.714 4.286v15.428h5.143V24l4.286-4.286h3.428L22.286 12V0zm14.571 11.143l-3.428 3.428h-3.429l-3 3v-3H6.857V1.714h13.714z"/></svg>
  <div><div class="header-title">Bot Monitor</div><div class="header-sub" id="channel-label">Loading…</div></div>
  <div class="header-spacer"></div>
  <div class="pill"><div class="dot live" id="pulse-dot"></div><span id="pill-text">Scanning…</span></div>
  <a href="/logout" class="logout">Sign out</a>
</header>

<main class="page">
  <div class="error-banner" id="error-banner"></div>

  <div class="stats-grid" style="margin-bottom:10px">
    <div class="stat"><div class="stat-label">Live chatters</div><div class="stat-val purple" id="s-chatters">—</div></div>
    <div class="stat"><div class="stat-label">Followers scanned</div><div class="stat-val blue" id="s-followers">—</div></div>
    <div class="stat"><div class="stat-label">Suspected bots</div><div class="stat-val orange" id="s-suspects">—</div></div>
  </div>
  <div class="stats-grid2" style="margin-bottom:1.5rem">
    <div class="stat"><div class="stat-label">Bots in chat</div><div class="stat-val orange" id="s-suspects-chat">—</div></div>
    <div class="stat"><div class="stat-label">Bots followers-only</div><div class="stat-val red" id="s-suspects-follow">—</div></div>
    <div class="stat"><div class="stat-label">New accounts &lt;30d</div><div class="stat-val red" id="s-new">—</div></div>
    <div class="stat"><div class="stat-label">Established 1yr+</div><div class="stat-val green" id="s-est">—</div></div>
  </div>

  <div class="section-title" style="margin-bottom:.75rem">Accounts</div>
  <div class="filter-row">
    <button class="chip active" onclick="setFilter('all')" id="f-all">All</button>
    <button class="chip" onclick="setFilter('suspect')" id="f-suspect">⚠ Suspected bots</button>
    <button class="chip" onclick="setFilter('chatters')" id="f-chatters">In chat now</button>
    <button class="chip" onclick="setFilter('followers-only')" id="f-followers-only">Followers only</button>
  </div>
  <div class="search-wrap">
    <svg class="search-icon" xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
    <input type="text" id="search" placeholder="Filter by username…" oninput="renderTable()" />
  </div>
  <div class="table-wrap" style="margin-bottom:1.5rem">
    <div class="table-scroll">
      <table>
        <thead><tr>
          <th>Username</th><th>Created</th><th>Age</th><th>Source</th><th>Status</th>
        </tr></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>

  <div class="section-title">Scan log</div>
  <div class="log-list" id="log-list"><div style="color:var(--faint)">Loading…</div></div>
</main>

<script>
let allAccounts = [];
let activeFilter = 'all';

function setFilter(f) {
  activeFilter = f;
  document.querySelectorAll('.chip').forEach(b => b.classList.remove('active'));
  document.getElementById('f-' + f).classList.add('active');
  renderTable();
}

function ageBadge(days) {
  if (days < 30)  return ['badge-new',   'New'];
  if (days < 365) return ['badge-young', 'Young'];
  return ['badge-est', 'Established'];
}
function formatAge(days) {
  if (days < 1)   return 'Today';
  if (days < 30)  return days + 'd';
  if (days < 365) return Math.floor(days / 30) + 'mo';
  const y = Math.floor(days / 365), m = Math.floor((days % 365) / 30);
  return m > 0 ? y + 'y ' + m + 'mo' : y + 'y';
}

function renderTable() {
  const q = document.getElementById('search').value.toLowerCase();
  let data = allAccounts.filter(v => {
    if (!v.login.toLowerCase().includes(q)) return false;
    if (activeFilter === 'suspect')        return v.suspect;
    if (activeFilter === 'chatters')       return v.inChat;
    if (activeFilter === 'followers-only') return !v.inChat;
    return true;
  });
  const tbody = document.getElementById('tbody');
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty">No accounts match this filter.</td></tr>';
    return;
  }
  tbody.innerHTML = data.map(v => {
    const [ageCls, ageLabel] = ageBadge(v.days);
    const dateStr = new Date(v.created).toLocaleDateString('en-US', { year:'numeric', month:'short', day:'numeric' });
    const rowCls  = v.suspect ? ' class="suspected"' : '';
    const botBadge = v.suspect ? '<span class="badge badge-bot">⚠ Bot</span>' : '';
    const srcBadge = v.inChat
      ? '<span class="badge badge-chat">In chat</span>' + (v.isFollower ? '<span class="badge badge-follow">Follower</span>' : '')
      : '<span class="badge badge-follow">Follower</span>';
    return '<tr' + rowCls + '>' +
      '<td><a href="https://twitch.tv/' + v.login + '" target="_blank" class="account-link' + (v.suspect?' bot':'') + '">' + v.login + '</a></td>' +
      '<td style="color:var(--muted)">' + dateStr + '</td>' +
      '<td style="color:var(--faint)">' + formatAge(v.days) + '</td>' +
      '<td>' + srcBadge + '</td>' +
      '<td>' + botBadge + '<span class="badge ' + ageCls + '">' + ageLabel + '</span></td>' +
      '</tr>';
  }).join('');
}

async function poll() {
  try {
    const r = await fetch('/api/state');
    if (!r.ok) return;
    const d = await r.json();

    // Header
    document.getElementById('channel-label').textContent =
      (d.scanCount ? 'Scan #' + d.scanCount + ' · ' : '') +
      (d.lastScan ? 'Last: ' + new Date(d.lastScan).toLocaleTimeString() : 'Starting…');
    document.getElementById('pill-text').textContent = d.running ? 'Scanning…' : 'Watching';

    // Error
    const eb = document.getElementById('error-banner');
    if (d.error) { eb.textContent = '⚠ ' + d.error; eb.style.display = 'block'; }
    else { eb.style.display = 'none'; }

    // Stats
    document.getElementById('s-chatters').textContent       = d.stats.chatters.toLocaleString();
    document.getElementById('s-followers').textContent      = d.stats.followers.toLocaleString();
    document.getElementById('s-suspects').textContent       = d.stats.suspects;
    document.getElementById('s-suspects-chat').textContent  = d.stats.suspectsChat;
    document.getElementById('s-suspects-follow').textContent= d.stats.suspectsFollow;
    document.getElementById('s-new').textContent            = d.stats.newAccounts;
    document.getElementById('s-est').textContent            = d.stats.established;

    // Table
    allAccounts = d.accounts;
    renderTable();

    // Log
    const logEl = document.getElementById('log-list');
    if (!d.log.length) { logEl.innerHTML = '<div style="color:var(--faint)">No scans yet.</div>'; }
    else {
      logEl.innerHTML = d.log.map(e => {
        const t = new Date(e.time).toLocaleTimeString();
        return '<div class="log-entry ' + (e.type !== 'info' ? e.type : '') + '">[' + t + '] ' + e.msg + '</div>';
      }).join('');
    }
  } catch(e) { console.error('Poll error:', e); }
}

poll();
setInterval(poll, 10000); // refresh dashboard every 10s
</script>
</body>
</html>`;
}

// Start
const missingVars = ['TWITCH_CLIENT_ID','TWITCH_TOKEN','TWITCH_CHANNEL','TWITCH_MOD_LOGIN','DISCORD_WEBHOOK_URL','DASHBOARD_PASSWORD']
  .filter(v => !process.env[v]);
if (missingVars.length) {
  console.warn(`⚠ Missing env vars: ${missingVars.join(', ')} — set these in Render's environment settings.`);
}

app.listen(PORT, () => {
  console.log(`Dashboard running on http://localhost:${PORT}`);
  if (TWITCH_CLIENT_ID && TWITCH_TOKEN && TWITCH_CHANNEL) startLoop();
  else console.warn('Scanner not started — missing Twitch credentials in environment variables.');
});
