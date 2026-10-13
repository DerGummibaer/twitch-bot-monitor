'use strict';

const express      = require('express');
const session      = require('express-session');
const fetch        = require('node-fetch');
const path         = require('path');
const { createClient } = require('@libsql/client');
const { createCredits } = require('./credits');

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
  FOLLOWER_INTERVAL_SECONDS = 600, // followers rescanned every 10 min
  TURSO_URL,
  TURSO_AUTH_TOKEN
} = process.env;

const BOT_PATTERN = /^[a-z]{5}\d{2}$/i;

function isSuspect(login, createdAt) {
  const d = new Date(createdAt);
  return BOT_PATTERN.test(login) && d.getFullYear() === 2024 && d.getMonth() === 4;
}

// ─── Shared state ─────────────────────────────────────────────────────────────
const state = {
  accounts:      new Map(),   // login → account object
  unfollows:     [],          // [{login, followedAt, unfollowedAt}] — last 200
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

// ─── Long-term storage (Turso) ────────────────────────────────────────────────
// Keeps unfollows, bans and already-alerted bots across restarts.
// If TURSO_URL / TURSO_AUTH_TOKEN aren't set, everything still works in memory.
let db = null;

async function initDb() {
  if (!TURSO_URL || !TURSO_AUTH_TOKEN) {
    addLog('Turso not configured — history will reset on restart', 'warn');
    return;
  }
  try {
    db = createClient({ url: TURSO_URL, authToken: TURSO_AUTH_TOKEN });
    await db.batch([
      `CREATE TABLE IF NOT EXISTS unfollows (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         login TEXT NOT NULL,
         followed_at TEXT,
         unfollowed_at TEXT NOT NULL,
         suspect INTEGER NOT NULL DEFAULT 0)`,
      `CREATE TABLE IF NOT EXISTS bans (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         login TEXT NOT NULL,
         banned_at TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS alerted (login TEXT PRIMARY KEY)`
    ], 'write');

    const a = await db.execute('SELECT login FROM alerted');
    a.rows.forEach(r => state.alerted.add(r.login));

    const u = await db.execute(
      'SELECT login, followed_at, unfollowed_at, suspect FROM unfollows ORDER BY id DESC LIMIT 200'
    );
    state.unfollows = u.rows.map(r => ({
      login:        r.login,
      followedAt:   r.followed_at,
      unfollowedAt: r.unfollowed_at,
      suspect:      !!r.suspect
    }));

    addLog(`✅ Turso DB connected — loaded ${state.unfollows.length} unfollows, ${state.alerted.size} alerted bots`);
  } catch (e) {
    db = null;
    addLog(`Turso connection failed: ${e.message} — running without long-term storage`, 'error');
  }
}

async function dbRun(sql, args) {
  if (!db) return;
  try { await db.execute({ sql, args }); }
  catch (e) { addLog(`DB write error: ${e.message}`, 'warn'); }
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
  // Returns array of {user_login, followed_at}
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
      url:         'https://twitch-bot-monitor.onrender.com',
      fields: [{
        name:  '🔨 Take action',
        value: '[Open dashboard → Suspected bots](https://twitch-bot-monitor.onrender.com)',
        inline: false
      }],
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

    // Map of login → followed_at for this scan
    let followerLogins   = new Set();
    let followerDateMap  = new Map(); // login → followed_at ISO string
    if (doFollowers) {
      const followers = await fetchFollowers(broadcasterId, modId);
      followers.forEach(f => {
        followerLogins.add(f.user_login);
        followerDateMap.set(f.user_login, f.followed_at);
      });
      state.lastFollowerScan = now;

      // Detect unfollows — anyone who WAS a follower but isn't anymore
      for (const [login, acc] of state.accounts) {
        if (acc.isFollower && !followerLogins.has(login)) {
          const entry = {
            login,
            followedAt:   acc.followedAt   || null,
            unfollowedAt: new Date().toISOString(),
            suspect:      acc.suspect
          };
          state.unfollows.unshift(entry);
          await dbRun(
            'INSERT INTO unfollows (login, followed_at, unfollowed_at, suspect) VALUES (?, ?, ?, ?)',
            [entry.login, entry.followedAt, entry.unfollowedAt, entry.suspect ? 1 : 0]
          );
          addLog(`👋 ${login} unfollowed`, 'info');
        }
      }
      if (state.unfollows.length > 200) state.unfollows = state.unfollows.slice(0, 200);
    } else {
      for (const [login, acc] of state.accounts) {
        if (acc.isFollower) {
          followerLogins.add(login);
          if (acc.followedAt) followerDateMap.set(login, acc.followedAt);
        }
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
          followedAt: followerDateMap.get(u.login) || null,
          firstSeen:  new Date().toISOString()
        });
      });
    }

    // Update status for existing accounts
    for (const [login, acc] of state.accounts) {
      if (!toFetch.includes(login)) {
        acc.inChat = chatterLogins.has(login);
        if (doFollowers) {
          acc.isFollower = followerLogins.has(login);
          // Update followedAt if we have it (may have been null before)
          if (followerDateMap.has(login)) acc.followedAt = followerDateMap.get(login);
        }
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
    for (const v of newBots) {
      state.alerted.add(v.login);
      await dbRun('INSERT OR IGNORE INTO alerted (login) VALUES (?)', [v.login]);
    }

    state.scanCount++;
    state.lastScan = new Date().toISOString();

    const suspects = [...state.accounts.values()].filter(v => v.suspect).length;
    if (newBots.length > 0) {
      addLog(`⚠ ${newBots.length} new bot${newBots.length !== 1 ? 's' : ''} detected: ${newBots.map(v => v.login).join(', ')}`, 'warn');
      await sendDiscordAlert(newBots);
    } else {
      addLog(`Scan #${state.scanCount} — ${chatterLogins.size} chatters, ${followerLogins.size.toLocaleString()} followers, ${suspects} suspected bots total`);
    }

    // Stream credits — a failure here shouldn't break bot scanning
    try {
      await credits.afterScan({ chatters, broadcasterId, modId });
    } catch (e) {
      addLog(`Credits update failed: ${e.message}`, 'warn');
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

// Stream credits (live detection, chat tracking, OBS page at /credits/<private key>)
const credits = createCredits({
  app,
  getDb:          () => db,
  addLog,
  requireAuth,
  isSuspect,
  isFlagged:      login => !!(state.accounts.get(login) || {}).suspect,
  twitchHeaders,
  resolveUserIds,
  env:            process.env
});

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
      established:    accounts.filter(v => v.days >= 365).length,
      unfollows:      state.unfollows.length
    },
    accounts,
    unfollows: state.unfollows,
    stream:    credits.status()
  });
});

// API — ban a user
app.post('/api/ban', requireAuth, async (req, res) => {
  const { login } = req.body;
  if (!login) return res.status(400).json({ error: 'Missing login' });

  try {
    // Resolve broadcaster + mod IDs
    const logins = [TWITCH_CHANNEL, TWITCH_MOD_LOGIN].filter(Boolean);
    const users  = await resolveUserIds(logins);
    const broadcaster = users.find(u => u.login.toLowerCase() === TWITCH_CHANNEL.toLowerCase());
    const mod         = users.find(u => u.login.toLowerCase() === (TWITCH_MOD_LOGIN || TWITCH_CHANNEL).toLowerCase());
    if (!broadcaster) throw new Error('Broadcaster not found');
    const broadcasterId = broadcaster.id;
    const modId         = mod ? mod.id : broadcasterId;

    // Resolve target user ID
    const targets = await resolveUserIds([login]);
    if (!targets.length) return res.status(404).json({ error: `User "${login}" not found` });
    const targetId = targets[0].id;

    // Issue ban
    const r = await fetch(
      `https://api.twitch.tv/helix/moderation/bans?broadcaster_id=${broadcasterId}&moderator_id=${modId}`,
      {
        method:  'POST',
        headers: { ...twitchHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
          data: {
            user_id: targetId,
            reason:  'If you are not a bot, appeal and you\'ll be unbanned.'
          }
        })
      }
    );

    const d = await r.json();
    if (!r.ok) throw new Error(d.message || `Ban failed (HTTP ${r.status})`);

    // Remove from state so they vanish from the table
    state.accounts.delete(login);
    await dbRun('INSERT INTO bans (login, banned_at) VALUES (?, ?)', [login, new Date().toISOString()]);
    addLog(`🔨 Banned ${login}`, 'warn');
    res.json({ ok: true });

  } catch (e) {
    addLog(`Ban error for ${login}: ${e.message}`, 'error');
    res.status(500).json({ error: e.message });
  }
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
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Twitch Bot Monitor</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#0e0e10;color:#efeff1;font-family:'Inter',system-ui,sans-serif;font-size:14px;min-height:100vh}
  header{background:#18181b;border-bottom:1px solid #2d2d35;padding:14px 24px;display:flex;align-items:center;gap:12px;position:sticky;top:0;z-index:5}
  header h1{font-size:1rem;font-weight:700;color:#a970ff;flex:1}
  .badge{background:#2d2d35;padding:3px 10px;border-radius:20px;font-size:12px;color:#adadb8;white-space:nowrap}
  .badge.ok{background:#1a3a1a;color:#4caf50}
  .badge.err{background:#3a1a1a;color:#f04747}
  .badge:empty{display:none}
  .badge.live{background:#3a1a1a;color:#ff5c5c;font-weight:600}
  .credits-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.6fr);gap:20px;align-items:start}
  .card{background:#18181b;border:1px solid #2d2d35;border-radius:10px;padding:18px}
  .card h3{font-size:13px;font-weight:600;color:#efeff1;margin-bottom:10px}
  .card p{font-size:13px;color:#adadb8;line-height:1.55;margin-bottom:10px}
  .card ol{font-size:13px;color:#adadb8;line-height:1.6;padding-left:18px;margin-bottom:4px}
  .linkrow{display:flex;gap:8px;margin-bottom:12px}
  .linkrow input{flex:1;min-width:0;background:#0e0e10;border:1px solid #2d2d35;color:#efeff1;padding:7px 10px;border-radius:7px;font-size:12px;font-family:Consolas,Menlo,monospace}
  .btn{background:#a970ff;border:none;color:#fff;padding:7px 14px;border-radius:7px;cursor:pointer;font-size:12px;font-weight:600;text-decoration:none;white-space:nowrap}
  .btn:hover{background:#bf93ff}
  .btn.ghost{background:transparent;border:1px solid #3a3a4a;color:#adadb8}
  .btn.ghost:hover{border-color:#a970ff;color:#efeff1}
  .kv{display:grid;grid-template-columns:auto 1fr;gap:6px 14px;font-size:13px}
  .kv dt{color:#6b6b7a}
  .kv dd{color:#efeff1}
  .preview{position:relative;width:100%;max-width:100%;aspect-ratio:16/9;border-radius:10px;overflow:hidden;border:1px solid #2d2d35;background-color:#2a2a31;background-image:linear-gradient(45deg,#34343c 25%,transparent 25%,transparent 75%,#34343c 75%),linear-gradient(45deg,#34343c 25%,transparent 25%,transparent 75%,#34343c 75%);background-size:24px 24px;background-position:0 0,12px 12px}
  .preview iframe{position:absolute;inset:0;width:100%;height:100%;border:0}
  .preview-col{position:sticky;top:76px}
  .form{display:grid;gap:12px}
  .field{display:grid;gap:5px;font-size:12px;color:#adadb8}
  .field b{color:#efeff1;font-weight:600;margin-left:4px}
  .field input[type=text],.field select{background:#0e0e10;border:1px solid #2d2d35;color:#efeff1;padding:7px 10px;border-radius:7px;font-size:13px;width:100%}
  .field[hidden]{display:none}
  .field input[type=text]:focus,.field select:focus{outline:none;border-color:#a970ff}
  .field input[type=color]{width:100%;height:34px;background:#0e0e10;border:1px solid #2d2d35;border-radius:7px;padding:2px 3px;cursor:pointer}
  .field input[type=range]{accent-color:#a970ff;width:100%}
  .row2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
  .hint{font-size:12px;color:#6b6b7a;margin:-6px 0 0}
  .card p.hint{margin-bottom:0}
  .check{display:flex;gap:8px;align-items:center;font-size:13px;color:#efeff1;cursor:pointer}
  .check input{accent-color:#a970ff;width:15px;height:15px}
  fieldset.shows{border:1px solid #2d2d35;border-radius:8px;padding:10px 12px 12px;display:grid;grid-template-columns:1fr 1fr;gap:8px}
  fieldset.shows legend{font-size:12px;color:#adadb8;padding:0 4px}
  .actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
  #lkStatus{font-size:12px;color:#adadb8}
  #lkStatus.dirty{color:#f59e0b}
  @media(max-width:900px){.credits-grid{grid-template-columns:1fr}.preview-col{position:static}}
  .logout{border:1px solid #3a3a4a;color:#adadb8;padding:5px 12px;border-radius:6px;font-size:12px;text-decoration:none}
  .logout:hover{border-color:#a970ff;color:#efeff1}
  .main{padding:24px;max-width:1400px;margin:0 auto}
  .error-banner{display:none;background:#3a1a1a;border:1px solid #5a2a2a;color:#f04747;border-radius:10px;padding:10px 14px;margin-bottom:16px;font-size:13px}
  .stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}
  .stats.small .stat{padding:10px 16px}
  .stats.small .stat-val{font-size:1.15rem}
  .stats.small{margin-bottom:20px}
  .stat{background:#18181b;border:1px solid #2d2d35;border-radius:10px;padding:14px 20px}
  .stat-val{font-size:1.6rem;font-weight:700;color:#a970ff}
  .stat-lbl{font-size:11px;color:#adadb8;text-transform:uppercase;letter-spacing:.05em;margin-top:2px}
  .red{color:#f04747!important}.amber{color:#f59e0b!important}.green{color:#4caf50!important}.blue{color:#00c8ff!important}
  .tabs{display:flex;gap:2px;margin-bottom:16px;border-bottom:1px solid #2d2d35;flex-wrap:wrap}
  .tab{padding:8px 18px;cursor:pointer;border-bottom:2px solid transparent;font-size:13px;color:#adadb8;user-select:none;display:flex;align-items:center;gap:6px}
  .tab:hover{color:#efeff1}
  .tab.active{color:#a970ff;border-bottom-color:#a970ff}
  .count{background:#2d2d35;color:#adadb8;border-radius:20px;padding:0 7px;font-size:11px;font-weight:600}
  .count.hot{background:#f04747;color:#fff}
  .filters{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap;align-items:center}
  .filters select,.filters input{background:#18181b;border:1px solid #2d2d35;color:#efeff1;padding:7px 12px;border-radius:7px;font-size:13px;outline:none}
  .filters input{min-width:220px}
  .filters select:focus,.filters input:focus{border-color:#a970ff}
  .tbl-wrap{overflow:auto;max-height:65vh;border-radius:10px;border:1px solid #2d2d35}
  table{width:100%;border-collapse:collapse}
  th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #1e1e24}
  th{background:#18181b;color:#adadb8;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.04em;position:sticky;top:0;z-index:1;white-space:nowrap}
  th.sortable{cursor:pointer;user-select:none}
  th.sortable:hover{color:#efeff1}
  .sort-icon{display:inline-block;margin-left:4px;opacity:.4;font-style:normal}
  th.sorted .sort-icon{opacity:1;color:#a970ff}
  tr:hover td{background:#18181b88}
  tr.suspected td{background:rgba(240,71,71,.06)}
  tr.suspected:hover td{background:rgba(240,71,71,.11)}
  a.user{color:#a970ff;text-decoration:none;font-weight:500}
  a.user.bot{color:#f04747}
  a.user:hover{text-decoration:underline}
  .pill{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600;margin-right:4px;white-space:nowrap}
  .pill.bot{background:#3a1a1a;color:#f04747}
  .pill.clean{background:#1a1a3a;color:#5865f2}
  .pill.chatter{background:#1a3a3a;color:#00c8ff}
  .pill.follower{background:#1a3a1a;color:#4caf50}
  .pill.both{background:#2d1a3a;color:#a970ff}
  .pill.new{background:#3a1a1a;color:#f04747}
  .pill.young{background:#3a2e1a;color:#f59e0b}
  .pill.est{background:#1a3a1a;color:#4caf50}
  .muted{color:#adadb8}.faint{color:#6b6b7a}
  .ban-btn{background:#f04747;border:none;color:#fff;padding:4px 12px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600}
  .ban-btn:hover{background:#ff6060}
  .ban-btn:disabled{background:#444;cursor:not-allowed}
  .panel{display:none}.panel.active{display:block}
  .log-box{background:#18181b;border:1px solid #2d2d35;border-radius:10px;padding:16px;max-height:65vh;overflow-y:auto;font-family:Consolas,Menlo,monospace;font-size:12px;line-height:1.7;color:#adadb8}
  .log-box .warn{color:#f59e0b}.log-box .error{color:#f04747}
  #toast{position:fixed;bottom:20px;right:20px;background:#18181b;border:1px solid #2d2d35;border-radius:10px;padding:12px 18px;font-size:13px;opacity:0;transition:opacity .3s;pointer-events:none;z-index:99}
  #toast.show{opacity:1}
  .empty{text-align:center;padding:40px;color:#adadb8}
  @media(max-width:700px){.stats{grid-template-columns:repeat(2,1fr)}.main{padding:16px}header{padding:12px 16px;flex-wrap:wrap}}
</style>
</head>
<body>
<header>
  <h1>🤖 Twitch Bot Monitor</h1>
  <span id="statusBadge" class="badge">Connecting…</span>
  <span id="liveBadge" class="badge">Offline</span>
  <span id="scanInfo" class="badge"></span>
  <a class="logout" href="/logout">Sign out</a>
</header>
<div class="main">
  <div id="errorBanner" class="error-banner"></div>

  <div class="stats">
    <div class="stat"><div class="stat-val blue" id="sChatters">—</div><div class="stat-lbl">Chatters</div></div>
    <div class="stat"><div class="stat-val green" id="sFollowers">—</div><div class="stat-lbl">Followers</div></div>
    <div class="stat"><div class="stat-val red" id="sBots">—</div><div class="stat-lbl">Suspected bots</div></div>
    <div class="stat"><div class="stat-val" id="sUnfollows">—</div><div class="stat-lbl">Unfollows tracked</div></div>
  </div>
  <div class="stats small">
    <div class="stat"><div class="stat-val red" id="sBotsChat">—</div><div class="stat-lbl">Bots in chat</div></div>
    <div class="stat"><div class="stat-val amber" id="sBotsFollow">—</div><div class="stat-lbl">Bots followers-only</div></div>
    <div class="stat"><div class="stat-val red" id="sNew">—</div><div class="stat-lbl">New accounts &lt;30d</div></div>
    <div class="stat"><div class="stat-val green" id="sEst">—</div><div class="stat-lbl">Established 1yr+</div></div>
  </div>

  <div class="tabs">
    <div class="tab active" id="tab-accounts" onclick="switchTab(&quot;accounts&quot;)">Accounts</div>
    <div class="tab" id="tab-bots" onclick="switchTab(&quot;bots&quot;)">⚠ Suspected bots <span class="count" id="cBots">0</span></div>
    <div class="tab" id="tab-unfollows" onclick="switchTab(&quot;unfollows&quot;)">👋 Unfollows <span class="count" id="cUnfollows">0</span></div>
    <div class="tab" id="tab-credits" onclick="switchTab(&quot;credits&quot;)">🎬 Credits</div>
    <div class="tab" id="tab-log" onclick="switchTab(&quot;log&quot;)">Log</div>
  </div>

  <div id="panel-table" class="panel active">
    <div class="filters">
      <select id="fSource" onchange="render()">
        <option value="all">Chatters &amp; followers</option>
        <option value="chatter">In chat now</option>
        <option value="follower">Followers only</option>
      </select>
      <input id="fSearch" type="text" placeholder="Search username…" oninput="render()">
    </div>
    <div class="tbl-wrap">
      <table>
        <thead id="thead"></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>

  <div id="panel-credits" class="panel">
    <div class="credits-grid">
      <div>
        <div class="card" style="margin-bottom:16px">
          <h3>Add the credits to OBS</h3>
          <div class="linkrow">
            <input id="creditsLink" readonly value="Loading…">
            <button class="btn" onclick="copyCreditsLink()">Copy</button>
          </div>
          <ol>
            <li>In your ending scene, add a <b>Browser</b> source.</li>
            <li>Paste the link as the URL. Set the width and height to the area you want it in: 1920 × 1080 for full screen, or something like 600 × 1080 for a side column.</li>
            <li>Press OK, then drag the source where you want it. Dragging the corners works, but text stays sharpest if you change the width and height in the source's properties instead.</li>
            <li>Tick <b>Refresh browser when scene becomes active</b> so the credits start from the top every time.</li>
          </ol>
          <p style="margin-top:10px">The background is see-through unless you change it under <b>Look</b> below.</p>
          <p>Keep this link private. Anyone with it can see the credits, but nothing else.</p>
          <h3 style="margin-top:14px">Try it in OBS before a stream</h3>
          <p>Use this test link instead. It shows a made-up stream with an orange “Sample data” tag in the corner. Swap back to the real link above before going live.</p>
          <div class="linkrow" style="margin-bottom:0">
            <input id="testLink" readonly value="Loading…">
            <button class="btn ghost" onclick="copyTestLink()">Copy</button>
          </div>
        </div>
        <div class="card" id="lookCard" style="margin-bottom:16px">
          <h3>Look</h3>
          <div class="form">
            <label class="field"><span>Font</span>
              <select id="lkFontPick">
                <optgroup label="Classic and film"><option>Cormorant Garamond</option><option>EB Garamond</option><option>Playfair Display</option><option>Cinzel</option><option>Libre Baskerville</option><option>Merriweather</option></optgroup>
                <optgroup label="Clean and modern"><option>Poppins</option><option>Montserrat</option><option>Nunito</option><option>Quicksand</option><option>Fredoka</option><option>Raleway</option></optgroup>
                <optgroup label="Big and bold"><option>Bebas Neue</option><option>Oswald</option><option>Anton</option><option>Righteous</option><option>Bangers</option><option>Archivo Black</option></optgroup>
                <optgroup label="Gaming and retro"><option>Press Start 2P</option><option>VT323</option><option>Orbitron</option><option>Silkscreen</option><option>Audiowide</option><option>Russo One</option></optgroup>
                <optgroup label="Handwritten"><option>Caveat</option><option>Pacifico</option><option>Permanent Marker</option><option>Lobster</option><option>Kalam</option><option>Indie Flower</option></optgroup>
                <optgroup label="Typewriter"><option>Special Elite</option><option>Courier Prime</option><option>Cutive Mono</option></optgroup>
                <option value="__custom">Another font…</option>
              </select>
            </label>
            <label class="field" id="lkFontCustomWrap" hidden><span>Font name</span><input type="text" id="lkFont" maxlength="60" spellcheck="false" autocomplete="off" placeholder="e.g. Comic Neue"></label>
            <p class="hint">Pick one from the list, or choose <b>Another font…</b> and type the exact name of any font from fonts.google.com or one installed on the streamer’s PC.</p>
            <div class="row2">
              <label class="field"><span>Names</span><input type="color" id="lkText"></label>
              <label class="field"><span>Headings and details</span><input type="color" id="lkLabel"></label>
            </div>
            <label class="field"><span>Text size<b id="lkSizeVal"></b></span><input type="range" id="lkSize" min="60" max="160" step="5"></label>
            <label class="field"><span>Scroll speed<b id="lkSpeedVal"></b></span><input type="range" id="lkSpeed" min="20" max="200" step="5"></label>
            <label class="field"><span>Background</span>
              <select id="lkBg"><option value="transparent">See-through</option><option value="black">Solid black</option></select>
            </label>
            <label class="check"><input type="checkbox" id="lkShadow"> Shadow behind the text (helps it stand out over gameplay)</label>
            <label class="field"><span>Closing line</span><input type="text" id="lkClosing" maxlength="120" placeholder="Leave empty to hide it"></label>
            <fieldset class="shows">
              <legend>Show these sections</legend>
              <label class="check"><input type="checkbox" id="lkShow-honours"> Special mentions</label>
              <label class="check"><input type="checkbox" id="lkShow-raids"> Raiders</label>
              <label class="check"><input type="checkbox" id="lkShow-gifts"> Gifted subs</label>
              <label class="check"><input type="checkbox" id="lkShow-bits"> Bits</label>
              <label class="check"><input type="checkbox" id="lkShow-subs"> Subscribers</label>
              <label class="check"><input type="checkbox" id="lkShow-followers"> New followers</label>
              <label class="check"><input type="checkbox" id="lkShow-chatStats"> Chat totals</label>
            </fieldset>
            <div class="actions">
              <button class="btn" id="lkSave" onclick="saveLook()">Save</button>
              <button class="btn ghost" onclick="resetLook()">Reset to default</button>
              <span id="lkStatus"></span>
            </div>
          </div>
        </div>
        <div class="card">
          <h3 id="creditsHeading">This stream</h3>
          <dl class="kv" id="creditsSummary"></dl>
        </div>
      </div>
      <div class="preview-col">
        <div class="preview"><iframe id="creditsFrame" title="Credits preview"></iframe></div>
        <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;align-items:center">
          <button class="btn ghost" onclick="reloadPreview()">Restart preview</button>
          <label class="check" style="margin-left:6px"><input type="checkbox" id="useSample" onchange="reloadPreview()"> Preview with sample data</label>
          <a class="btn ghost" id="creditsOpen" href="#" target="_blank">Open full screen</a>
        </div>
      </div>
    </div>
  </div>

  <div id="panel-log" class="panel">
    <div id="logBox" class="log-box"></div>
  </div>
</div>
<div id="toast"></div>

<script>
let data = { accounts: [], unfollows: [], log: [], stats: {} };
let tab = 'accounts';
let sortKey = 'created';
let sortDir = -1;

const ACCOUNT_COLS = [
  { key: 'login',      label: 'Username',      sortable: true },
  { key: 'created',    label: 'Created',       sortable: true },
  { key: 'days',       label: 'Acct age',      sortable: true },
  { key: 'followedAt', label: 'Following for', sortable: true },
  { key: null,         label: 'Source' },
  { key: null,         label: 'Status' },
  { key: null,         label: '' }
];
const UNFOLLOW_COLS = [
  { key: 'login',        label: 'Username',      sortable: true },
  { key: 'duration',     label: 'Followed for',  sortable: true },
  { key: 'followedAt',   label: 'Followed on',   sortable: true },
  { key: 'unfollowedAt', label: 'Unfollowed',    sortable: true },
  { key: null,           label: 'Status' }
];

function switchTab(t) {
  tab = t;
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + t).classList.add('active');
  document.getElementById('panel-table').classList.toggle('active', t !== 'log' && t !== 'credits');
  document.getElementById('panel-log').classList.toggle('active', t === 'log');
  document.getElementById('panel-credits').classList.toggle('active', t === 'credits');
  if (t === 'credits') { renderCredits(); reloadPreview(); return; }
  document.getElementById('fSource').style.display = t === 'unfollows' ? 'none' : '';
  if (t === 'unfollows') { sortKey = 'unfollowedAt'; sortDir = -1; }
  else if (sortKey === 'unfollowedAt' || sortKey === 'duration') { sortKey = 'created'; sortDir = -1; }
  render();
}

function setSort(key) {
  if (sortKey === key) sortDir = -sortDir;
  else { sortKey = key; sortDir = key === 'login' ? 1 : -1; }
  render();
}

function esc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;' }[c])); }

function daysSince(iso) { return iso ? Math.floor((Date.now() - new Date(iso)) / 86400000) : null; }
function daysBetween(a, b) { return (a && b) ? Math.floor((new Date(b) - new Date(a)) / 86400000) : null; }

function fmtAge(n) {
  if (n === null || n === undefined || isNaN(n)) return '—';
  if (n < 1) return 'today';
  if (n < 30) return n + 'd';
  if (n < 365) return Math.floor(n / 30) + 'mo';
  const y = Math.floor(n / 365), m = Math.floor((n % 365) / 30);
  return m ? y + 'y ' + m + 'mo' : y + 'y';
}
function fmtDate(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
}
function fmtDateTime(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
}
function ageBadge(days) {
  if (days < 30) return '<span class="pill new">New</span>';
  if (days < 365) return '<span class="pill young">Young</span>';
  return '<span class="pill est">Established</span>';
}

function sortValue(row, key) {
  if (key === 'login') return row.login.toLowerCase();
  if (key === 'days') return row.days == null ? -1 : row.days;
  if (key === 'duration') { const d = daysBetween(row.followedAt, row.unfollowedAt); return d == null ? -1 : d; }
  // dates: created / followedAt / unfollowedAt — missing values sort last
  return row[key] ? new Date(row[key]).getTime() : null;
}
function sortRows(rows) {
  return [...rows].sort((a, b) => {
    const va = sortValue(a, sortKey), vb = sortValue(b, sortKey);
    if (va === null && vb === null) return 0;
    if (va === null) return 1;
    if (vb === null) return -1;
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  });
}

function headerHtml(cols) {
  return '<tr>' + cols.map(c => {
    if (!c.sortable) return '<th>' + c.label + '</th>';
    const on = sortKey === c.key;
    const arrow = on ? (sortDir === 1 ? '▲' : '▼') : '⇅';
    return '<th class="sortable' + (on ? ' sorted' : '') + '" onclick="setSort(&quot;' + c.key + '&quot;)">' + c.label + '<i class="sort-icon">' + arrow + '</i></th>';
  }).join('') + '</tr>';
}

function render() {
  const thead = document.getElementById('thead');
  const tbody = document.getElementById('tbody');
  const search = document.getElementById('fSearch').value.toLowerCase();

  if (tab === 'unfollows') {
    thead.innerHTML = headerHtml(UNFOLLOW_COLS);
    let rows = (data.unfollows || []).filter(u => u.login.toLowerCase().includes(search));
    rows = sortRows(rows);
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="empty">' + (search ? 'No unfollows match your search.' : 'No unfollows recorded yet. Followers are checked every few minutes.') + '</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(u => {
      const st = u.suspect ? '<span class="pill bot">⚠ Suspected bot</span>' : '<span class="pill clean">Clean</span>';
      return '<tr' + (u.suspect ? ' class="suspected"' : '') + '>'
        + '<td><a class="user' + (u.suspect ? ' bot' : '') + '" href="https://twitch.tv/' + esc(u.login) + '" target="_blank">' + esc(u.login) + '</a></td>'
        + '<td>' + fmtAge(daysBetween(u.followedAt, u.unfollowedAt)) + '</td>'
        + '<td class="muted">' + fmtDate(u.followedAt) + '</td>'
        + '<td class="muted">' + fmtDateTime(u.unfollowedAt) + '</td>'
        + '<td>' + st + '</td>'
        + '</tr>';
    }).join('');
    return;
  }

  thead.innerHTML = headerHtml(ACCOUNT_COLS);
  const source = document.getElementById('fSource').value;
  let rows = data.accounts || [];
  if (tab === 'bots') rows = rows.filter(a => a.suspect);
  if (source === 'chatter') rows = rows.filter(a => a.inChat);
  else if (source === 'follower') rows = rows.filter(a => !a.inChat);
  if (search) rows = rows.filter(a => a.login.toLowerCase().includes(search));
  rows = sortRows(rows);

  if (!rows.length) {
    const msg = tab === 'bots' ? 'No suspected bots right now.' : 'No accounts match.';
    tbody.innerHTML = '<tr><td colspan="7" class="empty">' + msg + '</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(a => {
    let src;
    if (a.inChat && a.isFollower) src = '<span class="pill both">Chatter + follower</span>';
    else if (a.inChat) src = '<span class="pill chatter">Chatter</span>';
    else src = '<span class="pill follower">Follower</span>';
    const status = (a.suspect ? '<span class="pill bot">⚠ Suspected bot</span>' : '') + ageBadge(a.days);
    const ban = a.suspect ? '<button class="ban-btn" data-login="' + esc(a.login) + '" onclick="banUser(this)">Ban</button>' : '';
    const following = a.isFollower && a.followedAt ? fmtAge(daysSince(a.followedAt)) : '—';
    return '<tr' + (a.suspect ? ' class="suspected"' : '') + '>'
      + '<td><a class="user' + (a.suspect ? ' bot' : '') + '" href="https://twitch.tv/' + esc(a.login) + '" target="_blank">' + esc(a.login) + '</a></td>'
      + '<td class="muted">' + fmtDate(a.created) + '</td>'
      + '<td>' + fmtAge(a.days) + '</td>'
      + '<td>' + following + '</td>'
      + '<td>' + src + '</td>'
      + '<td>' + status + '</td>'
      + '<td>' + ban + '</td>'
      + '</tr>';
  }).join('');
}

function creditsUrl() {
  const p = data.stream && data.stream.creditsPath;
  return p ? location.origin + p : '';
}
const SHOW_KEYS = ['honours', 'raids', 'gifts', 'bits', 'subs', 'followers', 'chatStats'];
let lookLoaded = false;
let lookDirty = false;
let previewTimer = null;

function fillLook(s) {
  const pick = document.getElementById('lkFontPick');
  const listed = Array.from(pick.options).some(o => o.value === s.font);
  pick.value = listed ? s.font : '__custom';
  document.getElementById('lkFont').value = listed ? '' : s.font;
  document.getElementById('lkFontCustomWrap').hidden = listed;
  document.getElementById('lkText').value = s.textColor;
  document.getElementById('lkLabel').value = s.labelColor;
  document.getElementById('lkSize').value = s.size;
  document.getElementById('lkSpeed').value = s.speed;
  document.getElementById('lkBg').value = s.background;
  document.getElementById('lkShadow').checked = !!s.shadow;
  document.getElementById('lkClosing').value = s.closingText;
  SHOW_KEYS.forEach(k => { document.getElementById('lkShow-' + k).checked = !!s.show[k]; });
  updateLookLabels();
}
function readLook() {
  const show = {};
  SHOW_KEYS.forEach(k => { show[k] = document.getElementById('lkShow-' + k).checked; });
  return {
    font:        (document.getElementById('lkFontPick').value === '__custom'
                    ? document.getElementById('lkFont').value.trim() : document.getElementById('lkFontPick').value) || 'Cormorant Garamond',
    textColor:   document.getElementById('lkText').value,
    labelColor:  document.getElementById('lkLabel').value,
    size:        Number(document.getElementById('lkSize').value),
    speed:       Number(document.getElementById('lkSpeed').value),
    background:  document.getElementById('lkBg').value,
    shadow:      document.getElementById('lkShadow').checked,
    closingText: document.getElementById('lkClosing').value,
    show
  };
}
function updateLookLabels() {
  document.getElementById('lkSizeVal').textContent = document.getElementById('lkSize').value + '%';
  const sp = Number(document.getElementById('lkSpeed').value);
  document.getElementById('lkSpeedVal').textContent = sp < 50 ? 'slow' : sp > 110 ? 'fast' : 'normal';
}
function setLookStatus(text, dirty) {
  const el = document.getElementById('lkStatus');
  el.textContent = text;
  el.className = dirty ? 'dirty' : '';
}
function sendPreview() {
  const f = document.getElementById('creditsFrame');
  if (f && f.contentWindow) f.contentWindow.postMessage({ type: 'credits-preview', settings: readLook() }, location.origin);
}
function onLookEdit(e) {
  if (e && e.target && e.target.id === 'lkFontPick') {
    const custom = e.target.value === '__custom';
    document.getElementById('lkFontCustomWrap').hidden = !custom;
    if (custom && e.type === 'change') document.getElementById('lkFont').focus();
  }
  lookDirty = true;
  updateLookLabels();
  setLookStatus('Unsaved changes — the preview shows them, OBS doesn’t yet', true);
  clearTimeout(previewTimer);
  previewTimer = setTimeout(sendPreview, 300);
}
async function postLook(body) {
  const r = await fetch('/api/credits-settings', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.ok) throw new Error(j.error || 'Could not save');
  if (data.stream) data.stream.settings = j.settings;
  fillLook(j.settings);
  lookDirty = false;
  sendPreview();
  return j;
}
async function saveLook() {
  const btn = document.getElementById('lkSave');
  btn.disabled = true;
  try {
    const j = await postLook(readLook());
    setLookStatus(j.stored ? 'Saved. OBS updates within about 10 seconds.' : 'Saved for now, but the database didn’t store it. It will reset if the app restarts.', !j.stored);
    showToast('✅ Credits look saved');
  } catch (e) {
    setLookStatus('Not saved: ' + e.message, true);
  }
  btn.disabled = false;
}
async function resetLook() {
  if (!confirm('Reset the credits to the default look?')) return;
  try {
    await postLook({ reset: true });
    setLookStatus('Back to the default look.', false);
    showToast('✅ Credits look reset');
  } catch (e) {
    setLookStatus('Not reset: ' + e.message, true);
  }
}
document.addEventListener('DOMContentLoaded', () => {
  const card = document.getElementById('lookCard');
  card.addEventListener('input', onLookEdit);
  card.addEventListener('change', onLookEdit);
  document.getElementById('creditsFrame').addEventListener('load', () => { if (lookDirty) setTimeout(sendPreview, 300); });
});

function reloadPreview() {
  const u = creditsUrl();
  if (!u) return;
  const sample = document.getElementById('useSample').checked;
  document.getElementById('creditsFrame').src = u + (sample ? '?sample=1&' : '?') + 't=' + Date.now();
  document.getElementById('creditsOpen').href = u + (sample ? '?sample=1' : '');
  document.getElementById('testLink').value = u + '?sample=1';
}
function copyTestLink() {
  const el = document.getElementById('testLink');
  const done = () => showToast('✅ Test link copied');
  if (navigator.clipboard) navigator.clipboard.writeText(el.value).then(done, () => el.select());
  else { el.select(); document.execCommand('copy'); done(); }
}
function copyCreditsLink() {
  const u = creditsUrl();
  if (!u) return;
  const done = () => showToast('✅ Link copied');
  if (navigator.clipboard) navigator.clipboard.writeText(u).then(done, () => { document.getElementById('creditsLink').select(); });
  else { document.getElementById('creditsLink').select(); document.execCommand('copy'); done(); }
}
function fmtDuration(sec) {
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60);
  return h ? h + 'h ' + m + 'm' : m + 'm';
}
function renderCredits() {
  const st = data.stream;
  if (!st) return;
  const link = document.getElementById('creditsLink');
  if (link.value !== creditsUrl()) link.value = creditsUrl();
  if (!lookLoaded && st.settings) {
    fillLook(st.settings);
    lookLoaded = true;
    if (st.credits && st.credits.empty) { document.getElementById('useSample').checked = true; reloadPreview(); }
  }
  const c = st.credits || {};
  const box = document.getElementById('creditsSummary');
  const heading = document.getElementById('creditsHeading');
  if (c.empty) {
    heading.textContent = 'No stream recorded yet';
    box.innerHTML = '<dd style="grid-column:1/-1;color:#adadb8">Stats start collecting the next time the channel goes live.</dd>';
    return;
  }
  heading.textContent = c.live ? 'This stream (live now)' : 'Last stream';
  const row = (k, v) => '<dt>' + k + '</dt><dd>' + v + '</dd>';
  const top = (arr, unit) => arr.length ? esc(arr[0].name) + ' <span class="faint">(' + arr[0].total.toLocaleString() + ' ' + unit + ')</span>' : '—';
  box.innerHTML =
    row('Started', fmtDateTime(c.startedAt)) +
    row('Length', fmtDuration(c.durationSec)) +
    row('Longest in chat', c.longest ? esc(c.longest.name) + ' <span class="faint">(' + fmtDuration(c.longest.seconds) + ')</span>' : '—') +
    row('Most messages', c.chattiest ? esc(c.chattiest.name) + ' <span class="faint">(' + c.chattiest.messages.toLocaleString() + ')</span>' : '—') +
    row('Top gifter', top(c.gifters, 'subs')) +
    row('Top cheerer', top(c.cheerers, 'bits')) +
    row('New followers', c.followers.length) +
    row('Subscribers', c.subs.length) +
    row('Raids', c.raids.length) +
    row('Chat', c.totals.messages.toLocaleString() + ' messages from ' + c.totals.chatters.toLocaleString() + ' people');
}

function renderChrome() {
  const lb = document.getElementById('liveBadge');
  const lv = data.stream && data.stream.live;
  if (lv && lv.isLive) {
    lb.textContent = '🔴 Live · ' + Number(lv.viewers).toLocaleString() + ' viewers · ' + fmtDuration((Date.now() - new Date(lv.startedAt)) / 1000);
    lb.className = 'badge live';
  } else {
    lb.textContent = 'Offline';
    lb.className = 'badge';
  }
  if (tab === 'credits') renderCredits();

  const s = data.stats || {};
  const set = (id, v) => { document.getElementById(id).textContent = (v == null ? '—' : Number(v).toLocaleString()); };
  set('sChatters', s.chatters); set('sFollowers', s.followers); set('sBots', s.suspects); set('sUnfollows', s.unfollows);
  set('sBotsChat', s.suspectsChat); set('sBotsFollow', s.suspectsFollow); set('sNew', s.newAccounts); set('sEst', s.established);

  const cBots = document.getElementById('cBots');
  cBots.textContent = s.suspects || 0;
  cBots.classList.toggle('hot', (s.suspects || 0) > 0);
  document.getElementById('cUnfollows').textContent = s.unfollows || 0;

  const badge = document.getElementById('statusBadge');
  if (data.error) { badge.textContent = '⚠ Error'; badge.className = 'badge err'; }
  else if (data.running) { badge.textContent = '● Scanning…'; badge.className = 'badge ok'; }
  else if (data.lastScan) { badge.textContent = '● Monitoring'; badge.className = 'badge ok'; }
  else { badge.textContent = 'Starting…'; badge.className = 'badge'; }

  document.getElementById('scanInfo').textContent = data.lastScan
    ? 'Scan #' + data.scanCount + ' · ' + new Date(data.lastScan).toLocaleTimeString()
    : '';

  const eb = document.getElementById('errorBanner');
  if (data.error) { eb.textContent = '⚠ ' + data.error; eb.style.display = 'block'; }
  else eb.style.display = 'none';

  document.getElementById('logBox').innerHTML = (data.log || []).map(e =>
    '<div class="' + (e.type !== 'info' ? esc(e.type) : '') + '">[' + new Date(e.time).toLocaleTimeString() + '] ' + esc(e.msg) + '</div>'
  ).join('') || '<div>No scans yet.</div>';
}

async function doBan(login) {
  const r = await fetch('/api/ban', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ login })
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.ok) throw new Error(j.error || 'Ban failed');
  data.accounts = (data.accounts || []).filter(a => a.login !== login);
}

async function banUser(btn) {
  const login = btn.getAttribute('data-login');
  btn.disabled = true; btn.textContent = 'Banning…';
  try { await doBan(login); showToast('✅ Banned ' + login); render(); }
  catch (e) { showToast('❌ ' + login + ': ' + e.message); btn.disabled = false; btn.textContent = 'Ban'; }
}

function showToast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(showToast._t);
  showToast._t = setTimeout(() => t.classList.remove('show'), 3500);
}

async function poll() {
  try {
    const r = await fetch('/api/state');
    if (r.redirected || r.status === 401) { location = '/login'; return; }
    if (r.ok) {
      data = await r.json();
      renderChrome();
      // Don't re-draw the table while a ban is in progress
      if (!document.querySelector('#tbody .ban-btn:disabled')) render();
    }
  } catch (e) { console.error(e); }
}

render();
poll();
setInterval(poll, 10000);
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

initDb().then(() => TWITCH_CHANNEL ? credits.init() : null).then(() => {
  app.listen(PORT, () => {
    console.log(`Dashboard running on http://localhost:${PORT}`);
    if (TWITCH_CLIENT_ID && TWITCH_TOKEN && TWITCH_CHANNEL) startLoop();
    else console.warn('Scanner not started — missing Twitch credentials in environment variables.');
  });
});
