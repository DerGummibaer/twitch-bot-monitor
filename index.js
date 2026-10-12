'use strict';
const express = require('express');
const session = require('express-session');
const fetch = require('node-fetch');
const { createClient } = require('@libsql/client');

// ─── Config from env ────────────────────────────────────────────────────────
const PORT             = process.env.PORT || 3000;
const TWITCH_TOKEN     = process.env.TWITCH_TOKEN;
const TWITCH_CLIENT_ID = process.env.TWITCH_CLIENT_ID;
const TWITCH_CHANNEL   = process.env.TWITCH_CHANNEL;
const DISCORD_WEBHOOK  = process.env.DISCORD_WEBHOOK_URL;
const DASHBOARD_PASS   = process.env.DASHBOARD_PASSWORD;
const SESSION_SECRET   = process.env.SESSION_SECRET || 'twitch-monitor-secret';
const TURSO_URL        = process.env.TURSO_URL;
const TURSO_AUTH_TOKEN = process.env.TURSO_AUTH_TOKEN;
const DASHBOARD_URL    = process.env.DASHBOARD_URL || 'https://twitch-bot-monitor.onrender.com';

const TWITCH_MOD_LOGIN  = process.env.TWITCH_MOD_LOGIN;
const CHATTER_INTERVAL  = (Number(process.env.SCAN_INTERVAL_SECONDS) || 120) * 1000;
const FOLLOWER_INTERVAL = (Number(process.env.FOLLOWER_INTERVAL_SECONDS) || 600) * 1000;
const BOT_PATTERN       = /^[a-z]{5}\d{2}$/i;

function isSuspect(login, createdAt) {
  const d = new Date(createdAt);
  return BOT_PATTERN.test(login) && d.getFullYear() === 2024 && d.getMonth() === 4;
}

// ─── In-memory state ─────────────────────────────────────────────────────────
const state = {
  accounts: new Map(),
  unfollows: [],
  alerted: new Set(),
  lastScan: null,
  lastFollowerScan: null,
  scanCount: 0,
  running: false,
  log: [],
  error: null,
};

function addLog(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  state.log.unshift(line);
  if (state.log.length > 100) state.log.pop();
}

// ─── Turso DB ────────────────────────────────────────────────────────────────
let db = null;

async function initDb() {
  if (!TURSO_URL || !TURSO_AUTH_TOKEN) {
    addLog('⚠️  Turso env vars not set — running without persistent storage');
    return;
  }
  try {
    db = createClient({ url: TURSO_URL, authToken: TURSO_AUTH_TOKEN });
    await db.execute(`CREATE TABLE IF NOT EXISTS unfollows (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      login     TEXT NOT NULL,
      followed_at  TEXT,
      unfollowed_at TEXT NOT NULL,
      suspect   INTEGER NOT NULL DEFAULT 0
    )`);
    await db.execute(`CREATE TABLE IF NOT EXISTS bans (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      login     TEXT NOT NULL,
      banned_at TEXT NOT NULL
    )`);
    await db.execute(`CREATE TABLE IF NOT EXISTS alerted (
      login TEXT PRIMARY KEY
    )`);
    addLog('✅ Turso DB connected');

    // Load persisted alerted set
    const alertedRows = await db.execute('SELECT login FROM alerted');
    for (const row of alertedRows.rows) state.alerted.add(row.login);
    addLog(`  Loaded ${state.alerted.size} alerted logins from DB`);

    // Load last 200 unfollows
    const unfollowRows = await db.execute(
      'SELECT login, followed_at, unfollowed_at, suspect FROM unfollows ORDER BY id DESC LIMIT 200'
    );
    state.unfollows = unfollowRows.rows.map(r => ({
      login: r.login,
      followedAt: r.followed_at,
      unfollowedAt: r.unfollowed_at,
      suspect: !!r.suspect,
    }));
    addLog(`  Loaded ${state.unfollows.length} unfollow records from DB`);
  } catch (err) {
    addLog(`❌ Turso init error: ${err.message}`);
    db = null;
  }
}

async function dbAddAlerted(login) {
  if (!db) return;
  try { await db.execute({ sql: 'INSERT OR IGNORE INTO alerted (login) VALUES (?)', args: [login] }); }
  catch (e) { addLog(`DB alerted write error: ${e.message}`); }
}

async function dbAddUnfollow(entry) {
  if (!db) return;
  try {
    await db.execute({
      sql: 'INSERT INTO unfollows (login, followed_at, unfollowed_at, suspect) VALUES (?,?,?,?)',
      args: [entry.login, entry.followedAt || null, entry.unfollowedAt, entry.suspect ? 1 : 0],
    });
  } catch (e) { addLog(`DB unfollow write error: ${e.message}`); }
}

async function dbAddBan(login) {
  if (!db) return;
  try {
    await db.execute({
      sql: 'INSERT INTO bans (login, banned_at) VALUES (?,?)',
      args: [login, new Date().toISOString()],
    });
  } catch (e) { addLog(`DB ban write error: ${e.message}`); }
}

// ─── Twitch API helpers ──────────────────────────────────────────────────────
async function twitchGet(url) {
  const res = await fetch(url, {
    headers: { 'Authorization': `Bearer ${TWITCH_TOKEN}`, 'Client-Id': TWITCH_CLIENT_ID },
  });
  if (!res.ok) throw new Error(`Twitch ${res.status}: ${await res.text()}`);
  return res.json();
}

async function getUserId(login) {
  const data = await twitchGet(`https://api.twitch.tv/helix/users?login=${login}`);
  return data.data[0]?.id;
}

async function getBroadcasterId() {
  const data = await twitchGet(`https://api.twitch.tv/helix/users?login=${TWITCH_CHANNEL}`);
  return data.data[0]?.id;
}

async function getModeratorId() {
  const url = TWITCH_MOD_LOGIN
    ? `https://api.twitch.tv/helix/users?login=${encodeURIComponent(TWITCH_MOD_LOGIN)}`
    : 'https://api.twitch.tv/helix/users';
  const data = await twitchGet(url);
  return data.data[0]?.id;
}

// ─── Scan chatters ────────────────────────────────────────────────────────────
async function scanChatters(broadcasterId, moderatorId) {
  const logins = new Set();
  let cursor = null;
  do {
    const url = `https://api.twitch.tv/helix/chat/chatters?broadcaster_id=${broadcasterId}&moderator_id=${moderatorId}&first=1000` + (cursor ? `&after=${cursor}` : '');
    const data = await twitchGet(url);
    for (const u of data.data) logins.add(u.user_login.toLowerCase());
    cursor = data.pagination?.cursor;
  } while (cursor);
  return logins;
}

// ─── Scan followers ───────────────────────────────────────────────────────────
async function scanFollowers(broadcasterId) {
  const map = new Map(); // login → followed_at
  let cursor = null;
  do {
    const url = `https://api.twitch.tv/helix/channels/followers?broadcaster_id=${broadcasterId}&first=100` + (cursor ? `&after=${cursor}` : '');
    const data = await twitchGet(url);
    for (const u of data.data) map.set(u.user_login.toLowerCase(), u.followed_at);
    cursor = data.pagination?.cursor;
  } while (cursor);
  return map;
}

// ─── Enrich user data ─────────────────────────────────────────────────────────
async function enrichUsers(logins) {
  const results = [];
  const chunks = [];
  const arr = [...logins];
  for (let i = 0; i < arr.length; i += 100) chunks.push(arr.slice(i, i + 100));
  for (const chunk of chunks) {
    const qs = chunk.map(l => `login=${encodeURIComponent(l)}`).join('&');
    const data = await twitchGet(`https://api.twitch.tv/helix/users?${qs}`);
    results.push(...data.data);
  }
  return results;
}

// ─── Main scan loop ───────────────────────────────────────────────────────────
let broadcasterId = null;
let moderatorId = null;
let lastFollowerMap = new Map();

async function runScan() {
  if (!broadcasterId) {
    broadcasterId = await getBroadcasterId();
    moderatorId = await getModeratorId();
    addLog(`Broadcaster ID: ${broadcasterId}, Moderator ID: ${moderatorId}`);
  }

  state.scanCount++;
  const scanNum = state.scanCount;

  // ── Chatters ──
  const chatterLogins = await scanChatters(broadcasterId, moderatorId);

  // ── Followers ──
  let followerMap = lastFollowerMap;
  const now = Date.now();
  const doFollowers = !state.lastFollowerScan || (now - new Date(state.lastFollowerScan).getTime()) >= FOLLOWER_INTERVAL;
  if (doFollowers) {
    followerMap = await scanFollowers(broadcasterId);
    // Detect unfollows
    if (lastFollowerMap.size > 0) {
      for (const [login, followedAt] of lastFollowerMap) {
        if (!followerMap.has(login)) {
          const acct = state.accounts.get(login);
          const entry = {
            login,
            followedAt,
            unfollowedAt: new Date().toISOString(),
            suspect: acct ? acct.suspect : BOT_PATTERN.test(login),
          };
          state.unfollows.unshift(entry);
          if (state.unfollows.length > 200) state.unfollows.pop();
          await dbAddUnfollow(entry);
        }
      }
    }
    lastFollowerMap = followerMap;
    state.lastFollowerScan = new Date().toISOString();
  }

  // ── Merge all logins to check ──
  const allLogins = new Set([...chatterLogins, ...followerMap.keys()]);

  // ── Enrich new logins ──
  const newLogins = [...allLogins].filter(l => !state.accounts.has(l));
  if (newLogins.length > 0) {
    const users = await enrichUsers(newLogins);
    for (const u of users) {
      const login = u.login.toLowerCase();
      const followedAt = followerMap.get(login) || null;
      state.accounts.set(login, {
        login,
        displayName: u.display_name,
        createdAt: u.created_at,
        suspect: isSuspect(login, u.created_at),
        isChatter: chatterLogins.has(login),
        isFollower: followerMap.has(login),
        followedAt,
      });
    }
  }

  // ── Update chatter/follower flags for existing accounts ──
  for (const [login, acct] of state.accounts) {
    acct.isChatter = chatterLogins.has(login);
    const wasFollower = acct.isFollower;
    acct.isFollower = followerMap.has(login);
    if (acct.isFollower && followerMap.has(login)) {
      acct.followedAt = followerMap.get(login);
    }
    if (!acct.isChatter && !acct.isFollower) {
      state.accounts.delete(login);
    }
  }

  // ── Discord alert for new suspects ──
  const newBots = [];
  for (const [login, acct] of state.accounts) {
    if (acct.suspect && !state.alerted.has(login)) {
      newBots.push(acct);
    }
  }

  if (newBots.length > 0 && DISCORD_WEBHOOK) {
    const desc = newBots.map(b =>
      `**${b.login}** — created ${b.createdAt ? b.createdAt.slice(0,10) : '?'}`
    ).join('\n');
    await fetch(DISCORD_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        username: 'Twitch Bot Monitor',
        embeds: [{
          title: `⚠️ ${newBots.length} suspected bot(s) detected in ${TWITCH_CHANNEL}`,
          description: desc,
          url: DASHBOARD_URL,
          fields: [{ name: '🔨 Take action', value: `[Open dashboard →](${DASHBOARD_URL})` }],
          color: 0xf59e0b,
          timestamp: new Date().toISOString(),
          footer: { text: 'Twitch Bot Monitor · render.com' },
        }],
      }),
    });
    for (const b of newBots) {
      state.alerted.add(b.login);
      await dbAddAlerted(b.login);
    }
  }

  state.lastScan = new Date().toISOString();
  state.error = null;
  addLog(`Scan #${scanNum} — ${chatterLogins.size} chatters, ${followerMap.size} followers, ${[...state.accounts.values()].filter(a=>a.suspect).length} suspected bots total`);
}

async function startScanner() {
  if (state.running) return;
  state.running = true;

  const loop = async () => {
    try { await runScan(); }
    catch (err) {
      state.error = err.message;
      addLog(`❌ Scan error: ${err.message}`);
    }
    setTimeout(loop, CHATTER_INTERVAL);
  };
  loop();
}

// ─── Express ──────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 7 * 24 * 60 * 60 * 1000 },
}));

function requireAuth(req, res, next) {
  if (req.session.authed) return next();
  res.status(401).json({ error: 'Unauthorized' });
}

app.get('/login', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Login — Twitch Bot Monitor</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#0e0e10;color:#efeff1;font-family:'Inter',system-ui,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh}
  .card{background:#18181b;border:1px solid #2d2d35;border-radius:12px;padding:40px;width:340px}
  h1{font-size:1.2rem;font-weight:600;margin-bottom:24px;color:#a970ff}
  input{width:100%;padding:10px 14px;background:#0e0e10;border:1px solid #3a3a4a;border-radius:8px;color:#efeff1;font-size:14px;outline:none;margin-bottom:16px}
  input:focus{border-color:#a970ff}
  button{width:100%;padding:11px;background:#a970ff;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer}
  button:hover{background:#bf93ff}
  .err{color:#f04747;font-size:13px;margin-bottom:12px}
</style>
</head>
<body>
<div class="card">
  <h1>🤖 Twitch Bot Monitor</h1>
  ${req.query.error ? '<p class="err">Incorrect password</p>' : ''}
  <form method="POST" action="/login">
    <input type="password" name="password" placeholder="Password" autofocus>
    <button type="submit">Sign in</button>
  </form>
</div>
</body>
</html>`);
});

app.post('/login', (req, res) => {
  if (DASHBOARD_PASS && req.body.password === DASHBOARD_PASS) {
    req.session.authed = true;
    res.redirect('/');
  } else {
    res.redirect('/login?error=1');
  }
});

app.post('/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/login');
});

// ─── API ──────────────────────────────────────────────────────────────────────
app.get('/api/state', requireAuth, (req, res) => {
  const accounts = [...state.accounts.values()];
  res.json({
    accounts,
    unfollows: state.unfollows,
    lastScan: state.lastScan,
    lastFollowerScan: state.lastFollowerScan,
    scanCount: state.scanCount,
    running: state.running,
    log: state.log,
    error: state.error,
  });
});

app.post('/api/ban', requireAuth, async (req, res) => {
  const { login } = req.body;
  if (!login) return res.status(400).json({ error: 'Missing login' });
  try {
    const bId = broadcasterId || await getBroadcasterId();
    const mId = moderatorId  || await getModeratorId();
    const userData = await twitchGet(`https://api.twitch.tv/helix/users?login=${encodeURIComponent(login)}`);
    const targetId = userData.data[0]?.id;
    if (!targetId) return res.status(404).json({ error: 'User not found' });

    const banRes = await fetch(`https://api.twitch.tv/helix/moderation/bans?broadcaster_id=${bId}&moderator_id=${mId}`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${TWITCH_TOKEN}`,
        'Client-Id': TWITCH_CLIENT_ID,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ data: { user_id: targetId, reason: "If you are not a bot, appeal and you'll be unbanned." } }),
    });
    if (!banRes.ok) {
      const txt = await banRes.text();
      return res.status(banRes.status).json({ error: txt });
    }
    state.accounts.delete(login);
    await dbAddBan(login);
    addLog(`🔨 Banned ${login}`);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Dashboard ────────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  if (!req.session.authed) return res.redirect('/login');
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Twitch Bot Monitor</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#0e0e10;color:#efeff1;font-family:'Inter',system-ui,sans-serif;font-size:14px;min-height:100vh}
  header{background:#18181b;border-bottom:1px solid #2d2d35;padding:14px 24px;display:flex;align-items:center;gap:16px}
  header h1{font-size:1rem;font-weight:700;color:#a970ff;flex:1}
  .badge{background:#2d2d35;padding:3px 10px;border-radius:20px;font-size:12px;color:#adadb8}
  .badge.ok{background:#1a3a1a;color:#4caf50}
  .badge.err{background:#3a1a1a;color:#f04747}
  .logout{background:none;border:1px solid #3a3a4a;color:#adadb8;padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12px}
  .logout:hover{border-color:#a970ff;color:#efeff1}
  .main{padding:24px;max-width:1400px;margin:0 auto}
  .stats{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap}
  .stat{background:#18181b;border:1px solid #2d2d35;border-radius:10px;padding:14px 20px;flex:1;min-width:140px}
  .stat-val{font-size:1.6rem;font-weight:700;color:#a970ff}
  .stat-lbl{font-size:11px;color:#adadb8;text-transform:uppercase;letter-spacing:.05em;margin-top:2px}
  .tabs{display:flex;gap:2px;margin-bottom:16px;border-bottom:1px solid #2d2d35}
  .tab{padding:8px 20px;cursor:pointer;border-bottom:2px solid transparent;font-size:13px;color:#adadb8;user-select:none}
  .tab.active{color:#a970ff;border-bottom-color:#a970ff}
  .panel{display:none}.panel.active{display:block}
  .filters{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap;align-items:center}
  .filters select,.filters input{background:#18181b;border:1px solid #2d2d35;color:#efeff1;padding:7px 12px;border-radius:7px;font-size:13px;outline:none}
  .filters select:focus,.filters input:focus{border-color:#a970ff}
  table{width:100%;border-collapse:collapse}
  th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #1e1e24}
  th{background:#18181b;color:#adadb8;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.04em;position:sticky;top:0;z-index:1;white-space:nowrap}
  th.sortable{cursor:pointer;user-select:none}
  th.sortable:hover{color:#efeff1}
  .sort-icon{display:inline-block;margin-left:4px;opacity:.4;font-style:normal}
  th.sort-asc .sort-icon,th.sort-desc .sort-icon{opacity:1;color:#a970ff}
  tr:hover td{background:#18181b55}
  .pill{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600}
  .pill.bot{background:#3a1a1a;color:#f04747}
  .pill.clean{background:#1a1a3a;color:#5865f2}
  .pill.chatter{background:#1a3a3a;color:#00c8ff}
  .pill.follower{background:#1a3a1a;color:#4caf50}
  .pill.both{background:#2d1a3a;color:#a970ff}
  .ban-btn{background:#f04747;border:none;color:#fff;padding:4px 12px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600}
  .ban-btn:hover{background:#ff6060}
  .ban-btn:disabled{background:#444;cursor:not-allowed}
  .tbl-wrap{overflow-x:auto;border-radius:10px;border:1px solid #2d2d35}
  .log-box{background:#18181b;border:1px solid #2d2d35;border-radius:10px;padding:16px;max-height:400px;overflow-y:auto;font-family:monospace;font-size:12px;line-height:1.7;color:#adadb8}
  .log-box .err{color:#f04747}
  #toast{position:fixed;bottom:20px;right:20px;background:#18181b;border:1px solid #2d2d35;border-radius:10px;padding:12px 18px;font-size:13px;opacity:0;transition:opacity .3s;pointer-events:none;z-index:99}
  #toast.show{opacity:1}
  .empty{text-align:center;padding:40px;color:#adadb8}
</style>
</head>
<body>
<header>
  <h1>🤖 Twitch Bot Monitor</h1>
  <span id="statusBadge" class="badge">Connecting…</span>
  <span id="scanInfo" class="badge" style="font-size:11px"></span>
  <button class="logout" onclick="fetch('/logout',{method:'POST'}).then(()=>location='/login')">Sign out</button>
</header>
<div class="main">
  <div class="stats">
    <div class="stat"><div class="stat-val" id="sChatters">—</div><div class="stat-lbl">Chatters</div></div>
    <div class="stat"><div class="stat-val" id="sFollowers">—</div><div class="stat-lbl">Followers</div></div>
    <div class="stat"><div class="stat-val" id="sBots" style="color:#f04747">—</div><div class="stat-lbl">Suspected Bots</div></div>
    <div class="stat"><div class="stat-val" id="sUnfollows">—</div><div class="stat-lbl">Unfollows Tracked</div></div>
  </div>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('accounts',this)">Accounts</div>
    <div class="tab" onclick="switchTab('unfollows',this)">Unfollows</div>
    <div class="tab" onclick="switchTab('log',this)">Log</div>
  </div>

  <div id="panel-accounts" class="panel active">
    <div class="filters">
      <select id="fFilter" onchange="render()">
        <option value="all">All accounts</option>
        <option value="suspect">Suspected bots only</option>
        <option value="chatter">Chatters only</option>
        <option value="follower">Followers only</option>
      </select>
      <input id="fSearch" type="text" placeholder="Search username…" oninput="render()">
    </div>
    <div class="tbl-wrap">
      <table>
        <thead id="thead-accounts"></thead>
        <tbody id="tbody-accounts"></tbody>
      </table>
    </div>
  </div>

  <div id="panel-unfollows" class="panel">
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>Username</th><th>Followed</th><th>Unfollowed</th><th>Status</th>
        </tr></thead>
        <tbody id="tbody-unfollows"></tbody>
      </table>
    </div>
  </div>

  <div id="panel-log" class="panel">
    <div id="logBox" class="log-box"></div>
  </div>
</div>
<div id="toast"></div>

<script>
let data = {};
let sortKey = 'suspect';
let sortDir = -1;

const COLS = [
  { key: 'login',     label: 'Username',   sortable: true  },
  { key: 'createdAt', label: 'Created',     sortable: true  },
  { key: 'age',       label: 'Acct Age',    sortable: true  },
  { key: 'followedAt',label: 'Following',   sortable: true  },
  { key: 'source',    label: 'Source',      sortable: false },
  { key: 'suspect',   label: 'Status',      sortable: false },
  { key: 'ban',       label: '',            sortable: false },
];

function switchTab(id, el) {
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('panel-' + id).classList.add('active');
  el.classList.add('active');
}

function setSort(key) {
  if (sortKey === key) sortDir = -sortDir;
  else { sortKey = key; sortDir = -1; }
  render();
}

function ageDays(iso) {
  if (!iso) return null;
  return Math.floor((Date.now() - new Date(iso)) / 86400000);
}

function fmtDate(iso) {
  if (!iso) return '—';
  return iso.slice(0, 10);
}

function fmtDays(n) {
  if (n === null) return '—';
  if (n < 30) return n + 'd';
  if (n < 365) return Math.floor(n/30) + 'mo';
  return (n/365).toFixed(1) + 'y';
}

function sortAccounts(arr) {
  return [...arr].sort((a, b) => {
    let av, bv;
    if (sortKey === 'login')     { av = a.login; bv = b.login; }
    else if (sortKey === 'createdAt') { av = a.createdAt || ''; bv = b.createdAt || ''; }
    else if (sortKey === 'age')  { av = ageDays(a.createdAt) ?? 999999; bv = ageDays(b.createdAt) ?? 999999; }
    else if (sortKey === 'followedAt') { av = a.followedAt || ''; bv = b.followedAt || ''; }
    else { av = a.suspect ? 1 : 0; bv = b.suspect ? 1 : 0; }
    if (av < bv) return sortDir;
    if (av > bv) return -sortDir;
    return 0;
  });
}

function render() {
  if (!data.accounts) return;

  const filter = document.getElementById('fFilter').value;
  const search = document.getElementById('fSearch').value.toLowerCase();

  // Build thead
  const thead = document.getElementById('thead-accounts');
  thead.innerHTML = '<tr>' + COLS.map(c => {
    if (!c.sortable) return '<th>' + c.label + '</th>';
    const active = sortKey === c.key;
    const arrow = active ? (sortDir === -1 ? '▼' : '▲') : '⇅';
    const cls = active ? (sortDir === -1 ? 'sortable sort-desc' : 'sortable sort-asc') : 'sortable';
    return '<th class="' + cls + '" onclick="setSort(&quot;' + c.key + '&quot;)">' + c.label + ' <i class="sort-icon">' + arrow + '</i></th>';
  }).join('') + '</tr>';

  let accounts = [...(data.accounts || [])];
  if (filter === 'suspect') accounts = accounts.filter(a => a.suspect);
  else if (filter === 'chatter') accounts = accounts.filter(a => a.isChatter);
  else if (filter === 'follower') accounts = accounts.filter(a => a.isFollower);
  if (search) accounts = accounts.filter(a => a.login.includes(search));
  accounts = sortAccounts(accounts);

  const tbody = document.getElementById('tbody-accounts');
  if (accounts.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty">No accounts match</td></tr>';
    return;
  }
  tbody.innerHTML = accounts.map(a => {
    const age = ageDays(a.createdAt);
    const fAge = ageDays(a.followedAt);
    let src = '';
    if (a.isChatter && a.isFollower) src = '<span class="pill both">Chatter+Follower</span>';
    else if (a.isChatter) src = '<span class="pill chatter">Chatter</span>';
    else src = '<span class="pill follower">Follower</span>';
    const status = a.suspect
      ? '<span class="pill bot">⚠ Suspected Bot</span>'
      : '<span class="pill clean">Clean</span>';
    const banBtn = a.suspect
      ? '<button class="ban-btn" onclick="banUser(&quot;' + a.login + '&quot;,this)">Ban</button>'
      : '';
    return '<tr>'
      + '<td><a href="https://twitch.tv/' + a.login + '" target="_blank" style="color:#a970ff;text-decoration:none">' + a.login + '</a></td>'
      + '<td>' + fmtDate(a.createdAt) + '</td>'
      + '<td>' + fmtDays(age) + '</td>'
      + '<td>' + (a.followedAt ? fmtDays(fAge) + ' ago' : '—') + '</td>'
      + '<td>' + src + '</td>'
      + '<td>' + status + '</td>'
      + '<td>' + banBtn + '</td>'
      + '</tr>';
  }).join('');

  // Unfollows
  const utbody = document.getElementById('tbody-unfollows');
  const unfollows = data.unfollows || [];
  if (unfollows.length === 0) {
    utbody.innerHTML = '<tr><td colspan="4" class="empty">No unfollows recorded yet</td></tr>';
  } else {
    utbody.innerHTML = unfollows.map(u => {
      const st = u.suspect
        ? '<span class="pill bot">⚠ Bot</span>'
        : '<span class="pill clean">Clean</span>';
      return '<tr>'
        + '<td><a href="https://twitch.tv/' + u.login + '" target="_blank" style="color:#a970ff;text-decoration:none">' + u.login + '</a></td>'
        + '<td>' + fmtDate(u.followedAt) + '</td>'
        + '<td>' + fmtDate(u.unfollowedAt) + '</td>'
        + '<td>' + st + '</td>'
        + '</tr>';
    }).join('');
  }

  // Log
  document.getElementById('logBox').innerHTML = (data.log || []).map(l => {
    const cls = l.includes('❌') ? 'err' : '';
    return '<div class="' + cls + '">' + l + '</div>';
  }).join('');

  // Stats
  const accts = data.accounts || [];
  document.getElementById('sChatters').textContent = accts.filter(a=>a.isChatter).length;
  document.getElementById('sFollowers').textContent = accts.filter(a=>a.isFollower).length;
  document.getElementById('sBots').textContent = accts.filter(a=>a.suspect).length;
  document.getElementById('sUnfollows').textContent = (data.unfollows||[]).length;

  // Header
  const badge = document.getElementById('statusBadge');
  if (data.error) { badge.textContent = '⚠ Error'; badge.className = 'badge err'; }
  else if (data.running) { badge.textContent = '● Live'; badge.className = 'badge ok'; }
  else { badge.textContent = 'Stopped'; badge.className = 'badge'; }

  const si = document.getElementById('scanInfo');
  si.textContent = data.lastScan ? 'Last scan: ' + new Date(data.lastScan).toLocaleTimeString() : '';
}

async function banUser(login, btn) {
  btn.disabled = true;
  btn.textContent = 'Banning…';
  try {
    const r = await fetch('/api/ban', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ login }),
    });
    const j = await r.json();
    if (j.ok) {
      showToast('✅ Banned ' + login);
      data.accounts = (data.accounts || []).filter(a => a.login !== login);
      render();
    } else {
      showToast('❌ ' + (j.error || 'Ban failed'));
      btn.disabled = false; btn.textContent = 'Ban';
    }
  } catch (e) {
    showToast('❌ ' + e.message);
    btn.disabled = false; btn.textContent = 'Ban';
  }
}

function showToast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3000);
}

async function poll() {
  try {
    const r = await fetch('/api/state');
    if (r.status === 401) { location = '/login'; return; }
    data = await r.json();
    render();
  } catch (e) { console.error(e); }
  setTimeout(poll, 15000);
}

poll();
</script>
</body>
</html>`);
});

// ─── Boot ─────────────────────────────────────────────────────────────────────
(async () => {
  await initDb();
  app.listen(PORT, () => addLog(`🚀 Server running on port ${PORT}`));
  if (!TWITCH_TOKEN || !TWITCH_CLIENT_ID || !TWITCH_CHANNEL) {
    addLog('⚠️  Missing TWITCH_TOKEN / TWITCH_CLIENT_ID / TWITCH_CHANNEL — scanner paused');
  } else {
    startScanner();
  }
})();
