'use strict';

// ─── Stream credits ───────────────────────────────────────────────────────────
// Tracks one stream at a time: when it goes live, who followed, subbed, gifted,
// cheered and raided, how long each person was in chat and how many messages
// they sent. Serves a scrolling "movie credits" page for OBS.
//
// Chat is read anonymously (Twitch allows read-only "justinfan" logins), so
// the token doesn't need any extra scopes.

const fs        = require('fs');
const path      = require('path');
const crypto    = require('crypto');
const fetch     = require('node-fetch');
const WebSocket = require('ws');

// Accounts that never appear in follower / longest-in-chat / most-messages credits
const KNOWN_BOTS = [
  'nightbot', 'streamelements', 'streamlabs', 'moobot', 'fossabot', 'wizebot',
  'soundalerts', 'sery_bot', 'kofistreambot', 'botrixoficial', 'commanderroot',
  'own3d', 'streamcaptainbot', 'pokemoncommunitygame', 'frostytoolsdotcom',
  'blerp', 'tangiabot', 'lumiastream', 'creatisbot', 'streamstickers'
];

// If the stream drops and comes back within this window, it's the same stream
const RESUME_GRACE_MS = 15 * 60 * 1000;

// How the credits look. Edited from the dashboard's Credits tab.
const SECTION_KEYS = ['honours', 'raids', 'gifts', 'bits', 'subs', 'followers', 'chatStats'];
const DEFAULT_SETTINGS = Object.freeze({
  font:        'Cormorant Garamond',
  textColor:   '#f2ede3',
  labelColor:  '#d2cbbd',
  size:        100,          // percent
  speed:       70,           // pixels per second at 1080p
  background:  'transparent',
  shadow:      true,
  closingText: 'Thanks for watching',
  show: Object.freeze({ honours: true, raids: true, gifts: true, bits: true, subs: true, followers: true, chatStats: true })
});

function clamp(n, lo, hi, fallback) {
  n = Number(n);
  return Number.isFinite(n) ? Math.min(hi, Math.max(lo, Math.round(n))) : fallback;
}

// Accept only safe values, so nothing typed in the editor can break the page
function cleanSettings(input, base) {
  const src = input && typeof input === 'object' ? input : {};
  const prev = base || DEFAULT_SETTINGS;
  const color = (v, d) => (typeof v === 'string' && /^#[0-9a-f]{6}$/i.test(v.trim()) ? v.trim().toLowerCase() : d);
  const font = typeof src.font === 'string' ? src.font.trim().replace(/\s+/g, ' ') : prev.font;
  const show = {};
  for (const k of SECTION_KEYS) {
    show[k] = src.show && typeof src.show[k] === 'boolean' ? src.show[k] : prev.show[k];
  }
  return {
    font:        /^[A-Za-z0-9 \-]{1,60}$/.test(font) ? font : prev.font,
    textColor:   color(src.textColor, prev.textColor),
    labelColor:  color(src.labelColor, prev.labelColor),
    size:        clamp(src.size  !== undefined ? src.size  : prev.size,  60, 160, prev.size),
    speed:       clamp(src.speed !== undefined ? src.speed : prev.speed, 20, 200, prev.speed),
    background:  ['transparent', 'black'].includes(src.background) ? src.background : prev.background,
    shadow:      typeof src.shadow === 'boolean' ? src.shadow : prev.shadow,
    closingText: typeof src.closingText === 'string' ? src.closingText.trim().slice(0, 120) : prev.closingText,
    show
  };
}

function parseIrc(line) {
  const msg = { tags: {}, prefix: null, command: null, params: [], trailing: null };
  let rest = line;
  if (rest[0] === '@') {
    const sp = rest.indexOf(' ');
    for (const kv of rest.slice(1, sp).split(';')) {
      const eq = kv.indexOf('=');
      const key = eq === -1 ? kv : kv.slice(0, eq);
      const val = eq === -1 ? '' : kv.slice(eq + 1);
      msg.tags[key] = val
        .replace(/\\s/g, ' ')
        .replace(/\\:/g, ';')
        .replace(/\\r/g, '\r')
        .replace(/\\n/g, '\n')
        .replace(/\\\\/g, '\\');
    }
    rest = rest.slice(sp + 1);
  }
  if (rest[0] === ':') {
    const sp = rest.indexOf(' ');
    msg.prefix = rest.slice(1, sp);
    rest = rest.slice(sp + 1);
  }
  const t = rest.indexOf(' :');
  if (t !== -1) {
    msg.trailing = rest.slice(t + 2);
    rest = rest.slice(0, t);
  }
  const parts = rest.split(' ').filter(Boolean);
  msg.command = parts[0];
  msg.params  = parts.slice(1);
  return msg;
}

function createCredits(ctx) {
  const { app, getDb, addLog, requireAuth, isSuspect, isFlagged, twitchHeaders, resolveUserIds, env } = ctx;
  const channel = String(env.TWITCH_CHANNEL || '').toLowerCase();
  const scanMs  = (Number(env.SCAN_INTERVAL_SECONDS) || 120) * 1000;
  const hidden  = new Set([
    ...KNOWN_BOTS,
    channel,
    ...String(env.CREDITS_HIDE || '').toLowerCase().split(',').map(s => s.trim()).filter(Boolean)
  ]);
  // Private link for OBS. Derived from SESSION_SECRET so no extra setting is needed.
  const creditsKey = env.CREDITS_KEY ||
    crypto.createHash('sha256').update(String(env.SESSION_SECRET) + ':obs-credits').digest('hex').slice(0, 24);
  const pageHtml = fs.readFileSync(path.join(__dirname, 'credits.html'), 'utf8');

  const live = {
    isLive: false, title: null, game: null, viewers: 0,
    startedAt: null, channelName: env.TWITCH_CHANNEL || '', lastChecked: 0
  };
  let session = null;
  let settings = cleanSettings({}, DEFAULT_SETTINGS);
  let lastPresenceTick = 0;
  const chat = { ws: null, connected: false, backoff: 2000, lastMsgAt: 0 };

  const isOpen = () => !!session && !session.endedAt;

  function newSession(id, startedAt, title, game) {
    return {
      id, startedAt, endedAt: null, lastLive: new Date().toISOString(), title, game,
      events: [], followSet: new Set(), chatters: new Map(), dirty: new Set()
    };
  }

  // ── Storage ────────────────────────────────────────────────────────────────
  async function dbExec(sql, args = []) {
    const db = getDb();
    if (!db) return null;
    try { return await db.execute({ sql, args }); }
    catch (e) { addLog(`Credits DB error: ${e.message}`, 'warn'); return null; }
  }

  async function addMissingColumns(db, table, columns) {
    const info = await db.execute(`PRAGMA table_info(${table})`);
    const have = new Set(info.rows.map(r => r.name));
    for (const [name, type] of Object.entries(columns)) {
      if (have.has(name)) continue;
      await db.execute(`ALTER TABLE ${table} ADD COLUMN ${name} ${type}`);
      addLog(`🛠 Updated the ${table} table (added ${name}) — saved data kept`);
    }
  }

  async function initStorage() {
    const db = getDb();
    if (!db) return;
    await db.batch([
      `CREATE TABLE IF NOT EXISTS streams (
         id TEXT PRIMARY KEY, started_at TEXT, ended_at TEXT, last_live TEXT, title TEXT, game TEXT, channel_name TEXT)`,
      `CREATE TABLE IF NOT EXISTS stream_events (
         id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id TEXT NOT NULL, type TEXT NOT NULL,
         login TEXT, display TEXT, amount INTEGER, extra TEXT, ts TEXT)`,
      `CREATE TABLE IF NOT EXISTS stream_chatters (
         stream_id TEXT NOT NULL, login TEXT NOT NULL, display TEXT,
         messages INTEGER NOT NULL DEFAULT 0, seconds INTEGER NOT NULL DEFAULT 0, suspect INTEGER,
         PRIMARY KEY (stream_id, login))`,
      `CREATE TABLE IF NOT EXISTS credits_settings (id INTEGER PRIMARY KEY CHECK (id = 1), json TEXT NOT NULL)`
    ], 'write');

    // Tables made by an older version keep their old columns ("CREATE TABLE IF NOT EXISTS"
    // doesn't touch them), so add anything newer here. Existing data is left as it is.
    await addMissingColumns(db, 'streams',         { last_live: 'TEXT', title: 'TEXT', game: 'TEXT', channel_name: 'TEXT' });
    await addMissingColumns(db, 'stream_events',   { extra: 'TEXT', ts: 'TEXT' });
    await addMissingColumns(db, 'stream_chatters', { suspect: 'INTEGER' });

    const st = await db.execute('SELECT json FROM credits_settings WHERE id = 1');
    if (st.rows.length) {
      try { settings = cleanSettings(JSON.parse(st.rows[0].json), DEFAULT_SETTINGS); }
      catch (e) { addLog('Saved credits look was unreadable — using the default look', 'warn'); }
    }

    // Restore the most recent stream so credits survive a restart
    const r = await db.execute('SELECT * FROM streams ORDER BY started_at DESC LIMIT 1');
    if (!r.rows.length) return;
    const s = r.rows[0];
    session = newSession(s.id, s.started_at, s.title, s.game);
    session.endedAt  = s.ended_at || null;
    session.lastLive = s.last_live || s.started_at;
    if (s.channel_name) live.channelName = s.channel_name;
    const ev = await db.execute({
      sql: 'SELECT type, login, display, amount, extra, ts FROM stream_events WHERE stream_id = ? ORDER BY id',
      args: [s.id]
    });
    for (const e of ev.rows) {
      session.events.push({
        type: e.type, login: e.login, display: e.display,
        amount: Number(e.amount) || 0, extra: e.extra, ts: e.ts
      });
      if (e.type === 'follow') session.followSet.add(e.login);
    }
    const ch = await db.execute({
      sql: 'SELECT login, display, messages, seconds, suspect FROM stream_chatters WHERE stream_id = ?',
      args: [s.id]
    });
    for (const c of ch.rows) {
      session.chatters.set(c.login, {
        display: c.display, messages: Number(c.messages) || 0, seconds: Number(c.seconds) || 0,
        suspect: c.suspect === null || c.suspect === undefined ? null : !!c.suspect
      });
    }
    addLog(`🎬 Restored credits for the stream from ${new Date(s.started_at).toLocaleString('en-GB')}`);
  }

  async function saveStream() {
    if (!session) return;
    await dbExec(
      `INSERT INTO streams (id, started_at, ended_at, last_live, title, game, channel_name) VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET ended_at = excluded.ended_at, last_live = excluded.last_live,
         title = excluded.title, game = excluded.game, channel_name = excluded.channel_name`,
      [session.id, session.startedAt, session.endedAt, session.lastLive, session.title, session.game, live.channelName]
    );
  }

  async function addEvent(e) {
    if (!isOpen()) return;
    session.events.push(e);
    await dbExec(
      'INSERT INTO stream_events (stream_id, type, login, display, amount, extra, ts) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [session.id, e.type, e.login, e.display, e.amount || 0, e.extra || null, e.ts]
    );
  }

  async function flushChatters() {
    const db = getDb();
    if (!db || !session || !session.dirty.size) return;
    const stmts = [...session.dirty].map(login => {
      const c = session.chatters.get(login);
      return {
        sql: `INSERT INTO stream_chatters (stream_id, login, display, messages, seconds, suspect) VALUES (?, ?, ?, ?, ?, ?)
              ON CONFLICT(stream_id, login) DO UPDATE SET display = excluded.display,
                messages = excluded.messages, seconds = excluded.seconds, suspect = excluded.suspect`,
        args: [session.id, login, c.display, c.messages, Math.round(c.seconds),
               c.suspect === null ? null : (c.suspect ? 1 : 0)]
      };
    });
    session.dirty.clear();
    try {
      for (let i = 0; i < stmts.length; i += 200) await db.batch(stmts.slice(i, i + 200), 'write');
    } catch (e) { addLog(`Credits DB error: ${e.message}`, 'warn'); }
  }

  function chatter(login, display) {
    let c = session.chatters.get(login);
    if (!c) { c = { display: display || login, messages: 0, seconds: 0, suspect: null }; session.chatters.set(login, c); }
    if (isFlagged(login)) c.suspect = true;   // once flagged, stays flagged for this stream
    if (display) c.display = display;
    session.dirty.add(login);
    return c;
  }

  // ── Live detection ─────────────────────────────────────────────────────────
  let checking = null;
  function checkLive() {
    if (!checking) checking = doCheckLive().finally(() => { checking = null; });
    return checking;
  }

  async function doCheckLive() {
    const r = await fetch(`https://api.twitch.tv/helix/streams?user_login=${encodeURIComponent(channel)}`,
      { headers: twitchHeaders() });
    const d = await r.json();
    if (!r.ok) throw new Error(d.message || 'Stream status check failed');
    const s = d.data && d.data[0];
    const nowIso = new Date().toISOString();
    live.lastChecked = Date.now();

    if (s) {
      Object.assign(live, {
        isLive: true, title: s.title, game: s.game_name, viewers: s.viewer_count,
        startedAt: s.started_at, channelName: s.user_name
      });
      const lastSeen = session ? new Date(session.endedAt || session.lastLive).getTime() : 0;
      const sameStream = session && (session.id === s.id || Date.now() - lastSeen < RESUME_GRACE_MS);
      if (sameStream) {
        if (session.endedAt) addLog('🔴 Stream back online — continuing the same credits');
        session.endedAt = null;
      } else {
        if (isOpen()) { session.endedAt = session.lastLive; await saveStream(); }
        session = newSession(s.id, s.started_at, s.title, s.game_name);
        lastPresenceTick = 0;
        addLog(`🔴 ${s.user_name} went live: ${s.title}`);
      }
      session.title    = s.title;
      session.game     = s.game_name;
      session.lastLive = nowIso;
      await saveStream();
    } else {
      Object.assign(live, { isLive: false, viewers: 0 });
      if (isOpen()) {
        session.endedAt = session.lastLive;
        lastPresenceTick = 0;
        await flushChatters();
        await saveStream();
        addLog('⚫ Stream ended — credits are ready');
      }
    }
  }

  // Run fn now if a stream is open; otherwise check (at most every 30s) and run if it turns out we're live
  function whenLive(fn) {
    if (isOpen()) return fn();
    if (Date.now() - live.lastChecked < 30000) return;
    checkLive().then(() => { if (isOpen()) return fn(); })
      .catch(e => addLog(`Live check failed: ${e.message}`, 'warn'));
  }

  // ── Called at the end of every monitor scan ────────────────────────────────
  async function afterScan({ chatters, broadcasterId, modId }) {
    await checkLive();
    if (!isOpen()) return;

    // Time in chat: credit everyone present with the time since the last scan
    const now = Date.now();
    const dt  = lastPresenceTick ? Math.min(now - lastPresenceTick, scanMs * 2) : 0;
    lastPresenceTick = now;
    for (const c of chatters) chatter(c.user_login, c.user_name).seconds += dt / 1000;

    await pollNewFollowers(broadcasterId, modId);
    await checkChattersForBots();
    await flushChatters();
    await saveStream();
  }

  // Decide once per person per stream whether they're a suspected bot, so a bot that
  // chats and then leaves still can't win "most messages" or "longest in chat".
  async function checkChattersForBots() {
    const unknown = [];
    for (const [login, c] of session.chatters) {
      if (c.suspect !== null) continue;
      if (isFlagged(login)) { c.suspect = true; session.dirty.add(login); continue; }
      // Only names shaped like the bot pattern need their account date looked up.
      // (Testing with a May 2024 date tells us whether the name alone already rules it out.)
      if (!isSuspect(login, '2024-05-15T00:00:00Z')) { c.suspect = false; session.dirty.add(login); continue; }
      unknown.push(login);
    }
    if (!unknown.length) return;
    const users   = await resolveUserIds(unknown);
    const created = new Map(users.map(u => [u.login.toLowerCase(), u.created_at]));
    for (const login of unknown) {
      const c = session.chatters.get(login);
      c.suspect = created.has(login) ? isSuspect(login, created.get(login)) : false;
      session.dirty.add(login);
    }
  }

  async function pollNewFollowers(broadcasterId, modId) {
    // Newest followers come first, so one page catches everyone since the last scan
    const r = await fetch(
      `https://api.twitch.tv/helix/channels/followers?broadcaster_id=${broadcasterId}&moderator_id=${modId}&first=100`,
      { headers: twitchHeaders() });
    const d = await r.json();
    if (!r.ok) throw new Error(d.message || 'Recent followers check failed');
    const start = new Date(session.startedAt).getTime();
    const fresh = d.data.filter(f =>
      new Date(f.followed_at).getTime() >= start && !session.followSet.has(f.user_login));
    if (!fresh.length) return;
    const users   = await resolveUserIds(fresh.map(f => f.user_login));
    const created = new Map(users.map(u => [u.login.toLowerCase(), u.created_at]));
    for (const f of fresh) {
      session.followSet.add(f.user_login);
      const suspect = created.has(f.user_login) && isSuspect(f.user_login, created.get(f.user_login));
      await addEvent({
        type: 'follow', login: f.user_login, display: f.user_name, amount: 0,
        extra: suspect ? 'suspect' : null, ts: f.followed_at
      });
    }
  }

  // ── Chat ───────────────────────────────────────────────────────────────────
  function connectChat() {
    if (!channel) return;
    const ws = new WebSocket('wss://irc-ws.chat.twitch.tv:443');
    chat.ws = ws;
    chat.lastMsgAt = Date.now();
    ws.on('open', () => {
      ws.send('CAP REQ :twitch.tv/tags twitch.tv/commands');
      ws.send('PASS SCHMOOPIIE');
      ws.send('NICK justinfan' + Math.floor(10000 + Math.random() * 80000));
      ws.send('JOIN #' + channel);
    });
    ws.on('message', data => {
      chat.lastMsgAt = Date.now();
      for (const line of String(data).split('\r\n')) {
        if (!line) continue;
        try { handleLine(line); }
        catch (e) { addLog(`Chat parse error: ${e.message}`, 'warn'); }
      }
    });
    ws.on('error', e => addLog(`Chat connection error: ${e.message}`, 'warn'));
    ws.on('close', () => {
      if (chat.ws !== ws) return;
      if (chat.connected) addLog('Chat connection closed — reconnecting', 'warn');
      chat.connected = false;
      setTimeout(connectChat, chat.backoff);
      chat.backoff = Math.min(chat.backoff * 2, 60000);
    });
  }

  // Twitch pings every ~5 min; if we hear nothing for 7, the connection is dead
  setInterval(() => {
    if (chat.ws && Date.now() - chat.lastMsgAt > 7 * 60 * 1000) {
      chat.lastMsgAt = Date.now();
      try { chat.ws.terminate(); } catch (e) { /* closing anyway */ }
    }
  }, 60 * 1000).unref();

  function send(line) {
    if (chat.ws && chat.ws.readyState === WebSocket.OPEN) chat.ws.send(line);
  }

  function handleLine(line) {
    const m = parseIrc(line);
    switch (m.command) {
      case 'PING':      return send('PONG :' + (m.trailing || 'tmi.twitch.tv'));
      case 'RECONNECT': return chat.ws && chat.ws.terminate();
      case 'ROOMSTATE':
        if (!chat.connected) {
          chat.connected = true;
          chat.backoff = 2000;
          addLog(`💬 Listening to #${channel} chat`);
        }
        return;
      case 'PRIVMSG':    return onMessage(m);
      case 'USERNOTICE': return onNotice(m);
    }
  }

  function onMessage(m) {
    const login   = (m.prefix || '').split('!')[0].toLowerCase();
    const display = m.tags['display-name'] || login;
    const bits    = parseInt(m.tags.bits, 10) || 0;
    if (!login) return;
    whenLive(() => {
      chatter(login, display).messages++;
      if (bits > 0) {
        return addEvent({ type: 'bits', login, display, amount: bits, ts: new Date().toISOString() });
      }
    });
  }

  function onNotice(m) {
    const t       = m.tags;
    const id      = t['msg-id'];
    const login   = (t.login || '').toLowerCase();
    const display = t['display-name'] || login;
    let ev = null;

    if (id === 'sub' || id === 'resub') {
      ev = { type: 'sub', login, display,
             amount: parseInt(t['msg-param-cumulative-months'], 10) || 1,
             extra: t['msg-param-sub-plan'] || null };
    } else if (id === 'giftpaidupgrade' || id === 'anongiftpaidupgrade' || id === 'primepaidupgrade') {
      ev = { type: 'sub', login, display, amount: 1, extra: t['msg-param-sub-plan'] || null };
    } else if (id === 'subgift') {
      // A gift bomb arrives as one "submysterygift" followed by one "subgift" per sub,
      // so counting only "subgift" gives the right total without double counting.
      const anon = login === 'ananonymousgifter' || !login;
      ev = { type: 'gift',
             login:   anon ? 'anonymous' : login,
             display: anon ? 'Anonymous' : display,
             amount: 1,
             extra: t['msg-param-recipient-display-name'] || t['msg-param-recipient-user-name'] || null };
    } else if (id === 'raid') {
      ev = { type: 'raid',
             login:   (t['msg-param-login'] || login).toLowerCase(),
             display: t['msg-param-displayName'] || display,
             amount:  parseInt(t['msg-param-viewerCount'], 10) || 0 };
    }
    if (!ev) return;
    ev.ts = new Date().toISOString();
    whenLive(() => addEvent(ev));
  }

  // ── Building the credits ───────────────────────────────────────────────────
  function buildCredits() {
    if (!session) return { empty: true, channel: live.channelName || env.TWITCH_CHANNEL || '' };

    const leaveOut = login => hidden.has(login) || isFlagged(login);

    const totalBy = type => {
      const m = new Map();
      for (const e of session.events) {
        if (e.type !== type) continue;
        const cur = m.get(e.login) || { name: e.display, total: 0 };
        cur.total += e.amount;
        cur.name = e.display;
        m.set(e.login, cur);
      }
      return [...m.values()].sort((a, b) => b.total - a.total);
    };

    const subs = new Map();
    for (const e of session.events) {
      if (e.type === 'sub') subs.set(e.login, { name: e.display, months: e.amount, plan: e.extra });
    }

    const followers = session.events
      .filter(e => e.type === 'follow' && e.extra !== 'suspect' && !leaveOut(e.login))
      .map(e => e.display);

    const raids = session.events
      .filter(e => e.type === 'raid')
      .map(e => ({ name: e.display, viewers: e.amount }));

    const people = [...session.chatters.entries()]
      .filter(([login, c]) => !c.suspect && !leaveOut(login))
      .map(([, c]) => ({ name: c.display, messages: c.messages, seconds: Math.round(c.seconds) }));
    const longest   = people.filter(p => p.seconds > 0).sort((a, b) => b.seconds - a.seconds)[0] || null;
    const chattiest = people.filter(p => p.messages > 0).sort((a, b) => b.messages - a.messages)[0] || null;

    const end = session.endedAt ? new Date(session.endedAt) : new Date();
    return {
      empty:       false,
      live:        isOpen(),
      channel:     live.channelName || env.TWITCH_CHANNEL || '',
      title:       session.title,
      game:        session.game,
      startedAt:   session.startedAt,
      endedAt:     session.endedAt,
      durationSec: Math.max(0, Math.round((end - new Date(session.startedAt)) / 1000)),
      longest:     longest   && { name: longest.name,   seconds:  longest.seconds },
      chattiest:   chattiest && { name: chattiest.name, messages: chattiest.messages },
      gifters:     totalBy('gift'),
      cheerers:    totalBy('bits'),
      subs:        [...subs.values()],
      raids,
      followers,
      totals: {
        messages: [...session.chatters.values()].reduce((n, c) => n + c.messages, 0),
        chatters: session.chatters.size
      }
    };
  }

  // ── Routes ─────────────────────────────────────────────────────────────────
  app.get('/credits/:key', (req, res) => {
    if (req.params.key !== creditsKey) return res.status(404).send('Not found');
    res.type('html').send(pageHtml);
  });
  app.get('/credits/:key/data', (req, res) => {
    if (req.params.key !== creditsKey) return res.status(404).json({ error: 'Not found' });
    const body = req.query.sample === '1' ? { ...sampleCredits(), sample: true } : buildCredits();
    res.set('Cache-Control', 'no-store').json({ ...body, settings });
  });

  // A made-up stream for trying out the look. Never stored, never mixed with real data.
  function sampleCredits() {
    const now = Date.now();
    const followers = ['PixelPanda', 'mossy_otter', 'Kaito_7', 'luna_wav', 'BreadBox', 'SirLurksALot',
      'tea_and_toast', 'Frostbyte', 'juniper_jay', 'CaptainCozy', 'OkayOkayOkay', 'ZeldaFan88', 'marigold', 'NewFan'];
    return {
      empty: false, live: false,
      channel: live.channelName || env.TWITCH_CHANNEL || 'YourChannel',
      title: 'Sample stream — this is test data',
      game: 'Just Chatting',
      startedAt: new Date(now - (3 * 3600 + 12 * 60) * 1000).toISOString(),
      endedAt: new Date(now).toISOString(),
      durationSec: 3 * 3600 + 12 * 60,
      longest:   { name: 'Lurker_Lou', seconds: 3 * 3600 + 10 * 60 },
      chattiest: { name: 'CozyGamer', messages: 412 },
      gifters:   [{ name: 'GiftGuru', total: 25 }, { name: 'Kindred', total: 2 }, { name: 'Anonymous', total: 1 }],
      cheerers:  [{ name: 'CozyGamer', total: 5000 }, { name: 'Chatty_Cathy', total: 150 }],
      subs: [
        { name: 'SubSam', months: 1, plan: '1000' },
        { name: 'LoyalLia', months: 14, plan: '2000' },
        { name: 'PrimePete', months: 1, plan: 'Prime' }
      ],
      raids: [{ name: 'RaidRita', viewers: 42 }, { name: 'TinyStreamer', viewers: 3 }],
      followers,
      totals: { messages: 2318, chatters: 97 }
    };
  }

  // Save the look from the dashboard editor
  app.post('/api/credits-settings', requireAuth, async (req, res) => {
    const next = req.body && req.body.reset ? cleanSettings({}, DEFAULT_SETTINGS) : cleanSettings(req.body, settings);
    settings = next;
    const r = await dbExec(
      `INSERT INTO credits_settings (id, json) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET json = excluded.json`,
      [JSON.stringify(settings)]
    );
    const saved = !getDb() || r !== null;
    addLog(req.body && req.body.reset ? '🎬 Credits look reset to default' : '🎬 Credits look updated');
    res.json({ ok: true, settings, stored: saved });
  });

  function status() {
    return {
      live: { ...live },
      chatConnected: chat.connected,
      creditsPath: '/credits/' + creditsKey,
      credits: buildCredits(),
      settings,
      defaults: DEFAULT_SETTINGS
    };
  }

  async function init() {
    try { await initStorage(); }
    catch (e) { addLog(`Credits storage setup failed: ${e.message}`, 'error'); }
    connectChat();
  }

  return { init, afterScan, status, _test: { handleLine, parseIrc, buildCredits, checkLive, getSession: () => session } };
}

module.exports = { createCredits, parseIrc };
