# Twitch Bot Monitor

24/7 Twitch bot detector that runs on Render.com. Scans your chatters and followers, detects accounts matching the May 2024 bot pattern (5 letters + 2 numbers), and fires Discord alerts.

---

## Deploy to Render (step by step)

### 1. Put this on GitHub

- Go to [github.com/new](https://github.com/new) and create a repo called `twitch-bot-monitor`
- Upload all files from this folder (drag and drop works fine)

### 2. Create a Render Web Service

- Go to [render.com](https://render.com) and sign up / log in
- Click **New → Web Service**
- Connect your GitHub account and select the `twitch-bot-monitor` repo
- Fill in the settings:
  - **Name:** twitch-bot-monitor (or anything you like)
  - **Region:** pick the closest to you
  - **Branch:** main
  - **Runtime:** Node
  - **Build Command:** `npm install`
  - **Start Command:** `npm start`
  - **Instance Type:** Free

### 3. Set environment variables

In Render, go to your service → **Environment** → add these:

| Key | Value |
|-----|-------|
| `TWITCH_CLIENT_ID` | Your Twitch app Client ID |
| `TWITCH_TOKEN` | Your OAuth token (without "Bearer") |
| `TWITCH_CHANNEL` | Broadcaster username (lowercase) |
| `TWITCH_MOD_LOGIN` | Mod account username (whose token this is) |
| `DISCORD_WEBHOOK_URL` | Your Discord webhook URL |
| `DASHBOARD_PASSWORD` | Password to protect the dashboard |
| `SESSION_SECRET` | Any long random string (e.g. `xK9mP2qL8nR5vT3w`) |
| `SCAN_INTERVAL_SECONDS` | `120` (scan every 2 minutes) |
| `FOLLOWER_INTERVAL_SECONDS` | `600` (rescan followers every 10 min) |

### 4. Deploy

- Click **Create Web Service** — Render will build and deploy automatically
- Your dashboard will be live at `https://your-service-name.onrender.com`
- Log in with the `DASHBOARD_PASSWORD` you set

---

## Getting your Twitch credentials

**Client ID:**
1. Go to [dev.twitch.tv/console](https://dev.twitch.tv/console)
2. Click **Register Your Application**
3. Name: anything, OAuth Redirect URL: `http://localhost`, Category: Other
4. Copy the Client ID

**OAuth Token:**
1. Go to [twitchtokengenerator.com](https://twitchtokengenerator.com)
2. Enter your Client ID and Client Secret
3. Enable scopes: `moderator:read:chatters`, `moderator:read:followers`, `user:read:email`
4. Copy the Access Token (not the refresh token)

**Discord Webhook:**
1. Open Discord, go to the channel you want alerts in
2. Edit Channel → Integrations → Webhooks → New Webhook
3. Copy Webhook URL

---

## Notes

- Render's free tier may spin down after inactivity — to keep it always alive, set up a free uptime monitor at [uptimerobot.com](https://uptimerobot.com) pointing at your Render URL
- The scanner only alerts once per account per session — restarting resets the alert history
- The dashboard auto-refreshes every 10 seconds
