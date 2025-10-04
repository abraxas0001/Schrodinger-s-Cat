<div align="center">

<h1>Schrodinger's Cat 🐾📦</h1>
<p><strong>High‑performance Telegram File ↔ Link Bot</strong><br>
Store media in a private channel, deliver it on demand via /start deep links,<br>
enforce force‑subscription, customize + sanitize captions, and keep automatic backups.
</p>

<p>
<a href="https://github.com/abraxas0001/Schrodinger-s-Cat"><img alt="Repo" src="https://img.shields.io/badge/Repo-Schrodinger's%20Cat-24292e?style=for-the-badge"/></a>
<a href="#features"><img alt="Features" src="https://img.shields.io/badge/Features-Packed-success?style=for-the-badge"/></a>
<a href="#deploy"><img alt="Deploy" src="https://img.shields.io/badge/Deploy-Heroku%20|%20Railway%20|%20Docker-blue?style=for-the-badge"/></a>
<a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge"/></a>
</p>

</div>

---

## ⚡ Overview

This bot acts as a **content gateway**: you drop files into a private channel (DB channel) and users retrieve them through unique deep links (`/start get-<id>` encoded). You control presentation (captions, replacements, appended text, link wipes) and behavior (protection, auto delete, force‑sub) entirely through admin commands—no code edits needed.

> Think of it as a “headless CDN” for Telegram with on‑the‑fly caption mutation + safety controls.

---
## ✨ Features

| Area | Power | Commands / Behavior |
|------|-------|---------------------|
| Caption Replace | Swap any text fragment in original captions | `/replace old new`, `/replace off` |
| Global Caption | Attach a fallback when media has no caption | `/globalcap text | on | off` |
| Link Replace | Replace just one specific link everywhere | `/replace_link old new` |
| Replace All Links | Force every detected link to a single URL | `/replace_all_link link | on | off` |
| Append Caption | Add extra lines only if a caption already exists | `/caption_add text | off` |
| Strip Original Links | Remove links from the original (before other rules) | `/caption_clean on | off` |
| Caption Snapshot | View current settings state | `/caption` |
| Content Protection | Toggle Telegram’s forward‑restriction | `/protect`, `/check_protect` |
| Auto Delete | Scheduled deletion after delivery | `/dlt_time`, `/check_dlt_time` |
| Force Subscription | Require joins before delivering | `/addchnl`, `/delchnl`, `/listchnl`, `/fsub_mode` |
| Broadcast | Send docs / photos / custom text to all users | `/dbroadcast`, `/pbroadcast`, `/broadcast` |
| Admin Management | Multi‑admin support | `/add_admin`, `/deladmin`, `/admins` |
| User Control | Ban / Unban / Ban list | `/ban`, `/unban`, `/banlist` |
| Backups | Daily + on‑demand Mongo export (JSON/BSON fallback) | `/backup` |

---
## 🧠 Caption Mutation Pipeline

Order of operations when a user requests media:

1. Load original caption (HTML if available)
2. If `/caption_clean on`: strip `<a ...>...</a>`, parenthesized links `(http…)`, bare `https://...`
3. If file has no caption and global caption enabled → inject global
4. Apply text replace (`/replace`)
5. Apply single link swap (`/replace_link`)
6. Apply replace‑all link override (`/replace_all_link on`)
7. Append extra text (only if caption not empty) (`/caption_add`)
8. Deliver with optional content protection

This guarantees appended or admin‑added blocks never get “eaten” by stripping.

---
## 🛡 Force‑Sub + Access Links

Each media posted (or batch) in the DB channel yields a deep link like:

```
https://t.me/<your_bot>?start=<encoded_token>
```

Users must satisfy force subscription (if enabled) or they receive a join prompt with retry button.

---
## 🗄 Backups

Daily scheduler (midnight server time) sends a zipped Mongo export to the owner. Manual trigger: `/backup`.

Modes:
- JSON line export (default)
- BSON (attempts `mongodump`, falls back gracefully)

---
## 🧩 Tech Stack

| Layer | Tech |
|-------|------|
| Bot Framework | Pyrogram / PyroFork |
| Async Runtime | Python `asyncio` |
| DB | MongoDB (Motor + PyMongo) |
| Scheduler | APScheduler |
| Web Keep‑Alive | aiohttp mini server |
| Deployment | Docker / Procfile / Heroku-compatible |

---
## 🚀 Deploy

### 1. Environment Variables
Create `.env` (or set in hosting panel):

```
TG_BOT_TOKEN=123456:ABCDEF...
APP_ID=123456
API_HASH=xxxxxxxxxxxxxxxx
CHANNEL_ID=-100XXXXXXXXXX          # DB channel (bot must be admin)
DATABASE_URL=mongodb+srv://...
DATABASE_NAME=Cluster0
OWNER_ID=123456789
PORT=8001
```

### 2. Docker
```bash
docker build -t schrodinger-bot .
docker run -e TG_BOT_TOKEN=... -e APP_ID=... -e API_HASH=... -e CHANNEL_ID=... -e DATABASE_URL=... -e OWNER_ID=... schrodinger-bot
```

### 3. Local (Python 3.11+ recommended)
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

### 4. Heroku / Railway
- Add repo
- Set env vars
- Deploy (Procfile already included)

---
## 🔐 Security Notes

- Never commit real tokens / DB URIs (currently defaults are hard‑coded – replace them!).
- Use a dedicated least‑privilege Mongo user.
- Rotate OWNER_ID if handing over ownership.
- Consider enabling Telegram 2FA on the managing account.

---
## 🛠 Admin Command Cheat Sheet

```
/caption                # Show current caption system state
/caption_clean on|off   # Strip original links
/replace old new | off
/globalcap text | on | off
/replace_link old new | off
/replace_all_link link | on | off
/caption_add text | off
/dlt_time 600           # Auto delete seconds
/check_dlt_time
/protect | /check_protect
/addchnl /delchnl /listchnl /fsub_mode
/ban /unban /banlist
/add_admin /deladmin /admins
/backup
```

---
## 🧪 Development Tips

Run syntax check:
```bash
python -m compileall .
```

Quick Mongo shell (Atlas):
```bash
mongosh "${DATABASE_URL}" --quiet
```

Extend caption logic: edit `plugins/start.py` (look for the mutation loop).

---
## 🐞 Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Links not stripped | `/caption_clean` off | Run `/caption_clean on` |
| Photo fails to send | Empty caption with HTML parse | We skip parse when empty (already fixed) |
| No deep link files | Bot not admin in DB channel | Promote bot, retry |
| Force-sub loop | Expired invite or missing channel | Regenerate link / verify channel id |
| Backup fails | `mongodump` missing | Falls back to JSON automatically |

---
## 🤝 Contributing

1. Fork
2. Create feature branch
3. Make changes (add tests/docs if relevant)
4. PR with clear description

---
## 📄 License

Released under the MIT License – see [`LICENSE`](LICENSE).

---
## 💬 Credits

Built with ❤️ using Pyrogram + MongoDB.

Feel free to open issues or suggest improvements.

> “If you observe a file without requesting it, does it still exist?” – Schrodinger’s (File) Cat
