# 🎯 Quick Reference Card

## For New Users

### First Time Setup (2 minutes)

```bash
# 1. Clone the repo
git clone https://github.com/abraxas0001/Schrodinger-s-Cat.git
cd Schrodinger-s-Cat

# 2. Copy environment template
cp .env.example .env

# 3. Edit .env with your credentials
# (Use notepad, nano, vim, or any text editor)
notepad .env  # Windows
nano .env     # Linux/Mac

# 4. Install dependencies
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt

# 5. Run!
python main.py
```

### Where to Get Credentials

| What | Where | How |
|------|-------|-----|
| Bot Token | [@BotFather](https://t.me/BotFather) | `/newbot` |
| API ID & Hash | [my.telegram.org](https://my.telegram.org) | API Tools → Create App |
| Your User ID | [@userinfobot](https://t.me/userinfobot) | Send any message |
| Channel ID | [@username_to_id_bot](https://t.me/username_to_id_bot) | Forward from channel |
| MongoDB | [MongoDB Atlas](https://mongodb.com/cloud/atlas) | Free M0 cluster |

### Your .env File Should Look Like

```env
TG_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
APP_ID=12345678
API_HASH=abc123def456ghi789jklmno
CHANNEL_ID=-100123456789
OWNER_ID=987654321
DATABASE_URL=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true
DATABASE_NAME=FileStoreBot
```

### Essential Admin Commands

```
/caption          - View all caption settings
/protect          - Toggle content protection
/dlt_time 600     - Auto-delete after 10 minutes
/addchnl          - Add force-sub channel
/backup           - Get database backup
```

### Common First-Time Issues

❌ **Bot doesn't start**
- Check if bot token is correct
- Make sure bot is admin in database channel

❌ **"Database channel invalid"**
- Channel ID must be negative (start with -100)
- Bot must be admin with all permissions

❌ **MongoDB connection failed**
- Check if IP is whitelisted (allow 0.0.0.0/0)
- Verify password is correct in connection string

❌ **Module not found errors**
- Activate virtual environment first
- Run `pip install -r requirements.txt`

### File Structure You'll See

```
Schrodinger-s-Cat/
├── .env                  ← YOUR CREDENTIALS (create this!)
├── .env.example          ← Template
├── SETUP.md             ← Full setup guide
├── README.md            ← Features & overview
├── SECURITY_AUDIT.md    ← Security info
├── bot.py               ← Main bot class
├── main.py              ← Run this!
├── config.py            ← Configuration loader
├── requirements.txt     ← Dependencies
├── database/
│   └── database.py      ← Database operations
└── plugins/
    ├── start.py         ← /start handler
    ├── admin.py         ← Admin commands
    └── ...              ← Other features
```

### Testing Your Bot

1. Open Telegram
2. Search for your bot (@YourBotUsername)
3. Send `/start`
4. Should see welcome message ✅

### Next Steps After Setup

1. ✅ Send a file to your database channel
2. ✅ Forward it to bot (as admin)
3. ✅ Bot will give you a shareable link
4. ✅ Share the link with users
5. ✅ Configure features with admin commands

### Getting Help

- 📖 **Full Guide:** [SETUP.md](SETUP.md)
- 🔐 **Security:** [SECURITY_AUDIT.md](SECURITY_AUDIT.md)
- 📚 **Features:** [README.md](README.md)
- 🐛 **Issues:** [GitHub Issues](https://github.com/abraxas0001/Schrodinger-s-Cat/issues)

---

**Remember:** Never commit your `.env` file! It's already in `.gitignore`.
