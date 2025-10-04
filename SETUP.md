# 🚀 Setup Guide - Schrodinger's Cat Bot

This guide will help you deploy your own instance of the Schrodinger's Cat file-sharing bot.

---

## 📋 Prerequisites

Before you begin, make sure you have:
- A Telegram account
- A MongoDB Atlas account (free tier works)
- Python 3.11+ installed (for local deployment)
- Git installed

---

## ⚡ Quick Start (5 Steps)

### Step 1: Get Telegram Credentials

#### 1.1 Create Your Bot
1. Open Telegram and search for [@BotFather](https://t.me/BotFather)
2. Send `/newbot` command
3. Follow prompts to choose a name and username
4. **Save the bot token** (looks like: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`)

#### 1.2 Get API Credentials
1. Visit https://my.telegram.org
2. Log in with your phone number
3. Click **"API Development Tools"**
4. Create a new application (any name/description)
5. **Save your `APP_ID`** (number like: `12345678`)
6. **Save your `API_HASH`** (looks like: `abc123def456ghi789jkl`)

#### 1.3 Get Your User ID
1. Open [@userinfobot](https://t.me/userinfobot) in Telegram
2. Send any message
3. **Save your `ID`** (number like: `123456789`)

#### 1.4 Create Database Channel
1. In Telegram, create a **new private channel**
2. Add your bot as an **administrator** with **all permissions**
3. Forward any message from the channel to [@username_to_id_bot](https://t.me/username_to_id_bot)
4. **Save the channel ID** (looks like: `-100123456789`)

---

### Step 2: Get MongoDB Database

1. Go to https://www.mongodb.com/cloud/atlas
2. Sign up (free)
3. Create a **FREE cluster** (M0)
4. Click **"Connect"** → **"Connect your application"**
5. Copy the connection string (looks like: `mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/`)
6. **Replace `<password>`** with your actual password
7. **Save this connection string**

---

### Step 3: Configure Your Bot

#### Option A: Local Deployment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/abraxas0001/Schrodinger-s-Cat.git
   cd Schrodinger-s-Cat
   ```

2. **Copy the example environment file:**
   ```bash
   cp .env.example .env
   ```

3. **Edit `.env` file** (use any text editor):
   ```bash
   # Windows
   notepad .env
   
   # Linux/Mac
   nano .env
   ```

4. **Fill in your credentials:**
   ```env
   TG_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
   APP_ID=12345678
   API_HASH=abc123def456ghi789jkl
   CHANNEL_ID=-100123456789
   OWNER_ID=123456789
   DATABASE_URL=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
   DATABASE_NAME=FileStoreBot
   ```

5. **Install dependencies:**
   ```bash
   # Create virtual environment
   python -m venv .venv
   
   # Activate it
   # Windows:
   .venv\Scripts\activate
   # Linux/Mac:
   source .venv/bin/activate
   
   # Install packages
   pip install -r requirements.txt
   ```

6. **Run the bot:**
   ```bash
   python main.py
   ```

#### Option B: Heroku Deployment

1. **Fork this repository** on GitHub

2. **Go to [Heroku](https://heroku.com)** and create account

3. **Click this button** (after forking):
   
   [![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy)

4. **Fill in the form** with your credentials from Step 1 & 2

5. **Click "Deploy App"**

6. **Done!** Your bot is live

#### Option C: Railway Deployment

1. **Fork this repository** on GitHub

2. **Go to [Railway](https://railway.app)** and sign in with GitHub

3. **Click "New Project"** → **"Deploy from GitHub repo"**

4. **Select your forked repository**

5. **Add environment variables** (click "Variables" tab):
   - Add each variable from your `.env.example`
   - Fill in the values

6. **Deploy!**

---

### Step 4: Test Your Bot

1. **Open your bot** in Telegram (search for @YourBotUsername)
2. **Send `/start`** command
3. **Should see welcome message!** ✅

---

### Step 5: Configure Bot Features

Now that your bot is running, configure it using admin commands:

```
/protect - Toggle content protection
/dlt_time 600 - Auto-delete files after 600 seconds
/globalcap Your caption here - Set global caption
/caption - View all caption settings
```

See full admin commands in the [README.md](README.md)

---

## 🔧 Configuration Reference

### Required Variables

| Variable | Where to Get It | Example |
|----------|-----------------|---------|
| `TG_BOT_TOKEN` | @BotFather | `123456:ABCdef...` |
| `APP_ID` | my.telegram.org | `12345678` |
| `API_HASH` | my.telegram.org | `abc123def456...` |
| `CHANNEL_ID` | @username_to_id_bot | `-100123456789` |
| `OWNER_ID` | @userinfobot | `123456789` |
| `DATABASE_URL` | MongoDB Atlas | `mongodb+srv://...` |

### Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_NAME` | `FileStoreBot` | Database name |
| `PORT` | `8001` | Web server port |
| `OWNER` | `YourUsername` | Your username |
| `TG_BOT_WORKERS` | `200` | Worker threads |
| `FSUB_LINK_EXPIRY` | `0` | Link expiry (0=never) |
| `PROTECT_CONTENT` | `True` | Forward protection |
| `START_PIC` | URL | Start message image |
| `FORCE_PIC` | URL | Force-sub image |

---

## 🐛 Troubleshooting

### Bot doesn't start
- ❌ **Wrong token** → Check @BotFather token
- ❌ **Bot not admin in channel** → Add bot to channel with admin rights
- ❌ **Wrong channel ID** → Get ID from @username_to_id_bot

### Database errors
- ❌ **Wrong connection string** → Check MongoDB Atlas
- ❌ **IP not whitelisted** → In Atlas: Network Access → Add `0.0.0.0/0`

### Files not sending
- ❌ **Bot not in channel** → Add bot as admin
- ❌ **Wrong channel ID** → Must be negative number

### Force-sub not working
- ❌ **Commands:** `/addchnl` to add channels
- ❌ **Commands:** `/fsub_mode` to enable

---

## 📚 Next Steps

1. ✅ **Read [README.md](README.md)** for all features
2. ✅ **Check [SECURITY_AUDIT.md](SECURITY_AUDIT.md)** for security tips
3. ✅ **Join our community** (if available)
4. ✅ **Star the repo** if you find it useful! ⭐

---

## 🆘 Need Help?

- 📖 Read the [README](README.md)
- 🐛 [Open an issue](https://github.com/abraxas0001/Schrodinger-s-Cat/issues)
- 💬 Check existing issues first

---

## ⚠️ Security Reminder

- ✅ **Never commit** `.env` file to git
- ✅ **Never share** your bot token or API credentials
- ✅ **Use strong passwords** for MongoDB
- ✅ **Enable 2FA** on your Telegram account

---

**Happy file sharing!** 🐱📦
