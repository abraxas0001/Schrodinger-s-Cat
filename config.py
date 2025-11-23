import os
from os import environ,getenv
import logging
from logging.handlers import RotatingFileHandler


#--------------------------------------------
#Bot token @Botfather
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "") # Get from @BotFather
APP_ID = int(os.environ.get("APP_ID", "0")) # Get from my.telegram.org
API_HASH = os.environ.get("API_HASH", "") # Get from my.telegram.org
#--------------------------------------------

CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "0")) # Your DB channel ID (must be negative for supergroups/channels)
OWNER = os.environ.get("OWNER", "YourUsername") # Owner username without @
OWNER_ID = int(os.environ.get("OWNER_ID", "0")) # Your Telegram user ID
#--------------------------------------------
PORT = os.environ.get("PORT", "8001")
#--------------------------------------------
DB_URI = os.environ.get("DATABASE_URL", "") # MongoDB connection string
DB_NAME = os.environ.get("DATABASE_NAME", "FileStoreBot") # Database name
#--------------------------------------------
FSUB_LINK_EXPIRY = int(os.getenv("FSUB_LINK_EXPIRY", "0"))  # 0 means no expiry
BAN_SUPPORT = os.environ.get("BAN_SUPPORT", "https://t.me/BeingHumanAssociation") # Your support group link
TG_BOT_WORKERS = int(os.environ.get("TG_BOT_WORKERS", "200"))
#--------------------------------------------
START_PIC = os.environ.get("START_PIC", "https://i.pinimg.com/736x/d7/2b/a9/d72ba9bc6ccd1180cfd143c91f5c5e5b.jpg")
FORCE_PIC = os.environ.get("FORCE_PIC", "https://i.pinimg.com/736x/ab/b7/42/abb742eda8f1fd1a46e09412e8f62dca.jpg")
#--------------------------------------------

#--------------------------------------------
HELP_TXT = "<b><blockquote>ᴛʜɪs ɪs ᴀɴ ғɪʟᴇ ᴛᴏ ʟɪɴᴋ ʙᴏᴛ ᴡᴏʀᴋ ғᴏʀ @Lifesuckkkkkssss\n\n❏ ʙᴏᴛ ᴄᴏᴍᴍᴀɴᴅs\n├/start : sᴛᴀʀᴛ ᴛʜᴇ ʙᴏᴛ\n├/about : ᴏᴜʀ Iɴғᴏʀᴍᴀᴛɪᴏɴ\n└/help : ʜᴇʟᴘ ʀᴇʟᴀᴛᴇᴅ ʙᴏᴛ\n\n sɪᴍᴘʟʏ ᴄʟɪᴄᴋ ᴏɴ ʟɪɴᴋ ᴀɴᴅ sᴛᴀʀᴛ ᴛʜᴇ ʙᴏᴛ ᴊᴏɪɴ ʙᴏᴛʜ ᴄʜᴀɴɴᴇʟs ᴀɴᴅ ᴛʀʏ ᴀɢᴀɪɴ ᴛʜᴀᴛs ɪᴛ.....!\n\n ᴅᴇᴠᴇʟᴏᴘᴇᴅ ʙʏ <a href=https://t.me/beinghumanassociation>sᴜʙᴀʀᴜ</a></blockquote></b>"
ABOUT_TXT = "<b><blockquote>ᴀʟʟ ᴍᴇᴅɪᴀ sʜᴀʀᴇᴅ ᴏɴ ᴛʜɪs ᴄʜᴀɴɴᴇʟ ɪs sᴏᴜʀᴄᴇᴅ ғʀᴏᴍ ᴘᴜʙʟɪᴄʟʏ ᴀᴠᴀɪʟᴀʙʟᴇ ᴘʟᴀᴛғᴏʀᴍs ᴀɴᴅ ɪs ɴᴏᴛ ᴏᴡɴᴇᴅ ᴏʀ ᴄʀᴇᴀᴛᴇᴅ ʙʏ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ ᴏᴡɴᴇʀ.</b></blockquote>\n<b><blockquote>ᴛʜᴇ ᴄᴏɴᴛᴇɴᴛ ɪs ᴘʀᴏᴠɪᴅᴇᴅ sᴏʟᴇʟʏ ғᴏʀ ɪɴғᴏʀᴍᴀᴛɪᴏɴᴀʟ ᴀɴᴅ ᴇɴᴛᴇʀᴛᴀɪɴᴍᴇɴᴛ ᴘᴜʀᴘᴏsᴇs.</b></blockquote>\n<b><blockquote>ᴠɪᴇᴡᴇʀs ᴍᴜsᴛ ʙᴇ 18 ʏᴇᴀʀs ᴏғ ᴀɡᴇ ᴏʀ ᴏʟᴅᴇʀ ᴛᴏ ᴀᴄᴄᴇss ᴀɴᴅ ᴄᴏɴsᴜᴍᴇ ᴛʜᴇ sʜᴀʀᴇᴅ ᴍᴇᴅɪᴀ.</b></blockquote>\n<b><blockquote>ɴᴏɴᴇ ᴏғ ᴛʜᴇ ᴄᴏɴᴛᴇɴᴛ ᴘᴏsᴛᴇᴅ ɪs ɪɴᴛᴇɴᴅᴇᴅ ᴛᴏ ᴅᴇғᴀᴍᴇ, ʜᴀʀᴍ, ᴏʀ ᴍɪsʀᴇᴘʀᴇsᴇɴᴛ ᴀɴʏ ᴘᴇʀsᴏɴ, ɢʀᴏᴜᴘ, ᴏʀ ᴇɴᴛɪᴛʏ. ᴛʜᴇ ᴄʜᴀɴɴᴇʟ ᴀɴᴅ ɪᴛs ᴏᴡɴᴇʀ ᴀssᴜᴍᴇ ɴᴏ ʀᴇsᴘᴏɴsɪʙɪʟɪᴛʏ ғᴏʀ ʜᴏᴡ ᴛʜᴇ ᴄᴏɴᴛᴇɴᴛ ɪs ᴜsᴇᴅ ʙᴇʏᴏɴᴅ ɪᴛs ɪɴᴛᴇɴᴅᴇᴅ ᴘᴜʀᴘᴏsᴇ.</b></blockquote>\n<b><blockquote>ʙʏ ᴀᴄᴄᴇssɪɴɢ ᴛʜɪs ᴄʜᴀɴɴᴇʟ, ʏᴏᴜ ᴀᴄᴋɴᴏᴡʟᴇᴅɢᴇ ᴀɴᴅ ᴀɢʀᴇᴇ ᴛᴏ ᴛʜᴇsᴇ ᴛᴇʀᴍs.</b></blockquote>\n\n◈ ғᴏʀ ᴀɴʏ ʀᴇǫᴜᴇsᴛ ᴏʀ ʀᴇᴍᴏᴠᴀʟ, ʀᴇᴀᴄʜ ᴏᴜᴛ ᴀᴅᴍɪɴs ᴀᴛ: <a href=https://t.me/BeingHumanAssociation/3>ʙᴇɪɴɢ ʜᴜᴍᴀɴ.</a>\n</blockquote></b>"
#--------------------------------------------
#--------------------------------------------
START_MSG = os.environ.get("START_MESSAGE", "<b>ʜᴇʟʟᴏ {mention}\n\n<blockquote>ᴛɪʀᴇᴅ ᴏꜰ ᴀᴅs ᴀɴᴅ ᴊᴏɪɴɪɴɢ ᴍᴜʟᴛɪᴘʟᴇ ᴄʜᴀɴɴᴇʟs?</b></blockquote>\n<b><blockquote>ɴᴏᴛ ᴛᴏ ᴡᴏʀʀʏ ᴀɴʏᴍᴏʀᴇ.</b></blockquote>\n<b><blockquote>ɪ ᴡɪʟʟ ʙᴇ sʜᴀʀɪɴɢ ғʀᴇᴇ sʜᴀʀᴇᴀʙʟᴇ ᴍᴇᴅɪᴀ ᴏɴ ʀᴀɴᴅᴏᴍ ᴅᴀʏs 💌</b></blockquote>")
FORCE_MSG = os.environ.get("FORCE_SUB_MESSAGE", "ʜᴇʟʟᴏ {mention}\n\n<b><blockquote>ᴊᴏɪɴ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs ᴀɴᴅ ᴛʜᴇɴ ᴄʟɪᴄᴋ ᴏɴ ʀᴇʟᴏᴀᴅ button ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ʀᴇǫᴜᴇꜱᴛᴇᴅ ꜰɪʟᴇ.</b></blockquote>")

CMD_TXT = """<blockquote><b>» ᴀᴅᴍɪɴ ᴄᴏᴍᴍᴀɴᴅs:</b></blockquote>

<b>›› /dlt_time :</b> sᴇᴛ ᴀᴜᴛᴏ ᴅᴇʟᴇᴛᴇ ᴛɪᴍᴇ
<b>›› /check_dlt_time :</b> ᴄʜᴇᴄᴋ ᴄᴜʀʀᴇɴᴛ ᴅᴇʟᴇᴛᴇ ᴛɪᴍᴇ
<b>›› /protect :</b> ᴛᴏɢɢʟᴇ ᴄᴏɴᴛᴇɴᴛ ᴘʀᴏᴛᴇᴄᴛɪᴏɴ
<b>›› /check_protect :</b> ᴄʜᴇᴄᴋ ᴄᴏɴᴛᴇɴᴛ ᴘʀᴏᴛᴇᴄᴛɪᴏɴ sᴛᴀᴛᴜs
<blockquote><b>›› /caption :</b> sʜᴏᴡ ᴄᴀᴘᴛɪᴏɴ & ʟɪɴᴋ sᴇᴛᴛɪɴɢs sᴜᴍᴍᴀʀʏ
<b>›› /replace :</b> ʀᴇᴘʟᴀᴄᴇ ᴛᴇxᴛ ɪɴ ᴀʟʟ ᴄᴀᴘᴛɪᴏɴs
<b>›› /globalcap :</b> sᴇᴛ ᴏʀ ᴛᴏɢɢʟᴇ ɢʟᴏʙᴀʟ ᴄᴀᴘᴛɪᴏɴ
<b>›› /replace_link :</b> ʀᴇᴘʟᴀᴄᴇ sᴘᴇᴄɪꜰɪᴄ ʟɪɴᴋɪɴ ᴄᴀᴘᴛɪᴏɴs
<b>›› /replace_all_link :</b> ᴛᴏɢɡʟᴇ ʀᴇᴘʟᴀᴄᴇ ᴀʟʟ ʟɪɴᴋs ᴡɪᴛʜ ᴏɴᴇ
<b>›› /caption_add :</b> ᴀᴘᴘᴇɴᴅ ᴛᴇxᴛ ᴛᴏ ᴇxɪsᴛɪɴɢ ᴄᴀᴘᴛɪᴏɴs
<b>›› /caption_clean :</b> ʀᴇᴍᴏᴠᴇ ʟɪɴᴋs ꜰʀᴏᴍ ᴏʀɪɢɪɴᴀʟ ᴄᴀᴘᴛɪᴏɴ </blockquote>
<b>›› /dbroadcast :</b> ʙʀᴏᴀᴅᴄᴀsᴛ ᴅᴏᴄᴜᴍᴇɴᴛ / ᴠɪᴅᴇᴏ
<b>›› /ban :</b> ʙᴀɴ ᴀ ᴜꜱᴇʀ
<b>›› /unban :</b> ᴜɴʙᴀɴ ᴀ ᴜꜱᴇʀ
<b>›› /banlist :</b> ɢᴇᴛ ʟɪsᴛ ᴏꜰ ʙᴀɴɴᴇᴅ ᴜꜱᴇʀs
<b>›› /addchnl :</b> ᴀᴅᴅ ꜰᴏʀᴄᴇ sᴜʙ ᴄʜᴀɴɴᴇʟ
<b>›› /delchnl :</b> ʀᴇᴍᴏᴠᴇ ꜰᴏʀᴄᴇ sᴜʙ ᴄʜᴀɴɴᴇʟ
<b>›› /listchnl :</b> ᴠɪᴇᴡ ᴀᴅᴅᴇᴅ ᴄʜᴀɴɴᴇʟs
<b>›› /fsub_mode :</b> ᴛᴏɢɢʟᴇ ꜰᴏʀᴄᴇ sᴜʙ ᴍᴏᴅᴇ
<b>›› /pbroadcast :</b> sᴇɴᴅ ᴘʜᴏᴛᴏ ᴛᴏ ᴀʟʟ ᴜꜱᴇʀs
<b>›› /add_admin :</b> ᴀᴅᴅ ᴀɴ ᴀᴅᴍɪɴ
<b>›› /deladmin :</b> ʀᴇᴍᴏᴠᴇ ᴀɴ ᴀᴅᴍɪɴ
<b>›› /admins :</b> ɢᴇᴛ ʟɪsᴛ ᴏꜰ ᴀᴅᴍɪɴs
<b>›› /backup :</b> ɢᴇᴛ ʟᴀᴛᴇsᴛ ᴅᴀᴛᴀʙᴀsᴇ ʙᴀᴄᴋᴜᴘ
<b>›› /delreq :</b> Rᴇᴍᴏᴠᴇᴅ ʟᴇғᴛᴏᴠᴇʀ ɴᴏɴ-ʀᴇǫᴜᴇsᴛ ᴜsᴇʀs
"""
#--------------------------------------------
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "<b>• ʙʏ @HxHLinks</b>") #set your Custom Caption here, Keep None for Disable Custom Caption
PROTECT_CONTENT = True if os.environ.get('PROTECT_CONTENT', "True") == "True" else False #set True if you want to prevent users from forwarding files from bot
#--------------------------------------------
#Set true if you want Disable your Channel Posts Share button
DISABLE_CHANNEL_BUTTON = os.environ.get("DISABLE_CHANNEL_BUTTON",None) == 'False'
#--------------------------------------------
# Custom batch configuration
CUSTOM_BATCH_CONCURRENCY = int(os.environ.get("CUSTOM_BATCH_CONCURRENCY", "6"))
CUSTOM_BATCH_MAX_RETRIES = int(os.environ.get("CUSTOM_BATCH_MAX_RETRIES", "20"))
CUSTOM_BATCH_SEQUENTIAL_RETRIES = int(os.environ.get("CUSTOM_BATCH_SEQUENTIAL_RETRIES", "10"))
CUSTOM_BATCH_RECOPY_MODE = os.environ.get("CUSTOM_BATCH_RECOPY_MODE", "ask").lower()  # ask | allow | deny
#--------------------------------------------
BOT_STATS_TEXT = "<b>BOT UPTIME</b>\n{uptime}"
USER_REPLY_TEXT = "ʙᴀᴋᴋᴀ ! ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴍʏ ꜱᴇɴᴘᴀɪ!!"
#--------------------------------------------

LOG_FILE_NAME = "filesharingbot.txt"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(name)s - %(message)s",
    datefmt='%d-%b-%y %H:%M:%S',
    handlers=[
        RotatingFileHandler(
            LOG_FILE_NAME,
            maxBytes=50000000,
            backupCount=10
        ),
        logging.StreamHandler()
    ]
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

def LOGGER(name: str) -> logging.Logger:
    return logging.getLogger(name)

