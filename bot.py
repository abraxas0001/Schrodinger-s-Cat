from aiohttp import web
from plugins import web_server
import asyncio
import pyromod.listen
from pyrogram import Client
from pyrogram.enums import ParseMode
import sys
from datetime import datetime
#neel_leen on Tg
from config import *
import subprocess
import zipfile
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from database.database import db  # <-- added import


name ="""
 BY BEING HUMAN ASSOCIATION
"""


class Bot(Client):
    def __init__(self):
        super().__init__(
            name="Bot",
            api_hash=API_HASH,
            api_id=APP_ID,
            plugins={
                "root": "plugins"
            },
            workers=TG_BOT_WORKERS,
            bot_token=TG_BOT_TOKEN
        )
        self.LOGGER = LOGGER
        self.scheduler = AsyncIOScheduler()

    async def start(self):
        await super().start()
        usr_bot_me = await self.get_me()
        self.uptime = datetime.now()

        try:
            db_channel = await self.get_chat(CHANNEL_ID)
            self.db_channel = db_channel
            test = await self.send_message(chat_id = db_channel.id, text = "Test Message")
            await test.delete()
        except Exception as e:
            self.LOGGER(__name__).warning(e)
            self.LOGGER(__name__).warning(f"Make Sure bot is Admin in DB Channel, and Double check the CHANNEL_ID Value, Current Value {CHANNEL_ID}")
            self.LOGGER(__name__).info("\nBot Stopped. Join https://t.me/neel_leen for support")
            sys.exit()

        self.set_parse_mode(ParseMode.HTML)
        self.LOGGER(__name__).info(f"Bot Running..!\n\nCreated by \nhttps://t.me/neel_leen")
        self.LOGGER(__name__).info(f"""BOT DEPLOYED BY @neel_leen""")

        self.set_parse_mode(ParseMode.HTML)
        self.username = usr_bot_me.username
        self.LOGGER(__name__).info(f"Bot Running..! Made by @neel_leen")   

        # Start Web Server
        app = web.AppRunner(await web_server())
        await app.setup()
        await web.TCPSite(app, "0.0.0.0", PORT).start()

        # Start Daily Backup Scheduler
        self.scheduler.add_job(self.daily_backup, CronTrigger(hour=0, minute=0))  # Daily at midnight
        self.scheduler.start()

        try: await self.send_message(OWNER_ID, text = f"<b><blockquote> Bᴏᴛ Rᴇsᴛᴀʀᴛᴇᴅ by @BeingHumanAssociation</blockquote></b>")
        except: pass

    async def stop(self, *args):
        await super().stop()
        self.LOGGER(__name__).info("Bot stopped.")

    def run(self):
        """Run the bot."""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.start())
        self.LOGGER(__name__).info("Bot is now running. Thanks to @BeingHumanAssociation")
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            self.LOGGER(__name__).info("Shutting down...")
        finally:
            loop.run_until_complete(self.stop())

    async def daily_backup(self, chat_id=None, mode: str = "json"):
        """Create a zipped backup of MongoDB collections and send it.
        mode = 'json' (default) exports newline-delimited extended JSON.
        mode = 'bson' attempts to use 'mongodump' (if available). Falls back to json on failure.
        """
        import tempfile, json, shutil
        from bson.json_util import dumps as bson_dumps

        if chat_id is None:
            chat_id = OWNER_ID

        # Normalize mode
        mode = (mode or "json").lower()
        if mode not in ("json", "bson"):  # safety
            mode = "json"

        try:
            work_dir = tempfile.mkdtemp(prefix="tgdb_backup_")
            zip_path = os.path.join(work_dir, "mongodb_backup.zip")

            if mode == "bson":
                try:
                    dump_out = os.path.join(work_dir, "bson_dump")
                    dump_cmd = [
                        "mongodump",
                        "--uri", DB_URI,
                        "--out", dump_out
                    ]
                    subprocess.run(dump_cmd, check=True)
                    # Zip bson dump
                    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                        for root, _, files in os.walk(dump_out):
                            for f in files:
                                full = os.path.join(root, f)
                                arc = os.path.relpath(full, dump_out)
                                zf.write(full, arc)
                    caption = "📦 MongoDB Backup (BSON)"
                except (FileNotFoundError, subprocess.CalledProcessError) as e:
                    # Fallback to JSON export
                    self.LOGGER(__name__).warning(f"BSON backup failed ({e}); falling back to JSON export.")
                    mode = "json"  # fallback
                except Exception as e:
                    self.LOGGER(__name__).warning(f"Unexpected BSON backup error ({e}); falling back to JSON.")
                    mode = "json"

            if mode == "json":
                collections = {
                    "channels.jsonl": db.channel_data,
                    "admins.jsonl": db.admins_data,
                    "users.jsonl": db.user_data,
                    "banned_user.jsonl": db.banned_user_data,
                    "autho_user.jsonl": db.autho_user_data,
                    "del_timer.jsonl": db.del_timer_data,
                    "fsub.jsonl": db.fsub_data,
                    "request_forcesub.jsonl": db.rqst_fsub_data,
                    "request_forcesub_channel.jsonl": db.rqst_fsub_Channel_data,
                }
                for filename, collection in collections.items():
                    file_path = os.path.join(work_dir, filename)
                    try:
                        async for doc in collection.find({}):
                            with open(file_path, 'a', encoding='utf-8') as f:
                                f.write(bson_dumps(doc))
                                f.write('\n')
                    except Exception as e:
                        with open(file_path, 'a', encoding='utf-8') as f:
                            f.write(json.dumps({"_error": str(e)}))
                            f.write('\n')
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for root, _, files in os.walk(work_dir):
                        for f in files:
                            if f.endswith('.jsonl'):
                                full = os.path.join(root, f)
                                zf.write(full, f)
                caption = "📦 MongoDB Backup (JSON export)"

            await self.send_document(
                chat_id=chat_id,
                document=zip_path,
                caption=caption
            )
            self.LOGGER(__name__).info("Backup sent successfully.")
        except Exception as e:
            self.LOGGER(__name__).error(f"Backup failed: {e}")
            try:
                await self.send_message(chat_id, f"❌ Backup failed: {e}")
            except:
                pass
        finally:
            try:
                shutil.rmtree(work_dir, ignore_errors=True)
            except Exception as ce:
                self.LOGGER(__name__).warning(f"Cleanup warning: {ce}")


