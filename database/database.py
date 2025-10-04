#BEING HUMAN ASSOCIATION
#NEEL_LEEN on Tg

import motor.motor_asyncio
import pymongo
import certifi
import os
import time
import logging
import sys, io
from config import DB_URI, DB_NAME
from pymongo.errors import ServerSelectionTimeoutError

logging.basicConfig(level=logging.INFO)

# =========
# Sync client (PyMongo) with retry
# =========
MAX_RETRIES = 5
RETRY_DELAY = 3  # seconds

dbclient = None
for attempt in range(1, MAX_RETRIES + 1):
    try:
        logging.info(f"[DB] Connecting to MongoDB (sync)... Attempt {attempt}/{MAX_RETRIES}")
        dbclient = pymongo.MongoClient(
            DB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            tls=True,
            tlsCAFile=certifi.where()  # use up-to-date CA certs
        )
        dbclient.admin.command("ping")  # force check
        logging.info("[DB] ✅ Sync connection OK")
        break
    except ServerSelectionTimeoutError as e:
        logging.error(f"[DB] ❌ Sync connection failed: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
        else:
            logging.critical("[DB] MongoDB is unreachable. Check URI/network.")
            raise SystemExit(1)

database = dbclient[DB_NAME]


class Neel:
    def __init__(self, DB_URI, DB_NAME):
        # =========
        # Async client (Motor) with TLS CA
        # =========
        self.dbclient = motor.motor_asyncio.AsyncIOMotorClient(
            DB_URI,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        self.database = self.dbclient[DB_NAME]

        # Collections
        self.channel_data = self.database['channels']
        self.admins_data = self.database['admins']
        self.user_data = self.database['users']
        self.banned_user_data = self.database['banned_user']
        self.autho_user_data = self.database['autho_user']
        self.del_timer_data = self.database['del_timer']
        self.fsub_data = self.database['fsub']
        self.rqst_fsub_data = self.database['request_forcesub']
        self.rqst_fsub_Channel_data = self.database['request_forcesub_channel']
        self.protect_content_data = self.database['protect_content']
        self.caption_replace_data = self.database['caption_replace']
        self.global_caption_data = self.database['global_caption']
        self.link_replace_data = self.database['link_replace']
        self.replace_all_link_data = self.database['replace_all_link']
        self.caption_append_data = self.database['caption_append']
        self.caption_strip_data = self.database['caption_strip']

    # USER DATA
    async def present_user(self, user_id: int):
        return bool(await self.user_data.find_one({'_id': user_id}))

    async def add_user(self, user_id: int):
        await self.user_data.insert_one({'_id': user_id})

    async def full_userbase(self):
        docs = await self.user_data.find().to_list(length=None)
        return [doc['_id'] for doc in docs]

    async def del_user(self, user_id: int):
        await self.user_data.delete_one({'_id': user_id})

    # ADMIN DATA
    async def admin_exist(self, admin_id: int):
        return bool(await self.admins_data.find_one({'_id': admin_id}))

    async def add_admin(self, admin_id: int):
        if not await self.admin_exist(admin_id):
            await self.admins_data.insert_one({'_id': admin_id})

    async def del_admin(self, admin_id: int):
        if await self.admin_exist(admin_id):
            await self.admins_data.delete_one({'_id': admin_id})

    async def get_all_admins(self):
        docs = await self.admins_data.find().to_list(length=None)
        return [doc['_id'] for doc in docs]

    # BAN USER DATA
    async def ban_user_exist(self, user_id: int):
        return bool(await self.banned_user_data.find_one({'_id': user_id}))

    async def add_ban_user(self, user_id: int):
        if not await self.ban_user_exist(user_id):
            await self.banned_user_data.insert_one({'_id': user_id})

    async def del_ban_user(self, user_id: int):
        if await self.ban_user_exist(user_id):
            await self.banned_user_data.delete_one({'_id': user_id})

    async def get_ban_users(self):
        docs = await self.banned_user_data.find().to_list(length=None)
        return [doc['_id'] for doc in docs]

    # AUTO DELETE TIMER SETTINGS
    async def set_del_timer(self, value: int):
        if await self.del_timer_data.find_one({}):
            await self.del_timer_data.update_one({}, {'$set': {'value': value}})
        else:
            await self.del_timer_data.insert_one({'value': value})

    async def get_del_timer(self):
        data = await self.del_timer_data.find_one({})
        return data.get('value', 600) if data else 0

    # PROTECT CONTENT SETTINGS
    async def set_protect_content(self, value: bool):
        if await self.protect_content_data.find_one({}):
            await self.protect_content_data.update_one({}, {'$set': {'value': value}})
        else:
            await self.protect_content_data.insert_one({'value': value})

    async def get_protect_content(self):
        data = await self.protect_content_data.find_one({})
        return data.get('value', True) if data else True

    # CAPTION REPLACE SETTINGS
    async def set_caption_replace(self, old_text: str, new_text: str):
        if old_text:
            await self.caption_replace_data.replace_one(
                {},
                {'old': old_text, 'new': new_text},
                upsert=True
            )
        else:
            await self.caption_replace_data.delete_one({})

    async def get_caption_replace(self):
        data = await self.caption_replace_data.find_one({})
        if not data:
            return '', ''
        return data.get('old', ''), data.get('new', '')

    # GLOBAL CAPTION SETTINGS
    async def set_global_caption(self, text: str | None = None, enabled: bool | None = None):
        update = {}
        if text is not None:
            update['text'] = text
        if enabled is not None:
            update['enabled'] = enabled
        if update:
            await self.global_caption_data.update_one({}, {'$set': update}, upsert=True)

    async def get_global_caption(self):
        data = await self.global_caption_data.find_one({})
        if not data:
            return '', False
        return data.get('text', ''), data.get('enabled', False)

    # LINK REPLACE SETTINGS
    async def set_link_replace(self, old_link: str, new_link: str):
        if old_link:
            await self.link_replace_data.replace_one(
                {},
                {'old': old_link, 'new': new_link},
                upsert=True
            )
        else:
            await self.link_replace_data.delete_one({})

    async def get_link_replace(self):
        data = await self.link_replace_data.find_one({})
        if not data:
            return '', ''
        return data.get('old', ''), data.get('new', '')

    async def set_replace_all_link(self, link: str | None = None, enabled: bool | None = None):
        update = {}
        if link is not None:
            update['link'] = link
        if enabled is not None:
            update['enabled'] = enabled
        if update:
            await self.replace_all_link_data.update_one({}, {'$set': update}, upsert=True)

    async def get_replace_all_link(self):
        data = await self.replace_all_link_data.find_one({})
        if not data:
            return '', False
        return data.get('link', ''), data.get('enabled', False)

    async def set_caption_append(self, text: str | None = None):
        if text:
            await self.caption_append_data.update_one({}, {'$set': {'text': text}}, upsert=True)
        else:
            await self.caption_append_data.delete_one({})

    async def get_caption_append(self):
        data = await self.caption_append_data.find_one({})
        return data.get('text', '') if data else ''

    async def set_caption_strip(self, enabled: bool):
        await self.caption_strip_data.update_one({}, {'$set': {'enabled': enabled}}, upsert=True)

    async def get_caption_strip(self):
        data = await self.caption_strip_data.find_one({})
        return data.get('enabled', False) if data else False

    # CHANNEL MANAGEMENT
    async def channel_exist(self, channel_id: int):
        return bool(await self.fsub_data.find_one({'_id': channel_id}))

    async def add_channel(self, channel_id: int):
        if not await self.channel_exist(channel_id):
            await self.fsub_data.insert_one({'_id': channel_id})

    async def rem_channel(self, channel_id: int):
        if await self.channel_exist(channel_id):
            await self.fsub_data.delete_one({'_id': channel_id})

    async def show_channels(self):
        docs = await self.fsub_data.find().to_list(length=None)
        return [doc['_id'] for doc in docs]

    async def get_channel_mode(self, channel_id: int):
        data = await self.fsub_data.find_one({'_id': channel_id})
        return data.get("mode", "off") if data else "off"

    async def set_channel_mode(self, channel_id: int, mode: str):
        await self.fsub_data.update_one(
            {'_id': channel_id},
            {'$set': {'mode': mode}},
            upsert=True
        )

    # REQUEST FORCE-SUB MANAGEMENT
    async def req_user(self, channel_id: int, user_id: int):
        try:
            await self.rqst_fsub_Channel_data.update_one(
                {'_id': int(channel_id)},
                {'$addToSet': {'user_ids': int(user_id)}},
                upsert=True
            )
        except Exception as e:
            logging.error(f"[DB ERROR] Failed to add user to request list: {e}")

    async def del_req_user(self, channel_id: int, user_id: int):
        await self.rqst_fsub_Channel_data.update_one(
            {'_id': channel_id},
            {'$pull': {'user_ids': user_id}}
        )

    async def req_user_exist(self, channel_id: int, user_id: int):
        try:
            found = await self.rqst_fsub_Channel_data.find_one({
                '_id': int(channel_id),
                'user_ids': int(user_id)
            })
            return bool(found)
        except Exception as e:
            logging.error(f"[DB ERROR] Failed to check request list: {e}")
            return False

    async def reqChannel_exist(self, channel_id: int):
        return channel_id in await self.show_channels()


db = Neel(DB_URI, DB_NAME)
