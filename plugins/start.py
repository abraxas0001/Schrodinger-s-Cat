import asyncio
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta
from pyrogram import Client, filters, __version__
from pyrogram.enums import ParseMode, ChatAction
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ReplyKeyboardMarkup, ChatInviteLink, ChatPrivileges
from pyrogram.errors.exceptions.bad_request_400 import UserNotParticipant
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, UserNotParticipant
from bot import Bot
from config import *
from helper_func import *
from database.database import *
# Add this near the top of start.py, after imports
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

CUSTOM_BUTTON = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️", url="https://t.me/HxHLinks")]
    ]
)

LINK_REGEX = re.compile(r'https?://[^\s]+')
ANCHOR_TAG_REGEX = re.compile(r'<a\b[^>]*>.*?</a>', re.IGNORECASE | re.DOTALL)
BRACKETED_LINK_REGEX = re.compile(r'\(\s*https?://[^)]+\)', re.IGNORECASE)

BAN_SUPPORT = f"{BAN_SUPPORT}"

@Bot.on_message(filters.command('start') & filters.private)
async def start_command(client: Client, message: Message):
    user_id = message.from_user.id

    # Add user if not already present
    if not await db.present_user(user_id):
        try:
            await db.add_user(user_id)
        except:
            pass

    # Check if user is banned
    banned_users = await db.get_ban_users()
    if user_id in banned_users:
        return await message.reply_text(
            "<b>⛔️ You are Bᴀɴɴᴇᴅ from using this bot.</b>\n\n"
            "<i>Contact support if you think this is a mistake.</i>",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Contact Support", url=BAN_SUPPORT)]]
            )
        )
    # ✅ Check Force Subscription
    if not await is_subscribed(client, user_id):
        #await temp.delete()
        return await not_joined(client, message)

    # File auto-delete time in seconds (Set your desired time in seconds here)
    FILE_AUTO_DELETE = await db.get_del_timer()  # Example: 3600 seconds (1 hour)

    # Protect content setting (initialize from env if not set)
    if not await db.protect_content_data.find_one({}):
        initial_protect = True if os.environ.get('PROTECT_CONTENT', "True").lower() == "true" else False
        await db.set_protect_content(initial_protect)
    protect_content = await db.get_protect_content()
    replace_old, replace_new = await db.get_caption_replace()
    global_cap_text, global_cap_enabled = await db.get_global_caption()
    link_old, link_new = await db.get_link_replace()
    all_link, all_link_enabled = await db.get_replace_all_link()
    caption_append = await db.get_caption_append()
    strip_links = await db.get_caption_strip()

    # Handle normal message flow
    text = message.text
    if len(text) > 7:
        try:
            base64_string = text.split(" ", 1)[1]
        except IndexError:
            return

        string = await decode(base64_string)
        argument = string.split("-")

        ids = []
        if len(argument) == 3:
            try:
                start = int(int(argument[1]) / abs(client.db_channel.id))
                end = int(int(argument[2]) / abs(client.db_channel.id))
                ids = range(start, end + 1) if start <= end else list(range(start, end - 1, -1))
            except Exception as e:
                print(f"Error decoding IDs: {e}")
                return

        elif len(argument) == 2:
            try:
                ids = [int(int(argument[1]) / abs(client.db_channel.id))]
            except Exception as e:
                print(f"Error decoding ID: {e}")
                return

        temp_msg = await message.reply("<b>Please wait...</b>")
        try:
            messages = await get_messages(client, ids)
        except Exception as e:
            await message.reply_text("Something went wrong!")
            print(f"Error getting messages: {e}")
            return
        finally:
            await temp_msg.delete()
 
        neel_msgs = []
        for msg in messages:
            original_caption = msg.caption.html if msg.caption else ""

            if strip_links and original_caption:
                cleaned_caption = ANCHOR_TAG_REGEX.sub('', original_caption)
                cleaned_caption = BRACKETED_LINK_REGEX.sub('', cleaned_caption)
                cleaned_caption = LINK_REGEX.sub('', cleaned_caption)
                cleaned_caption = re.sub(r'\(\s*\)', '', cleaned_caption)
                cleaned_caption = re.sub(r' {2,}', ' ', cleaned_caption)
                cleaned_caption = re.sub(r'(\n\s*){2,}', '\n', cleaned_caption)
                cleaned_caption = re.sub(r'\s*\n\s*', '\n', cleaned_caption)
                original_caption = cleaned_caption.strip()

            if bool(CUSTOM_CAPTION) and bool(msg.document):
                base_caption = CUSTOM_CAPTION.format(
                    previouscaption=original_caption,
                    filename=msg.document.file_name
                )
            else:
                base_caption = original_caption

            caption = base_caption or ""
            if not caption and global_cap_enabled and global_cap_text:
                caption = global_cap_text

            if caption:
                if replace_old:
                    caption = caption.replace(replace_old, replace_new)
                if link_old:
                    caption = caption.replace(link_old, link_new)
                if all_link_enabled and all_link:
                    caption = LINK_REGEX.sub(all_link, caption)
                if caption_append:
                    caption = f"{caption}\n{caption_append}"

            caption_to_send = caption or None
            reply_markup = CUSTOM_BUTTON
            copy_kwargs = {
                'chat_id': message.from_user.id,
                'reply_markup': reply_markup,
                'protect_content': protect_content
            }
            if caption_to_send is not None:
                copy_kwargs['caption'] = caption_to_send
                copy_kwargs['parse_mode'] = ParseMode.HTML
            try:
                copied_msg = await msg.copy(**copy_kwargs)
                await asyncio.sleep(0.5)
                neel_msgs.append(copied_msg)
            except Exception as e:
                print(f"Failed to send message: {e}")

        if FILE_AUTO_DELETE > 0:
            notification_msg = await message.reply(
                f"<b>Tʜɪs Fɪʟᴇ ᴡɪʟʟ ʙᴇ Dᴇʟᴇᴛᴇᴅ ɪɴ  {get_exp_time(FILE_AUTO_DELETE)}. Pʟᴇᴀsᴇ sᴀᴠᴇ ᴏʀ ғᴏʀᴡᴀʀᴅ ɪᴛ ᴛᴏ ʏᴏᴜʀ sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs ʙᴇғᴏʀᴇ ɪᴛ ɢᴇᴛs Dᴇʟᴇᴛᴇᴅ.</b>"
            )
            reload_url = (
                f"https://t.me/{client.username}?start={message.command[1]}"
                if message.command and len(message.command) > 1
                else None
            )
            asyncio.create_task(
                schedule_auto_delete(client, neel_msgs, notification_msg, FILE_AUTO_DELETE, reload_url)
            )
    else:
        reply_markup = InlineKeyboardMarkup(
            [
                    [InlineKeyboardButton("• ʏᴏᴜ ᴀʀᴇ ᴡᴇʟᴄᴏᴍᴇ •", url="https://t.me/HxHLinks")],

    [
                    InlineKeyboardButton("• ᴅɪsᴄʟᴀɪᴍᴇʀ", callback_data = "about"),
                    InlineKeyboardButton('ᴄʀᴇᴀᴛᴏʀ •', url= "https://t.me/LordShekhar")

    ]
            ]
        )
        await message.reply_photo(
            photo=START_PIC,
            caption=START_MSG.format(
                first=message.from_user.first_name,
                last=message.from_user.last_name,
                username=None if not message.from_user.username else '@' + message.from_user.username,
                mention=message.from_user.mention,
                id=message.from_user.id
            ),
            reply_markup=reply_markup)
        
        return




# Create a global dictionary to store chat data
chat_data_cache = {}

async def not_joined(client: Client, message: Message):
    temp = await message.reply("<b><i>ᴡᴀɪᴛ ᴀ sᴇᴄ..</i></b>")

    user_id = message.from_user.id
    buttons = []
    count = 0

    try:
        all_channels = await db.show_channels()  # Should return list of (chat_id, mode) tuples
        for total, chat_id in enumerate(all_channels, start=1):
            mode = await db.get_channel_mode(chat_id)  # fetch mode 

            await message.reply_chat_action(ChatAction.TYPING)

            if not await is_sub(client, user_id, chat_id):
                try:
                    # Cache chat info
                    if chat_id in chat_data_cache:
                        data = chat_data_cache[chat_id]
                    else:
                        data = await client.get_chat(chat_id)
                        chat_data_cache[chat_id] = data

                    name = data.title

                    # Generate proper invite link based on the mode
                    if mode == "on" and not data.username:
                        invite = await client.create_chat_invite_link(
                            chat_id=chat_id,
                            creates_join_request=True,
                            expire_date=datetime.utcnow() + timedelta(seconds=FSUB_LINK_EXPIRY) if FSUB_LINK_EXPIRY else None
                            )
                        link = invite.invite_link

                    else:
                        if data.username:
                            link = f"https://t.me/{data.username}"
                        else:
                            invite = await client.create_chat_invite_link(
                                chat_id=chat_id,
                                expire_date=datetime.utcnow() + timedelta(seconds=FSUB_LINK_EXPIRY) if FSUB_LINK_EXPIRY else None)
                            link = invite.invite_link

                    buttons.append([InlineKeyboardButton(text=name, url=link)])
                    count += 1
                    await temp.edit(f"<b>{'! ' * count}</b>")

                except Exception as e:
                    print(f"Error with chat {chat_id}: {e}")
                    return await temp.edit(
                        f"<b><i>! Eʀʀᴏʀ, Cᴏɴᴛᴀᴄᴛ ᴅᴇᴠᴇʟᴏᴘᴇʀ ᴛᴏ sᴏʟᴠᴇ ᴛʜᴇ ɪssᴜᴇs @neel_leen</i></b>\n"
                        f"<blockquote expandable><b>Rᴇᴀsᴏɴ:</b> {e}</blockquote>"
                    )

        # Retry Button
        try:
            buttons.append([
                InlineKeyboardButton(
                    text='♻️ Tʀʏ Aɢᴀɪɴ',
                    url=f"https://t.me/{client.username}?start={message.command[1]}"
                )
            ])
        except IndexError:
            pass

        await message.reply_photo(
            photo=FORCE_PIC,
            caption=FORCE_MSG.format(
                first=message.from_user.first_name,
                last=message.from_user.last_name,
                username=None if not message.from_user.username else '@' + message.from_user.username,
                mention=message.from_user.mention,
                id=message.from_user.id
            ),
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    except Exception as e:
        print(f"Final Error: {e}")
        await temp.edit(
            f"<b><i>! Eʀʀᴏʀ, Cᴏɴᴛᴀᴄᴛ ᴅᴇᴠᴇʟᴏᴘᴇʀ ᴛᴏ sᴏʟᴠᴇ ᴛʜᴇ ɪssᴜᴇs @BeingHumanAssociation</i></b>\n"
            f"<blockquote expandable><b>Rᴇᴀsᴏɴ:</b> {e}</blockquote>"
        )

#=====================================================================================##

@Bot.on_message(filters.command('commands') & filters.private & admin)
async def bcmd(bot: Bot, message: Message):        
    reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("• ᴄʟᴏsᴇ •", callback_data = "close")]])
    await message.reply(text=CMD_TXT, reply_markup = reply_markup, quote= True)

async def schedule_auto_delete(client, neel_msgs, notification_msg, file_auto_delete, reload_url):
    await asyncio.sleep(file_auto_delete)
    for snt_msg in neel_msgs:
        if snt_msg:
            try:
                await snt_msg.delete()
            except Exception as e:
                print(f"Error deleting message {snt_msg.id}: {e}")

    try:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("ɢᴇᴛ ғɪʟᴇ ᴀɢᴀɪɴ!", url=reload_url)]]
        ) if reload_url else None

        await notification_msg.edit(
            "<b>ʏᴏᴜʀ ᴠɪᴅᴇᴏ / ꜰɪʟᴇ ɪꜱ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ᴅᴇʟᴇᴛᴇᴅ !!\n\nᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ʙᴜᴛᴛᴏɴ ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ᴅᴇʟᴇᴛᴇᴅ ᴠɪᴅᴇᴏ / ꜰɪʟᴇ 👇</b>",
            reply_markup=keyboard
        )
    except Exception as e:
        print(f"Error updating notification with 'Get File Again' button: {e}")
