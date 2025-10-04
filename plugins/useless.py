import asyncio
import html
import os
import random
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

#=====================================================================================##

@Bot.on_message(filters.command('stats') & admin)
async def stats(bot: Bot, message: Message):
    now = datetime.now()
    delta = now - bot.uptime
    time = get_readable_time(delta.seconds)
    await message.reply(BOT_STATS_TEXT.format(uptime=time))


#=====================================================================================##

WAIT_MSG = "<b>Working....</b>"

#=====================================================================================##


@Bot.on_message(filters.command('users') & filters.private & admin)
async def get_users(client: Bot, message: Message):
    msg = await client.send_message(chat_id=message.chat.id, text=WAIT_MSG)
    users = await db.full_userbase()
    await msg.edit(f"{len(users)} users are using this bot")


#=====================================================================================##

#AUTO-DELETE

@Bot.on_message(filters.private & filters.command('dlt_time') & admin)
async def set_delete_time(client: Bot, message: Message):
    try:
        duration = int(message.command[1])

        await db.set_del_timer(duration)

        await message.reply(f"<b>Dᴇʟᴇᴛᴇ Tɪᴍᴇʀ ʜᴀs ʙᴇᴇɴ sᴇᴛ ᴛᴏ <blockquote>{duration} sᴇᴄᴏɴᴅs.</blockquote></b>")

    except (IndexError, ValueError):
        await message.reply("<b>Pʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ᴅᴜʀᴀᴛɪᴏɴ ɪɴ sᴇᴄᴏɴᴅs.</b> Usage: /dlt_time {duration}")

@Bot.on_message(filters.private & filters.command('check_dlt_time') & admin)
async def check_delete_time(client: Bot, message: Message):
    duration = await db.get_del_timer()

    await message.reply(f"<b><blockquote>Cᴜʀʀᴇɴᴛ ᴅᴇʟᴇᴛᴇ ᴛɪᴍᴇʀ ɪs sᴇᴛ ᴛᴏ {duration}sᴇᴄᴏɴᴅs.</blockquote></b>")

#=====================================================================================##

# PROTECT CONTENT TOGGLE

@Bot.on_message(filters.private & filters.command('protect') & admin)
async def toggle_protect_content(client: Bot, message: Message):
    current = await db.get_protect_content()
    new_value = not current
    await db.set_protect_content(new_value)
    status = "ENABLED" if new_value else "DISABLED"
    await message.reply(f"<b>Content protection has been {status}.</b>")

@Bot.on_message(filters.private & filters.command('check_protect') & admin)
async def check_protect_content(client: Bot, message: Message):
    current = await db.get_protect_content()
    status = "ENABLED" if current else "DISABLED"
    await message.reply(f"<b><blockquote>Content protection is currently {status}.</blockquote></b>")


#=====================================================================================##
# CAPTION & LINK CONTROLS

@Bot.on_message(filters.private & filters.command('replace') & admin)
async def set_caption_replace(client: Bot, message: Message):
    if len(message.command) < 2:
        return await message.reply("<b>Usage:</b> <code>/replace old_text new_text</code> or <code>/replace off</code>")

    if message.command[1].lower() in {"off", "none"}:
        await db.set_caption_replace('', '')
        return await message.reply("<b>Caption replacement disabled.</b>")

    if len(message.command) < 3:
        return await message.reply("<b>Usage:</b> <code>/replace old_text new_text</code>")

    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        return await message.reply("<b>Usage:</b> <code>/replace old_text new_text</code>")

    old_text = parts[1]
    new_text = parts[2]
    await db.set_caption_replace(old_text, new_text)
    await message.reply(f"<b>Captions will replace:</b> <code>{old_text}</code> → <code>{new_text}</code>")


@Bot.on_message(filters.private & filters.command('globalcap') & admin)
async def global_caption_cmd(client: Bot, message: Message):
    args = message.text.split(maxsplit=1)
    current_text, enabled = await db.get_global_caption()

    if len(args) == 1:
        status = "ON" if enabled else "OFF"
        display = current_text if current_text else "<i>Not set</i>"
        return await message.reply(f"<b>Global caption:</b> {display}\n<b>Status:</b> {status}\n<b>Usage:</b> <code>/globalcap welcome!</code> | <code>/globalcap on</code> | <code>/globalcap off</code>")

    arg = args[1].strip()
    lowered = arg.lower()
    if lowered == 'on':
        if not current_text:
            return await message.reply("<b>Set a caption first using:</b> <code>/globalcap Your caption</code>")
        await db.set_global_caption(enabled=True)
        return await message.reply("<b>Global caption enabled.</b>")
    if lowered == 'off':
        await db.set_global_caption(enabled=False)
        return await message.reply("<b>Global caption disabled.</b>")

    await db.set_global_caption(text=arg, enabled=True)
    await message.reply(f"<b>Global caption set to:</b> {arg}")


@Bot.on_message(filters.private & filters.command('replace_link') & admin)
async def replace_link_cmd(client: Bot, message: Message):
    if len(message.command) < 2:
        return await message.reply("<b>Usage:</b> <code>/replace_link old_link new_link</code> or <code>/replace_link off</code>")

    if message.command[1].lower() in {"off", "none"}:
        await db.set_link_replace('', '')
        return await message.reply("<b>Link replacement disabled.</b>")

    if len(message.command) < 3:
        return await message.reply("<b>Usage:</b> <code>/replace_link old_link new_link</code>")

    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        return await message.reply("<b>Usage:</b> <code>/replace_link old_link new_link</code>")

    old_link = parts[1]
    new_link = parts[2]
    await db.set_link_replace(old_link, new_link)
    await message.reply(f"<b>Links will replace:</b> <code>{old_link}</code> → <code>{new_link}</code>")


@Bot.on_message(filters.private & filters.command('replace_all_link') & admin)
async def replace_all_link_cmd(client: Bot, message: Message):
    args = message.text.split(maxsplit=1)
    stored_link, enabled = await db.get_replace_all_link()

    if len(args) == 1:
        status = "ON" if enabled else "OFF"
        display = stored_link if stored_link else "<i>Not set</i>"
        return await message.reply(f"<b>Replace-all link:</b> {display}\n<b>Status:</b> {status}\n<b>Usage:</b> <code>/replace_all_link https://example.com</code> | <code>/replace_all_link on</code> | <code>/replace_all_link off</code>")

    arg = args[1].strip()
    lowered = arg.lower()
    if lowered == 'on':
        if not stored_link:
            return await message.reply("<b>Set a link first using:</b> <code>/replace_all_link https://example.com</code>")
        await db.set_replace_all_link(enabled=True)
        return await message.reply("<b>Replace-all links enabled.</b>")
    if lowered == 'off':
        await db.set_replace_all_link(enabled=False)
        return await message.reply("<b>Replace-all links disabled.</b>")

    await db.set_replace_all_link(link=arg, enabled=True)
    await message.reply(f"<b>All links will be replaced with:</b> {arg}")


@Bot.on_message(filters.private & filters.command('caption_add') & admin)
async def caption_add_cmd(client: Bot, message: Message):
    args = message.text.split(maxsplit=1)
    current = await db.get_caption_append()

    if len(args) == 1:
        display = current if current else "<i>None</i>"
        return await message.reply(
            f"<b>Current appended caption:</b> {display}\n"
            "<b>Usage:</b> <code>/caption_add extra text</code> | <code>/caption_add off</code>"
        )

    value = args[1].strip()
    if value.lower() in {"off", "none"}:
        await db.set_caption_append(None)
        return await message.reply("<b>Extra caption cleared.</b>")

    await db.set_caption_append(value)
    await message.reply(f"<b>Extra caption will append:</b> {value}")


@Bot.on_message(filters.private & filters.command('caption_clean') & admin)
async def caption_clean_cmd(client: Bot, message: Message):
    args = message.text.split(maxsplit=1)
    current = await db.get_caption_strip()

    if len(args) == 1:
        status = 'ON ✅' if current else 'OFF ❌'
        return await message.reply(
            f"<b>Strip original links:</b> {status}\n"
            "<b>Usage:</b> <code>/caption_clean on</code> | <code>/caption_clean off</code>"
        )

    arg = args[1].strip().lower()
    if arg in {'on', 'enable'}:
        await db.set_caption_strip(True)
        return await message.reply("<b>Original caption links will be removed.</b>")
    if arg in {'off', 'disable'}:
        await db.set_caption_strip(False)
        return await message.reply("<b>Original caption links will remain untouched.</b>")

    await message.reply("<b>Usage:</b> <code>/caption_clean on</code> | <code>/caption_clean off</code>")


@Bot.on_message(filters.private & filters.command('caption') & admin)
async def caption_overview_cmd(client: Bot, message: Message):
    replace_old, replace_new = await db.get_caption_replace()
    global_text, global_enabled = await db.get_global_caption()
    link_old, link_new = await db.get_link_replace()
    all_link, all_link_enabled = await db.get_replace_all_link()
    caption_append = await db.get_caption_append()
    strip_links = await db.get_caption_strip()

    lines = ["<b>📋 Caption Controls Overview</b>", "<blockquote>"]

    if replace_old:
        lines.append(
            f"<b>• Caption replace:</b> <code>{html.escape(replace_old)}</code> → <code>{html.escape(replace_new)}</code>"
        )
    else:
        lines.append("<b>• Caption replace:</b> <i>Disabled</i>")

    if global_text:
        lines.append(
            f"<b>• Global caption:</b> {'ON ✅' if global_enabled else 'OFF ❌'} — <code>{html.escape(global_text)}</code>"
        )
    else:
        status = 'ON ✅' if global_enabled else 'OFF ❌'
        lines.append(f"<b>• Global caption:</b> {status} — <i>Not set</i>")

    if link_old:
        lines.append(
            f"<b>• Link replace:</b> <code>{html.escape(link_old)}</code> → <code>{html.escape(link_new)}</code>"
        )
    else:
        lines.append("<b>• Link replace:</b> <i>Disabled</i>")

    if all_link:
        status = 'ON ✅' if all_link_enabled else 'OFF ❌'
        lines.append(f"<b>• Replace-all link:</b> {status} — <code>{html.escape(all_link)}</code>")
    else:
        lines.append("<b>• Replace-all link:</b> <i>Not set</i>")

    if caption_append:
        lines.append(f"<b>• Caption append:</b> <code>{html.escape(caption_append)}</code>")
    else:
        lines.append("<b>• Caption append:</b> <i>None</i>")

    lines.append(f"<b>• Strip original links:</b> {'ON ✅' if strip_links else 'OFF ❌'}")

    lines.append("<b>• Apply order:</b> replace text → replace links → replace-all → append → fallback global")
    lines.append("</blockquote>")
    lines.append(
        "<b>Commands:</b> <code>/replace</code>, <code>/globalcap</code>, <code>/replace_link</code>, "
        "<code>/replace_all_link</code>, <code>/caption_add</code>, <code>/caption_clean</code>"
    )

    await message.reply("\n".join(lines), disable_web_page_preview=True)

