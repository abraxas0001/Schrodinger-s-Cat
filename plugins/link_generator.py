# (©)Codexbotz

import asyncio
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from bot import Bot
from config import DISABLE_CHANNEL_BUTTON
from helper_func import encode, get_message_id, admin, interactive_users, get_flood_wait_seconds


@Bot.on_message(filters.private & admin & filters.command('batch'))
async def batch(client: Client, message: Message):
    """Interactive range batch link creator with Stop / Cancel buttons."""
    uid = message.from_user.id
    interactive_users.add(uid)
    try:
        BATCH_KB = ReplyKeyboardMarkup([["STOP BATCH", "CANCEL BATCH"]], resize_keyboard=True)
        await message.reply(
            "Send FIRST message (or link) from DB channel.\nUse buttons to stop or cancel.",
            reply_markup=BATCH_KB
        )

        # Helper to detect control command
        def is_stop(txt: str | None):
            if not txt: return False
            t = txt.strip().upper()
            return t in ("STOP", "STOP BATCH")

        def is_cancel(txt: str | None):
            if not txt: return False
            t = txt.strip().upper()
            return t in ("CANCEL", "CANCEL BATCH")

        # Collect first message
        while True:
            try:
                first_message = await client.ask(
                    text="Waiting for FIRST message...\n(Forward or send link)",
                    chat_id=message.from_user.id,
                    filters=(filters.forwarded | (filters.text & ~filters.forwarded)),
                    timeout=120
                )
            except Exception:
                await message.reply("⏱️ Timed out.", reply_markup=ReplyKeyboardRemove())
                return

            if is_cancel(first_message.text):
                await first_message.reply("❌ Batch cancelled.", reply_markup=ReplyKeyboardRemove())
                return
            if is_stop(first_message.text):
                await first_message.reply("⚠️ Need both FIRST and LAST messages. Cancelled.", reply_markup=ReplyKeyboardRemove())
                return

            f_msg_id = await get_message_id(client, first_message)
            if f_msg_id:
                break
            else:
                await first_message.reply("❌ Error\nThis is not from DB Channel.")

        await message.reply("✅ First captured. Now send LAST message (or link).", reply_markup=BATCH_KB)

        # Collect second message
        while True:
            try:
                second_message = await client.ask(
                    text="Waiting for LAST message...\n(Forward or send link)",
                    chat_id=message.from_user.id,
                    filters=(filters.forwarded | (filters.text & ~filters.forwarded)),
                    timeout=120
                )
            except Exception:
                await message.reply("⏱️ Timed out.", reply_markup=ReplyKeyboardRemove())
                return

            if is_cancel(second_message.text):
                await second_message.reply("❌ Batch cancelled.", reply_markup=ReplyKeyboardRemove())
                return
            if is_stop(second_message.text):
                await second_message.reply("⚠️ Batch cancelled before completion.", reply_markup=ReplyKeyboardRemove())
                return

            s_msg_id = await get_message_id(client, second_message)
            if s_msg_id:
                break
            else:
                await second_message.reply("❌ Error\nThis is not from DB Channel.")

        string = f"get-{f_msg_id * abs(client.db_channel.id)}-{s_msg_id * abs(client.db_channel.id)}"
        base64_string = await encode(string)
        link = f"https://t.me/{client.username}?start={base64_string}"
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=f'https://telegram.me/share/url?url={link}')]])
        await second_message.reply_text(
            f"<b>✅ Here is your batch link</b>\n\n{link}",
            reply_markup=reply_markup
        )
        await message.reply("✅ Batch complete.", reply_markup=ReplyKeyboardRemove())
    finally:
        interactive_users.discard(uid)


@Bot.on_message(filters.private & admin & filters.command('genlink'))
async def link_generator(client: Client, message: Message):
    uid = message.from_user.id
    interactive_users.add(uid)
    try:
        while True:
            try:
                channel_message = await client.ask(
                    text="Forward Message from the DB Channel (with Quotes)..\nor Send the DB Channel Post link",
                    chat_id=message.from_user.id,
                    filters=(filters.forwarded | (filters.text & ~filters.forwarded)),
                    timeout=60
                )
            except:
                return
            msg_id = await get_message_id(client, channel_message)
            if msg_id:
                break
            else:
                await channel_message.reply("❌ Error\n\nThis message is not from my DB Channel", quote=True)

        base64_string = await encode(f"get-{msg_id * abs(client.db_channel.id)}")
        link = f"https://t.me/{client.username}?start={base64_string}"
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=f'https://telegram.me/share/url?url={link}')]])
        await channel_message.reply_text(f"<b>Here is your link</b>\n\n{link}", quote=True, reply_markup=reply_markup)
    finally:
        interactive_users.discard(uid)


@Bot.on_message(filters.private & filters.command("custom_batch"))
async def custom_batch(client: Client, message: Message):
    print("custom_batch called")
    collected = []
    uid = message.from_user.id
    interactive_users.add(uid)
    try:
        BATCH_KB = ReplyKeyboardMarkup([["STOP BATCH", "CANCEL BATCH"]], resize_keyboard=True)

        await message.reply(
            "Send all messages you want to include.\nPress STOP BATCH to finish or CANCEL BATCH to abort.",
            reply_markup=BATCH_KB
        )

        cancelled = False
        while True:
            try:
                user_msg = await client.ask(
                    chat_id=message.chat.id,
                    text="Waiting... (STOP BATCH to finish / CANCEL BATCH to abort)",
                    timeout=90
                )
            except asyncio.TimeoutError:
                # Timeout ends collection (if nothing collected, it's effectively cancel)
                break

            txt = (user_msg.text or '').strip().upper() if user_msg.text else ''
            if txt in ("STOP", "STOP BATCH"):
                break
            if txt in ("CANCEL", "CANCEL BATCH"):
                cancelled = True
                break

            try:
                while True:
                    try:
                        sent = await user_msg.copy(client.db_channel.id, disable_notification=True)
                        collected.append(sent.id)
                        await asyncio.sleep(0.4)  # small delay to prevent flood
                        break
                    except FloodWait as e:
                        wait_time = int(getattr(e, "value", 1))
                        await asyncio.sleep(wait_time)
                    except Exception as e:
                        await message.reply(f"❌ Failed to store a message:\n<code>{e}</code>")
                        break
            except Exception as e:
                await message.reply(f"❌ Error:\n<code>{e}</code>")

        if cancelled:
            await message.reply("❌ Custom batch cancelled.", reply_markup=ReplyKeyboardRemove())
            return

        await message.reply("✅ Batch collection complete.", reply_markup=ReplyKeyboardRemove())

        if not collected:
            await message.reply("❌ No messages were added to batch.")
            return

        start_id = collected[0] * abs(client.db_channel.id)
        end_id = collected[-1] * abs(client.db_channel.id)
        string = f"get-{start_id}-{end_id}"
        base64_string = await encode(string)
        link = f"https://t.me/{client.username}?start={base64_string}"

        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=f'https://telegram.me/share/url?url={link}')]])
        await message.reply(f"<b>Here is your custom batch link:</b>\n\n{link}", reply_markup=reply_markup)

        # Attach share URL inline button to each message in DB channel (if enabled)
        if not DISABLE_CHANNEL_BUTTON:
            for mid in collected:
                try:
                    await client.edit_message_reply_markup(client.db_channel.id, mid, reply_markup=reply_markup)
                except FloodWait as e:
                    await asyncio.sleep(get_flood_wait_seconds(e))
                except Exception:
                    pass

    finally:
        interactive_users.discard(uid)

@Bot.on_message(filters.private & filters.command("bulk_custom_batch"))
async def bulk_custom_batch(client: Client, message: Message):
    collected = []
    uid = message.from_user.id
    interactive_users.add(uid)
    try:
        BATCH_KB = ReplyKeyboardMarkup([["STOP BATCH", "CANCEL BATCH"]], resize_keyboard=True)

        await message.reply(
            "Send all media files you want to include in the bulk batch.\nPress STOP BATCH to finish or CANCEL BATCH to abort.\n\nAll media will be forwarded to the database channel.",
            reply_markup=BATCH_KB
        )

        cancelled = False
        while True:
            try:
                user_msg = await client.ask(
                    chat_id=message.chat.id,
                    text="Waiting for media... (STOP BATCH to finish / CANCEL BATCH to abort)",
                    timeout=90
                )
            except asyncio.TimeoutError:
                # Timeout ends collection
                break

            txt = (user_msg.text or '').strip().upper() if user_msg.text else ''
            if txt in ("STOP", "STOP BATCH"):
                break
            if txt in ("CANCEL", "CANCEL BATCH"):
                cancelled = True
                break

            # Check if it's media
            if not (user_msg.photo or user_msg.video or user_msg.document or user_msg.audio or user_msg.voice or user_msg.animation):
                await user_msg.reply("❌ Please send media files only (photos, videos, documents, etc.)", quote=True)
                continue

            try:
                while True:
                    try:
                        sent = await user_msg.copy(client.db_channel.id, disable_notification=True)
                        collected.append(sent.id)
                        await asyncio.sleep(0.4)  # small delay to prevent flood
                        break
                    except FloodWait as e:
                        wait_time = int(getattr(e, "value", 1))
                        await asyncio.sleep(wait_time)
                    except Exception as e:
                        await user_msg.reply(f"❌ Failed to forward media:\n<code>{e}</code>", quote=True)
                        break
            except Exception as e:
                await user_msg.reply(f"❌ Error processing media:\n<code>{e}</code>", quote=True)

        if cancelled:
            await message.reply("❌ Bulk custom batch cancelled.", reply_markup=ReplyKeyboardRemove())
            return

        await message.reply("✅ Bulk batch collection complete.", reply_markup=ReplyKeyboardRemove())

        if not collected:
            await message.reply("❌ No media was forwarded to the batch.")
            return

        # Sort collected IDs to ensure proper order
        collected.sort()

        start_id = collected[0] * abs(client.db_channel.id)
        end_id = collected[-1] * abs(client.db_channel.id)
        string = f"get-{start_id}-{end_id}"
        base64_string = await encode(string)
        link = f"https://t.me/{client.username}?start={base64_string}"

        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=f'https://telegram.me/share/url?url={link}')]])

        # Attach share URL inline button to each forwarded message in DB channel (if enabled)
        if not DISABLE_CHANNEL_BUTTON:
            for mid in collected:
                try:
                    await client.edit_message_reply_markup(client.db_channel.id, mid, reply_markup=reply_markup)
                except FloodWait as e:
                    await asyncio.sleep(get_flood_wait_seconds(e))
                except Exception:
                    pass

        await message.reply(
            f"<b>✅ Bulk Custom Batch Complete!</b>\n\n"
            f"📊 Media forwarded: {len(collected)} files\n"
            f"📁 Database Channel: @{client.db_channel.username or client.db_channel.title}\n\n"
            f"🔗 <b>Shareable Batch Link:</b>\n{link}",
            reply_markup=reply_markup
        )

    except Exception as e:
        await message.reply(f"❌ An error occurred: {e}")
    finally:
        interactive_users.discard(uid)
