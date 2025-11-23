# (©)Codexbotz

import asyncio
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from bot import Bot
from config import DISABLE_CHANNEL_BUTTON, CUSTOM_BATCH_CONCURRENCY, CUSTOM_BATCH_MAX_RETRIES, CUSTOM_BATCH_SEQUENTIAL_RETRIES
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


@Bot.on_message(filters.private & admin & filters.command("custom_batch"))
async def custom_batch(client: Client, message: Message):
    collected = []
    seq = 0
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

            # Collect message ID from DB channel (forwarded message or channel link)
            msg_id = await get_message_id(client, user_msg)
            if not msg_id:
                await user_msg.reply("❌ Please forward a message from the DB Channel (or send the channel link)", quote=True)
                continue
            seq += 1
            collected.append((seq, msg_id))
            # end: collecting user messages

        if cancelled:
            await message.reply("❌ Custom batch cancelled.", reply_markup=ReplyKeyboardRemove())
            return

        if not collected:
            await message.reply("❌ No messages were added to batch.", reply_markup=ReplyKeyboardRemove())
            return
        # Copy all collected messages concurrently with limited concurrency and retry on FloodWait
        async def copy_worker(seq_num, usr_msg, sem, max_retries=CUSTOM_BATCH_MAX_RETRIES):
            # Strong retry strategy: flood waits are handled by sleeping required time
            # Non-Flood exceptions are retried with exponential backoff
            attempts = 0
            backoff = 1
            while True:
                try:
                    async with sem:
                        sent = await usr_msg.copy(client.db_channel.id, disable_notification=True)
                        # Attach per-message share button for this specific copy (if enabled)
                        if not DISABLE_CHANNEL_BUTTON:
                            try:
                                converted_id_single = sent.id * abs(client.db_channel.id)
                                base64_single = await encode(f"get-{converted_id_single}")
                                share_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=f'https://telegram.me/share/url?url=https://t.me/{client.username}?start={base64_single}')]])
                                await client.edit_message_reply_markup(client.db_channel.id, sent.id, reply_markup=share_markup)
                            except FloodWait as e:
                                await asyncio.sleep(get_flood_wait_seconds(e))
                            except Exception:
                                # ignore per-message edit failures
                                pass
                        return seq_num, sent.id
                except FloodWait as e:
                    # sleep required Telegram specified time (robust attribute access)
                    await asyncio.sleep(get_flood_wait_seconds(e))
                    # continue retrying until success
                except Exception as e:
                    attempts += 1
                    if attempts >= max_retries:
                        return seq_num, None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 20)

        # We already collected DB message ids; results are the collected IDs in original order
        results = list(collected)
        total = len(results)
        failed = []
        # Keep only successful copies and sort by sequence
        results = [r for r in results if r and r[1] is not None]
        if not results:
            await message.reply("❌ Failed to store any messages to DB Channel.", reply_markup=ReplyKeyboardRemove())
            return
        results.sort(key=lambda x: x[0])
        copied_ids = [item[1] for item in results]
        start_id = copied_ids[0] * abs(client.db_channel.id)
        end_id = copied_ids[-1] * abs(client.db_channel.id)
        string = f"get-{start_id}-{end_id}"
        base64_string = await encode(string)
        link = f"https://t.me/{client.username}?start={base64_string}"

        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=f'https://telegram.me/share/url?url={link}')]])
        await message.reply(f"<b>✅ Here is your custom batch link:</b>\n\n{link}", reply_markup=reply_markup)

        # Attach share URL inline button to each copied message in DB channel (if enabled)
        if not DISABLE_CHANNEL_BUTTON:
            for mid in copied_ids:
                attempts = 0
                while True:
                    try:
                        await client.edit_message_reply_markup(client.db_channel.id, mid, reply_markup=reply_markup)
                        break
                    except FloodWait as e:
                        await asyncio.sleep(get_flood_wait_seconds(e))
                        attempts += 1
                        if attempts > 3:
                            break
                    except Exception:
                        break
        await message.reply(f"✅ Done. Registered {len(results)}/{total} items. Failed: {len(failed)}")
        if failed:
            failed_str = ', '.join(str(s) for s in failed[:30])
            await message.reply(f"⚠️ Failed to copy {len(failed)} items (seq numbers): {failed_str}")
        await message.reply("✅ Done.", reply_markup=ReplyKeyboardRemove())
    finally:
        interactive_users.discard(uid)
