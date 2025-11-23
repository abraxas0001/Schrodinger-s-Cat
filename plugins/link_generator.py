# (©)Codexbotz

import asyncio
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from bot import Bot
from config import DISABLE_CHANNEL_BUTTON, CUSTOM_BATCH_CONCURRENCY, CUSTOM_BATCH_MAX_RETRIES, CUSTOM_BATCH_SEQUENTIAL_RETRIES, CUSTOM_BATCH_RECOPY_MODE
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
    collected = []  # list of dicts: {seq, type: 'db'|'copy', msg_id?, message?}
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
            # If user sent a non-DB message, allow optional re-copy to DB channel according to config
            if not msg_id:
                # Ask the user if they want to copy this message to DB channel
                try:
                    ask_reply = await client.ask(text="This message is not from DB Channel. Do you want me to copy it to the DB Channel and include it? Reply YES to copy, NO to skip.", chat_id=message.chat.id, timeout=45)
                except Exception:
                    await user_msg.reply("❌ No response. Skipping this message.", quote=True)
                    continue
                if CUSTOM_BATCH_RECOPY_MODE == 'allow':
                    seq += 1
                    collected.append({"seq": seq, "type": "copy", "message": user_msg, "msg_id": None})
                elif CUSTOM_BATCH_RECOPY_MODE == 'deny':
                    await user_msg.reply("ℹ️ Message is not from DB channel; skipping as recopy is disabled.", quote=True)
                else:
                    if ask_reply and (ask_reply.text or '').strip().upper() in ("YES", "Y", "SURE", "COPY"):
                        seq += 1
                        # don't copy now; add for bulk copying later to preserve sequence and concurrency
                        collected.append({"seq": seq, "type": "copy", "message": user_msg, "msg_id": None})
                    else:
                        await user_msg.reply("ℹ️ Message skipped.", quote=True)
                continue
            seq += 1
            collected.append({"seq": seq, "type": "db", "msg_id": msg_id})
            # end: collecting user messages

        if cancelled:
            await message.reply("❌ Custom batch cancelled.", reply_markup=ReplyKeyboardRemove())
            return

        if not collected:
            await message.reply("❌ No messages were added to batch.", reply_markup=ReplyKeyboardRemove())
            return
        # Copy non-DB messages concurrently with limited concurrency and retries
        async def copy_worker(seq_num, usr_msg, sem, max_retries=CUSTOM_BATCH_MAX_RETRIES):
            attempts = 0
            backoff = 1
            while True:
                try:
                    async with sem:
                        sent = await usr_msg.copy(client.db_channel.id, disable_notification=True)
                        return seq_num, sent.id
                except FloodWait as e:
                    await asyncio.sleep(get_flood_wait_seconds(e))
                except Exception:
                    attempts += 1
                    if attempts >= max_retries:
                        return seq_num, None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 20)

        # Attach share buttons concurrently with limited concurrency and retries
        async def attach_worker(mid, sem, reply_markup, max_retries=CUSTOM_BATCH_SEQUENTIAL_RETRIES):
            attempts = 0
            backoff = 1
            while True:
                try:
                    async with sem:
                        await client.edit_message_reply_markup(client.db_channel.id, mid, reply_markup=reply_markup)
                        return True
                except FloodWait as e:
                    await asyncio.sleep(get_flood_wait_seconds(e))
                except Exception:
                    attempts += 1
                    if attempts >= max_retries:
                        return False
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 20)

        # We already collected DB message ids and copy candidates; results are the collected entries
        results = list(collected)
        total = len(results)
        failed = []
        # Build copy tasks and run them concurrently
        copy_tasks = []
        sem = asyncio.Semaphore(CUSTOM_BATCH_CONCURRENCY)
        for item in results:
            if item.get("type") == "copy":
                copy_tasks.append(copy_worker(item.get("seq"), item.get("message"), sem))
        copy_results = []
        if copy_tasks:
            copy_results = await asyncio.gather(*copy_tasks)
        copy_map = {s: mid for s, mid in copy_results if mid}
        failed_copies = [s for s, mid in copy_results if not mid]

        # Convert results to (seq, msg_id) and filter out failed entries
        final_results = []
        for item in results:
            if item.get("type") == "db":
                final_results.append((item.get("seq"), item.get("msg_id")))
            else:
                final_results.append((item.get("seq"), copy_map.get(item.get("seq"))))

        if not any(r[1] is not None for r in final_results):
            await message.reply("❌ Failed to store any messages to DB Channel.", reply_markup=ReplyKeyboardRemove())
            return
        final_results.sort(key=lambda x: x[0])
        copied_ids = [item[1] for item in final_results if item[1] is not None]
        start_id = copied_ids[0] * abs(client.db_channel.id)
        end_id = copied_ids[-1] * abs(client.db_channel.id)
        string = f"get-{start_id}-{end_id}"
        base64_string = await encode(string)
        link = f"https://t.me/{client.username}?start={base64_string}"

        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=f'https://telegram.me/share/url?url={link}')]])
        await message.reply(f"<b>✅ Here is your custom batch link:</b>\n\n{link}", reply_markup=reply_markup)

        # Attach share URL inline button to each message in DB channel (if enabled) using concurrency
        failed_attach = []
        attach_results = []
        if not DISABLE_CHANNEL_BUTTON:
            sem = asyncio.Semaphore(CUSTOM_BATCH_CONCURRENCY)
            tasks = [attach_worker(mid, sem, reply_markup) for mid in copied_ids]
            attach_results = await asyncio.gather(*tasks)
            for mid, ok in zip(copied_ids, attach_results):
                if not ok:
                    failed_attach.append(mid)
        ok_count = len(copied_ids) - len(failed_attach) if copied_ids else 0
        await message.reply(f"✅ Done. Registered {ok_count}/{total} items. Attach failures: {len(failed_attach)}. Copy failures: {len(failed_copies)}")
        if failed_attach:
            failed_str = ', '.join(str(s) for s in failed_attach[:30])
            await message.reply(f"⚠️ Failed to attach share button to {len(failed_attach)} items (message ids): {failed_str}")
        if failed_copies:
            failed_str = ', '.join(str(s) for s in failed_copies[:30])
            await message.reply(f"⚠️ Failed to copy {len(failed_copies)} items (sequence numbers): {failed_str}")
        await message.reply("✅ Done.", reply_markup=ReplyKeyboardRemove())
    finally:
        interactive_users.discard(uid)
