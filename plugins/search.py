import asyncio
import re
from math import ceil
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
from bot import Bot
from helper_func import encode, admin, is_subscribed
from database.database import db

# Store search results temporarily (user_id -> list of message_ids)
search_cache = {}

def format_size(size_bytes):
    """Format file size to MB/GB"""
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def truncate_text(text, max_length=50):
    """Truncate text to max_length"""
    if len(text) > max_length:
        return text[:max_length] + "..."
    return text

def calculate_relevance_score(query: str, text: str, is_filename: bool = False) -> int:
    """Calculate relevance score for search matching"""
    if not text:
        return 0
    
    text_lower = text.lower()
    query_lower = query.lower()
    score = 0
    
    # Exact match gets highest score
    if query_lower == text_lower:
        score += 1000
    
    # Starts with query gets high score
    if text_lower.startswith(query_lower):
        score += 500
    
    # Word boundary match (whole word)
    import re
    if re.search(r'\b' + re.escape(query_lower) + r'\b', text_lower):
        score += 300
    
    # Contains query
    if query_lower in text_lower:
        score += 100
    
    # Filename gets bonus
    if is_filename:
        score += 50
    
    # Multiple word matching (fuzzy)
    query_words = query_lower.split()
    if len(query_words) > 1:
        matched_words = sum(1 for word in query_words if word in text_lower)
        score += matched_words * 50
    
    return score

async def search_files(client: Client, query: str, limit=1000):
    """Search through all files in database channel by caption and filename with relevance ranking"""
    results = []
    search_query = query.lower()
    
    # Split query into words for multi-word search
    query_words = [w.strip() for w in search_query.split() if w.strip()]
    
    total_media_count = 0
    checked_files = []
    
    try:
        # Get all messages from database channel
        async for msg in client.get_chat_history(client.db_channel.id, limit=limit):
            if not msg.media:
                continue
            
            total_media_count += 1
            
            relevance_score = 0
            caption_text = msg.caption or ""
            file_name = ""
            
            # Get filename based on media type
            if msg.document and msg.document.file_name:
                file_name = msg.document.file_name
            elif msg.video and msg.video.file_name:
                file_name = msg.video.file_name
            elif msg.audio and msg.audio.file_name:
                file_name = msg.audio.file_name
            elif msg.audio and msg.audio.title:
                file_name = msg.audio.title
            
            # Debug: Store sample filenames
            if total_media_count <= 5:
                checked_files.append(file_name or f"No filename (caption: {caption_text[:30]})")
            
            # Calculate relevance for filename
            if file_name:
                relevance_score += calculate_relevance_score(search_query, file_name, is_filename=True)
            
            # Calculate relevance for caption
            if caption_text:
                relevance_score += calculate_relevance_score(search_query, caption_text, is_filename=False)
            
            # Multi-word fuzzy matching
            if len(query_words) > 1:
                combined_text = f"{file_name} {caption_text}".lower()
                all_words_present = all(word in combined_text for word in query_words)
                if all_words_present:
                    relevance_score += 200
            
            # If we have any relevance, add to results
            if relevance_score > 0:
                # Get file info
                file_size = 0
                display_name = "Unknown"
                
                if msg.document:
                    display_name = msg.document.file_name or "Document"
                    file_size = msg.document.file_size
                elif msg.video:
                    display_name = msg.video.file_name or f"Video {msg.video.duration}s"
                    file_size = msg.video.file_size
                elif msg.audio:
                    display_name = msg.audio.file_name or msg.audio.title or "Audio"
                    file_size = msg.audio.file_size
                elif msg.photo:
                    display_name = "Photo"
                    file_size = msg.photo.file_size
                
                results.append({
                    'msg_id': msg.id,
                    'file_name': display_name,
                    'file_size': file_size,
                    'caption': caption_text,
                    'relevance': relevance_score
                })
    
    except Exception as e:
        print(f"Search error: {e}")
    
    # Debug logging
    print(f"Search query: '{query}' | DB Channel ID: {client.db_channel.id} | Total media: {total_media_count} | Matches: {len(results)}")
    print(f"Sample files checked: {checked_files[:5]}")
    
    # Sort by relevance score (highest first)
    results.sort(key=lambda x: x['relevance'], reverse=True)
    
    return results

def create_search_keyboard(results, page=1, user_id=None, items_per_page=9):
    """Create paginated inline keyboard for search results"""
    total_pages = ceil(len(results) / items_per_page)
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    
    buttons = []
    
    # File buttons
    for item in results[start_idx:end_idx]:
        file_name = truncate_text(item['file_name'], 45)
        size_text = format_size(item['file_size'])
        button_text = f"➤ {size_text} || {file_name}"
        
        # Create callback data with message ID
        callback_data = f"file_{item['msg_id']}"
        buttons.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    # Navigation buttons
    nav_buttons = []
    
    # Previous page button
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"search_page_{page-1}"))
    
    # Page indicator
    nav_buttons.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="pages_info"))
    
    # Next page button
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"search_page_{page+1}"))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    return InlineKeyboardMarkup(buttons)

@Bot.on_message(filters.private & filters.text & ~filters.command(['start', 'help', 'about', 'batch', 'genlink', 'custom_batch', 'commands', 'caption', 'replace', 'globalcap', 'replace_link', 'replace_all_link', 'caption_add', 'caption_clean', 'protect', 'check_protect', 'dlt_time', 'check_dlt_time', 'broadcast', 'pbroadcast', 'dbroadcast', 'ban', 'unban', 'banlist', 'addchnl', 'delchnl', 'listchnl', 'fsub_mode', 'add_admin', 'deladmin', 'admins', 'backup', 'delreq']))
async def search_handler(client: Client, message: Message):
    """Handle text search queries with refined relevance ranking"""
    user_id = message.from_user.id
    
    # Check if user is banned
    banned_users = await db.get_ban_users()
    if user_id in banned_users:
        return
    
    # Check force subscription
    if not await is_subscribed(client, user_id):
        return
    
    query = message.text.strip()
    
    # Minimum search length
    if len(query) < 2:
        await message.reply("⚠️ Please enter at least 2 characters to search.")
        return
    
    # Maximum search length to prevent abuse
    if len(query) > 100:
        await message.reply("⚠️ Search query too long. Maximum 100 characters.")
        return
    
    # Search progress message
    progress_msg = await message.reply("🔍 Searching for files...")
    
    try:
        # Perform search with increased limit
        results = await search_files(client, query, limit=1000)
        
        if not results:
            await progress_msg.edit(
                f"❌ No files found matching: <code>{query}</code>\n\n"
                f"💡 <b>Search Tips:</b>\n"
                f"• Try different keywords\n"
                f"• Use fewer words\n"
                f"• Check spelling\n\n"
                f"<i>Searched through recent messages in database channel</i>"
            )
            return
        
        # Limit to top 100 most relevant results
        results = results[:100]
        
        # Store results in cache
        search_cache[user_id] = results
        
        # Create keyboard
        keyboard = create_search_keyboard(results, page=1, user_id=user_id)
        
        # Send results
        result_text = f"🔎 <b>Search Results for:</b> <code>{query}</code>\n\n"
        result_text += f"📊 Found <b>{len(results)}</b> file(s) (sorted by relevance)\n\n"
        result_text += "👇 Click on any file to download:"
        
        await progress_msg.edit(result_text, reply_markup=keyboard)
        
    except Exception as e:
        await progress_msg.edit(f"❌ Search failed: {e}")
        print(f"Search error: {e}")

@Bot.on_callback_query(filters.regex(r'^search_page_(\d+)$'))
async def search_page_callback(client: Client, callback_query: CallbackQuery):
    """Handle page navigation"""
    user_id = callback_query.from_user.id
    page = int(callback_query.data.split('_')[2])
    
    # Get cached results
    if user_id not in search_cache:
        await callback_query.answer("⚠️ Search expired. Please search again.", show_alert=True)
        return
    
    results = search_cache[user_id]
    
    # Create keyboard for the requested page
    keyboard = create_search_keyboard(results, page=page, user_id=user_id)
    
    # Update message
    try:
        await callback_query.message.edit_reply_markup(reply_markup=keyboard)
        await callback_query.answer(f"Page {page}")
    except Exception as e:
        await callback_query.answer("⚠️ Error changing page", show_alert=True)

@Bot.on_callback_query(filters.regex(r'^file_(\d+)$'))
async def file_callback(client: Client, callback_query: CallbackQuery):
    """Handle file delivery when user clicks on a search result"""
    user_id = callback_query.from_user.id
    msg_id = int(callback_query.data.split('_')[1])
    
    await callback_query.answer("📥 Sending file...")
    
    try:
        # Get the message from database channel
        msg = await client.get_messages(client.db_channel.id, msg_id)
        
        if not msg or not msg.media:
            await callback_query.answer("❌ File not found!", show_alert=True)
            return
        
        # Get caption settings
        protect_content = await db.get_protect_content()
        replace_old, replace_new = await db.get_caption_replace()
        global_cap_text, global_cap_enabled = await db.get_global_caption()
        link_old, link_new = await db.get_link_replace()
        all_link, all_link_enabled = await db.get_replace_all_link()
        caption_append = await db.get_caption_append()
        strip_links = await db.get_caption_strip()
        
        # Process caption
        original_caption = msg.caption or ""
        
        if strip_links and original_caption:
            ANCHOR_TAG_REGEX = re.compile(r'<a\b[^>]*>.*?</a>', re.IGNORECASE | re.DOTALL)
            BRACKETED_LINK_REGEX = re.compile(r'\(\s*https?://[^)]+\)', re.IGNORECASE)
            LINK_REGEX = re.compile(r'https?://[^\s]+')
            
            cleaned_caption = ANCHOR_TAG_REGEX.sub('', original_caption)
            cleaned_caption = BRACKETED_LINK_REGEX.sub('', cleaned_caption)
            cleaned_caption = LINK_REGEX.sub('', cleaned_caption)
            cleaned_caption = re.sub(r'\(\s*\)', '', cleaned_caption)
            cleaned_caption = re.sub(r' {2,}', ' ', cleaned_caption)
            cleaned_caption = re.sub(r'(\n\s*){2,}', '\n', cleaned_caption)
            cleaned_caption = re.sub(r'\s*\n\s*', '\n', cleaned_caption)
            original_caption = cleaned_caption.strip()
        
        caption = original_caption
        
        if not caption and global_cap_enabled and global_cap_text:
            caption = global_cap_text
        
        if caption:
            if replace_old:
                caption = caption.replace(replace_old, replace_new)
            if link_old:
                caption = caption.replace(link_old, link_new)
            if all_link_enabled and all_link:
                LINK_REGEX = re.compile(r'https?://[^\s]+')
                caption = LINK_REGEX.sub(all_link, caption)
            if caption_append:
                caption = f"{caption}\n{caption_append}"
        
        caption_to_send = caption or None
        
        # Custom button
        CUSTOM_BUTTON = InlineKeyboardMarkup(
            [[InlineKeyboardButton("𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️", url="https://t.me/HxHLinks")]]
        )
        
        # Send file
        copy_kwargs = {
            'chat_id': user_id,
            'reply_markup': CUSTOM_BUTTON,
            'protect_content': protect_content
        }
        
        if caption_to_send:
            copy_kwargs['caption'] = caption_to_send
            copy_kwargs['parse_mode'] = ParseMode.HTML
        
        await msg.copy(**copy_kwargs)
        
        # Auto-delete if enabled
        FILE_AUTO_DELETE = await db.get_del_timer()
        if FILE_AUTO_DELETE > 0:
            await callback_query.message.reply(
                f"<b>This file will be deleted in {FILE_AUTO_DELETE} seconds. Please save it.</b>"
            )
            
    except Exception as e:
        await callback_query.answer("❌ Failed to send file!", show_alert=True)
        print(f"File delivery error: {e}")

@Bot.on_callback_query(filters.regex(r'^pages_info$'))
async def pages_info_callback(client: Client, callback_query: CallbackQuery):
    """Handle page info button click"""
    await callback_query.answer("📄 Page indicator", show_alert=False)
