import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required!")

ADMIN_ID = os.getenv('ADMIN_ID')
if not ADMIN_ID:
    raise ValueError("ADMIN_ID environment variable is required!")
ADMIN_ID = int(ADMIN_ID)
import json
from copy import deepcopy
import asyncio
import random
import time
import logging
import re
from datetime import datetime, timedelta, time as datetime_time
from collections import defaultdict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.constants import ParseMode
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, ContextTypes, filters)
from telegram.error import TimedOut, NetworkError, RetryAfter, BadRequest, Conflict

from backup_manager import DatabaseBackupManager
import zipfile

# Initialize logger
logger = logging.getLogger(__name__)

# Auto-deletion tracking
AUTO_DELETE_HOURS = 24  # Delete messages after 24 hour
AUTO_DELETE_ENABLED = True  # Global toggle for auto-deletion
AUTO_DELETE_NOTIFICATIONS = False  # Send notifications when media is auto-deleted - DISABLED GLOBALLY
NOTIFICATION_COOLDOWN_HOURS = 1  # Minimum hours between notifications per user
sent_messages_tracker = {}  # {message_id: {"chat_id": int, "timestamp": datetime, "message_id": int}}
TRACKING_FILE = "auto_delete_tracking.json"  # Persistent storage for tracking data
USER_PREFERENCES_FILE = "user_preferences.json"  # User notification preferences
HISTORICAL_CLEANUP_RUNNING = False  # Prevent multiple simultaneous historical cleanups

# Update offset tracking for processing old messages
UPDATE_OFFSET_FILE = "last_update_offset.json"
last_update_offset = 0
offset_save_counter = 0  # Save offset every N updates to reduce I/O

# Daily backup tracking
LAST_BACKUP_FILE = "last_backup.json"
last_backup_time = None
BACKUP_INTERVAL_HOURS = 24  # Backup every 24 hours
# Skip automatic backup once immediately after restart/redeploy
SKIP_STARTUP_BACKUP = True

# Local filesystem backups (optional). When disabled, only Telegram-based backups are used.
LOCAL_BACKUPS_ENABLED = False
LOCAL_BACKUP_DIR = "backups"

# User preferences storage
user_preferences = {}  # {chat_id: {"auto_delete_notifications": bool}}

# Notification cooldown tracking
last_notification_time = {}  # {chat_id: datetime} - Track last notification time per user

# Inline keyboard state cache for PUSH selector: restore the exact previous keyboard on Back
# Keyed by (chat_id, message_id) -> list[list[InlineKeyboardButton]]
push_prev_keyboards = {}
# Track the specific media being managed in the PUSH selector per message, by file_id
# Keyed by (chat_id, message_id) -> {"file_id": str, "item": dict}
push_session_targets = {}

# Track PUSH add/remove changes for status and undo
push_change_seq = 0
push_changes = {}  # id -> {id, file_id, item_snapshot, tag, action, timestamp, undone, index}
push_changes_order = []  # maintain display order (newest at end)
# Persistent storage for PUSH change history (survives redeploys)
PUSH_CHANGES_FILE = "push_changes.json"

def record_push_change(file_id: str, item_snapshot, tag_name: str, action: str, index: int | None = None):
    """Record a push change (add/remove) for status and undo support."""
    # Create a compact snapshot (deepcopy dict items to avoid mutation later)
    snap = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    global push_change_seq
    push_change_seq += 1
    change_id = push_change_seq
    entry = {
        "id": change_id,
        "file_id": file_id,
        "item_snapshot": snap,
        "tag": tag_name,
        "action": action,  # 'add' or 'remove'
        "timestamp": ts,
        "undone": False,
        "index": index,
    }
    push_changes[change_id] = entry
    push_changes_order.append(change_id)
    # Keep the log bounded
    MAX_LOG = 200
    if len(push_changes_order) > MAX_LOG:
        old_id = push_changes_order.pop(0)
        push_changes.pop(old_id, None)
    # Persist log after recording
    try:
        save_push_changes()
    except Exception as e:
        print(f"⚠️ Failed to persist push changes: {e}")
    return change_id

# Minimal logging counters
import sys
log_counters = {
    'auto_deleted': 0,
    'messages_tracked': 0,
    'cleanup_checks': 0,
    'notifications_skipped': 0,
    'data_saves': 0,
    'random_selections': 0,
    'cycle_resets': 0,
    'new_users': 0,
    'pending_deletions': 0
}

# Counter for limiting print frequency
_print_counter = 0
_last_printed_stats = None

def print_counter_line():
    """Print a single line showing all counter stats (updates in place)"""
    global _print_counter, _last_printed_stats
    
    # Update tracked messages count to current actual count
    log_counters['messages_tracked'] = len(sent_messages_tracker)
    
    # Create current stats tuple for comparison
    current_stats = (log_counters['auto_deleted'], log_counters['messages_tracked'], 
                    log_counters['cleanup_checks'], log_counters['data_saves'],
                    log_counters['random_selections'], log_counters['cycle_resets'],
                    log_counters['new_users'], log_counters['pending_deletions'])
    
    _print_counter += 1
    
    # Print on first call or when any stat changes
    if (_print_counter == 1 or _last_printed_stats is None or current_stats != _last_printed_stats):
        # Use \r to overwrite the same line (except for first call)
        if _print_counter == 1:
            print(f"🤖 Bot Online | Auto-deleted: {log_counters['auto_deleted']} | Tracked: {log_counters['messages_tracked']} | Cleanup: {log_counters['cleanup_checks']} | Saves: {log_counters['data_saves']} | Random: {log_counters['random_selections']} | Cycles: {log_counters['cycle_resets']} | New Users: {log_counters['new_users']} | Pending: {log_counters['pending_deletions']}")
        else:
            sys.stdout.write(f"\r🤖 Bot Online | Auto-deleted: {log_counters['auto_deleted']} | Tracked: {log_counters['messages_tracked']} | Cleanup: {log_counters['cleanup_checks']} | Saves: {log_counters['data_saves']} | Random: {log_counters['random_selections']} | Cycles: {log_counters['cycle_resets']} | New Users: {log_counters['new_users']} | Pending: {log_counters['pending_deletions']}")
            sys.stdout.flush()
        _last_printed_stats = current_stats

def load_tracking_data():
    """Load tracking data from file on startup"""
    global sent_messages_tracker
    try:
        if os.path.exists(TRACKING_FILE):
            with open(TRACKING_FILE, 'r') as f:
                data = json.load(f)
            
            # Convert timestamp strings back to datetime objects
            for key, message_info in data.items():
                if isinstance(message_info.get('timestamp'), str):
                    message_info['timestamp'] = datetime.fromisoformat(message_info['timestamp'])
            
            sent_messages_tracker = data
            log_counters['messages_tracked'] = len(sent_messages_tracker)
            if len(sent_messages_tracker) > 0:
                print(f"📁 Loaded {len(sent_messages_tracker)} tracked messages")
        else:
            pass  # Silent start
    except Exception as e:
        print(f"❌ Error loading tracking data: {e}")
        sent_messages_tracker = {}

def save_tracking_data():
    """Save tracking data to file"""
    try:
        # Convert datetime objects to strings for JSON serialization
        data_to_save = {}
        for key, message_info in sent_messages_tracker.copy().items():  # Use copy() to avoid iteration issues
            data_copy = message_info.copy()
            if isinstance(data_copy.get('timestamp'), datetime):
                data_copy['timestamp'] = data_copy['timestamp'].isoformat()
            data_to_save[key] = data_copy
        
        with open(TRACKING_FILE, 'w') as f:
            json.dump(data_to_save, f, indent=2)
        log_counters['data_saves'] += 1
        print_counter_line()
    except Exception as e:
        print(f"\n❌ Error saving: {e}")

def load_update_offset():
    """Load the last processed update offset"""
    global last_update_offset
    try:
        if os.path.exists(UPDATE_OFFSET_FILE):
            with open(UPDATE_OFFSET_FILE, 'r') as f:
                data = json.load(f)
                last_update_offset = data.get("last_offset", 0)
        else:
            last_update_offset = 0
    except Exception as e:
        print(f"❌ Error loading offset: {e}")
        last_update_offset = 0

def save_update_offset(offset):
    """Save the last processed update offset"""
    global last_update_offset
    try:
        # Only save if offset is actually newer
        if offset > last_update_offset:
            last_update_offset = offset
            with open(UPDATE_OFFSET_FILE, 'w') as f:
                json.dump({
                    "last_offset": offset, 
                    "saved_at": datetime.now().isoformat(),
                    "total_processed": offset
                }, f)
    except Exception as e:
        print(f"❌ Error saving offset: {e}")

def load_last_backup_time():
    """Load the last backup time from file"""
    global last_backup_time
    try:
        if os.path.exists(LAST_BACKUP_FILE):
            with open(LAST_BACKUP_FILE, 'r') as f:
                data = json.load(f)
                last_backup_time = datetime.fromisoformat(data.get("last_backup"))
                print(f"📅 Last backup time loaded: {last_backup_time}")
        else:
            last_backup_time = None
            print("📅 No previous backup time found - first backup will run soon")
    except Exception as e:
        print(f"❌ Error loading last backup time: {e}")
        last_backup_time = None

def save_last_backup_time():
    """Save the current time as last backup time"""
    global last_backup_time
    try:
        last_backup_time = datetime.now()
        with open(LAST_BACKUP_FILE, 'w') as f:
            json.dump({
                "last_backup": last_backup_time.isoformat(),
                "next_backup": (last_backup_time + timedelta(hours=BACKUP_INTERVAL_HOURS)).isoformat()
            }, f)
        print(f"💾 Last backup time saved: {last_backup_time}")
    except Exception as e:
        print(f"❌ Error saving last backup time: {e}")

def should_run_backup():
    """Check if it's time to run a backup based on the elapsed time"""
    global last_backup_time
    if last_backup_time is None:
        return True  # First backup
    
    current_time = datetime.now()
    time_diff = current_time - last_backup_time
    hours_passed = time_diff.total_seconds() / 3600
    
    # Check if enough time has passed for next backup
    return hours_passed >= BACKUP_INTERVAL_HOURS

async def process_pending_updates(application):
    """Process all pending updates that were missed during downtime"""
    try:
        # Get updates with offset
        updates = await application.bot.get_updates(
            offset=last_update_offset + 1
        )
        
        if updates:
            # Process updates silently
            for update in updates:
                try:
                    # Process update logic would go here
                    # Save the offset after each successful update
                    save_update_offset(update.update_id)
                    
                except Exception as e:
                    print(f"❌ Error processing update {update.update_id}: {e}")
                    # Still save offset to avoid reprocessing failed updates
                    save_update_offset(update.update_id)
            print(f"✅ Processed {len(updates)} pending updates")
        # else: silent if no updates
            
    except Exception as e:
        print(f"❌ Error checking updates: {e}")

def load_user_preferences():
    """Load user preferences from file"""
    global user_preferences
    try:
        if os.path.exists(USER_PREFERENCES_FILE):
            with open(USER_PREFERENCES_FILE, 'r') as f:
                user_preferences = json.load(f)
            # Convert string keys to int (chat_id)
            user_preferences = {int(k) if k.isdigit() else k: v for k, v in user_preferences.items()}
            print(f"✅ Loaded user preferences for {len(user_preferences)} users")
        else:
            user_preferences = {}
            print("⚠️ No user preferences file found, creating new")
    except Exception as e:
        print(f"❌ Error loading preferences: {e}")
        user_preferences = {}

def save_user_preferences():
    """Save user preferences to file"""
    try:
        # Convert int keys to string for JSON serialization
        data_to_save = {str(k): v for k, v in user_preferences.items()}
        
        with open(USER_PREFERENCES_FILE, 'w') as f:
            json.dump(data_to_save, f, indent=2)
        log_counters['data_saves'] += 1
        print_counter_line()
    except Exception as e:
        print(f"\n❌ Error saving preferences: {e}")

def get_user_preference(chat_id, preference_key, default_value=True):
    """Get a user's preference value"""
    return user_preferences.get(chat_id, {}).get(preference_key, default_value)

def set_user_preference(chat_id, preference_key, value):
    """Set a user preference value"""
    if chat_id not in user_preferences:
        user_preferences[chat_id] = {}
    user_preferences[chat_id][preference_key] = value
    save_user_preferences()

def can_send_notification(chat_id):
    """Check if we can send a notification to the user (respects cooldown)"""
    if chat_id not in last_notification_time:
        return True
    
    current_time = datetime.now()
    last_time = last_notification_time[chat_id]
    time_diff = current_time - last_time
    
    # Check if enough time has passed (convert hours to seconds)
    cooldown_seconds = NOTIFICATION_COOLDOWN_HOURS * 3600
    return time_diff.total_seconds() >= cooldown_seconds

def update_notification_time(chat_id):
    """Update the last notification time for a user"""
    last_notification_time[chat_id] = datetime.now()
    # Removed verbose notification logging

# Global semaphore to limit concurrent video sends and avoid rate limiting
video_send_semaphore = asyncio.Semaphore(16)  # Wider parallelism so more users start immediately
# Per-user semaphores to ensure fair processing across multiple users
user_semaphores = {}  # {user_id: Semaphore}
# Global task reference to prevent "coroutine never awaited" warnings
auto_backup_task = None

# Rate limiting for media delivery to avoid Telegram flood errors
MEDIA_SEND_COOLDOWN_SECONDS = float(os.getenv("MEDIA_SEND_COOLDOWN_SECONDS", "0.1"))
# Different cooldowns for different types of chats
# Base cooldown values - will be adjusted dynamically based on chat behavior
PRIVATE_CHAT_COOLDOWN = 0.1  # Faster for private chats
GROUP_CHAT_COOLDOWN = 0.2    # Slower for group chats
SUPERGROUP_CHAT_COOLDOWN = 0.3  # Much slower for supergroups with many users
CHANNEL_CHAT_COOLDOWN = 0.5  # Even slower for channels
PASSLINK_COOLDOWN = 0.12  # Base cooldown for passlink media delivery

# Dynamically adjusted cooldowns per chat_id
dynamic_cooldowns = {}
# Last flood control encounter time per chat_id
flood_control_timestamps = {}
user_last_send_time = defaultdict(lambda: 0.0)
consecutive_timeouts = defaultdict(int)  # Track consecutive timeouts to adjust dynamically


async def get_adaptive_cooldown(chat_id, chat_type=None):
    """Get adaptive cooldown based on chat type, recent performance, and flood history"""
    # Start with base timeout factor from consecutive timeouts
    base_timeout_factor = min(1.0 + (consecutive_timeouts.get(chat_id, 0) * 0.08), 1.3)
    
    # Check if we have a dynamic cooldown for this chat
    current_time = time.time()
    if chat_id in dynamic_cooldowns:
        # Get dynamic cooldown but apply decay over time (every 30 seconds reduce by 10%)
        dynamic_value = dynamic_cooldowns[chat_id]
        last_flood_time = flood_control_timestamps.get(chat_id, 0)
        time_since_flood = current_time - last_flood_time
        decay_factor = max(0.5, 1.0 - (time_since_flood / 300.0))  # Decay over 5 minutes
        
        # Apply the dynamic factor but ensure it doesn't go below 1.0
        dynamic_factor = 1.0 + ((dynamic_value - 1.0) * decay_factor)
        timeout_factor = max(base_timeout_factor, dynamic_factor)
    else:
        timeout_factor = base_timeout_factor
    
    # Base cooldown on chat type
    if chat_type == "private":
        return PRIVATE_CHAT_COOLDOWN * timeout_factor
    elif chat_type == "group":
        return GROUP_CHAT_COOLDOWN * timeout_factor
    elif chat_type == "supergroup":
        return SUPERGROUP_CHAT_COOLDOWN * timeout_factor
    elif chat_type == "channel":
        return CHANNEL_CHAT_COOLDOWN * timeout_factor
    else:
        # Default to the supergroup setting if chat type is unknown (safer)
        return SUPERGROUP_CHAT_COOLDOWN * timeout_factor


async def throttle_user_delivery(rate_key, chat_id=None, chat_type=None):
    """Ensure a minimum gap between media sends per user/chat to avoid flood waits."""
    if rate_key is None:
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
        
    # Check if this is a special case - passlinks should use lower cooldown
    is_passlink = getattr(asyncio.current_task(), '_is_passlink', False)
    
    if is_passlink:
        # Use minimal throttling for passlinks
        cooldown = PASSLINK_COOLDOWN
    else:
        # Get adaptive cooldown based on chat type and performance
        cooldown = await get_adaptive_cooldown(chat_id, chat_type)
    
    last_time = user_last_send_time[rate_key]
    wait_time = (last_time + cooldown) - loop.time()
    
    # For faster delivery, reduce wait time
    if wait_time > 0:
        # Cap maximum wait time to prevent excessive delays
        wait_time = min(wait_time, 0.5)
        await asyncio.sleep(wait_time)


def mark_user_delivery(rate_key, success=True, chat_id=None):
    """Record the most recent media send time for the user/chat."""
    if rate_key is None:
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    
    # Update tracking for adaptive cooldown - more aggressively reduce consecutive timeouts on success
    if success and chat_id is not None:
        current = consecutive_timeouts.get(chat_id, 0)
        # Reset timeout count more quickly to return to normal speeds faster
        if current > 0:
            # More aggressive reduction on success to improve user experience
            consecutive_timeouts[chat_id] = max(0, current - 0.8)
            
        # Also reduce dynamic cooldown for successful deliveries
        if chat_id in dynamic_cooldowns:
            current_cooldown = dynamic_cooldowns.get(chat_id)
            if current_cooldown > 1.0:
                # Gradually reduce the dynamic cooldown to return to normal speed
                dynamic_cooldowns[chat_id] = max(1.0, current_cooldown * 0.95)
            consecutive_timeouts[chat_id] = max(0, current - 1.0)  # Reduce by 1 full point
            # For very low counts, just reset to 0
            if consecutive_timeouts[chat_id] < 0.5:
                consecutive_timeouts[chat_id] = 0
    
    user_last_send_time[rate_key] = loop.time()

def get_user_semaphore(user_id):
    """Get or create a semaphore for a specific user to allow parallel processing per user"""
    if user_id not in user_semaphores:
        user_semaphores[user_id] = asyncio.Semaphore(1)  # Ensure fairness: 1 active send per user
    return user_semaphores[user_id]

async def safe_answer_callback_query(query, text="", show_alert=False, timeout=3.0):
    """Safely answer callback query with timeout protection and rate limiting"""
    try:
        await asyncio.wait_for(query.answer(text, show_alert=show_alert), timeout=timeout)
        # Small delay to prevent rate limiting
        await asyncio.sleep(0.1)
    except asyncio.TimeoutError:
        pass  # Timeout is expected for some queries
    except Exception as e:
        pass  # Silent error handling
        
async def delete_message_after_delay(bot, chat_id, message_id, delay_seconds=1):
    """Delete a message after a specified delay"""
    try:
        await asyncio.sleep(delay_seconds)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass  # Silently ignore any errors (e.g., message already deleted)

async def track_sent_message(message, auto_delete_delay_hours=AUTO_DELETE_HOURS):
    """Track a sent message for auto-deletion with persistent storage"""
    if not AUTO_DELETE_ENABLED or not message or not hasattr(message, 'message_id'):
        return
        
    message_info = {
        "chat_id": message.chat_id,
        "message_id": message.message_id,
        "timestamp": datetime.now(),
        "delete_after_hours": auto_delete_delay_hours
    }
    key = f"{message.chat_id}_{message.message_id}"
    sent_messages_tracker[key] = message_info
    
    print_counter_line()
    
    # Save to file in background to avoid blocking the event loop
    try:
        global auto_backup_task
        auto_backup_task = asyncio.create_task(asyncio.to_thread(save_tracking_data))
    except Exception:
        # Fallback to direct call if scheduling fails
        save_tracking_data()

async def send_and_track_message(send_func, *args, user_id=None, max_retries=5, **kwargs):
    """Wrapper to send a message with retry + rate limit handling, then track for auto-deletion."""
    chat_id = kwargs.get("chat_id")
    if chat_id is None and args:
        # Some reply_* methods don't expose chat_id; fall back to positional value if plausible
        potential_chat_id = args[0]
        if isinstance(potential_chat_id, int):
            chat_id = potential_chat_id

    rate_key = user_id if user_id is not None else chat_id
    media_tokens = ("video", "photo", "document", "audio", "voice", "animation", "sticker")
    func_name = getattr(send_func, "__name__", "")
    is_media_send = any(token in func_name for token in media_tokens) or any(token in kwargs for token in media_tokens)

    for attempt in range(max_retries):
        try:
            if is_media_send:
                await throttle_user_delivery(rate_key)

            message = await send_func(*args, **kwargs)

            if not message:
                return None

            if is_media_send:
                mark_user_delivery(rate_key)

            # Track messages (handle single message or list of messages)
            if isinstance(message, list):
                for msg in message:
                    await track_sent_message(msg)
            else:
                await track_sent_message(message)

            return message

        except TimedOut:
            # Record the timeout and adjust dynamic cooldown
            if chat_id is not None:
                consecutive_timeouts[chat_id] = consecutive_timeouts.get(chat_id, 0) + 0.3
                
                # Also adjust dynamic cooldown moderately for timeouts
                current_dynamic = dynamic_cooldowns.get(chat_id, 1.0)
                dynamic_cooldowns[chat_id] = min(2.0, current_dynamic * 1.1)
                
            if attempt < max_retries - 1:
                # Use minimal backoff for faster recovery
                wait_time = 0.3 + (0.3 * attempt)  # 0.6s, 0.9s, 1.2s...
                await asyncio.sleep(wait_time)
                continue
            print(f"Failed to send message after {max_retries} attempts due to timeout")
            return None

        except RetryAfter as e:
            # Record the flood control event and adjust dynamic cooldown
            if chat_id is not None:
                consecutive_timeouts[chat_id] = consecutive_timeouts.get(chat_id, 0) + 1.0
                
                # More aggressive adjustment for flood control
                current_dynamic = dynamic_cooldowns.get(chat_id, 1.0)
                dynamic_cooldowns[chat_id] = min(3.0, current_dynamic * 1.3)
                flood_control_timestamps[chat_id] = time.time()
                
                print(f"Flood control for chat {chat_id}: cooldown now {dynamic_cooldowns[chat_id]:.2f}x")
                
            if attempt < max_retries - 1:
                # Wait exactly as required by Telegram plus a very small buffer
                wait_time = e.retry_after + 0.05
                
                print(f"Rate limited for {e.retry_after}s, waiting {wait_time}s before retry (attempt {attempt + 1}/{max_retries})...")
                await asyncio.sleep(wait_time)
                continue
            print(f"Failed to send message after {max_retries} attempts due to rate limiting")
            return None

        except (NetworkError, BadRequest) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                await asyncio.sleep(wait_time)
                continue
            print(f"Failed to send message after {max_retries} attempts due to: {e}")
            return None

        except Exception as e:
            print(f"Error sending/tracking message: {e}")
            if attempt >= max_retries - 1:
                return None
            await asyncio.sleep(1)

    return None

async def cleanup_old_messages(context):
    """Background task to delete old messages"""
    current_time = datetime.now()
    messages_to_delete = []
    
    log_counters['cleanup_checks'] += 1
    
    for key, message_info in list(sent_messages_tracker.items()):
        delete_time = message_info["timestamp"] + timedelta(hours=message_info["delete_after_hours"])
        time_left = delete_time - current_time
        
        if current_time >= delete_time:
            messages_to_delete.append(key)
    
    # Update pending deletions count and show progress if there are messages to delete
    log_counters['pending_deletions'] = len(messages_to_delete)
    if messages_to_delete:
        print_counter_line()
    
    # Process deletions with progress
    for i, key in enumerate(messages_to_delete, 1):
        message_info = sent_messages_tracker[key]
        try:
            await context.bot.delete_message(
                chat_id=message_info["chat_id"],
                message_id=message_info["message_id"]
            )
            log_counters['auto_deleted'] += 1
            log_counters['pending_deletions'] = len(messages_to_delete) - i
            
            if i % 5 == 0 or i == len(messages_to_delete):  # Update every 5 deletions or at the end
                print_counter_line()
            
            # Send notification to user about the deletion (if enabled and cooldown allows)
            chat_id = message_info["chat_id"]
            if (AUTO_DELETE_NOTIFICATIONS and 
                get_user_preference(chat_id, "auto_delete_notifications", True) and
                can_send_notification(chat_id)):
                
                try:
                    notification_message = (
                        "🧹 **Auto-Cleanup Notification**\n\n"
                        "The file you received has been automatically removed to keep the bot clean.\n\n"
                        "💡 **Tip**: Use the favorites feature (⭐) to save important videos permanently!\n"
                        f"⏰ Auto-cleanup happens after {AUTO_DELETE_HOURS} hour(s) for all shared media."
                    )
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=notification_message,
                        parse_mode='Markdown'
                    )
                    # Update the notification time after successful send
                    update_notification_time(chat_id)
                except Exception as notification_error:
                    pass  # Silent error handling
            
            else:
                # Notifications disabled or in cooldown - just count it
                log_counters['notifications_skipped'] += 1
                    
        except Exception as e:
            pass  # Silent error handling
    
    # Remove deleted messages from tracker
    for key in messages_to_delete:
        sent_messages_tracker.pop(key, None)
    
    # Reset pending deletions counter
    log_counters['pending_deletions'] = 0
    
    # Save tracking data after cleanup
    if messages_to_delete:
        try:
            global auto_backup_task
            auto_backup_task = asyncio.create_task(asyncio.to_thread(save_tracking_data))
        except Exception:
            save_tracking_data()
    
    # Update counter display after cleanup
    print_counter_line()

async def start_background_cleanup(app):
    """Start background cleanup task that works with app.run_polling()"""
    await asyncio.sleep(60)  # Wait 1 minute before first check
    while True:
        try:
            await cleanup_old_messages(app)
            await asyncio.sleep(300)  # Check every 5 minutes
        except asyncio.CancelledError:
            # Graceful shutdown
            break
        except Exception as e:
            print(f"Error in background cleanup: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retrying

async def check_daily_backup(app):
    """Check if daily backup should be run and execute it if needed"""
    while True:
        try:
            global SKIP_STARTUP_BACKUP
            if SKIP_STARTUP_BACKUP:
                # On first loop after startup, do NOT send backups; set baseline to now
                try:
                    save_last_backup_time()
                    print("⏭️ Startup backup skipped (baseline time set)")
                except Exception as e:
                    print(f"⚠️ Could not set baseline on startup: {e}")
                SKIP_STARTUP_BACKUP = False
                # Wait until next cycle
                await asyncio.sleep(3600)
                continue
            if should_run_backup():
                print("🔄 Daily backup check: Time to backup!")
                await perform_daily_backup(app)
                save_last_backup_time()
                print(f"✅ Daily backup completed at {datetime.now()}")
            else:
                # Calculate hours until next backup
                if last_backup_time:
                    next_backup = last_backup_time + timedelta(hours=BACKUP_INTERVAL_HOURS)
                    hours_until = (next_backup - datetime.now()).total_seconds() / 3600
                    print(f"⏰ Next backup in {hours_until:.1f} hours")
        except asyncio.CancelledError:
            # Graceful shutdown
            break
        except Exception as e:
            print(f"❌ Error in daily backup check: {e}")
        
        # Check every hour
        await asyncio.sleep(3600)  # 1 hour = 3600 seconds

async def perform_daily_backup(app):
    """Perform the actual daily backup by sending files to admin"""
    try:
        print("📤 Starting daily backup to Telegram...")
        
        json_files = [
            "media_db.json", "users_db.json", "favorites_db.json",
            "passed_links.json", "active_links.json", "deleted_media.json",
            "random_state.json", "user_preferences.json", "admin_list.json",
            "exempted_users.json", "caption_config.json", "protection_settings.json",
            "auto_delete_tracking.json", "last_update_offset.json", "push_changes.json",
            "off_limits.json"
        ]

        sent_count = 0
        for filename in json_files:
            if os.path.exists(filename):
                with open(filename, 'rb') as file:
                    await app.bot.send_document(
                        chat_id=ADMIN_ID,
                        document=file,
                        filename=f"daily_{filename}",
                        caption=f"🔄 **Daily Auto-Backup**\n📁 `{filename}`\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    )
        sent_count += 1

        await app.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"✅ **Daily Auto-Backup Complete**\n📤 {sent_count} files backed up to Telegram\n🔒 Your Railway data is now safely stored!"
        )
        
        print(f"📤 Daily backup sent {sent_count} files to admin")
        
    except Exception as e:
        print(f"❌ Daily backup failed: {e}")
        try:
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"❌ Daily backup failed: {str(e)}"
            )
        except Exception as e2:
            print(f"❌ Could not send backup failure notification: {e2}")


async def start_background_reindex(app):
    """Background task to periodically run reindex routines for favorites and likes.
    Uses favorites_data['reindex_config'] to determine enabled/interval values.
    Safe to run concurrently: functions used are idempotent and persist data.
    """
    global reindex_task_active
    if reindex_task_active:
        # Already running; bail out
        return
    reindex_task_active = True
    # Wait briefly for startup to complete
    await asyncio.sleep(10)
    try:
        while True:
            try:
                rc = favorites_data.get("reindex_config", {})
                enabled = rc.get("auto_reindex_enabled", False)
                interval = int(rc.get("interval_seconds", 3600))

                if not enabled:
                    # Sleep and re-check periodically
                    await asyncio.sleep(max(60, interval if interval else 3600))
                    continue

                print("🔁 Auto reindex: running scheduled reindex routine...")
                # Run update metadata, normalize & recalc in threads to avoid blocking
                try:
                    await asyncio.to_thread(update_video_metadata_for_all_favorites)
                    await asyncio.to_thread(normalize_video_likes_counts)
                    await asyncio.to_thread(recalc_video_likes_from_favorites)
                    print("✅ Auto reindex: completed successfully")
                except Exception as re:
                    print(f"⚠️ Auto reindex encountered an error: {re}")
                # Refresh all active viewers to ensure caption/menu matches new like counts
                try:
                    await refresh_all_active_top_viewers(app)
                except Exception:
                    pass

            except asyncio.CancelledError:
                # Graceful shutdown
                break
            except Exception as e:
                print(f"❌ Error in auto reindex task: {e}")

            # Sleep until next iteration
            try:
                interval = int(favorites_data.get("reindex_config", {}).get("interval_seconds", 3600))
            except Exception:
                interval = 3600
            await asyncio.sleep(max(30, interval))
    finally:
        # Mark as inactive when the loop exits
        reindex_task_active = False

async def post_shutdown_callback(application):
    """Gracefully cancel background tasks to avoid pending task warnings."""
    # Cancel tracked tasks
    for task in BACKGROUND_TASKS:
        if not task.done():
            task.cancel()
    # Wait briefly for cancellation
    await asyncio.sleep(0)
    # Optionally log
    try:
        print("🔻 Background tasks cancelled cleanly.")
    except Exception:
        pass

async def post_init_callback(application):
    """Callback to start background tasks after app initialization"""
    # Ensure no webhook is configured before starting polling to avoid conflicts
    try:
        await application.bot.delete_webhook(drop_pending_updates=False)
        logging.info("Webhook cleared ahead of polling startup")
    except Exception as e:
        logging.warning("Failed to delete webhook before polling: %s", e)
    
    # Load existing tracking data from file
    load_tracking_data()
    
    # Load user preferences from file
    load_user_preferences()
    
    # Load PUSH change history (for /pushst persistence)
    load_push_changes()

    # Load last update offset for processing old messages
    load_update_offset()
    
    # Load last backup time
    load_last_backup_time()
    # On fresh deploy/restart, don't auto-backup immediately; set baseline to now
    if last_backup_time is None:
        try:
            # Set baseline so the hourly checker doesn't send backups on startup
            save_last_backup_time()
        except Exception as e:
            print(f"⚠️ Could not set backup baseline on startup: {e}")
    
    # Set bot commands menu with retry logic
    try:
        # Safely set bot commands if commands iterable exists
        try:
            if 'commands' in globals() and isinstance(commands, (list, tuple)):
                await application.bot.set_my_commands(commands)
            else:
                print("⚠️ 'commands' not defined or not a list; skipping set_my_commands.")
        except Exception as e_cmd:
            print(f"⚠️ set_my_commands failed: {e_cmd}")
        print("✅ Bot commands menu set successfully")
    except Exception as e:
        print(f"⚠️ Failed to set commands menu: {e}")
        print("🔄 Bot will continue without menu - commands still work")
    
    # Process any pending updates that were missed
    await process_pending_updates(application)

    # Normalize any tag containers (dicts with numeric keys -> list) to prevent index errors in favorites
    try:
        normalize_all_media_containers()
    except Exception as e:
        print(f"⚠️ normalize_all_media_containers failed: {e}")
    
    # Update counter display after loading data
    print_counter_line()

    # Sanitize passed_links: drop empty tags so Switch cycle never shows them
    try:
        sanitize_passed_links_remove_empty_tags()
    except Exception as e:
        print(f"⚠️ Could not sanitize passed_links on startup: {e}")
    
    if AUTO_DELETE_ENABLED:
        # Start the background cleanup task and record it
        task_cleanup = asyncio.create_task(start_background_cleanup(application))
        BACKGROUND_TASKS.append(task_cleanup)
        # Auto-deletion background task started silently
    
    # Notify admin that bot restarted (no backup sent)
    try:
        await application.bot.send_message(chat_id=ADMIN_ID, text="🔁 Bot restarted and ready.")
    except Exception as e:
        print(f"⚠️ Could not notify admin about restart: {e}")

    # Start the daily backup check task (more reliable than job_queue)
    task_backup = asyncio.create_task(check_daily_backup(application))
    BACKGROUND_TASKS.append(task_backup)
    print("✅ Daily backup monitoring started")
    # Start periodic reindex background task if enabled in favorites_data
    try:
        if favorites_data.get("reindex_config", {}).get("auto_reindex_enabled"):
            task_reindex = asyncio.create_task(start_background_reindex(application))
            BACKGROUND_TASKS.append(task_reindex)
            print("✅ Auto reindex background task started")
    except Exception as e:
        print(f"⚠️ Failed to start Auto reindex background task: {e}")

    
    # Schedule daily auto-backup to Telegram (2 AM UTC) - fallback method
    async def daily_telegram_backup(context: ContextTypes.DEFAULT_TYPE):
        """Daily backup routine - sends all JSON files to admin"""
        try:
            json_files = [
                "media_db.json", "users_db.json", "favorites_db.json",
                "passed_links.json", "active_links.json", "deleted_media.json",
                "random_state.json", "user_preferences.json", "admin_list.json",
                "exempted_users.json", "caption_config.json", "protection_settings.json",
                "auto_delete_tracking.json", "last_update_offset.json", "push_changes.json",
                "off_limits.json"
            ]

            sent_count = 0
            for filename in json_files:
                if os.path.exists(filename):
                    with open(filename, 'rb') as file:
                        await context.bot.send_document(
                            chat_id=ADMIN_ID,
                            document=file,
                            filename=f"daily_{filename}",
                            caption=f"🔄 **Daily Auto-Backup**\n📁 `{filename}`\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                        )
                    sent_count += 1
            # finished sending daily auto-backup files

            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"✅ **Daily Auto-Backup Complete**\n📤 {sent_count} files backed up to Telegram\n🔒 Your Railway data is now safely stored!"
            )
        except Exception as e:
            await context.bot.send_message(
                chat_id=ADMIN_ID, 
                text=f"❌ Daily backup failed: {str(e)}"
            )

    # Schedule daily backup at 2 AM UTC (kept as fallback)
    if hasattr(application, 'job_queue') and application.job_queue:
        application.job_queue.run_daily(
            daily_telegram_backup,
            time=datetime_time(2, 0),  # 2 AM UTC
            name="daily_backup"
        )
        print("✅ Daily backup job scheduled via job_queue (fallback)")
    else:
        print("⚠️ Job queue not available - relying on time-based backup checks")

async def historical_cleanup_old_media(context, max_hours_old=AUTO_DELETE_HOURS, max_messages_per_chat=100, dry_run=False):
    """
    Historical cleanup system to delete old media messages that predate the tracking system
    
    This function uses a safer approach:
    1. Scans the media_db.json to find all bot-sent media
    2. Attempts to delete messages older than the specified time
    3. Uses file modification times as a proxy for message age
    
    Args:
        context: Bot context or application
        max_hours_old: Delete messages older than this many hours (default: same as auto-delete)
        max_messages_per_chat: Maximum number of recent messages to check per chat
        dry_run: If True, only count messages without deleting them
    """
    if not AUTO_DELETE_ENABLED:
        # Auto-deletion disabled, skip cleanup
        return {"status": "disabled", "deleted_count": 0, "error_count": 0}
    
    print(f"\n🧹 Starting historical cleanup - checking messages older than {max_hours_old} hours")
    print(f"🧹 Dry run mode: {dry_run}")
    
    cutoff_time = datetime.now() - timedelta(hours=max_hours_old)
    deleted_count = 0
    error_count = 0
    checked_count = 0
    
    try:
        # Strategy 1: Check media_db.json for previously sent media
        media_db_file = "media_db.json"
        video_db_file = "video_db.json"
        
        media_files_to_check = []
        
        # Load media databases
        for db_file in [media_db_file, video_db_file]:
            if os.path.exists(db_file):
                try:
                    with open(db_file, 'r') as f:
                        db_data = json.load(f)
                    media_files_to_check.extend(db_data.keys())
                    print(f"🧹 Loaded {len(db_data)} entries from {db_file}")
                except Exception as e:
                    print(f"❌ Error loading {db_file}: {e}")
        
        if not media_files_to_check:
            # No media database found, try alternative method
            return await historical_cleanup_by_recent_scan(context, max_hours_old, dry_run)
        
        print(f"🧹 Found {len(media_files_to_check)} media files to check")
        
        # Get registered users to know which chats to check
        users_db_file = "users_db.json"
        if not os.path.exists(users_db_file):
            # No users database file found
            return {"status": "no_users_file", "deleted_count": 0, "error_count": 0}
        
        with open(users_db_file, 'r') as f:
            users_db = json.load(f)
        
        print(f"🧹 Checking {len(users_db)} registered users")
        
        # For each chat, try to clean up old media
        for user_id_str, user_info in users_db.items():
            try:
                chat_id = int(user_id_str)
                username = user_info.get('username', user_info.get('first_name', 'Unknown'))
                
                print(f"🧹 Processing chat {chat_id} ({username})...")
                
                # Get recent messages to find bot-sent media
                messages_cleaned_in_chat = 0
                
                try:
                    # Send a temp message to get current message ID range
                    temp_msg = await context.bot.send_message(chat_id, "🔍")
                    current_msg_id = temp_msg.message_id
                    await context.bot.delete_message(chat_id, current_msg_id)
                    
                    # Check recent messages working backwards
                    start_check_id = max(1, current_msg_id - max_messages_per_chat)
                    
                    for msg_id in range(current_msg_id - 1, start_check_id, -1):
                        if messages_cleaned_in_chat >= max_messages_per_chat:
                            break
                        
                        # Skip if already tracked in current session
                        if f"{chat_id}_{msg_id}" in sent_messages_tracker:
                            continue
                        
                        checked_count += 1
                        
                        try:
                            # Try to delete the message - if it's a bot message and exists, it will be deleted
                            # If it's not a bot message or doesn't exist, we'll get an error (which is fine)
                            if not dry_run:
                                await context.bot.delete_message(chat_id, msg_id)
                                deleted_count += 1
                                messages_cleaned_in_chat += 1
                                print(f"✅ Historical cleanup: deleted message {msg_id} from chat {chat_id}")
                                
                                # Rate limiting
                                await asyncio.sleep(0.05)
                            else:
                                # In dry run, we can't easily check if it's our message without trying to delete
                                # So we'll estimate based on message density
                                if (current_msg_id - msg_id) > 50:  # Assume messages older than 50 IDs might be ours
                                    deleted_count += 1
                                    
                        except BadRequest as e:
                            error_msg = str(e).lower()
                            if any(phrase in error_msg for phrase in [
                                "message to delete not found",
                                "message can't be deleted",
                                "bad request",
                                "not found"
                            ]):
                                # Expected errors - message doesn't exist or isn't ours
                                pass
                            else:
                                error_count += 1
                                print(f"❌ Unexpected error deleting {msg_id}: {e}")
                                
                        except Exception as e:
                            error_count += 1
                            print(f"❌ Error deleting message {msg_id}: {e}")
                
                except Exception as e:
                    print(f"❌ Error processing chat {chat_id}: {e}")
                    error_count += 1
                
                # Rate limiting between chats
                await asyncio.sleep(0.2)
                
            except Exception as e:
                print(f"❌ Error processing user {user_id_str}: {e}")
                error_count += 1
        
        result = {
            "status": "completed",
            "deleted_count": deleted_count,
            "error_count": error_count,
            "checked_count": checked_count,
            "dry_run": dry_run
        }
        
        if dry_run:
            print(f"🧹 Historical cleanup (DRY RUN) completed: {deleted_count} messages would be deleted, {error_count} errors, {checked_count} messages checked")
        else:
            print(f"🧹 Historical cleanup completed: {deleted_count} messages deleted, {error_count} errors, {checked_count} messages checked")
        print_counter_line()
        
        return result
        
    except Exception as e:
        print(f"❌ Historical cleanup failed: {e}")
        return {"status": "error", "deleted_count": deleted_count, "error_count": error_count + 1, "error": str(e)}

async def historical_cleanup_by_recent_scan(context, max_hours_old, dry_run=False):
    """
    Alternative historical cleanup that scans recent messages more conservatively
    """
    # Using conservative recent message scan method
    
    deleted_count = 0
    error_count = 0
    checked_count = 0
    
    try:
        # Get registered users
        users_db_file = "users_db.json"
        if not os.path.exists(users_db_file):
            return {"status": "no_users_file", "deleted_count": 0, "error_count": 0}
        
        with open(users_db_file, 'r') as f:
            users_db = json.load(f)
        
        # Only check a few recent messages per chat to be safe
        max_messages_to_check = 20  # Very conservative
        
        for user_id_str, user_info in users_db.items():
            try:
                chat_id = int(user_id_str)
                username = user_info.get('username', user_info.get('first_name', 'Unknown'))
                
                print(f"🧹 Conservative scan of chat {chat_id} ({username})...")
                
                # Get current message ID
                temp_msg = await context.bot.send_message(chat_id, "🔍")
                current_msg_id = temp_msg.message_id
                await context.bot.delete_message(chat_id, current_msg_id)
                
                # Only check very recent messages to be safe
                start_id = max(1, current_msg_id - max_messages_to_check)
                
                for msg_id in range(current_msg_id - 1, start_id, -1):
                    # Skip tracked messages
                    if f"{chat_id}_{msg_id}" in sent_messages_tracker:
                        continue
                    
                    checked_count += 1
                    
                    if not dry_run:
                        try:
                            await context.bot.delete_message(chat_id, msg_id)
                            deleted_count += 1
                            print(f"✅ Conservative cleanup: deleted message {msg_id}")
                            await asyncio.sleep(0.1)
                        except:
                            # Silently ignore - expected for non-bot messages
                            pass
                    else:
                        # Conservative estimate in dry run
                        deleted_count += 1
                
                await asyncio.sleep(0.3)
                
            except Exception as e:
                error_count += 1
                print(f"❌ Error in conservative scan: {e}")
        
        return {
            "status": "completed",
            "deleted_count": deleted_count,
            "error_count": error_count,
            "checked_count": checked_count,
            "dry_run": dry_run
        }
        
    except Exception as e:
        return {"status": "error", "deleted_count": 0, "error_count": 1, "error": str(e)}

# Welcome image configuration
# Using file_id for production deployment
WELCOME_IMAGE_FILE_ID = "AgACAgUAAxkBAAECwb9ovJ6vclCn0ucY51T2QNapjxtq8QACycgxGyc56FXuV3ayeCijawEAAwIAA3kAAzYE"  # Welcome image file ID
CHANNEL_NOTIFY_VIDEO_ID = "BAACAgUAAxkBAAEMbodo_eavM_AppNuQSovZtyHbSCEdfwACzxsAAuZ98FcMaMRir8pEkDYE"  # Channel notification video ID

# ADMIN_ID is now loaded from environment variables at the top of the file
ADMIN_LIST_FILE = "admin_list.json"
REQUIRED_CHANNELS = ["Lifesuckkkkkssss", "bhaicharabackup"]
MEDIA_FILE = "media_db.json"
EXEMPTED_FILE = "exempted_users.json"
USERS_FILE = "users_db.json"
ACTIVE_LINKS_FILE = "active_links.json"
PASSED_LINKS_FILE = "passed_links.json"
OFF_LIMITS_FILE = "off_limits.json"  # Special category for exclusive content
FAVORITES_FILE = "favorites_db.json"
RANDOM_STATE_FILE = "random_state.json"
CAPTION_CONFIG_FILE = "caption_config.json"
DELETED_MEDIA_FILE = "deleted_media.json"
AUTODELETE_CONFIG_FILE = "autodelete_config.json"
BOT_USERNAME = "bhaicharabackupbot"  # 🔁 REPLACE THIS

# Caption configuration
caption_config = {
    "global_caption": "",  # Global caption to add to all files
    "replacements": [],     # List of {"find": str, "replace": str}
    "link_override_enabled": False,  # When true, rewrite all links to override URL
    "link_override_url": ""         # Target URL to replace any links with
}

media_data = {}
exempted_users = set()
users_data = {}  # Track all bot users
active_links = set()
passed_links = []  # List of specific video_ids that are passed
off_limits_data = []  # List of video_ids for exclusive Off Limits category
favorites_data = {"user_favorites": {}, "video_likes": {}}

# Runtime resume storage for interrupted multi-send operations
resume_states = {}
# Track background tasks to cancel gracefully on shutdown
BACKGROUND_TASKS = []
# Track active Top Videos viewer messages so we can refresh captions on like changes
active_top_viewers = {}  # (chat_id, message_id) -> {"video_id": str, "tag": str, "idx": int, "media_type": str, "range_limit": int, "page": int, "reply_markup": InlineKeyboardMarkup}
random_state = {"shown_videos": [], "all_videos": []}
media_buffer = defaultdict(list)
media_tag_map = {}
media_group_tasks = {}
custom_batch_sessions = {}  # Track active custom batch sessions
ai_batch_sessions = {}  # Track active AI batch sessions
admin_list = [ADMIN_ID]  # Initialize with main admin
deleted_media_storage = {}  # Store deleted media for restoration
user_operations = {}  # Track ongoing operations per user

# Admin Quick Push (auto-tag after forwarding media)
admin_quick_push_buffer = defaultdict(list)  # user_id -> list[media_item]
admin_quick_push_tasks = {}  # user_id -> asyncio.Task

# Protection settings
PROTECTION_FILE = "protection_settings.json"
protection_enabled = True  # Default protection on

# Track whether reindex background task is already started to avoid duplicates
reindex_task_active = False

def load_protection_settings():
    """Load protection settings from file"""
    global protection_enabled
    try:
        if os.path.exists(PROTECTION_FILE):
            with open(PROTECTION_FILE, "r") as f:
                settings = json.load(f)
                protection_enabled = settings.get("enabled", True)
    except Exception as e:
        print(f"Error loading protection settings: {e}")
        protection_enabled = True

def save_protection_settings():
    """Save protection settings to file"""
    try:
        with open(PROTECTION_FILE, "w") as f:
            json.dump({"enabled": protection_enabled}, f)
        print(f"Protection settings saved: {protection_enabled}")
    except Exception as e:
        print(f"Error saving protection settings: {e}")
        # Try to save to a backup location
        try:
            with open("protection_settings_backup.json", "w") as f:
                json.dump({"enabled": protection_enabled}, f)
            print("Protection settings saved to backup file")
        except Exception as e2:
            print(f"Error saving to backup: {e2}")

# Load protection settings on startup
load_protection_settings()

# Keyword-to-Tag Mapping for Smart Search
# Users can type these keywords to get media from corresponding tags
KEYWORD_TAG_MAPPING = {
    # Dance related
    
    # Romance/Intimacy related - maps to "turn.on" tag
    "intimacy": ["turn.on"],
    "intimate": ["turn.on"],
    "romance": ["turn.on"],
    "romantic": ["turn.on"],
    "love": ["turn.on"],
    "lovely": ["turn.on"],
    "passion": ["turn.on"],
    "passionate": ["turn.on"],
    "care": ["turn.on"],
    "caring": ["turn.on"],
    "affection": ["turn.on"],
    "affectionate": ["turn.on"],
    "tender": ["turn.on"],
    "kiss": ["turn.on"],
    "kissing": ["turn.on"],
    "hug": ["turn.on"],
    "embrace": ["turn.on"],
    "cuddle": ["turn.on"],
    
    # Add more mappings as needed
    # Example: "action": ["action", "fight", "combat"],
}

def find_tag_by_keyword(keyword: str) -> str:
    """Find the best matching tag for a user's keyword.
    Returns the tag name if found, None otherwise."""
    keyword_lower = keyword.lower().strip()
    
    # Direct match in mapping
    if keyword_lower in KEYWORD_TAG_MAPPING:
        possible_tags = KEYWORD_TAG_MAPPING[keyword_lower]
        # Return first tag that exists in passed_links
        for tag in possible_tags:
            # Check if tag exists in passed_links
            if any(k.startswith(f"{tag}_") for k in passed_links):
                return tag
        # If no tag found in passed_links, return first option
        return possible_tags[0] if possible_tags else None
    
    # Fuzzy match: check if keyword is part of any tag in passed_links
    for link_key in passed_links:
        if "_" in link_key:
            tag = link_key.rsplit("_", 1)[0]
            if keyword_lower in tag.lower():
                return tag
    
    return None

def get_protection_status():
    """Get current protection status"""
    return protection_enabled

def should_protect_content(user_id, content):
    """Check if content should be protected for a given user"""
    # Exempt admin users from protection
    if user_id in admin_list:
        return False

    # Return protection setting if not admin
    return protection_enabled

def save_autodelete_config():
    """Save auto-delete configuration to file"""
    with open(AUTODELETE_CONFIG_FILE, "w") as f:
        config_data = {
            "AUTO_DELETE_HOURS": AUTO_DELETE_HOURS,
            "AUTO_DELETE_ENABLED": AUTO_DELETE_ENABLED
        }
        json.dump(config_data, f, indent=2)
    print(f"💾 Saved autodelete config: {AUTO_DELETE_HOURS} hours, enabled: {AUTO_DELETE_ENABLED}")


def load_autodelete_config():
    """Load auto-delete configuration from file"""
    global AUTO_DELETE_HOURS, AUTO_DELETE_ENABLED
    try:
        if os.path.exists(AUTODELETE_CONFIG_FILE):
            with open(AUTODELETE_CONFIG_FILE, "r") as f:
                config = json.load(f)
                AUTO_DELETE_HOURS = config.get("AUTO_DELETE_HOURS", 24)  # Default to 24 hours
                AUTO_DELETE_ENABLED = config.get("AUTO_DELETE_ENABLED", True)
            print(f"✅ Loaded autodelete config: {AUTO_DELETE_HOURS} hours, enabled: {AUTO_DELETE_ENABLED}")
        else:
            print(f"ℹ️ No autodelete config file found, using defaults: {AUTO_DELETE_HOURS} hours")
    except Exception as e:
        print(f"❌ Error loading autodelete config: {e}")
        # Keep default values

# Soft deletion & revocation helpers:
# We store media as original dicts. For soft delete we replace entry with
# {"deleted": True, "data": <original>}. For revoke we set entry["revoked"] = True.
# Random / browsing logic must skip entries where deleted or revoked.



if os.path.exists(MEDIA_FILE):
    with open(MEDIA_FILE, "r") as f:
        try:
            media_data = json.load(f)
        except json.JSONDecodeError:
            media_data = {}

if os.path.exists(EXEMPTED_FILE):
    with open(EXEMPTED_FILE, "r") as f:
        try:
            exempted_users = set(json.load(f))
        except json.JSONDecodeError:
            exempted_users = set()

if os.path.exists(ACTIVE_LINKS_FILE):
    with open(ACTIVE_LINKS_FILE, "r") as f:
        try:
            data = json.load(f)
            if isinstance(data, list):
                # Old format - convert to new dict format
                active_links = {}
                for link_key in data:
                    active_links[link_key] = {"type": "reference"}  # Mark as reference to media_data
            else:
                active_links = data
        except json.JSONDecodeError:
            active_links = {}

if os.path.exists(PASSED_LINKS_FILE):
    with open(PASSED_LINKS_FILE, "r") as f:
        try:
            passed_links = json.load(f)
        except json.JSONDecodeError:
            passed_links = []

if os.path.exists(OFF_LIMITS_FILE):
    with open(OFF_LIMITS_FILE, "r") as f:
        try:
            off_limits_data = json.load(f)
        except json.JSONDecodeError:
            off_limits_data = []

if os.path.exists(FAVORITES_FILE):
    with open(FAVORITES_FILE, "r") as f:
        try:
            favorites_data = json.load(f)
            # Ensure the structure exists
            if "user_favorites" not in favorites_data:
                favorites_data["user_favorites"] = {}
            if "video_likes" not in favorites_data:
                favorites_data["video_likes"] = {}
            if "video_metadata" not in favorites_data:
                favorites_data["video_metadata"] = {}
        except json.JSONDecodeError:
            favorites_data = {"user_favorites": {}, "video_likes": {}, "video_metadata": {}}

# Ensure reindex configuration defaults exist (persisted in favorites file)
if "reindex_config" not in favorites_data:
    favorites_data["reindex_config"] = {"auto_reindex_enabled": False, "interval_seconds": 3600}
else:
    rc = favorites_data["reindex_config"]
    if not isinstance(rc, dict):
        favorites_data["reindex_config"] = {"auto_reindex_enabled": False, "interval_seconds": 3600}
    else:
        rc.setdefault("auto_reindex_enabled", False)
        rc.setdefault("interval_seconds", 3600)


def update_video_metadata_for_all_favorites():
    """Ensure `favorites_data['video_metadata']` contains metadata for all known favorites.
    This scans all favorites (dict or legacy list), attempts to locate tag/index and file_id, records
    metadata via record_video_metadata, and saves if any new entries were added.
    """
    try:
        metadata_map = favorites_data.setdefault("video_metadata", {})
        made_update = False
        user_map = favorites_data.get("user_favorites", {})
        for user_id, favs in user_map.items():
            entries = []
            if isinstance(favs, dict):
                entries = list(favs.items())
            elif isinstance(favs, list):
                entries = [(video_id, {"video_id": video_id}) for video_id in favs if isinstance(video_id, str)]
            else:
                continue

            for fav_key, fav_data in entries:
                tag = fav_data.get("tag") if isinstance(fav_data, dict) else None
                idx = fav_data.get("index") if isinstance(fav_data, dict) else None
                file_id = fav_data.get("file_id") if isinstance(fav_data, dict) else None
                video_id = fav_data.get("video_id") if isinstance(fav_data, dict) else fav_key

                # Attempt to parse tag/index if not present
                if (tag is None or idx is None) and isinstance(video_id, str) and "_" in video_id:
                    try:
                        t, idx_str = video_id.rsplit("_", 1)
                        idx_int = int(idx_str)
                        tag = t
                        idx = idx_int
                    except Exception:
                        pass

                # Try to lookup using safe_get_media_item when tag/index found
                if tag and idx is not None:
                    media_item, err = safe_get_media_item(tag, idx)
                    if not err and media_item and isinstance(media_item, dict):
                        fid = media_item.get("file_id")
                        if fid:
                            # Record using file id and tag/index
                            if fid not in metadata_map:
                                record_video_metadata(tag, idx, fid, f"{tag}_{idx}")
                                made_update = True
                            else:
                                # Ensure tag/index info persisted for file_id
                                record_video_metadata(tag, idx, fid, f"{tag}_{idx}")
                        else:
                            # Ensure video-id key is recorded even if no file id
                            if f"{tag}_{idx}" not in metadata_map:
                                record_video_metadata(tag, idx, None, f"{tag}_{idx}")
                                made_update = True
                else:
                    # If no tag/index was found, treat fav_key as a file_id; attempt to find media for it
                    # Try to find a matching media entry in media_data by file_id
                    guessed = False
                    if isinstance(fav_key, str) and not ("_" in fav_key):
                        fid = fav_key
                        for t, v in media_data.items():
                            if isinstance(v, list):
                                for idx_i, item in enumerate(v):
                                    if isinstance(item, dict) and item.get("file_id") == fid:
                                        if fid not in metadata_map:
                                            record_video_metadata(t, idx_i, fid, f"{t}_{idx_i}")
                                            made_update = True
                                        guessed = True
                                        break
                                if guessed:
                                    break

        if made_update:
            try:
                save_favorites()
            except Exception:
                pass
        return True
    except Exception as e:
        print(f"[update_video_metadata_for_all_favorites] Error: {e}")
        return False


# Immediately update metadata for any loaded favorite entries to ensure the map stays useful
update_video_metadata_for_all_favorites()


def normalize_video_likes_counts():
    """Normalize favorites_data['video_likes'] so counts are keyed by file_id where available.
    This merges counts for any video_id (tag_idx) keys into corresponding file_id keys.
    Returns a dict of normalized counts and whether any changes were made.
    """
    try:
        likes_map = favorites_data.setdefault("video_likes", {})
        new_map = {}
        made_update = False
        for key, count in list(likes_map.items()):
            # Try to get metadata for this key
            metadata = get_video_metadata(key)
            if metadata and metadata.get("file_id"):
                fid = metadata.get("file_id")
                new_map[fid] = new_map.get(fid, 0) + count
                if fid != key:
                    made_update = True
            else:
                # Keep the original if no mapping to file_id
                new_map[key] = new_map.get(key, 0) + count
        if made_update or new_map != likes_map:
            favorites_data["video_likes"] = new_map
            try:
                save_favorites()
            except Exception:
                pass
        return True
    except Exception as e:
        print(f"[normalize_video_likes_counts] Error: {e}")
        return False


normalize_video_likes_counts()


def recalc_video_likes_from_favorites():
    """Recalculate video_likes counts from user_favorites and persist.
    This ensures counts match the actual number of users who have each favorite.
    """
    try:
        new_map = {}
        user_map = favorites_data.get("user_favorites", {})
        for user_id, favs in user_map.items():
            # Normalize to dict
            if isinstance(favs, list):
                favs_map = {}
                for vid in favs:
                    if isinstance(vid, str):
                        favs_map[vid] = {"video_id": vid}
                favs = favs_map
            if not isinstance(favs, dict):
                continue
            for fav_key, fav_data in favs.items():
                if not isinstance(fav_data, dict):
                    # fallback: key is video_id
                    key = fav_key
                else:
                    key = fav_data.get("file_id") or fav_data.get("video_id") or fav_key
                new_map[key] = new_map.get(key, 0) + 1

        # Replace and persist
        favorites_data["video_likes"] = new_map
        try:
            save_favorites()
        except Exception:
            pass
        return True
    except Exception as e:
        print(f"[recalc_video_likes_from_favorites] Error: {e}")
        return False


def find_media_by_file_id(file_id: str):
    """Find the media item by file_id across all tags. Returns (tag, idx, item) or (None, None, None).
    This is used by /getfile to quickly locate a media resource by its Telegram file_id.
    """
    try:
        for tag, videos in media_data.items():
            if not isinstance(videos, list):
                continue
            for idx, item in enumerate(videos):
                if isinstance(item, dict) and item.get("file_id") == file_id:
                    return tag, idx, item
        return None, None, None
    except Exception as e:
        print(f"[find_media_by_file_id] Error: {e}")
        return None, None, None


if os.path.exists(USERS_FILE):
    with open(USERS_FILE, "r") as f:
        try:
            users_data = json.load(f)
        except json.JSONDecodeError:
            users_data = {}

if os.path.exists(RANDOM_STATE_FILE):
    with open(RANDOM_STATE_FILE, "r") as f:
        try:
            random_state = json.load(f)
            # Ensure the structure exists
            if "shown_videos" not in random_state:
                random_state["shown_videos"] = []
            if "all_videos" not in random_state:
                random_state["all_videos"] = []
        except json.JSONDecodeError:
            random_state = {"shown_videos": [], "all_videos": []}

if os.path.exists(CAPTION_CONFIG_FILE):
    with open(CAPTION_CONFIG_FILE, "r") as f:
        try:
            caption_config = json.load(f)
            # Ensure the structure exists
            if "global_caption" not in caption_config:
                caption_config["global_caption"] = ""
            if "replacements" not in caption_config or not isinstance(caption_config.get("replacements"), list):
                caption_config["replacements"] = []
            if "link_override_enabled" not in caption_config:
                caption_config["link_override_enabled"] = False
            if "link_override_url" not in caption_config:
                caption_config["link_override_url"] = ""
        except json.JSONDecodeError:
            caption_config = {"global_caption": "", "replacements": [], "link_override_enabled": False, "link_override_url": ""}

# Load admin list
if os.path.exists(ADMIN_LIST_FILE):
    with open(ADMIN_LIST_FILE, "r") as f:
        try:
            admin_list = json.load(f)
            # Ensure main admin is always in the list
            if ADMIN_ID not in admin_list:
                admin_list.append(ADMIN_ID)
        except json.JSONDecodeError:
            admin_list = [ADMIN_ID]
else:
    admin_list = [ADMIN_ID]

# Load deleted media storage
if os.path.exists(DELETED_MEDIA_FILE):
    with open(DELETED_MEDIA_FILE, "r") as f:
        try:
            deleted_media_storage = json.load(f)
        except json.JSONDecodeError:
            deleted_media_storage = {}

# Load autodelete configuration
load_autodelete_config()

# Initialize backup manager (optional local backups)
if LOCAL_BACKUPS_ENABLED:
    try:
        backup_manager = DatabaseBackupManager(data_dir=".", backup_dir=LOCAL_BACKUP_DIR)
    except Exception as e:
        print(f"⚠️ Failed to initialize local backup manager: {e}")
        backup_manager = None
else:
    backup_manager = None

def safe_get_media_item(tag: str, idx: int):
    """Unified, defensive media accessor.
    Handles list or dict containers (numeric-string keyed). Returns (item, error_msg).
    """
    # Force idx to int if it's a string
    try:
        idx = int(idx)
    except (ValueError, TypeError):
        return None, f"index must be numeric, got {type(idx).__name__}"
    
    container = media_data.get(tag)
    if container is None:
        return None, f"tag '{tag}' not found"
    try:
        if isinstance(container, dict):
            # Normalize numeric keys to list in-place once
            numeric_keys = [k for k in container.keys() if isinstance(k, str) and k.isdigit()]
            if numeric_keys and len(numeric_keys) == len(container):
                ordered = [container[k] for k in sorted(numeric_keys, key=lambda k: int(k))]
                media_data[tag] = ordered  # cache normalized list
                container = ordered
        if not isinstance(container, list):
            return None, f"tag '{tag}' container not list after normalization (type={type(container).__name__})"
        if idx < 0 or idx >= len(container):
            return None, f"index {idx} out of range for tag '{tag}' (len={len(container)})"
        item = container[idx]
        if not isinstance(item, dict):
            return None, f"item at {tag}[{idx}] not dict (type={type(item).__name__})"
        return item, None
    except Exception as e:
        return None, f"exception accessing {tag}[{idx}]: {e}"

def normalize_all_media_containers():
    """Normalize any tag container stored as a dict of numeric-string keys into a list.
    Keeps ordering, ensures downstream list indexing works universally.
    Safe to call multiple times. Only transforms pure numeric-key dicts.
    """
    transformed = 0
    for tag, container in list(media_data.items()):
        if isinstance(container, dict):
            numeric_keys = [k for k in container.keys() if isinstance(k, str) and k.isdigit()]
            if numeric_keys and len(numeric_keys) == len(container):
                try:
                    ordered = [container[k] for k in sorted(numeric_keys, key=lambda k: int(k))]
                    media_data[tag] = ordered
                    transformed += 1
                except Exception as e:
                    print(f"[normalize] failed to transform tag '{tag}': {e}")
    if transformed:
        print(f"[normalize] transformed {transformed} tag container(s) to list form")


def register_active_top_viewer(chat_id: int, message_id: int, data: dict):
    """Register an active Top Viewer message so it can be updated when likes change.
    Data should include: video_id (canonical or tag_idx), tag, index, media_type, range_limit, page, reply_markup, user_id (if any)
    """
    try:
        key = (int(chat_id), int(message_id))
        active_top_viewers[key] = data
    except Exception:
        pass


def unregister_active_top_viewer(chat_id: int, message_id: int):
    try:
        key = (int(chat_id), int(message_id))
        active_top_viewers.pop(key, None)
    except Exception:
        pass


async def refresh_all_active_top_viewers(context: ContextTypes.DEFAULT_TYPE, batch_size: int = 15, delay: float = 0.25):
    """Refresh captions and favorite buttons for all active Top Videos viewer messages.
    Batch updates to avoid hitting Telegram rate limits.
    """
    try:
        items = list(active_top_viewers.items())
        if not items:
            return True
        count = 0
        for (chat_id, message_id), meta in items:
            try:
                video_key = meta.get("video_key") or meta.get("video_id")
                # Resolve metadata
                metadata = get_video_metadata(video_key)
                if not metadata and isinstance(video_key, str) and "_" in video_key:
                    metadata = get_video_metadata(video_key)
                if metadata:
                    tag = metadata.get("tag")
                    idx = metadata.get("index")
                    file_id = metadata.get("file_id")
                else:
                    tag = meta.get("tag")
                    idx = meta.get("idx")
                    file_id = meta.get("file_id")

                canonical_key = file_id if file_id else video_key
                likes = get_likes_count(canonical_key)
                ranks = get_sorted_canonical_likes()
                rank = None
                for i, (k, v) in enumerate(ranks, start=1):
                    if k == canonical_key:
                        rank = i
                        break
                if rank is None:
                    # If not found among ranks, skip updating
                    continue

                total = len(ranks)
                range_limit = meta.get("range_limit")
                if range_limit:
                    header = f"🔥 <b>Top {range_limit} | Video #{rank} | ❤️ {likes} likes</b>\n\n"
                else:
                    header = f"🔥 <b>Top Video #{rank} of {total} | ❤️ {likes} likes</b>\n\n"

                # Build the rest of the caption (basic metadata)
                share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                caption_body = f"📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_link}'>🔗 Link</a>"
                new_caption = header + caption_body

                # Update reply_markup to toggle favorite button correctly for viewer's user_id
                reply_markup = meta.get("reply_markup")
                user_id = meta.get("user_id")
                if user_id:
                    user_id_str = str(user_id)
                else:
                    user_id_str = None
                is_fav = False
                if user_id_str:
                    is_fav = is_video_favorited(user_id_str, tag, idx, file_id=file_id)
                if reply_markup:
                    try:
                        new_markup = update_favorite_button_in_keyboard(reply_markup, meta.get("video_id") or video_key, is_adding=not is_fav)
                    except Exception:
                        new_markup = reply_markup
                else:
                    new_markup = None

                # Attempt to edit message caption
                try:
                    if new_markup:
                        await context.bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=new_caption, parse_mode=ParseMode.HTML, reply_markup=new_markup)
                    else:
                        await context.bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=new_caption, parse_mode=ParseMode.HTML)
                except Exception as e:
                    # Remove mapping if message no longer exists or can't be edited
                    try:
                        errmsg = str(e)
                        if "not found" in errmsg.lower() or "chat not found" in errmsg.lower() or "message can't be edited" in errmsg.lower():
                            unregister_active_top_viewer(chat_id, message_id)
                    except Exception:
                        pass

            except Exception:
                pass

            count += 1
            if count % batch_size == 0:
                await asyncio.sleep(delay)
        return True
    except Exception:
        return False


def save_media():
    with open(MEDIA_FILE, "w") as f:
        json.dump(media_data, f, indent=2)
    # Note: Auto-backup is handled by periodic backup task


def save_exempted():
    with open(EXEMPTED_FILE, "w") as f:
        json.dump(list(exempted_users), f, indent=2)
    # Note: Auto-backup is handled by periodic backup task


def save_active_links():
    with open(ACTIVE_LINKS_FILE, "w") as f:
        json.dump(active_links, f, indent=2)


def save_passed_links():
    with open(PASSED_LINKS_FILE, "w") as f:
        json.dump(passed_links, f, indent=2)


def save_off_limits():
    with open(OFF_LIMITS_FILE, "w") as f:
        json.dump(off_limits_data, f, indent=2)


def save_favorites():
    with open(FAVORITES_FILE, "w") as f:
        json.dump(favorites_data, f, indent=2)
    # Note: Auto-backup is handled by periodic backup task


def sanitize_passed_links_remove_empty_tags():
    """Remove tags that have zero valid media from passed_links.json.

    A tag is considered valid if at least one referenced key points to an
    existing non-deleted, non-revoked media item with a usable file_id of
    type video/photo. This keeps the Switch-cycle clean (e.g., removes 'rd2'
    if it has no usable media).
    """
    global passed_links
    try:
        # Load fresh list from disk to avoid stale memory if edited elsewhere
        if os.path.exists(PASSED_LINKS_FILE):
            with open(PASSED_LINKS_FILE, 'r') as f:
                current = json.load(f)
        else:
            current = passed_links if isinstance(passed_links, list) else []

        # Build validity per tag
        valid_count = {}
        all_tags = set()
        for key in current:
            if '_' not in key:
                continue
            tag, idx_str = key.rsplit('_', 1)
            all_tags.add(tag)
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            if tag not in media_data or not isinstance(media_data[tag], list):
                continue
            if not (0 <= idx < len(media_data[tag])):
                continue
            item = media_data[tag][idx]
            # Skip tombstones and revoked
            if isinstance(item, dict) and item.get('deleted'):
                continue
            if isinstance(item, dict) and item.get('revoked'):
                continue
            file_id = item.get('file_id') if isinstance(item, dict) else None
            mtype = item.get('type') if isinstance(item, dict) else None
            if file_id and mtype in ("video", "photo"):
                valid_count[tag] = valid_count.get(tag, 0) + 1

        # Tags with zero valid items
        empty_tags = {t for t in all_tags if valid_count.get(t, 0) == 0}

        if not empty_tags:
            return {"removed": 0, "empty_tags": []}

        # Produce a filtered list without any entries from empty tags
        filtered = [k for k in current if (k.split('_', 1)[0] not in empty_tags)]

        # Save to disk and update memory
        with open(PASSED_LINKS_FILE, 'w') as f:
            json.dump(filtered, f, indent=2)
        passed_links = filtered

        print(f"🧹 Cleaned empty tags from passed_links: removed {len(empty_tags)} -> {sorted(empty_tags)}")
        return {"removed": len(empty_tags), "empty_tags": sorted(empty_tags)}
    except Exception as e:
        print(f"❌ Error sanitizing passed_links: {e}")
        return {"removed": 0, "empty_tags": [], "error": str(e)}


def save_random_state():
    with open(RANDOM_STATE_FILE, "w") as f:
        json.dump(random_state, f, indent=2)


def save_deleted_media():
    with open(DELETED_MEDIA_FILE, "w") as f:
        json.dump(deleted_media_storage, f, indent=2)


def save_caption_config():
    with open(CAPTION_CONFIG_FILE, "w") as f:
        json.dump(caption_config, f, indent=2)


# ================== CAPTION MANAGEMENT COMMANDS ==================
async def set_global_caption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_global_caption <text> — set a global caption for all files"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return

    # Extract full text after command to preserve spaces/newlines
    text = update.message.text
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("❌ Usage: /set_global_caption <text>")
        return
    caption_text = parts[1]

    caption_config["global_caption"] = caption_text
    save_caption_config()
    await update.message.reply_text("✅ Global caption updated.")


async def add_replacement_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/add_replacement <find> | <replace> — add a caption replacement rule"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return

    raw = update.message.text
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or "|" not in parts[1]:
        await update.message.reply_text("❌ Usage: /add_replacement <find> | <replace>")
        return

    find_replace = parts[1].split("|", 1)
    find = find_replace[0].strip()
    replace = find_replace[1].strip()

    if not find:
        await update.message.reply_text("❌ 'find' text cannot be empty.")
        return

    caption_config.setdefault("replacements", []).append({"find": find, "replace": replace})
    save_caption_config()
    await update.message.reply_text(f"✅ Added replacement #{len(caption_config['replacements'])}: '\u200b{find}' → '\u200b{replace}'")


async def list_replacements_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/list_replacements — list all caption replacement rules"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return

    rules = caption_config.get("replacements", [])
    if not rules:
        await update.message.reply_text("📄 No replacement rules configured.")
        return

    lines = ["📝 <b>Caption Replacement Rules</b>\n"]
    for i, r in enumerate(rules, 1):
        lines.append(f"{i}. <code>{r.get('find','')}</code> → <code>{r.get('replace','')}</code>")
    await update.message.reply_html("\n".join(lines))


async def remove_replacement_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/remove_replacement <index> — remove a rule by its index (from /list_replacements)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return

    if not context.args:
        await update.message.reply_text("❌ Usage: /remove_replacement <index>")
        return

    try:
        idx = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Index must be a number.")
        return

    rules = caption_config.get("replacements", [])
    if idx < 1 or idx > len(rules):
        await update.message.reply_text("❌ Invalid index.")
        return

    removed = rules.pop(idx - 1)
    caption_config["replacements"] = rules
    save_caption_config()
    await update.message.reply_text(f"✅ Removed rule: '{removed.get('find','')}' → '{removed.get('replace','')}'")


async def caption_config_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/caption_config — show current caption configuration"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return

    rules = caption_config.get("replacements", [])
    msg = [
        "🧾 <b>Caption Configuration</b>",
        "",
        "<b>Global Caption:</b>",
        f"<pre>{(caption_config.get('global_caption') or '').strip() or '— (empty) —'}</pre>",
        "",
        f"<b>Replacements:</b> {len(rules)} rule(s)"
    ]
    if rules:
        for i, r in enumerate(rules, 1):
            msg.append(f"{i}. <code>{r.get('find','')}</code> → <code>{r.get('replace','')}</code>")

    await update.message.reply_html("\n".join(msg))


# ================== LINK OVERRIDE COMMANDS ==================
async def set_link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_link <url> — enable override and set target URL"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    text = update.message.text
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("❌ Usage: /set_link <url>")
        return
    url = parts[1].strip()
    caption_config["link_override_enabled"] = True
    caption_config["link_override_url"] = url
    save_caption_config()
    await update.message.reply_text(f"✅ Link override enabled. All links will point to: {url}")


async def link_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/link_off — disable link override"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    caption_config["link_override_enabled"] = False
    save_caption_config()
    await update.message.reply_text("✅ Link override disabled.")


async def link_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/link_status — show link override status"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    enabled = caption_config.get("link_override_enabled", False)
    url = caption_config.get("link_override_url", "")
    status = "🟢 ENABLED" if enabled else "🔴 DISABLED"
    msg = f"🔗 <b>Link Override</b>\nStatus: {status}\nTarget: <code>{url or '—'}</code>"
    await update.message.reply_html(msg)


def save_admin_list():
    with open(ADMIN_LIST_FILE, "w") as f:
        json.dump(admin_list, f, indent=2)

def save_push_changes():
    """Persist PUSH change history to disk so it survives redeploys."""
    try:
        payload = {
            "seq": push_change_seq,
            "order": push_changes_order,
            "changes": list(push_changes.values()),
            "saved_at": datetime.now().isoformat(),
        }
        with open(PUSH_CHANGES_FILE, "w") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"❌ Error saving push changes: {e}")

def load_push_changes():
    """Load persisted PUSH change history if available."""
    global push_change_seq, push_changes, push_changes_order
    try:
        if os.path.exists(PUSH_CHANGES_FILE):
            with open(PUSH_CHANGES_FILE, "r") as f:
                data = json.load(f)
            # Rebuild in-memory structures
            push_change_seq = int(data.get("seq", 0))
            push_changes_order = [int(x) for x in data.get("order", [])]
            push_changes = {}
            for entry in data.get("changes", []):
                try:
                    cid = int(entry.get("id"))
                except Exception:
                    continue
                push_changes[cid] = entry
            # Basic sanity: drop ids missing entries
            push_changes_order = [cid for cid in push_changes_order if cid in push_changes]
            print(f"✅ Loaded {len(push_changes_order)} PUSH changes from disk")
        else:
            # Nothing to load — start fresh
            push_change_seq = 0
            push_changes = {}
            push_changes_order = []
    except Exception as e:
        print(f"❌ Error loading push changes: {e}")
        push_change_seq = 0
        push_changes = {}
        push_changes_order = []


async def auto_backup_data(data_type: str):
    """Automatically backup data after changes"""
    try:
        # Create a backup with a descriptive name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"auto_{data_type}_{timestamp}"
        description = f"Auto-backup after {data_type} data changes"
        
        success, message = await asyncio.get_event_loop().run_in_executor(
            None, backup_manager.create_backup, backup_name, description
        )
        
        if success:
            print(f"✅ Auto-backup created: {backup_name}")
        else:
            print(f"❌ Auto-backup failed: {message}")
            
    except Exception as e:
        print(f"❌ Auto-backup error: {e}")


def is_admin(user_id):
    """Check if a user is an admin"""
    # Convert user_id to int for consistency
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        return False
    
    # Make sure admin_list contains integer IDs
    return user_id in [int(admin) for admin in admin_list]


def track_user_operation(user_id, operation_type, operation_data=None):
    """Track an ongoing operation for a user"""
    user_operations[user_id] = {
        "type": operation_type,
        "data": operation_data,
        "cancelled": False
    }


def cancel_user_operation(user_id):
    """Cancel ongoing operation for a user"""
    if user_id in user_operations:
        user_operations[user_id]["cancelled"] = True
        return True
    return False


async def refresh_message_caption_likes(query, video_id):
    """If the message is a 'Top Video' or similar viewer for the provided video_id, update the likes count in its caption.
    Uses the canonical likes map and video metadata to recompute rank and rebuild the caption header.
    This is best-effort and ignores errors.
    """
    try:
        if not query or not getattr(query, 'message', None):
            return
        msg = query.message
        # Message must have a caption to edit
        if not getattr(msg, 'caption', None):
            return
        # Only update if current caption looks like Top video or contains likes header
        cap = msg.caption
        if 'Top Video' not in cap and 'Top' not in cap and 'Top ' not in cap:
            # Not a top video-like caption; do not edit
            return

        metadata = get_video_metadata(video_id)
        if not metadata:
            # Try backing out tag idx if video_id is direct form
            if '_' in video_id:
                parts = video_id.rsplit("_", 1)
                if len(parts) == 2:
                    t = parts[0]; idx = parts[1]
                    metadata = get_video_metadata(video_id)
        if metadata:
            tag = metadata.get('tag')
            idx = metadata.get('index')
            file_id = metadata.get('file_id')
        else:
            tag = None; idx = None; file_id = None

        canonical_key = file_id if file_id else video_id
        new_likes = get_likes_count(canonical_key)

        # Compute rank among sorted likes if available
        ranks = get_sorted_canonical_likes()
        rank = None
        for i, (k, v) in enumerate(ranks, start=1):
            if k == canonical_key:
                rank = i
                break
        if rank is None:
            # Not found; skip
            return

        # Build header
        cap_header = f"🔥 <b>Top Video #{rank} | ❤️ {new_likes} likes</b>\n\n"

        # Replace header: find first empty line
        parts = cap.split('\n\n', 1)
        if len(parts) == 2:
            new_caption = cap_header + parts[1]
        else:
            new_caption = cap_header

        try:
            await msg.edit_caption(caption=new_caption, parse_mode=ParseMode.HTML, reply_markup=msg.reply_markup)
        except Exception:
            # Fallback: try editing text if any
            try:
                await msg.edit_text(new_caption, parse_mode=ParseMode.HTML, reply_markup=msg.reply_markup)
            except Exception:
                pass
    except Exception:
        pass


def is_operation_cancelled(user_id):
    """Check if user's operation is cancelled"""
    cancelled = user_operations.get(user_id, {}).get("cancelled", False)
    return cancelled


def clear_user_operation(user_id):
    """Clear user's operation tracking"""
    user_operations.pop(user_id, None)


async def delete_media_entry(query, context: ContextTypes.DEFAULT_TYPE, video_key: str):
    """Delete a media item WITHOUT shrinking the list (index-stable).
    The deleted item is marked as a tombstone at the same index and also recorded
    to deleted_media_storage for listing/restore. Restores will replace the tombstone
    in-place when possible (no shifting).
    """
    global deleted_media_storage
    
    try:
        if "_" not in video_key:
            print(f"ERROR: Invalid video_key format: {video_key}")
            await safe_answer_callback_query(query, "❌ Bad key", show_alert=False)
            return
            
        tag, idx_str = video_key.rsplit("_", 1)
        try:
            idx = int(idx_str)
        except ValueError:
            print(f"ERROR: Invalid index in video_key: {video_key}")
            await safe_answer_callback_query(query, "❌ Bad index", show_alert=False)
            return
            
        if tag not in media_data:
            print(f"ERROR: Tag '{tag}' not found in media_data")
            await safe_answer_callback_query(query, "❌ Tag not found", show_alert=False)
            return
            
        if not (0 <= idx < len(media_data[tag])):
            print(f"ERROR: Index {idx} out of range for tag '{tag}' (length: {len(media_data[tag])})")
            await safe_answer_callback_query(query, "❌ Index out of range", show_alert=False)
            return
        
        entry = media_data[tag][idx]
        if video_key in deleted_media_storage:
            print(f"WARNING: Media {video_key} already in deleted storage, but there's new media at this index.")
            print(f"This means the old deleted entry is stale. Cleaning it up and proceeding with fresh deletion.")
            
            # Remove the stale deleted entry
            del deleted_media_storage[video_key]
            save_deleted_media()
            
            # Continue with normal deletion process for the current media
        
        # Store the deleted item with its original position
        deleted_media_storage[video_key] = {
            "data": entry,
            "original_position": idx,
            "tag": tag,
            "deleted_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Replace the item with a tombstone to keep indices stable
        tomb = {"deleted": True, "data": entry}
        # Preserve type for display where helpful
        if isinstance(entry, dict) and "type" in entry:
            tomb["type"] = entry.get("type")
        media_data[tag][idx] = tomb
        
        save_media()
        save_deleted_media()
        update_random_state()
        
        # Saved data and updated random state silently
        
    # Update only admin portion of keyboard (preserve other buttons)
        try:
            if query.message and query.message.reply_markup:
                ik = query.message.reply_markup.inline_keyboard
                new_admin_rows = build_admin_control_row(video_key)
                filtered = []
                for row in ik:
                    if not row:
                        continue
                    cb = getattr(row[0], 'callback_data', '') or ''
                    if cb.startswith(('revoke_media_', 'del_media_', 'restore_media_')):
                        continue
                    filtered.append(row)
                # For deleted items, show restore button
                restore_row = [InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")]
                filtered.append(restore_row)
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(filtered))
            else:
                restore_row = [InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")]
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([restore_row]))
        except Exception as e:
            print(f"ERROR: Failed to update reply markup: {e}")
            pass
            
        await safe_answer_callback_query(query, "🗑️ Media deleted!", show_alert=False)
        
        # Send a notification message with options
        notification_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("♻️ Restore Now", callback_data=f"restore_media_{video_key}"),
                InlineKeyboardButton("📋 View Deleted List", callback_data="list_deleted_media")
            ]
        ])
        
        await query.message.reply_text(
            f"🗑️ <b>Media Deleted Successfully</b>\n\n"
            f"📁 Tag: <code>{tag}</code>\n"
            f"📊 Original Index: <code>{idx}</code>\n"
            f"🔒 Index preserved (no shifting)\n\n"
            f"💡 Use the buttons below to restore or view all deleted media.",
            parse_mode=ParseMode.HTML,
            reply_markup=notification_keyboard
        )
        
    except Exception as e:
        print(f"ERROR: Exception in delete_media_entry: {e}")
        await safe_answer_callback_query(query, f"❌ Error: {str(e)}", show_alert=True)
        raise


async def revoke_media_entry(query, video_key: str):
    """Mark media as revoked (skipped but restorable)."""
    if "_" not in video_key:
        await safe_answer_callback_query(query, "❌ Bad key", show_alert=False)
        return
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await safe_answer_callback_query(query, "❌ Bad index", show_alert=False)
        return
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        await safe_answer_callback_query(query, "❌ Not found", show_alert=False)
        return
    entry = media_data[tag][idx]
    if isinstance(entry, dict) and entry.get("deleted"):
        await safe_answer_callback_query(query, "❌ It's deleted", show_alert=False)
        return
    if isinstance(entry, dict) and entry.get("revoked"):
        await safe_answer_callback_query(query, "⚠️ Already revoked", show_alert=False)
        return
    
    # Get media type for the confirmation message
    media_type = "unknown"
    if isinstance(entry, dict):
        media_type = entry.get("type", "unknown")
        entry["revoked"] = True
    else:
        media_data[tag][idx] = {"data": entry, "revoked": True, "type": "unknown"}
    
    save_media()
    update_random_state()
    
    # Update only admin rows to show restore
    try:
        if query.message and query.message.reply_markup:
            ik = query.message.reply_markup.inline_keyboard
            new_admin_rows = build_admin_control_row(video_key)
            filtered = []
            for row in ik:
                if not row:
                    continue
                cb = getattr(row[0], 'callback_data', '') or ''
                if cb.startswith(('revoke_media_', 'del_media_', 'restore_media_')):
                    continue
                filtered.append(row)
            filtered += new_admin_rows
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(filtered))
        else:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(build_admin_control_row(video_key)))
    except Exception:
        pass
    
    await safe_answer_callback_query(query, "🛑 Revoked", show_alert=False)
    
    # Send beautiful confirmation message like the delete format
    confirmation_text = (
        f"🛑 <b>Media Revoked Successfully</b>\n\n"
        f"📂 Tag: <code>{tag}</code>\n"
        f"📍 Index: <code>{idx}</code>\n\n"
        f"💡 Use the buttons below to restore or view all revoked media."
    )
    
    keyboard = [
        [
            InlineKeyboardButton("♻️ Restore Now", callback_data=f"restore_media_{video_key}"),
            InlineKeyboardButton("📋 View Revoked List", callback_data="view_revoked_list")
        ]
    ]
    
    await query.message.reply_html(
        confirmation_text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def restore_media_entry(query, video_key: str):
    """Restore a deleted (tombstoned) or revoked media entry in-place, preserving index stability."""
    global deleted_media_storage

    if "_" not in video_key:
        await safe_answer_callback_query(query, "❌ Bad key", show_alert=False)
        return

    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await safe_answer_callback_query(query, "❌ Bad index", show_alert=False)
        return

    # First, handle deleted items stored in deleted_media_storage
    if video_key in deleted_media_storage:
        info = deleted_media_storage.pop(video_key, None)
        if not info:
            await safe_answer_callback_query(query, "❌ Not found", show_alert=False)
            return
        original_data = info.get("data")
        target_tag = info.get("tag", tag)
        original_pos = info.get("original_position", idx)

        media_data.setdefault(target_tag, [])
        lst = media_data[target_tag]
        # Extend list with tombstones to preserve index if needed
        while len(lst) <= original_pos:
            lst.append({"deleted": True, "data": {}})
        # Replace tombstone (or overwrite) with original data
        lst[original_pos] = original_data

        save_media(); save_deleted_media(); update_random_state()

        # Update admin controls on the message if available
        try:
            if query.message:
                if query.message.reply_markup:
                    ik = query.message.reply_markup.inline_keyboard
                    new_rows = build_admin_control_row(video_key)
                    filtered = []
                    for row in ik:
                        if not row:
                            continue
                        cb = getattr(row[0], 'callback_data', '') or ''
                        if cb.startswith(('revoke_media_', 'del_media_', 'restore_media_')):
                            continue
                        filtered.append(row)
                    filtered += new_rows
                    await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(filtered))
                else:
                    await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(build_admin_control_row(video_key)))
        except Exception:
            pass

        await safe_answer_callback_query(query, "♻️ Restored", show_alert=False)
        return

    # Next, handle revoked entries in-place
    if tag in media_data and 0 <= idx < len(media_data[tag]):
        entry = media_data[tag][idx]
        if isinstance(entry, dict) and entry.get("revoked"):
            if isinstance(entry, dict) and "data" in entry:
                media_data[tag][idx] = entry["data"]
            else:
                try:
                    # Remove just the flag
                    entry.pop("revoked", None)
                except Exception:
                    pass
            save_media(); update_random_state()

            # Update admin controls on the message if available
            try:
                if query.message:
                    if query.message.reply_markup:
                        ik = query.message.reply_markup.inline_keyboard
                        new_rows = build_admin_control_row(video_key)
                        filtered = []
                        for row in ik:
                            if not row:
                                continue
                            cb = getattr(row[0], 'callback_data', '') or ''
                            if cb.startswith(('revoke_media_', 'del_media_', 'restore_media_')):
                                continue
                            filtered.append(row)
                        filtered += new_rows
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(filtered))
                    else:
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(build_admin_control_row(video_key)))
            except Exception:
                pass

            await safe_answer_callback_query(query, "✅ Revoked media restored successfully!", show_alert=False)
            return

    await safe_answer_callback_query(query, "❌ Nothing to restore", show_alert=False)


async def fix_media_entry(query, context: ContextTypes.DEFAULT_TYPE, video_key: str):
    """Allow admin to fix corrupted media entry by providing correct data.

    This function attempts lightweight automatic repairs for common data corruption:
    - Missing 'type' field (guessed from file_id prefix; defaults to 'video')
    - If 'file_id' itself is missing, we cannot auto-fix and notify the admin.
    """
    if "_" not in video_key:
        await query.answer("❌ Bad key", show_alert=False)
        return

    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await query.answer("❌ Bad index", show_alert=False)
        return

    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        await query.answer("❌ Not found", show_alert=False)
        return

    item = media_data[tag][idx]

    fixed = False
    if isinstance(item, dict):
        # Add missing type if file_id present
        if "file_id" in item and "type" not in item:
            file_id = item["file_id"]
            try:
                if file_id.startswith("BAACAgI"):
                    item["type"] = "video"
                elif file_id.startswith("AgACAgI"):
                    item["type"] = "photo"
                elif file_id.startswith("BQACAgI"):
                    item["type"] = "document"
                elif file_id.startswith("CQACAgI"):
                    item["type"] = "audio"
                elif file_id.startswith("AwACAgI"):
                    item["type"] = "voice"
                elif file_id.startswith("CgACAgI"):
                    item["type"] = "animation"
                else:
                    item["type"] = "video"
                fixed = True
            except Exception:
                item["type"] = "video"
                fixed = True

        # If no file_id, we cannot fix
        if "file_id" not in item:
            await query.answer("❌ Can't auto-fix missing file_id", show_alert=True)
            return

    if fixed:
        save_media()
        await query.answer("✅ Auto-fixed!", show_alert=False)
        await query.message.reply_text(
            f"✅ <b>Media Entry Fixed!</b>\n\n"
            f"📁 Tag: <code>{tag}</code>\n"
            f"📍 Index: <code>{idx}</code>\n"
            f"🔧 Fixed missing fields automatically\n\n"
            f"You can now view this media normally.",
            parse_mode=ParseMode.HTML
        )
    else:
        await query.answer("❌ Cannot auto-fix this entry", show_alert=True)
        await query.message.reply_text(
            f"❌ <b>Cannot Auto-Fix Entry</b>\n\n"
            f"This media entry has issues that require manual intervention.\n"
            f"Consider deleting this corrupted entry and re-uploading the media.",
            parse_mode=ParseMode.HTML
        )


async def show_raw_media_data(query, context: ContextTypes.DEFAULT_TYPE, video_key: str):
    """Show the raw data of a media entry for debugging"""
    if "_" not in video_key:
        await query.answer("❌ Bad key", show_alert=False)
        return
    
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await query.answer("❌ Bad index", show_alert=False)
        return
    
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        await query.answer("❌ Not found", show_alert=False)
        return
    
    item = media_data[tag][idx]
    
    # Format the raw data for display
    try:
        raw_data = json.dumps(item, indent=2, ensure_ascii=False)
        if len(raw_data) > 3000:  # Telegram message limit consideration
            raw_data = raw_data[:3000] + "...\n(truncated)"
    except Exception as e:
        raw_data = f"Error serializing data: {e}\nRaw repr: {repr(item)}"
    
    await query.answer("📄 Raw data shown", show_alert=False)
    await query.message.reply_text(
        f"📄 <b>Raw Media Data</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📍 Index: <code>{idx}</code>\n"
        f"🗂️ Data Type: <code>{type(item).__name__}</code>\n\n"
        f"<pre>{raw_data}</pre>",
        parse_mode=ParseMode.HTML
    )

def build_admin_control_row(video_key: str, show_delete=True):
    """Return list of rows (each row list of buttons) for admin control based on state."""
    global deleted_media_storage
    
    if "_" not in video_key:
        return []
    
    # Check if this item is in deleted storage
    is_deleted = video_key in deleted_media_storage
    
    # If deleted, show restore button
    if is_deleted:
        return [[InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")]]
    
    # Check if item exists in current media_data
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        return []
    
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        # Item doesn't exist in current data, check if it's deleted
        if is_deleted:
            return [[InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")]]
        return []
    
    entry = media_data[tag][idx]
    # Check for revoked status
    is_revoked = isinstance(entry, dict) and entry.get("revoked")
    
    rows = []
    if is_revoked:
        # When revoked show single restore button
        rows.append([InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")])
    else:
        # Active state: show revoke + delete first, PUSH at the bottom as requested
        push_button = InlineKeyboardButton("🔄 PUSH", callback_data=f"p_{video_key}")
        row = [InlineKeyboardButton("🛑 Revoke", callback_data=f"revoke_media_{video_key}")]
        if show_delete:
            row.append(InlineKeyboardButton("🗑️ Delete", callback_data=f"del_media_{video_key}"))
        rows.append(row)
        rows.append([push_button])
    return rows





def update_favorite_button_in_keyboard(reply_markup, video_key, is_adding=True):
    """
    Updates only the favorite button in the reply_markup for toggle behavior.
    - is_adding=True: Changes to 💔 REMOVE (for add action)
    - is_adding=False: Changes to ❤️ ADD (for remove action)
    Preserves all other buttons and rows.
    """
    if not reply_markup or not reply_markup.inline_keyboard:
        return reply_markup
    
    # Determine new button properties
    if is_adding:
        new_text = "💔 Remove"
        new_callback = f"remove_fav_{video_key}"
        old_callback_prefix = f"add_fav_{video_key}"
    else:
        new_text = "❤️ Add"
        new_callback = f"add_fav_{video_key}"
        old_callback_prefix = f"remove_fav_{video_key}"
    
    # Create a copy of the keyboard to modify
    updated_keyboard = []
    for row in reply_markup.inline_keyboard:
        updated_row = []
        for button in row:
            # Check if this is the favorite button to toggle
            if hasattr(button, 'callback_data') and button.callback_data == old_callback_prefix:
                # Replace with toggled button
                updated_button = InlineKeyboardButton(new_text, callback_data=new_callback)
                updated_row.append(updated_button)
            else:
                # Keep other buttons unchanged
                updated_row.append(button)
        updated_keyboard.append(updated_row)
    
    return InlineKeyboardMarkup(updated_keyboard)



# ---- Helpers: split and send long HTML safely ----
def split_text_for_telegram_html(text: str, max_len: int = 3800) -> list[str]:
    """Split a long HTML message into chunks under Telegram limits.
    Splits on line boundaries to avoid breaking tags as much as possible.
    """
    if len(text) <= max_len:
        return [text]
    lines = text.splitlines(keepends=True)
    chunks: list[str] = []
    curr = ""
    for line in lines:
        # If a single line is extremely long, hard-split it
        if len(line) > max_len:
            while len(line) > 0:
                space = max_len - len(curr)
                if space <= 0:
                    if curr:
                        chunks.append(curr)
                    curr = ""
                    space = max_len
                curr += line[:space]
                line = line[space:]
                if len(curr) >= max_len:
                    chunks.append(curr)
                    curr = ""
            continue
        if len(curr) + len(line) > max_len:
            if curr:
                chunks.append(curr)
            curr = line
        else:
            curr += line
    if curr:
        chunks.append(curr)
    return chunks


async def send_long_html(context, chat_id: int, text: str):
    """Send HTML text, splitting into multiple messages if needed. Returns last message."""
    parts = split_text_for_telegram_html(text)
    last_msg = None
    total = len(parts)
    for i, part in enumerate(parts, 1):
        prefix = f"<b>Help (part {i}/{total})</b>\n\n" if total > 1 else ""
        last_msg = await context.bot.send_message(chat_id=chat_id, text=prefix + part, parse_mode=ParseMode.HTML)
    return last_msg




async def safe_send_message(context, chat_id, text=None, photo=None, video=None, document=None, 
                           audio=None, voice=None, animation=None, sticker=None, caption=None, 
                           reply_markup=None, parse_mode=None, protect_content=None, max_retries=5, auto_delete=True, user_id=None,
                           message_thread_id=None, chat_type=None):
    """Safely send a message with retry logic for handling timeouts and auto-deletion tracking"""
    # Use global protection setting if not explicitly specified
    if protect_content is None:
        protect_content = should_protect_content(user_id, chat_id)
    
    # Try to determine chat type if not provided
    if chat_type is None:
        # Determine chat type from chat_id
        if isinstance(chat_id, int):
            if chat_id > 0:
                chat_type = "private"  # Positive IDs are user IDs
            else:
                chat_type = "group"     # Negative IDs are group chats
                # Note: Can't distinguish between group and supergroup from just ID
        
    rate_key = user_id if user_id is not None else chat_id
    is_media_send = any([photo, video, document, audio, voice, animation, sticker])

    for attempt in range(max_retries):
        try:
            if is_media_send:
                await throttle_user_delivery(rate_key)

            message = None
            if photo:
                message = await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content,
                    message_thread_id=message_thread_id
                )
            elif video:
                message = await context.bot.send_video(
                    chat_id=chat_id,
                    video=video,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content,
                    message_thread_id=message_thread_id
                )
            elif document:
                message = await context.bot.send_document(
                    chat_id=chat_id,
                    document=document,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content,
                    message_thread_id=message_thread_id
                )
            elif audio:
                message = await context.bot.send_audio(
                    chat_id=chat_id,
                    audio=audio,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content,
                    message_thread_id=message_thread_id
                )
            elif voice:
                message = await context.bot.send_voice(
                    chat_id=chat_id,
                    voice=voice,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content,
                    message_thread_id=message_thread_id
                )
            elif animation:
                message = await context.bot.send_animation(
                    chat_id=chat_id,
                    animation=animation,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content,
                    message_thread_id=message_thread_id
                )
            elif sticker:
                message = await context.bot.send_sticker(
                    chat_id=chat_id,
                    sticker=sticker,
                    reply_markup=reply_markup,
                    protect_content=protect_content,
                    message_thread_id=message_thread_id
                )
            else:
                message = await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    message_thread_id=message_thread_id
                )
            
            # Track message for auto-deletion if it contains media
            if message and auto_delete and (photo or video or document or audio or voice or animation or sticker):
                await track_sent_message(message)

            if message and is_media_send:
                mark_user_delivery(rate_key, success=True, chat_id=chat_id)
            
            return message
                
        except TimedOut:
            # Record the timeout to adjust cooldown
            if chat_id is not None:
                consecutive_timeouts[chat_id] += 1
                
            if attempt < max_retries - 1:
                # Use more aggressive backoff for groups
                if chat_type in ["group", "supergroup"]:
                    wait_time = 1.5 ** (attempt + 2)  # More aggressive: ~3.4s, ~5.2s, ~8s
                else:
                    wait_time = 2 ** attempt  # Standard: 1s, 2s, 4s
                    
                print(f"Timeout on attempt {attempt + 1}, retrying in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                print(f"Failed to send message after {max_retries} attempts due to timeout")
                return None
                
        except RetryAfter as e:
            # Record the rate limiting to adjust cooldown
            if chat_id is not None:
                # More aggressively increase cooldown for flood control
                current = consecutive_timeouts.get(chat_id, 0)
                consecutive_timeouts[chat_id] = current + 1  # Less aggressive penalty for retry
                
                # Log the current flood control state
                print(f"Flood control: chat_id={chat_id}, consecutive_timeouts={consecutive_timeouts[chat_id]}")
                
            if attempt < max_retries - 1:
                # Add exponential backoff with the required retry time
                base_wait = e.retry_after
                extra_buffer = min(10, 2 ** attempt)  # Add increasing buffer up to 10 seconds
                wait_time = base_wait + extra_buffer
                
                print(f"Rate limited for {base_wait}s, waiting {wait_time}s total before retry (attempt {attempt + 1}/{max_retries})...")
                await asyncio.sleep(wait_time)
                continue
            else:
                print(f"Failed to send message after {max_retries} attempts due to rate limiting")
                return None
                
        except (NetworkError, BadRequest) as e:
            if attempt < max_retries - 1:
                wait_time = 1.5 ** (attempt + 1)  # Slightly gentler backoff for network issues
                print(f"Network/Request error on attempt {attempt + 1}/{max_retries}: {e}, retrying in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                print(f"Failed to send message after {max_retries} attempts due to: {e}")
                return None
                
        except Exception as e:
            print(f"Unexpected error sending message: {e}")
            return None
    
    return None


async def send_media_by_type(context, chat_id, item, caption, reply_markup, parse_mode=ParseMode.HTML, protect_content=None, user_id=None):
    """Helper function to send media using the correct method based on type"""
    # Use global protection setting if not explicitly specified
    if protect_content is None:
        protect_content = should_protect_content(user_id, chat_id)
        
    media_kwargs = {
        "context": context,
        "chat_id": chat_id,
        "caption": caption,
        "reply_markup": reply_markup,
        "parse_mode": parse_mode,
        "protect_content": protect_content
    }
    media_kwargs["user_id"] = user_id
    
    if item["type"] == "video":
        media_kwargs["video"] = item["file_id"]
    elif item["type"] == "photo":
        media_kwargs["photo"] = item["file_id"]
    elif item["type"] == "document":
        media_kwargs["document"] = item["file_id"]
    elif item["type"] == "audio":
        media_kwargs["audio"] = item["file_id"]
    elif item["type"] == "voice":
        media_kwargs["voice"] = item["file_id"]
    elif item["type"] == "animation":
        media_kwargs["animation"] = item["file_id"]
    elif item["type"] == "sticker":
        media_kwargs["sticker"] = item["file_id"]
        # Stickers don't support captions
        media_kwargs.pop("caption", None)
    else:
        # Fallback for unknown types
        media_kwargs["text"] = f"Unsupported media type: {item['type']}"
    
    return await safe_send_message(**media_kwargs)


def build_final_caption(original_caption="", add_global=True):
    """Build the final caption with global caption"""
    # Use original caption as-is
    processed_caption = original_caption if original_caption else ""
    
    # Add global caption if enabled
    global_caption = caption_config.get("global_caption", "") if add_global else ""
    
    # Combine captions
    if global_caption and processed_caption:
        final = f"{processed_caption}\n\n{global_caption}"
    elif global_caption:
        final = global_caption
    elif processed_caption:
        final = processed_caption
    else:
        final = ""

    # Apply caption replacements
    try:
        for rule in caption_config.get("replacements", []):
            find = str(rule.get("find", ""))
            repl = str(rule.get("replace", ""))
            if find:
                final = final.replace(find, repl)
    except Exception:
        pass

    # Apply link override if enabled (rewrite all hrefs and raw URLs)
    try:
        if caption_config.get("link_override_enabled") and caption_config.get("link_override_url"):
            import re
            target = caption_config.get("link_override_url")
            # Replace href attributes
            final = re.sub(r"href=([\'\"])https?://[^\'\"]+\1", lambda m: f"href={m.group(1)}{target}{m.group(1)}", final)
            # Replace raw URLs not inside angle brackets (best-effort)
            final = re.sub(r"https?://[^\s<]+", target, final)
    except Exception:
        pass

    return final


def build_media_caption(original_caption="", tag="", index="", share_link="", media_type="video"):
    """Build caption specifically for media files (videos/photos) sent to users"""
    # Create the base system caption with metadata based on media type
    if media_type == "video":
        base_caption = f"🎬 <b>Shared Video</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "photo":
        base_caption = f"🖼️ <b>Shared Photo</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "document":
        base_caption = f"📄 <b>Shared Document</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "audio":
        base_caption = f"🎵 <b>Shared Audio</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "voice":
        base_caption = f"🎤 <b>Shared Voice</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "animation":
        base_caption = f"🎞️ <b>Shared Animation</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "sticker":
        base_caption = f"🎭 <b>Shared Sticker</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    else:
        # Generic fallback for any other type
        base_caption = f"📎 <b>Shared File</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    
    # Apply global caption
    return build_final_caption(base_caption, add_global=True)


# Global helper: provide a Home keyboard accessible from any function
# Global helper: build the default reply keyboard shown on /start
def build_initial_reply_keyboard():
    """Return the baseline reply keyboard shared with users on the welcome flow."""
    return ReplyKeyboardMarkup(
        [
            ["🎬 Random (18-)", "🏴‍☠️ Off Limits"] ,
            ["⭐ My Favorites"]
        ],
        resize_keyboard=True
    )


def get_home_keyboard(user_id):
    """Return a quick-access Home keyboard. This global helper mirrors the
    locally-scoped variant used in handlers so functions like top_videos_command
    can safely reference it."""
    try:
            # Everyone (including admins) sees the same normal user keyboard
            # Admin commands are accessible via /help only
            return ReplyKeyboardMarkup([
                ["🎬 GET FILES", "🎲 RANDOM MEDIA"],
                ["⭐ MY FAVORITES", "🔥 TOP VIDEOS"]
            ], resize_keyboard=True)
    except Exception:
        # As a safe fallback, return None so callers can omit reply_markup
        return None


# ================== INLINE KEYBOARD BUILDERS (Dynamic) ==================
def build_random_inline_keyboard(video_key: str, mode: str, is_favorited: bool, is_admin_user: bool = False):
    """Build a dynamic inline keyboard for random content.
    mode: 'safe' (18-) or 'adult' (18+)
    """
    # video_key format: tag_index, but for individual sharing we need tag_index_index
    if "_" in video_key:
        tag, idx = video_key.rsplit("_", 1)
        share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
    else:
        share_link = f"https://t.me/{BOT_USERNAME}?start={video_key}"
    share_url = f"https://t.me/share/url?url={share_link}"

    tag, idx = ("unknown", "0")
    if "_" in video_key:
        parts = video_key.rsplit("_", 1)
        tag, idx = parts[0], parts[1]

    # Favorite toggle button
    if is_favorited:
        fav_btn = InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_key}")
    else:
        fav_btn = InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_key}")

    # Tag switching logic (single switch button cycles through tags)
    # Build tag list from passed_links.json and EXCLUDE the special 'random' tag
    try:
        import json
        with open('passed_links.json', 'r') as f:
            _passed_links_list = json.load(f)
        _all_tags = []
        _seen = set()
        for key in _passed_links_list:
            if '_' in key:
                t = key.rsplit('_', 1)[0]
                # 'random' is a safe-only special tag; never include it in adult cycle
                if t not in _seen and t != 'random':
                    _all_tags.append(t)
                    _seen.add(t)
        tag_list = _all_tags if _all_tags else []
    except Exception:
        tag_list = []

    # Determine current tag for keyboard state
    # If adult mode, lock to the actual tag of current video
    if mode == "adult" and tag not in ("unknown", "0"):
        current_tag = tag
    elif mode == "safe":
        current_tag = "random"
    else:
        current_tag = mode

    # Next button
    if mode == "safe":
        # Safe mode must never route through passed_links. Keep it in true safe flow.
        next_btn = InlineKeyboardButton("⏭️ Next (18-)", callback_data="random_safe")
    else:
        # Tag-specific/adult browsing sticks to current_tag
        next_btn = InlineKeyboardButton(f"⏭️ Next ({current_tag})", callback_data=f"next_tag_{current_tag}")

    # Compute next tag in cycle (fallback to first tag)
    if not tag_list:
        next_tag = current_tag  # no-op if no tags available
    else:
        if current_tag in tag_list:
            idx = tag_list.index(current_tag)
            next_tag = tag_list[(idx + 1) % len(tag_list)]
        else:
            next_tag = tag_list[0]

    # Single switch button cycles to next tag (only if we have adult tags)
    switch_btn = None
    if tag_list:
        switch_btn = InlineKeyboardButton(f"🔀 Switch to {next_tag}", callback_data=f"switch_to_tag_{next_tag}")

    share_btn = InlineKeyboardButton("🔗 Share", url=share_url)
    # Shortened label per request
    favs_btn = InlineKeyboardButton("⭐ MY FAV", callback_data="view_favorites")

    # Layout tweak:
    # - Regular users: row1 [FavToggle, Next], row2 [FAV, Share], row3 [Switch]
    # - Admins:        row1 [FavToggle, Next], row2 [Switch, Share] (no FAV row)
    keyboard = [[fav_btn, next_btn]]
    if is_admin_user:
        if switch_btn is not None:
            keyboard.append([switch_btn, share_btn])
        else:
            keyboard.append([share_btn])
    else:
        keyboard.append([favs_btn, share_btn])
        if switch_btn is not None:
            keyboard.append([switch_btn])
    # Admin removal button
    if is_admin_user:
        keyboard += build_admin_control_row(video_key)
    return InlineKeyboardMarkup(keyboard)

    # (Unreachable duplicate kept consistent for clarity)
    share_btn = InlineKeyboardButton("🔗 Share", url=share_url)
    favs_btn = InlineKeyboardButton("⭐ MY FAV", callback_data="view_favorites")

    keyboard = [[fav_btn, next_btn]]
    if is_admin_user:
        if switch_btn is not None:
            keyboard.append([switch_btn, share_btn])
        else:
            keyboard.append([share_btn])
        keyboard += build_admin_control_row(video_key)
    else:
        keyboard.append([favs_btn, share_btn])
        if switch_btn is not None:
            keyboard.append([switch_btn])
    return InlineKeyboardMarkup(keyboard)


async def send_random_video(context: ContextTypes.DEFAULT_TYPE, chat_id: int, mode: str, user_id: int = None, silent_fail: bool = False):
    """Send a random video in given mode with dynamic buttons.
    
    Args:
        silent_fail: If True, skip silently when no videos available (no error message)
    """
    # Picker for a key based on mode/tag
    def _pick_key(m: str):
        if m == "safe":
            return get_random_from_tag_no_repeat("random")
        elif m == "adult":
            return get_random_from_passed_links()
        else:
            # Tag-specific browsing should respect passed_links (adult-approved) pool
            return get_random_from_passed_links_by_tag(m)

    # Initial candidate
    video_key = _pick_key(mode)

    if not video_key:
        # graceful error - skip silently if silent_fail is True
        if not silent_fail:
            if mode == "safe":
                await context.bot.send_message(chat_id, "❌ No media files available in random tag.")
            else:
                await context.bot.send_message(chat_id, "❌ No passed videos available. Admin needs to pass some videos first.")
        return

    # Robust selection loop: skip invalid/missing/revoked entries silently
    tried = set()
    valid = False
    for _ in range(12):  # try up to 12 candidates
        if not video_key or video_key in tried:
            video_key = _pick_key(mode)
        tried.add(video_key)
        if not video_key or "_" not in video_key:
            continue
        tag_idx = video_key.rsplit("_", 1)
        if len(tag_idx) != 2:
            continue
        tag, idx_str = tag_idx
        try:
            idx = int(idx_str)
        except ValueError:
            continue
        if tag not in media_data or not isinstance(media_data[tag], list) or not (0 <= idx < len(media_data[tag])):
            continue
        cand = media_data[tag][idx]
        if isinstance(cand, dict) and cand.get("revoked"):
            continue
        if not (isinstance(cand, dict) and cand.get("file_id") and cand.get("type") in ("video", "photo")):
            continue
        # Found a valid candidate
        video_data = cand
        valid = True
        break
    if not valid:
        if not silent_fail:
            await context.bot.send_message(chat_id, "❌ No available media found.")
        return
    user_id_str = str(chat_id)  # For favorites toggle (private chat context)
    is_favorited = user_id_str in favorites_data.get("user_favorites", {}) and video_key in favorites_data["user_favorites"].get(user_id_str, {})

    # For individual video sharing, use tag_index_index format
    share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
    share_url = f"https://t.me/share/url?url={share_link}"

    base_caption = ("🎬 <b>Random (18-)</b>" if mode == "safe" else "🎲 <b>Random (18+)</b>") \
        + f"\n\n📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_link}'>🔗 Link</a>"

    reply_markup = build_random_inline_keyboard(video_key, mode, is_favorited, is_admin_user=is_admin(chat_id))

    actual_user_id = user_id if user_id is not None else chat_id

    if video_data.get("type") == "video" and "file_id" in video_data:
        await send_and_track_message(context.bot.send_video, 
                                     chat_id=chat_id,
                                     video=video_data["file_id"],
                                     caption=build_final_caption(base_caption, add_global=False),
                                     parse_mode=ParseMode.HTML,
                                     protect_content=should_protect_content(actual_user_id, chat_id),
                                     reply_markup=reply_markup,
                                     user_id=actual_user_id)
    elif video_data.get("type") == "photo" and "file_id" in video_data:
        await send_and_track_message(context.bot.send_photo,
                                     chat_id=chat_id,
                                     photo=video_data["file_id"],
                                     caption=build_final_caption(base_caption, add_global=False),
                                     parse_mode=ParseMode.HTML,
                                     protect_content=should_protect_content(actual_user_id, chat_id),
                                     reply_markup=reply_markup,
                                     user_id=actual_user_id)
    else:
        await context.bot.send_message(chat_id, "❌ No valid media found for this entry.")


def save_users():
    with open(USERS_FILE, "w") as f:
        json.dump(users_data, f, indent=2)
    # Note: Auto-backup is handled by periodic backup task


def track_user(user_id, username=None, first_name=None, source="bot_interaction"):
    """Track a user interaction with the bot"""
    user_id_str = str(user_id)
    current_time = asyncio.get_event_loop().time()
    is_new_user = user_id_str not in users_data
    
    # Update or create user entry
    if user_id_str in users_data:
        users_data[user_id_str]["last_seen"] = current_time
        users_data[user_id_str]["username"] = username
        users_data[user_id_str]["first_name"] = first_name
        users_data[user_id_str]["interaction_count"] = users_data[user_id_str].get("interaction_count", 0) + 1
    else:
        users_data[user_id_str] = {
            "first_seen": current_time,
            "last_seen": current_time,
            "username": username,
            "first_name": first_name,
            "interaction_count": 1,
            "source": source
        }
    
    # Save to file
    save_users()
    
    return is_new_user


async def auto_register_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Automatically register any user who interacts with the bot"""
    if update.effective_user and not update.effective_user.is_bot:
        user = update.effective_user
        # Determine the source of interaction
        source = "bot_interaction"
        if update.message:
            if update.message.text and update.message.text.startswith('/start'):
                source = "start_command"
            elif update.message.photo or update.message.video:
                source = "media_upload"
            elif update.message.text:
                source = "text_message"
        elif update.callback_query:
            source = "button_click"
        
        # Track the user with the determined source
        is_new_user = track_user(user.id, user.username, user.first_name, source)
        
        # Only log new user registrations
        if is_new_user:
            log_counters['new_users'] += 1
            print_counter_line()


def migrate_existing_users():
    """Migrate existing users from favorites and exempted users to main users database"""
    current_time = asyncio.get_event_loop().time()
    
    # Migrate from favorites
    if favorites_data and "user_favorites" in favorites_data:
        for user_id_str in favorites_data["user_favorites"].keys():
            if user_id_str not in users_data:
                users_data[user_id_str] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "username": None,
                    "first_name": None,
                    "interaction_count": 1,
                    "source": "favorites_migration"
                }
    
    # Migrate from exempted users
    for user_id in exempted_users:
        user_id_str = str(user_id)
        if user_id_str not in users_data:
            users_data[user_id_str] = {
                "first_seen": current_time,
                "last_seen": current_time,
                "username": None,
                "first_name": None,
                "interaction_count": 1,
                "source": "exempted_migration"
            }
    
    # Save migrated data
    save_users()


def update_random_state():
    """Update the list of all available videos"""
    global random_state
    current_videos = []
    
    # Build list of all individual videos with tag_index format
    for tag, videos in media_data.items():
        if isinstance(videos, list):
            for idx, video in enumerate(videos):
                if isinstance(video, dict) and (video.get("deleted") or video.get("revoked")):
                    continue
                current_videos.append(f"{tag}_{idx}")
    
    # If we have new videos or the all_videos list is different, rebuild it
    if set(random_state["all_videos"]) != set(current_videos):
        random_state["all_videos"] = current_videos
        # Remove any shown videos that no longer exist
        random_state["shown_videos"] = [v for v in random_state["shown_videos"] if v in current_videos]
        save_random_state()


def get_next_random_video():
    """Get next random video ensuring all videos are shown before repeating"""
    global random_state
    
    update_random_state()
    
    if not random_state["all_videos"]:
        return None
    
    # If all videos have been shown, reset the shown list
    if len(random_state["shown_videos"]) >= len(random_state["all_videos"]):
        random_state["shown_videos"] = []
    
    # Get videos that haven't been shown yet
    available_videos = [v for v in random_state["all_videos"] if v not in random_state["shown_videos"]]
    
    if not available_videos:
        # This shouldn't happen, but just in case
        available_videos = random_state["all_videos"]
        random_state["shown_videos"] = []
    
    # Pick a random video from available ones
    selected_video = random.choice(available_videos)
    
    # Mark it as shown
    random_state["shown_videos"].append(selected_video)
    save_random_state()
    
    return selected_video


def get_random_from_tag_no_repeat(tag="random"):
    """Get random video from specific tag with TRUE no-repeat tracking (for GET FILES button)"""
    if tag not in media_data or not media_data[tag]:
        return None
    
    # Create list of all videos from this specific tag
    tag_videos = []
    for i, v in enumerate(media_data[tag]):
        if isinstance(v, dict) and (v.get("deleted") or v.get("revoked")):
            continue
        tag_videos.append(f"{tag}_{i}")
    
    if not tag_videos:
        return None
    
    # Initialize tracking system if not exists
    if 'tag_shown_videos' not in random_state:
        random_state['tag_shown_videos'] = {}
    
    if tag not in random_state['tag_shown_videos']:
        random_state['tag_shown_videos'][tag] = []
    
    # Get videos that haven't been shown yet for this tag
    shown_videos = random_state['tag_shown_videos'][tag]
    available_videos = [v for v in tag_videos if v not in shown_videos]
    
    # If all videos have been shown, reset the shown list for this tag
    if not available_videos:
        random_state['tag_shown_videos'][tag] = []
        available_videos = tag_videos
        log_counters['cycle_resets'] += 1
        print_counter_line()
    
    # Pick a random video from available ones
    selected_video = random.choice(available_videos)
    
    # Mark it as shown for this tag
    random_state['tag_shown_videos'][tag].append(selected_video)
    save_random_state()
    
    log_counters['random_selections'] += 1
    print_counter_line()
    return selected_video


def get_random_from_passed_links():
    """Get random video from passed links only (for RANDOM MEDIA button)"""
    if not passed_links:
        return None
    
    # Filter out revoked media from random selection
    available_links = []
    for video_key in passed_links:
        if "_" not in video_key:
            continue
        try:
            tag, idx_str = video_key.rsplit("_", 1)
            idx = int(idx_str)
            
            # Check if media exists and is not revoked
            if (tag in media_data and 
                isinstance(media_data[tag], list) and 
                0 <= idx < len(media_data[tag])):
                
                item = media_data[tag][idx]
                # Skip revoked items in random selection
                if isinstance(item, dict) and item.get("revoked"):
                    continue
                    
                available_links.append(video_key)
        except (ValueError, IndexError):
            continue
    
    # Return random from available (non-revoked) links
    if not available_links:
        return None
    
    return random.choice(available_links)


def get_random_from_passed_links_by_tag(tag_name: str):
    """Get random video from passed links filtered by a specific tag."""
    if not passed_links:
        return None
    available_links = []
    for video_key in passed_links:
        if "_" not in video_key:
            continue
        try:
            tag, idx_str = video_key.rsplit("_", 1)
            if tag != tag_name:
                continue
            idx = int(idx_str)
            if (tag in media_data and isinstance(media_data[tag], list) and 0 <= idx < len(media_data[tag])):
                item = media_data[tag][idx]
                if isinstance(item, dict) and item.get("revoked"):
                    continue
                available_links.append(video_key)
        except (ValueError, IndexError):
            continue
    if not available_links:
        return None
    return random.choice(available_links)


async def is_user_member(user_id, bot, channel):
    try:
        # Try with @ prefix first
        member = await bot.get_chat_member(chat_id=f"@{channel}",
                                           user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        try:
            # Try without @ prefix as backup
            member = await bot.get_chat_member(chat_id=channel,
                                               user_id=user_id)
            return member.status in ["member", "administrator", "creator"]
        except:
            return False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'
    args = context.args
    
    # Auto-register user (this handles all user tracking)
    await auto_register_user(update, context)

    if args:
        param = args[0].strip().lower()

    # Handle deleted media commands first
        if param.startswith("view_deleted_") and is_admin(user.id):
            video_key = param.replace("view_deleted_", "")
            if video_key in deleted_media_storage:
                # Create a fake query object to use existing view_deleted_media function
                class FakeQuery:
                    def __init__(self, user, message_obj, callback_data):
                        self.from_user = user
                        self.message = message_obj
                        self.data = callback_data
                    
                    async def answer(self, text="", show_alert=False):
                        pass  # No-op for fake query
                
                fake_query = FakeQuery(user, update.message, f"view_deleted_{video_key}")
                await view_deleted_media(fake_query, context)
            else:
                await update.message.reply_text(f"❌ Deleted media not found: {video_key}")
            return
        
        # Handle /pushst undo deep links
        if param.startswith("pushundo_move_") and is_admin(user.id):
            try:
                _, idpair = param.split("pushundo_move_", 1)
                a_str, b_str = idpair.split("_")
                id_a = int(a_str); id_b = int(b_str)
            except Exception:
                await update.message.reply_text("❌ Bad undo ids")
                return

            # Perform undo move (logic adapted from callback handler)
            try:
                for cid in (id_a, id_b):
                    entry = push_changes.get(cid)
                    if not entry:
                        continue
                    tag_name = entry.get("tag"); file_id = entry.get("file_id")
                    action = entry.get("action"); item_snapshot = entry.get("item_snapshot")
                    entry_index = entry.get("index")

                    if action == "add":
                        if tag_name in media_data and isinstance(media_data[tag_name], list):
                            if isinstance(entry_index, int) and 0 <= entry_index < len(media_data[tag_name]):
                                slot = media_data[tag_name][entry_index]
                                if (isinstance(slot, dict) and slot.get("file_id") == file_id) or slot == item_snapshot:
                                    tomb = {"deleted": True, "data": slot}
                                    if isinstance(slot, dict) and "type" in slot:
                                        tomb["type"] = slot.get("type")
                                    media_data[tag_name][entry_index] = tomb
                                    save_media(); update_random_state()
                            else:
                                before = len(media_data[tag_name])
                                media_data[tag_name] = [
                                    it for it in media_data[tag_name]
                                    if not (isinstance(it, dict) and it.get("file_id") == file_id)
                                ]
                                if len(media_data[tag_name]) < before:
                                    save_media(); update_random_state()
                    elif action == "remove":
                        media_data.setdefault(tag_name, [])
                        if isinstance(entry_index, int) and entry_index >= 0:
                            while len(media_data[tag_name]) <= entry_index:
                                media_data[tag_name].append({"deleted": True, "data": None})
                            media_data[tag_name][entry_index] = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
                            try:
                                del deleted_media_storage[f"{tag_name}_{entry_index}"]
                            except Exception:
                                pass
                            save_media(); update_random_state(); save_deleted_media()
                        else:
                            exists = any(isinstance(it, dict) and it.get("file_id") == file_id for it in media_data[tag_name])
                            if not exists:
                                to_add = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
                                media_data[tag_name].append(to_add)
                                save_media(); update_random_state()

                    # Remove from logs
                    if cid in push_changes:
                        push_changes.pop(cid, None)
                    if cid in push_changes_order:
                        try:
                            push_changes_order.remove(cid)
                        except ValueError:
                            pass

                save_push_changes()
                await update.message.reply_text("✅ Move undone")
            except Exception as e:
                print(f"ERROR in deep-link pushundo_move: {e}")
                await update.message.reply_text("❌ Undo failed")
            return

        if param.startswith("pushundo_") and is_admin(user.id):
            try:
                cid = int(param.replace("pushundo_", ""))
            except Exception:
                await update.message.reply_text("❌ Bad undo id")
                return

            entry = push_changes.get(cid)
            if not entry:
                await update.message.reply_text("❌ Change not found")
                return

            tag_name = entry.get("tag"); file_id = entry.get("file_id")
            action = entry.get("action"); item_snapshot = entry.get("item_snapshot")
            try:
                if action == "add":
                    if tag_name in media_data and isinstance(media_data[tag_name], list):
                        entry_index = entry.get("index")
                        if isinstance(entry_index, int) and 0 <= entry_index < len(media_data[tag_name]):
                            slot = media_data[tag_name][entry_index]
                            if (isinstance(slot, dict) and slot.get("file_id") == file_id) or slot == item_snapshot:
                                tomb = {"deleted": True, "data": slot}
                                if isinstance(slot, dict) and "type" in slot:
                                    tomb["type"] = slot.get("type")
                                media_data[tag_name][entry_index] = tomb
                                save_media(); update_random_state()
                        else:
                            before = len(media_data[tag_name])
                            media_data[tag_name] = [
                                it for it in media_data[tag_name]
                                if not (isinstance(it, dict) and it.get("file_id") == file_id)
                            ]
                            if len(media_data[tag_name]) < before:
                                save_media(); update_random_state()
                elif action == "remove":
                    media_data.setdefault(tag_name, [])
                    entry_index = entry.get("index")
                    if isinstance(entry_index, int) and entry_index >= 0:
                        while len(media_data[tag_name]) <= entry_index:
                            media_data[tag_name].append({"deleted": True, "data": None})
                        media_data[tag_name][entry_index] = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
                        try:
                            del deleted_media_storage[f"{tag_name}_{entry_index}"]
                        except Exception:
                            pass
                        save_media(); update_random_state(); save_deleted_media()
                    else:
                        exists = any(isinstance(it, dict) and it.get("file_id") == file_id for it in media_data[tag_name])
                        if not exists:
                            to_add = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
                            media_data[tag_name].append(to_add)
                            save_media(); update_random_state()
                else:
                    await update.message.reply_text("❌ Unknown action")
                    return

                push_changes.pop(cid, None)
                try:
                    push_changes_order.remove(cid)
                except Exception:
                    pass

                save_push_changes()
                await update.message.reply_text("✅ Undone")
            except Exception as e:
                print(f"ERROR in deep-link pushundo: {e}")
                await update.message.reply_text("❌ Undo failed")
            return

        if param.startswith("restore_") and is_admin(user.id):
            video_key = param.replace("restore_", "")
            
            # Check if it's a deleted item first
            if video_key in deleted_media_storage:
                # Create a fake query object to use existing restore_media_entry function
                class FakeQuery:
                    def __init__(self, user, message_obj):
                        self.from_user = user
                        self.message = message_obj
                    
                    async def answer(self, text="", show_alert=False):
                        if text and "successfully" in text.lower():
                            await update.message.reply_text(text)
                
                fake_query = FakeQuery(user, update.message)
                await restore_media_entry(fake_query, video_key)
            else:
                # Check if it's a revoked item in media_data
                try:
                    tag, idx_str = video_key.rsplit("_", 1)
                    idx = int(idx_str)
                    
                    if tag in media_data and idx < len(media_data[tag]):
                        media_item = media_data[tag][idx]
                        
                        if isinstance(media_item, dict) and media_item.get("revoked"):
                            # Remove the revoked flag to restore the media
                            media_item.pop("revoked", None)
                            
                            save_media()
                            update_random_state()
                            
                            await update.message.reply_text(f"✅ Revoked media <code>{video_key}</code> restored successfully!", parse_mode=ParseMode.HTML)
                            return
                
                    await update.message.reply_text(f"❌ No deleted or revoked media found: {video_key}")
                    return
                    
                except (ValueError, IndexError):
                    await update.message.reply_text(f"❌ Invalid media key format: {video_key}")
                    return
            return

        # Parse parameter - could be just tag or tag_start_end
        # Tags can contain dots, dashes, underscores - only last 2 underscore parts are indices
        print(f"🔍 [Deep Link Parse] Received param='{param}'")
        if '_' in param and param.count('_') >= 2:
            parts = param.rsplit('_', 2)  # Split from right to get last 2 indices
            tag = parts[0]  # Everything before last 2 underscores is the tag
            try:
                start_index = int(parts[1])
                end_index = int(parts[2])
                print(f"  ✅ Parsed as range: tag='{tag}', start={start_index}, end={end_index}")
            except (ValueError, IndexError):
                tag = param
                start_index = None
                end_index = None
                print(f"  ⚠️ Failed to parse indices, treating as simple tag: '{tag}'")
        else:
            tag = param
            start_index = None
            end_index = None
            print(f"  ℹ️ No indices found, treating as simple tag: '{tag}'")
        # Ensure start_index and end_index are within bounds if tag exists
        if tag in media_data and isinstance(media_data[tag], list):
            max_idx = len(media_data[tag]) - 1
            if start_index is not None and (start_index < 0 or start_index > max_idx):
                start_index = 0
            if end_index is not None and (end_index < 0 or end_index > max_idx):
                end_index = max_idx
        else:
            # Fallback: if tag not found and it may be a hyphen-encoded dot-tag, try decoding
            # Telegram deep-link allows only [A-Za-z0-9_\-]; '.' is not guaranteed, so some links may use '-' for '.'
            # If replacing '-' with '.' yields a known tag, use it
            if '-' in tag:
                alt_tag = tag.replace('-', '.')
                if alt_tag in media_data and isinstance(media_data[alt_tag], list):
                    print(f"  🔁 Decoded tag from '{tag}' to '{alt_tag}'")
                    tag = alt_tag
                    max_idx = len(media_data[tag]) - 1
                    if start_index is not None and (start_index < 0 or start_index > max_idx):
                        start_index = 0
                    if end_index is not None and (end_index < 0 or end_index > max_idx):
                        end_index = max_idx

        # Check if the link is active (unless user is admin)
        if user.id != ADMIN_ID:
            print(f"🔐 [Non-Admin Check] Checking access for tag='{tag}', indices=[{start_index}-{end_index}]")
            link_is_active = False
            
            # For shareable links, check if param exists directly (full key match) OR check by tag
            if param in active_links:
                print(f"  ✅ Direct match: param '{param}' found in active_links")
                link_is_active = True
            elif tag in active_links:
                # Simple tag match (e.g., 'w.section' without indices)
                print(f"  ✅ Tag match: '{tag}' found in active_links")
                link_is_active = True
            elif start_index is not None and end_index is not None:
                # Check if this range request falls within any existing passlink range
                for link_key, link_data in active_links.items():
                    if isinstance(link_data, dict) and link_data.get("tag") == tag:
                        stored_start = link_data.get("start_index", 0)
                        stored_end = link_data.get("end_index", 0) 
                        # Check if requested range is within stored range
                        if start_index >= stored_start and end_index <= stored_end:
                            link_is_active = True
                            break
            else:
                # Simple tag format - check if tag exists in any passlink
                for link_key, link_data in active_links.items():
                    if isinstance(link_data, dict) and link_data.get("tag") == tag:
                        link_is_active = True
                        break
            
            # If not accessible, deny access
            if not link_is_active:
                await update.message.reply_html(
                    (
                        "⛔ Access denied.\n\n"
                        "This shareable link or requested range is no longer active.\n"
                        "If you believe this is an error, request a fresh link from the admin."
                    )
                )
                return
            # Single file check (start_index equals end_index)
            is_single_file = start_index is not None and end_index is not None and start_index == end_index
            
            # Skip channel verification for single-file links
            if not is_single_file and user.id not in exempted_users:
                # Verify channel membership for multi-file links
                membership_failed = False
                for ch in REQUIRED_CHANNELS:
                    is_member = await is_user_member(user.id, context.bot, ch)
                    if not is_member:
                        membership_failed = True
                        break
                
                if membership_failed:
                    # For ranged links, preserve the range in retry callback
                    retry_param = param
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("JOIN✨", url="https://t.me/Lifesuckkkkkssss"),
                            InlineKeyboardButton("BACKUP🛡️", url="https://t.me/bhaicharabackup")
                        ],
                        [
                            InlineKeyboardButton("🔄 Try Again", callback_data=f"retry_{retry_param}")
                        ]
                    ])
                    
                    # Send notification with video
                    await context.bot.send_video(
                        chat_id=update.effective_chat.id,
                        video=CHANNEL_NOTIFY_VIDEO_ID,
                        caption=f"Hey {mention}!!\n"
                               "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                               "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                               "To access multiple videos, you must join our channels first!\n"
                               "Single video links don't require membership.\n\n"
                               "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                    return
            
            # For non-admin users, create custom range if needed
            print(f"📦 [Non-Admin Path] Looking up link data for tag='{tag}', param='{param}'")
            if param in active_links:
                # Direct link exists (exact key match) - use it as-is
                print(f"  ✅ Using direct link data for param '{param}'")
                link_data = active_links[param]
                await send_passlink_videos(update, context, param, link_data)
                return
            else:
                # Find any active_links entry by tag match (since keys may include ranges like 'tag_0_10')
                matched_key = None
                matched_data = None
                for link_key, stored_link_data in active_links.items():
                    if isinstance(stored_link_data, dict) and stored_link_data.get("tag") == tag:
                        matched_key = link_key
                        matched_data = stored_link_data
                        break

                # If no match, try hyphen->dot decoding for keys stored with dots
                if not matched_data and '-' in tag:
                    alt_tag = tag.replace('-', '.')
                    for link_key, stored_link_data in active_links.items():
                        if isinstance(stored_link_data, dict) and stored_link_data.get("tag") == alt_tag:
                            print(f"  🔁 Found passlink by alt tag '{alt_tag}' from '{tag}'")
                            tag = alt_tag
                            matched_key = link_key
                            matched_data = stored_link_data
                            break

                if matched_data:
                    print(f"  ✅ Found passlink entry '{matched_key}' for tag '{tag}'")
                    if start_index is not None and end_index is not None:
                        stored_start = matched_data.get("start_index", 0)
                        stored_end = matched_data.get("end_index", 0)
                        # Validate subset range
                        if start_index >= stored_start and end_index <= stored_end:
                            # Build custom subset from media_data to honor current visibility/revoked rules
                            if tag in media_data and isinstance(media_data[tag], list):
                                tag_videos = media_data[tag]
                                custom_videos = []
                                valid_indices = []
                                for idx in range(start_index, end_index + 1):
                                    if 0 <= idx < len(tag_videos):
                                        item = tag_videos[idx]
                                        if isinstance(item, dict) and "type" in item and "file_id" in item:
                                            if item.get("revoked") and not is_admin(update.effective_user.id):
                                                continue
                                            custom_videos.append(item)
                                            valid_indices.append(idx)
                                custom_link_data = {
                                    "type": "passlink_custom",
                                    "tag": tag,
                                    "start_index": start_index,
                                    "end_index": end_index,
                                    "videos": custom_videos,
                                    "actual_indices": valid_indices
                                }
                                print(f"  📤 Sending custom subset: {len(custom_videos)} items [{start_index}-{end_index}]")
                                await send_passlink_videos(update, context, param, custom_link_data)
                                return
                        else:
                            print(f"  ❌ Requested range [{start_index}-{end_index}] is outside stored [{stored_start}-{stored_end}]")
                            await update.message.reply_html("⛔ Requested range is outside the available passlink range.")
                            return
                    else:
                        # No range specified → use full stored passlink data
                        print(f"  ✅ Using full passlink data for tag '{tag}' via key '{matched_key}'")
                        await send_passlink_videos(update, context, matched_key, matched_data)
                        return

                # If we reach here, link not found in active_links
                print(f"  ❌ No matching link found for tag '{tag}' in active_links")
                await update.message.reply_html("⛔ Link not found or inactive.")
                return
        
        # Check if this is a custom range within a passlink range (for admin users)
        if start_index is not None and end_index is not None:
            # Check if this is a single file request (start_index equals end_index)
            is_single_file = start_index == end_index
            
            # Verify channel membership for multi-file links (skip for single file)
            if not is_single_file and user.id not in exempted_users and user.id != ADMIN_ID:
                membership_failed = False
                for ch in REQUIRED_CHANNELS:
                    is_member = await is_user_member(user.id, context.bot, ch)
                    print(f"User {user.id} membership in {ch}: {is_member}")
                    if not is_member:
                        membership_failed = True
                        break
                
                if membership_failed:
                    # For ranged links, preserve the range in retry callback
                    retry_param = f"{tag}_{start_index}_{end_index}"
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("JOIN✨", url="https://t.me/Lifesuckkkkkssss"),
                            InlineKeyboardButton("BACKUP🛡️", url="https://t.me/bhaicharabackup")
                        ],
                        [
                            InlineKeyboardButton("🔄 Try Again", callback_data=f"retry_{retry_param}")
                        ]
                    ])
                    
                    # Send notification with video
                    await context.bot.send_video(
                        chat_id=update.effective_chat.id,
                        video=CHANNEL_NOTIFY_VIDEO_ID,
                        caption=f"Hey {mention}!!\n"
                               "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                               "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                               "To access multiple videos, you must join our channels first!\n"
                               "Single video links don't require membership.\n\n"
                               "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                    return
            
            print(f"🔍 [Admin Range] Looking for tag={tag}, range=[{start_index}-{end_index}]")
            for link_key, link_data in active_links.items():
                print(f"  Checking link_key={link_key}, has_tag={isinstance(link_data, dict) and link_data.get('tag') == tag}")
                if isinstance(link_data, dict) and link_data.get("tag") == tag:
                    stored_start = link_data.get("start_index", 0)
                    stored_end = link_data.get("end_index", 0)
                    print(f"  ✓ Tag match! stored=[{stored_start}-{stored_end}], requested=[{start_index}-{end_index}]")
                    
                    # Check if requested range is within stored range
                    if start_index >= stored_start and end_index <= stored_end:
                        print(f"  ✅ Range valid! Building custom_videos...")
                        # Get videos directly from media_data for this custom range
                        if tag in media_data and isinstance(media_data[tag], list):
                            tag_videos = media_data[tag]
                            custom_videos = []
                            valid_indices = []
                            for idx in range(start_index, end_index + 1):
                                if 0 <= idx < len(tag_videos):
                                    item = tag_videos[idx]
                                    # Only include valid media items, skip revoked for non-admins
                                    if isinstance(item, dict) and "type" in item and "file_id" in item:
                                        # Skip revoked media for regular users
                                        if item.get("revoked") and not is_admin(update.effective_user.id):
                                            continue
                                        custom_videos.append(item)
                                        valid_indices.append(idx)
                            
                            print(f"  📤 Sending {len(custom_videos)} videos with indices={valid_indices}")
                            # Create custom link data for this range
                            custom_link_data = {
                                "type": "passlink_custom",
                                "tag": tag,
                                "start_index": start_index,
                                "end_index": end_index,
                                "videos": custom_videos,
                                "actual_indices": valid_indices
                            }
                            await send_passlink_videos(update, context, param, custom_link_data)
                            print(f"  ✅ send_passlink_videos completed, returning...")
                            return
        else:
            # Simple tag format - serve all videos from any matching passlink
            for link_key, link_data in active_links.items():
                if isinstance(link_data, dict) and link_data.get("tag") == tag:
                    # Apply membership verification only if this is NOT a single-file link
                    start_idx = link_data.get("start_index", 0)
                    end_idx = link_data.get("end_index", 0)
                    
                    # Single file check (start_index equals end_index)
                    is_single_file = start_idx == end_idx
                    
                    # Skip channel verification for single-file links
                    if not is_single_file and user.id not in exempted_users:
                        # Verify channel membership for multi-file links
                        membership_failed = False
                        for ch in REQUIRED_CHANNELS:
                            is_member = await is_user_member(user.id, context.bot, ch)
                            if not is_member:
                                membership_failed = True
                                break
                        
                        if membership_failed:
                            # For ranged links, preserve the range in retry callback
                            retry_param = link_key
                            keyboard = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("JOIN✨", url="https://t.me/Lifesuckkkkkssss"),
                                    InlineKeyboardButton("BACKUP🛡️", url="https://t.me/bhaicharabackup")
                                ],
                                [
                                    InlineKeyboardButton("🔄 Try Again", callback_data=f"retry_{retry_param}")
                                ]
                            ])
                            
                            # Send notification with video
                            await context.bot.send_video(
                                chat_id=update.effective_chat.id,
                                video=CHANNEL_NOTIFY_VIDEO_ID,
                                caption=f"Hey {mention}!!\n"
                                       "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                                       "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                                       "To access multiple videos, you must join our channels first!\n"
                                       "Single video links don't require membership.\n\n"
                                       "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
                                parse_mode=ParseMode.HTML,
                                reply_markup=keyboard
                            )
                            return
                    
                    await send_passlink_videos(update, context, param, link_data)
                    return

        # Check if it's an individual video key (tag_index) BUT avoid treating range patterns tag_idx_idx as single
        if '_' in param:
            parts = param.split('_')
            # Single video pattern: last part numeric AND second last part NOT numeric (so it's not a range tag_idx_idx)
            if len(parts) >= 2 and parts[-1].isdigit() and not (len(parts) >= 2 and parts[-2].isdigit()):
                video_tag = '_'.join(parts[:-1])
                try:
                    video_index = int(parts[-1])
                    
                    # First check if this video is within any active passlink range
                    for link_key, link_data in active_links.items():
                        if isinstance(link_data, dict) and link_data.get("tag") == video_tag:
                            passlink_start = link_data.get("start_index", 0)
                            passlink_end = link_data.get("end_index", 0)
                            
                            # If the video index is within the passlink range, serve from passlink
                            if passlink_start <= video_index <= passlink_end:
                                videos = link_data.get("videos", [])
                                if videos and (video_index - passlink_start) < len(videos):
                                    # Create custom link data for just this single video
                                    single_video_data = {
                                        "type": "passlink_single",
                                        "tag": video_tag,
                                        "start_index": video_index,
                                        "end_index": video_index,
                                        "videos": [videos[video_index - passlink_start]]
                                    }
                                    await send_passlink_videos(update, context, param, single_video_data)
                                    return
                    
                    # If not in any passlink, check if this video exists in media_data
                    if video_tag in media_data and isinstance(media_data[video_tag], list):
                        if 0 <= video_index < len(media_data[video_tag]):
                            # Send the specific video
                            video_data = media_data[video_tag][video_index]
                            
                            # Check if media is revoked
                            if isinstance(video_data, dict) and video_data.get("revoked"):
                                await update.message.reply_text("❌ This media has been revoked and is no longer accessible.")
                                return
                            
                            # Create shareable link for this specific video
                            share_link = f"https://t.me/{BOT_USERNAME}?start={param}"
                            share_url = f"https://t.me/share/url?url={share_link}"
                            
                            keyboard = [
                                [
                                    InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{param}"),
                                    InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
                                ]
                            ]
                            reply_markup = InlineKeyboardMarkup(keyboard)
                            
                            if video_data.get("type") == "video" and "file_id" in video_data:
                                # Build media caption
                                final_caption = build_media_caption("", video_tag, video_index, share_link, "video")
                                
                                await safe_send_message(
                                    context=context,
                                    chat_id=update.effective_chat.id,
                                    video=video_data["file_id"],
                                    caption=final_caption,
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id)
                                )
                            elif video_data.get("type") == "photo" and "file_id" in video_data:
                                # Build media caption
                                final_caption = build_media_caption("", video_tag, video_index, share_link, "photo")
                                
                                await safe_send_message(
                                    context=context,
                                    chat_id=update.effective_chat.id,
                                    photo=video_data["file_id"],
                                    caption=final_caption,
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id)
                                )
                            return
                except ValueError:
                    pass

        await send_tagged_with_range(update, context, tag, start_index,
                                     end_index)
        return

    text = (f"👋 Hey {mention}!!\n"
            "ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ <b>ᴍᴇᴏᴡ ɢᴀɴɢ</b> 💌\n"
            "═══════════════════════════\n"
            "<b>𝓽𝓲𝓻𝓮𝓭 𝓸𝓯 𝓪𝓭𝓼 𝓪𝓷𝓭 𝓳𝓸𝓲𝓷𝓲𝓷𝓰 𝓶𝓾𝓵𝓽𝓲𝓹𝓵𝓮 𝓬𝓱𝓪𝓷𝓷𝓮𝓵𝓼? 𝓷𝓸𝓽 𝓽𝓸 𝔀𝓸𝓻𝓻𝔂 𝓪𝓷𝔂𝓶𝓸𝓻𝓮. 𝓬𝓵𝒾𝓬𝓴 𝓫𝓮𝓵𝓸𝔀 𝓫𝓾𝓽𝓽𝓸𝓷 𝓪𝓷𝓭 𝓼𝓮𝓮 𝓽𝓱𝓮 𝓶𝓪𝓰𝓲𝓬</b>.\n"
            "<b>ρєα¢є συт</b> ✌️\n\n")

    # Inline keyboard for welcome: Random 18+ and Help
    inline_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎲 RANDOM 18+", callback_data="random_media")],
        [InlineKeyboardButton("❓ HELP", callback_data="show_help")]
    ])

    # Reply keyboard (main menu) only visible on /start
    reply_keyboard = ReplyKeyboardMarkup(
        [
            ["🎬 Random (18-)", "🏴‍☠️ Off Limits"] ,
            ["⭐ My Favorites"]
        ],
        resize_keyboard=True
    )

    # Send image with caption and inline keyboard (reply keyboard sent separately to persist)
    photo_sent = False
    
    # Try different methods to send the welcome image
    try:
        # Method 1: Try using stored file_id if available
        global WELCOME_IMAGE_FILE_ID
        if WELCOME_IMAGE_FILE_ID:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=WELCOME_IMAGE_FILE_ID,
                caption=text,
                parse_mode="HTML",
                protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id),
            reply_markup=inline_keyboard
            )
            photo_sent = True
        else:
            # No stored file_id available
            photo_sent = False
    except Exception as e:
        print(f"⚠️ Error sending welcome photo: {e}")
        photo_sent = False
    
    # Fallback: Send message without image if photo failed
    if not photo_sent:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🖼️ <i>(Welcome image temporarily unavailable)</i>\n\n{text}",
            parse_mode="HTML",
            protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id),
            reply_markup=inline_keyboard
        )    # Send a separate message to attach the reply keyboard for actions
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Select an action below ⬇️",
        reply_markup=reply_keyboard
    )


def generate_revoked_list(page=1):
    """Helper function to generate revoked media list content and keyboard"""
    # Get revoked items from media_data with details
    revoked_entries = []
    for tag, vids in media_data.items():
        if not isinstance(vids, list):
            continue
        for idx, v in enumerate(vids):
            if isinstance(v, dict) and v.get("revoked"):
                media_type = v.get("type", "unknown")
                
                # Get type emoji
                if media_type == "video":
                    type_icon = "🎥"
                elif media_type == "photo":
                    type_icon = "🖼"
                elif media_type == "document":
                    type_icon = "📄"
                else:
                    type_icon = "📎"
                    
                revoked_entries.append((f"{tag}_{idx}", tag, str(idx), media_type, type_icon))
    
    # Check if we have any revoked media
    if not revoked_entries:
        return "📋 No revoked media found.", None
    
    # Sort by tag, then by index
    revoked_entries.sort(key=lambda x: (x[1], int(x[2]) if x[2].isdigit() else 0))
    
    # Paginate (show 20 per page like deleted media)
    items_per_page = 20
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    page_entries = revoked_entries[start_idx:end_idx]
    
    if not page_entries:
        return f"📄 Page {page} is empty.", None
    
    # Build the beautiful formatted message
    text_lines = [f"🛑 <b>Revoked Media</b> (Page {page})"]
    text_lines.append("")
    
    for i, (key, tag, index, media_type, type_icon) in enumerate(page_entries, 1):
        # Create view link
        view_format = f"{tag}_{index}_{index}"
        view_link = f"https://t.me/bhaicharabackupbot?start={view_format}"
        
        # Format like deleted media: "1. Tag: code | Index: code | 💎 type | icon | View | Restore"
        line = f"{i}. Tag: <code>{tag}</code> | Index: <code>{index}</code> | 💎 {media_type} | 🛑 {type_icon} | <a href='{view_link}'>View</a> | <a href='https://t.me/bhaicharabackupbot?start=restore_{key}'>Restore</a>"
        text_lines.append(line)
    
    # Add pagination info
    total_pages = (len(revoked_entries) + items_per_page - 1) // items_per_page
    text_lines.append("")
    text_lines.append(f"📊 Showing {len(page_entries)} of {len(revoked_entries)} items")
    
    # Create inline keyboard for pagination
    keyboard = []
    nav_buttons = []
    
    # Previous page button
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Prev", callback_data=f"revoked_page_{page-1}"))
    
    # Page info
    nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    
    # Next page button
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"revoked_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Action buttons
    action_buttons = [
        InlineKeyboardButton("🔄 Refresh", callback_data="refresh_revoked_list"),
        InlineKeyboardButton("♻️ Restore All", callback_data="cleanup_revoked")
    ]
    keyboard.append(action_buttons)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    return "\n".join(text_lines), reply_markup

# List revoked media command (moved above callback handlers)
async def listremoved_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    
    # Get page number
    page = 1
    if context.args and len(context.args) > 0:
        try:
            page = max(1, int(context.args[0]))
        except ValueError:
            page = 1
    
    text, reply_markup = generate_revoked_list(page)
    
    if reply_markup is None:
        await update.message.reply_text(text)
    else:
        await update.message.reply_html(text, reply_markup=reply_markup)


async def send_enter_specific_rank_prompt(query, context: ContextTypes.DEFAULT_TYPE):
    """Send the prompt asking the user to enter a specific rank and track it for replies."""
    chat_id = getattr(query.message, "chat_id", None)
    if chat_id is None:
        logging.error("Cannot send rank prompt: chat_id is missing")
        return None

    user_id = query.from_user.id if query.from_user else None
    msg = (
        "<b>⌨️ ENTER SPECIFIC RANK</b>\n\n"
        "Reply to this message with just the rank number (e.g., <code>42</code>) to jump to that position.\n\n"
        "Your next message will be interpreted as the rank number."
    )
    plain_msg = (
        "⌨️ ENTER SPECIFIC RANK\n\n"
        "Reply to this message with just the rank number (e.g., 42) to jump to that position.\n\n"
        "Your next message will be interpreted as the rank number."
    )

    logging.info(f"User {user_id} requested enter_specific_rank")

    sent_message = None
    try:
        sent_message = await context.bot.send_message(
            chat_id=chat_id,
            text=msg,
            parse_mode=ParseMode.HTML
        )
        logging.info(
            "Sent enter_specific_rank prompt with message_id %s", sent_message.message_id
        )
    except Exception as e:
        logging.error("Error sending enter_specific_rank prompt: %s", e)
        try:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=plain_msg)
        except Exception as e2:
            logging.error("Failed to send plain text rank entry message: %s", e2)
            await query.answer("Unable to send rank prompt. Please try again later.", show_alert=True)
            return None

    if sent_message:
        context.user_data["rank_prompt_message_id"] = sent_message.message_id
        context.user_data["rank_prompt_chat_id"] = chat_id
        context.user_data["expecting_rank_reply"] = True

async def handle_button_click(update: Update,
                              context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    # Answer the callback query immediately to prevent timeout
    await safe_answer_callback_query(query)
    
    # Auto-register user (this handles all user tracking)
    await auto_register_user(update, context)

    # Fast routing for special callbacks that have dedicated handlers
    if query.data.startswith("resume_"):
        await handle_resume_callback(update, context)
        return
    if query.data == "return_home":
        await return_home_callback(update, context)
        return

    if query.data == "get_random":
        # GET FILES button - only from "random" tag
        await send_random_video(context, query.message.chat_id, mode="safe", user_id=query.from_user.id)

    elif query.data == "random_media":
        # Start adult random flow anchored to 'mediax' tag for the first item
        await send_random_video(context, query.message.chat_id, mode="mediax", user_id=query.from_user.id)

    elif query.data in ("random_safe", "random_18minus"):
        await send_random_video(context, query.message.chat_id, mode="safe", user_id=query.from_user.id)
    elif query.data == "next_offlimits":
        # Cycle another Off Limits item
        try:
            # Reuse the Off Limits viewer to send a fresh item
            await show_off_limits_content(update, context)
        except Exception as e:
            print(f"ERROR in next_offlimits handler: {e}")
            try:
                await query.answer("❌ Couldn't load next item", show_alert=True)
            except Exception:
                pass
    elif query.data.startswith("next_offlimits_tag_"):
        # Next within a specific Off Limits tag
        tag = query.data.replace("next_offlimits_tag_", "")
        try:
            await show_off_limits_content(update, context, force_tag=tag)
        except Exception as e:
            print(f"ERROR in next_offlimits_tag handler: {e}")
            try:
                await query.answer("❌ Couldn't load next item", show_alert=True)
            except Exception:
                pass
    elif query.data.startswith("switch_to_offlimits_tag_"):
        # Switch Off Limits browsing to another tag
        tag = query.data.replace("switch_to_offlimits_tag_", "")
        try:
            await show_off_limits_content(update, context, force_tag=tag)
        except Exception as e:
            print(f"ERROR in switch_to_offlimits_tag handler: {e}")
            try:
                await query.answer("❌ Couldn't switch tag", show_alert=True)
            except Exception:
                pass

    elif query.data.startswith("next_tag_"):
        tag = query.data.replace("next_tag_", "")
        # Special-case: 'random' next should stay in safe flow
        if tag == "random":
            await send_random_video(context, query.message.chat_id, mode="safe", user_id=query.from_user.id)
        else:
            await send_random_video(context, query.message.chat_id, mode=tag, user_id=query.from_user.id)
    elif query.data.startswith("switch_to_tag_"):
        tag = query.data.replace("switch_to_tag_", "")
        # Guard against legacy keyboards that might try to switch to 'random'
        if tag == "random":
            # Pick a sensible adult default: first tag in passed_links (excluding 'random'); fallback to 'mediax'
            fallback_tag = "mediax"
            try:
                with open('passed_links.json', 'r') as f:
                    _pl = json.load(f)
                _seen = set()
                for key in _pl:
                    if '_' in key:
                        t = key.rsplit('_', 1)[0]
                        if t and t != 'random' and t not in _seen:
                            fallback_tag = t
                            break
            except Exception:
                pass
            await send_random_video(context, query.message.chat_id, mode=fallback_tag, user_id=query.from_user.id)
        else:
            await send_random_video(context, query.message.chat_id, mode=tag, user_id=query.from_user.id)

    elif query.data == "view_favorites":
        await show_favorites_navigator(query, context, 0)

    elif query.data == "show_help":
        try:
            # Full 2-part help matching /help command
            help_part1 = (
                "<b>Help (part 1/2)</b>\n\n"
                "🤖 <b>Bhaichara Bot - Help Guide</b>\n\n"
                "❤️ <b>User Commands:</b>\n"
                "• <code>/start</code> - Start the bot and see main menu\n"
                "• <code>/top</code> - Browse the Top Videos navigator\n"
                "• <code>/jump &lt;rank&gt;</code> - Jump straight to a ranked video\n"
                "• <code>/favorites</code> - View your favorite videos with navigation\n"
                "• <code>/help</code> - Show this help message\n\n"
                "� <b>How to Use:</b>\n"
                "• Join 🗂️ <b>@BEINGHUMANASSOCIATION</b> for bulk access to all Tags\n"
                "• Click 🎲 <b>RANDOM MEDIA</b> for random videos from passed collection\n"
                "• Click 🏴‍☠️ <b>Off Limits</b> to browse exclusive picks (if enabled by admin)\n"
                "• Click ❤️ on any video to add to your favorites\n"
                "• Use ⭐ <b>MY FAVORITES</b> button for quick access to your collection\n"
                "• Navigate with ⬅️ <b>Previous</b> and ➡️ <b>Next</b> buttons\n\n"
                "⚡ <b>Quick Shortcuts:</b>\n"
                "Type these words or emojis for instant access:\n"
                "• <code>fav</code>, <code>favorites</code>, <code>❤️</code> → Open favorites\n"
                "• <code>random</code>, <code>rand</code>, <code>🎲</code>, <code>media</code> → Get random video\n"
                "• <code>help</code>, <code>?</code>, <code>❓</code> → Show this help overlay\n\n"
                "🔍 <b>Smart Tag Search:</b>\n"
                "Type tag names (full or partial) to get matching content:\n"
                "• <code>dance</code>, <code>turn</code>, <code>mediax</code>, <code>rd</code>, <code>rd2</code> → Direct tag access\n"
                "• <b>Smart Keywords:</b> <code>romance</code>, <code>love</code>, <code>passion</code>, <code>intimacy</code> → Maps to turn.on tag\n"
                "💡 Type actual tag names for best results. Partial matches work too!\n\n"
                "🎯 <b>Smart Features:</b>\n"
                "• <b>Two-Tier Random System:</b> GET FILES shows all content, RANDOM MEDIA shows curated content\n"
                "• <b>Smart Rotation:</b> All videos shown before any repeats\n"
                "• <b>Persistent Favorites:</b> Your favorites are saved permanently\n"
                "• <b>Navigation Memory:</b> Bot remembers your position in favorites\n"
                "• <b>Real-time Updates:</b> Instant feedback on all actions\n\n"
                "🧠 <b>Pro Tips:</b>\n"
                "• Use <code>/top</code> to open the Top Videos navigator with ready-made filters\n"
                "• Send <code>/jump index</code> to jump to that position instantly\n"
                "• Drop a single ❤️ (or type <code>fav</code>) anywhere to pull up your favorites\n"
                "• Tap 🔄 inside viewers to refresh the current list without leaving\n"
                "• Share any top video with friends via the 🔗 Share button inside the viewer\n\n"
                "🔒 <b>Privacy &amp; Safety:</b> All interactions stay private and protected by the bot's security settings."
            )

            help_part2 = (
                "\n\n<b>🔧 Admin Commands (Complete Reference):</b>\n\n"
                "📁 <b>Media Management:</b>\n"
                "• <code>/upload</code> — Reply to media to add to database\n"
                "• <code>/listvideos</code> — List all tags\n"
                "• <code>/listvideo &lt;tag&gt;</code> — List media in a tag\n"
                "• <code>/view &lt;tag&gt; &lt;index&gt;</code> — View specific media by index\n"
                "• <code>/cview &lt;tag&gt; [start] [end]</code> — Clean view of only visible media\n"
                "• <code>/remove &lt;tag&gt; &lt;index&gt;</code> — Remove specific media\n"
                "• <code>/off_limits &lt;tag&gt; [start] [end]</code> — Add items to Off Limits category\n"
                "• <code>/get &lt;tag&gt;</code> — Get all media from a tag\n"
                "• <code>/free &lt;tag&gt;</code> / <code>/unfree &lt;tag&gt;</code> — Toggle free access\n"
                "• <code>/listfree</code> — List all free tags\n"
                "• <code>/generatelink &lt;tag&gt;</code> — Generate public link for tag\n\n"
                "🔗 <b>Link Systems (Separate Stores):</b>\n"
                "• <code>/pass &lt;tag&gt; [start] [end]</code> — RANDOM MEDIA button access only\n"
                "• <code>/revoke &lt;tag&gt; [start] [end]</code> — Remove from RANDOM MEDIA\n"
                "• <code>/passlink &lt;tag&gt; [start] [end]</code> — Shareable link only\n"
                "• <code>/revokelink &lt;tag&gt; [start] [end]</code> — Disable shareable link\n"
                "• <code>/activelinks</code> — Active shareable links\n"
                "• <code>/passlinks</code> — Active RANDOM MEDIA links\n"
                "• <code>/listactive</code> — All active links (both types)\n\n"
                "🗑️ <b>Cleanup &amp; Recovery:</b>\n"
                "• <code>/fresh &lt;tag&gt; [i1 i2 ...]</code> — Shrink tag by removing useless gaps (keeps revoked)\n"
                "• <code>/forcefresh &lt;tag&gt;</code> — Aggressively shrink tag (also removes revoked)\n"
                "• <code>/push &lt;tag1&gt; &lt;tag2&gt; ...</code> — Pass ALL available items for multiple tags\n"
                "• <code>/passall &lt;tag&gt;</code> or <code>/push all &lt;tag&gt;</code> — Pass entire tag at once\n\n"
                "• <code>/passlinks</code> — Active RANDOM MEDIA links\n\n"
                "• <code>/listdeleted</code> — Show all deleted media\n"
                "• <code>/listrevoked</code> — Show all revoked media\n"
                "• <code>/listremoved</code> — Show all removed media\n"
                "• <code>/restoredeleted &lt;tag&gt; &lt;index&gt;</code> — Restore deleted item\n"
                "• <code>/restoreall</code> — Restore all deleted media\n"
                "• <code>/cleardeleted</code> — Permanently purge deleted storage\n"
                "• <code>/cleanupdeleted</code> — Fix corrupted deleted entries\n"
                "• <code>/debugdeleted &lt;tag&gt;</code> — Debug deleted media issues\n"
                "• <code>/deletedstats</code> — Statistics about deleted media\n\n"
                "� <b>Analytics &amp; Users:</b>\n"
                "• <code>/userstats</code> — User registration statistics\n"
                "• <code>/userinfo &lt;user_id&gt;</code> — Detailed user information\n"
                "• <code>/topusers</code> — Most active users ranking\n"
                "• <code>/userfavorites &lt;user_id&gt;</code> — View user's favorites\n"
                "• <code>/videostats &lt;tag&gt; &lt;index&gt;</code> — Who liked a video\n"
                "• <code>/topvideos [limit]</code> — Most liked videos with navigation\n"
                "• <code>/discover</code> — Discover users from all sources\n"
                "• <code>/addusers &lt;id1&gt; &lt;id2&gt; ...</code> — Add users to database\n\n"
                "📢 <b>Broadcasting:</b>\n"
                "• <code>/broadcast &lt;message&gt;</code> — Normal broadcast\n"
                "• <code>/dbroadcast &lt;message&gt;</code> — Auto-delete broadcast\n"
                "• <code>/pbroadcast &lt;message&gt;</code> — Pin broadcast\n"
                "• <code>/sbroadcast &lt;message&gt;</code> — Silent broadcast\n"
                "• <code>/fbroadcast</code> — Forward mode (reply to message)\n"
                "• <code>/bstats</code> — Broadcasting statistics\n\n"
                "🛡️ <b>Protection &amp; Auto-Delete:</b>\n"
                "• <code>/protection</code> / <code>/pon</code> / <code>/poff</code> / <code>/pstatus</code> / <code>/ps</code>\n"
                "• <code>/testprotection</code> — Test media protection\n"
                "• <code>/checkprotection</code> — Check protection details\n"
                f"• <code>/autodelete on/off/status</code> — Auto-delete after {AUTO_DELETE_HOURS}h\n"
                "• <code>/autodelete hours &lt;hours&gt;</code> — Set deletion time (0.1-168)\n"
                "• <code>/autodelete stats</code> / <code>/autodelete clear</code>\n"
                "• <code>/notifications on/off</code> — Control deletion notifications\n\n"
                "📦 <b>Batch Tools:</b>\n"
                "• <code>/custom_batch &lt;tag&gt;</code> — Start custom batch\n"
                "• <code>/stop_batch</code> / <code>/cancel_batch</code> / <code>/batch_status</code>\n"
                "• <code>/move &lt;src_tag&gt; &lt;start&gt; &lt;end&gt; &lt;dest_tag&gt;</code> — Batch move media range\n"
                "• <code>/add &lt;src_tag&gt; &lt;start&gt; &lt;end&gt; &lt;dest_tag&gt;</code> — Batch copy media range\n\n"
                "🤖 <b>AI Batch (On-Demand):</b>\n"
                "• <code>/ai_batch</code> — Start AI tagging session\n"
                "• <code>/ai_batch_status</code> — Check AI batch status\n"
                "• <code>/stop_ai_batch</code> / <code>/cancel_ai_batch</code>\n"
                "💡 See AI_SETUP.md for Cloudflare or local CLIP setup\n\n"
                "💾 <b>Backups:</b>\n"
                "• <code>/togglebackup on/off/status</code> — Toggle local backups\n"
                "• <code>/backup</code> — Create backup (if enabled)\n"
                "• <code>/listbackups</code> / <code>/restore &lt;name&gt;</code> / <code>/backupstats &lt;name&gt;</code>\n"
                "• <code>/deletebackup &lt;name&gt;</code> — Delete a backup\n"
                "• <code>/telegrambackup</code> — Send JSON files to Telegram (always available; includes off_limits.json)\n"
                "• <code>/autobackup on/off/now/status</code> — Daily auto-backup controls\n\n"
                "🔧 <b>Admin Tools:</b>\n"
                "• <code>/add_admin &lt;user_id&gt;</code> / <code>/remove_admin &lt;user_id&gt;</code> / <code>/list_admins</code>\n"
                "• <code>/set_global_caption &lt;text&gt;</code> — Set global caption\n"
                "• <code>/getfileid</code> — Get file_id (reply to media)\n"
                "• <code>/setwelcomeimage &lt;file_id&gt;</code> / <code>/testwelcomeimage</code>\n"
                "• <code>/checkupdates</code> — Check for pending old requests\n\n"
                "🧪 <b>Testing:</b>\n"
                "• <code>/testprotection</code> / <code>/testdeletion</code>\n\n"
                "⚠️ <b>Important:</b> <code>/pass</code> vs <code>/passlink</code> are separate!\n"
                "• <code>/pass</code> → RANDOM MEDIA button access\n"
                "• <code>/passlink</code> → Shareable links only\n\n"
                "♻️ <b>Inline PUSH:</b> Click PUSH on any media to add/remove it from tags inline with Back/Close buttons.\n"
                "🔁 <b>PUSH Status:</b> <code>/pushst</code> — See recent add/remove/move operations with Undo/Undo Move (index-stable)"
            )

            if is_admin(query.from_user.id):
                # Send Part 1
                await send_long_html(context, query.message.chat_id, help_part1)
                await asyncio.sleep(0.5)  # Small delay between messages
                await send_long_html(context, query.message.chat_id, "<b>Help (part 2/2)</b>\n" + help_part2)
                # Send admin keyboard as a separate message
                admin_keyboard = ReplyKeyboardMarkup([
                    ["📊 Stats", "📢 Broadcast", "👥 Users"],
                    ["🎬 Media", "💾 Backup", "🛡️ Protection"],
                    ["🧹 Cleanup", "🔧 Tools", "🧪 Test"],
                    ["🔁 Auto Reindex", "🔧 Reindex", "🏠 HOME"]
                ], resize_keyboard=True)
                await context.bot.send_message(query.message.chat_id, "Select an admin action below:", reply_markup=admin_keyboard)
            else:
                await send_long_html(context, query.message.chat_id, help_part1)
        except Exception as e:
            print(f"ERROR sending help message to {query.from_user.id}: {e}")
            import traceback
            traceback.print_exc()
            try:
                await query.answer("❌ Error displaying help. Please try /help command.", show_alert=True)
            except:
                pass

    elif query.data.startswith("fav_nav_"):
        # Handle favorites navigation
        index = int(query.data.replace("fav_nav_", ""))
        await show_favorites_navigator(query, context, index, edit_message=True)

    elif query.data.startswith("add_fav_"):
        await add_to_favorites(query, context)

    elif query.data.startswith("remove_fav_"):
        await remove_from_favorites(query, context)

    elif query.data.startswith("who_liked_"):
        await show_who_liked_video(query, context)

    elif query.data.startswith("view_video_"):
        await view_specific_video(query, context)

    elif query.data.startswith("view_deleted_"):
        await view_deleted_media(query, context)

    elif query.data.startswith("p_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("p_", "")
        print(f"DEBUG: PUSH button clicked - video_key: {video_key}")
        try:
            await show_push_tag_selector(query, context, video_key)
        except Exception as e:
            print(f"ERROR in PUSH button handler: {e}")
            import traceback
            traceback.print_exc()
            await query.answer(f"❌ Error: {str(e)}", show_alert=True)

    elif query.data.startswith("pa_") or query.data.startswith("pr_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        # Parse callback data: pa_tag_index_tagname or pr_tag_index_tagname
        action = "add" if query.data.startswith("pa_") else "remove"
        parts = query.data[3:].rsplit("_", 1)  # Remove prefix and split from right
        if len(parts) == 2:
            video_key, tag_name = parts
            await handle_push_tag_action(query, context, action, tag_name, video_key)
        else:
            await query.answer("❌ Invalid callback data", show_alert=True)

    elif query.data.startswith("pback_"):
        # Restore the exact previous inline keyboard (minimize the PUSH window back into the original buttons)
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("pback_", "")
        try:
            chat_id = query.message.chat_id
            message_id = query.message.message_id
            cache_key = (chat_id, message_id)
            prev_keyboard = push_prev_keyboards.pop(cache_key, None)
            # Clear any session target for this message
            try:
                push_session_targets.pop(cache_key, None)
            except Exception:
                pass

            if prev_keyboard is not None:
                # Restore cached keyboard exactly
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(prev_keyboard))
            else:
                # Fallback: rebuild with admin control row appended
                ik = query.message.reply_markup.inline_keyboard if query.message and query.message.reply_markup else []
                filtered = []
                for row in ik:
                    if not row:
                        continue
                    cb = getattr(row[0], 'callback_data', '') or ''
                    if cb.startswith(("pa_", "pr_", "pback_", "pclose_")):
                        continue
                    filtered.append(row)
                filtered += build_admin_control_row(video_key)
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(filtered))
            await query.answer("↩️ Back", show_alert=False)
        except Exception as e:
            print(f"ERROR restoring admin controls: {e}")
            await query.answer("❌ Failed to restore controls", show_alert=True)

    elif query.data.startswith("pclose_"):
        # Delete the media message entirely
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        try:
            # Clear cached keyboard for this message if it exists
            try:
                cache_key = (query.message.chat_id, query.message.message_id)
                push_prev_keyboards.pop(cache_key, None)
                push_session_targets.pop(cache_key, None)
            except Exception:
                pass
            await query.message.delete()
            try:
                unregister_active_top_viewer(query.message.chat_id, query.message.message_id)
            except Exception:
                pass
        except Exception as e:
            print(f"ERROR deleting message on close: {e}")
            await query.answer("❌ Failed to close", show_alert=True)

    elif query.data.startswith("del_media_"):
        # Admin / owner only
        if not is_admin(query.from_user.id):
            print(f"ERROR: User {query.from_user.id} is not an admin")
            await query.answer("❌ Not allowed", show_alert=False)
            return
        video_key = query.data.replace("del_media_", "")
        await delete_media_entry(query, context, video_key)
    elif query.data.startswith("revoke_media_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("revoke_media_", "")
        await revoke_media_entry(query, video_key)
    elif query.data.startswith("restore_media_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("restore_media_", "")
        await restore_media_entry(query, video_key)
    
    elif query.data.startswith("fix_media_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("fix_media_", "")
        await fix_media_entry(query, context, video_key)
    
    elif query.data.startswith("show_raw_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("show_raw_", "")
        await show_raw_media_data(query, context, video_key)

    elif query.data.startswith("raw_media_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("raw_media_", "")
        await show_raw_media_data(query, context, video_key)

    elif query.data.startswith("push_undo_move_"):
        # Handle undo for a coalesced move (two change ids)
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        try:
            _, _, idpair = query.data.partition("push_undo_move_")
            id_a_str, id_b_str = idpair.split("_")
            id_a = int(id_a_str); id_b = int(id_b_str)

            for cid in (id_a, id_b):
                entry = push_changes.get(cid)
                if not entry:
                    continue
                tag_name = entry.get("tag")
                file_id = entry.get("file_id")
                action = entry.get("action")
                item_snapshot = entry.get("item_snapshot")
                entry_index = entry.get("index")

                if action == "add":
                    # Undo add = remove from tag without shrinking if index known
                    if tag_name in media_data and isinstance(media_data[tag_name], list):
                        if isinstance(entry_index, int) and 0 <= entry_index < len(media_data[tag_name]):
                            slot = media_data[tag_name][entry_index]
                            # Only replace if the slot still matches the added item
                            if (isinstance(slot, dict) and slot.get("file_id") == file_id) or slot == item_snapshot:
                                tomb = {"deleted": True, "data": slot}
                                if isinstance(slot, dict) and "type" in slot:
                                    tomb["type"] = slot.get("type")
                                media_data[tag_name][entry_index] = tomb
                                save_media(); update_random_state()
                        else:
                            # Fallback: filter out by file_id (may shrink)
                            before = len(media_data[tag_name])
                            media_data[tag_name] = [
                                it for it in media_data[tag_name]
                                if not (isinstance(it, dict) and it.get("file_id") == file_id)
                            ]
                            if len(media_data[tag_name]) < before:
                                save_media(); update_random_state()
                elif action == "remove":
                    # Undo remove = restore in place if index known; else append back (avoid duplicates)
                    media_data.setdefault(tag_name, [])
                    if isinstance(entry_index, int) and entry_index >= 0:
                        # Ensure the list is long enough
                        while len(media_data[tag_name]) <= entry_index:
                            media_data[tag_name].append({"deleted": True, "data": None})
                        # Replace slot with original snapshot
                        media_data[tag_name][entry_index] = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
                        # Clean up deleted storage if present
                        try:
                            del deleted_media_storage[f"{tag_name}_{entry_index}"]
                        except Exception:
                            pass
                        save_media(); update_random_state(); save_deleted_media()
                    else:
                        exists = any(
                            isinstance(it, dict) and it.get("file_id") == file_id
                            for it in media_data[tag_name]
                        )
                        if not exists:
                            to_add = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
                            media_data[tag_name].append(to_add)
                            save_media(); update_random_state()

                # Remove from logs for cleanliness
                if cid in push_changes:
                    push_changes.pop(cid, None)
                if cid in push_changes_order:
                    try:
                        push_changes_order.remove(cid)
                    except ValueError:
                        pass

            # Persist updated history
            try:
                save_push_changes()
            except Exception as e:
                print(f"⚠️ Failed to persist push changes after undo move: {e}")

            await query.answer("✅ Move undone")
            # Refresh /pushst message
            try:
                # Default to first page, 10 per page
                text, reply_markup = build_push_status(10, 1)
                await query.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
            except Exception:
                pass
        except Exception as e:
            print(f"ERROR in push_undo_move: {e}")
            await query.answer("❌ Undo failed", show_alert=True)
    elif query.data.startswith("push_open_"):
        # Open media for a given change id (shows the associated file_id)
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        try:
            cid = int(query.data.replace("push_open_", ""))
        except Exception:
            await query.answer("❌ Bad id", show_alert=True)
            return

        entry = push_changes.get(cid)
        if not entry:
            await query.answer("❌ Not found", show_alert=True)
            return
        file_id = entry.get("file_id")
        snap = entry.get("item_snapshot") or {}
        mtype = None
        if isinstance(snap, dict):
            mtype = snap.get("type")

        # Find current tag/index for caption and detect type if missing
        found_tags = []
        if file_id:
            for tname, items in media_data.items():
                if not isinstance(items, list):
                    continue
                for idx, it in enumerate(items):
                    if isinstance(it, dict) and it.get("file_id") == file_id:
                        found_tags.append(f"<code>{tname}</code>")
                        if not mtype:
                            mtype = it.get("type")
                        break

        # Build caption with file_id and current tags
        tags_text = ", ".join(found_tags) if found_tags else "<i>none</i>"
        caption = (
            f"🆔 <b>File ID</b>: <code>{file_id[:50] if file_id else 'unknown'}</code>\n"
            f"� <b>Currently in tags</b>: {tags_text}"
        )
        try:
            chat_id = query.message.chat_id
            # Fallback type
            if not mtype:
                mtype = "video"
            if mtype == "video":
                await safe_send_message(context, chat_id, video=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif mtype == "photo":
                await safe_send_message(context, chat_id, photo=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif mtype == "document":
                await safe_send_message(context, chat_id, document=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif mtype == "animation":
                await safe_send_message(context, chat_id, animation=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif mtype == "audio":
                await safe_send_message(context, chat_id, audio=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif mtype == "voice":
                await safe_send_message(context, chat_id, voice=file_id, caption=caption, parse_mode=ParseMode.HTML)
            else:
                await safe_send_message(context, chat_id, document=file_id, caption=caption, parse_mode=ParseMode.HTML)
            await query.answer("👁️ View opened")
        except Exception as e:
            print(f"ERROR opening media for push_open: {e}")
            await query.answer("❌ Failed to open", show_alert=True)

    elif query.data.startswith("push_undo_"):
        # Handle undo for a specific push change id (single entry)
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        try:
            cid = int(query.data.replace("push_undo_", ""))
        except Exception:
            await query.answer("❌ Bad undo id", show_alert=True)
            return

        entry = push_changes.get(cid)
        if not entry:
            await query.answer("❌ Change not found", show_alert=True)
            return
        if entry.get("undone"):
            # Already undone; also remove from list if still present
            push_changes.pop(cid, None)
            try:
                push_changes_order.remove(cid)
            except Exception:
                pass
            await query.answer("ℹ️ Already undone")
            # Refresh
            try:
                text, reply_markup = build_push_status(10)
                await query.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
            except Exception:
                pass
            return

        tag_name = entry.get("tag")
        file_id = entry.get("file_id")
        action = entry.get("action")
        item_snapshot = entry.get("item_snapshot")

        try:
            if action == "add":
                # Undo add = remove without shrinking if possible
                if tag_name in media_data and isinstance(media_data[tag_name], list):
                    entry_index = entry.get("index")
                    if isinstance(entry_index, int) and 0 <= entry_index < len(media_data[tag_name]):
                        slot = media_data[tag_name][entry_index]
                        if (isinstance(slot, dict) and slot.get("file_id") == file_id) or slot == item_snapshot:
                            tomb = {"deleted": True, "data": slot}
                            if isinstance(slot, dict) and "type" in slot:
                                tomb["type"] = slot.get("type")
                            media_data[tag_name][entry_index] = tomb
                            save_media(); update_random_state()
                    else:
                        # Fallback: filter out by id (may shrink)
                        before = len(media_data[tag_name])
                        media_data[tag_name] = [
                            it for it in media_data[tag_name]
                            if not (isinstance(it, dict) and it.get("file_id") == file_id)
                        ]
                        if len(media_data[tag_name]) < before:
                            save_media(); update_random_state()
            elif action == "remove":
                # Undo remove = restore in place using recorded index if available
                media_data.setdefault(tag_name, [])
                entry_index = entry.get("index")
                if isinstance(entry_index, int) and entry_index >= 0:
                    while len(media_data[tag_name]) <= entry_index:
                        media_data[tag_name].append({"deleted": True, "data": None})
                    media_data[tag_name][entry_index] = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
                    try:
                        del deleted_media_storage[f"{tag_name}_{entry_index}"]
                    except Exception:
                        pass
                    save_media(); update_random_state(); save_deleted_media()
                else:
                    exists = any(
                        isinstance(it, dict) and it.get("file_id") == file_id
                        for it in media_data[tag_name]
                    )
                    if not exists:
                        to_add = deepcopy(item_snapshot) if isinstance(item_snapshot, dict) else item_snapshot
                        media_data[tag_name].append(to_add)
                        save_media(); update_random_state()
            else:
                await query.answer("❌ Unknown action", show_alert=True)
                return

            # Remove this entry from logs to keep list clean
            push_changes.pop(cid, None)
            try:
                push_changes_order.remove(cid)
            except Exception:
                pass

            # Persist updated history
            try:
                save_push_changes()
            except Exception as e:
                print(f"⚠️ Failed to persist push changes after undo: {e}")

            await query.answer("✅ Undone")

            # Refresh status message if present
            try:
                text, reply_markup = build_push_status(10, 1)
                await query.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
            except Exception:
                pass
        except Exception as e:
            print(f"ERROR in push_undo: {e}")
            await query.answer("❌ Undo failed", show_alert=True)
    elif query.data.startswith("pushst_page_"):
        # Pagination for /pushst
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        try:
            _, _, rest = query.data.partition("pushst_page_")
            page_str, per_str = rest.split("_")
            page = int(page_str); per = int(per_str)
        except Exception:
            page, per = 1, 10
        text, reply_markup = build_push_status(per, page)
        try:
            await query.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except Exception:
            pass

    elif query.data.startswith("push_refresh_"):
        # Refresh current page
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        try:
            _, _, rest = query.data.partition("push_refresh_")
            page_str, per_str = rest.split("_")
            page = int(page_str); per = int(per_str)
        except Exception:
            page, per = 1, 10
        text, reply_markup = build_push_status(per, page)
        try:
            await query.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except Exception:
            pass

    elif query.data == "push_clear":
        # Clear push changes list
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        try:
            push_changes.clear()
            push_changes_order.clear()
            global push_change_seq
            push_change_seq = 0
            # Persist clear
            try:
                save_push_changes()
            except Exception as e:
                print(f"⚠️ Failed to persist push changes after clear: {e}")
            await query.answer("🗑️ PUSH list cleared", show_alert=False)
            try:
                await query.edit_message_text(text="📋 No recent PUSH changes.")
            except Exception:
                pass
        except Exception as e:
            print(f"ERROR clearing push list: {e}")
            await query.answer("❌ Failed to clear", show_alert=True)

    elif query.data == "pushst_noop":
        # No-op for pagination info button
        await query.answer()

    elif query.data.startswith("retry_"):
        param = query.data.replace("retry_", "")

        # Parse parameter - could be just tag or tag_start_end
        if '_' in param and param.count('_') >= 2:
            parts = param.split('_')
            tag = '_'.join(parts[:-2])  # Handle tags with underscores
            try:
                start_index = int(parts[-2])
                end_index = int(parts[-1])
            except ValueError:
                tag = param
                start_index = None
                end_index = None
        else:
            tag = param
            start_index = None
            end_index = None

        # Create new context for callback query
        await send_tagged_from_callback_with_range(query, context, tag,
                                                   start_index, end_index)
    
    elif query.data.startswith("topvideos_page_range_"):
        # Handle top videos navigation with range limit
        try:
            # Parse the page number and range limit from the callback data
            parts = query.data.replace("topvideos_page_range_", "").split('_')
            page = int(parts[0])
            range_limit = int(parts[1])
            await show_top_videos_viewer(None, context, page, query, range_limit=range_limit)
        except Exception as e:
            logging.error(f"Error in topvideos_page_range_: {str(e)}")
            await query.answer("Error processing request. Please try again.", show_alert=True)
    
    elif query.data.startswith("topvideos_page_"):
        # Handle top videos navigation
        page = int(query.data.replace("topvideos_page_", ""))
        await show_top_videos_page(None, context, page, query)
    
    elif query.data == "topvideos_info":
        # Just answer the query, do nothing (info button)
        await query.answer("📊 Top Videos Navigation", show_alert=False)
    
    elif query.data.startswith("view_top_video_"):
        # Handle top video viewer navigation
        try:
            # Check if we have a range_limit in the callback data
            callback_parts = query.data.replace("view_top_video_", "").split('_')
            if len(callback_parts) > 1:
                page = int(callback_parts[0])
                range_limit = int(callback_parts[1])
                # Start sequential preview with range limit
                await handle_top_videos_preview(update, context, start_index=page, range_limit=range_limit)
            else:
                page = int(callback_parts[0])
                # Start sequential preview from page index
                await handle_top_videos_preview(update, context, start_index=page, range_limit=None)
        except Exception as e:
            logging.error(f"Error in view_top_video callback: {str(e)}")
            await query.answer(f"Error: {str(e)}", show_alert=True)
        
    elif query.data.startswith("top_range_"):
        # Handle top videos range selection
        try:
            range_limit = int(query.data.replace("top_range_", ""))
            # Show the videos viewer directly instead of a list page
            await show_top_videos_viewer(None, context, 0, query, range_limit=range_limit)
        except ValueError as e:
            logging.error(f"Error in top_range_: {str(e)}")
            await query.answer("Error processing request. Please try again.", show_alert=True)
        
    elif query.data == "top_videos_jump" or query.data == "jump_specific_rank":
        # Handle jump to specific rank request
        await query.answer("🎯 Jump to Specific Rank", show_alert=False)
        await show_jump_rank_interface(query, context)
        
    elif query.data.startswith("show_rank_range_"):
        try:
            start_rank = int(query.data.replace("show_rank_range_", ""))
            await query.answer(f"Showing ranks {start_rank}-{start_rank + 24}", show_alert=False)
            if hasattr(context, "user_data"):
                context.user_data["current_rank_range"] = start_rank
            await show_jump_rank_interface_with_range(query, context, start_rank)
        except Exception as e:
            logging.error(f"Error showing rank range: {str(e)}")
            await query.message.reply_text("Error showing rank range. Please try again.")
        return

    elif query.data == "enter_specific_rank":
        await query.answer("Enter a specific rank number", show_alert=False)
        
        # Send a message that explains how to enter a specific rank
        msg = (
            "<b>⌨️ ENTER SPECIFIC RANK</b>\n\n"
            "Reply with the top-video index you want to open. "
            "For example, send <code>45</code> to view the same item as tapping <code>#45</code>.\n\n"
            "Indices start at <code>0</code>. Your next message will be interpreted as that index. You can also pass cmd <code>/jump 45</code>."
        )
        
        # Log the action
        logging.info(f"User {query.from_user.id} requested enter_specific_rank")
        
        # Send as a new message to allow for reply
        try:
            sent = await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=msg,
                parse_mode=ParseMode.HTML
            )
            logging.info(f"Sent enter_specific_rank prompt with message_id {sent.message_id}")
            
            # Set user_data flags so the reply handler can recognize it
            context.user_data["rank_prompt_message_id"] = sent.message_id
            context.user_data["rank_prompt_chat_id"] = query.message.chat_id
            context.user_data["expecting_rank_reply"] = True
            
        except Exception as e:
            logging.error(f"Error sending enter_specific_rank prompt: {e}")
            # Try sending as plain text if HTML fails
            try:
                plain_msg = (
                    "⌨️ ENTER SPECIFIC RANK\n\n"
                    "Reply with the top-video index you want to open. "
                    "For example, send 45 to view the same item as tapping view_top_video_45.\n\n"
                    "Indices start at 0. Your next message will be interpreted as that index."
                )
                sent = await context.bot.send_message(chat_id=query.message.chat_id, text=plain_msg)
                
                # Set user_data flags for plain text version too
                context.user_data["rank_prompt_message_id"] = sent.message_id
                context.user_data["rank_prompt_chat_id"] = query.message.chat_id
                context.user_data["expecting_rank_reply"] = True
                
            except Exception as e2:
                logging.error(f"Failed to send plain text rank entry message: {str(e2)}")
        return
    elif query.data == "top_videos_navigator":
        # Show the top videos navigator menu
        total_videos = len(favorites_data.get("video_likes", {}))
        
        msg = f"🔥 <b>TOP VIDEOS NAVIGATOR</b>\n\n📊 Total videos with likes: <b>{total_videos}</b>\n\nChoose an option:\n\n• View specific ranges (Top 10/25/50)\n• Start viewing from the beginning\n• Jump to a specific rank\n\nVideos are sorted by number of likes (❤️) in descending order."
        
        keyboard = [
            [
                InlineKeyboardButton("🔝 Top 10", callback_data="view_top_video_0_10"),
                InlineKeyboardButton("🔝 Top 25", callback_data="view_top_video_0_25"),
                InlineKeyboardButton("🔝 Top 50", callback_data="view_top_video_0_50")
            ],
            [InlineKeyboardButton("🎬 Start Viewing Top Videos", callback_data="view_top_video_0")],
            [InlineKeyboardButton("🎯 Jump to Specific Rank", callback_data="jump_specific_rank")],
            [InlineKeyboardButton("🏠 Home", callback_data="return_home")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)

        chat_id = query.message.chat_id

        try:
            _sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=msg,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        except Exception as send_error:
            logging.error(f"Failed to send top videos navigator message: {send_error}")
            try:
                await query.edit_message_text(msg, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            except Exception as nested_error:
                logging.error(
                    f"Failed to show top videos navigator: send error {send_error}, edit error {nested_error}"
                )
                await query.answer("❌ Error loading navigator", show_alert=True)
            else:
                logging.debug("Navigator shown via edit fallback")
            return

        # Only remove the original message after successfully sending the navigator
        try:
            await query.message.delete()
        except Exception as delete_error:
            logging.debug(f"Could not delete previous message: {delete_error}")

        try:
            await query.answer("📊 Navigator opened", show_alert=False)
        except Exception as answer_error:
            logging.debug(f"Unable to answer callback query: {answer_error}")
        
    elif query.data == "return_home":
        # Return to main menu
        chat_id = query.message.chat_id
        await query.message.delete()
        await query.answer("Returning to home 🏠")
        
        # Send home menu
        keyboard = build_initial_reply_keyboard()
        await context.bot.send_message(chat_id=chat_id, text="Welcome to Bhaichara Bot! 🤖", reply_markup=keyboard)
    
    elif query.data == "close_menu":
        # Close the menu by deleting the message
        await query.message.delete()
        await query.answer("Menu closed", show_alert=False)
    
    elif query.data.startswith("topusers_page_"):
        # Handle top users navigation
        page = int(query.data.replace("topusers_page_", ""))
        await show_top_users_page(None, context, page, query)
    
    elif query.data == "topusers_info":
        # Just answer the query, do nothing (info button)
        await query.answer("👥 Top Users Navigation", show_alert=False)
    
    elif query.data == "close_menu":
        # Close the inline menu
        await query.edit_message_text("✅ Menu closed.")
    
    elif query.data == "random_safe":
        # Send random safe video (18-)
        await query.message.delete()  # Remove the selection menu
        await send_random_video(context, query.from_user.id, mode="safe")
        await query.answer("🎬 Sending random 18- video...", show_alert=False)
    
    elif query.data == "random_media":
        # Send random media video (18+)  
        await query.message.delete()  # Remove the selection menu
        await send_random_video(context, query.from_user.id, mode="mediax", user_id=query.from_user.id)
        await query.answer("🎲 Sending random 18+ video...", show_alert=False)
    
    elif query.data == "confirm_clear_deleted":
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        deleted_count = len(deleted_media_storage)
        deleted_media_storage.clear()
        save_deleted_media()
        
        await query.answer("🗑️ All deleted media cleared", show_alert=False)
        await query.edit_message_text(
            f"✅ <b>Deletion Complete</b>\n\n"
            f"🗑️ Permanently removed {deleted_count} deleted media entries.\n"
            f"This action cannot be undone.",
            parse_mode=ParseMode.HTML
        )
    
    elif query.data == "cancel_clear_deleted":
        await query.answer("❌ Clear operation cancelled", show_alert=False)
        await query.edit_message_text(
            "❌ <b>Operation Cancelled</b>\n\n"
            "No deleted media was cleared.",
            parse_mode=ParseMode.HTML
        )
    
    elif query.data == "list_deleted_media":
        # Handle "Back to Deleted List" button
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        # Just simulate the listdeleted command call properly
        try:
            # Create a minimal update object that works with listdeleted_command
            class FakeMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.from_user = from_user
                    
                async def reply_text(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
                    
                async def reply_html(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML, **kwargs)
            
            class FakeUpdate:
                def __init__(self, user, chat_id):
                    self.effective_user = user
                    self.message = FakeMessage(chat_id, user)
            
            fake_update = FakeUpdate(query.from_user, query.message.chat_id)
            
            # Delete the current message first
            try:
                await query.message.delete()
            except Exception as e:
                print(f"DEBUG: Could not delete message: {e}")
            
            # Call the actual listdeleted command
            await listdeleted_command(fake_update, context)
            await query.answer("🔄 Deleted list refreshed!")
            
        except Exception as e:
            print(f"ERROR in list_deleted_media callback: {e}")
            await query.answer("❌ Error refreshing list")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"❌ Error refreshing deleted list: {str(e)}"
            )
    
    elif query.data == "view_revoked_list":
        # Handle view revoked list button from revoke confirmation
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        try:
            # Create fake update to call listremoved_cmd
            class FakeMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.from_user = from_user
                    
                async def reply_text(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
                    
                async def reply_html(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML, **kwargs)
            
            class FakeUpdate:
                def __init__(self, user, chat_id):
                    self.effective_user = user
                    self.message = FakeMessage(chat_id, user)
            
            class FakeContext:
                def __init__(self):
                    self.args = []
                    self.bot = context.bot
            
            fake_update = FakeUpdate(query.from_user, query.message.chat_id)
            fake_context = FakeContext()
            
            await listremoved_cmd(fake_update, fake_context)
            await query.answer("📋 Showing all removed media!")
            
        except Exception as e:
            print(f"ERROR in view_revoked_list callback: {e}")
            await query.answer("❌ Error loading list")
    
    elif query.data.startswith("deleted_page_"):
        # Handle deleted media pagination
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        if query.data == "deleted_page_info":
            await query.answer("📄 Page navigation", show_alert=False)
            return
        
        try:
            page = int(query.data.replace("deleted_page_", ""))
            
            # Create fake update to call listdeleted_command with page
            class FakeMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.from_user = from_user
                    
                async def reply_text(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
                    
                async def reply_html(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML, **kwargs)
            
            class FakeUpdate:
                def __init__(self, user, chat_id):
                    self.effective_user = user
                    self.message = FakeMessage(chat_id, user)
            
            fake_update = FakeUpdate(query.from_user, query.message.chat_id)
            
            # Delete current message and show new page
            try:
                await query.message.delete()
            except Exception as e:
                print(f"DEBUG: Could not delete message: {e}")
            
            await listdeleted_command(fake_update, context, page)
            await query.answer(f"📄 Page {page + 1}")
            
        except ValueError:
            await query.answer("❌ Invalid page number")
    
    elif query.data == "cleanup_deleted":
        # Handle cleanup button - PERMANENTLY DELETE ALL deleted media (same as /cleardeleted)
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        if not deleted_media_storage:
            await query.answer("✅ No deleted media to clean up", show_alert=False)
            return
        
        # Immediately permanently delete all deleted media (no confirmation for button)
        deleted_count = len(deleted_media_storage)
        deleted_media_storage.clear()
        save_deleted_media()
        
        await query.answer(f"🗑️ Permanently deleted {deleted_count} items from database!", show_alert=True)
        
        # Update the message to show the list is now empty
        await query.edit_message_text(
            "🗑️ <b>Deleted Media (Page 1/1)</b>\n\n"
            "📋 No deleted media found - database is clean!\n\n"
            "All deleted media has been permanently removed.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_deleted_list")]
            ])
        )
    
    elif query.data == "refresh_deleted_list":
        # Handle refresh button
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        # Same as list_deleted_media handler but for refresh button
        try:
            class FakeMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.from_user = from_user
                    
                async def reply_text(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
                    
                async def reply_html(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML, **kwargs)
            
            class FakeUpdate:
                def __init__(self, user, chat_id):
                    self.effective_user = user
                    self.message = FakeMessage(chat_id, user)
            
            fake_update = FakeUpdate(query.from_user, query.message.chat_id)
            
            try:
                await query.message.delete()
            except Exception as e:
                print(f"DEBUG: Could not delete message: {e}")
            
            await listdeleted_command(fake_update, context, 0)  # Always go to first page on refresh
            await query.answer("🔄 List refreshed!")
            
        except Exception as e:
            print(f"ERROR in refresh_deleted_list callback: {e}")
            await query.answer("❌ Error refreshing list")

    elif query.data.startswith("revoked_page_"):
        # Handle revoked media pagination
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        if query.data == "revoked_page_info":
            await query.answer("📄 Page navigation", show_alert=False)
            return
        
        try:
            page = int(query.data.replace("revoked_page_", ""))
            
            # Generate updated content for the requested page
            text, reply_markup = generate_revoked_list(page)
            
            if reply_markup is None:
                # Empty page or no revoked media
                await query.edit_message_text(text)
            else:
                # Update the message with new page content
                await query.edit_message_text(
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            
            await query.answer(f"📄 Page {page}")
            
        except ValueError:
            await query.answer("❌ Invalid page number")
        except Exception as e:
            print(f"ERROR in revoked_page callback: {e}")
            await query.answer("❌ Error loading page")
    
    elif query.data == "cleanup_revoked":
        # Handle cleanup button - Remove all revoked flags
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        # Count and remove all revoked flags
        revoked_count = 0
        for tag, vids in media_data.items():
            if not isinstance(vids, list):
                continue
            for v in vids:
                if isinstance(v, dict) and v.get("revoked"):
                    v.pop("revoked", None)
                    revoked_count += 1
        
        if revoked_count == 0:
            await query.answer("✅ No revoked media to clean up", show_alert=False)
            return
        
        save_media()
        update_random_state()
        
        await query.answer(f"♻️ Restored {revoked_count} revoked items!", show_alert=True)
        
        # Update the message to show the list is now empty
        await query.edit_message_text(
            "🛑 <b>Revoked Media (Page 1/1)</b>\n\n"
            "📋 No revoked media found - all have been restored!\n\n"
            "All revoked media has been restored to active status.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_revoked_list")]
            ])
        )
    
    elif query.data == "refresh_revoked_list":
        # Handle refresh button for revoked media
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        try:
            # Generate updated content - always show page 1 on refresh
            text, reply_markup = generate_revoked_list(1)
            
            if reply_markup is None:
                # No revoked media found
                await query.edit_message_text(text)
            else:
                # Update the message with new content
                await query.edit_message_text(
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            
            await query.answer("🔄 List refreshed!")
            
        except Exception as e:
            print(f"ERROR in refresh_revoked_list callback: {e}")
            await query.answer("❌ Error refreshing list")

    elif query.data.startswith("confirm_delete_backup_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Admin access required", show_alert=True)
            return

        if not LOCAL_BACKUPS_ENABLED or backup_manager is None:
            await query.edit_message_text("ℹ️ Local backups are disabled on this bot.")
            return

        backup_name = query.data.replace("confirm_delete_backup_", "")
        try:
            loop = asyncio.get_event_loop()
            success, message = await loop.run_in_executor(
                None, backup_manager.delete_backup, backup_name
            )
            if success:
                await query.edit_message_text(f"✅ {message}")
            else:
                await query.edit_message_text(f"❌ {message}")
        except Exception as e:
            logging.error(f"Error deleting backup '{backup_name}': {str(e)}")
            await query.answer("❌ Failed to delete backup", show_alert=True)

    elif query.data == "cancel_delete_backup":
        await query.edit_message_text("❌ Backup deletion cancelled.")

    elif query.data == "test_image_ok":
        await query.edit_message_text("✅ Welcome image is working correctly!")

    elif query.data == "test_image_fail":
        await query.edit_message_text("❌ Welcome image failed to load. Check file_id or use /setwelcomeimage")


# Add a dictionary to track processing tasks for media groups
media_group_tasks = {}


async def process_media_group(media_group_id, delay=3):
    """Process a media group after a delay to ensure all files are collected"""
    await asyncio.sleep(delay)

    if media_group_id in media_tag_map and media_group_id in media_buffer:
        tag = media_tag_map.pop(media_group_id)
        items = sorted(media_buffer.pop(media_group_id),
                       key=lambda x: x["msg_id"])

        media_data.setdefault(tag, []).extend([{
            "file_id": x["file_id"],
            "type": x["type"],
            **({} if not x.get("filename") else {"filename": x["filename"]}),
            **({} if not x.get("caption") else {"caption": x["caption"]})
        } for x in items])
        save_media()

        # Send confirmation (we'll need to get the last message context)
        print(f"✅ Album of {len(items)} files uploaded under tag '{tag}'.")

    # Clean up the task reference
    if media_group_id in media_group_tasks:
        del media_group_tasks[media_group_id]


async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        # Check if regular user has active custom batch session
        if await handle_custom_batch_media(update, context):
            return
        return

    # Auto-register user (this handles all user tracking)
    await auto_register_user(update, context)

    # Check if admin has active AI batch session (priority over custom batch)
    if await handle_ai_batch_media(update, context):
        return

    # Check if admin has active custom batch session
    if await handle_custom_batch_media(update, context):
        return

    message = update.message
    if not message:
        return

    media_group_id = message.media_group_id
    caption = message.caption
    # Support multiple media types
    photo = message.photo[-1] if message.photo else None
    video = message.video
    document = message.document
    audio = message.audio
    voice = message.voice
    animation = getattr(message, 'animation', None)
    sticker = message.sticker
    file_id = None
    media_type = None
    filename = None
    if photo:
        file_id = photo.file_id; media_type = "photo"
    elif video:
        file_id = video.file_id; media_type = "video"
        filename = getattr(video, 'file_name', None)
    elif document:
        file_id = document.file_id; media_type = "document"
        filename = getattr(document, 'file_name', None)
    elif audio:
        file_id = audio.file_id; media_type = "audio"
        filename = getattr(audio, 'file_name', None)
    elif voice:
        file_id = voice.file_id; media_type = "voice"
    elif animation:
        file_id = animation.file_id; media_type = "animation"
        filename = getattr(animation, 'file_name', None)
    elif sticker:
        file_id = sticker.file_id; media_type = "sticker"

    if not file_id:
        return

    if media_group_id:
        # Store tag if this message has the upload command
        if caption and caption.lower().startswith("/upload "):
            tag = caption.split(" ", 1)[1].strip().lower()
            media_tag_map[media_group_id] = tag

        # Add file to buffer with filename and caption
        media_buffer[media_group_id].append({
            "file_id": file_id, 
            "type": media_type, 
            "msg_id": message.message_id,
            "filename": filename,
            "caption": caption
        })

        # Cancel existing task if any and create a new one
        if media_group_id in media_group_tasks:
            media_group_tasks[media_group_id].cancel()

        # Create new processing task with delay
        media_group_tasks[media_group_id] = asyncio.create_task(process_media_group(media_group_id))

        # Send confirmation for the last processed file
        if media_group_id in media_tag_map:
            tag = media_tag_map[media_group_id]
            current_count = len(media_buffer[media_group_id])
            await message.reply_text(f"📁 Collecting files for tag '{tag}' ({current_count} files so far)...")
    else:
        # Single file upload
        if caption and caption.lower().startswith("/upload "):
            tag = caption.split(" ", 1)[1].strip().lower()
            media_entry = {"file_id": file_id, "type": media_type}
            if filename:
                media_entry["filename"] = filename
            if caption:
                media_entry["caption"] = caption
            media_data.setdefault(tag, []).append(media_entry)
            save_media()
            await message.reply_text(f"✅ {media_type.capitalize()} uploaded under tag '{tag}'.")



















async def send_single_passlink_video(update: Update, context: ContextTypes.DEFAULT_TYPE, item: dict, tag: str, idx: int, actual_indices: list, start_index: int):
    """Send a single video from a passlink - works for both messages and callbacks."""
    # Resolve user and chat from Update or CallbackQuery
    user_id = update.effective_user.id if hasattr(update, 'effective_user') and update.effective_user else (
        update.callback_query.from_user.id if hasattr(update, 'callback_query') and update.callback_query else None
    )
    if hasattr(update, 'message') and update.message:
        chat_id = update.message.chat_id
        chat_type = update.message.chat.type
    elif hasattr(update, 'callback_query') and update.callback_query and update.callback_query.message:
        chat_id = update.callback_query.message.chat_id
        chat_type = update.callback_query.message.chat.type
    else:
        chat_id = None
        chat_type = None
    
    # Get adaptive cooldown value based on chat behavior
    cooldown_value = PASSLINK_COOLDOWN
    if chat_id in dynamic_cooldowns:
        # Apply dynamic cooldown if available (more aggressive than regular adaptive cooldown)
        cooldown_value *= dynamic_cooldowns.get(chat_id, 1.0)
    
    # Optimize cooldown for first videos for better user experience
    if idx < 3:
        # First few videos deliver quickly
        cooldown_value = min(0.01, cooldown_value * 0.1)
    
    # Apply dynamic passlink cooldown before sending
    await asyncio.sleep(cooldown_value)
    
    # Use both global rate limiting and per-user fair allocation
    # IMPORTANT: Acquire per-user semaphore FIRST. Scope the global semaphore ONLY
    # around the actual Telegram send to avoid holding global capacity during
    # per-user delays and pre/post processing.
    async with get_user_semaphore(user_id):  # Per-user fairness
        try:
            # idx is now the actual media_data index (fixed in send_passlink_videos)
            actual_index = idx

            # Create unique shareable link for this specific file
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{actual_index}_{actual_index}"
            share_link = f"https://telegram.me/share/url?url={file_link}"

            # Build proper media caption with global caption and replacements
            media_type = item.get("type", "video")
            cap = build_media_caption("", tag, str(actual_index), share_link, media_type)

            # Check if media is revoked (show for admins only)
            if isinstance(item, dict) and item.get("revoked"):
                if is_admin(update.effective_user.id):
                    cap += "\n\n🛑 <b>This media is revoked</b>"
                else:
                    return False  # Skip for regular users

            # Handle corrupted media: skip silently for smoother flows
            if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
                return False

            # Skip deleted entries for bulk sending
            if isinstance(item, dict) and item.get("deleted"):
                return False

            # Create favorite button
            video_id = f"{tag}_{actual_index}"
            user_id_str = str(update.effective_user.id)
            file_id = item.get('file_id') if isinstance(item, dict) else None
            is_favorited = is_video_favorited(user_id_str, tag, actual_index, file_id)

            if is_favorited:
                fav_button = InlineKeyboardButton("💔 Remove", 
                                                callback_data=f"remove_fav_{video_id}")
            else:
                fav_button = InlineKeyboardButton("❤️ Add", 
                                                callback_data=f"add_fav_{video_id}")

            # Check if user is admin
            if is_admin(update.effective_user.id):
                who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_id}")
                # Remove MY FAV and RANDOM from admin inline buttons
                rows = [
                    [fav_button, who_liked_button]
                ]
                rows += build_admin_control_row(video_id)
                keyboard = InlineKeyboardMarkup(rows)
            else:
                # Only show ADD button for regular users
                keyboard = InlineKeyboardMarkup([
                    [fav_button]
                ])
           

            # Send the media (scope global semaphore ONLY around the send)
            if item["type"] == "video":
                async with video_send_semaphore:  # Global rate limiting
                    await safe_send_message(
                        context=context,
                        chat_id=chat_id,
                        video=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                        user_id=user_id
                    )
            elif item["type"] == "photo":
                async with video_send_semaphore:  # Global rate limiting
                    await safe_send_message(
                        context=context,
                        chat_id=chat_id,
                        photo=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(update.effective_user.id, chat_id),
                        user_id=user_id
                    )
            else:
                # Use helper function to send media by type
                async with video_send_semaphore:  # Global rate limiting
                    await send_media_by_type(
                        context=context,
                        chat_id=chat_id,
                        item=item,
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(update.effective_user.id, chat_id),
                        user_id=user_id
                    )

            # Removed artificial per-item delay; rely on safe_send_message backoff and global semaphore
            return True

        except Exception as e:
            print(f"Error sending video {idx}: {e}")
            return False


async def send_passlink_videos(update: Update, context: ContextTypes.DEFAULT_TYPE, param: str, link_data: dict):
    """Send videos from a passlink with smart adaptive parallel processing"""
    user_id = update.effective_user.id
    chat_id = update.message.chat_id
    chat_type = update.message.chat.type
    
    # Extract link information
    tag = link_data.get("tag", "")
    start_index = link_data.get("start_index", 0)
    end_index = link_data.get("end_index", 0)
    videos = link_data.get("videos", [])
    
    if not videos:
        await update.message.reply_text("❌ No videos found in this link")
        return
        
    await safe_send_message(
        context=context,
        chat_id=update.message.chat_id,
        text=f"📁 Showing videos from tag '<code>{tag}</code>' (indexes {start_index}-{end_index}, {len(videos)} files):",
        parse_mode=ParseMode.HTML
    )
    
    # Determine optimal batch size based on chat type and flood history
    base_batch_size = 1
    if chat_type == "private":
        base_batch_size = 4  # Faster for private chats
    elif chat_type == "group":
        base_batch_size = 2  # Medium for groups
    
    # Adjust batch size if we've had flooding issues
    if chat_id in dynamic_cooldowns:
        flood_factor = dynamic_cooldowns[chat_id]
        # Reduce batch size based on flood history
        if flood_factor > 1.5:
            base_batch_size = 1  # Very conservative
        elif flood_factor > 1.2:
            base_batch_size = max(1, base_batch_size // 2)  # Halve batch size
    
    # Get actual indices for link data - ensure all are ints
    actual_indices_raw = link_data.get("actual_indices", list(range(start_index, end_index + 1)))
    # Convert any string indices to int (can happen if loaded from JSON)
    actual_indices = [int(x) if isinstance(x, str) else x for x in actual_indices_raw]
    
    # Send videos with optimized adaptive parallel processing
    sent_results = []
    valid_videos = []
    
    # Filter videos that should be sent
    # IMPORTANT: idx here is the local index in the videos array (0-based from videos list)
    # We need to map it to actual_indices to get the real media_data index
    for local_idx, item in enumerate(videos):
        # Skip corrupted/deleted media for regular users
        if (isinstance(item, dict) and item.get("deleted")) or not isinstance(item, dict) or "type" not in item or "file_id" not in item:
            if not is_admin(update.effective_user.id):
                continue
        
        # Skip revoked media for regular users
        if isinstance(item, dict) and item.get("revoked") and not is_admin(update.effective_user.id):
            continue
        
        # Get the actual media_data index for this video
        actual_idx = actual_indices[local_idx] if local_idx < len(actual_indices) else start_index + local_idx
        valid_videos.append((actual_idx, item))
    
    # Setup Stop button if multiple videos
    cancel_key = f"passlink_cancel_{chat_id}"
    context.chat_data[cancel_key] = False
    
    if len(valid_videos) > 1:
        stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True, one_time_keyboard=False)
        await update.message.reply_text("Tap 🛑 Stop to cancel anytime.", reply_markup=stop_keyboard)
    
    # Process videos with dynamic adaptive concurrency
    if valid_videos:
        # Send first few videos immediately for better user experience
        initial_batch = valid_videos[:3]
        for idx, item in initial_batch:
            # Check cancel flag
            if context.chat_data.get(cancel_key):
                    delivered = sum(1 for r in sent_results if r is True)
                    resume_id = f"{user_id}_{int(time.time()*1000)}"
                    remaining = valid_videos[len(sent_results):]
                    delivered_ct = sum(1 for r in sent_results if r is True)
                    resume_states[resume_id] = {
                        'mode': 'passlink',
                        'tag': tag,
                        'remaining': remaining,
                        'delivered': delivered_ct,
                        'total': len(valid_videos),
                        'actual_indices': actual_indices,
                        'start_index': start_index,
                        'chat_id': chat_id,
                        'user_id': user_id
                    }
                    resume_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("▶️ Continue Sending", callback_data=f"resume_{resume_id}")],
                        [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
                    ])
                    # Build cancel summary; only show auto-delete notice for non-admin users
                    cancel_text = (
                        f"⛔ Send operation stopped.\n\n"
                        f"✅ Delivered: {delivered}/{len(valid_videos)} videos from tag '{tag}'.\n\n"
                    )
                    if not is_admin(user_id):
                        cancel_text += f"⏰ Auto-Delete Notice: All media will be automatically deleted after {AUTO_DELETE_HOURS} hour(s).\n\n"
                    cancel_text += "💡 Use ❤️ ADD to save favorites."
                    await update.message.reply_html(cancel_text, reply_markup=resume_kb)
                    context.chat_data[cancel_key] = False
                    return
            
            try:
                result = await send_single_passlink_video(update, context, item, tag, idx, actual_indices, start_index)
                sent_results.append(result)
                
                # Detect any flooding and adjust parameters dynamically
                if chat_id in dynamic_cooldowns and dynamic_cooldowns[chat_id] > 1.2:
                    # If we've hit flood control, reduce batch size immediately
                    base_batch_size = 1
                    # Break out of initial batch to use more conservative approach
                    break
                    
            except RetryAfter as e:
                # Handle flood control - adapt immediately
                dynamic_cooldowns[chat_id] = min(5.0, dynamic_cooldowns.get(chat_id, 1.0) * 1.5) 
                flood_control_timestamps[chat_id] = time.time()
                base_batch_size = 1  # Switch to sequential processing
                sent_results.append(False)
                await asyncio.sleep(e.retry_after + 1)
                
            except Exception as e:
                sent_results.append(False)
        
        # Process remaining videos with adaptive batch sizes
        remaining = valid_videos[len(sent_results):]  # Skip already processed items
        if remaining:
            current_batch_size = base_batch_size
            for i in range(0, len(remaining), current_batch_size):
                # Check cancel flag
                if context.chat_data.get(cancel_key):
                    delivered = sum(1 for r in sent_results if r is True)
                    resume_id = f"{user_id}_{int(time.time()*1000)}"
                    remaining = valid_videos[len(sent_results):]
                    delivered_ct = sum(1 for r in sent_results if r is True)
                    resume_states[resume_id] = {
                        'mode': 'passlink',
                        'tag': tag,
                        'remaining': remaining,
                        'delivered': delivered_ct,
                        'total': len(valid_videos),
                        'actual_indices': actual_indices,
                        'start_index': start_index,
                        'chat_id': chat_id,
                        'user_id': user_id
                    }
                    resume_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("▶️ Continue Sending", callback_data=f"resume_{resume_id}")],
                        [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
                    ])
                    cancel_text = (
                        f"⛔ Send operation stopped.\n\n"
                        f"✅ Delivered: {delivered}/{len(valid_videos)} videos from tag '{tag}'.\n\n"
                    )
                    if not is_admin(user_id):
                        cancel_text += f"⏰ Auto-Delete Notice: All media will be automatically deleted after {AUTO_DELETE_HOURS} hour(s).\n\n"
                    cancel_text += "💡 Use ❤️ ADD to save favorites."
                    await update.message.reply_html(cancel_text, reply_markup=resume_kb)
                    context.chat_data[cancel_key] = False
                    return
                
                # Get current batch with dynamic size
                batch = remaining[i:i+current_batch_size]
                
                try:
                    tasks = []
                    for idx, item in batch:
                        task = send_single_passlink_video(update, context, item, tag, idx, actual_indices, start_index)
                        tasks.append(task)
                    
                    # Process batch
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Check for exceptions and adjust batch size if needed
                    had_exception = False
                    for result in batch_results:
                        if isinstance(result, Exception):
                            had_exception = True
                            if isinstance(result, RetryAfter):
                                # Reduce batch size on flood control
                                current_batch_size = 1
                                dynamic_cooldowns[chat_id] = min(5.0, dynamic_cooldowns.get(chat_id, 1.0) * 1.5)
                                flood_control_timestamps[chat_id] = time.time()
                                await asyncio.sleep(result.retry_after + 1)
                            sent_results.append(False)
                        else:
                            sent_results.append(result)
                    
                    # If this batch had no exceptions and was small, try increasing batch size
                    if not had_exception and current_batch_size < base_batch_size:
                        current_batch_size = min(current_batch_size + 1, base_batch_size)
                        
                    # Brief pause between batches to prevent flooding
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    # Fallback error handling
                    print(f"Error processing batch: {e}")
                    sent_results.extend([False] * len(batch))
                    current_batch_size = 1  # Reduce batch size on error
    else:
        # No valid videos to send
        pass
    
    # Helper to get appropriate keyboard - always show user keyboard after media sends
    def get_home_keyboard_for_user(uid):
        # Always return user keyboard for post-send completions (even for admins)
        return ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
    
    # Count successful sends
    sent_count = sum(1 for result in sent_results if result is True)
    total_files = len(videos)
    
    # Clear cancel flag and restore home keyboard after completion (remove Stop button)
    context.chat_data[cancel_key] = False
    home_keyboard = get_home_keyboard_for_user(user_id)
    
    # Send completion message
    if sent_count > 0:
        # Check if operation was cancelled
        completion_text = ""
        was_cancelled = context.chat_data.get(cancel_key, False)
        if was_cancelled:
            completion_text = (
                f"⛔ <b>Send operation stopped by user.</b>\n\n"
                f"✅ Delivered: {sent_count} out of {total_files} videos from tag '{tag}'\n\n"
            )
        else:
            completion_text = (
                f"✅ Successfully sent {sent_count} out of {total_files} videos from tag '{tag}'\n\n"
            )

        # Only add auto-delete notice for non-admin users
        if not is_admin(user_id):
            completion_text += (
                f"⏰ <b>Auto-Delete Notice:</b> All media will be automatically deleted after {AUTO_DELETE_HOURS} hour(s) for storage optimization.\n\n"
            )
        completion_text += (
            f"💡 <b>Save to Favorites:</b> Use the ❤️ ADD button on any video to add it to your favorites for permanent access!"
        )

        # Send completion with home keyboard and inline favorites button
        await context.bot.send_message(
            chat_id=update.message.chat_id,
            text=completion_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
            ])
        )
        
        # Send home keyboard separately to ensure it's set properly
        await context.bot.send_message(
            chat_id=update.message.chat_id,
            text="Select an option below:",
            reply_markup=home_keyboard
        )
    else:
        await context.bot.send_message(
            chat_id=update.message.chat_id,
            text=f"❌ No videos could be sent from tag '{tag}'",
            reply_markup=home_keyboard
        )


async def send_single_media_item(query_or_update, context, tag: str, idx: int, is_callback: bool = False):
    """Send a single media item - used for parallel sending"""
    try:
        # Determine if this is from callback query or regular update
        if is_callback:
            user_id = query_or_update.from_user.id
            chat_id = query_or_update.message.chat_id
            reply_func = query_or_update.message.reply_video if hasattr(query_or_update.message, 'reply_video') else None
        else:
            user_id = query_or_update.effective_user.id
            chat_id = query_or_update.message.chat_id
            reply_func = query_or_update.message.reply_video if hasattr(query_or_update.message, 'reply_video') else None

        # Use per-user semaphore for fairness, global semaphore only around actual send
        async with get_user_semaphore(user_id):
            # Check if index is out of range (media was deleted)
            if idx >= len(media_data[tag]):
                # For admins, show deleted media with restore option if it exists
                if is_admin(user_id):
                    video_key = f"{tag}_{idx}"
                    if video_key in deleted_media_storage:
                        deleted_info = deleted_media_storage[video_key]
                        deleted_item = deleted_info["data"]
                        
                        # Create unique shareable link for this specific file
                        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                        share_link = f"https://telegram.me/share/url?url={file_link}"
                        
                        # Build proper media caption with global caption and replacements
                        media_type = deleted_item.get("type", "video")
                        cap = build_media_caption("", tag, str(idx), share_link, media_type)
                        cap += "\n\n🗑️ <b>This media was deleted</b>"
                        
                        # Create favorite button
                        user_id_str = str(user_id)
                        file_id = deleted_item.get('file_id') if isinstance(deleted_item, dict) else None
                        is_favorited = is_video_favorited(user_id_str, tag, idx, file_id)

                        if is_favorited:
                            fav_button = InlineKeyboardButton("💔 Remove", 
                                                            callback_data=f"remove_fav_{video_key}")
                        else:
                            fav_button = InlineKeyboardButton("❤️ Add", 
                                                            callback_data=f"add_fav_{video_key}")

                        # Admin buttons for deleted media (remove MY FAV and RANDOM)
                        who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_key}")
                        restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")
                        
                        keyboard = InlineKeyboardMarkup([
                            [fav_button, who_liked_button],
                            [restore_button]
                        ])
                        
                        media_kwargs = {
                            "context": context,
                            "chat_id": chat_id,
                            "caption": cap,
                            "reply_markup": keyboard,
                            "parse_mode": ParseMode.HTML,
                            "protect_content": should_protect_content(user_id, chat_id),
                            "user_id": user_id
                        }
                        
                        if deleted_item["type"] == "video":
                            media_kwargs["video"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "photo":
                            media_kwargs["photo"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "document":
                            media_kwargs["document"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "audio":
                            media_kwargs["audio"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "voice":
                            media_kwargs["voice"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "animation":
                            media_kwargs["animation"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "sticker":
                            media_kwargs["sticker"] = deleted_item["file_id"]
                            # Stickers don't support captions
                            media_kwargs.pop("caption", None)
                        else:
                            # Fallback for unknown types
                            media_kwargs["text"] = f"Deleted media type: {deleted_item['type']}"
                        
                        async with video_send_semaphore:  # Global rate limiting
                            await safe_send_message(**media_kwargs)
                        return True
                # For regular users, silently skip without any notification
                return False
                
            item = media_data[tag][idx]

            # Handle deleted tombstone entries: show admin restore option, skip for users
            if isinstance(item, dict) and item.get("deleted"):
                if is_admin(user_id):
                    deleted_item = item.get("data", {})
                    file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                    share_link = f"https://telegram.me/share/url?url={file_link}"
                    media_type = (deleted_item.get("type") if isinstance(deleted_item, dict) else "video") or "video"
                    cap = build_media_caption("", tag, str(idx), share_link, media_type)
                    cap += "\n\n🗑️ <b>This media was deleted</b>"

                    video_id = f"{tag}_{idx}"
                    user_id_str = str(user_id)
                    file_id = deleted_item.get('file_id') if isinstance(deleted_item, dict) else None
                    is_favorited = is_video_favorited(user_id_str, tag, idx, file_id)
                    fav_button = InlineKeyboardButton("💔 Remove" if is_favorited else "❤️ Add", callback_data=(f"remove_fav_{video_id}" if is_favorited else f"add_fav_{video_id}"))
                    who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_id}")
                    restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
                    keyboard = InlineKeyboardMarkup([[fav_button, who_liked_button], [restore_button]])

                    # Try sending as media if we have a file_id in snapshot
                    media_kwargs = {
                        "context": context,
                        "chat_id": chat_id,
                        "caption": cap,
                        "reply_markup": keyboard,
                        "parse_mode": ParseMode.HTML,
                        "protect_content": should_protect_content(user_id, chat_id),
                        "user_id": user_id
                    }
                    if isinstance(deleted_item, dict) and "file_id" in deleted_item:
                        # Prefer original media type
                        dt = deleted_item.get("type", "video")
                        if dt == "video":
                            media_kwargs["video"] = deleted_item["file_id"]
                        elif dt == "photo":
                            media_kwargs["photo"] = deleted_item["file_id"]
                        elif dt == "document":
                            media_kwargs["document"] = deleted_item["file_id"]
                        elif dt == "audio":
                            media_kwargs["audio"] = deleted_item["file_id"]
                        elif dt == "voice":
                            media_kwargs["voice"] = deleted_item["file_id"]
                        elif dt == "animation":
                            media_kwargs["animation"] = deleted_item["file_id"]
                        elif dt == "sticker":
                            media_kwargs["sticker"] = deleted_item["file_id"]
                            media_kwargs.pop("caption", None)
                        else:
                            media_kwargs["text"] = cap
                            media_kwargs.pop("caption", None)
                    else:
                        media_kwargs["text"] = cap
                        media_kwargs.pop("caption", None)

                    async with video_send_semaphore:
                        await safe_send_message(**media_kwargs)
                return True if is_admin(user_id) else False
            
            # Skip if item is not a valid dictionary or missing required fields (skip silently)
            if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
                return False
                
            # Create unique shareable link for this specific file
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            share_link = f"https://telegram.me/share/url?url={file_link}"
            
            # Build proper media caption with global caption and replacements
            media_type = item.get("type", "video")
            cap = build_media_caption("", tag, str(idx), share_link, media_type)
            
            # Create favorite button
            video_id = f"{tag}_{idx}"
            user_id_str = str(user_id)
            file_id = item.get('file_id') if isinstance(item, dict) else None
            is_favorited = is_video_favorited(user_id_str, tag, idx, file_id)
            
            if is_favorited:
                fav_button = InlineKeyboardButton("💔 Remove", 
                                                callback_data=f"remove_fav_{video_id}")
            else:
                fav_button = InlineKeyboardButton("❤️ Add", 
                                                callback_data=f"add_fav_{video_id}")
            
            # Check if user is admin to show additional admin buttons
            if is_admin(user_id):
                who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_id}")
                rows = [
                    [fav_button, who_liked_button]
                ]
                rows += build_admin_control_row(video_id)
                keyboard = InlineKeyboardMarkup(rows)
            else:
                # Only show ADD button for regular users
                keyboard = InlineKeyboardMarkup([
                    [fav_button]
                ])
            
            # Send the media with global semaphore only around actual send
            if item["type"] == "video":
                async with video_send_semaphore:  # Global rate limiting
                    await safe_send_message(
                        context=context,
                        chat_id=chat_id,
                        video=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(user_id, chat_id),
                        user_id=user_id
                    )
            else:
                # Use helper function to send media by type
                async with video_send_semaphore:  # Global rate limiting
                    await send_media_by_type(
                        context=context,
                        chat_id=chat_id,
                        item=item,
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(user_id, chat_id),
                        user_id=user_id
                    )
            return True

    except Exception as e:
        # Show error to admins, silently skip for regular users
        if is_admin(user_id):
            print(f"Error sending media {idx}: {e}")
        else:
            print(f"Silently skipping media {idx} for regular user: {e}")
        return False


async def send_tagged_from_callback_with_range(
        query,
        context: ContextTypes.DEFAULT_TYPE,
        tag: str,
        start_index=None,
        end_index=None):
    user_id = query.from_user.id
    bot = context.bot

    user = query.from_user
    mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'

    # Check if user is exempted from channel requirements
    if user_id in exempted_users:
        print(f"User {user_id} is exempted from channel requirements")
        membership_failed = False
    else:
        # Only verify channel membership for multi-file links (skip for single file)
        is_single_file = start_index is not None and end_index is not None and start_index == end_index
        
        if is_single_file:
            # Skip verification for single-file links
            membership_failed = False
            print(f"User {user_id} accessing single file - skipping channel verification")
        else:
            # Check channel membership for multi-file links
            membership_failed = False
            for ch in REQUIRED_CHANNELS:
                is_member = await is_user_member(user_id, bot, ch)
                print(f"User {user_id} membership in {ch}: {is_member}")  # Debug log
                if not is_member:
                    membership_failed = True
                    break

    if membership_failed:
        clear_user_operation(user_id)
        # For ranged links, we need to preserve the range in retry callback
        retry_param = f"{tag}_{start_index}_{end_index}" if start_index is not None and end_index is not None else tag
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("JOIN✨",
                                     url="https://t.me/Lifesuckkkkkssss"),
                InlineKeyboardButton("BACKUP🛡️",
                                     url="https://t.me/bhaicharabackup")
            ],
             [
                 InlineKeyboardButton("🔄 Try Again",
                                      callback_data=f"retry_{retry_param}")
             ]])
        
        # Send notification with video
        await context.bot.send_video(
            chat_id=query.message.chat_id,
            video=CHANNEL_NOTIFY_VIDEO_ID,
            caption=f"Hey {mention}!!\n"
                   "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                   "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                   "To access multiple videos, you must join our channels first!\n"
                   "Single video links don't require membership.\n\n"
                   "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return

    if tag not in media_data:
        clear_user_operation(user_id)
        await query.message.reply_text("❌ No media found under this tag.")
        return

    # Determine range
    if start_index is None:
        start_index = 0
    if end_index is None:
        end_index = len(media_data[tag]) - 1

    # Validate range
    if start_index < 0 or start_index >= len(media_data[tag]):
        clear_user_operation(user_id)
        await query.message.reply_text(
            f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag])-1}"
        )
        return

    if end_index < start_index or end_index >= len(media_data[tag]):
        clear_user_operation(user_id)
        await query.message.reply_text(
            f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag])-1}"
        )
        return

    # Send media files in the specified range with content protection
    total_files = end_index - start_index + 1
    sent_count = 0
    
    # Send videos without starting notification
    
    # Create concurrent tasks for all media items to send them in parallel
    media_tasks = []
    for idx in range(start_index, end_index + 1):
        # Create task for each media item
        task = asyncio.create_task(
            send_single_media_item(query, context, tag, idx, is_callback=True)
        )
        media_tasks.append(task)
    
    # Wait for all media to be sent concurrently
    if media_tasks:
        results = await asyncio.gather(*media_tasks, return_exceptions=True)
        sent_count = sum(1 for result in results if result is True)
    
    # Send completion message with favorite reminder
    if sent_count > 0:
        completion_message = await safe_send_message(
            context=context,
            chat_id=query.message.chat_id,
            text=f"✅ <b>Media Delivery Complete!</b>\n\n"
                 f"📊 Sent {sent_count}/{total_files} files\n"
                 f"Don't forget to save your favorites!\n\n"
                 f"Use ❤️ ADD button to save videos in your MY FAVORITES LIST.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
            ])
        )
    
    # Clear operation when complete
    clear_user_operation(user_id)



async def send_tagged_with_range(update: Update, context: ContextTypes.DEFAULT_TYPE, tag: str, start_index=None, end_index=None):
    """Send tagged media with range for direct updates (not callback queries)"""
    user_id = update.effective_user.id
    bot = context.bot

    user = update.effective_user
    mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'

    # Check if user is exempted from channel requirements
    if user_id in exempted_users:
        print(f"User {user_id} is exempted from channel requirements")
        membership_failed = False
    else:
        # Only verify channel membership for multi-file links (skip for single file)
        is_single_file = start_index is not None and end_index is not None and start_index == end_index
        
        if is_single_file:
            # Skip verification for single-file links
            membership_failed = False
            print(f"User {user_id} accessing single file - skipping channel verification")
        else:
            # Check channel membership for multi-file links
            membership_failed = False
            for ch in REQUIRED_CHANNELS:
                is_member = await is_user_member(user_id, bot, ch)
                print(f"User {user_id} membership in {ch}: {is_member}")
                if not is_member:
                    membership_failed = True
                    break

    if membership_failed:
        clear_user_operation(user_id)
        # For ranged links, we need to preserve the range in retry callback
        retry_param = f"{tag}_{start_index}_{end_index}" if start_index is not None and end_index is not None else tag
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("JOIN✨",
                                     url="https://t.me/Lifesuckkkkkssss"),
                InlineKeyboardButton("BACKUP🛡️",
                                     url="https://t.me/bhaicharabackup")
            ],
             [
                 InlineKeyboardButton("🔄 Try Again",
                                      callback_data=f"retry_{retry_param}")
             ]])
        
        # Send notification with video
        await context.bot.send_video(
            chat_id=update.effective_chat.id,
            video=CHANNEL_NOTIFY_VIDEO_ID,
            caption=f"Hey {mention}!!\n"
                   "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                   "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                   "To access multiple videos, you must join our channels first!\n"
                   "Single video links don't require membership.\n\n"
                   "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return

    if tag not in media_data:
        clear_user_operation(user_id)
        await update.message.reply_text("❌ No media found under this tag.")
        return

    # Determine range
    if start_index is None:
        start_index = 0
    if end_index is None:
        end_index = len(media_data[tag]) - 1

    # Validate range
    if start_index < 0 or start_index >= len(media_data[tag]):
        clear_user_operation(user_id)
        await update.message.reply_text(
            f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag])-1}"
        )
        return

    if end_index < start_index or end_index >= len(media_data[tag]):
        clear_user_operation(user_id)
        await update.message.reply_text(
            f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag])-1}"
        )
        return

    # Send media files in the specified range with content protection
    total_files = end_index - start_index + 1
    sent_count = 0
    
    await update.message.reply_text(
        f"📁 Showing videos from tag '<code>{tag}</code>' (indexes {start_index}-{end_index}, {total_files} files):",
        parse_mode=ParseMode.HTML
    )
    
    for idx in range(start_index, end_index + 1):
        # Check if index is out of range (media was deleted)
        if idx >= len(media_data[tag]):
            # For admins, show deleted media with restore option if it exists
            if is_admin(update.effective_user.id):
                video_key = f"{tag}_{idx}"
                if video_key in deleted_media_storage:
                    deleted_info = deleted_media_storage[video_key]
                    original_data = deleted_info["data"]
                    
                    # Create unique shareable link for this specific file
                    file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                    share_link = f"https://telegram.me/share/url?url={file_link}"
                    
                    # Build normal media caption but indicate it's corrupted
                    cap = build_media_caption("", tag, str(idx), share_link, "video")
                    cap += "\n\n⚠️ <b>This media was deleted</b>"
                    
                    # Create normal favorite button
                    user_id_str = str(update.effective_user.id)
                    is_favorited = user_id_str in favorites_data["user_favorites"] and video_key in favorites_data["user_favorites"][user_id_str]
                    
                    if is_favorited:
                        fav_button = InlineKeyboardButton("💔 Remove", 
                                                        callback_data=f"remove_fav_{video_key}")
                    else:
                        fav_button = InlineKeyboardButton("❤️ Add", 
                                                        callback_data=f"add_fav_{video_key}")
                    
                    restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
                    
                    keyboard = InlineKeyboardMarkup([
                        [fav_button],
                        [my_favs_button, random_button],
                        [restore_button]
                    ])
                    
                    await update.message.reply_text(
                        cap,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
            continue
            
        item = media_data[tag][idx]

        # Handle deleted tombstones: show to admin with restore; skip for regular users
        if isinstance(item, dict) and item.get("deleted"):
            if is_admin(update.effective_user.id):
                deleted_item = item.get("data", {})
                file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                share_link = f"https://telegram.me/share/url?url={file_link}"
                media_type = (deleted_item.get("type") if isinstance(deleted_item, dict) else "video") or "video"
                cap = build_media_caption("", tag, str(idx), share_link, media_type)
                cap += "\n\n🗑️ <b>This media was deleted</b>"

                video_id = f"{tag}_{idx}"
                user_id_str = str(update.effective_user.id)
                is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"].get(user_id_str, {})
                fav_button = InlineKeyboardButton("💔 Remove" if is_favorited else "❤️ Add", callback_data=(f"remove_fav_{video_id}" if is_favorited else f"add_fav_{video_id}"))
                keyboard = InlineKeyboardMarkup([[fav_button], [restore_button]])

                await update.message.reply_text(cap, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            continue
        
        # Skip if item is not a valid dictionary or missing required fields (silently skip to keep flow smooth)
        if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
            continue
        
        # Check if media is revoked (only admins can see revoked media)
        if isinstance(item, dict) and item.get("revoked"):
            if is_admin(update.effective_user.id):
                # For admins, show revoked media with restore option
                file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                share_link = f"https://telegram.me/share/url?url={file_link}"
                
                # Get original data from revoked wrapper
                original_data = item.get("data", item)
                media_type = original_data.get("type", "video")
                cap = build_media_caption("", tag, str(idx), share_link, media_type)
                cap += "\n\n🛑 <b>This media is revoked</b>"
                
                video_id = f"{tag}_{idx}"
                user_id_str = str(update.effective_user.id)
                is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]
                
                if is_favorited:
                    fav_button = InlineKeyboardButton("💔 Remove", 
                                                    callback_data=f"remove_fav_{video_id}")
                else:
                    fav_button = InlineKeyboardButton("❤️ Add", 
                                                    callback_data=f"add_fav_{video_id}")
                
                
                restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
                
                keyboard = InlineKeyboardMarkup([
                    [fav_button],
                    [my_favs_button, random_button],
                    [restore_button]
                ])
                
                await update.message.reply_text(
                    cap,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard
                )
            # For regular users, silently skip revoked media
            continue
        
        # Create unique shareable link for this specific file
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        
        # Build proper media caption with global caption and replacements
        media_type = item.get("type", "video")
        cap = build_media_caption("", tag, str(idx), share_link, media_type)
        
        # Create favorite button
        video_id = f"{tag}_{idx}"
        user_id_str = str(update.effective_user.id)
        is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]
        
        if is_favorited:
            fav_button = InlineKeyboardButton("💔 Remove", 
                                            callback_data=f"remove_fav_{video_id}")
        else:
            fav_button = InlineKeyboardButton("❤️ ADD", 
                                            callback_data=f"add_fav_{video_id}")
        
        # Check if user is admin to show additional admin buttons
        if is_admin(update.effective_user.id):
            who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_id}")
            my_favs_button = InlineKeyboardButton("⭐ MY FAV", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            rows = [
                [fav_button, who_liked_button],
                [my_favs_button, random_button]
            ]
            rows += build_admin_control_row(video_id)
            keyboard = InlineKeyboardMarkup(rows)
        else:
            my_favs_button = InlineKeyboardButton("⭐ MY FAV", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            
            keyboard = InlineKeyboardMarkup([
                [fav_button],
                [my_favs_button, random_button]
            ])
        
        try:
            media_kwargs = {
                'context': context,
                'chat_id': update.message.chat_id,
                'caption': cap,
                'reply_markup': keyboard,
                'parse_mode': ParseMode.HTML,
                'protect_content': should_protect_content(update.effective_user.id, update.message.chat_id)
            }
            media_type = item.get('type')
            file_id = item.get('file_id')
            if media_type == 'video':
                await safe_send_message(**media_kwargs, video=file_id)
            elif media_type == 'photo':
                await safe_send_message(**media_kwargs, photo=file_id)
            elif media_type == 'animation':
                await safe_send_message(**media_kwargs, animation=file_id)
            elif media_type == 'document':
                await safe_send_message(**media_kwargs, document=file_id)
            elif media_type == 'audio':
                await safe_send_message(**media_kwargs, audio=file_id)
            elif media_type == 'voice':
                await safe_send_message(**media_kwargs, voice=file_id)
            elif media_type == 'sticker':
                sticker_kwargs = {k: v for k, v in media_kwargs.items() if k not in {'caption', 'parse_mode'}}
                await safe_send_message(**sticker_kwargs, sticker=file_id)
            else:
                await safe_send_message(**media_kwargs, document=file_id)
            sent_count += 1
        except Exception as e:
            print(f"Error sending video {idx}: {e}")
            continue
    
    # Send completion message with favorite reminder
    if sent_count > 0:
        # Create inline keyboard with MY FAVORITES button
        completion_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
        ])
        notice = ""
        if not is_admin(update.effective_user.id):
            notice = (
                f"⏰ <b>Auto-Delete Notice:</b> Media you received will auto-delete after {AUTO_DELETE_HOURS} hour(s).\n\n"
            )
        await update.message.reply_text(
            f"✅ Successfully sent {sent_count} out of {total_files} videos from tag '{tag}'\n\n"
            f"{notice}"
            f"💡 <b>Save to Favorites:</b> Use the ❤️ ADD button on any video to add it to your favorites for permanent access!",
            parse_mode=ParseMode.HTML,
            reply_markup=completion_keyboard
        )
    else:
        await update.message.reply_text(f"❌ No videos could be sent from tag '{tag}'")
    # Clear operation when complete
    clear_user_operation(user_id)


async def send_tagged(update: Update, context: ContextTypes.DEFAULT_TYPE,
                      tag: str):
    await send_tagged_with_range(update, context, tag)


async def get_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /get <tag>")
        return

    await send_tagged(update, context, context.args[0].strip().lower())


async def listvideo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List videos only for specific tags: wildwed, freakyfri, sizzlingsaturday, moodymonday, twistedtuesday, thirstthus, socialsunday"""
    if not is_admin(update.effective_user.id):
        return

    # Define the specific tags for /listvideo
    specific_tags = {'wildwed', 'freakyfri', 'sizzlingsaturday', 'moodymonday', 'twistedtuesday', 'thirstthus', 'socialsunday'}
    
    if not media_data:
        await update.message.reply_text("📂 No uploads yet.")
        return

    # Filter only the specific tags
    filtered_tags = {tag: files for tag, files in media_data.items() if tag in specific_tags}
    
    if not filtered_tags:
        await update.message.reply_text("📂 No videos found for the specified tags.")
        return

    msg = "<b>🗂 Special Tags (Visible Media):</b>\n"
    for tag in sorted(filtered_tags.keys()):
        files = filtered_tags[tag]
        # Count only visible (non tombstone/non revoked/non deleted) items
        visible = 0
        for item in files:
            if isinstance(item, dict):
                if item.get('deleted') or item.get('revoked'):
                    continue
                # Treat tombstone pattern {"deleted": True, "data": original} as deleted
                if item.get('deleted') and item.get('data'):
                    continue
                # Require presence of file_id or media content marker
                if not item.get('file_id') and not item.get('media'):  # fallback
                    continue
                visible += 1
            else:
                # Non-dict legacy entries count if truthy
                if item:
                    visible += 1
        next_index = len(files)
        msg += f"<code>{tag}</code> - {visible} media, available index: {next_index}\n"
    await update.message.reply_html(msg)


async def listvideos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List videos for all tags except the specific ones handled by /listvideo"""
    if not is_admin(update.effective_user.id):
        return

    # Define the specific tags that should be excluded (handled by /listvideo)
    excluded_tags = {'wildwed', 'freakyfri', 'sizzlingsaturday', 'moodymonday', 'twistedtuesday', 'thirstthus', 'socialsunday'}
    
    if not media_data:
        await update.message.reply_text("📂 No uploads yet.")
        return

    # Filter out the specific tags
    filtered_tags = {tag: files for tag, files in media_data.items() if tag not in excluded_tags}
    
    if not filtered_tags:
        await update.message.reply_text("📂 No other videos found.")
        return

    msg = "<b>🗂 Other Tags (Visible Media):</b>\n"
    for tag in sorted(filtered_tags.keys()):
        files = filtered_tags[tag]
        visible = 0
        for item in files:
            if isinstance(item, dict):
                if item.get('deleted') or item.get('revoked'):
                    continue
                if item.get('deleted') and item.get('data'):
                    continue
                if not item.get('file_id') and not item.get('media'):
                    continue
                visible += 1
            else:
                if item:
                    visible += 1
        next_index = len(files)
        msg += f"<code>{tag}</code> - {visible} media, available index: {next_index}\n"
    await update.message.reply_html(msg)


async def remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    args = context.args
    # Check 1–3 arguments
    if len(args) < 1 or len(args) > 3:
        await update.message.reply_text("Usage: /remove <tag> [<start_index>] [<end_index>]")
        return

    tag = args[0].strip().lower()
    if tag not in media_data:
        await update.message.reply_text("❌ Tag not found.")
        return

    # Mode 1: remove entire tag
    if len(args) == 1:
        del media_data[tag]
        save_media()
        await update.message.reply_text(f"🗑 Removed all media under tag '{tag}'.")
        return

    # Mode 2: remove single file at index (index-stable tombstone)
    if len(args) == 2:
        try:
            index = int(args[1])
        except ValueError:
            await update.message.reply_text("❌ Invalid index. Please provide a number.")
            return
        if index < 0 or index >= len(media_data[tag]):
            await update.message.reply_text(
                f"❌ Index out of range. Available indexes: 0-{len(media_data[tag]) - 1}"
            )
            return
        # Mark as deleted tombstone (no shrinking)
        original_item = media_data[tag][index]
        media_data[tag][index] = {"deleted": True, "data": original_item, "type": original_item.get("type") if isinstance(original_item, dict) else "unknown"}
        # Record in deleted storage
        video_key = f"{tag}_{index}"
        deleted_media_storage[video_key] = {
            "data": original_item,
            "original_position": index,
            "tag": tag,
            "deleted_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        save_media()
        save_deleted_media()
        await update.message.reply_text(f"🗑 Removed file at index {index} under tag '{tag}'.")
        return

    # Mode 3: remove range of files (mark tombstones without shrinking)
    try:
        start_index = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid start index. Please provide a number.")
        return
    try:
        end_index = int(args[2])
    except ValueError:
        await update.message.reply_text("❌ Invalid end index. Please provide a number.")
        return
    if start_index < 0 or start_index >= len(media_data[tag]):
        await update.message.reply_text(
            f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag]) - 1}"
        )
        return
    if end_index < start_index or end_index >= len(media_data[tag]):
        await update.message.reply_text(
            f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag]) - 1}"
        )
        return
    # Mark each entry in the range as deleted tombstone
    for i in range(start_index, end_index + 1):
        original_item = media_data[tag][i]
        if not (isinstance(original_item, dict) and original_item.get("deleted")):
            media_data[tag][i] = {"deleted": True, "data": original_item, "type": original_item.get("type") if isinstance(original_item, dict) else "unknown"}
            video_key = f"{tag}_{i}"
            deleted_media_storage[video_key] = {
                "data": original_item,
                "original_position": i,
                "tag": tag,
                "deleted_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
    save_media()
    save_deleted_media()
    count = end_index - start_index + 1
    await update.message.reply_text(
        f"🗑 Removed {count} files (indexes {start_index}-{end_index}) under tag '{tag}'."
    )


async def generatelink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text(
            "Usage: /generatelink <tag> [start_index] [end_index]")
        return

    tag = context.args[0].strip().lower()
    if tag not in media_data:
        await update.message.reply_text("❌ Tag not found.")
        return

    start_index = 0
    end_index = len(media_data[tag]) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(media_data[tag]):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag])-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(media_data[tag]):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag])-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end index. Please provide a number.")
            return

    # Create link with range parameters
    if start_index == 0 and end_index == len(media_data[tag]) - 1:
        # Full range, use simple format
        link_param = tag
        range_text = "all files"
    else:
        # Specific range, encode as tag_start_end
        link_param = f"{tag}_{start_index}_{end_index}"
        range_text = f"files {start_index}-{end_index}"

    link = f"https://t.me/{BOT_USERNAME}?start={link_param}"
    await update.message.reply_text(
        f"🔗 Shareable link for <code>{tag}</code> ({range_text}):\n{link}",
        parse_mode=ParseMode.HTML)


async def view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) == 0:
        thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
        await update.message.reply_text("Usage: /view <tag> [start_index] [end_index]", message_thread_id=thread_id)
        return

    tag = context.args[0].strip().lower()

    if tag not in media_data:
        await update.message.reply_text("❌ Tag not found.")
        return
        
    # Add status tracking for large ranges
    sent_count = 0

    # Determine the range to show
    start_index = 0
    end_index = len(media_data[tag]) - 1

    # If one index is provided, show just that single file
    if len(context.args) == 2:
        try:
            if context.args[1].lower() == "last":
                single_index = len(media_data[tag]) - 1
            else:
                single_index = int(context.args[1])
            if single_index < 0 or single_index >= len(media_data[tag]):
                thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
                await update.message.reply_text(
                    f"❌ Index out of range. Available indexes: 0-{len(media_data[tag])-1}",
                    message_thread_id=thread_id
                )
                return
            start_index = single_index
            end_index = single_index
        except ValueError:
            await update.message.reply_text("❌ Invalid index. Please provide a number.")
            return

    # If two indices are provided, show the range
    elif len(context.args) == 3:
        try:
            start_index = int(context.args[1]) if context.args[1].lower() != "last" else len(media_data[tag]) - 1
            end_index = int(context.args[2]) if context.args[2].lower() != "last" else len(media_data[tag]) - 1
        except ValueError:
            thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
            await update.message.reply_text("❌ Invalid index range. Please provide valid numbers.", message_thread_id=thread_id)
            return

        # Validate range
        if start_index < 0 or start_index >= len(media_data[tag]):
            thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
            await update.message.reply_text(
                f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag])-1}",
                message_thread_id=thread_id
            )
            return

        if end_index < start_index or end_index >= len(media_data[tag]):
            thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
            await update.message.reply_text(
                f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag])-1}",
                message_thread_id=thread_id
            )
            return

    # Check if range is too large and might cause flood control issues
    range_size = end_index - start_index + 1
    max_safe_range = 50  # Reasonable limit to avoid flood control
    
    # Show summary message with warning if range is large
    # Get message_thread_id if in a forum topic
    thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
    
    if start_index == end_index:
        await update.message.reply_text(
            f"📁 Showing media from tag '<code>{tag}</code>' at index {start_index}:",
            parse_mode=ParseMode.HTML,
            message_thread_id=thread_id)
    elif start_index == 0 and end_index == len(media_data[tag]) - 1:
        await update.message.reply_text(
            f"📁 Showing all media under tag '<code>{tag}</code>' ({len(media_data[tag])} files):",
            parse_mode=ParseMode.HTML,
            message_thread_id=thread_id)
    else:
        await update.message.reply_text(
            f"📁 Showing media from tag '<code>{tag}</code>' (indexes {start_index}-{end_index}, {range_size} files):",
            parse_mode=ParseMode.HTML,
            message_thread_id=thread_id)
    
    # Add warning, progress indicator, and Stop button for multi-item sends
    cancel_key = f"view_cancel_{update.effective_user.id}_{int(asyncio.get_event_loop().time()*1000)}"
    context.chat_data[cancel_key] = False
    stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True, one_time_keyboard=True)
    if range_size > 1:
        # Include message_thread_id if in a forum topic
        thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
        progress_msg = await update.message.reply_text(
            f"⏳ Starting media delivery ({range_size} files)...\n"
            f"Media will be sent with 0.5 second intervals to avoid flood controls.\n"
            f"Progress: 0/{range_size} files sent",
            message_thread_id=thread_id,
            reply_markup=stop_keyboard
        )

    # Send the media files in the specified range
    last_idx_processed = start_index - 1
    for idx in range(start_index, end_index + 1):
        # Check cancel signal
        if context.chat_data.get(cancel_key):
            break
        # Track last processed index early (used for accurate resume)
        last_idx_processed = idx
        # Check if index is out of range (media was deleted)
        if idx >= len(media_data[tag]):
            # For admins, show deleted media with restore option if it exists
            if is_admin(update.effective_user.id):
                video_key = f"{tag}_{idx}"
                if video_key in deleted_media_storage:
                    deleted_info = deleted_media_storage[video_key]
                    deleted_item = deleted_info["data"]
                    
                    # Create unique shareable link for this specific file
                    file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                    share_link = f"https://telegram.me/share/url?url={file_link}"
                    
                    # Build proper media caption with global caption and replacements
                    media_type = deleted_item.get("type", "video")
                    cap = build_media_caption("", tag, str(idx), share_link, media_type)
                    cap += "\n\n🗑️ <b>This media was deleted</b>"
                    
                    # Create favorite button
                    user_id_str = str(update.effective_user.id)
                    is_favorited = user_id_str in favorites_data["user_favorites"] and video_key in favorites_data["user_favorites"].get(user_id_str, {})

                    if is_favorited:
                        fav_button = InlineKeyboardButton("💔 Remove", 
                                                        callback_data=f"remove_fav_{video_key}")
                    else:
                        fav_button = InlineKeyboardButton("❤️ Add", 
                                                        callback_data=f"add_fav_{video_key}")

                    # Admin buttons for deleted media
                    who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_key}")
                    restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")
                    
                    keyboard = InlineKeyboardMarkup([
                        [who_liked_button],
                        [restore_button]
                    ])

                    try:
                        if deleted_item["type"] == "video":
                            await safe_send_message(
                                context=context,
                                chat_id=update.message.chat_id,
                                video=deleted_item["file_id"],
                                caption=cap,
                                reply_markup=keyboard,
                                parse_mode=ParseMode.HTML,
                                protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                            )
                        else:
                            await safe_send_message(
                                context=context,
                                chat_id=update.message.chat_id,
                                item=deleted_item,
                                caption=cap,
                                reply_markup=keyboard,
                                parse_mode=ParseMode.HTML,
                                protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                            )
                        sent_count += 1
                    except Exception as e:
                        print(f"Error sending deleted media {idx}: {e}")
                        await update.message.reply_text(f"❌ Error displaying deleted file at index {idx}")
            # For regular users, silently skip without any notification
            continue
            
        item = media_data[tag][idx]

        # Handle deleted tombstones in-place: show admin restore; no normal controls
        if isinstance(item, dict) and item.get("deleted"):
            deleted_item = item.get("data", {})
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            share_link = f"https://telegram.me/share/url?url={file_link}"
            media_type = (deleted_item.get("type") if isinstance(deleted_item, dict) else "video") or "video"
            cap = build_media_caption("", tag, str(idx), share_link, media_type)
            cap += "\n\n🗑️ <b>This media was deleted</b>"

            restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
            keyboard = InlineKeyboardMarkup([[restore_button]])

            # Try sending the media if we still have a file_id; else fallback to text
            media_kwargs = {
                "context": context,
                "chat_id": update.message.chat_id,
                "caption": cap,
                "reply_markup": keyboard,
                "parse_mode": ParseMode.HTML,
                "protect_content": should_protect_content(update.effective_user.id, update.message.chat_id)
            }
            if isinstance(deleted_item, dict) and "file_id" in deleted_item:
                dt = deleted_item.get("type", "video")
                if dt == "video":
                    media_kwargs["video"] = deleted_item["file_id"]
                elif dt == "photo":
                    media_kwargs["photo"] = deleted_item["file_id"]
                elif dt == "document":
                    media_kwargs["document"] = deleted_item["file_id"]
                elif dt == "audio":
                    media_kwargs["audio"] = deleted_item["file_id"]
                elif dt == "voice":
                    media_kwargs["voice"] = deleted_item["file_id"]
                elif dt == "animation":
                    media_kwargs["animation"] = deleted_item["file_id"]
                elif dt == "sticker":
                    media_kwargs["sticker"] = deleted_item["file_id"]
                    media_kwargs.pop("caption", None)
                    media_kwargs.pop("parse_mode", None)
                else:
                    media_kwargs["text"] = cap
                    media_kwargs.pop("caption", None)
                    media_kwargs.pop("parse_mode", None)
            else:
                media_kwargs["text"] = cap
                media_kwargs.pop("caption", None)
                media_kwargs.pop("parse_mode", None)

            try:
                await safe_send_message(**media_kwargs)
                sent_count += 1
            except Exception as e:
                print(f"Error sending deleted tombstone at {idx}: {e}")
            continue

        # Handle revoked items: show admin restore; no normal controls
        if isinstance(item, dict) and item.get("revoked"):
            original = item.get("data", item)
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            share_link = f"https://telegram.me/share/url?url={file_link}"
            media_type = original.get("type", "video") if isinstance(original, dict) else "video"
            cap = build_media_caption("", tag, str(idx), share_link, media_type)
            cap += "\n\n🛑 <b>This media is revoked</b>"

            restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
            keyboard = InlineKeyboardMarkup([[restore_button]])

            # Try sending original media if possible; else send text
            media_kwargs = {
                "context": context,
                "chat_id": update.message.chat_id,
                "caption": cap,
                "reply_markup": keyboard,
                "parse_mode": ParseMode.HTML,
                "protect_content": should_protect_content(update.effective_user.id, update.message.chat_id)
            }
            if isinstance(original, dict) and "file_id" in original:
                dt = original.get("type", "video")
                if dt == "video":
                    media_kwargs["video"] = original["file_id"]
                elif dt == "photo":
                    media_kwargs["photo"] = original["file_id"]
                elif dt == "document":
                    media_kwargs["document"] = original["file_id"]
                elif dt == "audio":
                    media_kwargs["audio"] = original["file_id"]
                elif dt == "voice":
                    media_kwargs["voice"] = original["file_id"]
                elif dt == "animation":
                    media_kwargs["animation"] = original["file_id"]
                elif dt == "sticker":
                    media_kwargs["sticker"] = original["file_id"]
                    media_kwargs.pop("caption", None)
                    media_kwargs.pop("parse_mode", None)
                else:
                    media_kwargs["text"] = cap
                    media_kwargs.pop("caption", None)
                    media_kwargs.pop("parse_mode", None)
            else:
                media_kwargs["text"] = cap
                media_kwargs.pop("caption", None)
                media_kwargs.pop("parse_mode", None)

            try:
                await safe_send_message(**media_kwargs)
                sent_count += 1
            except Exception as e:
                print(f"Error sending revoked item at {idx}: {e}")
            continue

        # Skip if item is not a valid dictionary or missing required fields (skip for all to keep it smooth)
        if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
            continue
            
            # Add forced delay between sending multiple items in supergroups to avoid flood control
            if sent_count > 1:
                # Add extra delay between sends for supergroups to avoid flood control
                if update.message.chat.type == "supergroup":
                    # Increase delay for larger ranges
                    wait_time = 3.0 if range_size > 5 else 2.0
                    await asyncio.sleep(wait_time)
                elif update.message.chat.type == "group":
                    await asyncio.sleep(1.5)
            
            # Update progress message for large ranges every 5 files
            if range_size > 1 and 'progress_msg' in locals() and sent_count % 5 == 0:
                try:
                    # Thread ID is already included in the message object
                    await progress_msg.edit_text(
                        f"⏳ Sending media...\n"
                        f"Progress: {sent_count}/{range_size} files sent"
                    )
                except Exception as e:
                    print(f"Error updating progress message: {e}")

        # Create unique shareable link for this specific file
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"

        # Build proper media caption with global caption and replacements
        media_type = item.get("type", "video")
        cap = build_media_caption("", tag, str(idx), share_link, media_type)

        # Create admin view buttons (normal, only for active items)
        video_id = f"{tag}_{idx}"
        push_button = InlineKeyboardButton("🔄 PUSH", callback_data=f"p_{video_id}")
        admin_row = [
            InlineKeyboardButton("🛑 Revoke", callback_data=f"revoke_media_{video_id}"),
            InlineKeyboardButton("🗑️ Remove Media", callback_data=f"del_media_{video_id}")
        ]
        keyboard = InlineKeyboardMarkup([[push_button], admin_row])

        try:
            # Use safe_send_message function to handle rate limiting and retries
            media_type = item["type"]
            file_id = item["file_id"]
            user_id = update.effective_user.id
            chat_id = update.message.chat_id
            
            # Consistent media send parameters
            media_params = {
                "context": context,
                "chat_id": chat_id, 
                "caption": cap,
                "parse_mode": ParseMode.HTML,
                "reply_markup": keyboard,
                "protect_content": should_protect_content(user_id, chat_id),
                "user_id": user_id,
                "chat_type": update.message.chat.type
            }
            
            # Add message_thread_id for forum chats to maintain topic thread
            if hasattr(update.message, 'message_thread_id') and update.message.message_thread_id:
                media_params["message_thread_id"] = update.message.message_thread_id
            
            # Use more retries for batch operations in supergroups
            extra_params = {}
            if update.message.chat.type == "supergroup" and range_size > 3:
                extra_params["max_retries"] = 5  # More retries for batch operations
            
            # Disable auto-deletion for admin /view (never delete viewed admin media)
            extra_params["auto_delete"] = False
                
            if media_type == "video":
                await safe_send_message(**media_params, **extra_params, video=file_id)
            elif media_type == "photo":
                await safe_send_message(**media_params, **extra_params, photo=file_id)
            elif media_type == "animation":
                await safe_send_message(**media_params, **extra_params, animation=file_id)
            elif media_type == "document":
                await safe_send_message(**media_params, **extra_params, document=file_id)
            elif media_type == "audio":
                await safe_send_message(**media_params, **extra_params, audio=file_id)
            elif media_type == "voice":
                await safe_send_message(**media_params, **extra_params, voice=file_id)
            elif media_type == "sticker":
                # Stickers don't support captions
                sticker_params = {**media_params}
                if "caption" in sticker_params:
                    sticker_params.pop("caption")
                if "parse_mode" in sticker_params:
                    sticker_params.pop("parse_mode")
                await safe_send_message(**sticker_params, **extra_params, sticker=file_id)
            else:
                # Fallback for unknown types - try to send as document
                await safe_send_message(**media_params, **extra_params, document=file_id)
        except Exception as e:
            # Show error to admins, silently skip for regular users
            if is_admin(update.effective_user.id):
                print(f"Error sending media {idx}: {e}")
                thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
                await update.message.reply_text(f"❌ Error sending file at index {idx}", message_thread_id=thread_id)
            else:
                print(f"Silently skipping media {idx} for regular user: {e}")
            continue

    # Update progress message for large ranges
    if range_size > 1 and 'progress_msg' in locals():
        # Thread ID is already included in the message object
        try:
            if context.chat_data.get(cancel_key):
                await progress_msg.edit_text(
                    f"⛔ Send operation stopped!\n"
                    f"Sent {sent_count}/{range_size} media files before cancellation."
                )
            else:
                await progress_msg.edit_text(
                    f"✅ Delivery complete!\n"
                    f"Successfully sent {sent_count}/{range_size} media files."
                )
        except Exception:
            pass
    
    # Remove Stop keyboard and clear cancel flag - offer resume if cancelled, else restore admin keyboard
    if range_size > 1:
        if context.chat_data.get(cancel_key):
            # Create resume state for /view so admins can continue later
            next_index_resume = max(start_index, last_idx_processed + 1)
            if next_index_resume <= end_index:
                resume_id = f"{update.effective_user.id}_{int(time.time()*1000)}"
                resume_states[resume_id] = {
                    'mode': 'view',
                    'tag': tag,
                    'next_index': next_index_resume,
                    'end_index': end_index,
                    'chat_id': update.message.chat_id,
                    'user_id': update.effective_user.id
                }
                resume_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Continue Sending", callback_data=f"resume_{resume_id}")],
                    [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
                ])
                await update.message.reply_html(
                    (
                        f"⛔ Send operation stopped.\n\n"
                        f"✅ Delivered: {sent_count}/{range_size} items so far.\n"
                        f"➡️ Next index: {next_index_resume} of {end_index}."
                    ),
                    reply_markup=resume_kb
                )
            else:
                await update.message.reply_text("✅ Nothing left to send.")
        else:
            # Show normal user keyboard after completion
            user_keyboard = ReplyKeyboardMarkup([
                ["🎲 Random", "🔥 Top Videos"],
                ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
            ], resize_keyboard=True)
            await update.message.reply_text("✅ Operation complete.", reply_markup=user_keyboard)
        context.chat_data[cancel_key] = False

async def cview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clean view: show only active visible media (skip deleted/revoked/tombstones).
    Usage: /cview <tag> [start_index] [end_index]
    Mirrors /view argument parsing but filters out non-visible items entirely.
    """
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) == 0:
        thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
        await update.message.reply_text("Usage: /cview <tag> [start_index] [end_index]", message_thread_id=thread_id)
        return

    tag = context.args[0].strip().lower()
    if tag not in media_data:
        await update.message.reply_text("❌ Tag not found.")
        return

    start_index = 0
    end_index = len(media_data[tag]) - 1
    if len(context.args) == 2:
        try:
            if context.args[1].lower() == "last":
                single_index = len(media_data[tag]) - 1
            else:
                single_index = int(context.args[1])
            if single_index < 0 or single_index >= len(media_data[tag]):
                await update.message.reply_text(f"❌ Index out of range. Available indexes: 0-{len(media_data[tag]) - 1}")
                return
            start_index = single_index
            end_index = single_index
        except ValueError:
            await update.message.reply_text("❌ Invalid index.")
            return
    elif len(context.args) == 3:
        try:
            start_index = int(context.args[1]) if context.args[1].lower() != "last" else len(media_data[tag]) - 1
            end_index = int(context.args[2]) if context.args[2].lower() != "last" else len(media_data[tag]) - 1
        except ValueError:
            await update.message.reply_text("❌ Invalid index range.")
            return
        if start_index < 0 or start_index >= len(media_data[tag]):
            await update.message.reply_text(f"❌ Start index out of range. Available: 0-{len(media_data[tag]) - 1}")
            return
        if end_index < start_index or end_index >= len(media_data[tag]):
            await update.message.reply_text(f"❌ End index out of range or less than start index. Available: {start_index}-{len(media_data[tag]) - 1}")
            return

    files = media_data[tag]
    # Build list of visible indices
    visible_indices = []
    for i in range(start_index, end_index + 1):
        if i >= len(files):
            continue
        it = files[i]
        if not isinstance(it, dict):
            continue
        if it.get('deleted') or it.get('revoked'):
            continue
        if it.get('deleted') and it.get('data'):
            continue
        if 'file_id' not in it:
            continue
        visible_indices.append(i)

    total_requested = end_index - start_index + 1
    thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
    await update.message.reply_text(
        f"🧼 Clean view for '<code>{tag}</code>' indexes {start_index}-{end_index} (visible {len(visible_indices)}/{total_requested})",
        parse_mode=ParseMode.HTML,
        message_thread_id=thread_id
    )

    if not visible_indices:
        await update.message.reply_text("❌ No visible media in this range.")
        return

    progress_msg = None
    cancel_key = f"cview_cancel_{update.effective_user.id}_{int(asyncio.get_event_loop().time()*1000)}"
    context.chat_data[cancel_key] = False
    stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True, one_time_keyboard=True)
    if len(visible_indices) > 1:
        progress_msg = await update.message.reply_text(
            f"⏳ Sending {len(visible_indices)} visible items...\nProgress: 0/{len(visible_indices)}",
            reply_markup=stop_keyboard
        )

    sent = 0
    last_pos_processed = -1
    for pos, idx in enumerate(visible_indices):
        if context.chat_data.get(cancel_key):
            break
        last_pos_processed = pos
        item = files[idx]
        media_type = item.get('type', 'video')
        file_id = item.get('file_id')
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        cap = build_media_caption("", tag, str(idx), share_link, media_type)

        push_button = InlineKeyboardButton("🔄 PUSH", callback_data=f"p_{tag}_{idx}")
        admin_row = [
            InlineKeyboardButton("🛑 Revoke", callback_data=f"revoke_media_{tag}_{idx}"),
            InlineKeyboardButton("🗑️ Remove", callback_data=f"del_media_{tag}_{idx}")
        ]
        keyboard = InlineKeyboardMarkup([[push_button], admin_row])

        try:
            media_params = {
                'context': context,
                'chat_id': update.message.chat_id,
                'caption': cap,
                'reply_markup': keyboard,
                'parse_mode': ParseMode.HTML,
                'protect_content': should_protect_content(update.effective_user.id, update.message.chat_id)
            }
            # Preserve topic thread if present
            if thread_id:
                media_params['message_thread_id'] = thread_id
            # cview should NEVER trigger auto deletion (explicit override)
            media_params["user_id"] = update.effective_user.id
            media_params["chat_type"] = update.message.chat.type
            common_kwargs = {"auto_delete": False}
            if media_type == 'video':
                await safe_send_message(**media_params, **common_kwargs, video=file_id)
            elif media_type == 'photo':
                await safe_send_message(**media_params, **common_kwargs, photo=file_id)
            elif media_type == 'animation':
                await safe_send_message(**media_params, **common_kwargs, animation=file_id)
            elif media_type == 'document':
                await safe_send_message(**media_params, **common_kwargs, document=file_id)
            elif media_type == 'audio':
                await safe_send_message(**media_params, **common_kwargs, audio=file_id)
            elif media_type == 'voice':
                await safe_send_message(**media_params, **common_kwargs, voice=file_id)
            elif media_type == 'sticker':
                sp = {k: v for k, v in media_params.items() if k not in {'caption', 'parse_mode'}}
                await safe_send_message(**sp, **common_kwargs, sticker=file_id)
            else:
                await safe_send_message(**media_params, **common_kwargs, document=file_id)
            sent += 1
        except Exception as e:
            print(f"cview send error idx {idx}: {e}")
            continue

        if progress_msg and sent % 5 == 0:
            try:
                await progress_msg.edit_text(f"⏳ Sending visible items...\nProgress: {sent}/{len(visible_indices)}")
            except Exception:
                pass

    if progress_msg:
        try:
            if context.chat_data.get(cancel_key):
                await progress_msg.edit_text(f"⛔ Send operation stopped!\nSent {sent}/{len(visible_indices)} before cancellation.")
            else:
                await progress_msg.edit_text(f"✅ Clean delivery complete ({sent}/{len(visible_indices)})")
        except Exception:
            pass
    
    # Remove Stop keyboard and clear cancel flag - offer resume if cancelled, otherwise restore admin keyboard
    if len(visible_indices) > 1:
        if context.chat_data.get(cancel_key):
            resume_id = f"{update.effective_user.id}_{int(time.time()*1000)}"
            resume_states[resume_id] = {
                'mode': 'cview',
                'tag': tag,
                'visible_indices': visible_indices,
                'pointer': last_pos_processed + 1 if last_pos_processed >= 0 else 0,
                'chat_id': update.message.chat_id,
                'user_id': update.effective_user.id,
                'delivered': sent,
                'total': len(visible_indices)
            }
            resume_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Continue Sending", callback_data=f"resume_{resume_id}")],
                [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
            ])
            await update.message.reply_html(
                (
                    f"⛔ Clean view sending stopped.\n\n"
                    f"✅ Delivered: {sent}/{len(visible_indices)} visible items from tag '{tag}'.\n\n"
                    f"💡 Use ❤️ ADD to save favorites."
                ),
                reply_markup=resume_kb
            )
            context.chat_data[cancel_key] = False
            return
        else:
            # Show normal user keyboard after completion
            user_keyboard = ReplyKeyboardMarkup([
                ["🎲 Random", "🔥 Top Videos"],
                ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
            ], resize_keyboard=True)
            await update.message.reply_text("✅ Operation complete.", reply_markup=user_keyboard)
        context.chat_data[cancel_key] = False


async def free(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /free <user_id>")
        return

    try:
        user_id = int(context.args[0])
        if user_id in exempted_users:
            await update.message.reply_text(
                f"❌ User {user_id} is already exempted.")
            return

        exempted_users.add(user_id)
        save_exempted()
        await update.message.reply_text(
            f"✅ User {user_id} has been exempted from channel requirements.")
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid user ID. Please provide a numeric user ID.")


async def unfree(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /unfree <user_id>")
        return

    try:
        user_id = int(context.args[0])
        if user_id not in exempted_users:
            await update.message.reply_text(
                f"❌ User {user_id} is not in exempted list.")
            return

        exempted_users.remove(user_id)
        save_exempted()
        await update.message.reply_text(
            f"✅ User {user_id} has been removed from exemptions and must now join required channels.")
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid user ID. Please provide a numeric user ID.")


async def listfree(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if not exempted_users:
        await update.message.reply_text("📂 No exempted users.")
        return

    msg = f"<b>🆓 Exempted Users ({len(exempted_users)}):</b>\n\n"
    
    # Send a "loading" message since getting user info might take time
    loading_msg = await update.message.reply_text("Loading user details...")
    
    user_count = 0
    for user_id in exempted_users:
        user_count += 1
        try:
            # Try to get user info
            user = await context.bot.get_chat(user_id)
            username = f"@{user.username}" if user.username else "No username"
            name = user.first_name
            if user.last_name:
                name += f" {user.last_name}"
            
            msg += f"{user_count}. <code>{user_id}</code> | <b>{name}</b> | {username}\n"
        except Exception as e:
            # If we can't get user info, just show the ID
            msg += f"{user_count}. <code>{user_id}</code> | <i>Unable to retrieve user info</i>\n"
    
    # Delete the loading message
    await loading_msg.delete()
    
    # Add note about missing user info
    if user_count > 0:
        msg += "\n<i>Note: User information might not be available if the user hasn't interacted with the bot recently.</i>"
    
    # Send the message with user details
    await update.message.reply_html(msg)


async def pass_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pass videos to RANDOM MEDIA button only (no shareable links).
    Usage:
      /pass <tag> [start] [end]
      /pass all <tag>
      /pass <tag1> <tag2> <tag3>  (multi-tag, passes ALL available indices for each)
    """
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 1:
        await update.message.reply_text("Usage: /pass <tag> [start] [end] or /pass all <tag> or /pass <tag1> <tag2> ...")
        return

    # Normalize args
    args = [a.strip().lower() for a in context.args]

    # Case: /pass all <tag> → delegate to /passall
    if args[0] == 'all':
        if len(args) != 2:
            await update.message.reply_text("Usage: /pass all <tag>")
            return
        context.args = [args[1]]
        await pass_all_command(update, context)
        return

    # Case: multi-tag mode: /pass tag1 tag2 tag3 → pass ALL for each
    def _is_int(s: str) -> bool:
        try:
            int(s)
            return True
        except Exception:
            return False

    if len(args) >= 2 and not _is_int(args[1]):
        results = []
        updated_total = 0
        global passed_links
        for tag_name in args:
            if tag_name not in media_data:
                results.append(f"❌ {tag_name} (not found)")
                continue
            tag_videos = media_data[tag_name]
            existing_indices = set()
            for link in passed_links[:]:
                if link.startswith(f"{tag_name}_"):
                    try:
                        index = int(link.split("_", 1)[1])
                        existing_indices.add(index)
                    except (ValueError, IndexError):
                        continue
            full_indices = set()
            for i, it in enumerate(tag_videos):
                if not isinstance(it, dict):
                    continue
                if it.get('deleted') or it.get('revoked'):
                    continue
                if 'file_id' not in it:
                    continue
                full_indices.add(i)
            all_indices = existing_indices.union(full_indices)
            # Rewrite entries for this tag
            passed_links = [link for link in passed_links if not link.startswith(f"{tag_name}_")]
            for i in sorted(all_indices):
                passed_links.append(f"{tag_name}_{i}")
            results.append(f"✅ {tag_name} ({len(all_indices)})")
            updated_total += len(all_indices)

        save_passed_links()
        await update.message.reply_html(
            "\n".join([
                "<b>Multi-Tag PASS complete</b>",
                *results,
                f"\nTotal passed indices across tags: <b>{updated_total}</b>"
            ])
        )
        return

    tag = args[0]
    
    # Check if tag exists
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found.")
        return
    
    # Get videos list (media_data[tag] is a list, not dict)
    tag_videos = media_data[tag]
    
    # Default to entire tag
    start_index = 0
    end_index = len(tag_videos) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end index. Please provide a number.")
            return
    
    # Check for existing entries with the same tag and merge ranges
    existing_indices = set()
    for link in passed_links[:]:  # Create a copy to iterate over
        if link.startswith(f"{tag}_"):
            try:
                index = int(link.split("_", 1)[1])
                existing_indices.add(index)
            except (ValueError, IndexError):
                continue
    
    # Add new indices to the set
    new_indices = set(range(start_index, end_index + 1))
    all_indices = existing_indices.union(new_indices)
    
    # Remove old entries for this tag
    passed_links[:] = [link for link in passed_links if not link.startswith(f"{tag}_")]
    
    # Add merged indices back
    videos_to_pass = []
    for i in sorted(all_indices):
        if i < len(tag_videos):
            video_key = f"{tag}_{i}"
            passed_links.append(video_key)
            if i in new_indices:
                videos_to_pass.append(video_key)
    
    if videos_to_pass:
        save_passed_links()
        count = len(videos_to_pass)
        
        # Determine range text for response
        if start_index == 0 and end_index == len(tag_videos) - 1:
            range_text = f"entire tag '{tag}'"
        else:
            range_text = f"'{tag}' (indices {start_index}-{end_index})"
        
        # Show merged range info
        merged_range = f"{min(all_indices)}-{max(all_indices)}" if len(all_indices) > 1 else str(min(all_indices))
        
        await update.message.reply_text(
            f"✅ Added {count} new videos from {range_text}\n"
            f"🔄 Merged with existing entries - total range: {tag} ({merged_range})\n"
            f"🎲 {len(all_indices)} videos available via RANDOM MEDIA button",
            parse_mode=ParseMode.HTML
        )
    else:
        range_text = f"'{tag}' (indices {start_index}-{end_index})" if start_index != 0 or end_index != len(tag_videos) - 1 else f"entire tag '{tag}'"
        await update.message.reply_text(
            f"ℹ️ All videos in {range_text} were already passed.",
            parse_mode=ParseMode.HTML
        )


async def pass_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pass ALL available indices from already-existing passed tags (no args needed).
    Extracts unique tags from passed_links and ensures all valid indices are included.
    """
    global passed_links

    if not is_admin(update.effective_user.id):
        return

    if context.args:
        await update.message.reply_text("❌ /passall takes no arguments. It passes all valid indices for already-passed tags.")
        return

    # Extract unique tags from passed_links
    tags_in_passed = set()
    for key in passed_links:
        if '_' not in key:
            continue
        tag, idx_str = key.rsplit('_', 1)
        try:
            int(idx_str)  # Validate index format
            tags_in_passed.add(tag)
        except ValueError:
            continue

    if not tags_in_passed:
        await update.message.reply_text("❌ No tags currently in passed_links. Use /pass first.")
        return

    # For each tag in passed_links, add all valid indices
    results = []
    total_count = 0
    for tag in sorted(tags_in_passed):
        if tag not in media_data:
            results.append(f"❌ {tag} (not in media_data)")
            continue
        tag_videos = media_data[tag]
        full_indices = set()
        for i, it in enumerate(tag_videos):
            if not isinstance(it, dict):
                continue
            if it.get('deleted') or it.get('revoked'):
                continue
            if 'file_id' not in it:
                continue
            full_indices.add(i)
        # Remove old entries for this tag
        passed_links = [link for link in passed_links if not link.startswith(f"{tag}_")]
        # Add all valid indices
        for i in sorted(full_indices):
            passed_links.append(f"{tag}_{i}")
        results.append(f"✅ {tag} ({len(full_indices)})")
        total_count += len(full_indices)

    save_passed_links()
    await update.message.reply_html(
        "\n".join([
            "<b>PassAll Complete</b> (filled all valid indices for already-passed tags)",
            *results,
            f"\nTotal indices: <b>{total_count}</b>"
        ])
    )


async def push_multi_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Multi-tag pass helper:
    - /push <tag1> <tag2> ...  → pass all available indices of each tag
    - /push all <tag>          → pass entire tag (alias of /passall)
    """
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(
            "Usage:\n"
            "• /push <tag1> <tag2> ...\n"
            "• /push all <tag>")
        return

    args = [a.strip().lower() for a in context.args]
    # Handle '/push all <tag>' alias
    if args[0] == 'all':
        if len(args) != 2:
            await update.message.reply_text("Usage: /push all <tag>")
            return
        context.args = [args[1]]
        await pass_all_command(update, context)
        return

    # Otherwise treat all args as tags
    results = []
    updated_total = 0
    global passed_links
    for tag in args:
        if tag not in media_data:
            results.append(f"❌ {tag} (not found)")
            continue
        tag_videos = media_data[tag]
        existing_indices = set()
        for link in passed_links[:]:
            if link.startswith(f"{tag}_"):
                try:
                    index = int(link.split("_", 1)[1])
                    existing_indices.add(index)
                except (ValueError, IndexError):
                    continue
        full_indices = set()
        for i, it in enumerate(tag_videos):
            if not isinstance(it, dict):
                continue
            if it.get('deleted') or it.get('revoked'):
                continue
            if 'file_id' not in it:
                continue
            full_indices.add(i)
        all_indices = existing_indices.union(full_indices)
        # Rewrite entries for this tag
        passed_links = [link for link in passed_links if not link.startswith(f"{tag}_")]
        for i in sorted(all_indices):
            passed_links.append(f"{tag}_{i}")
        results.append(f"✅ {tag} ({len(all_indices)})")
        updated_total += len(all_indices)

    save_passed_links()
    await update.message.reply_html(
        "\n".join([
            "<b>Multi-Tag PASS complete</b>",
            *results,
            f"\nTotal passed indices across tags: <b>{updated_total}</b>"
        ])
    )


def _build_index_remap_for_tag(old_items: list, new_items: list) -> dict:
    """Return mapping old_index -> new_index based on file_id match within a tag."""
    old_idx_to_fid = {}
    for i, it in enumerate(old_items):
        if isinstance(it, dict) and it.get('file_id') and not it.get('deleted'):
            old_idx_to_fid[i] = it.get('file_id')

    fid_to_new_idx = {}
    for j, it in enumerate(new_items):
        if isinstance(it, dict) and it.get('file_id') and not it.get('deleted'):
            fid_to_new_idx[it.get('file_id')] = j

    remap = {}
    for old_i, fid in old_idx_to_fid.items():
        if fid in fid_to_new_idx:
            remap[old_i] = fid_to_new_idx[fid]
    return remap


def _shrink_tag_list(tag: str, indices_to_remove: set[int]) -> tuple[int, int]:
    """Shrink tag list by removing items at given old indices; returns (removed_count, new_len).
    Updates media_data[tag] and returns the counts. Does not touch links.
    """
    items = media_data.get(tag, [])
    new_items = [it for idx, it in enumerate(items) if idx not in indices_to_remove]
    removed = len(items) - len(new_items)
    media_data[tag] = new_items
    save_media()
    update_random_state()
    return removed, len(new_items)


def _update_links_after_shrink(tag: str, old_items: list, new_items: list):
    """Update passed_links and active_links for a tag after shrinking indices, by file_id remap."""
    global passed_links, active_links
    remap = _build_index_remap_for_tag(old_items, new_items)

    # Update passed_links
    new_passed = []
    for key in passed_links:
        if '_' not in key:
            continue
        t, idx_str = key.rsplit('_', 1)
        if t != tag:
            new_passed.append(key)
            continue
        try:
            old_idx = int(idx_str)
        except ValueError:
            continue
        if old_idx in remap:
            new_passed.append(f"{tag}_{remap[old_idx]}")
        # else drop entries pointing to removed items
    passed_links = new_passed
    save_passed_links()

    # Update active_links (dict of video_key -> data)
    if isinstance(active_links, dict) and active_links:
        updated = {}
        for key, data in active_links.items():
            if '_' not in key:
                updated[key] = data
                continue
            t, idx_str = key.rsplit('_', 1)
            if t != tag:
                updated[key] = data
                continue
            try:
                old_idx = int(idx_str)
            except ValueError:
                continue
            if old_idx in remap:
                updated[f"{tag}_{remap[old_idx]}"] = data
            # else drop entries pointing to removed items
        active_links = updated
        save_active_links()


async def fresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Compact a tag by removing trailing or specified useless indices.
    Usage:
        /fresh <tag>                    → remove trailing deleted/revoked/invalid until valid found
        /fresh <tag> <start> <end>      → remove useless indices in range (start to end)
        /fresh <tag> <i1> <i2> <i3> ... → remove only those specific indices if useless
    """
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text("Usage: /fresh <tag> [index1 index2 ...]")
        return

    tag = context.args[0].strip().lower()
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found.")
        return

    items = media_data[tag]
    def is_useless(it):
        if not isinstance(it, dict):
            return True
        if it.get('deleted') or it.get('revoked'):
            return True
        if not it.get('file_id'):
            return True
        return False

    removed_indices = set()
    args = [a.strip().lower() for a in context.args]

    if len(args) == 1:
        # Default: remove trailing useless indices only (from end until valid found)
        for i in range(len(items) - 1, -1, -1):
            if is_useless(items[i]):
                removed_indices.add(i)
            else:
                # Stop at first valid
                break
    elif len(args) == 3:
        # Range mode: /fresh tag start end
        try:
            start_i = int(args[1])
            end_i = int(args[2])
        except ValueError:
            await update.message.reply_text("❌ Invalid range. Use /fresh <tag> <start> <end>")
            return
        if start_i > end_i or start_i < 0:
            await update.message.reply_text("❌ Invalid range bounds")
            return
        end_i = min(end_i, len(items) - 1)
        for i in range(start_i, end_i + 1):
            if 0 <= i < len(items) and is_useless(items[i]):
                removed_indices.add(i)
    else:
        # Individual indices mode: /fresh tag i1 i2 i3 ...
        for arg in args[1:]:
            try:
                i = int(arg)
            except ValueError:
                continue
            if 0 <= i < len(items) and is_useless(items[i]):
                removed_indices.add(i)

    if not removed_indices:
        await update.message.reply_text(f"ℹ️ No trailing useless indices found in '{tag}'.")
        return

    old_items = list(items)
    removed_count, new_len = _shrink_tag_list(tag, removed_indices)
    _update_links_after_shrink(tag, old_items, media_data[tag])

    # Purge removed indices from deleted_media_storage (avoid confusion on recover)
    global deleted_media_storage
    for idx in sorted(removed_indices):
        key = f"{tag}_{idx}"
        deleted_media_storage.pop(key, None)
    save_deleted_media()

    await update.message.reply_html(
        f"✅ <b>Fresh Complete</b> for <code>{tag}</code>\n\n"
        f"🗑️ Removed slots: <b>{removed_count}</b>\n"
        f"📏 New size: <b>{new_len}</b>"
    )


async def forcefresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Aggressively compact one or more tags by removing ALL useless entries.
    Usage: /forcefresh <tag1> [tag2] [tag3] ...
    After forcefresh, deleted media indices are PURGED from storage and cannot be recovered.
    """
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text("Usage: /forcefresh <tag1> [tag2] [tag3] ...")
        return

    tags = [arg.strip().lower() for arg in context.args]
    summary = []

    global deleted_media_storage

    def is_useless_force(it):
        if not isinstance(it, dict):
            return True
        if it.get('deleted') or it.get('revoked'):
            return True
        if not it.get('file_id'):
            return True
        return False

    for tag in tags:
        if tag not in media_data:
            summary.append(f"  ❌ {tag}: not found")
            continue

        items = media_data[tag]
        removed_indices = {i for i, it in enumerate(items) if is_useless_force(it)}

        if not removed_indices:
            summary.append(f"  ℹ️ {tag}: already clean")
            continue

        old_items = list(items)
        removed_count, new_len = _shrink_tag_list(tag, removed_indices)
        _update_links_after_shrink(tag, old_items, media_data[tag])

        # PURGE removed indices from deleted_media_storage (permanent deletion)
        for idx in removed_indices:
            key = f"{tag}_{idx}"
            deleted_media_storage.pop(key, None)

        summary.append(f"  ✅ {tag}: {removed_count} purged → {new_len} remaining")

    save_media()
    save_passed_links()
    save_active_links()
    save_deleted_media()

    msg = "🔥 <b>Force Fresh Complete</b>\n\n" + "\n".join(summary)
    await update.message.reply_html(msg)

async def off_limits_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add media to the exclusive Off Limits category - /off_limits tag [start] [end]"""
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text(
            "Usage: /off_limits <tag> [start_index] [end_index]\n\n"
            "Examples:\n"
            "• /off_limits exclusive (entire tag)\n"
            "• /off_limits exclusive 0 10 (range)"
        )
        return

    tag = context.args[0].strip().lower()
    
    # Check if tag exists
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found.")
        return
    
    # Get videos list
    tag_videos = media_data[tag]
    
    # Default to entire tag
    start_index = 0
    end_index = len(tag_videos) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text("❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text("❌ Invalid end index. Please provide a number.")
            return
    
    # Add videos to off_limits_data
    global off_limits_data
    added_count = 0
    skipped_count = 0
    
    for idx in range(start_index, end_index + 1):
        if 0 <= idx < len(tag_videos):
            item = tag_videos[idx]
            # Skip deleted/revoked/invalid media
            if isinstance(item, dict) and item.get('deleted'):
                skipped_count += 1
                continue
            if isinstance(item, dict) and item.get('revoked'):
                skipped_count += 1
                continue
            if not (isinstance(item, dict) and item.get('file_id') and item.get('type')):
                skipped_count += 1
                continue
            
            video_key = f"{tag}_{idx}"
            if video_key not in off_limits_data:
                off_limits_data.append(video_key)
                added_count += 1
    
    if added_count > 0:
        save_off_limits()
        range_text = f"'{tag}' (indices {start_index}-{end_index})" if start_index != 0 or end_index != len(tag_videos) - 1 else f"entire tag '{tag}'"
        await update.message.reply_text(
            f"🏴‍☠️ <b>Off Limits Category Updated</b>\n\n"
            f"✅ Added {added_count} videos from {range_text}\n"
            f"{'⚠️ Skipped ' + str(skipped_count) + ' invalid/deleted videos' if skipped_count > 0 else ''}\n"
            f"📊 Total in Off Limits: {len(off_limits_data)}\n\n"
            f"💡 Users can access via 🏴‍☠️ Off Limits button",
            parse_mode=ParseMode.HTML
        )
    else:
        range_text = f"'{tag}' (indices {start_index}-{end_index})" if start_index != 0 or end_index != len(tag_videos) - 1 else f"entire tag '{tag}'"
        await update.message.reply_text(
            f"ℹ️ All videos in {range_text} were already in Off Limits or invalid.",
            parse_mode=ParseMode.HTML
        )


async def pass_link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pass videos and create shareable links (for both RANDOM MEDIA button and generated links)"""
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /passlink <tag> [start_index] [end_index]")
        return

    tag = context.args[0].strip().lower()
    
    # Check if tag exists
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found.")
        return
    
    # Get videos list (media_data[tag] is a list, not dict)
    tag_videos = media_data[tag]
    
    # Default to entire tag
    start_index = 0
    end_index = len(tag_videos) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end index. Please provide a number.")
            return
    
    # Check for existing entries with the same tag
    existing_entry = None
    should_merge = False
    
    # Look for existing entries with the same tag
    for key, data in active_links.items():
        if data.get("tag") == tag and data.get("type") == "passlink":
            existing_start = data.get("start_index", 0)
            existing_end = data.get("end_index", 0)
            
            # Check if ranges overlap or are adjacent (should merge)
            if (start_index <= existing_end + 1 and end_index >= existing_start - 1):
                existing_entry = key
                should_merge = True
                break
    
    if should_merge:
        # Merge with existing entry
        existing_data = active_links[existing_entry]
        existing_videos = existing_data.get("videos", []).copy()
        existing_start = existing_data.get("start_index", 0)
        existing_end = existing_data.get("end_index", 0)
        existing_indices = set(range(existing_start, existing_end + 1))
        
        # Add new indices to existing set
        new_indices = set(range(start_index, end_index + 1))
        all_indices = existing_indices.union(new_indices)
        
        # Determine the new link key for the merged range
        new_start = min(all_indices)
        new_end = max(all_indices)
        if new_start == 0 and new_end == len(tag_videos) - 1:
            # Full tag - store just tag name
            link_key = tag
        else:
            # Specific range - store range key
            link_key = f"{tag}_{new_start}_{new_end}"
        
        # Remove old entry if it has a different key
        if existing_entry != link_key:
            del active_links[existing_entry]
    
        # Store the video data directly in active_links
        active_links[link_key] = {
            "type": "passlink",
            "tag": tag,
            "start_index": min(all_indices),
            "end_index": max(all_indices),
            "videos": existing_videos,
            "actual_indices": sorted(list(all_indices))  # Store actual indices for display
        }
        save_active_links()
        
        await update.message.reply_text(
            f"🔄 Updated shareable link for '{tag}' (indices {start_index}-{end_index})\n"
            f"📊 Total videos in link: {len(existing_videos)}\n"
            f"🔗 Link: {link_key}\n"
            f"ℹ️ Videos NOT added to RANDOM MEDIA button",
            parse_mode=ParseMode.HTML
        )
    else:
        # Create new entry
        all_indices = set(range(start_index, end_index + 1))
        
        # Get videos for this range only, filtering out corrupted/deleted media
        all_videos = []
        valid_indices = []
        for idx in range(start_index, end_index + 1):
            if 0 <= idx < len(tag_videos):
                item = tag_videos[idx]
                # Only include valid media items
                if isinstance(item, dict) and "type" in item and "file_id" in item:
                    all_videos.append(item)
                    valid_indices.append(idx)
        
        # Determine the link key and range for storage
        if start_index == 0 and end_index == len(tag_videos) - 1:
            # Full tag - store just tag name
            link_key = tag
            link = f"https://t.me/{BOT_USERNAME}?start={tag}"
            range_text = f"entire tag '{tag}'"
        else:
            # Specific range - store range key
            link_key = f"{tag}_{start_index}_{end_index}"
            link = f"https://t.me/{BOT_USERNAME}?start={tag}_{start_index}_{end_index}"
            range_text = f"'{tag}' (indices {start_index}-{end_index})"
    
        # Store the video data directly in active_links
        active_links[link_key] = {
            "type": "passlink",
            "tag": tag,
            "start_index": start_index,
            "end_index": end_index,
            "videos": all_videos,
            "actual_indices": valid_indices  # Store only the indices of valid videos
        }
        save_active_links()
        
        await update.message.reply_text(
            f"✅ Created shareable link for '{tag}' (indices {start_index}-{end_index})\n"
            f"📊 Total videos in link: {len(all_videos)}\n"
            f"🔗 Link: {link}\n"
            f"ℹ️ Videos NOT added to RANDOM MEDIA button",
            parse_mode=ParseMode.HTML
        )


async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Revoke videos from RANDOM MEDIA button and Off Limits.
    Removes matching entries from passed_links and off_limits_data.
    Usage: /revoke <tag> [<start_index>] [<end_index>]
    """
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /revoke <tag> [<start_index>] [<end_index>]")
        return

    tag = context.args[0].strip().lower()
    
    # Check if tag exists
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found.")
        return
    
    # Get videos list (media_data[tag] is a list, not dict)
    tag_videos = media_data[tag]
    
    # Default to entire tag
    start_index = 0
    end_index = len(tag_videos) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end index. Please provide a number.")
            return
    
    # Remove videos from passed_links and off_limits
    videos_to_revoke = []
    offlimits_revoked = []
    for i in range(start_index, end_index + 1):
        if i < len(tag_videos):
            video_key = f"{tag}_{i}"  # Use index directly since it's a list
            if video_key in passed_links:
                passed_links.remove(video_key)
                videos_to_revoke.append(video_key)
            # Also remove from Off Limits if present
            if video_key in off_limits_data:
                try:
                    off_limits_data.remove(video_key)
                    offlimits_revoked.append(video_key)
                except Exception:
                    pass
    
    if videos_to_revoke or offlimits_revoked:
        save_passed_links()
        # Persist Off Limits changes if any
        if offlimits_revoked:
            try:
                save_off_limits()
            except Exception as e:
                print(f"⚠️ Failed to save Off Limits after revoke: {e}")
        count = len(videos_to_revoke)
        ol_count = len(offlimits_revoked)
        
        # Determine range text for response
        if start_index == 0 and end_index == len(tag_videos) - 1:
            range_text = f"entire tag '{tag}'"
        else:
            range_text = f"'{tag}' (indices {start_index}-{end_index})"
        
        lines = [f"🚫 Revoked {count} videos from {range_text} (RANDOM MEDIA)"]
        if ol_count:
            lines.append(f"🏴‍☠️ Also removed {ol_count} from Off Limits")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    else:
        range_text = f"'{tag}' (indices {start_index}-{end_index})" if start_index != 0 or end_index != len(tag_videos) - 1 else f"entire tag '{tag}'"
        await update.message.reply_text(
            f"ℹ️ No videos in {range_text} were in RANDOM MEDIA or Off Limits.",
            parse_mode=ParseMode.HTML
        )


async def revoke_link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Revoke shareable links (removes from active_links)"""
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /revokelink <tag> [start_index] [end_index]")
        return

    tag = context.args[0].strip().lower()

    # Determine what to remove from active_links
    if len(context.args) == 3:
        # Range format: /revokelink tag start end
        try:
            revoke_start = int(context.args[1])
            revoke_end = int(context.args[2])
            link_key = f"{tag}_{revoke_start}_{revoke_end}"
            range_text = f"'{tag}' (indices {revoke_start}-{revoke_end})"
        except ValueError:
            await update.message.reply_text("❌ Invalid indices. Please provide numbers.")
            return
    else:
        # Simple tag format: /revokelink tag
        link_key = tag
        range_text = f"tag '{tag}'"
        revoke_start = None
        revoke_end = None

    # Check if exact link exists
    if link_key in active_links:
        # Get info about what's being removed
        link_info = active_links[link_key]
        video_count = len(link_info.get("videos", [])) if isinstance(link_info, dict) else 0

        del active_links[link_key]
        save_active_links()
        await update.message.reply_text(
            f"🚫 Shareable link for {range_text} has been revoked.\n"
            f"📊 {video_count} videos removed from shareable access.",
            parse_mode=ParseMode.HTML)
        return

    # If exact match not found and this is a simple tag format, find any entries with this tag
    if revoke_start is None and revoke_end is None:
        # Find all entries with this tag
        matching_keys = []
        for existing_key, link_data in active_links.items():
            if isinstance(link_data, dict) and link_data.get("tag") == tag:
                matching_keys.append(existing_key)
        
        if matching_keys:
            # Remove all matching entries
            total_videos = 0
            for key in matching_keys:
                link_info = active_links[key]
                total_videos += len(link_info.get("videos", []))
                del active_links[key]
            
            save_active_links()
            await update.message.reply_text(
                f"🚫 Shareable link for {range_text} has been revoked.\n"
                f"📊 {total_videos} videos removed from shareable access.",
                parse_mode=ParseMode.HTML)
            return

    # If no modifications were made
    await update.message.reply_text(
        f"❌ Shareable link for {range_text} is not active.")
    return


async def activelinks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show shareable links created with /passlink command"""
    if not is_admin(update.effective_user.id):
        return

    if not active_links:
        await update.message.reply_text("📂 No shareable links created with /passlink.")
        return
    # Build a cleaner list without full URLs (user requested tag + indices only)
    msg = "<b>🔗 Shareable Links (Independent Storage):</b>\n\n"
    for link_key in sorted(active_links.keys()):
        link_data = active_links[link_key]
        if isinstance(link_data, dict):
            videos = link_data.get("videos", [])
            tag = link_data.get("tag", "unknown")
            actual_indices_raw = link_data.get("actual_indices", [])
            # Convert any string indices to int (can happen if loaded from JSON)
            actual_indices = sorted([int(x) if isinstance(x, str) else x for x in actual_indices_raw])
            start_idx = link_data.get("start_index")
            end_idx = link_data.get("end_index")

            if actual_indices:
                # Group continuous sequences
                ranges = []
                i = 0
                while i < len(actual_indices):
                    start = actual_indices[i]
                    end = start
                    while i + 1 < len(actual_indices) and actual_indices[i + 1] == actual_indices[i] + 1:
                        i += 1
                        end = actual_indices[i]
                    if start == end:
                        ranges.append(str(start))
                    else:
                        ranges.append(f"{start}-{end}")
                    i += 1
                range_info = ", ".join(ranges)
                msg += f"🔗 <code>{tag}</code> ({range_info}) ({len(videos)} videos)\n"
            elif start_idx is not None and end_idx is not None and start_idx != end_idx:
                msg += f"🔗 <code>{tag}</code> ({start_idx}-{end_idx}) ({len(videos)} videos)\n"
            else:
                msg += f"🔗 <code>{tag}</code> ({len(videos)} videos)\n"
        else:
            # Legacy simple tag
            msg += f"🔗 <code>{link_key}</code>\n"

    msg += "\n💡 These are /passlink shareable links (separate from RANDOM MEDIA)."
    await update.message.reply_html(msg)


async def passlinks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show active links for RANDOM MEDIA button (from /pass command)"""
    if not is_admin(update.effective_user.id):
        return

    # Collect tags and their indices that have videos in passed_links.json (from /pass command)
    tags_data = {}
    for video_key in passed_links:
        if '_' in video_key:
            # Extract tag name and index from video_key like "rd2_0" -> "rd2", index=0
            parts = video_key.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                tag = '_'.join(parts[:-1])
                index = int(parts[-1])
                
                if tag not in tags_data:
                    tags_data[tag] = set()
                tags_data[tag].add(index)

    if not tags_data:
        await update.message.reply_text("📂 No active links for RANDOM MEDIA button.")
        return

    msg = "<b>🎲 Active Links for RANDOM MEDIA Button:</b>\n\n"
    for tag in sorted(tags_data.keys()):
        indices = sorted(tags_data[tag])
        video_count = len(indices)

        # Build compact range description
        if len(indices) == 1:
            range_info = f"index {indices[0]}"
        elif indices == list(range(min(indices), max(indices) + 1)):
            range_info = f"indices {min(indices)}-{max(indices)}"
        else:
            if len(indices) <= 8:
                range_info = f"indices {', '.join(map(str, indices))}"
            else:
                range_info = f"indices {indices[0]}-{indices[-1]} ({video_count} total)"

        msg += f"🎲 <code>{tag}</code> ({range_info})\n"

    # Append Off Limits category summary
    if off_limits_data:
        off_limits_tags: dict[str, set[int]] = {}
        for key in off_limits_data:
            if '_' not in key:
                continue
            parts = key.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                tag = '_'.join(parts[:-1])
                idx = int(parts[-1])
                off_limits_tags.setdefault(tag, set()).add(idx)

        if off_limits_tags:
            msg += "\n<b>🏴‍☠️ Off Limits Category:</b>\n\n"
            for tag in sorted(off_limits_tags.keys()):
                indices = sorted(off_limits_tags[tag])
                count = len(indices)
                if len(indices) == 1:
                    range_info = f"index {indices[0]}"
                elif indices == list(range(min(indices), max(indices) + 1)):
                    range_info = f"indices {min(indices)}-{max(indices)}"
                else:
                    if len(indices) <= 8:
                        range_info = f"indices {', '.join(map(str, indices))}"
                    else:
                        range_info = f"indices {indices[0]}-{indices[-1]} ({count} total)"
                msg += f"🏴‍☠️ <code>{tag}</code> ({range_info})\n"

    msg += "\n💡 RANDOM MEDIA = /pass links. Off Limits shown separately above."
    await update.message.reply_html(msg)


async def listactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    # Collect all active links from both sources
    all_active_links = set()
    
    # Add links from active_links.json (from /passlink command)
    for link_key in active_links.keys():
        all_active_links.add(link_key)
    
    # Add tags that have videos in passed_links.json (from /pass command)
    tags_in_passed = set()
    for video_key in passed_links:
        if '_' in video_key:
            # Extract tag name from video_key like "rd2_0" -> "rd2"
            parts = video_key.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                tag = '_'.join(parts[:-1])
                tags_in_passed.add(tag)
    
    # Add tags from passed_links to all_active_links
    for tag in tags_in_passed:
        all_active_links.add(tag)

    if not all_active_links:
        await update.message.reply_text("📂 No active links.")
        return

    msg = "<b>🔗 Active Links:</b>\n"
    for link_key in sorted(all_active_links):
        link = f"https://t.me/{BOT_USERNAME}?start={link_key}"
        if '_' in link_key and link_key.count('_') >= 2:
            # Check if it's a range format
            parts = link_key.split('_')
            if len(parts) >= 3 and parts[-2].isdigit() and parts[-1].isdigit():
                tag = '_'.join(parts[:-2])
                start_idx = parts[-2]
                end_idx = parts[-1]
                source = "🔗" if link_key in active_links else "🎲"
                # Show video count for passlink created links
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{tag}</code> ({start_idx}-{end_idx}){video_count} - {link}\n"
            else:
                # Not a range, just display as is
                source = "🔗" if link_key in active_links else "🎲"
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
        else:
            # Simple tag format
            source = "🔗" if link_key in active_links else "🎲"
            video_count = ""
            if link_key in active_links and isinstance(active_links[link_key], dict):
                videos = active_links[link_key].get("videos", [])
                video_count = f" ({len(videos)} videos)"
            msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
    
    msg += "\n🔗 = Created with /passlink (independent storage)\n🎲 = Created with /pass (RANDOM MEDIA access)"
    await update.message.reply_html(msg)


async def show_favorites_navigator(query, context: ContextTypes.DEFAULT_TYPE, index=0, edit_message=False):
    """Show user's favorite videos with navigation"""
    user_id = str(query.from_user.id)

    user_fav_map = favorites_data.setdefault("user_favorites", {})
    user_favs = user_fav_map.get(user_id)

    if not user_favs:
        empty_fav_text = "❤️ <b>You haven't added any videos to favorites yet!</b>\n\n💡 <i>Start by clicking the ❤️ button on videos you like to add them to your favorites.</i>"
        empty_fav_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🎲 Random 18-", callback_data="random_18minus"),
                InlineKeyboardButton("🎲 Random 18+", callback_data="random_18plus")
            ]
        ])
        if edit_message:
            await query.edit_message_text(empty_fav_text, reply_markup=empty_fav_keyboard, parse_mode=ParseMode.HTML)
        else:
            await query.message.reply_text(empty_fav_text, reply_markup=empty_fav_keyboard, parse_mode=ParseMode.HTML)
        return

    if isinstance(user_favs, list):
        # Migrate legacy list storage into dict to keep metadata consistent
        migrated = {}
        for video_id in user_favs:
            if not isinstance(video_id, str) or '_' not in video_id:
                continue
            try:
                tag, idx_str = video_id.rsplit('_', 1)
                idx = int(idx_str)
            except ValueError:
                continue
            media_item, err = safe_get_media_item(tag, idx)
            entry = {
                "tag": tag,
                "index": idx,
                "file_id": media_item.get("file_id") if media_item else None,
                "video_id": video_id,
                "type": media_item.get("type", "video") if media_item else "video"
            }
            if entry["file_id"]:
                migrated[entry["file_id"]] = entry
            else:
                migrated[video_id] = entry
            record_video_metadata(tag, idx, entry["file_id"], video_id)
        user_fav_map[user_id] = migrated
        save_favorites()
        user_favs = migrated

    if not isinstance(user_favs, dict) or not user_favs:
        if edit_message:
            await query.edit_message_text("❤️ <b>No favorites available!</b>", parse_mode=ParseMode.HTML)
        else:
            await query.message.reply_text("❤️ <b>No favorites available!</b>", parse_mode=ParseMode.HTML)
        return

    fav_list = list(user_favs.items())
    total_favorites = len(fav_list)

    if index >= total_favorites:
        index = 0
    elif index < 0:
        index = total_favorites - 1

    try:
        file_id, fav_data = fav_list[index]
        tag = fav_data.get("tag")
        current_idx = fav_data.get("index")

        if tag and tag in media_data:
            if current_idx is None or current_idx >= len(media_data[tag]) or not isinstance(media_data[tag][current_idx], dict):
                current_idx = None
                for idx, media_item in enumerate(media_data[tag]):
                    if isinstance(media_item, dict) and media_item.get("file_id") == file_id:
                        current_idx = idx
                        fav_data["index"] = current_idx
                        break

        if tag is None or current_idx is None or tag not in media_data or current_idx >= len(media_data[tag]):
            del user_fav_map[user_id][file_id]
            if file_id in favorites_data["video_likes"]:
                favorites_data["video_likes"][file_id] = max(0, favorites_data["video_likes"][file_id] - 1)
            save_favorites()
            if user_fav_map[user_id]:
                await show_favorites_navigator(query, context, index, edit_message)
            else:
                if edit_message:
                    await query.edit_message_text("⭐ No more favorites available!")
                else:
                    await query.message.reply_text("⭐ No more favorites available!")
            return

        media_item = media_data[tag][current_idx]
        video_id = f"{tag}_{current_idx}"
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{current_idx}_{current_idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        media_type = media_item.get("type", "video")
        base_caption = build_media_caption("", tag, str(current_idx), share_link, media_type)
        cap = f"⭐ <b>Favorite {index + 1}/{total_favorites}</b>\n{base_caption}"

        nav_buttons = []
        if total_favorites > 1:
            prev_index = index - 1 if index > 0 else total_favorites - 1
            next_index = index + 1 if index < total_favorites - 1 else 0
            nav_buttons.append([
                InlineKeyboardButton("⬅️ Previous", callback_data=f"fav_nav_{prev_index}"),
                InlineKeyboardButton("➡️ Next", callback_data=f"fav_nav_{next_index}")
            ])

        nav_buttons.append([
            InlineKeyboardButton("💔 Remove from Favorites", callback_data=f"remove_fav_{video_id}")
        ])
        nav_buttons.append([
            InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
        ])
        if is_admin(query.from_user.id):
            nav_buttons.append([
                InlineKeyboardButton("🗑️ Remove Media", callback_data=f"del_media_{video_id}")
            ])

        keyboard = InlineKeyboardMarkup(nav_buttons)

        media_kwargs = {
            "context": context,
            "chat_id": query.message.chat_id,
            "caption": cap,
            "reply_markup": keyboard,
            "parse_mode": ParseMode.HTML,
            "protect_content": should_protect_content(query.from_user.id, query.message.chat_id)
        }

        if media_item["type"] == "video":
            media_kwargs["video"] = media_item["file_id"]
        elif media_item["type"] == "photo":
            media_kwargs["photo"] = media_item["file_id"]
        elif media_item["type"] == "document":
            media_kwargs["document"] = media_item["file_id"]
        elif media_item["type"] == "audio":
            media_kwargs["audio"] = media_item["file_id"]
        elif media_item["type"] == "voice":
            media_kwargs["voice"] = media_item["file_id"]
        elif media_item["type"] == "animation":
            media_kwargs["animation"] = media_item["file_id"]
        elif media_item["type"] == "sticker":
            media_kwargs["sticker"] = media_item["file_id"]
            media_kwargs.pop("caption", None)
        else:
            media_kwargs["text"] = f"Unsupported media type: {media_item['type']}"

        await safe_send_message(**media_kwargs)
    except Exception as e:
        print(f"Error showing favorite: {e}")
        if edit_message:
            await query.edit_message_text("❌ Error loading favorite video.")
        else:
            await query.message.reply_text("❌ Error loading favorite video.")


def is_video_favorited(user_id_str: str, tag: str, index: int, file_id: str = None) -> bool:
    """Determine if a video is favorited.
    Supports legacy list storage (video_id keys) and new dict storage (file_id or video_id fallback).
    Uses safe_get_media_item to avoid numeric/dict indexing issues.
    """
    if user_id_str not in favorites_data.get("user_favorites", {}):
        return False
    user_favs = favorites_data["user_favorites"][user_id_str]
    video_id = f"{tag}_{index}"

    # Legacy list format
    if isinstance(user_favs, list):
        return video_id in user_favs

    if not isinstance(user_favs, dict):
        return False

    # Direct file_id match
    if file_id and file_id in user_favs:
        return True
    # video_id fallback key
    if video_id in user_favs:
        return True

    # Safe media lookup then re-check by actual file_id
    media_item, err = safe_get_media_item(tag, index)
    if not err and media_item:
        fid = media_item.get('file_id')
        if fid and fid in user_favs:
            return True
    return False


def ensure_user_favorites_dict(user_id_str: str) -> dict:
    """Return a dict-backed favorites map for the user, migrating legacy lists if necessary."""
    user_favs = favorites_data.get("user_favorites", {}).get(user_id_str)
    if isinstance(user_favs, dict):
        return user_favs

    if isinstance(user_favs, list):
        migrated = {}
        for video_key in user_favs:
            if not isinstance(video_key, str):
                continue
            parts = video_key.rsplit('_', 1)
            if len(parts) != 2:
                continue
            tag, idx_str = parts
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            media_item, err = safe_get_media_item(tag, idx)
            entry = {
                "tag": tag,
                "index": idx,
                "file_id": media_item.get("file_id") if media_item else None,
                "video_id": video_key,
                "type": media_item.get("type", "video") if media_item else "video"
            }
            migrated[video_key] = entry
            record_video_metadata(tag, idx, entry["file_id"], entry["video_id"])
        favorites_data.setdefault("user_favorites", {})[user_id_str] = migrated
        return migrated

    container = {}
    favorites_data.setdefault("user_favorites", {})[user_id_str] = container
    return container


def record_video_metadata(tag: str, idx: int, file_id: str | None, video_id: str) -> dict:
    """Keep a metadata map for top/video lookups keyed by video_id and file_id."""
    metadata_map = favorites_data.setdefault("video_metadata", {})
    entry = {
        "tag": tag,
        "index": idx,
        "file_id": file_id,
        "video_id": video_id
    }
    metadata_map[video_id] = entry
    if file_id:
        metadata_map[file_id] = entry
    return entry


def get_video_metadata(key: str) -> dict | None:
    """Resolve the recorded metadata for a like key (file_id or tag_index)."""
    metadata_map = favorites_data.get("video_metadata", {})
    entry = metadata_map.get(key)
    if entry:
        return entry
    if "_" in key:
        parts = key.rsplit("_", 1)
        if len(parts) == 2:
            tag, idx_str = parts
            try:
                idx = int(idx_str)
            except ValueError:
                return None
            entry = {
                "tag": tag,
                "index": idx,
                "file_id": None,
                "video_id": key
            }
            metadata_map[key] = entry
            return entry
    return None


def get_sorted_canonical_likes():
    """Return a list of (canonical_key, count) sorted by count desc.
    canonical_key is file_id if recorded, otherwise original key (tag_idx).
    """
    likes_map = favorites_data.get("video_likes", {})
    canonical_counts = {}
    try:
        for key, cnt in likes_map.items():
            md = get_video_metadata(key)
            if md and md.get("file_id"):
                ck = md.get("file_id")
            else:
                ck = key
            canonical_counts[ck] = canonical_counts.get(ck, 0) + cnt
    except Exception:
        # Fallback to raw likes_map
        canonical_counts = dict(likes_map)
    sorted_list = sorted(canonical_counts.items(), key=lambda x: x[1], reverse=True)
    return sorted_list


def get_likes_count(key: str) -> int:
    """Return the authoritative like count for a video key (file_id or tag_idx).
    Prefers file_id if mapping exists.
    """
    try:
        md = get_video_metadata(key)
        # If the metadata exists and has a file_id, use it as canonical key
        if md and md.get("file_id"):
            ck = md.get("file_id")
        else:
            # Attempt to populate metadata from media_data when possible
            if md and md.get("tag") and md.get("index") is not None:
                tag = md.get("tag")
                idx = md.get("index")
                media_item, err = safe_get_media_item(tag, idx)
                if not err and media_item and isinstance(media_item, dict) and media_item.get("file_id"):
                    record_video_metadata(tag, idx, media_item.get("file_id"), md.get("video_id"))
                    ck = media_item.get("file_id")
                else:
                    ck = key
            else:
                # Fallback: if we can parse tag_idx from key and find the media item
                if "_" in key:
                    try:
                        tag, idx_str = key.rsplit("_", 1)
                        idx = int(idx_str)
                        media_item, err = safe_get_media_item(tag, idx)
                        if not err and media_item and isinstance(media_item, dict) and media_item.get("file_id"):
                            record_video_metadata(tag, idx, media_item.get("file_id"), key)
                            ck = media_item.get("file_id")
                        else:
                            ck = key
                    except Exception:
                        ck = key
                else:
                    ck = key
        return favorites_data.get("video_likes", {}).get(ck, 0)
    except Exception:
        return favorites_data.get("video_likes", {}).get(key, 0)


async def add_to_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Add a video to user's favorites with fast UI feedback.
    Optimizes responsiveness by immediately acknowledging the tap and
    saving to disk asynchronously to avoid blocking the event loop.
    """
    user_id = str(query.from_user.id)
    video_id = query.data.replace("add_fav_", "")
    
    # Parse tag and index from video_id
    try:
        tag, idx_str = video_id.rsplit('_', 1)
        idx = int(idx_str)  # Force integer conversion
    except Exception as e:
        print(f"[favorites:add] parse error for {video_id}: {e}")
        await safe_answer_callback_query(query, "❌ Invalid video ID!", show_alert=False, timeout=1.5)
        return
    
    media_item, err = safe_get_media_item(tag, idx)
    if err:
        print(f"[favorites:add] {err} (video_id={video_id})")
        await query.answer("❌ Media not found!")
        return
    file_id = media_item.get('file_id')
    if not file_id:
        # Allow fallback to video_id key; still mark for potential later migration
        print(f"[favorites:add] Missing file_id; using fallback video_id key {video_id}")
    
    # Ensure the user has a dict-backed favorites container
    user_favs = ensure_user_favorites_dict(user_id)

    # Choose key: prefer file_id else video_id fallback
    fav_key = file_id if file_id else video_id
    if fav_key not in user_favs:
        user_favs[fav_key] = {
            "tag": tag,
            "index": idx,
            "file_id": file_id,
            "video_id": video_id,
            "type": media_item.get('type', 'video')
        }
        record_video_metadata(tag, idx, file_id, video_id)
        
        # Update video likes count by canonical key (prefer file_id, else video_id)
        canonical_key = file_id if file_id else video_id
        if canonical_key not in favorites_data["video_likes"]:
            favorites_data["video_likes"][canonical_key] = 0
        favorites_data["video_likes"][canonical_key] += 1

        # Save asynchronously to keep UI snappy
        try:
            await asyncio.to_thread(save_favorites)
        except Exception:
            save_favorites()
        # Recalculate likes from authoritative source (users' favorites) and normalize keys, thread off to avoid blocking
        try:
            await asyncio.to_thread(recalc_video_likes_from_favorites)
            await asyncio.to_thread(normalize_video_likes_counts)
        except Exception:
            recalc_video_likes_from_favorites()
            normalize_video_likes_counts()
        # Update the caption of current message if it belongs to Top Videos viewer
        try:
            await refresh_message_caption_likes(query, video_id)
        except Exception:
            pass

        # Immediately acknowledge the button tap
        await safe_answer_callback_query(query, "❤️ Added to favorites!", show_alert=False, timeout=1.5)

        # Toggle only the favorite button in the existing keyboard
        updated_markup = update_favorite_button_in_keyboard(query.message.reply_markup, video_id, is_adding=True)
        try:
            await query.edit_message_reply_markup(reply_markup=updated_markup)
        except Exception as e:
            print(f"Could not edit markup for add_fav: {e}")
        # Refresh all active top viewer messages to keep counts in sync
        try:
            await refresh_all_active_top_viewers(context)
        except Exception:
            pass
    else:
        await safe_answer_callback_query(query, "Already in favorites!", show_alert=False, timeout=1.5)


async def remove_from_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Remove a video from user's favorites with fast UI feedback.
    Saves to disk asynchronously to prevent callback delays.
    """
    user_id = str(query.from_user.id)
    video_id = query.data.replace("remove_fav_", "")
    
    # Parse tag and index to find file_id
    try:
        tag, idx_str = video_id.rsplit('_', 1)
        idx = int(idx_str)  # Force integer conversion
    except Exception as e:
        print(f"[favorites:remove] parse error for {video_id}: {e}")
        await safe_answer_callback_query(query, "❌ Invalid video ID!", show_alert=False, timeout=1.5)
        return
    
    # Find the file_id from current media_data
    file_id_to_remove = None
    media_item, err = safe_get_media_item(tag, idx)
    if not err and media_item and isinstance(media_item, dict):
        file_id_to_remove = media_item.get('file_id') or video_id
    
    user_favs = ensure_user_favorites_dict(user_id)

    # In case stored favorites were keyed by video_id instead of file_id, search metadata
    if not file_id_to_remove:
        for fav_key, fav_data in list(user_favs.items()):
            if fav_data.get('tag') == tag and fav_data.get('index') == idx:
                file_id_to_remove = fav_key
                break
    
    if file_id_to_remove:
        # Remove by direct key if exists else search by video_id metadata
        if file_id_to_remove in user_favs:
            del user_favs[file_id_to_remove]
        else:
            for k, v in list(user_favs.items()):
                if v.get('video_id') == video_id:
                    del user_favs[k]
                    file_id_to_remove = v.get('file_id', file_id_to_remove)
                    break

        # Update video likes count for canonical key(s)
        if file_id_to_remove:
            # Decrement by file_id key first if exists
            if file_id_to_remove in favorites_data.get("video_likes", {}):
                favorites_data["video_likes"][file_id_to_remove] = max(0, favorites_data["video_likes"][file_id_to_remove] - 1)
            # Also decrement fallback video_id key if present
            if video_id in favorites_data.get("video_likes", {}):
                favorites_data["video_likes"][video_id] = max(0, favorites_data["video_likes"][video_id] - 1)
        # Also run authoritative recalc and normalize in background to avoid mismatch
        try:
            await asyncio.to_thread(recalc_video_likes_from_favorites)
            await asyncio.to_thread(normalize_video_likes_counts)
        except Exception:
            recalc_video_likes_from_favorites()
            normalize_video_likes_counts()

        # Save asynchronously to keep UI snappy
        try:
            await asyncio.to_thread(save_favorites)
        except Exception:
            save_favorites()

        # Immediate acknowledge
        await safe_answer_callback_query(query, "💔 Removed from favorites!", show_alert=False, timeout=1.5)
        
        # Toggle only the favorite button in the existing keyboard
        updated_markup = update_favorite_button_in_keyboard(query.message.reply_markup, video_id, is_adding=False)
        try:
            await query.edit_message_reply_markup(reply_markup=updated_markup)
        except Exception as e:
            print(f"Could not edit markup for remove_fav: {e}")
        # Refresh all active top viewer messages to keep counts in sync
        try:
            await refresh_all_active_top_viewers(context)
        except Exception:
            pass
    else:
        await safe_answer_callback_query(query, "Not in favorites!", show_alert=False, timeout=1.5)


async def show_who_liked_video(query, context: ContextTypes.DEFAULT_TYPE):
    """Admin function to show which users have liked a specific video"""
    # Check if user is admin
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ Admin only feature!")
        return
    
    # Extract video_id from callback data
    video_id = query.data.replace("who_liked_", "")
    tag = None
    idx = None
    file_id = None
    if "_" in video_id:
        parts = video_id.rsplit("_", 1)
        if len(parts) == 2:
            tag, idx_str = parts
            try:
                idx = int(idx_str)
            except ValueError:
                idx = None
    
    if tag is not None and idx is not None:
        media_item, err = safe_get_media_item(tag, idx)
        if not err and media_item:
            file_id = media_item.get("file_id")
    
    # Find users who liked this video
    users_who_liked = []
    for user_id in list(favorites_data.get("user_favorites", {}).keys()):
        user_favs = ensure_user_favorites_dict(user_id)
        for fav_key, fav_data in user_favs.items():
            if not isinstance(fav_data, dict):
                continue
            match = False
            if tag is not None and idx is not None and fav_data.get("tag") == tag and fav_data.get("index") == idx:
                match = True
            elif file_id and fav_key == file_id:
                match = True
            elif file_id and fav_data.get("file_id") == file_id:
                match = True
            elif fav_data.get("video_id") == video_id:
                match = True
            if match:
                try:
                    user = await context.bot.get_chat(user_id)
                    display_name = user.first_name or user.username or f"User {user_id}"
                    users_who_liked.append(f"👤 {display_name} (ID: {user_id})")
                except:
                    users_who_liked.append(f"👤 User ID: {user_id}")
                break
    
    # Calculate authoritative likes count from listed users and use it for display
    total_likes = len(users_who_liked)
    # Ensure favorites_data reflects actual users; update video_likes map if incorrect
    try:
        canonical_key = file_id if file_id else video_id
        old_val = favorites_data.get("video_likes", {}).get(canonical_key, 0)
        if old_val != total_likes:
            favorites_data.setdefault("video_likes", {})[canonical_key] = total_likes
            try:
                # Save and optionally persist in background
                asyncio.create_task(asyncio.to_thread(save_favorites))
            except Exception:
                save_favorites()
    except Exception:
        pass
    
    # Build video info display fallback
    if tag is not None and idx is not None:
        video_info = f"📹 Video: <code>{tag}</code> | Index: <code>{idx}</code>"
    else:
        video_info = f"📹 Video: <code>{video_id}</code>"
    
    # Create response message
    if users_who_liked:
        user_list = "\n".join(users_who_liked)
        message = (
            f"👥 <b>Users who liked this video:</b>\n\n"
            f"{video_info}\n"
            f"❤️ Total likes: <b>{total_likes}</b>\n\n"
            f"{user_list}"
        )
    else:
        message = (
            f"💔 <b>No users have liked this video yet</b>\n\n"
            f"{video_info}\n"
            f"❤️ Total likes: <b>{total_likes}</b>"
        )
    
    await query.message.reply_html(message)
    await query.answer()


async def view_specific_video(query, context: ContextTypes.DEFAULT_TYPE):
    """Admin function to view a specific video from the top liked list"""
    # Check if user is admin
    if not is_admin(query.from_user.id):
        await query.answer("❌ Admin only feature!")
        return
    
    # Extract video_id from callback data
    video_id = query.data.replace("view_video_", "")
    
    try:
        # Parse video_id to get tag and index
        tag, idx_str = video_id.rsplit("_", 1)
        idx = int(idx_str)
        
        # Check if video exists in media_data
        if tag not in media_data or idx >= len(media_data[tag]):
            await query.message.reply_text(f"❌ Video not found: {tag}_{idx}")
            await query.answer()
            return
        
        # Get the video data
        video_data = media_data[tag][idx]
        
        # Create caption with video info and stats
        likes_count = get_likes_count(video_id)
        cap = (
            f"🎬 <b>Direct Video View</b>\n\n"
            f"📁 Tag: <code>{tag}</code>\n"
            f"📊 Index: <code>{idx}</code>\n"
            f"❤️ Total Likes: <b>{likes_count}</b>"
        )
        
        # Create admin buttons for this video
        fav_button = InlineKeyboardButton("❤️ Add to Favorites", 
                        callback_data=f"add_fav_{video_id}")
        who_liked_button = InlineKeyboardButton("👥 WHO", 
                              callback_data=f"who_liked_{video_id}")
        my_favs_button = InlineKeyboardButton("⭐ MY FAV", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
        
        keyboard = InlineKeyboardMarkup([
            [fav_button, who_liked_button],
            [my_favs_button, random_button],
            [InlineKeyboardButton("🗑️ Remove Media", callback_data=f"del_media_{video_id}")]
        ])
        
        # Send the video
        if video_data.get("type") == "video" and "file_id" in video_data:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                video=video_data["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
            )
        elif video_data.get("type") == "photo" and "file_id" in video_data:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                photo=video_data["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
            )
        else:
            await query.message.reply_text(f"❌ Invalid media data for {video_id}")
        
        await query.answer("🎬 Video loaded!")
        
    except ValueError:
        await query.message.reply_text(f"❌ Invalid video format: {video_id}")
        await query.answer()
    except Exception as e:
        await query.message.reply_text(f"❌ Error loading video: {str(e)}")
        await query.answer()


async def view_deleted_media(query, context: ContextTypes.DEFAULT_TYPE):
    """View a specific deleted media with restore option"""
    global deleted_media_storage
    
    try:
        # Extract video key from callback data (format: view_deleted_tag_idx)
        callback_data = query.data
        video_key = callback_data.replace("view_deleted_", "")
        
        print(f"🔍 Viewing deleted media: {video_key}")
        
        # Check if the deleted media exists
        if video_key not in deleted_media_storage:
            await query.message.reply_text("❌ Deleted media not found!")
            await query.answer()
            return
        
        # Get the deleted media data
        deleted_entry = deleted_media_storage[video_key]
        deleted_media = deleted_entry["data"]  # The actual media data is nested in "data"
        tag = deleted_entry["tag"]
        original_position = deleted_entry["original_position"]
        
        print(f"🔍 Retrieved deleted media data: type={deleted_media.get('type', 'N/A')}, has_file_id={'file_id' in deleted_media}")
        
        # Create caption for deleted media
        cap = (
            f"🗑️ <b>Deleted Media View</b>\n\n"
            f"📁 Tag: <code>{tag}</code>\n"
            f"📊 Original Position: <code>{original_position}</code>\n"
            f"⚠️ Status: <b>DELETED</b>\n"
            f"💾 Stored: {deleted_entry.get('deleted_date', 'Unknown')}"
        )
        
        # Create restore button
        restore_button = InlineKeyboardButton("🔄 Restore Media", 
                                            callback_data=f"restore_media_{video_key}")
        back_button = InlineKeyboardButton("⬅️ Back to Deleted List", 
                                         callback_data="list_deleted_media")
        
        keyboard = InlineKeyboardMarkup([
            [restore_button],
            [back_button]
        ])
        
        # Send the deleted media
        if deleted_media.get("type") == "video" and "file_id" in deleted_media:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                video=deleted_media["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
            )
        elif deleted_media.get("type") == "photo" and "file_id" in deleted_media:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                photo=deleted_media["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
            )
        else:
            # Debug information for troubleshooting
            print(f"❌ Invalid deleted media data for {video_key}")
            print(f"    Media type: {deleted_media.get('type', 'MISSING')}")
            print(f"    Has file_id: {'file_id' in deleted_media}")
            print(f"    Media keys: {list(deleted_media.keys())}")
            await query.message.reply_text(f"❌ Invalid deleted media data for {video_key}\nType: {deleted_media.get('type', 'MISSING')}\nHas file_id: {'file_id' in deleted_media}")
        
        await query.answer("🗑️ Deleted media loaded!")
        
    except Exception as e:
        await query.message.reply_text(f"❌ Error loading deleted media: {str(e)}")
        await query.answer()
        print(f"❌ Error in view_deleted_media: {str(e)}")


async def cleanup_deleted_media():
    """Clean up corrupted or invalid deleted media entries"""
    global deleted_media_storage
    
    corrupted_keys = []
    
    print(f"🧹 Starting cleanup of deleted media storage ({len(deleted_media_storage)} entries)")
    
    for video_key, deleted_entry in deleted_media_storage.items():
        try:
            # Check if the entry has the required structure
            if not isinstance(deleted_entry, dict):
                print(f"❌ Corrupted entry (not dict): {video_key}")
                corrupted_keys.append(video_key)
                continue
                
            if "data" not in deleted_entry:
                print(f"❌ Corrupted entry (no 'data' field): {video_key}")
                corrupted_keys.append(video_key)
                continue
                
            media_data = deleted_entry["data"]
            if not isinstance(media_data, dict):
                print(f"❌ Corrupted entry (data not dict): {video_key}")
                corrupted_keys.append(video_key)
                continue
                
            # Check if media data has required fields
            if "file_id" not in media_data or "type" not in media_data:
                print(f"❌ Invalid media data (missing file_id or type): {video_key}")
                corrupted_keys.append(video_key)
                continue
                
            # Silent validation - only show errors
            
        except Exception as e:
            print(f"\n❌ Error checking entry {video_key}: {str(e)}")
            corrupted_keys.append(video_key)
    
    # Remove corrupted entries
    if corrupted_keys:
        print(f"\n🗑️ Removing {len(corrupted_keys)} corrupted entries: {corrupted_keys}")
        for key in corrupted_keys:
            del deleted_media_storage[key]
        save_deleted_media()
        print(f"✅ Cleanup complete. {len(deleted_media_storage)} entries remaining")
    
    return len(corrupted_keys)


async def show_search_results_page(update: Update, context: ContextTypes.DEFAULT_TYPE, results: list, query: str, page: int = 0, edit_msg=False):
    """Show search results page with navigation and preview button"""
    ITEMS_PER_PAGE = 10
    total_pages = (len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(results))
    page_results = results[start_idx:end_idx]
    
    result_text = f"🔍 Found {len(results)} result(s) for '{query}'"
    if total_pages > 1:
        result_text += f" (Page {page + 1}/{total_pages})"
    
    # Create inline keyboard with results
    keyboard = []
    for i, result in enumerate(page_results, start_idx + 1):
        display_name = result['caption'] if result['caption'] else result['filename']
        if not display_name:
            display_name = f"{result['tag']} #{result['index']}"
        
        # Truncate long names for button
        if len(display_name) > 40:
            display_name = display_name[:37] + "..."
        
        # Add button for this result
        callback_data = f"search_result_{result['tag']}_{result['index']}"
        keyboard.append([InlineKeyboardButton(
            f"{i}. {display_name}",
            callback_data=callback_data
        )])
    
    # Add navigation and preview buttons
    nav_buttons = []
    if total_pages > 1:
        # Always show Previous button (wraps to last page from first page)
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"search_page_{page - 1}"))
        else:
            # On first page, Previous jumps to last page for reverse navigation
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev (Last)", callback_data=f"search_page_{total_pages - 1}"))
        
        # Preview button in the middle
        nav_buttons.append(InlineKeyboardButton("▶️ Preview All", callback_data="search_preview_all"))
        
        # Always show Next button (wraps to first page from last page)
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"search_page_{page + 1}"))
        else:
            # On last page, Next jumps to first page
            nav_buttons.append(InlineKeyboardButton("Next (First) ➡️", callback_data=f"search_page_0"))
    else:
        # Just preview button if single page with >1 results
        if len(results) > 1:
            nav_buttons.append(InlineKeyboardButton("▶️ Preview All", callback_data="search_preview_all"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Check if search query matches a tag in passed_links (random media)
    tag_in_random = query in media_data and any(f"{query}_" in link for link in passed_links)
    if tag_in_random:
        keyboard.append([InlineKeyboardButton(f"🎲 Watch from {query.upper()}", callback_data=f"random_from_tag_{query}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if edit_msg and update.callback_query:
        await update.callback_query.edit_message_text(
            result_text,
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            result_text,
            reply_markup=reply_markup
        )


def normalize_search_text(text):
    """Normalize text by treating special characters as spaces for word matching"""
    import re
    # Convert to lowercase
    text = text.lower()
    # Replace all special characters including . _ - with spaces for better word matching
    text = re.sub(r'[^a-z0-9]', ' ', text)
    # Remove extra spaces and strip
    text = ' '.join(text.split())
    return text

def calculate_match_score(query_normalized, text_normalized, original_text):
    """Calculate match score based on word matching and substring matching"""
    # Exact match gets highest score
    if query_normalized == text_normalized:
        return 1000
    
    # Check if text starts with query (very good match)
    if text_normalized.startswith(query_normalized):
        return 900
    
    # Word-based matching: check if all query words are in text (in order)
    query_words = query_normalized.split()
    text_words = text_normalized.split()
    
    # Check if query is a phrase match (all words appear consecutively)
    text_as_phrase = ' '.join(text_words)
    if query_normalized in text_as_phrase:
        position_bonus = min(100, 100 - text_as_phrase.index(query_normalized))
        return 500 + position_bonus
    
    # Check if all query words appear in text in order (not necessarily consecutive)
    if len(query_words) > 0:
        text_idx = 0
        matches = 0
        match_positions = []
        for qword in query_words:
            for i in range(text_idx, len(text_words)):
                # Check if query word matches text word (either contains or partial match)
                if qword == text_words[i]:  # Exact word match
                    matches += 1
                    match_positions.append(i)
                    text_idx = i + 1
                    break
                elif qword in text_words[i]:  # Query word is part of text word
                    matches += 1
                    match_positions.append(i)
                    text_idx = i + 1
                    break
                elif text_words[i] in qword:  # Text word is part of query word
                    matches += 1
                    match_positions.append(i)
                    text_idx = i + 1
                    break
        
        # Calculate score based on how many words matched
        if matches > 0:
            match_ratio = matches / len(query_words)
            base_score = 300 + (match_ratio * 100)
            
            # Bonus for matching all words
            if matches == len(query_words):
                base_score += 50
            
            # Bonus for consecutive matches
            if len(match_positions) > 1:
                consecutive = all(match_positions[i] + 1 == match_positions[i+1] 
                                for i in range(len(match_positions) - 1))
                if consecutive:
                    base_score += 30
            
            return base_score
    
    # Calculate longest common subsequence length
    query_len = len(query_normalized)
    text_len = len(text_normalized)
    
    # Simple longest common substring matching
    max_match = 0
    for i in range(query_len):
        for j in range(text_len):
            k = 0
            while (i + k < query_len and j + k < text_len and 
                   query_normalized[i + k] == text_normalized[j + k]):
                k += 1
            max_match = max(max_match, k)
    
    # Score based on percentage of query matched
    if query_len > 0:
        match_percentage = (max_match / query_len) * 100
        return match_percentage
    
    return 0

async def search_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search media by caption or filename with fuzzy matching"""
    query = update.message.text.strip().lower()
    
    # Don't search for very short queries or commands
    if len(query) < 3 or query.startswith('/'):
        return
    
    # 🚫 Disable search results in groups - only allow in private chats
    if update.message.chat.type != 'private':
        return  # Let other handlers (tag matching) process the message
    
    # Normalize query for fuzzy matching
    query_normalized = normalize_search_text(query)
    
    results = []
    
    # Search through all tags and media
    for tag, media_list in media_data.items():
        for idx, media_item in enumerate(media_list):
            if isinstance(media_item, dict) and not media_item.get("deleted"):
                # Search in caption first
                caption = media_item.get("caption", "")
                filename = media_item.get("filename", "")
                
                match_score = 0
                match_type = ""
                
                # Calculate match score for caption
                if caption:
                    caption_normalized = normalize_search_text(caption)
                    caption_score = calculate_match_score(query_normalized, caption_normalized, caption)
                    
                    if caption_score > match_score:
                        match_score = caption_score
                        match_type = "caption"
                
                # Calculate match score for filename
                if filename:
                    filename_normalized = normalize_search_text(filename)
                    filename_score = calculate_match_score(query_normalized, filename_normalized, filename)
                    
                    if filename_score > match_score:
                        match_score = filename_score
                        match_type = "filename"
                
                # Only include results with reasonable match scores (>20% match)
                if match_score > 20:
                    results.append({
                        "tag": tag,
                        "index": idx,
                        "caption": caption,
                        "filename": filename,
                        "type": media_item.get("type", "unknown"),
                        "match_type": match_type,
                        "file_id": media_item.get("file_id"),
                        "match_score": match_score
                    })
    
    if not results:
        return  # No results found, let other handlers process the message
    
    # Sort by match score (best matches first)
    results.sort(key=lambda x: x['match_score'], reverse=True)
    
    # Store ALL results in context for preview and navigation (no limit)
    context.user_data['search_results'] = results
    context.user_data['search_query'] = query
    
    # Show first page with navigation
    await show_search_results_page(update, context, results, query, page=0)


async def handle_search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle search page navigation"""
    query = update.callback_query
    await query.answer()
    
    page = int(query.data.replace("search_page_", ""))
    results = context.user_data.get('search_results', [])
    search_query = context.user_data.get('search_query', '')
    
    if not results:
        await query.answer("❌ Search results expired", show_alert=True)
        return
    
    await show_search_results_page(update, context, results, search_query, page=page, edit_msg=True)


async def handle_search_preview_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle preview all search results"""
    query = update.callback_query
    await query.answer("▶️ Starting preview...")
    
    results = context.user_data.get('search_results', [])
    if not results:
        await query.answer("❌ Search results expired", show_alert=True)
        return
    
    # Show stop button
    stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True)
    await query.message.reply_text(
        f"▶️ Sending {len(results)} media file(s)...",
        reply_markup=stop_keyboard
    )
    
    # Set up cancel key
    cancel_key = f"search_preview_cancel_{query.message.chat_id}"
    context.chat_data[cancel_key] = False
    
    # Store resume state
    context.user_data['preview_results'] = results
    context.user_data['preview_index'] = 0
    
    # Initialize checkpoint tracking (reset for new search)
    context.user_data['preview_last_checkpoint'] = 0
    
    sent_count = 0
    for idx, result in enumerate(results):
        # Check for cancel
        if context.chat_data.get(cancel_key):
            context.user_data['preview_index'] = idx
            break
        
        tag = result['tag']
        index = result['index']
        
        if tag not in media_data or index >= len(media_data[tag]):
            continue
        
        media_item = media_data[tag][index]
        if isinstance(media_item, dict) and media_item.get("deleted"):
            continue
        
        file_id = media_item.get("file_id")
        media_type = media_item.get("type", "video")
        caption = media_item.get("caption", "") or media_item.get("filename", "")
        
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{index}_{index}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        cap = build_media_caption(caption, tag, str(index), share_link, media_type)
        
        # Create 💗ADD button for each media
        video_id = f"{tag}_{index}"
        user_id_str = str(query.from_user.id)
        is_favorited = is_video_favorited(user_id_str, tag, index, file_id)
        
        if is_favorited:
            fav_button = InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_id}")
        else:
            fav_button = InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_id}")
        
        keyboard = InlineKeyboardMarkup([[fav_button]])
        
        try:
            if media_type == "video":
                await context.bot.send_video(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "photo":
                await context.bot.send_photo(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "document":
                await context.bot.send_document(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            sent_count += 1
            context.user_data['preview_index'] = idx + 1
            
            # 📊 Checkpoint: After every 100 search results
            if sent_count % 100 == 0 and idx + 1 < len(results):
                remaining = len(results) - (idx + 1)
                next_batch = min(50, remaining)
                
                # Update last checkpoint position
                context.user_data['preview_last_checkpoint'] = sent_count
                
                # Restore normal keyboard
                normal_keyboard = ReplyKeyboardMarkup([
                    ["🎲 Random", "🔥 Top Videos"],
                    ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
                ], resize_keyboard=True)
                
                await context.bot.send_message(
                    query.message.chat_id,
                    f"📊 Checkpoint reached!\n\n✅ Sent {sent_count} results so far\n⏳ {remaining} more remaining\n\nWant to continue?",
                    reply_markup=normal_keyboard
                )
                
                # Pause and show continue button
                checkpoint_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"▶️ Next {next_batch}", callback_data="continue_preview")],
                    [InlineKeyboardButton("🔙 Stop Preview", callback_data="search_page_0")]
                ])
                
                await context.bot.send_message(
                    query.message.chat_id,
                    "⏸️ Preview paused",
                    reply_markup=checkpoint_keyboard
                )
                
                # Stop the loop
                context.user_data['preview_index'] = idx + 1
                return
            
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Error sending preview media: {e}")
            continue
    
    # Clear cancel flag
    context.chat_data[cancel_key] = False
    
    # Check if stopped or completed
    if sent_count < len(results):
        # Stopped - show continue button
        continue_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Continue Sending", callback_data="continue_preview")]
        ])
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
        
        await context.bot.send_message(
            query.message.chat_id,
            f"⛔ Preview stopped. Sent {sent_count}/{len(results)} media file(s).",
            reply_markup=continue_keyboard
        )
        await context.bot.send_message(
            query.message.chat_id,
            "Menu restored:",
            reply_markup=normal_keyboard
        )
    else:
        # Completed
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
        
        await context.bot.send_message(
            query.message.chat_id,
            f"✅ Preview complete. Sent {sent_count}/{len(results)} media file(s).",
            reply_markup=normal_keyboard
        )


async def handle_continue_preview_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle continue sending for stopped preview"""
    query = update.callback_query
    await query.answer("▶️ Resuming preview...")
    
    results = context.user_data.get('preview_results', [])
    start_idx = context.user_data.get('preview_index', 0)
    total_sent_before = context.user_data.get('preview_last_checkpoint', 0)
    
    if not results or start_idx >= len(results):
        await query.message.reply_text("❌ No preview to resume.")
        return
    
    # Show stop button again
    stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True)
    await query.message.reply_text(
        f"▶️ Resuming from {start_idx + 1}/{len(results)}...",
        reply_markup=stop_keyboard
    )
    
    # Set up cancel key
    cancel_key = f"search_preview_cancel_{query.message.chat_id}"
    context.chat_data[cancel_key] = False
    
    sent_count = 0
    for idx in range(start_idx, len(results)):
        # Check for cancel
        if context.chat_data.get(cancel_key):
            context.user_data['preview_index'] = idx
            break
        
        result = results[idx]
        tag = result['tag']
        index = result['index']
        
        if tag not in media_data or index >= len(media_data[tag]):
            continue
        
        media_item = media_data[tag][index]
        if isinstance(media_item, dict) and media_item.get("deleted"):
            continue
        
        file_id = media_item.get("file_id")
        media_type = media_item.get("type", "video")
        caption = media_item.get("caption", "") or media_item.get("filename", "")
        
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{index}_{index}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        cap = build_media_caption(caption, tag, str(index), share_link, media_type)
        
        # Create 💗ADD button
        video_id = f"{tag}_{index}"
        user_id_str = str(query.from_user.id)
        is_favorited = is_video_favorited(user_id_str, tag, index, file_id)
        
        if is_favorited:
            fav_button = InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_id}")
        else:
            fav_button = InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_id}")
        
        keyboard = InlineKeyboardMarkup([[fav_button]])
        
        try:
            if media_type == "video":
                await context.bot.send_video(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "photo":
                await context.bot.send_photo(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "document":
                await context.bot.send_document(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            sent_count += 1
            context.user_data['preview_index'] = idx + 1
            
            # 📊 Checkpoint: After every 50 results in continue mode
            total_sent = total_sent_before + sent_count
            if sent_count % 50 == 0 and idx + 1 < len(results):
                remaining = len(results) - (idx + 1)
                next_batch = min(50, remaining)
                
                # Update checkpoint
                context.user_data['preview_last_checkpoint'] = total_sent
                
                # Restore normal keyboard
                normal_keyboard = ReplyKeyboardMarkup([
                    ["🎲 Random", "🔥 Top Videos"],
                    ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
                ], resize_keyboard=True)
                
                await context.bot.send_message(
                    query.message.chat_id,
                    f"📊 Checkpoint reached!\n\n✅ Sent {total_sent} results total ({sent_count} in this batch)\n⏳ {remaining} more remaining\n\nWant to continue?",
                    reply_markup=normal_keyboard
                )
                
                # Pause and show continue button
                checkpoint_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"▶️ Next {next_batch}", callback_data="continue_preview")],
                    [InlineKeyboardButton("🔙 Stop Preview", callback_data="search_page_0")]
                ])
                
                await context.bot.send_message(
                    query.message.chat_id,
                    "⏸️ Preview paused",
                    reply_markup=checkpoint_keyboard
                )
                
                # Stop the loop
                context.user_data['preview_index'] = idx + 1
                return
            
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Error sending preview media: {e}")
            continue
    
    # Clear cancel flag
    context.chat_data[cancel_key] = False
    
    # Check if stopped or completed
    total_sent = context.user_data.get('preview_index', 0)
    if total_sent < len(results):
        # Stopped again - show continue button
        continue_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Continue Sending", callback_data="continue_preview")]
        ])
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
        
        await context.bot.send_message(
            query.message.chat_id,
            f"⛔ Preview stopped. Sent {total_sent}/{len(results)} media file(s).",
            reply_markup=continue_keyboard
        )
        await context.bot.send_message(
            query.message.chat_id,
            "Menu restored:",
            reply_markup=normal_keyboard
        )
    else:
        # Completed
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
        
        await context.bot.send_message(
            query.message.chat_id,
            f"✅ Preview complete. Sent {total_sent}/{len(results)} media file(s).",
            reply_markup=normal_keyboard
        )


async def handle_random_from_tag_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle 'Watch from <tag>' button in search results"""
    query = update.callback_query
    await query.answer()
    
    tag = query.data.replace("random_from_tag_", "")
    
    # Use the existing random media function
    await send_random_video(context, query.message.chat_id, mode=tag, user_id=query.from_user.id)


async def handle_top_videos_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, start_index=0, range_limit=None):
    """Handle preview/sequential sending of top videos"""
    query = update.callback_query

    # Get sorted top videos
    if not favorites_data.get("video_likes"):
        await query.answer("❌ No top videos available", show_alert=True)
        return

    sorted_videos = get_sorted_canonical_likes()

    if not sorted_videos:
        await query.answer("❌ No top videos available", show_alert=True)
        return

    # Calculate end index based on range_limit
    if range_limit and range_limit > 0:
        end_index = min(start_index + range_limit, len(sorted_videos))
        videos_to_send = end_index - start_index
    else:
        end_index = len(sorted_videos)
        videos_to_send = len(sorted_videos) - start_index

    # Show stop button
    stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True)
    if range_limit:
        msg_text = f"▶️ Sending Top {videos_to_send} videos (#{start_index + 1} to #{end_index})..."
    else:
        msg_text = f"▶️ Sending {videos_to_send} top video(s)..."

    await query.message.reply_text(msg_text, reply_markup=stop_keyboard)

    # Set up cancel key
    cancel_key = f"top_videos_preview_cancel_{query.message.chat_id}"
    context.chat_data[cancel_key] = False

    # Store resume state - store FULL list, not just the range
    context.user_data['top_preview_videos'] = sorted_videos
    context.user_data['top_preview_index'] = start_index
    context.user_data['top_preview_end_index'] = end_index
    context.user_data['top_preview_range_limit'] = range_limit
    context.user_data['top_preview_last_checkpoint'] = 0  # Track last checkpoint position

    sent_count = 0
    for idx in range(start_index, end_index):
        # Check for cancel
        if context.chat_data.get(cancel_key):
            context.user_data['top_preview_index'] = idx
            break
    
        likes_key, _ = sorted_videos[idx]
        likes = get_likes_count(likes_key)
        metadata = get_video_metadata(likes_key)
        if not metadata:
            continue
        tag = metadata["tag"]
        media_idx = metadata["index"]
        video_id = metadata["video_id"]
    
        # Check if video exists and is valid
        if tag not in media_data or not isinstance(media_data[tag], list) or not (0 <= media_idx < len(media_data[tag])):
            continue
    
        media_item = media_data[tag][media_idx]
    
        # Skip deleted/revoked media
        if isinstance(media_item, dict) and (media_item.get("deleted") or media_item.get("revoked")):
            continue
    
        # Get media info
        if isinstance(media_item, dict):
            file_id = media_item.get("file_id")
            media_type = media_item.get("type", "video")
            caption = media_item.get("caption", "") or media_item.get("filename", "")
        else:
            file_id = media_item
            media_type = "video"
            caption = ""
    
        # Build caption with top video info
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{media_idx}_{media_idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
    
        rank = idx + 1
        if range_limit:
            cap_header = f"🔥 <b>Top {range_limit} | Video #{rank} | ❤️ {likes} likes</b>\n\n"
        else:
            cap_header = f"🔥 <b>Top Video #{rank} | ❤️ {likes} likes</b>\n\n"
    
        cap = build_media_caption(caption, tag, str(media_idx), share_link, media_type)
        cap = cap_header + cap
    
        # Create 💗ADD button
        user_id_str = str(query.from_user.id)
        is_favorited = is_video_favorited(user_id_str, tag, media_idx, file_id)
    
        if is_favorited:
            fav_button = InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_id}")
        else:
            fav_button = InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_id}")
    
        keyboard = InlineKeyboardMarkup([[fav_button]])
    
        try:
            if media_type == "video":
                await context.bot.send_video(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "photo":
                await context.bot.send_photo(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "document":
                await context.bot.send_document(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "animation":
                await context.bot.send_animation(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            sent_count += 1
            context.user_data['top_preview_index'] = idx + 1
            
            # 📊 Checkpoint: After every 100 media since last checkpoint
            last_checkpoint = context.user_data.get('top_preview_last_checkpoint', 0)
            if sent_count - last_checkpoint >= 100 and idx + 1 < end_index:
                remaining = end_index - (idx + 1)
                next_batch = min(50, remaining)
                
                # Update last checkpoint position
                context.user_data['top_preview_last_checkpoint'] = sent_count
                
                # Restore normal keyboard
                normal_keyboard = ReplyKeyboardMarkup([
                    ["🎲 Random", "🔥 Top Videos"],
                    ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
                ], resize_keyboard=True)
                
                await context.bot.send_message(
                    query.message.chat_id,
                    f"📊 Checkpoint reached!\n\n✅ Sent {sent_count} videos so far\n⏳ {remaining} more remaining\n\nWant to continue?",
                    reply_markup=normal_keyboard
                )
                
                # Pause and show continue button
                checkpoint_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"▶️ Next {next_batch}", callback_data=f"next_top_batch_{idx + 1}_50")],
                    [InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")]
                ])
                
                await context.bot.send_message(
                    query.message.chat_id,
                    "⏸️ Preview paused",
                    reply_markup=checkpoint_keyboard
                )
                
                # Stop the loop - user can click button to continue
                context.user_data['top_preview_index'] = idx + 1
                return
            
            await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"Error sending top video preview: {e}")
            continue

    # Clear cancel flag
    context.chat_data[cancel_key] = False

    # Check if stopped or completed within the requested range
    expected_count = end_index - start_index
    if sent_count < expected_count:
        # Stopped - show continue + back buttons
        continue_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Continue Sending", callback_data="continue_top_videos_preview")],
            [InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")]
        ])
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
    
        await context.bot.send_message(
            query.message.chat_id,
            f"⛔ Preview stopped. Sent {sent_count}/{expected_count} top video(s).",
            reply_markup=continue_keyboard
        )
        await context.bot.send_message(
            query.message.chat_id,
            "Menu restored:",
            reply_markup=normal_keyboard
        )
    else:
        # Completed this range
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
    
        await context.bot.send_message(
            query.message.chat_id,
            f"✅ Preview complete. Sent {sent_count} top video(s).",
            reply_markup=normal_keyboard
        )
        
        # Check if there are more videos to show in the FULL list
        current_end = context.user_data.get('top_preview_index', end_index)
        remaining = len(sorted_videos) - current_end
        
        # Create inline buttons for what's next
        inline_buttons = []
        if remaining > 0:
            # Offer to send next batch (up to 10)
            next_batch = min(10, remaining)
            inline_buttons.append([InlineKeyboardButton(f"▶️ Next {next_batch}", callback_data=f"next_top_batch_{current_end}_10")])
        
        inline_buttons.append([InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")])
        
        back_inline = InlineKeyboardMarkup(inline_buttons)
        await context.bot.send_message(
            query.message.chat_id,
            "What next?",
            reply_markup=back_inline
        )


async def handle_continue_top_videos_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle continue sending for stopped top videos preview"""
    query = update.callback_query
    await query.answer("▶️ Resuming preview...")

    sorted_videos = context.user_data.get('top_preview_videos', [])
    start_idx = context.user_data.get('top_preview_index', 0)
    end_idx = context.user_data.get('top_preview_end_index', len(context.user_data.get('top_preview_videos', [])))
    range_limit = context.user_data.get('top_preview_range_limit')

    if not sorted_videos or start_idx >= len(sorted_videos):
        # Debug info
        debug_msg = f"❌ No preview to resume.\n\nDebug Info:\n"
        debug_msg += f"• sorted_videos length: {len(sorted_videos)}\n"
        debug_msg += f"• start_idx: {start_idx}\n"
        debug_msg += f"• user_data keys: {list(context.user_data.keys())}\n"
        debug_msg += f"• chat_data keys: {list(context.chat_data.keys())}"
        await query.message.reply_text(debug_msg)
        return

    # Show stop button again
    stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True)
    await query.message.reply_text(
        f"▶️ Resuming from #{start_idx + 1}/{len(sorted_videos)}...",
        reply_markup=stop_keyboard
    )

    # Set up cancel key
    cancel_key = f"top_videos_preview_cancel_{query.message.chat_id}"
    context.chat_data[cancel_key] = False

    # Get last checkpoint to calculate TOTAL sent (not just this batch)
    total_sent_before = context.user_data.get('top_preview_last_checkpoint', 0)
    sent_count = 0
    # Use end_idx if it was set (for range-limited previews) or continue to end
    loop_end = min(end_idx, len(sorted_videos)) if end_idx else len(sorted_videos)
    for idx in range(start_idx, loop_end):
        # Check for cancel
        if context.chat_data.get(cancel_key):
            context.user_data['top_preview_index'] = idx
            break
    
        likes_key, likes = sorted_videos[idx]
        metadata = get_video_metadata(likes_key)
        if not metadata:
            continue
        tag = metadata["tag"]
        media_idx = metadata["index"]
        video_id = metadata["video_id"]
    
        # Check if video exists and is valid
        if tag not in media_data or not isinstance(media_data[tag], list) or not (0 <= media_idx < len(media_data[tag])):
            continue
    
        media_item = media_data[tag][media_idx]
    
        # Skip deleted/revoked media
        if isinstance(media_item, dict) and (media_item.get("deleted") or media_item.get("revoked")):
            continue
    
        # Get media info
        if isinstance(media_item, dict):
            file_id = media_item.get("file_id")
            media_type = media_item.get("type", "video")
            caption = media_item.get("caption", "") or media_item.get("filename", "")
        else:
            file_id = media_item
            media_type = "video"
            caption = ""
    
        # Build caption with top video info
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{media_idx}_{media_idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
    
        rank = idx + 1
        if range_limit:
            cap_header = f"🔥 <b>Top {range_limit} | Video #{rank} | ❤️ {likes} likes</b>\n\n"
        else:
            cap_header = f"🔥 <b>Top Video #{rank} | ❤️ {likes} likes</b>\n\n"
    
        cap = build_media_caption(caption, tag, str(media_idx), share_link, media_type)
        cap = cap_header + cap
    
        # Create 💗ADD button
        user_id_str = str(query.from_user.id)
        is_favorited = is_video_favorited(user_id_str, tag, media_idx, file_id)
    
        if is_favorited:
            fav_button = InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_id}")
        else:
            fav_button = InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_id}")
    
        keyboard = InlineKeyboardMarkup([[fav_button]])
    
        try:
            if media_type == "video":
                await context.bot.send_video(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "photo":
                await context.bot.send_photo(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "document":
                await context.bot.send_document(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "animation":
                await context.bot.send_animation(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            sent_count += 1
            context.user_data['top_preview_index'] = idx + 1
            
            # 📊 Checkpoint: After every 100 media since last checkpoint
            total_sent = total_sent_before + sent_count
            if sent_count >= 50 and sent_count % 50 == 0 and idx + 1 < loop_end:
                remaining = loop_end - (idx + 1)
                next_batch = min(50, remaining)
                
                # Update last checkpoint to current total
                context.user_data['top_preview_last_checkpoint'] = total_sent
                
                # Restore normal keyboard
                normal_keyboard = ReplyKeyboardMarkup([
                    ["🎲 Random", "🔥 Top Videos"],
                    ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
                ], resize_keyboard=True)
                
                await context.bot.send_message(
                    query.message.chat_id,
                    f"📊 Checkpoint reached!\n\n✅ Sent {total_sent} videos total ({sent_count} in this batch)\n⏳ {remaining} still remaining\n\nWant to continue?",
                    reply_markup=normal_keyboard
                )
                
                # Pause and show continue button
                checkpoint_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"▶️ Next {next_batch}", callback_data=f"next_top_batch_{idx + 1}_50")],
                    [InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")]
                ])
                
                await context.bot.send_message(
                    query.message.chat_id,
                    "⏸️ Preview paused",
                    reply_markup=checkpoint_keyboard
                )
                
                # Stop the loop
                context.user_data['top_preview_index'] = idx + 1
                return
            
            await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"Error sending top video preview: {e}")
            continue

    # Clear cancel flag
    context.chat_data[cancel_key] = False

    # Check if stopped or completed within the requested range
    total_sent = context.user_data.get('top_preview_index', 0)
    expected_end = loop_end
    if total_sent < expected_end:
        # Stopped again - show continue + back buttons
        continue_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Continue Sending", callback_data="continue_top_videos_preview")],
            [InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")]
        ])
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
    
        await context.bot.send_message(
            query.message.chat_id,
            f"⛔ Preview stopped. Sent {total_sent}/{len(sorted_videos)} top video(s).",
            reply_markup=continue_keyboard
        )
        await context.bot.send_message(
            query.message.chat_id,
            "Menu restored:",
            reply_markup=normal_keyboard
        )
    else:
        # Completed
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
    
        await context.bot.send_message(
            query.message.chat_id,
            f"✅ Preview complete. Sent {total_sent} top video(s).",
            reply_markup=normal_keyboard
        )
        
        # Check if there are more videos to show
        remaining = len(sorted_videos) - total_sent
        
        # Create inline buttons for what's next
        inline_buttons = []
        if remaining > 0:
            # Offer to send next 10
            next_batch = min(10, remaining)
            inline_buttons.append([InlineKeyboardButton(f"▶️ Next {next_batch}", callback_data=f"next_top_batch_{total_sent}_10")])
        
        inline_buttons.append([InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")])
        
        back_inline = InlineKeyboardMarkup(inline_buttons)
        await context.bot.send_message(
            query.message.chat_id,
            "What next?",
            reply_markup=back_inline
        )


async def handle_next_top_batch_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle 'Next <N>' button: send exactly N more top videos then pause again."""
    query = update.callback_query
    await query.answer("▶️ Sending batch...")

    # Parse callback data: next_top_batch_{start_index}_{count}
    data = query.data.replace("next_top_batch_", "")
    parts = data.split('_')
    try:
        start_idx = int(parts[0])
        batch_size = int(parts[1]) if len(parts) > 1 else 10
    except (ValueError, IndexError):
        await query.message.reply_text("❌ Error parsing batch request.")
        return

    # Re-offer stop button and cancel flag so user can stop the batch mid-send
    stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True)
    try:
        await query.message.reply_text(f"▶️ Sending Top {batch_size} videos...", reply_markup=stop_keyboard)
    except Exception:
        try:
            await context.bot.send_message(query.message.chat_id, f"▶️ Sending Top {batch_size} videos...", reply_markup=stop_keyboard)
        except Exception:
            pass

    cancel_key = f"top_videos_preview_cancel_{query.message.chat_id}"
    context.chat_data[cancel_key] = False

    sorted_videos = context.user_data.get('top_preview_videos', [])
    if not sorted_videos:
        await query.message.reply_text("❌ No video data found. Open Top Videos again.")
        return

    if start_idx >= len(sorted_videos):
        await query.message.reply_text("✅ All videos have been sent!")
        return

    end_idx = min(start_idx + batch_size, len(sorted_videos))
    total_sent_before = context.user_data.get('top_preview_last_checkpoint', 0)
    sent_in_batch = 0

    for idx in range(start_idx, end_idx):
        # Check for cancel
        if context.chat_data.get(cancel_key):
            # Save current position and break gracefully
            context.user_data['top_preview_index'] = idx
            break
        likes_key, likes = sorted_videos[idx]
        metadata = get_video_metadata(likes_key)
        if not metadata:
            continue
        tag = metadata["tag"]
        media_idx = metadata["index"]
        video_id = metadata["video_id"]

        # Validate media
        if tag not in media_data or not isinstance(media_data[tag], list) or not (0 <= media_idx < len(media_data[tag])):
            continue
        media_item = media_data[tag][media_idx]
        if isinstance(media_item, dict) and (media_item.get("deleted") or media_item.get("revoked")):
            continue

        # Extract media fields
        if isinstance(media_item, dict):
            file_id = media_item.get("file_id")
            media_type = media_item.get("type", "video")
            caption = media_item.get("caption", "") or media_item.get("filename", "")
        else:
            file_id = media_item
            media_type = "video"
            caption = ""

        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{media_idx}_{media_idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        rank = idx + 1
        range_limit = context.user_data.get('top_preview_range_limit')
        if range_limit:
            cap_header = f"🔥 <b>Top {range_limit} | Video #{rank} | ❤️ {likes} likes</b>\n\n"
        else:
            cap_header = f"🔥 <b>Top Video #{rank} | ❤️ {likes} likes</b>\n\n"
        cap = build_media_caption(caption, tag, str(media_idx), share_link, media_type)
        cap = cap_header + cap

        # Favorite button
        user_id_str = str(query.from_user.id)
        is_favorited = is_video_favorited(user_id_str, tag, media_idx, file_id)
        fav_button = InlineKeyboardButton(
            "💔 Remove" if is_favorited else "❤️ Add",
            callback_data=(f"remove_fav_{video_id}" if is_favorited else f"add_fav_{video_id}")
        )
        keyboard = InlineKeyboardMarkup([[fav_button]])

        try:
            if media_type == "video":
                await context.bot.send_video(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "photo":
                await context.bot.send_photo(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "document":
                await context.bot.send_document(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            elif media_type == "animation":
                await context.bot.send_animation(
                    query.message.chat_id,
                    file_id,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
                )
            sent_in_batch += 1
            context.user_data['top_preview_index'] = idx + 1
            await asyncio.sleep(0.4)
        except Exception as e:
            print(f"Error sending top video batch item: {e}")
            continue

    total_sent = total_sent_before + sent_in_batch
    remaining_total = len(sorted_videos) - end_idx

    # Clear cancel flag at end of prompt processing (clean up)
    try:
        context.chat_data[cancel_key] = False
    except Exception:
        pass

    # If finished all videos
    if end_idx >= len(sorted_videos) or remaining_total <= 0:
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
        await context.bot.send_message(
            query.message.chat_id,
            f"✅ All top videos sent. Total: {total_sent}.",
            reply_markup=normal_keyboard
        )
        context.user_data['top_preview_last_checkpoint'] = total_sent
        return

    # Prepare next checkpoint prompt (always batch_size=50 after first)
    next_batch = min(50, remaining_total)
    context.user_data['top_preview_last_checkpoint'] = total_sent

    normal_keyboard = ReplyKeyboardMarkup([
        ["🎲 Random", "🔥 Top Videos"],
        ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
    ], resize_keyboard=True)
    await context.bot.send_message(
        query.message.chat_id,
        f"📊 Checkpoint reached!\n\n✅ Sent {total_sent} top videos total ({sent_in_batch} just now)\n⏳ Remaining: {remaining_total}\n\nContinue?",
        reply_markup=normal_keyboard
    )
    checkpoint_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"▶️ Next {next_batch}", callback_data=f"next_top_batch_{end_idx}_{next_batch}")],
        [InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")]
    ])
    # If user canceled early, send the stopped UI similar to preview
    expected_count = end_idx - start_idx
    if sent_in_batch < expected_count:
        # Show stopped message with continue+back and restore home keyboard
        continue_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Continue Sending", callback_data="continue_top_videos_preview")],
            [InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")]
        ])
        normal_keyboard = ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
        await context.bot.send_message(
            query.message.chat_id,
            f"⛔ Preview stopped. Sent {sent_in_batch}/{expected_count} top video(s).",
            reply_markup=continue_keyboard
        )
        await context.bot.send_message(
            query.message.chat_id,
            "Menu restored:",
            reply_markup=normal_keyboard
        )
        return

    await context.bot.send_message(
        query.message.chat_id,
        "⏸️ Preview paused",
        reply_markup=checkpoint_keyboard
    )


async def handle_search_result_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle search result button clicks"""
    query = update.callback_query
    await query.answer()
    
    # Parse callback data: search_result_{tag}_{index}
    data = query.data.replace("search_result_", "")
    parts = data.split("_")
    
    if len(parts) < 2:
        await query.message.reply_text("❌ Invalid search result.")
        return
    
    # Reconstruct tag (may contain underscores)
    index = int(parts[-1])
    tag = "_".join(parts[:-1])
    
    # Get the media item
    if tag not in media_data or index >= len(media_data[tag]):
        await query.message.reply_text("❌ Media not found.")
        return
    
    media_item = media_data[tag][index]
    
    if isinstance(media_item, dict) and media_item.get("deleted"):
        await query.message.reply_text("❌ This media has been deleted.")
        return
    
    # Send the media
    file_id = media_item.get("file_id")
    media_type = media_item.get("type", "video")
    caption = media_item.get("caption", "") or media_item.get("filename", "")
    
    # Create unique shareable link for this specific file
    file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{index}_{index}"
    share_link = f"https://telegram.me/share/url?url={file_link}"
    
    # Build proper media caption with global caption and replacements
    cap = build_media_caption(caption, tag, str(index), share_link, media_type)
    
    # Create favorite button
    video_id = f"{tag}_{index}"
    user_id_str = str(update.effective_user.id)
    file_id = media_item.get('file_id') if isinstance(media_item, dict) else None
    is_favorited = is_video_favorited(user_id_str, tag, index, file_id)
    
    if is_favorited:
        fav_button = InlineKeyboardButton("💔 Remove from Favorites", 
                                        callback_data=f"remove_fav_{video_id}")
    else:
        fav_button = InlineKeyboardButton("❤️ ADD", 
                                        callback_data=f"add_fav_{video_id}")
    
    # Check if user is admin
    if is_admin(update.effective_user.id):
        who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_id}")
        rows = [[fav_button, who_liked_button]]
        rows += build_admin_control_row(video_id)
        keyboard = InlineKeyboardMarkup(rows)
    else:
        keyboard = InlineKeyboardMarkup([[fav_button]])
    
    try:
        if media_type == "video":
            await query.message.reply_video(
                file_id,
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(update.effective_user.id, query.message.chat_id)
            )
        elif media_type == "photo":
            await query.message.reply_photo(
                file_id,
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(update.effective_user.id, query.message.chat_id)
            )
        elif media_type == "document":
            await query.message.reply_document(
                file_id,
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(update.effective_user.id, query.message.chat_id)
            )
        elif media_type == "audio":
            await query.message.reply_audio(
                file_id,
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(update.effective_user.id, query.message.chat_id)
            )
        elif media_type == "animation":
            await query.message.reply_animation(
                file_id,
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(update.effective_user.id, query.message.chat_id)
            )
        else:
            await query.message.reply_text("❌ Unsupported media type.")
    except Exception as e:
        await query.message.reply_text(f"❌ Error sending media: {str(e)}")



async def view_user_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Show user's favorite videos using navigator"""
    await show_favorites_navigator(query, context, 0)


# (Deprecated duplicate help_command removed in favor of concise two-part help above)
async def handle_text_shortcuts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text shortcuts for quick access"""
    if not update.message or not update.message.text:
        return
    
    # Track user interaction
    user = update.effective_user
    track_user(user.id, user.username, user.first_name)

    # Admin Quick Push: handle awaited new tag name input
    if is_admin(update.effective_user.id):
        if context.user_data.get("qp_waiting_new_tag"):
            new_tag = update.message.text.strip().lower()
            pending = context.user_data.get("qp_pending_items", [])
            if not new_tag:
                await update.message.reply_text("❌ Tag name cannot be empty. Send a valid name or /cancel.")
                return
            # Create tag list if missing
            media_data.setdefault(new_tag, [])
            # Append items
            added = 0
            for item in pending:
                if isinstance(item, dict) and item.get("file_id") and item.get("type"):
                    media_data[new_tag].append({"file_id": item["file_id"], "type": item["type"], "caption": item.get("caption", "")})
                    added += 1
            save_media()
            context.user_data.pop("qp_waiting_new_tag", None)
            context.user_data.pop("qp_pending_items", None)
            await update.message.reply_text(f"✅ Created tag '<code>{new_tag}</code>' and added {added} item(s).", parse_mode=ParseMode.HTML)
            return

    # Fast-path: if we're expecting a rank reply and the user sent a numeric value, forward it to the handler
    user_data = context.user_data if hasattr(context, "user_data") else {}
    incoming_text = update.message.text.strip() if update.message and update.message.text else ""
    if user_data.get("expecting_rank_reply") and re.fullmatch(r"#?\d+", incoming_text):
        await handle_rank_number_reply(update, context)
        return
    
    # Handle Stop button for media sends
    if incoming_text == "🛑 Stop":
        # Set all cancel flags that match the pattern for this user/chat
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Set flags that might be active (we use wildcard matching via iteration)
        set_count = 0
        for key in list(context.chat_data.keys()):
            if 'cancel' in key:
                context.chat_data[key] = True
                set_count += 1
        
        # Also set the base patterns just in case (cover both user and chat level)
        base_keys = [
            f"view_cancel_{chat_id}", f"cview_cancel_{chat_id}", f"passlink_cancel_{chat_id}",
            f"view_cancel_{user_id}", f"cview_cancel_{user_id}", f"passlink_cancel_{user_id}"
        ]
        for key in base_keys:
            context.chat_data[key] = True
            set_count += 1
        
        # Show specialized post-stop keyboard (Random Safe / Off Limits / Favorites)
        stop_keyboard = ReplyKeyboardMarkup([
            ["🎬 Random (18-)", "🏴‍☠️ Off Limits"],
            ["⭐ My Favorites"]
        ], resize_keyboard=True)

        await update.message.reply_text(
            "⛔ Stopping media send operation...",
            reply_markup=stop_keyboard
        )
        return
    
    # ---- Reply Keyboard Button Labels (centralized) ----
    RANDOM_SAFE_LABEL = "🎬 Random (18-)"      # mapped to get_random_from_tag_no_repeat("random")
    RANDOM_PASSED_LABEL = "🎲 Random (18+)"    # mapped to get_random_from_passed_links()
    FAV_LABEL = "⭐ My Favorites"
    HELP_LABEL = "❓ Help"
    HOME_LABEL = "🏠 Home"

    # Helper to show admin quick access keyboard
    def admin_home_keyboard():
        return ReplyKeyboardMarkup([
            ["📊 Stats", "📢 Broadcast", "👥 Users"],
            ["🎬 Media", "💾 Backup", "🛡️ Protection"],
            ["🧹 Cleanup", "📝 Caption", "🔧 Tools"],
            ["🔁 Auto Reindex", "🔧 Reindex", "🧪 Test"],
            ["🏠 HOME"]
        ], resize_keyboard=True)
    
    # Helper to show user quick access keyboard  
    def user_home_keyboard():
        return ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)

    # Helper to show minimal Home keyboard (fallback)
    def minimal_home_keyboard():
        return ReplyKeyboardMarkup([[HOME_LABEL]], resize_keyboard=True)
    
    # Helper to get appropriate keyboard based on user type
    def get_home_keyboard(user_id):
        # Always return the condensed user keyboard; admin panel only via /help or HELP inline button
        return user_home_keyboard()

    # Obtain raw text early so it's available for Home check
    raw_text = update.message.text.strip()

    # If user presses Home, show appropriate quick access menu
    if raw_text == HOME_LABEL:
        user_id = update.effective_user.id
        if is_admin(user_id):
            await update.message.reply_text(
                "🏠 **Admin Quick Access**\n\n"
                "Choose from commonly used admin functions:",
                reply_markup=admin_home_keyboard(),
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "🏠 **Quick Access Menu**\n\n"
                "Here are your available options:",
                reply_markup=user_home_keyboard(), 
                parse_mode='Markdown'
            )
        return

    # Handle "🏠 HOME" button - redirect to /start
    if raw_text == "🏠 HOME":
        await start(update, context)
        return

    # Handle User Quick Access buttons  
    if raw_text == "🎲 Random":
        # Show random category selection
        keyboard = [
            [InlineKeyboardButton("🎬 Random (18-)", callback_data="random_safe")],
            [InlineKeyboardButton("🎲 Random (18+)", callback_data="random_media")],
            [InlineKeyboardButton("🏠 Home", callback_data="return_home")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🎲 **Choose Random Category:**\n\n"
            "Select which type of random video you want:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🔥 Top Videos":
        # Call the top videos function for users
        await top_videos_command(update, context)
        return
        
    elif raw_text == "𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️":
        # Direct redirect to the link
        keyboard = [[InlineKeyboardButton("🌐 Visit Channel", url="https://t.me/beinghumanassociation")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🌐 **Visit Our Community**\n\n"
            "Click below to join our community channel:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Handle Admin Quick Access buttons
    if raw_text == "📊 Stats":
        await update.message.reply_text(
            "📊 **Quick Stats Commands:**\n\n"
            "• `/userstats` - User registration statistics\n"
            "• `/userinfo <user_id>` - Get detailed user information\n"
            "• `/topusers` - Most active users ranking\n"
            "• `/videostats <tag> <index>` - Check who liked a video\n"
            "• `/topvideos` - Most liked videos with navigation\n"
            "• `/bstats` - Broadcasting statistics\n"
            "• `/deletedstats` - Deleted media statistics",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "📢 Broadcast":
        await update.message.reply_text(
            "📢 **Broadcasting Options:**\n\n"
            "• `/broadcast <message>` - Normal broadcast\n"
            "• `/dbroadcast <message>` - Auto-delete broadcast\n"
            "• `/pbroadcast <message>` - Pin broadcast\n" 
            "• `/sbroadcast <message>` - Silent broadcast\n"
            "• `/fbroadcast <message>` - Forward mode broadcast",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return

    elif raw_text == "📝 Caption":
        # Show caption management quick guide
        await update.message.reply_text(
            "📝 Caption Management:\n"
            "/set_global_caption <text> — Set global caption for all files\n"
            "/add_replacement <find> | <replace> — Add caption replacement rule\n"
            "/list_replacements — Show all replacement rules\n"
            "/remove_replacement <index> — Remove replacement rule\n"
            "/caption_config — Show current caption configuration\n\n"
            "🔗 Link Override:\n"
            "/set_link <url> — Force all links to this URL\n"
            "/link_off — Disable link override\n"
            "/link_status — Show override status",
            reply_markup=admin_home_keyboard()
        )
        return
        
    elif raw_text == "👥 Users":
        await update.message.reply_text(
            "👥 **User Management:**\n\n"
            "• `/discover` - Discover users from all sources\n"
            "• `/addusers <id1> <id2>` - Add users to database\n"
            "• `/topusers` - Most active users ranking\n"
            "• `/userinfo <user_id>` - Get user information\n"
            "• `/userfavorites <user_id>` - Check user's favorites\n"
            "• `/add_admin <user_id>` - Add admin privileges\n"
            "• `/remove_admin <user_id>` - Remove admin privileges\n"
            "• `/list_admins` - List all administrators",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'  
        )
        return
        
    elif raw_text == "🧹 Cleanup":
        await update.message.reply_text(
            "🧹 **Cleanup & Media Management:**\n\n"
            "• `/listdeleted` - Show all deleted media\n"
            "• `/listrevoked` - Show all revoked media\n"
            "• `/listremoved` - Show all removed media\n"
            "• `/cleanupdeleted` - Clean up corrupted deleted media\n"
            "• `/restoredeleted <tag> <index>` - Restore deleted media\n"
            "• `/cleardeleted` - Permanently remove all deleted media\n"
            "• `/restoreall` - Restore all deleted media\n"
            "• `/restoremedia <tag> <index>` - Restore specific media\n"
            "• `/autodelete` - Auto-deletion controls\n"
            "• `/notifications on/off` - Control deletion notifications",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🔧 Tools":
        await update.message.reply_text(
            "🔧 **Admin Tools:**\n\n"
            "• `/list_admins` - List all administrators\n"
            "• `/add_admin <user_id>` - Add admin privileges\n"
            "• `/remove_admin <user_id>` - Remove admin privileges\n"
            "• `/checkupdates` - Check for pending requests\n"
            "• `/reindexfavs` - Reindex favorites and normalize like counts (admin)\n"
            "• `/autoreindex on/off/status` - Toggle auto reindexing and set interval (minutes)\n"
            "• `/autodelete on/off/status` - Auto-deletion controls\n"
            "• `/autodelete hours <hours>` - Set deletion time\n"
            "• `/autodelete clear` - Clear tracking list\n"
            "• `/autodelete stats` - View deletion statistics",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return

    elif raw_text == "🔁 Auto Reindex":
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required.")
            return
        rc = favorites_data.setdefault("reindex_config", {"auto_reindex_enabled": False, "interval_seconds": 3600})
        status = "ENABLED" if rc.get("auto_reindex_enabled") else "DISABLED"
        interval = rc.get("interval_seconds", 3600)
        await update.message.reply_text(
            f"🔁 Auto Reindex is currently: {status}\nInterval: {interval} seconds.\n\nUse: /autoreindex on [minutes] | /autoreindex off",
            reply_markup=admin_home_keyboard()
        )
        return

    elif raw_text == "🔧 Reindex":
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required.")
            return
        await update.message.reply_text(
            "⚠️ Reindex will rebuild metadata and normalize counts. To proceed, run: /reindexfavs confirm\n(Or use /reindex confirm)",
            reply_markup=admin_home_keyboard()
        )
        return
        
    elif raw_text == "🎬 Media":
        await update.message.reply_text(
            "🎬 <b>Media &amp; Content Management:</b>\n\n"
            "• <code>/upload</code> — Reply to media to add to database\n"
            "• <code>/listvideos</code> — List all tags\n"
            "• <code>/listvideo &lt;tag&gt;</code> — List media in a tag\n"
            "• <code>/cview &lt;tag&gt; [start] [end]</code> — Clean view: only visible media (skip deleted/revoked)\n"
            "• <code>/view &lt;tag&gt; &lt;index&gt;</code> — View specific media\n"
            "• <code>/remove &lt;tag&gt; &lt;index&gt;</code> — Remove specific media\n"
            "• <code>/off_limits &lt;tag&gt; [start] [end]</code> — Add items to Off Limits category\n"
            "• <code>/free &lt;tag&gt;</code> / <code>/unfree &lt;tag&gt;</code> — Toggle free access\n"
            "• <code>/listfree</code> — Show all free tags\n\n"
            "🔗 <b>Link Systems</b> (separate stores):\n"
            "• <code>/pass &lt;tag&gt; [start] [end]</code> — RANDOM MEDIA access only\n"
            "• <code>/revoke &lt;tag&gt; [start] [end]</code> — Remove from RANDOM MEDIA\n"
            "• <code>/passlink &lt;tag&gt; [start] [end]</code> — Shareable link only\n"
            "• <code>/revokelink &lt;tag&gt; [start] [end]</code> — Disable shareable link\n"
            "• <code>/activelinks</code> / <code>/passlinks</code> / <code>/listactive</code> — View active links\n\n"
            "🔁 <b>PUSH Status</b>:\n"
            "• <code>/pushst</code> — View recent PUSH add/remove/move with Undo/Undo Move (index-stable)\n\n"
            "📈 <b>Media Analytics</b>:\n"
            "• <code>/topvideos</code> — Top rated videos\n"
            "• <code>/videostats &lt;tag&gt; &lt;index&gt;</code> — Who liked this video\n\n"
            "📦 <b>Batch Tools</b>:\n"
            "• <code>/custom_batch &lt;tag&gt;</code> — Start manual batch\n"
            "• <code>/stop_batch</code> / <code>/cancel_batch</code> / <code>/batch_status</code> — Manage batch\n"
            "• <code>/move &lt;src&gt; &lt;start&gt; &lt;end&gt; &lt;dest&gt;</code> — Batch move media range\n"
            "• <code>/add &lt;src&gt; &lt;start&gt; &lt;end&gt; &lt;dest&gt;</code> — Batch copy media range\n\n"
            "🤖 <b>AI Batch (on-demand)</b>:\n"
            "• <code>/ai_batch</code> — Start AI tagging session\n"
            "• <code>/stop_ai_batch</code> / <code>/cancel_ai_batch</code> / <code>/ai_batch_status</code>\n\n"
            "📝 Other tools: <code>/set_global_caption</code>, <code>/getfileid</code>, <code>/setwelcomeimage</code>, <code>/testwelcomeimage</code>",
            reply_markup=admin_home_keyboard(),
            parse_mode=ParseMode.HTML
        )
        return
        
    elif raw_text == "💾 Backup":
        await update.message.reply_text(
            "💾 **Backup & Restore:**\n\n"
            "**Local Filesystem Backups:**\n"
            "• `/togglebackup on/off/status` - Toggle local backups\n"
            "• `/backup` - Create new backup\n"
            "• `/listbackups` - List all backups\n"
            "• `/restore <backup_name>` - Restore from backup\n"
            "• `/backupstats <backup_name>` - Show backup details\n"
            "• `/deletebackup <backup_name>` - Delete backup\n\n"
            "**Telegram Backups (Always Active):**\n"
            "• `/telegrambackup` - Send files to Telegram\n"
            "• `/autobackup on/off/now/status` - Auto-backup controls",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🛡️ Protection":
        await update.message.reply_text(
            "🛡️ **Protection Commands:**\n\n"
            "• `/protection` - Check protection status\n"
            "• `/pon` - Protection ON (shortcut)\n"
            "• `/poff` - Protection OFF (shortcut)\n"
            "• `/pstatus` - Protection status (shortcut)\n"
            "• `/ps` - Protection status (shortcut)\n"
            "• `/testprotection` - Test media protection\n"
            "• `/checkprotection` - Check protection details",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🧪 Test":
        await update.message.reply_text(
            "🧪 **Testing & Debug:**\n\n"
            "• `/testprotection` - Test media protection\n"
            "• `/testdeletion` - Test deletion functionality\n"
            "• `/debugdeleted <tag>` - Debug deleted media issues\n"
            "• `/checkupdates` - Check for pending old requests",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return

    # ========== Handle AI Batch Keyboard Buttons ==========
    elif raw_text == "🛑 Stop AI Batch":
        await stop_ai_batch_command(update, context)
        return
    
    elif raw_text == "❌ Cancel AI Batch":
        await cancel_ai_batch_command(update, context)
        return
    # ========== END AI Batch Buttons ==========

    # Handle User Quick Access buttons  
    elif raw_text == "🎲 Random":
        # Show random category selection
        keyboard = [
            [InlineKeyboardButton("🎬 Random (18-)", callback_data="random_safe")],
            [InlineKeyboardButton("🎲 Random (18+)", callback_data="random_media")],
            [InlineKeyboardButton("🏠 Home", callback_data="return_home")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🎲 **Choose Random Category:**\n\n"
            "Select which type of random video you want:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🔥 Top Videos":
        # Call the top videos function for users
        await top_videos_command(update, context)
        return
        
    elif raw_text == "𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️":
        # Create a special keyboard with a button that looks like a message but is actually a link
        # When clicked, this will open the channel directly without sending any bot message
        keyboard = [[InlineKeyboardButton("🔗Being Human Association", url="https://t.me/beinghumanassociation")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Delete the user's message to make it look like a redirect
        try:
            await update.message.delete()
        except:
            pass  # Ignore errors if we can't delete the message
            
        # Send a self-destructing message with the button (will be deleted after 1 second)
        message = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Redirecting to channel...",
            reply_markup=reply_markup
        )
        
        # Schedule message deletion after 1 second
        asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, message.message_id, 1))
        return

    # Main menu reply keyboard buttons - handle ALL variations
    # Random (18-) button - 📹 or 🎬 emoji
    if raw_text in {RANDOM_SAFE_LABEL, "🎬 Download Video", "🎬 GET FILES", "GET FILES", 
                     "📹 Random (18-)", "🎬 Random (18-)", "Random (18-)"}:
        await send_random_video(context, update.effective_chat.id, mode="safe")
        await update.message.reply_text("🔁 Menu minimized.", reply_markup=get_home_keyboard(update.effective_user.id))
        return

    # Off Limits button - 🏴‍☠️ emoji
    if raw_text in {"🏴‍☠️ Off Limits", "Off Limits", "offlimits"}:
        await show_off_limits_content(update, context)
        await update.message.reply_text("🔁 Menu minimized.", reply_markup=get_home_keyboard(update.effective_user.id))
        return

    # My Favorites button - ⭐ emoji
    if raw_text in {FAV_LABEL, "⭐ My Favorites", "My Favorites", "⭐My Favorites"}:
        await favorites_command(update, context)
        await update.message.reply_text("📂 Favorites opened.", reply_markup=get_home_keyboard(update.effective_user.id))
        return

    if raw_text == HELP_LABEL:
        # Show help without altering the current keyboard; admin panel appears only within help
        await help_command(update, context)
        return
    
    # Convert to lowercase for other shortcuts
    text = incoming_text.lower()
    
    # Batch management keyboard buttons (lowercase check)
    if text in ["🛑 stop batch", "/stop_batch"]:
        from telegram import ReplyKeyboardRemove
        await stop_batch_command(update, context)
        await update.message.reply_text("Batch stopped.", reply_markup=ReplyKeyboardRemove())
        return

    if text in ["❌ cancel batch", "/cancel_batch"]:
        from telegram import ReplyKeyboardRemove
        await cancel_batch_command(update, context)
        await update.message.reply_text("Batch cancelled.", reply_markup=ReplyKeyboardRemove())
        return

    # Other text shortcuts
    if text in ["fav", "favorites", "❤️", "♥️"]:
        await favorites_command(update, context)
        return

    if text in ["random", "rand", "🎲", "🎬", "media"]:
        # Send a random video and exit; do NOT show active links list on text triggers (even for admins)
        await send_random_video(context, update.effective_chat.id, "adult", update.effective_user.id)
        return

    # FIRST: Try searching media by caption or filename using fuzzy matching
    # This takes priority over direct tag matching
    query = text
    if len(query) >= 3 and not query.startswith('/'):
        # Call the fuzzy search function
        await search_media(update, context)
        # If search_media found results, it will handle the response and return
        # If no results, it returns None and we continue to tag matching below
        # Check if results were found and stored
        if context.user_data.get('search_results'):
            return
    
    # SECOND: Smart keyword-to-tag matching - only if no search results found
    matched_tag = find_tag_by_keyword(text)
    if matched_tag:
        # User typed a keyword that matches a tag - send a random video from that tag
        # Use silent_fail=True so no error message if tag has no videos
        try:
            await send_random_video(context, update.effective_chat.id, mode=matched_tag, user_id=update.effective_user.id, silent_fail=True)
            return
        except Exception as e:
            print(f"Error sending keyword-matched video for '{text}' -> tag '{matched_tag}': {e}")
            # Silently continue if error occurs


async def listactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    # Collect all active links from both sources
    all_active_links = set()
    
    # Add links from active_links.json (from /passlink command)
    for link_key in active_links.keys():
        all_active_links.add(link_key)
    
    # Add tags that have videos in passed_links.json (from /pass command)
    tags_in_passed = set()
    for video_key in passed_links:
        if '_' in video_key:
            # Extract tag name from video_key like "rd2_0" -> "rd2"
            parts = video_key.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                tag = '_'.join(parts[:-1])
                tags_in_passed.add(tag)
    
    # Add tags from passed_links to all_active_links
    for tag in tags_in_passed:
        all_active_links.add(tag)

    if not all_active_links:
        await update.message.reply_text("📂 No active links.")
        return

    msg = "<b>🔗 Active Links:</b>\n"
    for link_key in sorted(all_active_links):
        link = f"https://t.me/{BOT_USERNAME}?start={link_key}"
        if '_' in link_key and link_key.count('_') >= 2:
            # Check if it's a range format
            parts = link_key.split('_')
            if len(parts) >= 3 and parts[-2].isdigit() and parts[-1].isdigit():
                tag = '_'.join(parts[:-2])
                start_idx = parts[-2]
                end_idx = parts[-1]
                source = "🔗" if link_key in active_links else "🎲"
                # Show video count for passlink created links
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{tag}</code> ({start_idx}-{end_idx}){video_count} - {link}\n"
            else:
                # Not a range, just display as is
                source = "🔗" if link_key in active_links else "🎲"
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
        else:
            # Simple tag format
            source = "🔗" if link_key in active_links else "🎲"
            video_count = ""
            if link_key in active_links and isinstance(active_links[link_key], dict):
                videos = active_links[link_key].get("videos", [])
                video_count = f" ({len(videos)} videos)"
            msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
    
    msg += "\n🔗 = Created with /passlink (independent storage)\n🎲 = Created with /pass (RANDOM MEDIA access)"
    await update.message.reply_html(msg)




async def view_user_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Show user's favorite videos using navigator"""
    await show_favorites_navigator(query, context, 0)


async def user_favorites_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to check user's favorites"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /userfavorites <user_id>")
        return
    
    try:
        target_user_id = context.args[0].strip()
        
        user_fav_map = favorites_data.get("user_favorites", {})
        user_favs = user_fav_map.get(target_user_id)
        if not user_favs:
            await update.message.reply_text(f"❌ User {target_user_id} has no favorites.")
            return

        if isinstance(user_favs, dict):
            entries = list(user_favs.items())
        else:
            entries = [(video_id, {"video_id": video_id}) for video_id in user_favs if isinstance(video_id, str)]

        msg = f"<b>👤 User {target_user_id} Favorites ({len(entries)} videos):</b>\n\n"
        keyboard_buttons = []
        for i, (fav_key, fav_data) in enumerate(entries, 1):
            tag = fav_data.get("tag")
            idx = fav_data.get("index")
            video_id = fav_data.get("video_id") or fav_key
            if tag is None or idx is None:
                if "_" in video_id:
                    try:
                        tag, idx_str = video_id.split('_', 1)
                        idx = int(idx_str)
                    except Exception:
                        idx = None
            display_line = f"{i}. "
            if tag and idx is not None:
                display_line += f"Tag: <code>{tag}</code> | Index: <code>{idx}</code>"
            else:
                display_line += f"Key: <code>{video_id}</code>"
            msg += display_line + "\n"
            button_text = f"🎬 View #{i}: {video_id}"
            callback_data = f"view_video_{video_id}"
            keyboard_buttons.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

        if keyboard_buttons:
            reply_markup = InlineKeyboardMarkup(keyboard_buttons)
            await update.message.reply_html(msg, reply_markup=reply_markup)
        else:
            await update.message.reply_html(msg)

    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please provide a numeric user ID.")


async def video_stats_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to check how many users liked a specific video"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /videostats <tag> <index>")
        return
    
    tag = context.args[0].strip().lower()
    try:
        idx = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid index. Please provide a number.")
        return
    
    video_id = f"{tag}_{idx}"
    metadata = get_video_metadata(video_id)
    likes_count = get_likes_count(metadata.get("file_id") or video_id)
    
    if tag not in media_data or idx >= len(media_data[tag]):
        await update.message.reply_text("❌ Video not found.")
        return
    
    # Find users who liked this video
    users_who_liked = []
    for user_id in list(favorites_data.get("user_favorites", {}).keys()):
        user_favs = ensure_user_favorites_dict(user_id)
        for fav_key, fav_data in user_favs.items():
            if not isinstance(fav_data, dict):
                continue
            match = False
            if fav_data.get("tag") == tag and fav_data.get("index") == idx:
                match = True
            elif metadata and metadata.get("file_id") and fav_key == metadata.get("file_id"):
                match = True
            elif fav_data.get("file_id") and metadata and fav_data.get("file_id") == metadata.get("file_id"):
                match = True
            elif fav_data.get("video_id") == video_id:
                match = True
            if match:
                users_who_liked.append(user_id)
                break
    
    msg = f"<b>📊 Video Statistics</b>\n"
    msg += f"Tag: <code>{tag}</code>\n"
    msg += f"Index: <code>{idx}</code>\n"
    msg += f"❤️ Total Likes: <code>{likes_count}</code>\n\n"
    
    if users_who_liked:
        msg += f"<b>👥 Users who liked this video:</b>\n"
        for user_id in users_who_liked:
            msg += f"• <code>{user_id}</code>\n"
    else:
        msg += "No users have liked this video yet."
    
    # Add direct view button
    view_button = InlineKeyboardButton(f"🎬 View Video: {tag}_{idx}", 
                                     callback_data=f"view_video_{video_id}")
    keyboard = InlineKeyboardMarkup([[view_button]])
    
    await update.message.reply_html(msg, reply_markup=keyboard)


async def top_videos_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command to see most liked videos with navigation - available for all users"""
    if not favorites_data["video_likes"]:
        await update.message.reply_text("📊 No top videos available yet.", reply_markup=get_home_keyboard(update.effective_user.id))
        return
    
    # Show first top video directly
    await show_top_videos_viewer(update, context, 0)


async def top_videos_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command for users to see most liked videos with navigation"""
    try:
        if not favorites_data["video_likes"]:
            await update.message.reply_text("📊 No top videos available yet.", reply_markup=get_home_keyboard(update.effective_user.id))
            return
        
        # Show the top videos navigator menu instead of directly showing videos
        await show_top_videos_navigator(update, context)
    except Exception as e:
        print(f"❌ Error in top_videos_command: {str(e)}")
        await update.message.reply_text(f"❌ An error occurred: {str(e)}")
    
# This function was intentionally removed to prevent duplication.
# The other implementation at line ~8142 is being used instead.
        
async def show_top_videos_range(query, context, limit=10):
    """Show top videos with a specific limit (10, 25, 50)"""
    # Sort videos by likes count
    sorted_videos = get_sorted_canonical_likes()
    
    total_videos = len(sorted_videos)
    
    # Limit to the requested number
    limit = min(limit, total_videos)
    top_videos = sorted_videos[:limit]
    
    # Build message
    msg = f"<b>� Top {limit} Videos</b>\n\n"
    
    for i, (likes_key, likes) in enumerate(top_videos, 1):
        likes = get_likes_count(likes_key)
        metadata = get_video_metadata(likes_key)
        if metadata:
            tag = metadata["tag"]
            idx = metadata["index"]
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            msg += f"{i}. Tag: <code>{tag}</code> | Index: <code>{idx}</code> | ❤️ {likes} likes | <a href='{file_link}'>🔗 Link</a>\n"
        else:
            msg += f"{i}. Unknown video key: <code>{likes_key}</code> | ❤️ {likes} likes\n"
    
    # Create navigation keyboard with range buttons
    keyboard = []
    
    # Add buttons to view videos from this range
    buttons_row = []
    for i in range(0, min(5, limit)):
        rank = i + 1
        buttons_row.append(InlineKeyboardButton(f"#{rank}", callback_data=f"topvideo_view_{i}"))
    if buttons_row:
        keyboard.append(buttons_row)
    
    # Add second row of buttons if needed
    if limit > 5:
        buttons_row = []
        for i in range(5, min(10, limit)):
            rank = i + 1
            buttons_row.append(InlineKeyboardButton(f"#{rank}", callback_data=f"view_top_video_{i}"))
        if buttons_row:
            keyboard.append(buttons_row)
    
    # Add menu navigation
    keyboard.append([
        InlineKeyboardButton("🎯 Jump to Specific Rank", callback_data="jump_specific_rank")
    ])
    keyboard.append([
        InlineKeyboardButton("🔢 Top Videos Menu", callback_data="top_videos_navigator")
    ])
    keyboard.append([
        InlineKeyboardButton("🏠 Home", callback_data="return_home")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Edit the message with the top videos list
    await query.edit_message_text(msg, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    
async def show_jump_rank_interface_with_range(query, context, start_rank=1):
    """Show the interface for jumping to a specific rank with a particular range"""
    # Get total number of videos with likes
    sorted_videos = get_sorted_canonical_likes()
    total_videos = len(sorted_videos)
    
    # Create the message
    msg = f"🎯 <b>JUMP TO SPECIFIC RANK</b>\n\nSelect a video rank to view or choose a range:\n\nUse 'Enter Specific Rank' to jump to any position"
    
    # Create keyboard with numbered buttons based on start_rank
    keyboard = []
    
    # Calculate the end rank (max 25 buttons)
    end_rank = min(start_rank + 24, total_videos)
    
    # Create rows of 5 buttons each
    for i in range(0, 5):  # 5 rows
        row = []
        for j in range(1, 6):  # 5 buttons per row
            rank = start_rank + (i * 5) + (j - 1)
            if rank <= end_rank:
                # Zero-based index for the callback
                index = rank - 1
                row.append(InlineKeyboardButton(f"#{rank}", callback_data=f"view_top_video_{index}"))
        if row:  # Only add non-empty rows
            keyboard.append(row)
    
    # Range buttons - these show different index ranges
    range_row = []
    
    # Add range buttons based on current range
    if start_rank == 1:  # Current view is 1-25
        range_row.append(InlineKeyboardButton("1-25", callback_data="show_rank_range_1"))
        range_row.append(InlineKeyboardButton("26-50", callback_data="show_rank_range_26"))
        if total_videos > 50:
            range_row.append(InlineKeyboardButton("51-75", callback_data="show_rank_range_51"))
    elif start_rank == 26:  # Current view is 26-50
        range_row.append(InlineKeyboardButton("1-25", callback_data="show_rank_range_1"))
        range_row.append(InlineKeyboardButton("26-50", callback_data="show_rank_range_26"))
        if total_videos > 50:
            range_row.append(InlineKeyboardButton("51-75", callback_data="show_rank_range_51"))
        if total_videos > 75:
            range_row.append(InlineKeyboardButton("76-100", callback_data="show_rank_range_76"))
    elif start_rank >= 51:  # Current view is 51+ (handle dynamically)
        # Always show previous range
        prev_start = max(1, start_rank - 25)
        prev_end = prev_start + 24
        range_row.append(InlineKeyboardButton(f"{prev_start}-{prev_end}", callback_data=f"show_rank_range_{prev_start}"))
        
        # Current range
        curr_end = min(start_rank + 24, total_videos)
        range_row.append(InlineKeyboardButton(f"{start_rank}-{curr_end}", callback_data=f"show_rank_range_{start_rank}"))
        
        # Next range if available
        next_start = start_rank + 25
        if next_start <= total_videos:
            next_end = min(next_start + 24, total_videos)
            range_row.append(InlineKeyboardButton(f"{next_start}-{next_end}", callback_data=f"show_rank_range_{next_start}"))
    
    if range_row:
        keyboard.append(range_row)
    
    # Enter specific rank button
    keyboard.append([InlineKeyboardButton("⌨️ Enter Specific Rank", callback_data="enter_specific_rank")])
    
    # Back button
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="top_videos_navigator")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Try to edit the message if it has text, otherwise send a new message
    try:
        # Check if the message has text to edit
        if query.message.text:
            await query.edit_message_text(msg, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            # Message has no text (likely a media message), send new message instead
            await query.message.reply_html(msg, reply_markup=reply_markup)
    except Exception as e:
        logging.error(f"Error in show_jump_rank_interface_with_range: {str(e)}")
        # Try to send as new message if editing fails
        try:
            await query.message.reply_html(msg, reply_markup=reply_markup)
        except Exception as nested_error:
            logging.error(f"Failed to send fallback message: {str(nested_error)}")

async def show_jump_rank_interface(query, context):
    """Show the interface for jumping to a specific rank in top videos"""
    # Default start_rank is 1 (showing ranks 1-25)
    start_rank = 1
    
    # Check if we have a stored rank range in context
    if hasattr(context, 'user_data') and 'current_rank_range' in context.user_data:
        start_rank = context.user_data['current_rank_range']
    
    # Use the range-based implementation
    await show_jump_rank_interface_with_range(query, context, start_rank)

async def show_jump_rank_interface_with_range(query, context, start_rank=1):
    """Show the interface for jumping to a specific rank with a particular range"""
    # Get total number of videos with likes
    sorted_videos = get_sorted_canonical_likes()
    total_videos = len(sorted_videos)
    
    # Create the message
    msg = f"🎯 <b>JUMP TO SPECIFIC RANK</b>\n\nSelect a video rank to view or choose a range:\n\nUse 'Enter Specific Rank' to jump to any position"
    
    # Create keyboard with numbered buttons based on start_rank
    keyboard = []
    
    # Calculate the end rank (max 25 buttons)
    end_rank = min(start_rank + 24, total_videos)
    
    # Create rows of 5 buttons each
    for i in range(0, 5):  # 5 rows
        row = []
        for j in range(1, 6):  # 5 buttons per row
            rank = start_rank + (i * 5) + (j - 1)
            if rank <= end_rank:
                # Zero-based index for the callback
                index = rank - 1
                row.append(InlineKeyboardButton(f"#{rank}", callback_data=f"view_top_video_{index}"))
        if row:  # Only add non-empty rows
            keyboard.append(row)
    
    # Range buttons - these show different index ranges
    range_row = []
    
    # Add range buttons based on current range
    if start_rank == 1:  # Current view is 1-25
        range_row.append(InlineKeyboardButton("1-25", callback_data="show_rank_range_1"))
        range_row.append(InlineKeyboardButton("26-50", callback_data="show_rank_range_26"))
        if total_videos > 50:
            range_row.append(InlineKeyboardButton("51-75", callback_data="show_rank_range_51"))
    elif start_rank == 26:  # Current view is 26-50
        range_row.append(InlineKeyboardButton("1-25", callback_data="show_rank_range_1"))
        range_row.append(InlineKeyboardButton("26-50", callback_data="show_rank_range_26"))
        if total_videos > 50:
            range_row.append(InlineKeyboardButton("51-75", callback_data="show_rank_range_51"))
        if total_videos > 75:
            range_row.append(InlineKeyboardButton("76-100", callback_data="show_rank_range_76"))
    elif start_rank >= 51:  # Current view is 51+ (handle dynamically)
        # Always show previous range
        prev_start = max(1, start_rank - 25)
        prev_end = prev_start + 24
        range_row.append(InlineKeyboardButton(f"{prev_start}-{prev_end}", callback_data=f"show_rank_range_{prev_start}"))
        
        # Current range
        curr_end = min(start_rank + 24, total_videos)
        range_row.append(InlineKeyboardButton(f"{start_rank}-{curr_end}", callback_data=f"show_rank_range_{start_rank}"))
        
        # Next range if available
        next_start = start_rank + 25
        if next_start <= total_videos:
            next_end = min(next_start + 24, total_videos)
            range_row.append(InlineKeyboardButton(f"{next_start}-{next_end}", callback_data=f"show_rank_range_{next_start}"))
    
    if range_row:
        keyboard.append(range_row)
    
    # Enter specific rank button
    keyboard.append([InlineKeyboardButton("⌨️ Enter Specific Rank", callback_data="enter_specific_rank")])
    
    # Back button
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="top_videos_navigator")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Edit the message with the jump interface
    try:
        await query.edit_message_text(msg, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Error in show_jump_rank_interface_with_range: {str(e)}")
        # Try to send as new message if editing fails
        try:
            await query.message.reply_html(msg, reply_markup=reply_markup)
        except Exception as nested_error:
            logging.error(f"Failed to send fallback message: {str(nested_error)}")

# This function was duplicated and has been removed to avoid conflicts

async def jump_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /jump command to jump to a specific rank in top videos"""
    if not context.args:
        await update.message.reply_text("Please provide a rank number. Example: /jump 42")
        return
    
    try:
        # Treat the argument as the exact index used in the inline buttons
        index = int(context.args[0])

        if index < 0:
            await update.message.reply_text("⚠️ Index must be zero or a positive number.")
            return

        # Check if we have enough videos; use canonicalized list to ensure ranking by file_id and aggregated counts
        sorted_videos = get_sorted_canonical_likes()

        total_videos = len(sorted_videos)

        if total_videos == 0:
            await update.message.reply_text("📊 No top videos available yet.")
            return

        if index >= total_videos:
            await update.message.reply_text(
                f"⚠️ Valid indices are 0 to {total_videos - 1}. Please enter a number within range."
            )
            return

        # Show the video at the requested index (mirrors view_top_video_<index>)
        await show_top_videos_viewer(update, context, index)

    except ValueError:
        await update.message.reply_text("⚠️ Please enter a valid number.")

async def handle_rank_number_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle replies to the 'Enter Specific Rank' message"""
    message = update.message
    if not message:
        return

    logging.info(f"Checking if message is a rank reply: {message.text}")

    user_data = context.user_data if hasattr(context, "user_data") else None
    prompt_message_id = user_data.get("rank_prompt_message_id") if user_data else None
    prompt_chat_id = user_data.get("rank_prompt_chat_id") if user_data else None
    expecting_rank = user_data.get("expecting_rank_reply", False) if user_data else False
    text_content = message.text.strip() if message.text else ""

    # Prevent duplicate processing when routed through multiple handlers
    if user_data is not None:
        processed_id = user_data.get("last_rank_processed_message_id")
        if processed_id == message.message_id:
            logging.debug("Rank message already processed; ignoring duplicate call")
            return

    if prompt_chat_id and message.chat and message.chat.id != prompt_chat_id:
        logging.debug("Ignoring potential rank reply from different chat")
        return

    replied_message = message.reply_to_message
    is_rank_reply = False

    # Direct reply to stored prompt
    if replied_message and prompt_message_id and replied_message.message_id == prompt_message_id:
        is_rank_reply = True
        logging.info("Matched rank prompt via stored message_id")

    # Fallback: reply detection by text/caption contents
    if not is_rank_reply and replied_message:
        if replied_message.text and "ENTER SPECIFIC RANK" in replied_message.text:
            is_rank_reply = True
            logging.info("Found 'ENTER SPECIFIC RANK' in text")
        elif getattr(replied_message, "caption_html", None) and "ENTER SPECIFIC RANK" in replied_message.caption_html:
            is_rank_reply = True
            logging.info("Found 'ENTER SPECIFIC RANK' in caption_html")
        elif replied_message.caption and "ENTER SPECIFIC RANK" in replied_message.caption:
            is_rank_reply = True
            logging.info("Found 'ENTER SPECIFIC RANK' in caption")

    # Allow non-reply input when we're explicitly expecting a rank
    if not is_rank_reply and expecting_rank:
        is_rank_reply = True
        logging.info("Accepting rank input without explicit reply because we're expecting it")

    # Allow standalone numeric messages (e.g., "45" or "#45") to trigger rank lookup
    if not is_rank_reply and text_content:
        if re.fullmatch(r"#?\d+", text_content):
            is_rank_reply = True
            logging.info("Treating standalone numeric message as rank request")

    if not is_rank_reply:
        # Not a rank entry message; let other handlers process it
        if expecting_rank:
            logging.debug("Expecting rank but message not recognized; reminding user")
            await message.reply_text("⚠️ Please send a rank number like 42.")
        return

    try:
        number_match = re.search(r"-?\d+", text_content)
        if not number_match:
            raise ValueError
        raw_value = int(number_match.group())
        logging.info(f"User entered top video value: {raw_value}")

        if raw_value < 0:
            await message.reply_text("⚠️ Index must be zero or a positive number.")
            return

        sorted_videos = sorted(
            get_sorted_canonical_likes(),
            key=lambda x: x[1],
            reverse=True
        )

        total_videos = len(sorted_videos)
        logging.info(f"Total videos available: {total_videos}")

        if total_videos == 0:
            await message.reply_text("📊 No top videos available yet.")
            return

        index = raw_value
        if index >= total_videos:
            # Allow 1-based input as a convenience if it fits within range
            if 1 <= raw_value <= total_videos:
                index = raw_value - 1
            else:
                await message.reply_text(
                    f"⚠️ Valid indices are 0 to {total_videos - 1}. Please enter a number within range."
                )
                return

        logging.info(f"Showing top video at index {index} (rank {index + 1})")
        await show_top_videos_viewer(update, context, index)

        # Clear prompt tracking on success
        if user_data is not None:
            user_data["expecting_rank_reply"] = False
            user_data.pop("rank_prompt_message_id", None)
            user_data.pop("rank_prompt_chat_id", None)
            user_data["last_rank_processed_message_id"] = message.message_id

    except ValueError:
        logging.info(f"Invalid rank number: {message.text}")
        await message.reply_text("⚠️ Please enter a valid number.")

async def show_top_videos_viewer(update, context, page=0, query=None, range_limit=None):
    """Show top videos one by one like a viewer with navigation"""
    # Check if there are any videos with likes
    if not favorites_data.get("video_likes"):
        msg = "📊 No top videos available yet."
        if query:
            await query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return
        
    # Sort videos by likes count
    sorted_videos = get_sorted_canonical_likes()
    
    total_videos = len(sorted_videos)
    
    # If range_limit is specified, filter the videos list
    if range_limit and range_limit > 0:
        sorted_videos = sorted_videos[:range_limit]
        max_videos = len(sorted_videos)
    else:
        max_videos = total_videos
    
    if page >= max_videos:
        page = max_videos - 1
    if page < 0:
        page = 0
    
    if not sorted_videos:
        msg = "📊 No top videos available yet."
        if query:
            await query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return
    
    likes_key, likes = sorted_videos[page]
    # Ensure likes is authoritative (canonical)
    likes = get_likes_count(likes_key)
    metadata = get_video_metadata(likes_key)
    if not metadata:
        # Skip invalid metadata and try next
        if page + 1 < max_videos:
            await show_top_videos_viewer(update, context, page + 1, query, range_limit=range_limit)
        else:
            msg = "📊 No valid top videos found."
            if query:
                await query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
        return

    tag = metadata["tag"]
    idx = metadata["index"]
    video_id = metadata["video_id"]
    file_id = metadata.get("file_id")
    
    # Check if video exists in database
    if tag not in media_data or not isinstance(media_data[tag], list) or not (0 <= idx < len(media_data[tag])):
        # Skip invalid video and try next
        if page + 1 < max_videos:
            await show_top_videos_viewer(update, context, page + 1, query, range_limit=range_limit)
        else:
            msg = "📊 No valid top videos found."
            if query:
                await query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
        return
    
    video_data = media_data[tag][idx]
    
    # Check if media is revoked
    if isinstance(video_data, dict) and video_data.get("revoked"):
        # Skip revoked video and try next
        if page + 1 < max_videos:
            await show_top_videos_viewer(update, context, page + 1, query, range_limit=range_limit)
        else:
            msg = "📊 No valid top videos found."
            if query:
                await query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
        return
    
    # Get user info for favorites
    if update:
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id if update.effective_user else None
    elif query:
        chat_id = query.message.chat_id
        user_id = query.from_user.id if query.from_user else None
    else:
        chat_id = None
        user_id = None
    
    user_id_str = str(user_id) if user_id else str(chat_id) if chat_id else None
    is_favorited = False
    if user_id_str:
        is_favorited = is_video_favorited(user_id_str, tag, idx, file_id=file_id)
    
    # Create caption with top video info
    share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
    share_url = f"https://t.me/share/url?url={share_link}"
    
    # Adjust caption based on whether we're showing a specific range
    # Recompute rank from canonical list (in case counts moved it)
    ranks = get_sorted_canonical_likes()
    current_rank = None
    for i, (k, v) in enumerate(ranks, start=1):
        if k == likes_key:
            current_rank = i
            break
    if current_rank is None:
        current_rank = page + 1
    if range_limit:
        caption = f"🔥 <b>Top {range_limit} | Video #{current_rank} | ❤️ {likes} likes</b>\n\n"
    else:
        caption = f"🔥 <b>Top Video #{current_rank} of {total_videos} | ❤️ {likes} likes</b>\n\n"
    
    caption += f"📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_link}'>🔗 Link</a>"
    
    # Create navigation keyboard for top videos
    keyboard = []
    nav_buttons = []
    
    # Always show Previous/Next navigation (loop around within range if specified)
    if range_limit:
        # When in range mode, only navigate within the range
        prev_page = page - 1 if page > 0 else len(sorted_videos) - 1  # Loop to last if at first
        next_page = page + 1 if page < len(sorted_videos) - 1 else 0  # Loop to first if at last
        
        # Add range_limit to the callback data to preserve the range when navigating
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"view_top_video_{prev_page}_{range_limit}"))
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"view_top_video_{next_page}_{range_limit}"))
    else:
        # Regular navigation through all videos
        prev_page = page - 1 if page > 0 else total_videos - 1  # Loop to last if at first
        next_page = page + 1 if page < total_videos - 1 else 0  # Loop to first if at last
        
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"view_top_video_{prev_page}"))
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"view_top_video_{next_page}"))
    
    keyboard.append(nav_buttons)
    
    # Action buttons row
    action_buttons = []
    if is_favorited:
        action_buttons.append(InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_id}"))
    else:
        action_buttons.append(InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_id}"))
    
    action_buttons.append(InlineKeyboardButton("🔗 Share", url=share_url))
    keyboard.append(action_buttons)
    
    # Additional buttons
    # Preview from current position button
    if range_limit:
        preview_text = f"▶️ Preview from #{page + 1} (Top {range_limit})"
        preview_callback = f"preview_from_rank_{page}_{range_limit}"
    else:
        preview_text = f"▶️ Preview from #{page + 1}"
        preview_callback = f"preview_from_rank_{page}"
    keyboard.append([InlineKeyboardButton(preview_text, callback_data=preview_callback)])
    keyboard.append([InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")])
    
    # Admin controls if admin
    if is_admin(chat_id):
        admin_buttons = build_admin_control_row(video_id)
        if admin_buttons:
            keyboard.extend(admin_buttons)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send the actual video/media
    # Send the media based on type
    if isinstance(video_data, dict):
        file_id = video_data.get("file_id")
        media_type = video_data.get("type", "video")
    else:
        file_id = video_data
        media_type = "video"

    try:
        from telegram import InputMediaVideo, InputMediaPhoto, InputMediaDocument, InputMediaAnimation
        
        # Always send as new message since we delete the previous one
        chat_id_to_use = chat_id
        
        # Prepare the media sending parameters
        media_kwargs = {
            "context": context,
            "chat_id": chat_id_to_use,
            "caption": caption,
            "reply_markup": reply_markup,
            "parse_mode": ParseMode.HTML,
            "protect_content": should_protect_content(user_id, chat_id_to_use) if user_id else should_protect_content(chat_id_to_use, chat_id_to_use),
            "user_id": user_id
        }
        
        if media_type == "video":
            media_kwargs["video"] = file_id
        elif media_type == "photo":
            media_kwargs["photo"] = file_id
        elif media_type == "document":
            media_kwargs["document"] = file_id
        elif media_type == "animation":
            media_kwargs["animation"] = file_id
        else:
            # Fallback to video
            media_kwargs["video"] = file_id
        
        # Send the new media message
        sent_msg = await safe_send_message(**media_kwargs)
        # Register this message as an active Top Viewer so we can refresh it later
        try:
            if sent_msg and getattr(sent_msg, 'message_id', None):
                register_active_top_viewer(sent_msg.chat_id, sent_msg.message_id, {
                    "video_key": likes_key,
                    "video_id": video_id,
                    "tag": tag,
                    "idx": idx,
                    "file_id": file_id,
                    "media_type": media_type,
                    "range_limit": range_limit,
                    "page": page,
                    "reply_markup": reply_markup,
                    "user_id": user_id
                })
        except Exception:
            pass
        
    except Exception as e:
        logging.error(f"Error sending top video: {str(e)}")
        error_msg = "❌ Error loading video."
        if query:
            await query.message.reply_text(error_msg)
        else:
            await update.message.reply_text(error_msg)
        
    except asyncio.TimeoutError:
        error_msg = f"⏱️ Request timed out while loading top video #{page + 1}. Please try again."
        if query:
            try:
                await context.bot.send_message(chat_id=query.message.chat_id, text=error_msg)
            except:
                pass
        else:
            await update.message.reply_text(error_msg)
    except Exception as e:
        error_msg = f"❌ Could not send top video #{page + 1}. Error: {str(e)[:100]}"
        if query:
            try:
                await context.bot.send_message(chat_id=query.message.chat_id, text=error_msg)
            except:
                pass
        else:
            await update.message.reply_text(error_msg)


async def show_top_videos_navigator(update, context):
    """Show the top videos navigator menu with Top 10/25/50 options and Jump to Specific Rank"""
    logging.info("show_top_videos_navigator called")
    # Count total videos with likes
    total_videos = len(favorites_data.get("video_likes", {}))
    logging.info(f"Total videos with likes: {total_videos}")
    
    # Create the navigation menu similar to the image
    msg = f"🔥 <b>TOP VIDEOS NAVIGATOR</b>\n\n📊 Total videos with likes: <b>{total_videos}</b>\n\nChoose an option:\n\n• View specific ranges (Top 10/25/50)\n• Start viewing from the beginning\n• Jump to a specific rank\n\nVideos are sorted by number of likes (❤️) in descending order."
    
    # Create keyboard with top navigation options
    keyboard = [
        [
            InlineKeyboardButton("🔝 Top 10", callback_data="view_top_video_0_10"),
            InlineKeyboardButton("🔝 Top 25", callback_data="view_top_video_0_25"),
            InlineKeyboardButton("🔝 Top 50", callback_data="view_top_video_0_50")
        ],
        [InlineKeyboardButton("🎬 Start Viewing Top Videos", callback_data="view_top_video_0")],
        [InlineKeyboardButton("🎯 Jump to Specific Rank", callback_data="jump_specific_rank")],
        [InlineKeyboardButton("🏠 Home", callback_data="return_home")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send the navigator menu
    logging.info("Sending top videos navigator menu")
    try:
        if isinstance(update, Update):
            await update.message.reply_html(msg, reply_markup=reply_markup)
        else:
            # Handle case when it's a callback query
            await update.edit_message_text(msg, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Error in show_top_videos_navigator: {str(e)}")
        # Try to send a simplified message if there was an error
        try:
            if hasattr(update, 'message'):
                await update.message.reply_text("Top Videos Navigator - Please select an option", reply_markup=reply_markup)
            elif hasattr(update, 'callback_query'):
                await update.callback_query.message.reply_text("Top Videos Navigator - Please select an option", reply_markup=reply_markup)
            else:
                # Direct send as new message
                chat_id = update.effective_chat.id if hasattr(update, 'effective_chat') else None
                if chat_id:
                    await context.bot.send_message(chat_id=chat_id, text="Top Videos Navigator - Please select an option", reply_markup=reply_markup)
        except Exception as nested_error:
            logging.error(f"Failed to send fallback message: {str(nested_error)}")

async def show_top_videos_page(update, context, page=0, query=None, range_limit=None):
    """Show a page of top videos with navigation buttons"""
    items_per_page = 10
    
    # Sort videos by likes count
    sorted_videos = get_sorted_canonical_likes()
    
    # If range_limit is specified, limit the videos list
    if range_limit and range_limit > 0:
        sorted_videos = sorted_videos[:range_limit]
    
    total_videos = len(sorted_videos)
    total_pages = (total_videos + items_per_page - 1) // items_per_page
    
    if page >= total_pages:
        page = total_pages - 1
    if page < 0:
        page = 0
    
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_videos = sorted_videos[start_idx:end_idx]
    
    # Build message
    if range_limit and range_limit > 0:
        msg = f"<b>📊 Top {range_limit} Liked Videos (Page {page + 1}/{total_pages})</b>\n\n"
    else:
        msg = f"<b>📊 Top Liked Videos (Page {page + 1}/{total_pages})</b>\n\n"
    
    for i, (likes_key, likes) in enumerate(page_videos, start_idx + 1):
        metadata = get_video_metadata(likes_key)
        if metadata:
            tag = metadata["tag"]
            idx = metadata["index"]
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            msg += f"{i}. Tag: <code>{tag}</code> | Index: <code>{idx}</code> | ❤️ {likes} likes | <a href='{file_link}'>📺 View</a>\n"
        else:
            msg += f"{i}. Unknown video key: <code>{likes_key}</code> | ❤️ {likes} likes\n"
    
    # Create navigation buttons
    keyboard = []
    nav_buttons = []
    
    # Create page navigation buttons, preserving range_limit if it's set
    if range_limit and range_limit > 0:
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"topvideos_page_range_{page-1}_{range_limit}"))
        
        nav_buttons.append(InlineKeyboardButton(f"📊 {page + 1}/{total_pages}", callback_data="topvideos_info"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"topvideos_page_range_{page+1}_{range_limit}"))
    else:
        # Regular navigation without range limit
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"topvideos_page_{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton(f"📊 {page + 1}/{total_pages}", callback_data="topvideos_info"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"topvideos_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Add refresh and back buttons
    if range_limit and range_limit > 0:
        # Show "Back to Navigator" button for range-limited views
        keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data=f"view_top_video_0_{range_limit}")])
        keyboard.append([InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")])
    else:
        keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="topvideos_page_0")])
        keyboard.append([InlineKeyboardButton("🔙 Back to Navigator", callback_data="top_videos_navigator")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send or edit message
    if query:
        await query.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)


async def show_off_limits_content(update: Update, context: ContextTypes.DEFAULT_TYPE, force_tag: str | None = None):
    """Show random media from the Off Limits category.
    If force_tag is provided, restrict selection and navigation to that tag.
    """
    global off_limits_data
    
    # Allow both command and callback contexts
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = update.effective_user.id if update.effective_user else None
    
    if not off_limits_data:
        # Gracefully handle when triggered from a button (no update.message)
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🏴‍☠️ <b>Off Limits</b>\n\n"
                "🚧 Under construction or owner hasn't pushed media yet in this category.\n\n"
                "💡 <i>Check back later for exclusive content!</i>"
            ),
            parse_mode=ParseMode.HTML
        )
        return
    
    # Pick a random video from off_limits (optionally restricted to a tag)
    valid_keys = []
    for key in off_limits_data:
        if '_' not in key:
            continue
        tag, idx_str = key.rsplit('_', 1)
        if force_tag and tag != force_tag:
            continue
        try:
            idx = int(idx_str)
        except ValueError:
            continue
        if tag not in media_data or not isinstance(media_data[tag], list):
            continue
        if not (0 <= idx < len(media_data[tag])):
            continue
        item = media_data[tag][idx]
        # Skip deleted/revoked
        if isinstance(item, dict) and (item.get('deleted') or item.get('revoked')):
            continue
        if isinstance(item, dict) and item.get('file_id'):
            valid_keys.append(key)
    
    if not valid_keys:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🏴‍☠️ <b>Off Limits</b>\n\n"
                "❌ No valid media found in this category.\n\n"
                "💡 <i>Admin needs to add media using /off_limits command.</i>"
            ),
            parse_mode=ParseMode.HTML
        )
        return
    
    # Send a random valid item
    video_key = random.choice(valid_keys)
    tag, idx_str = video_key.rsplit('_', 1)
    idx = int(idx_str)
    item = media_data[tag][idx]
    
    share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
    base_caption = f"🏴‍☠️ <b>Off Limits</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_link}'>🔗 Link</a>"
    
    # Determine if current user has this in favorites
    is_favorited = False
    try:
        uid = str(user_id)
        uf = favorites_data.get("user_favorites", {})
        is_favorited = (uid in uf) and (video_key in uf.get(uid, []))
    except Exception:
        is_favorited = False
    
    # Build dynamic inline keyboard similar to Random section
    fav_btn = (InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_key}")
               if is_favorited else InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_key}"))
    # Build Off Limits tag list for Switch cycling (only tags with valid items)
    offlimits_tags = []
    seen = set()
    for key in off_limits_data:
        if '_' not in key:
            continue
        t, i_str = key.rsplit('_', 1)
        if t in seen:
            continue
        try:
            ii = int(i_str)
        except ValueError:
            continue
        if t in media_data and isinstance(media_data[t], list) and 0 <= ii < len(media_data[t]):
            itm = media_data[t][ii]
            if isinstance(itm, dict) and not (itm.get('deleted') or itm.get('revoked')) and itm.get('file_id'):
                offlimits_tags.append(t)
                seen.add(t)

    # Determine next tag in Off Limits cycle
    next_tag = None
    if offlimits_tags:
        try:
            cur_idx = offlimits_tags.index(tag)
            next_tag = offlimits_tags[(cur_idx + 1) % len(offlimits_tags)] if offlimits_tags else None
        except ValueError:
            if offlimits_tags:
                next_tag = offlimits_tags[0]

    # Next stays within current tag context if available
    if force_tag or tag:
        next_btn = InlineKeyboardButton(f"⏭️ Next ({tag})", callback_data=f"next_offlimits_tag_{tag}")
    else:
        next_btn = InlineKeyboardButton("⏭️ Next", callback_data="next_offlimits")
    share_btn = InlineKeyboardButton("🔗 Share", url=f"https://t.me/share/url?url={share_link}")
    favs_btn = InlineKeyboardButton("⭐ MY FAV", callback_data="view_favorites")
    
    keyboard = [[fav_btn, next_btn]]
    if is_admin(user_id):
        # Admin layout mirrors Random: row2 [Switch, Share] if switch exists
        if next_tag:
            switch_btn = InlineKeyboardButton(f"🔀 Switch to {next_tag}", callback_data=f"switch_to_offlimits_tag_{next_tag}")
            keyboard.append([switch_btn, share_btn])
        else:
            keyboard.append([share_btn])
        keyboard += build_admin_control_row(video_key)
    else:
        # User layout: row2 [MY FAV, Share], row3 [Switch]
        keyboard.append([favs_btn, share_btn])
        if next_tag:
            switch_btn = InlineKeyboardButton(f"🔀 Switch to {next_tag}", callback_data=f"switch_to_offlimits_tag_{next_tag}")
            keyboard.append([switch_btn])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if item.get("type") == "video":
        await send_and_track_message(
            context.bot.send_video,
            chat_id=chat_id,
            video=item["file_id"],
            caption=build_final_caption(base_caption, add_global=False),
            parse_mode=ParseMode.HTML,
            protect_content=should_protect_content(user_id, chat_id),
            reply_markup=reply_markup,
            user_id=user_id
        )
    elif item.get("type") == "photo":
        await send_and_track_message(
            context.bot.send_photo,
            chat_id=chat_id,
            photo=item["file_id"],
            caption=build_final_caption(base_caption, add_global=False),
            parse_mode=ParseMode.HTML,
            protect_content=should_protect_content(user_id, chat_id),
            reply_markup=reply_markup,
            user_id=user_id
        )


async def favorites_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command for users to view their favorites using navigator"""
    user_id = str(update.effective_user.id)
    
    user_fav_map = favorites_data.get("user_favorites", {})
    if not user_fav_map.get(user_id):
        keyboard = InlineKeyboardMarkup([
[
            InlineKeyboardButton("🎲 Random 18-", callback_data="random_safe"),
            InlineKeyboardButton("🎲 Random 18+", callback_data="random_media")
]
])
        # Since this is a command, we just reply to the user
        await update.message.reply_text(
            "❤️ <b>You haven't added any videos to favorites yet!</b>\n\n💡 <i>Start by clicking the ❤️ button on videos you like to add them to your favorites.</i>",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return
    
    # Create a fake query object to use with the navigator
    class FakeQuery:
        def __init__(self, message, user):
            self.message = message
            self.from_user = user
    
    fake_query = FakeQuery(update.message, update.effective_user)
    await show_favorites_navigator(fake_query, context, 0)


def build_user_help_text(include_admin: bool = False) -> str:
    parts = [
        "🤖 <b>Bhaichara Bot - Help Guide</b>\n\n",
        "❤️ <b>User Commands:</b>\n",
        "• <b>/start</b> - Start the bot and see main menu\n",
        "• <b>/top</b> - Browse the Top Videos navigator\n",
        "• <b>/jump &lt;rank&gt;</b> - Jump straight to a ranked video\n",
        "• <b>/favorites</b> - View your favorite videos with navigation\n",
        "• <b>/help</b> - Show this help message\n\n",

        "🎬 <b>How to Use:</b>\n",
        "• Join 🗂️ <b>@BEINGHUMANASSOCIATION</b> for bulk access to all Tags\n",
        "• Click 🎲 <b>RANDOM MEDIA</b> for random videos from passed collection\n",
        "• Click 🏴‍☠️ <b>Off Limits</b> to browse exclusive picks (if enabled by admin)\n",
        "• Click ❤️ on any video to add to your favorites\n",
        "• Use ⭐ <b>MY FAVORITES</b> button for quick access to your collection\n",
        "• Navigate with ⬅️ <b>Previous</b> and ➡️ <b>Next</b> buttons\n\n",

        "⚡ <b>Quick Shortcuts:</b>\n",
        "Type these words or emojis for instant access:\n",
        "• <code>fav</code> , <code>favorites</code> , <code>❤️</code> → Open favorites\n",
        "• <code>random</code> , <code>rand</code> , <code>🎲</code> , <code>media</code> → Get random video\n",
        "• <code>help</code> , <code>?</code> , <code>❓</code> → Show this help overlay\n\n",

        "🎯 <b>Smart Features:</b>\n",
        "• <b>Two-Tier Random System:</b> GET FILES shows all content, RANDOM MEDIA shows curated content\n",
        "• <b>Smart Rotation:</b> All videos shown before any repeats\n",
        "• <b>Persistent Favorites:</b> Your favorites are saved permanently\n",
        "• <b>Navigation Memory:</b> Bot remembers your position in favorites\n",
        "• <b>Real-time Updates:</b> Instant feedback on all actions\n\n",

        "🧠 <b>Pro Tips:</b>\n",
        "• Use <code> /top </code> to open the Top Videos navigator with ready-made filters\n",
        "• Send <code> /jump index </code> to jump to that position instantly\n",
        "• Drop a single ❤️ (or type <code>fav</code>) anywhere to pull up your favorites\n",
        "• Tap 🔄 inside viewers to refresh the current list without leaving\n",
        "• Share any top video with friends via the 🔗 Share button inside the viewer\n\n",

        "🔒 <b>Privacy & Safety:</b> All interactions stay private and protected by the bot's security settings."
    ]

    if include_admin:
        parts.extend([
            "\n\n🔧 <b>Admin Commands (Complete Reference):</b>\n\n"
            
            "📁 <b>Media Management:</b>\n"
            "• <code>/upload</code> — Reply to media to add to database\n"
            "• <code>/listvideos</code> — List all tags\n"
            "• <code>/listvideo &lt;tag&gt;</code> — List media in a tag\n"
            "• <code>/view &lt;tag&gt; &lt;index&gt;</code> — View specific media by index\n"
            "• <code>/cview &lt;tag&gt; [start] [end]</code> — Clean view of only visible media\n"
            "• <code>/remove &lt;tag&gt; &lt;index&gt;</code> — Remove specific media\n"
            "• <code>/off_limits &lt;tag&gt; [start] [end]</code> — Add items to Off Limits category\n"
            "• <code>/get &lt;tag&gt;</code> — Get all media from a tag\n"
            "• <code>/free &lt;tag&gt;</code> / <code>/unfree &lt;tag&gt;</code> — Toggle free access\n"
            "• <code>/listfree</code> — List all free tags\n"
            "• <code>/generatelink &lt;tag&gt;</code> — Generate public link for tag\n\n"
            
            "🔗 <b>Link Systems (Separate Stores):</b>\n"
            "• <code>/pass &lt;tag&gt; [start] [end]</code> — RANDOM MEDIA button access only\n"
            "• <code>/pass &lt;tag1&gt; &lt;tag2&gt; ...</code> — Pass ALL available items for multiple tags\n"
            "• <code>/passall &lt;tag&gt;</code> — Pass entire tag at once\n"
            "• <code>/revoke &lt;tag&gt; [start] [end]</code> — Remove from RANDOM MEDIA\n"
            "• <code>/passlink &lt;tag&gt; [start] [end]</code> — Shareable link only\n"
            "• <code>/revokelink &lt;tag&gt; [start] [end]</code> — Disable shareable link\n"
            "• <code>/activelinks</code> — Active shareable links\n"
            "• <code>/passlinks</code> — Active RANDOM MEDIA links\n"
            "• <code>/listactive</code> — All active links (both types)\n\n"
            
            "🗑️ <b>Cleanup &amp; Recovery:</b>\n"
            "• <code>/fresh &lt;tag&gt; [i1 i2 ...]</code> — Shrink tag by removing useless gaps (deleted/revoked/invalid)\n"
            "• <code>/fresh &lt;tag&gt; &lt;start&gt; &lt;end&gt;</code> — Shrink within range only (deleted/revoked/invalid)\n"
            "• <code>/forcefresh &lt;tag&gt;</code> — Aggressively shrink tag (also removes revoked)\n"
            "• <code>/listdeleted</code> — Show all deleted media\n"
            "• <code>/listrevoked</code> — Show all revoked media\n"
            "• <code>/listremoved</code> — Show all removed media\n"
            "• <code>/restoredeleted &lt;tag&gt; &lt;index&gt;</code> — Restore deleted item\n"
            "• <code>/restoreall</code> — Restore all deleted media\n"
            "• <code>/cleardeleted</code> — Permanently purge deleted storage\n"
            "• <code>/cleanupdeleted</code> — Fix corrupted deleted entries\n"
            "• <code>/debugdeleted &lt;tag&gt;</code> — Debug deleted media issues\n"
            "• <code>/deletedstats</code> — Statistics about deleted media\n\n"
            
            "📊 <b>Analytics &amp; Users:</b>\n"
            "• <code>/userstats</code> — User registration statistics\n"
            "• <code>/userinfo &lt;user_id&gt;</code> — Detailed user information\n"
            "• <code>/topusers</code> — Most active users ranking\n"
            "• <code>/userfavorites &lt;user_id&gt;</code> — View user's favorites\n"
            "• <code>/videostats &lt;tag&gt; &lt;index&gt;</code> — Who liked a video\n"
            "• <code>/topvideos [limit]</code> — Most liked videos with navigation\n"
            "• <code>/discover</code> — Discover users from all sources\n"
            "• <code>/addusers &lt;id1&gt; &lt;id2&gt; ...</code> — Add users to database\n\n"
            
            "📢 <b>Broadcasting:</b>\n"
            "• <code>/broadcast &lt;message&gt;</code> — Normal broadcast\n"
            "• <code>/dbroadcast &lt;message&gt;</code> — Auto-delete broadcast\n"
            "• <code>/pbroadcast &lt;message&gt;</code> — Pin broadcast\n"
            "• <code>/sbroadcast &lt;message&gt;</code> — Silent broadcast\n"
            "• <code>/fbroadcast</code> — Forward mode (reply to message)\n"
            "• <code>/bstats</code> — Broadcasting statistics\n\n"
            
            "🛡️ <b>Protection &amp; Auto-Delete:</b>\n"
            "• <code>/protection</code> / <code>/pon</code> / <code>/poff</code> / <code>/pstatus</code> / <code>/ps</code>\n"
            "• <code>/testprotection</code> — Test media protection\n"
            "• <code>/checkprotection</code> — Check protection details\n"
            f"• <code>/autodelete on/off/status</code> — Auto-delete after {AUTO_DELETE_HOURS}h\n"
            "• <code>/autodelete hours &lt;hours&gt;</code> — Set deletion time (0.1-168)\n"
            "• <code>/autodelete stats</code> / <code>/autodelete clear</code>\n"
            "• <code>/notifications on/off</code> — Control deletion notifications\n\n"
            
            "📦 <b>Batch Tools:</b>\n"
            "• <code>/custom_batch &lt;tag&gt;</code> — Start custom batch\n"
            "• <code>/stop_batch</code> / <code>/cancel_batch</code> / <code>/batch_status</code>\n"
            "• <code>/move &lt;src_tag&gt; &lt;start&gt; &lt;end&gt; &lt;dest_tag&gt;</code> — Batch move media range\n"
            "• <code>/add &lt;src_tag&gt; &lt;start&gt; &lt;end&gt; &lt;dest_tag&gt;</code> — Batch copy media range\n\n"
            
            "🤖 <b>AI Batch (On-Demand):</b>\n"
            "• <code>/ai_batch</code> — Start AI tagging session\n"
            "• <code>/ai_batch_status</code> — Check AI batch status\n"
            "• <code>/stop_ai_batch</code> / <code>/cancel_ai_batch</code>\n"
            "💡 See AI_SETUP.md for Cloudflare or local CLIP setup\n\n"
            
            "💾 <b>Backups:</b>\n"
            "• <code>/togglebackup on/off/status</code> — Toggle local backups\n"
            "• <code>/backup</code> — Create backup (if enabled)\n"
            "• <code>/listbackups</code> / <code>/restore &lt;name&gt;</code> / <code>/backupstats &lt;name&gt;</code>\n"
            "• <code>/deletebackup &lt;name&gt;</code> — Delete a backup\n"
            "• <code>/telegrambackup</code> — Send JSON files to Telegram (always available; includes off_limits.json)\n"
            "• <code>/autobackup on/off/now/status</code> — Daily auto-backup controls\n\n"
            
            "� <b>Admin Tools:</b>\n"
            "• <code>/add_admin &lt;user_id&gt;</code> / <code>/remove_admin &lt;user_id&gt;</code> / <code>/list_admins</code>\n"
            "• <code>/set_global_caption &lt;text&gt;</code> — Set global caption\n"
            "• <code>/add_replacement &lt;find&gt; | &lt;replace&gt;</code> — Add caption rule\n"
            "• <code>/list_replacements</code> — List all caption rules\n"
            "• <code>/remove_replacement &lt;index&gt;</code> — Remove a rule\n"
            "• <code>/caption_config</code> — Show caption configuration\n"
            "• <code>/set_link &lt;url&gt;</code> — Override all links\n"
            "• <code>/link_off</code> — Disable link override\n"
            "• <code>/link_status</code> — Link override status\n"
            "• <code>/getfileid</code> — Get file_id (reply to media)\n"
            "• <code>/setwelcomeimage &lt;file_id&gt;</code> / <code>/testwelcomeimage</code>\n"
            "• <code>/checkupdates</code> — Check for pending old requests\n\n"
            
            "🧪 <b>Testing:</b>\n"
            "• <code>/testprotection</code> / <code>/testdeletion</code>\n\n"
            
            "⚠️ <b>Important:</b> <code>/pass</code> vs <code>/passlink</code> are separate!\n"
            "• <code>/pass</code> → RANDOM MEDIA button access\n"
            "• <code>/passlink</code> → Shareable links only\n\n"
            
            "♻️ <b>Inline PUSH:</b> Click PUSH on any media to add/remove it from tags inline with Back/Close buttons.\n"
            "🔁 <b>PUSH Status:</b> <code>/pushst</code> — See recent add/remove/move operations with Undo/Undo Move (index-stable)"
        ])

    return "".join(parts)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show complete 2-part help. Admin panel ONLY here (and HELP inline)."""
    user_id = update.effective_user.id

    # Help Part 1/2
    help_part1 = (
        "<b>Help (part 1/2)</b>\n\n"
        "🤖 <b>Bhaichara Bot - Help Guide</b>\n\n"
        "❤️ <b>User Commands:</b>\n"
        "• <code>/start</code> - Start the bot and see main menu\n"
        "• <code>/top</code> - Browse the Top Videos navigator\n"
        "• <code>/jump &lt;rank&gt;</code> - Jump straight to a ranked video\n"
        "• <code>/favorites</code> - View your favorite videos with navigation\n"
        "• <code>/help</code> - Show this help message\n\n"
        "� <b>How to Use:</b>\n"
        "• Join 🗂️ <b>@BEINGHUMANASSOCIATION</b> for bulk access to all Tags\n"
        "• Click 🎲 <b>RANDOM MEDIA</b> for random videos from passed collection\n"
        "• Click 🏴‍☠️ <b>Off Limits</b> to browse exclusive picks (if enabled by admin)\n"
        "• Click ❤️ on any video to add to your favorites\n"
        "• Use ⭐ <b>MY FAVORITES</b> button for quick access to your collection\n"
        "• Navigate with ⬅️ <b>Previous</b> and ➡️ <b>Next</b> buttons\n\n"
        "⚡ <b>Quick Shortcuts:</b>\n"
        "Type these words or emojis for instant access:\n"
        "• <code>fav</code>, <code>favorites</code>, <code>❤️</code> → Open favorites\n"
        "• <code>random</code>, <code>rand</code>, <code>🎲</code>, <code>media</code> → Get random video\n"
        "• <code>help</code>, <code>?</code>, <code>❓</code> → Show this help overlay\n\n"
        "🔍 <b>Smart Tag Search:</b>\n"
        "Type tag names (or part of them) to get instant content:\n"
        "• <code>inti</code> → intimacy videos\n"
        "• <code>turn</code> → Romantic/intimate content (turn.on tag)\n"
        "• <code>mediax</code> → Premium media collection\n"
        "• <code>snap</code> → Snapgod collections\n"
        "💡 <b>Smart Keywords:</b> Type <code>romance</code>, <code>love</code>, <code>passion</code>, <code>intimacy</code> → auto-maps to turn.on tag\n"
        "✨ No full tag name needed - bot finds matches automatically!\n"
        "📄 <b>Advanced Search:</b> Type any keyword to search captions/filenames. Results show paginated list with navigation (◀️ Prev/Next ▶️), ▶️ Preview All button to send all results with 🛑 Stop support, and 🎲 Watch from <tag> button if tag is in RANDOM MEDIA.\n\n"
        "🎯 <b>Smart Features:</b>\n"
        "• <b>Two-Tier Random System:</b> GET FILES shows all content, RANDOM MEDIA shows curated content\n"
        "• <b>Smart Rotation:</b> All videos shown before any repeats\n"
        "• <b>Persistent Favorites:</b> Your favorites are saved permanently\n"
        "• <b>Navigation Memory:</b> Bot remembers your position in favorites\n"
        "• <b>Real-time Updates:</b> Instant feedback on all actions\n\n"
        "🧠 <b>Pro Tips:</b>\n"
        "• Use <code>/top</code> to open the Top Videos navigator with ready-made filters\n"
        "• Send <code>/jump index</code> to jump to that position instantly\n"
        "• Drop a single ❤️ (or type <code>fav</code>) anywhere to pull up your favorites\n"
        "• Tap 🔄 inside viewers to refresh the current list without leaving\n"
        "• Share any top video with friends via the 🔗 Share button inside the viewer\n\n"
        "🔒 <b>Privacy &amp; Safety:</b> All interactions stay private and protected by the bot's security settings."
    )

    # Admin-only Part 2/2
    help_part2 = (
        "\n\n<b>🔧 Admin Commands (Complete Reference):</b>\n\n"
        "📁 <b>Media Management:</b>\n"
        "• <code>/upload</code> — Reply to media to add to database\n"
        "• <code>/listvideos</code> — List all tags\n"
        "• <code>/listvideo &lt;tag&gt;</code> — List media in a tag\n"
        "• <code>/view &lt;tag&gt; &lt;index&gt;</code> — View specific media by index\n"
        "• <code>/cview &lt;tag&gt; [start] [end]</code> — Clean view of only visible media\n"
        "• <code>/remove &lt;tag&gt; &lt;index&gt;</code> — Remove specific media\n"
        "• <code>/off_limits &lt;tag&gt; [start] [end]</code> — Add items to Off Limits category\n"
        "• <code>/get &lt;tag&gt;</code> — Get all media from a tag\n"
        "• <code>/free &lt;tag&gt;</code> / <code>/unfree &lt;tag&gt;</code> — Toggle free access\n"
        "• <code>/listfree</code> — List all free tags\n"
        "• <code>/generatelink &lt;tag&gt;</code> — Generate public link for tag\n\n"
        "🔗 <b>Link Systems (Separate Stores):</b>\n"
        "• <code>/pass &lt;tag&gt; [start] [end]</code> — RANDOM MEDIA button access only\n"
        "• <code>/revoke &lt;tag&gt; [start] [end]</code> — Remove from RANDOM MEDIA\n"
        "• <code>/passlink &lt;tag&gt; [start] [end]</code> — Shareable link only\n"
        "• <code>/revokelink &lt;tag&gt; [start] [end]</code> — Disable shareable link\n"
        "• <code>/activelinks</code> — Active shareable links\n"
        "• <code>/passlinks</code> — Active RANDOM MEDIA links\n"
        "• <code>/listactive</code> — All active links (both types)\n\n"
        "🗑️ <b>Cleanup &amp; Recovery:</b>\n"
        "• <code>/listdeleted</code> — Show all deleted media\n"
        "• <code>/listrevoked</code> — Show all revoked media\n"
        "• <code>/listremoved</code> — Show all removed media\n"
        "• <code>/restoredeleted &lt;tag&gt; &lt;index&gt;</code> — Restore deleted item\n"
        "• <code>/restoreall</code> — Restore all deleted media\n"
        "• <code>/cleardeleted</code> — Permanently purge deleted storage\n"
        "• <code>/cleanupdeleted</code> — Fix corrupted deleted entries\n"
        "• <code>/debugdeleted &lt;tag&gt;</code> — Debug deleted media issues\n"
        "• <code>/deletedstats</code> — Statistics about deleted media\n\n"
        "� <b>Analytics &amp; Users:</b>\n"
        "• <code>/userstats</code> — User registration statistics\n"
        "• <code>/userinfo &lt;user_id&gt;</code> — Detailed user information\n"
        "• <code>/topusers</code> — Most active users ranking\n"
        "• <code>/userfavorites &lt;user_id&gt;</code> — View user's favorites\n"
        "• <code>/videostats &lt;tag&gt; &lt;index&gt;</code> — Who liked a video\n"
        "• <code>/topvideos [limit]</code> — Most liked videos with navigation\n"
        "• <code>/discover</code> — Discover users from all sources\n"
        "• <code>/addusers &lt;id1&gt; &lt;id2&gt; ...</code> — Add users to database\n\n"
        "📢 <b>Broadcasting:</b>\n"
        "• <code>/broadcast &lt;message&gt;</code> — Normal broadcast\n"
        "• <code>/dbroadcast &lt;message&gt;</code> — Auto-delete broadcast\n"
        "• <code>/pbroadcast &lt;message&gt;</code> — Pin broadcast\n"
        "• <code>/sbroadcast &lt;message&gt;</code> — Silent broadcast\n"
        "• <code>/fbroadcast</code> — Forward mode (reply to message)\n"
        "• <code>/bstats</code> — Broadcasting statistics\n\n"
        "🛡️ <b>Protection &amp; Auto-Delete:</b>\n"
        "• <code>/protection</code> / <code>/pon</code> / <code>/poff</code> / <code>/pstatus</code> / <code>/ps</code>\n"
        "• <code>/testprotection</code> — Test media protection\n"
        "• <code>/checkprotection</code> — Check protection details\n"
        f"• <code>/autodelete on/off/status</code> — Auto-delete after {AUTO_DELETE_HOURS}h\n"
        "• <code>/autodelete hours &lt;hours&gt;</code> — Set deletion time (0.1-168)\n"
        "• <code>/autodelete stats</code> / <code>/autodelete clear</code>\n"
        "• <code>/notifications on/off</code> — Control deletion notifications\n\n"
        "📦 <b>Batch Tools:</b>\n"
        "• <code>/custom_batch &lt;tag&gt;</code> — Start custom batch\n"
        "• <code>/stop_batch</code> / <code>/cancel_batch</code> / <code>/batch_status</code>\n"
        "• <code>/move &lt;src_tag&gt; &lt;start&gt; &lt;end&gt; &lt;dest_tag&gt;</code> — Batch move media range\n"
        "• <code>/add &lt;src_tag&gt; &lt;start&gt; &lt;end&gt; &lt;dest_tag&gt;</code> — Batch copy media range\n\n"
        "🧹 <b>Tag Compaction:</b>\n"
        "• <code>/fresh &lt;tag&gt;</code> — Remove trailing useless indices (from end until valid found)\n"
        "• <code>/fresh &lt;tag&gt; &lt;start&gt; &lt;end&gt;</code> — Clean useless indices in range\n"
        "• <code>/fresh &lt;tag&gt; &lt;i1&gt; &lt;i2&gt; &lt;i3&gt; ...</code> — Clean specific indices only\n"
        "• <code>/forcefresh &lt;tag1&gt; [tag2] [tag3] ...</code> — Aggressive cleanup for entire tag(s); PURGES deleted media permanently\n"
        "• <code>/pass &lt;tag1&gt; &lt;tag2&gt; &lt;tag3&gt;</code> — Pass multiple tags to RANDOM MEDIA\n"
        "• <code>/passall</code> — Fill all valid indices for already-passed tags (no args needed)\n\n"
        "🤖 <b>AI Batch (On-Demand):</b>\n"
        "• <code>/ai_batch</code> — Start AI tagging session\n"
        "• <code>/ai_batch_status</code> — Check AI batch status\n"
        "• <code>/stop_ai_batch</code> / <code>/cancel_ai_batch</code>\n"
        "💡 See AI_SETUP.md for Cloudflare or local CLIP setup\n\n"
        "💾 <b>Backups:</b>\n"
        "• <code>/togglebackup on/off/status</code> — Toggle local backups\n"
        "• <code>/backup</code> — Create backup (if enabled)\n"
        "• <code>/listbackups</code> / <code>/restore &lt;name&gt;</code> / <code>/backupstats &lt;name&gt;</code>\n"
        "• <code>/deletebackup &lt;name&gt;</code> — Delete a backup\n"
        "• <code>/telegrambackup</code> — Send JSON files to Telegram (always available; includes off_limits.json)\n"
        "• <code>/autobackup on/off/now/status</code> — Daily auto-backup controls\n\n"
        "🔧 <b>Admin Tools:</b>\n"
        "• <code>/add_admin &lt;user_id&gt;</code> / <code>/remove_admin &lt;user_id&gt;</code> / <code>/list_admins</code>\n"
        "• <code>/set_global_caption &lt;text&gt;</code> — Set global caption\n"
        "• <code>/add_replacement &lt;find&gt; | &lt;replace&gt;</code> — Add caption rule\n"
        "• <code>/list_replacements</code> — List all caption rules\n"
        "• <code>/remove_replacement &lt;index&gt;</code> — Remove a rule\n"
        "• <code>/caption_config</code> — Show caption configuration\n"
        "• <code>/set_link &lt;url&gt;</code> — Override all links\n"
        "• <code>/link_off</code> — Disable link override\n"
        "• <code>/link_status</code> — Link override status\n"
        "• <code>/getfileid</code> — Get file_id (reply to media)\n"
        "• <code>/setwelcomeimage &lt;file_id&gt;</code> / <code>/testwelcomeimage</code>\n"
        "• <code>/checkupdates</code> — Check for pending old requests\n\n"
        "🧪 <b>Testing:</b>\n"
        "• <code>/testprotection</code> / <code>/testdeletion</code>\n\n"
        "⚠️ <b>Important:</b> <code>/pass</code> vs <code>/passlink</code> are separate!\n"
        "• <code>/pass</code> → RANDOM MEDIA button access\n"
        "• <code>/passlink</code> → Shareable links only\n\n"
        "♻️ <b>Inline PUSH:</b> Click PUSH on any media to add/remove it from tags inline with Back/Close buttons.\n"
        "🔁 <b>PUSH Status:</b> <code>/pushst</code> — See recent add/remove/move operations with Undo/Undo Move (index-stable)"
    )

    if user_id == ADMIN_ID:
        # Sanitize unsupported demo tokens before sending
        help_part1_s = help_part1.replace('<tag>', '&lt;tag&gt;')
        help_part2_s = help_part2.replace('<tag>', '&lt;tag&gt;')
        # Send Part 1
        await send_long_html(context, update.message.chat_id, help_part1_s)
        await asyncio.sleep(0.5)  # Small delay between messages
        await send_long_html(context, update.message.chat_id, "<b>Help (part 2/2)</b>\n" + help_part2_s)
        # Send admin keyboard as a separate message
        admin_keyboard = ReplyKeyboardMarkup([
            ["📊 Stats", "📢 Broadcast", "👥 Users"],
            ["🎬 Media", "💾 Backup", "🛡️ Protection"],
            ["🧹 Cleanup", "📝 Caption", "🔧 Tools"],
            ["🧪 Test", "🏠 HOME"]
        ], resize_keyboard=True)
        await context.bot.send_message(update.message.chat_id, "Select an admin action below:", reply_markup=admin_keyboard)
    else:
        await send_long_html(context, update.message.chat_id, help_part1.replace('<tag>', '&lt;tag&gt;'))


# ================== BROADCASTING SYSTEM ==================

async def get_all_users():
    """Get all users who have interacted with the bot"""
    users = set()
    
    # Add all tracked users (main source)
    users.update(users_data.keys())
    
    # Add users from favorites (fallback)
    if favorites_data and "user_favorites" in favorites_data:
        users.update(favorites_data["user_favorites"].keys())
    
    # Add exempted users (fallback)
    users.update(str(uid) for uid in exempted_users)
    
    return [int(uid) for uid in users if uid.isdigit()]


async def discover_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to discover and migrate users from all possible sources"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    current_time = asyncio.get_event_loop().time()
    initial_count = len(users_data)
    
    # Get all unique user IDs from various sources
    discovered_users = set()
    
    # From favorites (already done in migration, but double-check)
    if favorites_data and "user_favorites" in favorites_data:
        discovered_users.update(favorites_data["user_favorites"].keys())
    
    # From exempted users
    discovered_users.update(str(uid) for uid in exempted_users)
    
    # Add any missing users to the database
    for user_id_str in discovered_users:
        if user_id_str not in users_data and user_id_str.isdigit():
            users_data[user_id_str] = {
                "first_seen": current_time,
                "last_seen": current_time,
                "username": None,
                "first_name": None,
                "interaction_count": 1
            }
    
    save_users()
    new_count = len(users_data)
    added = new_count - initial_count
    
    await update.message.reply_text(
        f"🔍 <b>User Discovery Complete</b>\n\n"
        f"📊 Users before: {initial_count}\n"
        f"➕ Users discovered: {added}\n"
        f"📊 Total users now: {new_count}\n\n"
        f"💡 <b>Next Steps:</b>\n"
        f"• Use /addusers to manually add known user IDs\n"
        f"• Users will be automatically tracked from now on\n"
        f"• Check /bstats for current user count",
        parse_mode=ParseMode.HTML
    )


async def add_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to manually add user IDs to the database"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        await update.message.reply_text("❌ Usage: /addusers <user_id1> <user_id2> <user_id3> ...")
        return
    
    added_count = 0
    current_time = asyncio.get_event_loop().time()
    
    for user_id_str in context.args:
        try:
            user_id = int(user_id_str)
            user_id_str = str(user_id)
            
            if user_id_str not in users_data:
                users_data[user_id_str] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "username": None,
                    "first_name": None,
                    "interaction_count": 1
                }
                added_count += 1
        except ValueError:
            continue
    
    save_users()
    await update.message.reply_text(f"✅ Added {added_count} users to the database.\n📊 Total users now: {len(users_data)}")


async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                          delete_after=None, pin_message=False, silent=False):
    """Core broadcasting function"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return

    if not context.args:
        await update.message.reply_text("❌ Please provide a message to broadcast.")
        return

    # Preserve original formatting by extracting message after command
    command_text = update.message.text
    command_parts = command_text.split(maxsplit=1)
    if len(command_parts) < 2:
        await update.message.reply_text("❌ Please provide a message to broadcast.")
        return
    message_text = command_parts[1]
    users = await get_all_users()
    
    if not users:
        await update.message.reply_text("❌ No users found to broadcast to.")
        return

    success_count = 0
    failed_count = 0
    pinned_count = 0
    deleted_messages = []

    status_msg = await update.message.reply_text(
        f"📡 Broadcasting to {len(users)} users...\n"
        f"✅ Sent: {success_count}\n"
        f"❌ Failed: {failed_count}"
    )

    for user_id in users:
        try:
            sent_message = await context.bot.send_message(
                chat_id=user_id,
                text=message_text,
                parse_mode=ParseMode.HTML,
                disable_notification=silent
            )
            
            success_count += 1
            
            # Pin message if requested
            if pin_message:
                try:
                    await context.bot.pin_chat_message(
                        chat_id=user_id,
                        message_id=sent_message.message_id
                    )
                    pinned_count += 1
                except:
                    pass  # Ignore pin failures
            
            # Store message for deletion if requested
            if delete_after:
                deleted_messages.append((user_id, sent_message.message_id))
                
        except Exception as e:
            failed_count += 1
            continue

        # Update status every 10 messages
        if (success_count + failed_count) % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"📡 Broadcasting to {len(users)} users...\n"
                    f"✅ Sent: {success_count}\n"
                    f"❌ Failed: {failed_count}"
                )
            except:
                pass

    # Final status update
    final_text = (
        f"📡 <b>Broadcast Complete!</b>\n\n"
        f"👥 Total users: {len(users)}\n"
        f"✅ Successfully sent: {success_count}\n"
        f"❌ Failed: {failed_count}"
    )
    
    if pin_message:
        final_text += f"\n📌 Pinned: {pinned_count}"
    
    if delete_after:
        final_text += f"\n🗑 Will auto-delete in {delete_after} seconds"
        
    await status_msg.edit_text(final_text, parse_mode=ParseMode.HTML)

    # Schedule deletion if requested
    if delete_after and deleted_messages:
        await asyncio.sleep(delete_after)
        deleted_count = 0
        
        for user_id, message_id in deleted_messages:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=message_id)
                deleted_count += 1
            except:
                continue
        
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🗑 Auto-deleted {deleted_count} broadcast messages."
        )


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Normal broadcast message"""
    await broadcast_message(update, context)


async def dbroadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Auto-deleting broadcast message"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ Usage: /dbroadcast <seconds> <message>\n"
            "Example: /dbroadcast 60 This message will delete in 60 seconds"
        )
        return

    try:
        delete_after = int(context.args[0])
        # Preserve original formatting
        command_text = update.message.text
        command_parts = command_text.split(maxsplit=2)
        if len(command_parts) < 3:
            await update.message.reply_text("❌ Please provide a message to broadcast.")
            return
        
        # Temporarily modify message text to extract message part
        original_text = update.message.text
        update.message.text = f"/dbroadcast {command_parts[2]}"
        context.args = ["dummy"]  # Bypass empty args check
        
        await broadcast_message(update, context, delete_after=delete_after)
        
        # Restore original text
        update.message.text = original_text
    except ValueError:
        await update.message.reply_text("❌ First argument must be the number of seconds.")


async def pbroadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pin broadcast message"""
    await broadcast_message(update, context, pin_message=True)


async def sbroadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Silent broadcast (no notification)"""
    await broadcast_message(update, context, silent=True)


async def fbroadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Forward broadcast from a channel/chat"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "❌ Please reply to a message to forward it as broadcast.\n"
            "Usage: Reply to any message and use /fbroadcast"
        )
        return

    users = await get_all_users()
    
    if not users:
        await update.message.reply_text("❌ No users found to broadcast to.")
        return

    success_count = 0
    failed_count = 0
    
    status_msg = await update.message.reply_text(
        f"📡 Forward broadcasting to {len(users)} users...\n"
        f"✅ Sent: {success_count}\n"
        f"❌ Failed: {failed_count}"
    )

    for user_id in users:
        try:
            await update.message.reply_to_message.forward(chat_id=user_id)
            success_count += 1
        except:
            failed_count += 1
            continue

        # Update status every 10 messages
        if (success_count + failed_count) % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"📡 Forward broadcasting to {len(users)} users...\n"
                    f"✅ Sent: {success_count}\n"
                    f"❌ Failed: {failed_count}"
                )
            except:
                pass

    # Final status
    await status_msg.edit_text(
        f"📡 <b>Forward Broadcast Complete!</b>\n\n"
        f"👥 Total users: {len(users)}\n"
        f"✅ Successfully sent: {success_count}\n"
        f"❌ Failed: {failed_count}",
        parse_mode=ParseMode.HTML
    )


async def broadcast_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show broadcast statistics"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return

    users = await get_all_users()
    
    stats_text = [
        f"📊 <b>Broadcast Statistics</b>\n\n"
        f"👥 Total users in database: {len(users)}\n"
        f"❤️ Users with favorites: {len(favorites_data.get('user_favorites', {}))}\n"
        f"🆓 Exempted users: {len(exempted_users)}\n\n"
        f"📡 <b>Available Commands:</b>\n"
        f"• <code>/broadcast &lt;message&gt;</code> - Normal broadcast\n"
        f"• <code>/dbroadcast &lt;seconds&gt; &lt;message&gt;</code> - Auto-deleting broadcast\n"
        f"• <code>/pbroadcast &lt;message&gt;</code> - Pin broadcast\n"
        f"• <code>/sbroadcast &lt;message&gt;</code> - Silent broadcast\n"
        f"• <code>/fbroadcast</code> - Forward broadcast (reply to message)\n"
        f"• <code>/bstats</code> - Show these statistics"
    ]

    await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)


async def top_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show most active users with navigation"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not users_data:
        await update.message.reply_text("📊 No users in database yet.")
        return
    
    # Send the first page
    await show_top_users_page(update, context, 0)


async def show_top_users_page(update, context, page=0, query=None):
    """Show a page of top users with navigation buttons"""
    items_per_page = 10
    
    # Filter out admin and sort users by interaction count (descending)
    filtered_users = {k: v for k, v in users_data.items() if int(k) != ADMIN_ID}
    sorted_users = sorted(
        filtered_users.items(), 
        key=lambda x: x[1].get('interaction_count', 0), 
        reverse=True
    )
    
    total_users = len(sorted_users)
    total_pages = (total_users + items_per_page - 1) // items_per_page
    
    if page >= total_pages:
        page = total_pages - 1
    if page < 0:
        page = 0
    
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_users = sorted_users[start_idx:end_idx]
    
    # Build top users message
    stats_text = f"🔥 <b>Most Active Users (Page {page + 1}/{total_pages})</b>\n\n"
    
    for i, (user_id, user_data) in enumerate(page_users, start_idx + 1):
        first_name = user_data.get('first_name', 'Unknown')
        username = user_data.get('username', '')
        interactions = user_data.get('interaction_count', 0)
        
        # Add emoji for top positions
        if i == 1:
            emoji = "🥇"
        elif i == 2:
            emoji = "🥈"
        elif i == 3:
            emoji = "🥉"
        else:
            emoji = f"{i}."
        
        # Format username display
        if username:
            name_display = f"{first_name} (@{username})"
        else:
            name_display = first_name or f"User {user_id}"
        
        # Add interaction count with appropriate emoji
        if interactions >= 100:
            interaction_emoji = "🚀"
        elif interactions >= 50:
            interaction_emoji = "🔥"
        elif interactions >= 10:
            interaction_emoji = "⭐"
        elif interactions >= 5:
            interaction_emoji = "✨"
        else:
            interaction_emoji = "👤"
        
        stats_text += f"{emoji} {name_display} - {interactions} interactions {interaction_emoji}\n"
    
    # Add summary only on first page
    if page == 0:
        total_all_users = len(filtered_users)
        total_interactions = sum(user.get('interaction_count', 0) for user in filtered_users.values())
        
        stats_text += f"\n📊 <b>Summary:</b>\n"
        stats_text += f"👥 Total Users: {total_all_users}\n"
        stats_text += f"💬 Total Interactions: {total_interactions}\n"
        if total_all_users > 0:
            stats_text += f"📈 Average per User: {total_interactions/total_all_users:.1f}"
        else:
            stats_text += f"📈 Average per User: 0.0"
    
    # Create navigation buttons
    keyboard = []
    nav_buttons = []
    
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"topusers_page_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(f"👥 {page + 1}/{total_pages}", callback_data="topusers_info"))
    
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"topusers_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Add refresh button
    keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="topusers_page_0")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send or edit message
    if query:
        await query.edit_message_text(stats_text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    else:
        await update.message.reply_html(stats_text, reply_markup=reply_markup)


async def user_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to check user registration statistics"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Count users by source
    total_users = len(users_data)
    source_counts = {}
    
    for user_data in users_data.values():
        source = user_data.get("source", "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
    
    # Create stats message
    stats_text = (
        f"📊 <b>User Registration Statistics</b>\n\n"
        f"👥 Total Registered Users: {total_users}\n\n"
        f"📈 <b>Users by Registration Source:</b>\n"
    )
    
    for source, count in sorted(source_counts.items()):
        emoji = {
            "start_command": "🏠",
            "button_click": "🔘",
            "text_message": "💬", 
            "media_upload": "📸",
            "bot_interaction": "🤖",
            "favorites_migration": "❤️",
            "exempted_migration": "🆓",
            "manual_addition": "✋"
        }.get(source, "❓")
        
        stats_text += f"{emoji} {source.replace('_', ' ').title()}: {count}\n"
    
    stats_text += f"\n💡 All users are automatically registered when they interact with the bot!"
    
    await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)


async def userinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get detailed information about a specific user"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Check if user ID is provided
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> <code>/userinfo &lt;user_id&gt;</code>\n\n"
            "💡 <i>Example:</i> <code>/userinfo 123456789</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_id = int(context.args[0])
        user_id_str = str(user_id)
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please provide a numeric user ID.")
        return
    
    # Check if user exists in database
    if user_id_str not in users_data:
        await update.message.reply_text(f"❌ User with ID <code>{user_id}</code> not found in database.", parse_mode=ParseMode.HTML)
        return
    
    # Get user data
    user_info = users_data[user_id_str]
    
    # Format timestamps
    import datetime
    def format_timestamp(timestamp):
        if timestamp:
            try:
                dt = datetime.datetime.fromtimestamp(timestamp)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                return "Invalid timestamp"
        return "Not available"
    
    favorites_count = len(favorites_data.get("user_favorites", {}).get(user_id_str, {}))
    
    # Get user's preferences
    user_prefs = user_preferences.get(user_id, {})
    auto_delete_notif = user_prefs.get("auto_delete_notifications", True)
    
    # Try to get additional info from Telegram
    try:
        chat_info = await context.bot.get_chat(user_id)
        telegram_username = f"@{chat_info.username}" if chat_info.username else "No username"
        telegram_first_name = chat_info.first_name or "No first name"
        telegram_last_name = chat_info.last_name or ""
        telegram_bio = getattr(chat_info, 'bio', 'No bio') or "No bio"
        is_premium = getattr(chat_info, 'is_premium', False)
    except Exception as e:
        telegram_username = "Unable to fetch"
        telegram_first_name = "Unable to fetch"
        telegram_last_name = ""
        telegram_bio = "Unable to fetch"
        is_premium = False
    
    # Build detailed info message
    info_text = (
        f"👤 <b>User Information</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
        f"👤 <b>Username:</b> {telegram_username}\n"
        f"📝 <b>First Name:</b> {telegram_first_name}\n"
        f"📝 <b>Last Name:</b> {telegram_last_name}\n"
        f"📖 <b>Bio:</b> {telegram_bio}\n"
        f"⭐ <b>Premium:</b> {'Yes' if is_premium else 'No'}\n\n"
        
        f"📊 <b>Bot Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 <b>First Seen:</b> {format_timestamp(user_info.get('first_seen'))}\n"
        f"🕐 <b>Last Seen:</b> {format_timestamp(user_info.get('last_seen'))}\n"
        f"🔢 <b>Interactions:</b> {user_info.get('interaction_count', 0)}\n"
        f"📂 <b>Registration Source:</b> {user_info.get('source', 'Unknown').replace('_', ' ').title()}\n"
        f"❤️ <b>Favorites:</b> {favorites_count} videos\n\n"
        
        f"⚙️ <b>Settings</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔔 <b>Auto-delete Notifications:</b> {'Enabled' if auto_delete_notif else 'Disabled'}\n\n"
        
        f"💡 <i>This shows all available information about the user.</i>"
    )
    
    await update.message.reply_text(info_text, parse_mode=ParseMode.HTML)


# =================== END BROADCASTING SYSTEM ===================


# =================== BATCH PROCESSING SYSTEM ===================

# =================== PROTECTION COMMANDS ===================

# Protection commands for enabling and disabling content protection
async def pon_cmd(message, args):
    """Enable protection settings by writing to protection_settings.json."""
    try:
        with open('protection_settings.json', 'w') as f:
            json.dump({'enabled': True}, f)
        # TODO: Send confirmation message, e.g., message.reply('Protection enabled.')
    except Exception as e:
        # TODO: Log error e
        pass

async def poff_cmd(message, args):
    """Disable protection settings by writing to protection_settings.json."""
    try:
        with open('protection_settings.json', 'w') as f:
            json.dump({'enabled': False}, f)
        # TODO: Send confirmation message, e.g., message.reply('Protection disabled.')
    except Exception as e:
        # TODO: Log error e
        pass

async def protection_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Turn on media protection (prevent saving/downloading)"""
    try:
        user_id = update.effective_user.id
        print(f"Protection ON command from user {user_id}")
        if not is_admin(user_id):
            print(f"User {user_id} is not admin")
            await update.message.reply_text("❌ Admin access required.")
            return
        
        global protection_enabled
        protection_enabled = True
        save_protection_settings()
        print(f"Protection enabled: {protection_enabled}")
        
        await update.message.reply_text(
            "🛡️ <b>Protection ENABLED</b>\n\n"
            "✅ Media content is now protected from saving/downloading",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"Error in protection_on_command: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def protection_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Turn off media protection (allow saving/downloading)"""
    try:
        user_id = update.effective_user.id
        print(f"Protection OFF command from user {user_id}")
        if not is_admin(user_id):
            print(f"User {user_id} is not admin")
            await update.message.reply_text("❌ Admin access required.")
            return
        
        global protection_enabled
        protection_enabled = False
        save_protection_settings()
        print(f"Protection enabled: {protection_enabled}")
        
        await update.message.reply_text(
            "🔓 <b>Protection DISABLED</b>\n\n"
            "⚠️ Media content can now be saved/downloaded by users",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"Error in protection_off_command: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def protection_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check comprehensive protection status or toggle protection on/off"""
    global protection_enabled
    
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Handle on/off arguments
    if context.args:
        command = context.args[0].lower()
        if command == "on":
            protection_enabled = True
            save_protection_settings()
            await update.message.reply_text("🛡️ <b>Media Protection ENABLED</b>\n\n📊 All media content is now protected from saving/downloading", parse_mode=ParseMode.HTML)
            return
        elif command == "off":
            protection_enabled = False
            save_protection_settings()
            await update.message.reply_text("🔓 <b>Media Protection DISABLED</b>\n\n⚠️ Users can now save/download media content", parse_mode=ParseMode.HTML)
            return
    
    # Show comprehensive status if no arguments
    # Media Protection Status
    protection_icon = "🛡️" if protection_enabled else "🔓"
    protection_text = "ENABLED" if protection_enabled else "DISABLED"
    protection_desc = "protected from saving" if protection_enabled else "can be saved/downloaded"
    
    # Auto-Deletion Status
    auto_delete_icon = "🧹" if AUTO_DELETE_ENABLED else "⏸️"
    auto_delete_text = "ENABLED" if AUTO_DELETE_ENABLED else "DISABLED"
    
    # Notification Status
    notification_icon = "🔕" if not AUTO_DELETE_NOTIFICATIONS else "🔔"
    notification_text = "DISABLED" if not AUTO_DELETE_NOTIFICATIONS else "ENABLED"
    
    # Admin Protection
    admin_count = len(admin_list)
    
    # Channel Protection
    required_channels = len(REQUIRED_CHANNELS)
    
    # Current Statistics
    tracked_messages = len(sent_messages_tracker)
    total_users = len(users_data)
    total_media = sum(len(videos) for videos in media_data.values())
    exempted_count = len(exempted_users)
    
    comprehensive_status = (
        f"🛡️ <b>COMPREHENSIVE PROTECTION STATUS</b>\n\n"
        
        f"📱 <b>Media Protection:</b>\n"
        f"{protection_icon} Status: <b>{protection_text}</b>\n"
        f"📊 Content is currently {protection_desc}\n\n"
        
        f"🧹 <b>Auto-Deletion System:</b>\n"
        f"{auto_delete_icon} Status: <b>{auto_delete_text}</b>\n"
        f"⏰ Timer: <b>{AUTO_DELETE_HOURS} hour(s)</b>\n"
        f"📊 Tracked Messages: <b>{tracked_messages}</b>\n"
        f"⏳ Cleanup Interval: <b>5 minutes</b>\n\n"
        
        f"🔔 <b>Notifications:</b>\n"
        f"{notification_icon} Auto-Delete Alerts: <b>{notification_text}</b>\n"
        f"⏰ Cooldown Period: <b>{NOTIFICATION_COOLDOWN_HOURS} hour(s)</b>\n\n"
        
        f"👥 <b>Access Control:</b>\n"
        f"🔑 Admin Count: <b>{admin_count} users</b>\n"
        f"🚫 Exempted Users: <b>{exempted_count} users</b>\n"
        f"📢 Required Channels: <b>{required_channels} channels</b>\n\n"
        
        f"📊 <b>Database Stats:</b>\n"
        f"👤 Total Users: <b>{total_users}</b>\n"
        f"🎬 Total Media: <b>{total_media} files</b>\n"
        f"📂 Media Categories: <b>{len(media_data)} tags</b>\n\n"
        
        f"⚙️ <b>Security Features:</b>\n"
        f"✅ Channel membership verification\n"
        f"✅ Admin-only management commands\n"
        f"✅ Rate limiting protection\n"
        f"✅ Automatic user tracking\n"
        f"✅ Media revocation system\n"
        f"✅ Favorites protection\n\n"
        
        f"🔧 <b>Quick Actions:</b>\n"
        f"• <code>/protection on</code> - Enable media protection\n"
        f"• <code>/protection off</code> - Disable media protection\n"
        f"• <code>/autodelete</code> - Auto-deletion controls\n"
        f"• <code>/admin</code> - Admin management panel"
    )
    
    await update.message.reply_text(
        comprehensive_status,
        parse_mode=ParseMode.HTML
    )


async def test_protection_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a test media to verify protection is working"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Find any media from the database to test with
    test_media = None
    test_tag = None
    test_index = None
    
    for tag, media_list in media_data.items():
        if media_list and isinstance(media_list, list):
            for idx, item in enumerate(media_list):
                if isinstance(item, dict) and not item.get("deleted") and not item.get("revoked"):
                    test_media = item
                    test_tag = tag
                    test_index = idx
                    break
            if test_media:
                break
    
    if not test_media:
        await update.message.reply_text("❌ No media found in database to test with.")
        return
    
    status_icon = "🛡️" if protection_enabled else "🔓"
    status_text = "ENABLED" if protection_enabled else "DISABLED"
    
    caption = (f"🧪 <b>Protection Test</b>\n\n"
               f"{status_icon} Current Status: <b>{status_text}</b>\n"
               f"📁 Test Media: <code>{test_tag}_{test_index}</code>\n\n"
               f"💡 Try to save/download this media to test protection!")
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("❤️ ADD", callback_data=f"add_fav_{test_tag}_{test_index}")],
        [InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")]
    ])
    
    await send_media_by_type(
        context=context,
        chat_id=update.effective_chat.id,
        item=test_media,
        caption=caption,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id)
    )


async def test_deletion_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test the new deletion and restoration system"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    global deleted_media_storage
    
    # Show current deletion status
    deleted_count = len(deleted_media_storage)
    total_media = sum(len(videos) for videos in media_data.values() if isinstance(videos, list))
    
    status_text = (
        f"🧪 <b>Deletion System Test</b>\n\n"
        f"📊 <b>Current Status:</b>\n"
        f"• Active Media: {total_media}\n"
        f"• Deleted Media: {deleted_count}\n\n"
        f"💡 <b>How it works:</b>\n"
        f"• 🗑️ Delete: Removes & reorders (no gaps)\n"
        f"• ♻️ Restore: Puts back at original position\n"
        f"• No more 'Error sending file at index X'\n\n"
    )
    
    if deleted_count > 0:
        status_text += f"🗃️ <b>Deleted Items:</b>\n"
        for video_key, info in list(deleted_media_storage.items())[:5]:  # Show first 5
            tag = info['tag']
            pos = info['original_position']
            status_text += f"• <code>{video_key}</code> (was at {tag}[{pos}])\n"
        if deleted_count > 5:
            status_text += f"• ... and {deleted_count - 5} more\n"
    
    await update.message.reply_text(status_text, parse_mode=ParseMode.HTML)


async def check_protection_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check protection status and database statistics"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Count media in database
    total_media = 0
    protected_sends = 0
    unprotected_sends = 0
    
    for tag, media_list in media_data.items():
        if isinstance(media_list, list):
            for item in media_list:
                if not (isinstance(item, dict) and (item.get("deleted") or item.get("revoked"))):
                    total_media += 1
    
    # Simulate sending to count what would be protected vs unprotected
    current_protection = protection_enabled
    if current_protection:
        protected_sends = total_media
        unprotected_sends = 0
    else:
        protected_sends = 0
        unprotected_sends = total_media
    
    status_icon = "🛡️" if protection_enabled else "🔓"
    status_text = "ENABLED" if protection_enabled else "DISABLED"
    
    await update.message.reply_text(
        f"🔍 <b>Protection Analysis</b>\n\n"
        f"{status_icon} <b>Current Status: {status_text}</b>\n\n"
        f"📊 <b>Database Statistics:</b>\n"
        f"• Total Active Media: <code>{total_media}</code>\n"
        f"• Would be Protected: <code>{protected_sends}</code>\n"
        f"• Would be Unprotected: <code>{unprotected_sends}</code>\n\n"
        f"📱 <b>Protection Coverage:</b>\n"
        f"• All media requests use: <code>should_protect_content()</code>\n"
        f"• Setting applies to: <b>ALL database media</b>\n"
        f"• Includes: Random, Favorites, Links, View commands\n\n"
        f"💡 Use /testprotection to verify with actual media",
        parse_mode=ParseMode.HTML
    )


# =================== CUSTOM BATCH SYSTEM ===================

async def custom_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to start custom batch collection"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text(
            "Usage: /custom_batch <tag_name>\n\n"
            "Example: /custom_batch red"
        )
        return
    
    tag = context.args[0].strip().lower()
    user_id = update.effective_user.id
    
    # Initialize custom batch session
    custom_batch_sessions[user_id] = {
        'tag': tag,
        'media_list': [],
        'active': True,
        'start_time': asyncio.get_event_loop().time(),
        'status_chat_id': update.effective_chat.id,
        'status_message_id': None,
        'type_display': '—'
    }
    
    reply_keyboard = ReplyKeyboardMarkup(
        [["🛑 Stop Batch", "❌ Cancel Batch"]],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    intro_msg = await update.message.reply_text(
        f"📥 <b>Custom Batch Started</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📨 Status: Collecting media...\n\n"
        f"📋 <b>Instructions:</b>\n"
        f"• Forward/send any media files\n"
        f"• Media will be stored under tag '{tag}'\n"
        f"• Send /stop_batch to finish and get link\n"
        f"• Send /cancel_batch to cancel\n\n"
        f"🔄 Ready to receive media!",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_keyboard
    )

    # No status message yet; we'll create one on first media to avoid confusion/spam


async def stop_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop custom batch collection and generate link"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    if user_id not in custom_batch_sessions or not custom_batch_sessions[user_id]['active']:
        await update.message.reply_text("❌ No active custom batch session found.")
        return
    
    session = custom_batch_sessions[user_id]
    tag = session['tag']
    media_list = session['media_list']
    
    if not media_list:
        await update.message.reply_text("❌ No media collected. Batch cancelled.")
        del custom_batch_sessions[user_id]
        return
    
    # Save media to database with caption processing
    if tag not in media_data:
        media_data[tag] = []
    
    start_index = len(media_data[tag])
    
    # Process captions for each media item (preserve admin-provided caption)
    processed_media_list = []
    for media_item in media_list:
        # Create a copy of the media item
        processed_item = {
            "file_id": media_item["file_id"],
            "type": media_item["type"]
        }
        
        # Keep caption as-is (no processing needed)
        original_caption = media_item.get("caption", "")
        if original_caption:
            processed_item["caption"] = original_caption
        
        processed_media_list.append(processed_item)
    
    media_data[tag].extend(processed_media_list)
    end_index = len(media_data[tag]) - 1
    
    save_media()
    
    # Generate shareable link
    if start_index == 0 and end_index == len(media_data[tag]) - 1:
        link_param = tag
        range_text = "all files"
    else:
        link_param = f"{tag}_{start_index}_{end_index}"
        range_text = f"files {start_index}-{end_index}"
    
    link = f"https://t.me/{BOT_USERNAME}?start={link_param}"
    
    # Mark session as inactive
    custom_batch_sessions[user_id]['active'] = False
    
    await update.message.reply_text(
        f"✅ <b>Custom Batch Complete!</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📊 Media collected: {len(media_list)} files\n"
        f"📋 Range: {range_text}\n\n"
        f"🔗 <b>Shareable Link:</b>\n{link}\n\n"
        f"✨ Users can now access these media files!",
        parse_mode=ParseMode.HTML
    )

    # Update the status message to final state
    try:
        status_chat_id = session.get('status_chat_id')
        status_message_id = session.get('status_message_id')
        type_display = session.get('type_display', 'media')
        if status_chat_id and status_message_id:
            await context.bot.edit_message_text(
                chat_id=status_chat_id,
                message_id=status_message_id,
                text=(
                    f"✅ Batch complete!\n"
                    f"📁 Tag: {tag}\n"
                    f"🗂 Type: {type_display}\n"
                    f"📊 Total collected: {len(media_list)}\n"
                    f"🔗 Link: {link}"
                )
            )
    except Exception:
        pass


async def cancel_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel custom batch collection"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    if user_id not in custom_batch_sessions or not custom_batch_sessions[user_id]['active']:
        await update.message.reply_text("❌ No active custom batch session found.")
        return
    
    session = custom_batch_sessions[user_id]
    tag = session['tag']
    collected_count = len(session['media_list'])
    
    # Update status message to cancelled
    try:
        status_chat_id = session.get('status_chat_id')
        status_message_id = session.get('status_message_id')
        type_display = session.get('type_display', 'media')
        if status_chat_id and status_message_id:
            await context.bot.edit_message_text(
                chat_id=status_chat_id,
                message_id=status_message_id,
                text=(
                    f"❌ Batch cancelled\n"
                    f"📁 Tag: {tag}\n"
                    f"🗂 Type: {type_display}\n"
                    f"📊 Collected before cancel: {collected_count}"
                )
            )
    except Exception:
        pass

    # Remove session
    del custom_batch_sessions[user_id]
    
    await update.message.reply_text(
        f"❌ <b>Custom Batch Cancelled</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📊 Media that was collected: {collected_count} files\n"
        f"🗑️ All collected data discarded",
        parse_mode=ParseMode.HTML
    )


async def batch_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check current batch session status"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    if user_id not in custom_batch_sessions or not custom_batch_sessions[user_id]['active']:
        await update.message.reply_text("📊 No active custom batch session.")
        return
    
    session = custom_batch_sessions[user_id]
    tag = session['tag']
    collected_count = len(session['media_list'])
    start_time = session['start_time']
    current_time = asyncio.get_event_loop().time()
    duration = int(current_time - start_time)
    
    await update.message.reply_text(
        f"📊 <b>Custom Batch Status</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📊 Media collected: {collected_count} files\n"
        f"⏱️ Duration: {duration} seconds\n"
        f"🔄 Status: Active - collecting media\n\n"
        f"💡 Send /stop_batch to finish or /cancel_batch to cancel",
        parse_mode=ParseMode.HTML
    )


async def handle_custom_batch_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle media received during custom batch session"""
    user_id = update.effective_user.id
    
    # Check if user has active custom batch session
    if user_id in custom_batch_sessions and custom_batch_sessions[user_id]['active']:
        message = update.message
        
        # Extract ANY supported media/file types
        photo = message.photo[-1] if message.photo else None
        video = message.video
        document = message.document
        audio = message.audio
        voice = message.voice
        animation = getattr(message, 'animation', None)
        sticker = message.sticker  # still has a file_id

        file_id = None
        media_type = None

        if photo:
            file_id = photo.file_id
            media_type = "photo"
        elif video:
            file_id = video.file_id
            media_type = "video"
        elif document:
            file_id = document.file_id
            media_type = "document"
        elif audio:
            file_id = audio.file_id
            media_type = "audio"
        elif voice:
            file_id = voice.file_id
            media_type = "voice"
        elif animation:
            file_id = animation.file_id
            media_type = "animation"
        elif sticker:
            file_id = sticker.file_id
            media_type = "sticker"
        else:
            # As a fallback allow plain text messages to be saved as pseudo-document if admin wants
            if message.text and message.text.strip():
                file_id = None
                media_type = "text"
            else:
                return False

        original_caption = message.caption or message.text or ""

        media_item = {
            "type": media_type,
            "caption": original_caption
        }
        if file_id:
            media_item["file_id"] = file_id

        # Append and update counters
        session = custom_batch_sessions[user_id]
        session['media_list'].append(media_item)
        collected_count = len(session['media_list'])
        tag = session['tag']

        # Update type display: if mixed types across the batch, show 'media'
        prev_disp = session.get('type_display') or '—'
        if prev_disp == '—':
            new_disp = media_type
        elif prev_disp == media_type:
            new_disp = media_type
        else:
            new_disp = 'media'
        session['type_display'] = new_disp

        # Update single status message instead of sending a new one
        status_chat_id = session.get('status_chat_id')
        status_message_id = session.get('status_message_id')
        if status_chat_id and status_message_id:
            # Edit existing status message (silently ignore failures to avoid spam)
            try:
                await context.bot.edit_message_text(
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                    text=(
                        f"✅ Batch collecting media...\n"
                        f"📁 Tag: {tag}\n"
                        f"🗂 Type: {session.get('type_display', media_type)}\n"
                        f"📊 Total collected: {collected_count}"
                    )
                )
            except Exception:
                # Silently ignore edit failures (rate limit, message too old, etc.)
                pass
        else:
            # First message - create status tracker
            try:
                new_msg = await message.reply_text(
                    f"✅ Batch collecting media...\n"
                    f"📁 Tag: {tag}\n"
                    f"🗂 Type: {session.get('type_display', media_type)}\n"
                    f"📊 Total collected: {collected_count}"
                )
                session['status_message_id'] = new_msg.message_id
                session['status_chat_id'] = new_msg.chat_id
            except Exception:
                pass
        return True
    
    return False


# =================== END BATCH PROCESSING SYSTEM ===================


# =================== ADMIN QUICK PUSH (AUTO-COLLECT) ===================
# Per-admin session state for multi-select & pagination
admin_quick_push_sessions = {}  # uid -> {"selected": set[str], "page": int, "message_id": int, "chat_id": int}

QP_PAGE_SIZE = 21  # 7 rows * 3 columns

def _get_all_tags_sorted():
    try:
        tags = sorted(list(media_data.keys()))
    except Exception:
        tags = []
    return tags

def _build_quick_push_markup(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build text and inline keyboard for the Quick Push multi-select UI.

    - Paginates all tags (3 columns, up to QP_PAGE_SIZE per page)
    - Uses ✅ for selected tags, ⬜ for unselected
    - Adds controls: Prev/Next, Push, Clear, + New Tag, Cancel
    """
    items = admin_quick_push_buffer.get(uid, [])
    count = len(items)
    session = admin_quick_push_sessions.setdefault(uid, {"selected": set(), "page": 0, "message_id": None, "chat_id": None})
    selected = session.get("selected", set())
    page = max(0, int(session.get("page", 0)))

    tags = _get_all_tags_sorted()
    total = len(tags)
    if total == 0:
        # Still show controls to create new tag or cancel
        header = (
            "🧩 <b>Quick Push</b>\n\n"
            f"Collected: <b>{count}</b> item(s).\n"
            "No tags yet. Create a new tag to continue."
        )
        kb_rows = [[
            InlineKeyboardButton("➕ New Tag", callback_data="qp_new"),
            InlineKeyboardButton("✖️ Cancel", callback_data="qp_cancel")
        ]]
        return header, InlineKeyboardMarkup(kb_rows)

    # Pagination slice
    start = page * QP_PAGE_SIZE
    end = start + QP_PAGE_SIZE
    shown = tags[start:end]

    kb_rows = []
    row = []
    for t in shown:
        is_sel = t in selected
        label = ("✅ " if is_sel else "⬜ ") + t[:24]
        row.append(InlineKeyboardButton(label, callback_data=f"qp_t:{t}"))
        if len(row) == 3:
            kb_rows.append(row); row = []
    if row:
        kb_rows.append(row)

    # Controls row(s)
    # Pagination controls
    max_page = max(0, (total - 1) // QP_PAGE_SIZE)
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"qp_pg:{page-1}"))
    nav_row.append(InlineKeyboardButton(f"📄 {page+1}/{max_page+1}", callback_data="qp_pg:-1"))
    if page < max_page:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"qp_pg:{page+1}"))
    if nav_row:
        kb_rows.append(nav_row)

    # Action controls
    actions = []
    actions.append(InlineKeyboardButton("➕ New Tag", callback_data="qp_new"))
    push_label = f"🚀 Push ({len(selected)})" if selected else "🚀 Push"
    actions.append(InlineKeyboardButton(push_label, callback_data="qp_push"))
    actions.append(InlineKeyboardButton("🧹 Clear", callback_data="qp_clear"))
    kb_rows.append(actions)

    # Cancel row
    kb_rows.append([InlineKeyboardButton("✖️ Cancel", callback_data="qp_cancel")])

    text = (
        "🧩 <b>Quick Push</b>\n\n"
        f"Collected: <b>{count}</b> item(s).\n"
        f"Selected tags: <b>{len(selected)}</b>. Tap to toggle; Push when ready."
    )
    return text, InlineKeyboardMarkup(kb_rows)

async def admin_quick_push_media_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Collect media forwarded by admin and, after 2s of inactivity, show tag picker."""
    try:
        message = update.message
        if not message or not is_admin(update.effective_user.id):
            return

        # Only trigger in private chats with bot (not groups)
        if message.chat.type != 'private':
            return

        # Ignore if a custom or AI batch session is active
        uid = update.effective_user.id
        if uid in custom_batch_sessions and custom_batch_sessions[uid].get('active'):
            return
        if uid in ai_batch_sessions and ai_batch_sessions[uid].get('active'):
            return

        # Extract media
        photo = message.photo[-1] if message.photo else None
        video = message.video
        document = message.document
        audio = message.audio
        voice = message.voice
        animation = getattr(message, 'animation', None)
        sticker = message.sticker

        media_item = None
        if photo:
            media_item = {"file_id": photo.file_id, "type": "photo"}
        elif video:
            media_item = {"file_id": video.file_id, "type": "video"}
        elif document:
            media_item = {"file_id": document.file_id, "type": "document"}
        elif audio:
            media_item = {"file_id": audio.file_id, "type": "audio"}
        elif voice:
            media_item = {"file_id": voice.file_id, "type": "voice"}
        elif animation:
            media_item = {"file_id": animation.file_id, "type": "animation"}
        elif sticker:
            media_item = {"file_id": sticker.file_id, "type": "sticker"}
        else:
            return

        # Include original caption if any
        media_item["caption"] = message.caption or ""

        # Store in buffer
        admin_quick_push_buffer[uid].append(media_item)

        # Debounce prompt (2s)
        if uid in admin_quick_push_tasks:
            try:
                admin_quick_push_tasks[uid].cancel()
            except Exception:
                pass
        admin_quick_push_tasks[uid] = asyncio.create_task(_delayed_show_quick_push_prompt(uid, message.chat_id, context))
    except Exception as e:
        try:
            print(f"QuickPush error: {e}")
        except:
            pass


async def _delayed_show_quick_push_prompt(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE, delay: float = 2.0):
    await asyncio.sleep(delay)
    # If buffer empty, do nothing
    items = admin_quick_push_buffer.get(user_id, [])
    if not items:
        return

    # Initialize/Reset session for this user
    admin_quick_push_sessions[user_id] = {
        "selected": set(),
        "page": 0,
        "message_id": None,
        "chat_id": chat_id,
    }

    text, keyboard = _build_quick_push_markup(user_id)
    try:
        sent = await context.bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        # Store message id to support subsequent edits
        admin_quick_push_sessions[user_id]["message_id"] = sent.message_id
    except Exception:
        pass


async def handle_quick_push_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data

    # Cancel
    if data == "qp_cancel":
        admin_quick_push_buffer.pop(uid, None)
        admin_quick_push_sessions.pop(uid, None)
        try:
            await query.edit_message_text("❌ Quick push cancelled.")
        except Exception:
            pass
        return

    # New tag flow
    if data == "qp_new":
        items = admin_quick_push_buffer.get(uid, [])
        if not items:
            await query.edit_message_text("❌ No items to push.")
            return
        context.user_data["qp_waiting_new_tag"] = True
        # Keep a copy for text handler
        context.user_data["qp_pending_items"] = items.copy()
        admin_quick_push_buffer.pop(uid, None)
        admin_quick_push_sessions.pop(uid, None)
        await query.edit_message_text("➕ Send the new tag name (single word recommended).")
        return

    # Toggle tag selection (new multi-select)
    if data.startswith("qp_t:") or data.startswith("qp_tag:"):
        tag = data.split(":", 1)[1]
        # Ensure session exists
        sess = admin_quick_push_sessions.setdefault(uid, {"selected": set(), "page": 0, "message_id": query.message.message_id, "chat_id": query.message.chat_id})
        selected = sess.setdefault("selected", set())
        if tag in selected:
            selected.remove(tag)
        else:
            selected.add(tag)
        # Re-render
        text, keyboard = _build_quick_push_markup(uid)
        try:
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=keyboard)
            except Exception:
                pass
        return

    # Pagination
    if data.startswith("qp_pg:"):
        try:
            target_page = int(data.split(":", 1)[1])
        except ValueError:
            target_page = -1
        sess = admin_quick_push_sessions.setdefault(uid, {"selected": set(), "page": 0, "message_id": query.message.message_id, "chat_id": query.message.chat_id})
        if target_page >= 0:
            sess["page"] = target_page
        # Re-render
        text, keyboard = _build_quick_push_markup(uid)
        try:
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=keyboard)
            except Exception:
                pass
        return

    # Clear selection
    if data == "qp_clear":
        sess = admin_quick_push_sessions.setdefault(uid, {"selected": set(), "page": 0, "message_id": query.message.message_id, "chat_id": query.message.chat_id})
        sess["selected"] = set()
        text, keyboard = _build_quick_push_markup(uid)
        try:
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=keyboard)
            except Exception:
                pass
        return

    # Push to selected tags
    if data == "qp_push":
        items = admin_quick_push_buffer.get(uid, [])
        if not items:
            await query.edit_message_text("❌ No items to push.")
            return
        sess = admin_quick_push_sessions.get(uid) or {"selected": set()}
        selected = list(sess.get("selected", set()))
        if not selected:
            await query.answer("Select at least one tag", show_alert=True)
            return
        total_added = 0
        for tag in selected:
            media_data.setdefault(tag, [])
            for it in items:
                if not (isinstance(it, dict) and it.get("file_id") and it.get("type")):
                    continue
                # Avoid duplicates by file_id
                exists = False
                for ex in media_data[tag]:
                    if isinstance(ex, dict) and ex.get("file_id") == it["file_id"]:
                        exists = True; break
                if exists:
                    continue
                media_data[tag].append({
                    "file_id": it["file_id"],
                    "type": it["type"],
                    "caption": it.get("caption", "")
                })
                total_added += 1
        save_media()
        admin_quick_push_buffer.pop(uid, None)
        admin_quick_push_sessions.pop(uid, None)
        await query.edit_message_text(
            f"✅ Pushed {total_added} new item(s) into <b>{len(selected)}</b> tag(s).",
            parse_mode=ParseMode.HTML
        )
        return

    # Fallback
    await query.message.reply_text("❌ Unknown action.")


# =================== AI BATCH PROCESSING SYSTEM ===================

async def ai_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to start AI-powered batch classification"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    # Check if AI is configured
    from ai_classifier import get_classifier, AI_PROVIDER
    if AI_PROVIDER == "disabled":
        await update.message.reply_text(
            "❌ AI classification is disabled.\n\n"
            "To enable:\n"
            "1. Set AI_PROVIDER=cloudflare (or local)\n"
            "2. Configure Cloudflare credentials (see AI_SETUP.md)\n"
            "3. Restart the bot"
        )
        return
    
    # Initialize AI batch session
    ai_batch_sessions[user_id] = {
        'media_list': [],
        'classified_items': [],
        'active': True,
        'start_time': asyncio.get_event_loop().time(),
        'stats': {
            'total': 0,
            'classified': 0,
            'low_confidence': 0,
            'errors': 0
        }
    }
    
    reply_keyboard = ReplyKeyboardMarkup(
        [["🛑 Stop AI Batch", "❌ Cancel AI Batch"]],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    
    await update.message.reply_text(
        f"🤖 <b>AI Batch Classification Started</b>\n\n"
        f"📋 <b>How it works:</b>\n"
        f"• Forward/send any media files\n"
        f"• AI will auto-classify and assign tags\n"
        f"• Similar media will be grouped into albums\n"
        f"• You can manually edit after batch completes\n\n"
        f"⚙️ <b>Settings:</b>\n"
        f"• Provider: <code>{AI_PROVIDER}</code>\n"
        f"• Confidence threshold: <code>0.35</code>\n"
        f"• Album grouping: <code>enabled</code>\n\n"
        f"📊 <b>Status:</b> Ready to receive media\n\n"
        f"💡 Send /stop_ai_batch when done or /cancel_ai_batch to abort",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_keyboard
    )


async def stop_ai_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop AI batch and save all classified media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    # Restore admin keyboard (used in all return paths)
    admin_keyboard = ReplyKeyboardMarkup([
        ["📊 Stats", "📢 Broadcast", "👥 Users"],
        ["🎬 Media", "💾 Backup", "🛡️ Protection"],
        ["🧹 Cleanup", "🔧 Tools", "🧪 Test"],
        ["🏠 HOME"]
    ], resize_keyboard=True)
    
    if user_id not in ai_batch_sessions or not ai_batch_sessions[user_id]['active']:
        await update.message.reply_text(
            "❌ No active AI batch session found.",
            reply_markup=admin_keyboard
        )
        return
    
    session = ai_batch_sessions[user_id]
    classified_items = session['classified_items']
    stats = session['stats']
    
    if not classified_items:
        await update.message.reply_text(
            "❌ No media was successfully classified.\n"
            "AI batch cancelled.",
            reply_markup=admin_keyboard
        )
        del ai_batch_sessions[user_id]
        return
    
    # Group items by tag
    tag_groups = {}
    for item in classified_items:
        tag = item['tag']
        if tag not in tag_groups:
            tag_groups[tag] = []
        tag_groups[tag].append(item)
    
    # Save to database
    saved_summary = []
    for tag, items in tag_groups.items():
        if tag not in media_data:
            media_data[tag] = []
        
        start_idx = len(media_data[tag])
        
        for item in items:
            media_entry = {
                "file_id": item["file_id"],
                "type": item["type"]
            }
            
            # Add album metadata if grouped
            if item.get("album_id"):
                media_entry["album_id"] = item["album_id"]
            
            # Store AI metadata
            media_entry["ai_classified"] = True
            media_entry["ai_confidence"] = item["confidence"]
            
            media_data[tag].append(media_entry)
        
        end_idx = len(media_data[tag]) - 1
        
        # Generate link for this tag's range
        if start_idx == 0 and end_idx == len(media_data[tag]) - 1:
            link_param = tag
        else:
            link_param = f"{tag}_{start_idx}_{end_idx}"
        
        link = f"https://t.me/{BOT_USERNAME}?start={link_param}"
        saved_summary.append(f"• <code>{tag}</code>: {len(items)} files\n  {link}")
    
    save_media()
    
    # Mark session as inactive
    ai_batch_sessions[user_id]['active'] = False
    
    # Build completion message
    duration = int(asyncio.get_event_loop().time() - session['start_time'])
    
    await update.message.reply_text(
        f"✅ <b>AI Batch Complete!</b>\n\n"
        f"📊 <b>Statistics:</b>\n"
        f"• Total processed: {stats['total']}\n"
        f"• Successfully classified: {stats['classified']}\n"
        f"• Low confidence: {stats['low_confidence']}\n"
        f"• Errors: {stats['errors']}\n"
        f"⏱️ Duration: {duration}s\n\n"
        f"📁 <b>Saved to tags:</b>\n" + "\n".join(saved_summary) + "\n\n"
        f"✨ Media is now available to users!",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_keyboard
    )


async def cancel_ai_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel AI batch session"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    # Restore admin keyboard (used in all return paths)
    admin_keyboard = ReplyKeyboardMarkup([
        ["📊 Stats", "📢 Broadcast", "👥 Users"],
        ["🎬 Media", "💾 Backup", "🛡️ Protection"],
        ["🧹 Cleanup", "🔧 Tools", "🧪 Test"],
        ["🏠 HOME"]
    ], resize_keyboard=True)
    
    if user_id not in ai_batch_sessions or not ai_batch_sessions[user_id]['active']:
        await update.message.reply_text(
            "❌ No active AI batch session found.",
            reply_markup=admin_keyboard
        )
        return
    
    session = ai_batch_sessions[user_id]
    processed_count = session['stats']['total']
    
    # Remove session
    del ai_batch_sessions[user_id]
    
    await update.message.reply_text(
        f"❌ <b>AI Batch Cancelled</b>\n\n"
        f"📊 Processed before cancel: {processed_count} items\n"
        f"🗑️ All data discarded",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_keyboard
    )


async def ai_batch_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check current AI batch status"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    if user_id not in ai_batch_sessions or not ai_batch_sessions[user_id]['active']:
        await update.message.reply_text("📊 No active AI batch session.")
        return
    
    session = ai_batch_sessions[user_id]
    stats = session['stats']
    duration = int(asyncio.get_event_loop().time() - session['start_time'])
    
    # Count items by tag
    tag_counts = {}
    for item in session['classified_items']:
        tag = item['tag']
        tag_counts[tag] = tag_counts.get(tag, 0) + 1
    
    tag_summary = "\n".join([f"  • {tag}: {count}" for tag, count in sorted(tag_counts.items())])
    if not tag_summary:
        tag_summary = "  <i>No items classified yet</i>"
    
    await update.message.reply_text(
        f"📊 <b>AI Batch Status</b>\n\n"
        f"⏱️ Duration: {duration}s\n"
        f"📈 Progress:\n"
        f"  • Total processed: {stats['total']}\n"
        f"  • Successfully classified: {stats['classified']}\n"
        f"  • Low confidence: {stats['low_confidence']}\n"
        f"  • Errors: {stats['errors']}\n\n"
        f"📁 <b>Tags assigned:</b>\n{tag_summary}\n\n"
        f"💡 Send /stop_ai_batch to finish or /cancel_ai_batch to abort",
        parse_mode=ParseMode.HTML
    )


async def handle_ai_batch_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle media received during AI batch session"""
    user_id = update.effective_user.id
    
    # Check if user has active AI batch session
    if user_id not in ai_batch_sessions or not ai_batch_sessions[user_id]['active']:
        return False
    
    message = update.message
    session = ai_batch_sessions[user_id]
    
    # Extract media
    photo = message.photo[-1] if message.photo else None
    video = message.video
    document = message.document
    audio = message.audio
    voice = message.voice
    animation = getattr(message, 'animation', None)
    
    file_id = None
    media_type = None
    file_obj = None
    
    if photo:
        file_id = photo.file_id
        media_type = "photo"
        file_obj = photo
    elif video:
        file_id = video.file_id
        media_type = "video"
        file_obj = video
    elif document:
        file_id = document.file_id
        media_type = "document"
        file_obj = document
    elif audio:
        file_id = audio.file_id
        media_type = "audio"
        file_obj = audio
    elif voice:
        file_id = voice.file_id
        media_type = "voice"
        file_obj = voice
    elif animation:
        file_id = animation.file_id
        media_type = "animation"
        file_obj = animation
    else:
        return False
    
    session['stats']['total'] += 1
    item_number = session['stats']['total']
    
    # Send processing message
    processing_msg = await message.reply_text(
        f"🤖 Processing item #{item_number}...\n"
        f"📁 Type: {media_type}"
    )
    
    try:
        # Download media for classification
        file = await context.bot.get_file(file_id)
        
        # Get candidate tags from existing media_data
        candidate_tags = list(media_data.keys())[:30]  # Top 30 tags
        
        if not candidate_tags:
            await processing_msg.edit_text(
                f"⚠️ Item #{item_number}: No existing tags found\n"
                f"💡 Create some tags first with /upload or /custom_batch"
            )
            session['stats']['errors'] += 1
            return True
        
        # Run AI classification
        from ai_classifier import get_classifier
        classifier = get_classifier()
        
        if media_type in ["photo", "document"]:
            # Download image data directly to memory
            try:
                image_data = await file.download_as_bytearray()
                best_tag, confidence = await classifier.classify_image(bytes(image_data), candidate_tags)
            except Exception as download_err:
                logger.error(f"Failed to download image for classification: {download_err}")
                session['stats']['errors'] += 1
                await processing_msg.edit_text(
                    f"❌ Item #{item_number}: Failed to download media\n"
                    f"Error: {str(download_err)[:80]}"
                )
                return True
        elif media_type == "video":
            # Download video to temp file for frame extraction
            import tempfile
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
                    await file.download_to_drive(tmp.name)
                    temp_path = tmp.name
                
                best_tag, confidence = await classifier.classify_video(temp_path, candidate_tags)
                os.unlink(temp_path)
            except Exception as video_err:
                logger.error(f"Failed to process video: {video_err}")
                session['stats']['errors'] += 1
                try:
                    os.unlink(temp_path)
                except:
                    pass
                await processing_msg.edit_text(
                    f"❌ Item #{item_number}: Failed to process video\n"
                    f"Error: {str(video_err)[:80]}"
                )
                return True
        else:
            # For audio/voice, use first tag as fallback
            best_tag = candidate_tags[0] if candidate_tags else "uncategorized"
            confidence = 0.0
        
        if best_tag and confidence >= 0.35:
            # Successfully classified
            classified_item = {
                "file_id": file_id,
                "type": media_type,
                "tag": best_tag,
                "confidence": confidence,
                "album_id": None  # Will be set if similar to previous items
            }
            
            # Check similarity with recent items for album grouping
            if session['classified_items'] and media_type in ["photo", "video"]:
                last_item = session['classified_items'][-1]
                if last_item['type'] == media_type and last_item['tag'] == best_tag:
                    # Compute similarity (simplified: just group consecutive items with same tag)
                    # Full implementation would compare embeddings
                    if not last_item.get('album_id'):
                        # Create new album
                        import uuid
                        album_id = str(uuid.uuid4())[:8]
                        last_item['album_id'] = album_id
                    classified_item['album_id'] = last_item['album_id']
            
            session['classified_items'].append(classified_item)
            session['stats']['classified'] += 1
            
            album_text = f"\n📚 Album: {classified_item['album_id']}" if classified_item['album_id'] else ""
            
            await processing_msg.edit_text(
                f"✅ Item #{item_number} classified!\n"
                f"📁 Tag: <code>{best_tag}</code>\n"
                f"🎯 Confidence: {confidence:.2%}{album_text}\n"
                f"📊 Progress: {session['stats']['classified']}/{session['stats']['total']}",
                parse_mode=ParseMode.HTML
            )
        else:
            # Low confidence or failed
            session['stats']['low_confidence'] += 1
            
            await processing_msg.edit_text(
                f"⚠️ Item #{item_number}: Low confidence\n"
                f"🤔 Best guess: {best_tag if best_tag else 'unknown'} ({confidence:.2%})\n"
                f"💡 Consider manual tagging for this item"
            )
        
        return True
        
    except Exception as e:
        session['stats']['errors'] += 1
        logger.error(f"AI classification error: {e}")
        
        await processing_msg.edit_text(
            f"❌ Item #{item_number}: Classification failed\n"
            f"Error: {str(e)[:100]}"
        )
        
        # Clean up temp file if it exists
        try:
            if 'temp_path' in locals():
                os.unlink(temp_path)
        except:
            pass
        
        return True


# =================== END AI BATCH PROCESSING SYSTEM ===================


# =================== CAPTION MANAGEMENT SYSTEM ===================

async def set_global_caption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to set global caption for all files"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        # Show current global caption
        current = caption_config.get("global_caption", "")
        if current:
            await update.message.reply_text(
                f"📝 <b>Current Global Caption:</b>\n\n{current}",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("📝 No global caption is currently set.")
        return
    
    # Set new global caption
    new_caption = " ".join(context.args)
    caption_config["global_caption"] = new_caption
    save_caption_config()
    
    await update.message.reply_text(
        f"✅ <b>Global Caption Updated!</b>\n\n"
        f"📝 New Caption:\n{new_caption}",
        parse_mode=ParseMode.HTML
    )


# =================== END CAPTION MANAGEMENT SYSTEM ===================


# =================== ADMIN MANAGEMENT SYSTEM ===================

async def add_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new admin (only main admin can add other admins)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /add_admin <user_id>")
        return
    
    try:
        new_admin_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please provide a valid number.")
        return
    
    if new_admin_id in admin_list:
        await update.message.reply_text(f"⚠️ User {new_admin_id} is already an admin.")
        return
    
    admin_list.append(new_admin_id)
    save_admin_list()
    
    await update.message.reply_text(f"✅ User {new_admin_id} has been added as an admin.")


async def remove_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove an admin (only admins can remove other admins)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /remove_admin <user_id>")
        return
    
    try:
        admin_id_to_remove = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please provide a valid number.")
        return
    
    if admin_id_to_remove == ADMIN_ID:
        await update.message.reply_text("❌ Cannot remove the main admin.")
        return
    
    if admin_id_to_remove not in admin_list:
        await update.message.reply_text(f"⚠️ User {admin_id_to_remove} is not an admin.")
        return
    
    admin_list.remove(admin_id_to_remove)
    save_admin_list()
    
    await update.message.reply_text(f"✅ User {admin_id_to_remove} has been removed from admin list.")


async def list_admins_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all admins"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not admin_list:
        await update.message.reply_text("📋 No admins found.")
        return
    
    message = "<b>👨‍💼 Admin List:</b>\n\n"
    
    for i, admin_id in enumerate(admin_list, 1):
        if admin_id == ADMIN_ID:
            message += f"{i}. <code>{admin_id}</code> (Main Admin) 👑\n"
        else:
            message += f"{i}. <code>{admin_id}</code>\n"
    
    await update.message.reply_html(message)


async def get_file_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get file_id from any media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Check if replying to a message with media
    if update.message.reply_to_message:
        reply_msg = update.message.reply_to_message
        file_id = None
        media_type = None
        
        if reply_msg.photo:
            file_id = reply_msg.photo[-1].file_id  # Get highest resolution
            media_type = "photo"
        elif reply_msg.video:
            file_id = reply_msg.video.file_id
            media_type = "video"
        elif reply_msg.document:
            file_id = reply_msg.document.file_id
            media_type = "document"
        elif reply_msg.audio:
            file_id = reply_msg.audio.file_id
            media_type = "audio"
        elif reply_msg.voice:
            file_id = reply_msg.voice.file_id
            media_type = "voice"
        elif reply_msg.animation:
            file_id = reply_msg.animation.file_id
            media_type = "animation"
        
        if file_id:
            await update.message.reply_text(
                f"📄 <b>File Information</b>\n\n"
                f"🗂 <b>Type:</b> {media_type}\n"
                f"🆔 <b>File ID:</b>\n<code>{file_id}</code>\n\n"
                f"💡 <b>To set as welcome image:</b>\n"
                f"<code>/setwelcomeimage {file_id}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("❌ No media found in the replied message.")
    else:
        await update.message.reply_text(
            "📋 <b>Get File ID</b>\n\n"
            "Reply to any photo/video/document with this command to get its file_id.\n\n"
            "<b>Usage:</b>\n"
            "1. Send or find a media message\n"
            "2. Reply to it with <code>/getfileid</code>\n"
            "3. Copy the file_id from the response",
            parse_mode=ParseMode.HTML
        )


async def getfile_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send the file associated with a given file_id.
    Usage: /getfile <file_id>
    Admin only command to send the file by searching media_data for the file_id, otherwise tries to forward file by id.
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /getfile <file_id>")
        return

    file_id = context.args[0].strip()
    tag, idx, item = find_media_by_file_id(file_id)
    if tag is None:
        # Not found; try to send by assuming file_id is valid - send_document as fallback
        try:
            await safe_send_message(
                context=context,
                chat_id=update.effective_chat.id,
                document=file_id,
                caption=f"📄 File by ID: <code>{file_id}</code>",
                parse_mode=ParseMode.HTML
            )
            return
        except Exception as e:
            await update.message.reply_text(f"❌ File not found in media_data and failed to send by id: {e}")
            return

    # Build caption and send according to type
    media_type = item.get("type", "video")
    caption = build_media_caption(item.get("caption", ""), tag, str(idx), f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}", media_type)
    try:
        kwargs = {
            "context": context,
            "chat_id": update.effective_chat.id,
            "caption": caption,
            "parse_mode": ParseMode.HTML,
            "protect_content": should_protect_content(update.effective_user.id, update.effective_chat.id)
        }
        if media_type == "video":
            kwargs["video"] = item.get("file_id")
        elif media_type == "photo":
            kwargs["photo"] = item.get("file_id")
        elif media_type == "document":
            kwargs["document"] = item.get("file_id")
        elif media_type == "audio":
            kwargs["audio"] = item.get("file_id")
        elif media_type == "voice":
            kwargs["voice"] = item.get("file_id")
        elif media_type == "animation":
            kwargs["animation"] = item.get("file_id")
        elif media_type == "sticker":
            kwargs["sticker"] = item.get("file_id")
            kwargs.pop("caption", None)
        else:
            kwargs["document"] = item.get("file_id")

        await safe_send_message(**kwargs)
    except Exception as e:
        await update.message.reply_text(f"❌ Error sending file: {str(e)}")


async def reindex_favs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to reindex favorites and likes data: rebuild metadata, normalize likes, recalc counts."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return

    # Require explicit confirmation argument to proceed to avoid accidental heavy operations
    # Usage: /reindexfavs confirm
    if not context.args or (len(context.args) >= 1 and context.args[0].lower() not in ("confirm", "force")):
        await update.message.reply_text("⚠️ This will rebuild metadata and recalculate all like counts. To proceed, run: /reindexfavs confirm")
        return
    await update.message.reply_text("🔄 Reindexing favorites and normalizing like counts. This may take a while...")

    # Before state
    before_meta = len(favorites_data.get("video_metadata", {}))
    before_likes = len(favorites_data.get("video_likes", {}))
    total_favs_before = sum(len(v) if isinstance(v, dict) else (len(v) if isinstance(v, list) else 0) for v in favorites_data.get("user_favorites", {}).values())

    # Run reindex routines
    update_video_metadata_for_all_favorites()
    normalize_video_likes_counts()
    # Recalc authoritative like counts from users favorites to ensure consistency
    recalc_video_likes_from_favorites()

    # After state
    after_meta = len(favorites_data.get("video_metadata", {}))
    after_likes = len(favorites_data.get("video_likes", {}))
    total_favs_after = sum(len(v) if isinstance(v, dict) else (len(v) if isinstance(v, list) else 0) for v in favorites_data.get("user_favorites", {}).values())

    msg = (
        f"✅ Reindex completed.\n\n"
        f"📌 Video metadata: {before_meta} -> {after_meta}\n"
        f"📊 video_likes entries: {before_likes} -> {after_likes}\n"
        f"👥 Total favorites stored: {total_favs_before} -> {total_favs_after}\n"
    )
    await update.message.reply_text(msg)
    # Refresh all active Top viewers to reflect the reindex and canonical counts
    try:
        await refresh_all_active_top_viewers(context)
    except Exception:
        pass


async def refreshviewers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only: refresh all registered Top Videos viewer messages immediately."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    await update.message.reply_text("🔁 Refreshing active Top Videos viewers...")
    try:
        await refresh_all_active_top_viewers(context)
        await update.message.reply_text("✅ Refreshed active Top Videos viewers.")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to refresh viewers: {e}")


async def autoreindex_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to toggle periodic auto-reindex.
    Usage:
        /autoreindex               - show current status
        /autoreindex on [minutes]  - enable auto reindex with optional interval (minutes)
        /autoreindex off           - disable auto reindex
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return

    args = context.args
    rc = favorites_data.setdefault("reindex_config", {"auto_reindex_enabled": False, "interval_seconds": 3600})
    if not args:
        status = rc.get("auto_reindex_enabled", False)
        interval = rc.get("interval_seconds", 3600)
        await update.message.reply_text(f"Auto reindex is {'ENABLED' if status else 'DISABLED'}. Interval: {interval} seconds.")
        return

    cmd = args[0].lower()
    if cmd in ("on", "enable"):
        try:
            if len(args) > 1:
                # Allow minutes specified as argument
                mins = int(args[1])
                interval = max(30, mins * 60)
            else:
                interval = rc.get("interval_seconds", 3600)
            rc["auto_reindex_enabled"] = True
            rc["interval_seconds"] = interval
            save_favorites()
            # Start the background task if not already running
            try:
                if not reindex_task_active:
                    task = asyncio.create_task(start_background_reindex(context.application))
                    BACKGROUND_TASKS.append(task)
            except Exception:
                pass
            await update.message.reply_text(f"✅ Auto reindex ENABLED. Interval set to {interval} seconds.")
        except Exception as e:
            await update.message.reply_text(f"❌ Invalid interval: {e}")
    elif cmd in ("off", "disable"):
        rc["auto_reindex_enabled"] = False
        save_favorites()
        await update.message.reply_text("✅ Auto reindex DISABLED. Any running background task will stop at the next loop iteration.")
    else:
        await update.message.reply_text("Usage: /autoreindex on [minutes] | off | (no args = status)")


async def set_welcome_image_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to set welcome image using file_id"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    global WELCOME_IMAGE_FILE_ID
    
    if not context.args:
        # Show current status
        status = f"Current welcome image file_id: `{WELCOME_IMAGE_FILE_ID}`" if WELCOME_IMAGE_FILE_ID else "No welcome image file_id set"
        await update.message.reply_text(
            f"🖼️ <b>Welcome Image Status</b>\n\n{status}\n\n"
            f"<b>Usage:</b>\n"
            f"• <code>/setwelcomeimage &lt;file_id&gt;</code> - Set welcome image\n"
            f"• <code>/setwelcomeimage clear</code> - Clear current image\n"
            f"• Reply to a photo with <code>/setwelcomeimage</code> to use that photo",
            parse_mode=ParseMode.HTML
        )
        return
    
    if context.args[0].lower() == "clear":
        WELCOME_IMAGE_FILE_ID = None
        await update.message.reply_text("✅ Welcome image cleared. Bot will try to use local file or show text-only welcome.")
        return
    
    # Check if replying to a photo
    if update.message.reply_to_message and update.message.reply_to_message.photo:
        # Use the photo from the replied message
        photo = update.message.reply_to_message.photo[-1]  # Get highest resolution
        WELCOME_IMAGE_FILE_ID = photo.file_id
        await update.message.reply_text(f"✅ Welcome image updated using replied photo!\nFile ID: `{WELCOME_IMAGE_FILE_ID}`", parse_mode=ParseMode.MARKDOWN)
        return
    
    # Use provided file_id
    file_id = context.args[0].strip()
    if len(file_id) > 10:  # Basic validation
        WELCOME_IMAGE_FILE_ID = file_id
        await update.message.reply_text(f"✅ Welcome image file_id updated to: `{file_id}`", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text("❌ Invalid file_id provided. Please provide a valid Telegram file_id.")


async def test_welcome_image_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to test the welcome image"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    global WELCOME_IMAGE_FILE_ID
    
    test_text = (f"🧪 <b>Welcome Image Test</b>\n\n"
                 f"This is a test of your welcome image.\n"
                 f"File ID: `{WELCOME_IMAGE_FILE_ID or 'None'}`")
    
    test_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Image Works", callback_data="test_image_ok")],
        [InlineKeyboardButton("❌ No Image Shown", callback_data="test_image_fail")]
    ])
    
    photo_sent = False
    
    try:
        if WELCOME_IMAGE_FILE_ID:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=WELCOME_IMAGE_FILE_ID,
                caption=test_text,
                parse_mode=ParseMode.HTML,
                reply_markup=test_keyboard
            )
            photo_sent = True
    except Exception as e:
        await update.message.reply_text(f"❌ Error testing welcome image: {str(e)}")
        photo_sent = False
    
    if not photo_sent:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"📋 <i>(No image available)</i>\n\n{test_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=test_keyboard
        )


# =================== END ADMIN MANAGEMENT SYSTEM ===================


async def listdeleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    """List all deleted media entries with pagination in simple format"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # First, clean up any corrupted deleted media entries
    if deleted_media_storage:
        print("🧹 Running automatic cleanup before listing deleted media...")
        corrupted_count = await cleanup_deleted_media()
        if corrupted_count > 0:
            await update.message.reply_text(f"🧹 Cleaned up {corrupted_count} corrupted deleted media entries.")
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media found.")
        return
    
    # Convert to list for pagination and sorting
    all_deleted = []
    for video_key, deleted_info in deleted_media_storage.items():
        tag = deleted_info.get("tag", "unknown")
        original_pos = deleted_info.get("original_position", "?")
        data = deleted_info.get("data", {})
        media_type = data.get("type", "unknown")
        all_deleted.append((video_key, tag, original_pos, media_type))
    
    # Sort by tag first, then by original position
    all_deleted.sort(key=lambda x: (x[1], x[2] if isinstance(x[2], int) else 999))
    
    # Pagination settings
    items_per_page = 10
    total_items = len(all_deleted)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    
    if page < 0:
        page = 0
    elif page >= total_pages:
        page = total_pages - 1
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)
    current_items = all_deleted[start_idx:end_idx]
    
    # Build message
    message = f"🗑️ <b>Deleted Media (Page {page + 1}/{total_pages})</b>\n\n"
    
    for i, (video_key, tag, original_pos, media_type) in enumerate(current_items, start_idx + 1):
        # Create view link similar to your example
        view_link = f"https://t.me/bhaicharabackupbot?start=view_deleted_{video_key}"
        restore_link = f"https://t.me/bhaicharabackupbot?start=restore_{video_key}"
        
        message += f"{i}. Tag: <code>{tag}</code> | Index: <code>{original_pos}</code> | "
        message += f"�️ {media_type} | "
        message += f"<a href='{view_link}'>👁️ View</a> | "
        message += f"<a href='{restore_link}'>♻️ Restore</a>\n"
    
    # Create navigation buttons
    nav_buttons = []
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"deleted_page_{page - 1}"))
        
        # Page info button (non-clickable info)
        nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="deleted_page_info"))
        
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"deleted_page_{page + 1}"))
        
        nav_buttons.append(nav_row)
    
    # Add utility buttons
    utility_buttons = [
        InlineKeyboardButton("🧹 Cleanup", callback_data="cleanup_deleted"),
        InlineKeyboardButton("🔄 Refresh", callback_data="refresh_deleted_list")
    ]
    nav_buttons.append(utility_buttons)
    
    keyboard = InlineKeyboardMarkup(nav_buttons) if nav_buttons else None
    
    # Send the message
    await update.message.reply_html(message, reply_markup=keyboard)


async def cleanup_deleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually clean up corrupted deleted media entries"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media to clean up.")
        return
    
    # Show initial count
    initial_count = len(deleted_media_storage)
    await update.message.reply_text(f"🧹 Starting cleanup of {initial_count} deleted media entries...")
    
    # Run the cleanup
    corrupted_count = await cleanup_deleted_media()
    
    # Report results
    remaining_count = len(deleted_media_storage)
    if corrupted_count > 0:
        await update.message.reply_text(
            f"✅ Cleanup complete!\n"
            f"🗑️ Removed: {corrupted_count} corrupted entries\n"
            f"📁 Remaining: {remaining_count} valid entries"
        )
    else:
        await update.message.reply_text(f"✅ No corrupted entries found. All {remaining_count} entries are valid.")


async def autodelete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control auto-deletion settings"""
    global AUTO_DELETE_ENABLED, AUTO_DELETE_HOURS
    
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        # Show current status
        status = "🟢 ENABLED" if AUTO_DELETE_ENABLED else "🔴 DISABLED"
        await update.message.reply_text(
            f"🧹 <b>Auto-Delete Status</b>\n\n"
            f"Status: {status}\n"
            f"Delete after: {AUTO_DELETE_HOURS} hour(s)\n"
            f"Tracked messages: {len(sent_messages_tracker)}\n\n"
            f"<b>Commands:</b>\n"
            f"• <code>/autodelete on</code> - Enable auto-deletion\n"
            f"• <code>/autodelete off</code> - Disable auto-deletion\n"
            f"• <code>/autodelete hours &lt;number&gt;</code> - Set deletion time\n"
            f"• <code>/autodelete clear</code> - Clear tracking list\n"
            f"• <code>/autodelete stats</code> - Show detailed stats",
            parse_mode=ParseMode.HTML
        )
        return
    
    command = context.args[0].lower()
    
    if command == "on":
        AUTO_DELETE_ENABLED = True
        save_autodelete_config()
        await update.message.reply_text("✅ Auto-deletion ENABLED")
    
    elif command == "off":
        AUTO_DELETE_ENABLED = False
        save_autodelete_config()
        await update.message.reply_text("❌ Auto-deletion DISABLED")
    
    elif command == "hours" and len(context.args) >= 2:
        try:
            hours = float(context.args[1])
            if 0.1 <= hours <= 168:  # Between 6 minutes and 1 week
                AUTO_DELETE_HOURS = hours
                save_autodelete_config()
                await update.message.reply_text(f"⏰ Auto-deletion time set to {hours} hour(s)")
            else:
                await update.message.reply_text("❌ Hours must be between 0.1 and 168")
        except ValueError:
            await update.message.reply_text("❌ Invalid number format")
    
    elif command == "clear":
        cleared_count = len(sent_messages_tracker)
        sent_messages_tracker.clear()
        save_tracking_data()  # Save the cleared state
        await update.message.reply_text(f"🧹 Cleared {cleared_count} tracked messages")
    
    elif command == "stats":
        if not sent_messages_tracker:
            await update.message.reply_text("📊 No messages currently tracked")
            return
        
        # Group by chat
        chat_counts = {}
        oldest_time = None
        newest_time = None
        
        for key, info in sent_messages_tracker.items():
            chat_id = info["chat_id"]
            timestamp = info["timestamp"]
            
            chat_counts[chat_id] = chat_counts.get(chat_id, 0) + 1
            
            if oldest_time is None or timestamp < oldest_time:
                oldest_time = timestamp
            if newest_time is None or timestamp > newest_time:
                newest_time = timestamp
        
        stats_text = f"📊 <b>Auto-Delete Stats</b>\n\n"
        stats_text += f"Total tracked: {len(sent_messages_tracker)}\n"
        
        if oldest_time:
            stats_text += f"Oldest: {oldest_time.strftime('%H:%M:%S')}\n"
            stats_text += f"Newest: {newest_time.strftime('%H:%M:%S')}\n\n"
        
        stats_text += "<b>By Chat:</b>\n"
        for chat_id, count in list(chat_counts.items())[:10]:  # Show top 10
            stats_text += f"Chat {chat_id}: {count} messages\n"
        
        await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)
    
    elif command == "histclean":
        # Historical cleanup command with safety measures
        global HISTORICAL_CLEANUP_RUNNING
        
        if HISTORICAL_CLEANUP_RUNNING:
            await update.message.reply_text(
                "⚠️ <b>Historical cleanup already running!</b>\n\n"
                "Please wait for the current cleanup to finish before starting another one.",
                parse_mode=ParseMode.HTML
            )
            return
        
        force_mode = len(context.args) >= 2 and context.args[1].lower() == "force"
        dry_run = not force_mode
        
        # SAFETY: Disable historical cleanup for now due to issues
        await update.message.reply_text(
            "🚨 <b>Historical cleanup temporarily disabled</b>\n\n"
            "❌ The historical cleanup system has been disabled due to:\n"
            "• Multiple simultaneous runs causing conflicts\n"
            "• Risk of rate limiting with 302 users\n"
            "• Potential deletion of recent messages\n\n"
            "✅ The regular auto-deletion tracking is working fine.\n"
            "📊 Use <code>/autodelete stats</code> to see tracked messages.",
            parse_mode=ParseMode.HTML
        )
        return
    
    else:
        await update.message.reply_text("❌ Invalid command. Use /autodelete without arguments for help.")


async def notifications_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control auto-deletion notification preferences"""
    chat_id = update.effective_chat.id
    
    if not context.args:
        # Show current status
        current_setting = get_user_preference(chat_id, "auto_delete_notifications", True)
        status = "🟢 ENABLED" if current_setting else "🔴 DISABLED"
        
        # Check cooldown status
        cooldown_info = ""
        if chat_id in last_notification_time:
            time_since_last = datetime.now() - last_notification_time[chat_id]
            cooldown_remaining = NOTIFICATION_COOLDOWN_HOURS * 3600 - time_since_last.total_seconds()
            if cooldown_remaining > 0:
                cooldown_info = f"\n🔕 Cooldown: {cooldown_remaining/60:.1f} minutes remaining"
            else:
                cooldown_info = f"\n✅ Cooldown: Ready to send notifications"
        
        await update.message.reply_text(
            f"🔔 <b>Auto-Delete Notifications</b>\n\n"
            f"Status: {status}{cooldown_info}\n\n"
            f"When enabled, you'll receive a notification when media is automatically deleted.\n"
            f"⏱️ <b>Cooldown:</b> Maximum 1 notification per {NOTIFICATION_COOLDOWN_HOURS} hour(s) to avoid spam.\n\n"
            f"<b>Commands:</b>\n"
            f"• <code>/notifications on</code> - Enable notifications\n"
            f"• <code>/notifications off</code> - Disable notifications\n\n"
            f"💡 <b>Tip:</b> Use favorites (⭐) to save important videos permanently!",
            parse_mode=ParseMode.HTML
        )
        return
    
    command = context.args[0].lower()
    
    if command == "on":
        set_user_preference(chat_id, "auto_delete_notifications", True)
        await update.message.reply_text(
            "🔔 ✅ Auto-deletion notifications ENABLED\n\n"
            f"You'll now receive notifications when media is automatically deleted after {AUTO_DELETE_HOURS} hour(s).\n"
            f"⏱️ Note: Maximum 1 notification per {NOTIFICATION_COOLDOWN_HOURS} hour(s) to prevent spam."
        )
    
    elif command == "off":
        set_user_preference(chat_id, "auto_delete_notifications", False)
        await update.message.reply_text(
            "🔕 Auto-deletion notifications DISABLED\n\n"
            "You won't receive notifications when media is automatically deleted.\n"
            f"💡 Remember: Media is still auto-deleted after {AUTO_DELETE_HOURS} hour(s), but you won't be notified."
        )
    
    else:
        await update.message.reply_text("❌ Invalid command. Use /notifications without arguments for help.")


async def debug_deleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command to check deleted media vs actual media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /debugdeleted <tag>\nExample: /debugdeleted thirstthus")
        return
    
    tag = context.args[0].strip()
    
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found in media data.")
        return
    
    # Get current media count for this tag
    current_count = len(media_data[tag])
    
    # Get deleted entries for this tag
    deleted_entries = []
    for video_key, deleted_info in deleted_media_storage.items():
        if deleted_info.get("tag") == tag:
            deleted_entries.append((video_key, deleted_info))
    
    # Sort by original position
    deleted_entries.sort(key=lambda x: x[1].get("original_position", 0))
    
    message = f"🔍 <b>Debug Info for Tag: {tag}</b>\n\n"
    message += f"📊 Current media count: <code>{current_count}</code>\n"
    message += f"🗑️ Deleted entries: <code>{len(deleted_entries)}</code>\n\n"
    
    if deleted_entries:
        message += "<b>Deleted Media Details:</b>\n"
        for video_key, deleted_info in deleted_entries:
            original_pos = deleted_info.get("original_position", "?")
            deleted_date = deleted_info.get("deleted_date", "Unknown")
            data = deleted_info.get("data", {})
            media_type = data.get("type", "unknown")
            
            # Check if this would be a valid index now
            status = "✅ Valid" if original_pos == "?" or int(original_pos) <= current_count else "⚠️ Stale"
            
            message += f"  • <code>{video_key}</code>\n"
            message += f"    Original Index: {original_pos} | Type: {media_type} | {status}\n"
            message += f"    Deleted: {deleted_date}\n\n"
    else:
        message += "No deleted entries found for this tag.\n"
    
    await update.message.reply_html(message)


async def restoredeleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restore a specific deleted or revoked media by video_key"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /restoredeleted <video_key>\nExample: /restoredeleted wildwed_65")
        return
    
    video_key = context.args[0].strip()
    
    # Check if it's a deleted item in deleted_media_storage
    if video_key in deleted_media_storage:
        # Handle deleted media restoration
        deleted_info = deleted_media_storage[video_key]
        original_data = deleted_info["data"]
        original_position = deleted_info["original_position"]
        tag = deleted_info["tag"]
        
        # Ensure the tag still exists
        if tag not in media_data:
            media_data[tag] = []
        
        # Insert the media back at its original position
        if original_position <= len(media_data[tag]):
            media_data[tag].insert(original_position, original_data)
        else:
            media_data[tag].append(original_data)
        
        # Remove from deleted storage
        del deleted_media_storage[video_key]
        
        save_media()
        save_deleted_media()
        update_random_state()
        
        await update.message.reply_text(f"✅ Deleted media <code>{video_key}</code> restored successfully!", parse_mode=ParseMode.HTML)
        
    else:
        # Check if it's a revoked item in media_data
        try:
            tag, idx_str = video_key.rsplit("_", 1)
            idx = int(idx_str)
            
            if tag in media_data and idx < len(media_data[tag]):
                media_item = media_data[tag][idx]
                
                if isinstance(media_item, dict) and media_item.get("revoked"):
                    # Remove the revoked flag to restore the media
                    media_item.pop("revoked", None)
                    
                    save_media()
                    update_random_state()
                    
                    await update.message.reply_text(f"✅ Revoked media <code>{video_key}</code> restored successfully!", parse_mode=ParseMode.HTML)
                    return
            
            await update.message.reply_text(f"❌ No deleted or revoked media found with key: <code>{video_key}</code>", parse_mode=ParseMode.HTML)
            return
            
        except (ValueError, IndexError):
            await update.message.reply_text(f"❌ Invalid media key format: <code>{video_key}</code>", parse_mode=ParseMode.HTML)
            return


async def cleardeleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Permanently remove all deleted media from storage"""


async def cleardeleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear all deleted media permanently (cannot be undone)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media to clear.")
        return
    
    deleted_count = len(deleted_media_storage)
    
    # Create confirmation buttons
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Clear All", callback_data="confirm_clear_deleted"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_clear_deleted")
        ]
    ])
    
    await update.message.reply_text(
        f"⚠️ <b>Permanent Deletion Warning</b>\n\n"
        f"This will permanently remove all {deleted_count} deleted media entries.\n"
        f"This action cannot be undone!\n\n"
        f"Are you sure you want to continue?",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )


async def restoreall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restore all deleted media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media to restore.")
        return
    
    restored_count = 0
    
    # Sort by original position to maintain order
    sorted_items = sorted(
        deleted_media_storage.items(),
        key=lambda x: (x[1].get("tag", ""), x[1].get("original_position", 0))
    )
    
    for video_key, deleted_info in sorted_items:
        original_data = deleted_info["data"]
        original_position = deleted_info["original_position"]
        tag = deleted_info["tag"]
        
        # Ensure the tag still exists
        if tag not in media_data:
            media_data[tag] = []
        
        # Insert the media back at its original position
        if original_position <= len(media_data[tag]):
            media_data[tag].insert(original_position, original_data)
        else:
            media_data[tag].append(original_data)
        
        restored_count += 1
    
    # Clear all deleted media
    deleted_media_storage.clear()
    
    save_media()
    save_deleted_media()
    update_random_state()
    
    await update.message.reply_text(
        f"♻️ <b>All Media Restored Successfully!</b>\n\n"
        f"📊 Restored: <code>{restored_count}</code> items\n"
        f"✅ All deleted media has been restored to original positions.",
        parse_mode=ParseMode.HTML
    )


async def deletedstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show statistics about deleted media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media found.")
        return
    
    # Analyze deleted media
    by_tag = {}
    by_type = {}
    total_deleted = len(deleted_media_storage)
    
    for video_key, deleted_info in deleted_media_storage.items():
        tag = deleted_info.get("tag", "unknown")
        data = deleted_info.get("data", {})
        media_type = data.get("type", "unknown")
        
        by_tag[tag] = by_tag.get(tag, 0) + 1
        by_type[media_type] = by_type.get(media_type, 0) + 1
    
    message = "<b>📊 Deleted Media Statistics</b>\n\n"
    message += f"🗑️ <b>Total Deleted:</b> {total_deleted} items\n\n"
    
    message += "<b>📁 By Tag:</b>\n"
    for tag, count in sorted(by_tag.items(), key=lambda x: x[1], reverse=True):
        message += f"  • {tag}: {count} items\n"
    
    message += "\n<b>📄 By Type:</b>\n"
    for media_type, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
        message += f"  • {media_type}: {count} items\n"
    
    await update.message.reply_html(message)


async def process_pending_updates(application):
    """Process all pending updates that were missed during downtime"""
    try:
        print("🔄 Checking for pending updates...")
        
        # Use shorter timeout for startup check
        updates = await application.bot.get_updates(
            offset=last_update_offset + 1 if last_update_offset > 0 else None,
            limit=50,  # Reduced limit for faster processing
            timeout=5   # Shorter timeout
        )
        
        if updates:
            print(f"📨 Found {len(updates)} pending updates to process")
            processed_count = 0
            
            for update in updates:
                try:
                    # Process the update through the normal handler
                    await application.process_update(update)
                    
                    # Save the offset after each successful update
                    save_update_offset(update.update_id)
                    processed_count += 1
                    
                    # Show progress for large batches
                    if processed_count % 5 == 0:
                        print(f"📊 Processed {processed_count}/{len(updates)} updates...")
                        
                    # Small delay to prevent overwhelming the connection pool
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    print(f"❌ Error processing update {update.update_id}: {e}")
                    # Still save offset to avoid reprocessing failed updates
                    save_update_offset(update.update_id)
            
            print(f"✅ Processed {processed_count} pending updates up to offset: {last_update_offset}")
        else:
            print("✅ No pending updates found")
            
    except Exception as e:
        print(f"❌ Error checking pending updates: {e}")
        # Don't fail startup if pending updates check fails
        print("🔄 Bot will continue with normal operation")

async def force_check_updates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to manually check for pending updates"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Add a delay to avoid connection pool conflicts
    await asyncio.sleep(1)
    
    await update.message.reply_text("🔄 Manually checking for pending updates...")
    
    try:
        # Get the current application instance
        application = context.application
        
        # Use a more conservative approach for manual checks
        updates = await application.bot.get_updates(
            offset=last_update_offset + 1 if last_update_offset > 0 else None,
            limit=20,  # Smaller batch for manual checks
            timeout=3   # Very short timeout for manual checks
        )
        
        if updates:
            await update.message.reply_text(
                f"📨 Found {len(updates)} pending updates!\n"
                f"🔄 Processing them now (this may take a moment)..."
            )
            
            processed = 0
            failed = 0
            
            for pending_update in updates:
                try:
                    await application.process_update(pending_update)
                    save_update_offset(pending_update.update_id)
                    processed += 1
                    
                    # Small delay between processing updates
                    await asyncio.sleep(0.2)
                    
                except Exception as e:
                    print(f"❌ Error processing update {pending_update.update_id}: {e}")
                    save_update_offset(pending_update.update_id)
                    failed += 1
            
            result_msg = f"✅ Processing complete!\n📊 Processed: {processed}"
            if failed > 0:
                result_msg += f"\n⚠️ Failed: {failed}"
            result_msg += f"\n📋 Current offset: {last_update_offset}"
            
            await update.message.reply_text(result_msg)
            
        else:
            await update.message.reply_text(
                f"✅ No pending updates found.\n"
                f"📊 Current offset: {last_update_offset}"
            )
            
    except Exception as e:
        error_msg = str(e)
        if "Pool timeout" in error_msg:
            await update.message.reply_text(
                "⚠️ Connection pool busy - bot is processing many requests.\n"
                "🔄 Try again in a few seconds, or wait for automatic processing."
            )
        elif "timeout" in error_msg.lower():
            await update.message.reply_text(
                "⏱️ Request timed out - this is normal during high activity.\n"
                "✅ The bot continues processing updates automatically."
            )
        else:
            await update.message.reply_text(f"❌ Error checking updates: {error_msg}")

# Set bot commands for the menu (shown when typing '/')
# Keep concise but comprehensive. Admin-only commands will still show; non-admins will get a friendly denial.
commands = [
    # Core user commands
    BotCommand("start", "🏠 Start the bot"),
    BotCommand("help", "❓ Help and command list"),
    BotCommand("favorites", "❤️ View your favorites"),
    BotCommand("top", "� Browse Top Videos"),
    BotCommand("jump", "🔢 Jump to rank"),

    # Content management
    BotCommand("upload", "⬆️ Upload media (reply)"),
    BotCommand("listvideos", "🗂️ List tags"),
    BotCommand("listvideo", "🗂️ List a tag"),
    BotCommand("view", "👁️ View tag index"),
    BotCommand("cview", "🧼 Clean view (visible only)"),
    BotCommand("remove", "🗑️ Remove tag index"),
    BotCommand("free", "🆓 Make tag free"),
    BotCommand("unfree", "🚫 Remove free access"),
    BotCommand("listfree", "🧾 List free tags"),

    # Link systems (separate stores)
    BotCommand("pass", "🎲 Random access (RANDOM MEDIA)"),
    BotCommand("revoke", "🛑 Revoke random access"),
    BotCommand("passlink", "🔗 Create shareable link"),
    BotCommand("revokelink", "❌ Disable shareable link"),
    BotCommand("activelinks", "📋 Active shareable links"),
    BotCommand("passlinks", "📋 Active random access links"),
    BotCommand("listactive", "📋 All active links"),

    # Cleanup and recovery
    BotCommand("listdeleted", "�️ View deleted media"),
    BotCommand("listrevoked", "🛑 View revoked media"),
    BotCommand("listremoved", "🧹 View removed media"),
    BotCommand("restoredeleted", "♻️ Restore a deleted item"),
    BotCommand("restoreall", "♻️ Restore all deleted"),
    BotCommand("cleardeleted", "🧯 Purge deleted storage"),
    BotCommand("cleanupdeleted", "🧼 Fix corrupted deleted"),

    # Analytics and users
    BotCommand("userstats", "👥 User statistics"),
    BotCommand("userinfo", "👤 User info"),
    BotCommand("topusers", "👑 Top users"),
    BotCommand("videostats", "� Video stats"),
    BotCommand("topvideos", "🏆 Top videos"),

    # Broadcasting
    BotCommand("broadcast", "📢 Broadcast"),
    BotCommand("dbroadcast", "🕒 Auto-delete broadcast"),
    BotCommand("pbroadcast", "� Pinned broadcast"),
    BotCommand("sbroadcast", "🔕 Silent broadcast"),
    BotCommand("fbroadcast", "↪️ Forward broadcast"),
    BotCommand("bstats", "📊 Broadcast stats"),

    # Protection and auto-delete
    BotCommand("protection", "🛡️ Protection status"),
    BotCommand("protectionon", "🛡️ Enable protection"),
    BotCommand("protectionoff", "🛡️ Disable protection"),
    BotCommand("autodelete", "🧹 Auto-delete controls"),
    BotCommand("notifications", "🔔 Deletion notifications"),

    # Batch tools
    BotCommand("custom_batch", "📦 Start custom batch"),
    BotCommand("stop_batch", "⏹️ Stop batch"),
    BotCommand("cancel_batch", "❌ Cancel batch"),
    BotCommand("batch_status", "ℹ️ Batch status"),

    # AI batch (on-demand)
    BotCommand("ai_batch", "🤖 Start AI batch"),
    BotCommand("stop_ai_batch", "⏹️ Stop AI batch"),
    BotCommand("cancel_ai_batch", "❌ Cancel AI batch"),
    BotCommand("ai_batch_status", "ℹ️ AI batch status"),

    # Backups (local + Telegram)
    BotCommand("togglebackup", "🔄 Toggle local backups"),
    BotCommand("backup", "💾 Create backup"),
    BotCommand("listbackups", "📋 List backups"),
    BotCommand("restore", "♻️ Restore backup"),
    BotCommand("backupstats", "📊 Backup stats"),
    BotCommand("deletebackup", "🗑️ Delete backup"),
    BotCommand("telegrambackup", "📤 Backup to Telegram"),
    BotCommand("autobackup", "🗓️ Auto-backup control"),

    # Misc tools
    BotCommand("set_global_caption", "📝 Set global caption"),
    BotCommand("getfileid", "🆔 Get file_id"),
    BotCommand("getfile", "📂 Send file by file_id"),
    BotCommand("reindexfavs", "🔧 Reindex favorites (admin)"),
    BotCommand("reindex", "🔧 Reindex favorites (admin) (alias)"),
    BotCommand("autoreindex", "🔁 Toggle auto reindex (admin)"),
    BotCommand("setwelcomeimage", "🖼️ Set welcome image"),
    BotCommand("testwelcomeimage", "�️ Test welcome image"),
    BotCommand("add_admin", "➕ Add admin"),
    BotCommand("remove_admin", "➖ Remove admin"),
    BotCommand("list_admins", "📃 List admins"),
    BotCommand("checkupdates", "🔍 Check pending updates"),
    BotCommand("move", "📦 Batch move media between tags"),
    BotCommand("add", "➕ Batch copy media between tags")
]

# Hide local-backup related commands if feature is disabled
if not LOCAL_BACKUPS_ENABLED:
    commands = [c for c in commands if getattr(c, 'command', '') not in {"backup", "listbackups", "restore", "backupstats", "deletebackup", "togglebackup"}]

async def show_push_tag_selector(query, context, video_key):
    """Show tag selection interface for PUSH system - displays all tags with green checkmarks for existing media"""
    if not is_admin(query.from_user.id):
        await query.answer("❌ Admin access required", show_alert=True)
        return
    
    if "_" not in video_key:
        await query.answer("❌ Invalid video key", show_alert=True)
        return
    
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await query.answer("❌ Invalid index", show_alert=True)
        return
    
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        await query.answer("❌ Media not found", show_alert=True)
        return
    
    # Determine the target media for this selector session using a stable identity (file_id)
    chat_id = query.message.chat_id
    message_id = query.message.message_id
    session_key = (chat_id, message_id)

    # Establish or reuse the session target (file_id + original item snapshot)
    if session_key in push_session_targets:
        target_info = push_session_targets[session_key]
        target_file_id = target_info.get("file_id")
        target_item = target_info.get("item")
    else:
        current_media = media_data[tag][idx]
        # Best-effort to extract file_id and store snapshot for later adds
        target_file_id = current_media.get("file_id") if isinstance(current_media, dict) else None
        target_item = deepcopy(current_media) if isinstance(current_media, dict) else current_media
        push_session_targets[session_key] = {"file_id": target_file_id, "item": target_item}

    # Get all available tags
    all_tags = list(media_data.keys())
    all_tags.sort()

    # Find which tags contain this media (by file_id when available)
    existing_tags = []
    for check_tag in all_tags:
        if check_tag not in media_data:
            continue
        for media_item in media_data[check_tag]:
            if isinstance(media_item, dict) and target_file_id:
                if media_item.get("file_id") == target_file_id:
                    existing_tags.append(check_tag)
                    break
            else:
                # Fallback: direct object comparison if no file_id available
                if media_item == target_item:
                    existing_tags.append(check_tag)
                    break
    
    # Build the message
    message = (
        f"🎯 <b>PUSH Media Manager</b>\n\n"
        f"📁 Current Tag (source): <code>{tag}</code>\n"
        f"📍 Index: <code>{idx}</code>\n\n"
        f"💡 Tap a tag to toggle membership:\n"
        f"✅ Green = in tag (tap to remove)\n"
        f"➕ Add = not in tag (tap to add)\n\n"
        f"<b>Available Tags:</b>\n"
    )
    
    # Create buttons for each tag
    keyboard = []
    row = []
    
    for tag_name in all_tags:
        # Determine button text and callback
        if tag_name in existing_tags:
            # Media exists in this tag - show as removable
            button_text = f"✅ {tag_name}"
            callback_data = f"pr_{video_key}_{tag_name}"
        else:
            # Media doesn't exist in this tag - show as addable
            button_text = f"➕ {tag_name}"
            callback_data = f"pa_{video_key}_{tag_name}"
        
        row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
        
        # Create new row every 2 buttons for better layout
        if len(row) >= 2:
            keyboard.append(row)
            row = []
    
    # Add remaining buttons
    if row:
        keyboard.append(row)

    # Add control buttons: Back (restore original controls) above Close (delete message)
    keyboard.insert(0, [InlineKeyboardButton("⬅️ Back", callback_data=f"pback_{video_key}")])
    keyboard.append([InlineKeyboardButton("❌ Close", callback_data=f"pclose_{video_key}")])

    markup = InlineKeyboardMarkup(keyboard)

    # Before switching to the selector UI, cache the current inline keyboard so Back can restore it
    try:
        cache_key = (chat_id, message_id)
        if cache_key not in push_prev_keyboards:
            if query.message and query.message.reply_markup and query.message.reply_markup.inline_keyboard:
                # Deep copy to avoid accidental mutation while user interacts with selector
                push_prev_keyboards[cache_key] = deepcopy(query.message.reply_markup.inline_keyboard)
            else:
                push_prev_keyboards[cache_key] = None
    except Exception as _cache_err:
        # Non-fatal: if caching fails we'll just fall back to minimal controls on Back
        logger.debug(f"PUSH cache prev keyboard failed: {_cache_err}")

    # Edit only the inline keyboard to keep media/caption intact
    try:
        await query.edit_message_reply_markup(reply_markup=markup)
    except Exception as e:
        print(f"ERROR updating PUSH selector markup: {e}")
        await query.answer(f"❌ Error: {str(e)}", show_alert=True)


async def handle_push_tag_action(query, context, action, tag_name, video_key):
    """Handle adding or removing media from tags in PUSH system"""
    if not is_admin(query.from_user.id):
        await query.answer("❌ Admin access required", show_alert=True)
        return
    
    if "_" not in video_key:
        await query.answer("❌ Invalid video key", show_alert=True)
        return
    
    source_tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await query.answer("❌ Invalid index", show_alert=True)
        return

    # Resolve the stable target from session (preferred), with fallback to original tag/index
    chat_id = query.message.chat_id
    message_id = query.message.message_id
    session_key = (chat_id, message_id)
    target_info = push_session_targets.get(session_key)

    # Fallback resolution in case session isn't set yet
    fallback_item = None
    if source_tag in media_data and 0 <= idx < len(media_data[source_tag]):
        fallback_item = media_data[source_tag][idx]

    if target_info and target_info.get("file_id"):
        target_file_id = target_info["file_id"]
        target_item = target_info.get("item", fallback_item)
    else:
        # No session info; fall back to the current item (may be unstable if indices shift)
        target_item = fallback_item
        target_file_id = target_item.get("file_id") if isinstance(target_item, dict) else None
    
    if action == "add":
        # Add media to the tag
        if tag_name not in media_data:
            media_data[tag_name] = []
        
        # Check if media already exists in target tag
        already_exists = False
        for existing_item in media_data[tag_name]:
            if isinstance(existing_item, dict) and target_file_id:
                if existing_item.get("file_id") == target_file_id:
                    already_exists = True
                    break
            else:
                if existing_item == target_item:
                    already_exists = True
                    break
        
        if not already_exists:
            # Prefer adding the exact stored item snapshot to preserve type and structure
            item_to_add = deepcopy(target_item) if isinstance(target_item, dict) else target_item
            # Record the index before appending for index-stable undo
            new_index = len(media_data[tag_name])
            media_data[tag_name].append(item_to_add)
            save_media()
            update_random_state()
            # Record change for status/undo
            rec_file_id = item_to_add.get("file_id") if isinstance(item_to_add, dict) else target_file_id
            record_push_change(rec_file_id, item_to_add, tag_name, "add", index=new_index)
            await query.answer(f"✅ Added to {tag_name}", show_alert=True)
        else:
            await query.answer(f"⚠️ Already exists in {tag_name}", show_alert=True)
    
    elif action == "remove":
        # Remove media from the tag WITHOUT shrinking the list: replace matched item with a tombstone
        if tag_name in media_data and isinstance(media_data[tag_name], list):
            found_index = None
            for i, it in enumerate(media_data[tag_name]):
                if isinstance(it, dict) and target_file_id:
                    if it.get("file_id") == target_file_id:
                        found_index = i
                        break
                else:
                    if it == target_item:
                        found_index = i
                        break

            if found_index is not None:
                original_item = media_data[tag_name][found_index]
                tomb = {"deleted": True, "data": original_item}
                if isinstance(original_item, dict) and "type" in original_item:
                    tomb["type"] = original_item.get("type")
                media_data[tag_name][found_index] = tomb

                # Record in deleted storage so it shows in /listdeleted
                video_key = f"{tag_name}_{found_index}"
                deleted_media_storage[video_key] = {
                    "data": original_item,
                    "original_position": found_index,
                    "tag": tag_name,
                    "deleted_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                save_media(); save_deleted_media(); update_random_state()
                # Record change for status/undo using session snapshot
                rec_file_id = target_file_id
                record_push_change(rec_file_id, target_item, tag_name, "remove", index=found_index)
                await query.answer(f"✅ Removed from {tag_name}", show_alert=True)
            else:
                await query.answer(f"⚠️ Not found in {tag_name}", show_alert=True)
        else:
            await query.answer(f"⚠️ Tag {tag_name} not found", show_alert=True)
    
    # Refresh the PUSH interface in-place (update buttons without closing)
    await show_push_tag_selector(query, context, video_key)


def build_push_status(per_page: int = 10, page: int = 1):
    """Build a refined, paginated PUSH status (persistent, clean arrow-based formatting).

    - Skips undone entries
    - Coalesces add/remove pairs into a single Move
    - Format: lines like "(moved) from tag | idx --> tag | idx | 👁️ View | ♻️ Undo" as HTML links
    - View deep-links open the media; Undo deep-links revert the change
    """
    # Collect up to MAX_LOG recent ids (already bounded when recording)
    if not push_changes_order:
        return ("📋 No recent PUSH changes.", None)

    # Helper: list current tags for a file id
    def current_tags_for(fid: str):
        if not fid:
            return []
        tags = []
        for tname, items in media_data.items():
            for it in items if isinstance(items, list) else []:
                if isinstance(it, dict) and it.get("file_id") == fid:
                    tags.append(tname)
                    break
        return sorted(tags)

    # Build flattened list of display entries (newest first)
    recent_ids = list(reversed(push_changes_order))
    entries = {cid: push_changes.get(cid) for cid in recent_ids}
    consumed = set()
    display = []

    for i, cid in enumerate(recent_ids):
        if cid in consumed:
            continue
        entry = entries.get(cid)
        if not entry or entry.get("undone"):
            continue
        file_id = entry.get("file_id") or ""
        ts = entry.get("timestamp")
        action = entry.get("action")
        tag = entry.get("tag")
        idx = entry.get("index")

        # Try to find counterpart for move
        pair_cid = None
        pair_entry = None
        for j in range(i + 1, len(recent_ids)):
            other_cid = recent_ids[j]
            if other_cid in consumed:
                continue
            oe = entries.get(other_cid)
            if not oe or oe.get("undone"):
                continue
            if (oe.get("file_id") == file_id) and (oe.get("action") != action) and (oe.get("tag") != tag):
                pair_cid = other_cid
                pair_entry = oe
                break

        if pair_entry:
            if action == "add":
                src_tag = pair_entry.get("tag"); src_idx = pair_entry.get("index")
                dst_tag = tag; dst_idx = idx
                ts_show = ts
                first_id, second_id = pair_cid, cid
            else:
                src_tag = tag; src_idx = idx
                dst_tag = pair_entry.get("tag"); dst_idx = pair_entry.get("index")
                ts_show = pair_entry.get("timestamp")
                first_id, second_id = cid, pair_cid

            display.append({
                "kind": "move",
                "file_id": file_id,
                "src_tag": src_tag, "src_idx": src_idx,
                "dst_tag": dst_tag, "dst_idx": dst_idx,
                "ts": ts_show,
                "first_id": first_id, "second_id": second_id,
                "tags_now": current_tags_for(file_id)
            })
            consumed.add(cid); consumed.add(pair_cid)
        else:
            display.append({
                "kind": "single",
                "file_id": file_id,
                "action": action,
                "tag": tag,
                "idx": idx,
                "ts": ts,
                "cid": cid,
                "tags_now": current_tags_for(file_id)
            })

    # Pagination
    per_page = max(1, min(50, per_page))
    total = len(display)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(total_pages, page))
    start = (page - 1) * per_page
    end = start + per_page
    page_items = display[start:end]

    # Build text in the requested format
    lines = [f"📦 <b>PUSH Activity</b> (Page {page}/{total_pages})", ""]

    for i, item in enumerate(page_items, 1):
        if item["kind"] == "move":
            # Preferred view link points at destination slot if known
            dst_idx = item.get('dst_idx')
            if dst_idx is not None:
                view_param = f"{item['dst_tag']}_{dst_idx}_{dst_idx}"
            else:
                view_param = item['dst_tag']
            view_link = f"https://t.me/{BOT_USERNAME}?start={view_param}"
            undo_link = f"https://t.me/{BOT_USERNAME}?start=pushundo_move_{item['first_id']}_{item['second_id']}"

            src_idx_txt = item.get('src_idx', '?')
            dst_idx_txt = item.get('dst_idx', '?')
            line = (
                f"{i}. ↔️ <code>{item['src_tag']}</code> | <code>{src_idx_txt}</code> "
                f"--> <code>{item['dst_tag']}</code> | <code>{dst_idx_txt}</code> "
                f"| <a href='{view_link}'>👁️ View</a> | <a href='{undo_link}'>♻️ Undo</a>"
            )
            lines.append(line)
        else:
            # Single add/remove - find origin for added items
            idx_val = item.get('idx')
            file_id = item.get('file_id')
            
            if item["action"] == "add":
                # Try to find where this file_id exists in other tags (original location)
                origin_tag = None
                origin_idx = None
                for tname, items in media_data.items():
                    if tname == item['tag']:
                        continue  # Skip the destination tag
                    if not isinstance(items, list):
                        continue
                    for ii, it in enumerate(items):
                        if isinstance(it, dict) and it.get("file_id") == file_id:
                            origin_tag = tname
                            origin_idx = ii
                            break
                    if origin_tag:
                        break
                
                if idx_val is not None:
                    view_param = f"{item['tag']}_{idx_val}_{idx_val}"
                else:
                    # Fallback: try to find current slot for this file_id
                    found = None
                    for tname, items in media_data.items():
                        if not isinstance(items, list):
                            continue
                        for ii, it in enumerate(items):
                            if isinstance(it, dict) and it.get("file_id") == file_id:
                                found = (tname, ii)
                                break
                        if found:
                            break
                    if found:
                        view_param = f"{found[0]}_{found[1]}_{found[1]}"
                    else:
                        view_param = item['tag']
                view_link = f"https://t.me/{BOT_USERNAME}?start={view_param}"
                undo_link = f"https://t.me/{BOT_USERNAME}?start=pushundo_{item['cid']}"
                
                idx_txt = item.get('idx', '?')
                if origin_tag and origin_idx is not None:
                    # Show with origin: "➕ to tag | idx ← origin_tag | origin_idx" (using ← instead of <--)
                    line = (
                        f"{i}. ➕ to <code>{item['tag']}</code> | <code>{idx_txt}</code> "
                        f"← <code>{origin_tag}</code> | <code>{origin_idx}</code> "
                        f"| <a href='{view_link}'>👁️ View</a> | <a href='{undo_link}'>♻️ Undo</a>"
                    )
                else:
                    # No origin found, just show added to
                    line = (
                        f"{i}. ➕ to <code>{item['tag']}</code> | <code>{idx_txt}</code> "
                        f"| <a href='{view_link}'>👁️ View</a> | <a href='{undo_link}'>♻️ Undo</a>"
                    )
            else:
                # Removed items: link to special deleted viewer if we know the slot
                if idx_val is not None:
                    video_key = f"{item['tag']}_{idx_val}"
                    view_link = f"https://t.me/{BOT_USERNAME}?start=view_deleted_{video_key}"
                else:
                    view_link = f"https://t.me/{BOT_USERNAME}"
                undo_link = f"https://t.me/{BOT_USERNAME}?start=pushundo_{item['cid']}"
                
                idx_txt = item.get('idx', '?')
                line = (
                    f"{i}. ➖ from <code>{item['tag']}</code> | <code>{idx_txt}</code> "
                    f"| <a href='{view_link}'>👁️ View</a> | <a href='{undo_link}'>♻️ Undo</a>"
                )
            lines.append(line)

    lines.append("")
    lines.append(f"📊 Showing {len(page_items)} of {total} entries")

    # Only bottom nav and utilities (no per-entry inline keyboard rows)
    keyboard = []

    # Pagination nav
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"pushst_page_{page-1}_{per_page}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="pushst_noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"pushst_page_{page+1}_{per_page}"))
    if nav:
        keyboard.append(nav)

    # Utilities
    keyboard.append([
        InlineKeyboardButton("🔄 Refresh", callback_data=f"push_refresh_{page}_{per_page}"),
        InlineKeyboardButton("🗑️ Clear List", callback_data="push_clear")
    ])

    return ("\n".join(lines).strip(), InlineKeyboardMarkup(keyboard))


async def push_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show recent PUSH operations with undo buttons."""
    if not is_admin(update.effective_user.id):
        return

    # Optional args: per_page [page]
    per_page = 10
    page = 1
    if context.args:
        try:
            per_page = int(context.args[0])
            if len(context.args) > 1:
                page = int(context.args[1])
        except Exception:
            pass

    text, reply_markup = build_push_status(per_page, page)
    await update.message.reply_html(text, reply_markup=reply_markup)


async def batch_move_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Move media items from tag1[index1-index2] to tag2.
    Usage: /move <source_tag> <start_index> <end_index> <dest_tag>
    Removes items from source and adds to destination (maintains PUSH tracking).
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required")
        return

    if len(context.args) != 4:
        await update.message.reply_text(
            "📝 <b>Usage:</b> <code>/move &lt;source_tag&gt; &lt;start_index&gt; &lt;end_index&gt; &lt;dest_tag&gt;</code>\n\n"
            "📌 <b>Example:</b>\n"
            "<code>/move tag1 0 10 tag2</code>\n"
            "Moves items 0-10 from tag1 to tag2",
            parse_mode=ParseMode.HTML
        )
        return

    source_tag = context.args[0].strip().lower()
    dest_tag = context.args[3].strip().lower()

    # Parse indices
    try:
        start_idx = int(context.args[1])
        end_idx = int(context.args[2])
    except ValueError:
        await update.message.reply_text("❌ Start and end indices must be numbers")
        return

    # Validate source tag
    if source_tag not in media_data or not isinstance(media_data[source_tag], list):
        await update.message.reply_text(f"❌ Source tag '{source_tag}' not found")
        return

    source_items = media_data[source_tag]
    if start_idx < 0 or end_idx >= len(source_items) or start_idx > end_idx:
        await update.message.reply_text(
            f"❌ Invalid range. Tag '{source_tag}' has indices 0-{len(source_items)-1}"
        )
        return

    # Initialize destination tag if needed
    if dest_tag not in media_data:
        media_data[dest_tag] = []

    # Batch move operation
    moved_count = 0
    skipped_count = 0
    error_count = 0

    progress_msg = await update.message.reply_text(
        f"🔄 Moving {end_idx - start_idx + 1} items from '{source_tag}' to '{dest_tag}'...",
        parse_mode=ParseMode.HTML
    )

    for i in range(start_idx, end_idx + 1):
        if i >= len(source_items):
            break

        item = source_items[i]
        
        # Skip deleted/invalid items
        if isinstance(item, dict) and item.get("deleted"):
            skipped_count += 1
            continue

        if not isinstance(item, dict) or not item.get("file_id"):
            skipped_count += 1
            continue

        file_id = item.get("file_id")

        # Check if already exists in destination
        already_exists = False
        for existing_item in media_data[dest_tag]:
            if isinstance(existing_item, dict) and existing_item.get("file_id") == file_id:
                already_exists = True
                break

        if already_exists:
            skipped_count += 1
            continue

        try:
            # Add to destination (record at new index)
            new_index = len(media_data[dest_tag])
            item_copy = deepcopy(item)
            media_data[dest_tag].append(item_copy)
            record_push_change(file_id, item_copy, dest_tag, "add", index=new_index)

            # Mark as deleted in source (tombstone to preserve indices)
            tomb = {"deleted": True, "data": item}
            if "type" in item:
                tomb["type"] = item.get("type")
            media_data[source_tag][i] = tomb

            # Track in deleted storage
            video_key = f"{source_tag}_{i}"
            deleted_media_storage[video_key] = {
                "data": item,
                "original_position": i,
                "tag": source_tag,
                "deleted_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            record_push_change(file_id, item, source_tag, "remove", index=i)
            moved_count += 1

        except Exception as e:
            print(f"ERROR moving item {i} from {source_tag}: {e}")
            error_count += 1

    # Save all changes
    save_media()
    save_deleted_media()
    update_random_state()

    # Report results
    await progress_msg.edit_text(
        f"✅ <b>Move Operation Complete</b>\n\n"
        f"📦 Moved: {moved_count} items\n"
        f"⏭️ Skipped: {skipped_count} items (duplicates/invalid)\n"
        f"❌ Errors: {error_count}\n\n"
        f"📁 From: <code>{source_tag}</code> [{start_idx}-{end_idx}]\n"
        f"📁 To: <code>{dest_tag}</code>\n\n"
        f"💡 Use <code>/pushst</code> to see tracked changes and undo if needed",
        parse_mode=ParseMode.HTML
    )


async def batch_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add (copy) media items from tag1[index1-index2] to tag2.
    Usage: /add <source_tag> <start_index> <end_index> <dest_tag>
    Copies items without removing from source (maintains PUSH tracking).
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required")
        return

    if len(context.args) != 4:
        await update.message.reply_text(
            "📝 <b>Usage:</b> <code>/add &lt;source_tag&gt; &lt;start_index&gt; &lt;end_index&gt; &lt;dest_tag&gt;</code>\n\n"
            "📌 <b>Example:</b>\n"
            "<code>/add tag1 0 10 tag2</code>\n"
            "Copies items 0-10 from tag1 to tag2 (keeps originals)",
            parse_mode=ParseMode.HTML
        )
        return

    source_tag = context.args[0].strip().lower()
    dest_tag = context.args[3].strip().lower()

    # Parse indices
    try:
        start_idx = int(context.args[1])
        end_idx = int(context.args[2])
    except ValueError:
        await update.message.reply_text("❌ Start and end indices must be numbers")
        return

    # Validate source tag
    if source_tag not in media_data or not isinstance(media_data[source_tag], list):
        await update.message.reply_text(f"❌ Source tag '{source_tag}' not found")
        return

    source_items = media_data[source_tag]
    if start_idx < 0 or end_idx >= len(source_items) or start_idx > end_idx:
        await update.message.reply_text(
            f"❌ Invalid range. Tag '{source_tag}' has indices 0-{len(source_items)-1}"
        )
        return

    # Initialize destination tag if needed
    if dest_tag not in media_data:
        media_data[dest_tag] = []

    # Batch add operation
    added_count = 0
    skipped_count = 0
    error_count = 0

    progress_msg = await update.message.reply_text(
        f"🔄 Adding {end_idx - start_idx + 1} items from '{source_tag}' to '{dest_tag}'...",
        parse_mode=ParseMode.HTML
    )

    for i in range(start_idx, end_idx + 1):
        if i >= len(source_items):
            break

        item = source_items[i]
        
        # Skip deleted/invalid items
        if isinstance(item, dict) and item.get("deleted"):
            skipped_count += 1
            continue

        if not isinstance(item, dict) or not item.get("file_id"):
            skipped_count += 1
            continue

        file_id = item.get("file_id")

        # Check if already exists in destination
        already_exists = False
        for existing_item in media_data[dest_tag]:
            if isinstance(existing_item, dict) and existing_item.get("file_id") == file_id:
                already_exists = True
                break

        if already_exists:
            skipped_count += 1
            continue

        try:
            # Add to destination (keep source intact)
            new_index = len(media_data[dest_tag])
            item_copy = deepcopy(item)
            media_data[dest_tag].append(item_copy)
            record_push_change(file_id, item_copy, dest_tag, "add", index=new_index)
            added_count += 1

        except Exception as e:
            print(f"ERROR adding item {i} from {source_tag}: {e}")
            error_count += 1

    # Save all changes
    save_media()
    update_random_state()

    # Report results
    await progress_msg.edit_text(
        f"✅ <b>Add Operation Complete</b>\n\n"
        f"➕ Added: {added_count} items\n"
        f"⏭️ Skipped: {skipped_count} items (duplicates/invalid)\n"
        f"❌ Errors: {error_count}\n\n"
        f"📁 From: <code>{source_tag}</code> [{start_idx}-{end_idx}]\n"
        f"📁 To: <code>{dest_tag}</code>\n\n"
        f"💡 Use <code>/pushst</code> to see tracked changes and undo if needed",
        parse_mode=ParseMode.HTML
    )


async def protection_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enable protection for media content - admin only"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return
    
    global protection_enabled
    protection_enabled = True
    save_protection_settings()
    
    await update.message.reply_text(
        "✅ Protection enabled!\n\n"
        "🔒 All media content will now be protected from saving and forwarding.\n"
        "👑 Note: Admins are always exempt from protection."
    )

async def protection_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disable protection for media content - admin only"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return
    
    global protection_enabled
    protection_enabled = False
    save_protection_settings()
    
    await update.message.reply_text(
        "✅ Protection disabled!\n\n"
        "🔓 Media content can now be saved and forwarded by all users."
    )

# Protection commands for enabling and disabling content protection
async def pon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enable protection settings - admin only"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return
    
    global protection_enabled
    protection_enabled = True
    save_protection_settings()
    
    await update.message.reply_text(
        "✅ Protection enabled!\n\n"
        "🔒 All media content will now be protected from saving and forwarding.\n"
        "👑 Note: Admins are always exempt from protection."
    )

async def poff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disable protection settings - admin only"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return
    
    global protection_enabled
    protection_enabled = False
    save_protection_settings()
    
    await update.message.reply_text(
        "✅ Protection disabled!\n\n"
        "🔓 Media content can now be saved and forwarded by all users."
    )


async def handle_resume_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    token = query.data.replace("resume_", "")
    state = resume_states.get(token)
    if not state:
        try:
            await query.answer("❌ Resume expired", show_alert=True)
        except Exception: pass
        return
    mode = state['mode']
    user_id = state['user_id']
    chat_id = state['chat_id']
    tag = state['tag']
    # Re-offer stop button
    stop_keyboard = ReplyKeyboardMarkup([["🛑 Stop"]], resize_keyboard=True)
    await context.bot.send_message(chat_id=chat_id, text="▶️ Resuming send...", reply_markup=stop_keyboard)

    # Use a single if/elif chain without intervening statements to avoid syntax issues
    if mode == 'view':
        next_index = state.get('next_index', 0)
        end_index = state.get('end_index', -1)
        total = (end_index - next_index + 1) if end_index >= next_index else 0
        cancel_key = f"view_cancel_{chat_id}_{int(time.time()*1000)}"
        context.chat_data[cancel_key] = False
        sent = 0
        last_idx_processed = next_index - 1
        for idx in range(next_index, end_index + 1):
            if context.chat_data.get(cancel_key):
                break
            if idx >= len(media_data.get(tag, [])):
                continue
            item = media_data[tag][idx]
            if isinstance(item, dict) and (item.get('deleted') or item.get('revoked')):
                continue
            try:
                media_type = item.get('type', 'video') if isinstance(item, dict) else 'video'
                file_id = item.get('file_id') if isinstance(item, dict) else None
                share_link = f"https://telegram.me/share/url?url=https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                cap = build_media_caption("", tag, str(idx), share_link, media_type)
                # Build admin keyboard same as /view
                video_id = f"{tag}_{idx}"
                push_button = InlineKeyboardButton("🔄 PUSH", callback_data=f"p_{video_id}")
                admin_row = [
                    InlineKeyboardButton("🛑 Revoke", callback_data=f"revoke_media_{video_id}"),
                    InlineKeyboardButton("🗑️ Remove Media", callback_data=f"del_media_{video_id}")
                ]
                keyboard = InlineKeyboardMarkup([[push_button], admin_row])
                media_params = {
                    'context': context,
                    'chat_id': chat_id,
                    'caption': cap,
                    'reply_markup': keyboard,
                    'parse_mode': ParseMode.HTML,
                    'protect_content': should_protect_content(user_id, chat_id)
                }
                if media_type == 'video' and file_id:
                    await safe_send_message(**media_params, auto_delete=False, video=file_id)
                else:
                    await safe_send_message(**media_params, auto_delete=False)
                sent += 1
                last_idx_processed = idx
            except Exception:
                continue
        # If cancelled mid-resume, offer a fresh resume token again
        if context.chat_data.get(cancel_key):
            new_next = last_idx_processed + 1
            if new_next <= end_index:
                resume_id2 = f"{user_id}_{int(time.time()*1000)}"
                resume_states[resume_id2] = {
                    'mode': 'view',
                    'tag': tag,
                    'next_index': new_next,
                    'end_index': end_index,
                    'chat_id': chat_id,
                    'user_id': user_id,
                    'delivered': (last_idx_processed - next_index + 1) if last_idx_processed >= next_index else 0,
                    'total': total
                }
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Continue Sending", callback_data=f"resume_{resume_id2}")]])
                await context.bot.send_message(chat_id=chat_id, text=f"⛔ Stopped. Delivered {sent} items in this run. You can continue from index {new_next}.", reply_markup=kb)
                try:
                    del resume_states[token]
                except KeyError:
                    pass
                context.chat_data[cancel_key] = False
                return
        # Restore home keyboard after completion
        home_keyboard = ReplyKeyboardMarkup([
            ["📊 Stats", "📢 Broadcast", "👥 Users"],
            ["🎬 Media", "💾 Backup", "🛡️ Protection"],
            ["🧹 Cleanup", "🔧 Tools", "🧪 Test"],
            ["🏠 HOME"]
        ], resize_keyboard=True) if is_admin(user_id) else ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
        remaining_total = (end_index - next_index + 1)
        await context.bot.send_message(chat_id=chat_id, text=f"✅ Resume complete. Sent {sent}/{remaining_total} remaining items.", reply_markup=home_keyboard)
    elif mode == 'cview':
        visible_indices = state.get('visible_indices', [])
        pointer = state.get('pointer', 0)
        cancel_key = f"cview_cancel_{chat_id}_{int(time.time()*1000)}"
        context.chat_data[cancel_key] = False
        sent = 0
        last_pos_processed = pointer - 1
        for pos in range(pointer, len(visible_indices)):
            idx = visible_indices[pos]
            if context.chat_data.get(cancel_key):
                break
            if idx >= len(media_data.get(tag, [])):
                continue
            item = media_data[tag][idx]
            if isinstance(item, dict) and (item.get('deleted') or item.get('revoked')):
                continue
            try:
                media_type = item.get('type', 'video') if isinstance(item, dict) else 'video'
                file_id = item.get('file_id') if isinstance(item, dict) else None
                share_link = f"https://telegram.me/share/url?url=https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                cap = build_media_caption("", tag, str(idx), share_link, media_type)
                # Admin keyboard like /cview
                push_button = InlineKeyboardButton("🔄 PUSH", callback_data=f"p_{tag}_{idx}")
                admin_row = [
                    InlineKeyboardButton("🛑 Revoke", callback_data=f"revoke_media_{tag}_{idx}"),
                    InlineKeyboardButton("🗑️ Remove", callback_data=f"del_media_{tag}_{idx}")
                ]
                keyboard = InlineKeyboardMarkup([[push_button], admin_row])
                media_params = {
                    'context': context,
                    'chat_id': chat_id,
                    'caption': cap,
                    'reply_markup': keyboard,
                    'parse_mode': ParseMode.HTML,
                    'protect_content': should_protect_content(user_id, chat_id)
                }
                if media_type == 'video' and file_id:
                    await safe_send_message(**media_params, auto_delete=False, video=file_id)
                else:
                    await safe_send_message(**media_params, auto_delete=False)
                sent += 1
                last_pos_processed = pos
            except Exception:
                continue
        if context.chat_data.get(cancel_key):
            new_pointer = last_pos_processed + 1
            if new_pointer < len(visible_indices):
                resume_id2 = f"{user_id}_{int(time.time()*1000)}"
                resume_states[resume_id2] = {
                    'mode': 'cview',
                    'tag': tag,
                    'visible_indices': visible_indices,
                    'pointer': new_pointer,
                    'chat_id': chat_id,
                    'user_id': user_id,
                    'delivered': (last_pos_processed - pointer + 1) if last_pos_processed >= pointer else 0,
                    'total': len(visible_indices) - pointer
                }
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Continue Sending", callback_data=f"resume_{resume_id2}")]])
                remaining = len(visible_indices) - new_pointer
                await context.bot.send_message(chat_id=chat_id, text=f"⛔ Stopped. Delivered {sent} items in this run. Remaining visible items: {remaining}.", reply_markup=kb)
                try:
                    del resume_states[token]
                except KeyError:
                    pass
                context.chat_data[cancel_key] = False
                return
        # Restore home keyboard after completion
        home_keyboard = ReplyKeyboardMarkup([
            ["📊 Stats", "📢 Broadcast", "👥 Users"],
            ["🎬 Media", "💾 Backup", "🛡️ Protection"],
            ["🧹 Cleanup", "🔧 Tools", "🧪 Test"],
            ["🏠 HOME"]
        ], resize_keyboard=True) if is_admin(user_id) else ReplyKeyboardMarkup([
            ["🎲 Random", "🔥 Top Videos"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)
        await context.bot.send_message(chat_id=chat_id, text=f"✅ Resume complete. Sent {sent}/{len(visible_indices) - pointer} remaining visible items.", reply_markup=home_keyboard)
    elif mode == 'passlink':
        # Support both old (valid_videos + pointer) and new (remaining) formats
        if 'remaining' in state:
            remaining = state['remaining']
            delivered_so_far = state.get('delivered', 0)
            total = state.get('total', delivered_so_far + len(remaining))
        else:
            # Legacy format: convert valid_videos + pointer to remaining
            valid_videos = state.get('valid_videos', [])
            pointer = state.get('pointer', 0)
            remaining = valid_videos[pointer:]
            delivered_so_far = state.get('delivered', 0)
            total = state.get('total', len(valid_videos))
        
        actual_indices_raw = state.get('actual_indices', [])
        # Convert any string indices to int (can happen if loaded from JSON)
        actual_indices = [int(x) if isinstance(x, str) else x for x in actual_indices_raw]
        start_index = state.get('start_index', 0)
        cancel_key = f"passlink_cancel_{chat_id}_{int(time.time()*1000)}"
        context.chat_data[cancel_key] = False
        sent_this_run = 0
        new_remaining = []
        for local_pos, (idx, item) in enumerate(remaining):
            if context.chat_data.get(cancel_key):
                new_remaining.extend(remaining[local_pos:])
                break
            try:
                await send_single_passlink_video(update, context, item, tag, idx, actual_indices, start_index)
                sent_this_run += 1
            except Exception:
                new_remaining.append((idx, item))
        if context.chat_data.get(cancel_key):
            delivered_total = delivered_so_far + sent_this_run
            resume_id2 = f"{user_id}_{int(time.time()*1000)}"
            resume_states[resume_id2] = {
                'mode': 'passlink',
                'tag': tag,
                'remaining': new_remaining,
                'delivered': delivered_total,
                'total': total,
                'actual_indices': actual_indices,
                'start_index': start_index,
                'chat_id': chat_id,
                'user_id': user_id
            }
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Continue Sending", callback_data=f"resume_{resume_id2}")],
                [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
            ])
            await context.bot.send_message(chat_id=chat_id, text=f"⛔ Stopped. Delivered {sent_this_run} this run (total {delivered_total}/{total}). Remaining: {len(new_remaining)}.", reply_markup=kb)
            try:
                del resume_states[token]
            except KeyError:
                pass
            context.chat_data[cancel_key] = False
            return
        delivered_final = delivered_so_far + sent_this_run
        if new_remaining:
            resume_id2 = f"{user_id}_{int(time.time()*1000)}"
            resume_states[resume_id2] = {
                'mode': 'passlink',
                'tag': tag,
                'remaining': new_remaining,
                'delivered': delivered_final,
                'total': total,
                'actual_indices': actual_indices,
                'start_index': start_index,
                'chat_id': chat_id,
                'user_id': user_id
            }
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Continue Sending", callback_data=f"resume_{resume_id2}")],
                [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
            ])
            await context.bot.send_message(chat_id=chat_id, text=f"⚠️ Some items failed. Delivered {sent_this_run} (total {delivered_final}/{total}). {len(new_remaining)} left.", reply_markup=kb)
        else:
            # Restore home keyboard after completion
            home_keyboard = ReplyKeyboardMarkup([
                ["📊 Stats", "📢 Broadcast", "👥 Users"],
                ["🎬 Media", "💾 Backup", "🛡️ Protection"],
                ["🧹 Cleanup", "🔧 Tools", "🧪 Test"],
                ["🏠 HOME"]
            ], resize_keyboard=True) if is_admin(user_id) else ReplyKeyboardMarkup([
                ["🎲 Random", "🔥 Top Videos"],
                ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
            ], resize_keyboard=True)
            await context.bot.send_message(
                chat_id=chat_id, 
                text=f"✅ Resume complete. Delivered {delivered_final}/{total} total passlink items.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⭐ MY FAVORITES", callback_data="view_favorites")]
                ])
            )
            # Send home keyboard separately
            await context.bot.send_message(
                chat_id=chat_id,
                text="Select an option below:",
                reply_markup=home_keyboard
            )
    # Cleanup state
    try: del resume_states[token]
    except KeyError: pass


async def return_home_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback to return user to home with a welcome note."""
    query = update.callback_query
    try:
        await safe_answer_callback_query(query)
    except Exception:
        pass
    user_id = query.from_user.id
    # Build appropriate home keyboard
    if is_admin(user_id):
        kb = ReplyKeyboardMarkup([
            ["📊 Stats", "📢 Broadcast", "👥 Users"],
            ["🎬 Media", "💾 Backup", "🛡️ Protection"],
            ["🧹 Cleanup", "🔧 Tools", "🧪 Test"],
            ["🏠 HOME"]
        ], resize_keyboard=True)
    else:
        kb = build_initial_reply_keyboard()

    # Notify and show home keyboard
    try:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="👋 Welcome to Meow Bot!",
            reply_markup=kb
        )
    except Exception:
        # Fallback to editing the original message if sending fails
        try:
            await query.edit_message_text("👋 Welcome to Meow Bot!")
        except Exception:
            pass
def main():
    # Configure logging - reduce verbosity, only show WARNING and above for libraries
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.WARNING  # Set to WARNING to reduce spam
    )
    # Set our own logger to INFO level so we can see our debug messages
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    print("🤖 Bhaichara Bot - Starting...")
    app = Application.builder().token(BOT_TOKEN).connection_pool_size(16).pool_timeout(30).concurrent_updates(128).post_init(post_init_callback).post_shutdown(post_shutdown_callback).build()
    
    # Initialize random state
    update_random_state()
    
    # Migrate existing users to users database
    migrate_existing_users()
    
    print("⚡ Bot application built successfully")

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("upload", upload))
    app.add_handler(CommandHandler("listvideo", listvideo))
    app.add_handler(CommandHandler("listvideos", listvideos))
    app.add_handler(CommandHandler("remove", remove))
    app.add_handler(CommandHandler("get", get_tag))
    app.add_handler(CommandHandler("generatelink", generatelink))
    app.add_handler(CommandHandler("view", view))
    app.add_handler(CommandHandler("cview", cview))
    app.add_handler(CommandHandler("pass", pass_link))
    app.add_handler(CommandHandler("passall", pass_all_command))
    app.add_handler(CommandHandler("fresh", fresh_command))
    app.add_handler(CommandHandler("forcefresh", forcefresh_command))
    app.add_handler(CommandHandler("passlink", pass_link_command))
    app.add_handler(CommandHandler("off_limits", off_limits_command))
    app.add_handler(CommandHandler("revoke", revoke))
    app.add_handler(CommandHandler("revokelink", revoke_link_command))
    app.add_handler(CommandHandler("listactive", listactive))
    app.add_handler(CommandHandler("activelinks", activelinks_command))
    app.add_handler(CommandHandler("passlinks", passlinks_command))
    app.add_handler(CommandHandler("free", free))
    app.add_handler(CommandHandler("unfree", unfree))
    app.add_handler(CommandHandler("listfree", listfree))
    app.add_handler(CommandHandler("favorites", favorites_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("top", top_videos_command))
    app.add_handler(CommandHandler("jump", jump_command))
    
    # Reorder handlers to prioritize text shortcuts
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_shortcuts))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_rank_number_reply))
    
    # Broadcasting commands
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("dbroadcast", dbroadcast_command))
    app.add_handler(CommandHandler("pbroadcast", pbroadcast_command))
    app.add_handler(CommandHandler("sbroadcast", sbroadcast_command))
    app.add_handler(CommandHandler("fbroadcast", fbroadcast_command))
    app.add_handler(CommandHandler("bstats", broadcast_stats_command))
    app.add_handler(CommandHandler("userstats", user_stats_command))
    app.add_handler(CommandHandler("userinfo", userinfo_command))
    app.add_handler(CommandHandler("topusers", top_users_command))
    app.add_handler(CommandHandler("addusers", add_users_command))
    app.add_handler(CommandHandler("discover", discover_users_command))
    app.add_handler(CommandHandler("protection", protection_status_command))
    app.add_handler(CommandHandler("protectionon", protection_on_command))
    app.add_handler(CommandHandler("protectionoff", protection_off_command))
    # Add shorter aliases for protection commands
    app.add_handler(CommandHandler("pon", pon_cmd))
    app.add_handler(CommandHandler("poff", poff_cmd))
    app.add_handler(CommandHandler("pstatus", protection_status_command))
    app.add_handler(CommandHandler("ps", protection_status_command))
    app.add_handler(CommandHandler("testprotection", test_protection_command))
    app.add_handler(CommandHandler("testdeletion", test_deletion_command))
    app.add_handler(CommandHandler("checkprotection", check_protection_command))
    app.add_handler(CommandHandler("custom_batch", custom_batch_command))
    app.add_handler(CommandHandler("stop_batch", stop_batch_command))
    app.add_handler(CommandHandler("cancel_batch", cancel_batch_command))
    app.add_handler(CommandHandler("batch_status", batch_status_command))
    # AI batch commands
    app.add_handler(CommandHandler("ai_batch", ai_batch_command))
    app.add_handler(CommandHandler("stop_ai_batch", stop_ai_batch_command))
    app.add_handler(CommandHandler("cancel_ai_batch", cancel_ai_batch_command))
    app.add_handler(CommandHandler("ai_batch_status", ai_batch_status_command))
    # Caption management handlers
    app.add_handler(CommandHandler("set_global_caption", set_global_caption_command))
    app.add_handler(CommandHandler("add_replacement", add_replacement_command))
    app.add_handler(CommandHandler("list_replacements", list_replacements_command))
    app.add_handler(CommandHandler("remove_replacement", remove_replacement_command))
    app.add_handler(CommandHandler("caption_config", caption_config_command))
    # Link override handlers
    app.add_handler(CommandHandler("set_link", set_link_command))
    app.add_handler(CommandHandler("link_off", link_off_command))
    app.add_handler(CommandHandler("link_status", link_status_command))
    app.add_handler(CommandHandler("add_admin", add_admin_command))
    app.add_handler(CommandHandler("remove_admin", remove_admin_command))
    app.add_handler(CommandHandler("list_admins", list_admins_command))
    app.add_handler(CommandHandler("getfileid", get_file_id_command))
    app.add_handler(CommandHandler("getfile", getfile_command))
    app.add_handler(CommandHandler("reindexfavs", reindex_favs_command))
    app.add_handler(CommandHandler("reindex", reindex_favs_command))
    app.add_handler(CommandHandler("autoreindex", autoreindex_command))
    app.add_handler(CommandHandler("refreshviewers", refreshviewers_command))
    app.add_handler(CommandHandler("setwelcomeimage", set_welcome_image_command))
    app.add_handler(CommandHandler("testwelcomeimage", test_welcome_image_command))
    app.add_handler(CommandHandler("userfavorites", user_favorites_admin))
    app.add_handler(CommandHandler("videostats", video_stats_admin))
    app.add_handler(CommandHandler("topvideos", top_videos_admin))
    app.add_handler(CommandHandler("pushst", push_status_command))
    app.add_handler(CommandHandler("move", batch_move_command))
    app.add_handler(CommandHandler("add", batch_add_command))
    # Deleted media management commands
    app.add_handler(CommandHandler("listdeleted", listdeleted_command))
    app.add_handler(CommandHandler("cleanupdeleted", cleanup_deleted_command))
    app.add_handler(CommandHandler("debugdeleted", debug_deleted_command))
    app.add_handler(CommandHandler("restoredeleted", restoredeleted_command))
    app.add_handler(CommandHandler("cleardeleted", cleardeleted_command))
    app.add_handler(CommandHandler("restoreall", restoreall_command))
    app.add_handler(CommandHandler("deletedstats", deletedstats_command))
    # Auto-deletion management command
    app.add_handler(CommandHandler("autodelete", autodelete_command))
    # Auto-deletion notifications command
    app.add_handler(CommandHandler("notifications", notifications_command))
    # Force check for pending updates (admin only)
    app.add_handler(CommandHandler("checkupdates", force_check_updates_command))
    # List revoked media only (for deleted media use /listdeleted)
    # MOVED ABOVE CALLBACK HANDLERS
    app.add_handler(CommandHandler("listremoved", listremoved_cmd))
    
    # New command for listing only revoked media in the same format as deleted media
    async def listrevoked_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            return
        
        # Get revoked items from media_data
        revoked = []
        for tag, vids in media_data.items():
            if not isinstance(vids, list):
                continue
            for idx, v in enumerate(vids):
                if isinstance(v, dict) and v.get("revoked"):
                    media_type = v.get("type", "unknown")
                    revoked.append((f"{tag}_{idx}", tag, idx, media_type))
        
        if not revoked:
            await update.message.reply_text("📋 No revoked media found.")
            return
            
        # Sort by tag first, then by index
        revoked.sort(key=lambda x: (x[1], x[2]))
        
        # Pagination settings (matching deleted media format)
        items_per_page = 10
        total_items = len(revoked)
        total_pages = (total_items + items_per_page - 1) // items_per_page
        page = 0  # Start with first page
        
        start_idx = page * items_per_page
        end_idx = min(start_idx + items_per_page, total_items)
        current_items = revoked[start_idx:end_idx]
        
        # Build message in exact format as deleted media
        message = f"🛑 <b>Revoked Media (Page {page + 1}/{total_pages})</b>\n\n"
        
        for i, (video_key, tag, original_pos, media_type) in enumerate(current_items, start_idx + 1):
            # Create view link similar to deleted media format
            view_link = f"https://t.me/bhaicharabackupbot?start=view_revoked_{video_key}"
            restore_link = f"https://t.me/bhaicharabackupbot?start=restore_{video_key}"
            
            message += f"{i}. Tag: <code>{tag}</code> | Index: <code>{original_pos}</code> | "
            message += f"💎 {media_type} | "
            message += f"<a href='{view_link}'>👁️ View</a> | "
            message += f"<a href='{restore_link}'>♻️ Restore</a>\n"
        
        # Create navigation buttons (matching deleted media format)
        nav_buttons = []
        if total_pages > 1:
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"revoked_page_{page - 1}"))
            
            # Page info button (non-clickable info)
            nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="revoked_page_info"))
            
            if page < total_pages - 1:
                nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"revoked_page_{page + 1}"))
            
            nav_buttons.append(nav_row)
        
        # Add utility buttons (matching deleted media format)
        utility_buttons = [
            InlineKeyboardButton("🧹 Cleanup", callback_data="cleanup_revoked"),
            InlineKeyboardButton("🔄 Refresh", callback_data="refresh_revoked_list")
        ]
        nav_buttons.append(utility_buttons)
        
        keyboard = InlineKeyboardMarkup(nav_buttons) if nav_buttons else None
        
        # Send the message
        await update.message.reply_html(message, reply_markup=keyboard)
    
    app.add_handler(CommandHandler("listrevoked", listrevoked_cmd))
    # Restore command: /restoremedia <tag> <index>
    async def restoremedia_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            return
        if len(context.args) != 2:
            await update.message.reply_text("Usage: /restoremedia <tag> <index>")
            return
        tag = context.args[0].strip().lower()
        try:
            idx = int(context.args[1])
        except ValueError:
            await update.message.reply_text("❌ Bad index")
            return
        video_key = f"{tag}_{idx}"
        # Build fake query-like object for reuse (minimal)
        class Dummy:
            def __init__(self, message):
                self.message = message
            async def answer(self, *a, **k):
                pass
        dummy = Dummy(update.message)
        await restore_media_entry(dummy, video_key)
    app.add_handler(CommandHandler("restoremedia", restoremedia_cmd))

    # Admin Quick Push handlers
    # Run Quick Push after media upload handlers to avoid interference with batch
    app.add_handler(MessageHandler(filters.ALL, admin_quick_push_media_handler), group=10)
    app.add_handler(CallbackQueryHandler(handle_quick_push_callback, pattern=r'^(qp_t:|qp_tag:|qp_new|qp_cancel|qp_pg:|qp_push|qp_clear)'))
    
    # Backup command handlers
    async def toggle_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Toggle local filesystem backups on/off"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        global LOCAL_BACKUPS_ENABLED, backup_manager
        
        if not context.args:
            # Show current status
            status = "🟢 ENABLED" if LOCAL_BACKUPS_ENABLED else "🔴 DISABLED"
            import os
            folder_exists = os.path.exists(LOCAL_BACKUP_DIR)
            folder_count = len(os.listdir(LOCAL_BACKUP_DIR)) if folder_exists else 0
            
            msg = (
                f"📊 **Local Backup System Status**\n\n"
                f"Status: {status}\n"
                f"Backup Folder: `{LOCAL_BACKUP_DIR}`\n"
                f"Folder Exists: {'✅ Yes' if folder_exists else '❌ No'}\n"
                f"Stored Backups: {folder_count}\n\n"
                f"**Usage:**\n"
                f"• `/togglebackup on` - Enable local backups\n"
                f"• `/togglebackup off` - Disable local backups\n"
                f"• `/togglebackup status` - Show this status\n\n"
                f"**Note:** Telegram backups (`/telegrambackup` and daily auto-backup) remain active regardless of this setting."
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        action = context.args[0].lower()
        
        if action in ['on', 'enable', 'true', '1']:
            if LOCAL_BACKUPS_ENABLED:
                await update.message.reply_text("ℹ️ Local backups are already enabled")
                return
            
            LOCAL_BACKUPS_ENABLED = True
            
            # Initialize backup_manager if not already initialized
            if backup_manager is None:
                try:
                    from backup_manager import DatabaseBackupManager
                    backup_manager = DatabaseBackupManager(data_dir=".", backup_dir=LOCAL_BACKUP_DIR)
                    await update.message.reply_text(
                        "✅ **Local Backups ENABLED**\n\n"
                        f"📁 Backups will be stored in: `{LOCAL_BACKUP_DIR}/`\n\n"
                        "**Available Commands:**\n"
                        "• `/backup` - Create new backup\n"
                        "• `/listbackups` - List all backups\n"
                        "• `/restore <name>` - Restore from backup\n"
                        "• `/backupstats <name>` - Show backup details\n"
                        "• `/deletebackup <name>` - Delete a backup\n\n"
                        "⚠️ **Important:** Bot restart required for commands to appear in menu!",
                        parse_mode=ParseMode.MARKDOWN
                    )
                except Exception as e:
                    await update.message.reply_text(f"❌ Failed to initialize backup manager: {str(e)}")
                    LOCAL_BACKUPS_ENABLED = False
                    return
            else:
                await update.message.reply_text(
                    "✅ **Local Backups ENABLED**\n\n"
                    "Local backup commands are now active!",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        elif action in ['off', 'disable', 'false', '0']:
            if not LOCAL_BACKUPS_ENABLED:
                await update.message.reply_text("ℹ️ Local backups are already disabled")
                return
            
            LOCAL_BACKUPS_ENABLED = False
            await update.message.reply_text(
                "🔴 **Local Backups DISABLED**\n\n"
                "Local backup commands will no longer work.\n\n"
                "✅ **Telegram backups remain active:**\n"
                "• `/telegrambackup` - Manual backup to Telegram\n"
                "• Daily auto-backup to Telegram\n\n"
                "💡 Use `/togglebackup on` to re-enable local backups anytime.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        elif action == 'status':
            # Redirect to showing status
            context.args = []
            await toggle_backup_command(update, context)
        
        else:
            await update.message.reply_text(
                "❌ Invalid option\n\n"
                "**Usage:**\n"
                "• `/togglebackup on` - Enable\n"
                "• `/togglebackup off` - Disable\n"
                "• `/togglebackup status` - Show status",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def telegram_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send all current JSON files directly to admin via Telegram"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        await update.message.reply_text("📤 Sending current JSON files and ZIP backup...")
        
        # List of all important JSON files
        json_files = [
            "media_db.json",
            "users_db.json", 
            "favorites_db.json",
            "passed_links.json",
            "active_links.json",
            "deleted_media.json",
            "random_state.json",
            "user_preferences.json",
            "admin_list.json",
            "exempted_users.json",
            "caption_config.json",
            "protection_settings.json",
            "auto_delete_tracking.json",
            "last_update_offset.json",
            "push_changes.json",
            "off_limits.json"
        ]
        
        sent_count = 0
        missing_count = 0
        
        # Send individual JSON files (without railway_ prefix)
        for filename in json_files:
            try:
                if os.path.exists(filename):
                    # Read the current file from filesystem
                    with open(filename, 'rb') as file:
                        await context.bot.send_document(
                            chat_id=update.effective_chat.id,
                            document=file,
                            filename=filename,  # Removed "railway_" prefix
                            caption=f"📁 **JSON File**: `{filename}`\n🕐 Extracted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                        )
                    sent_count += 1
                else:
                    missing_count += 1
                    print(f"⚠️ File not found: {filename}")
            except Exception as e:
                await update.message.reply_text(f"❌ Error sending {filename}: {str(e)}")
                missing_count += 1
        
        # Create and send ZIP file containing all JSON files
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            zip_filename = f"backup_{timestamp}.zip"
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for filename in json_files:
                    if os.path.exists(filename):
                        zipf.write(filename, filename)  # Add file to ZIP with original name
            
            # Send the ZIP file
            with open(zip_filename, 'rb') as zip_file:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=zip_file,
                    filename=zip_filename,
                    caption=f"📦 **Complete ZIP Backup**\n🕐 Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC\n📁 Contains all {sent_count} JSON files"
                )
            
            # Clean up the temporary ZIP file
            os.remove(zip_filename)
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error creating ZIP backup: {str(e)}")
        
        # Summary message
        summary = (
            f"✅ **Telegram Backup Complete**\n\n"
            f"📤 Individual files sent: {sent_count}\n"
            f"📦 ZIP backup: Created and sent\n"
            f"❌ Missing files: {missing_count}\n"
            f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
            f"💡 Received both individual JSON files and a complete ZIP archive!"
        )
        await update.message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)
    
    async def auto_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Configure automatic backups to Telegram"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not context.args:
            # Show current status
            # Check if auto-backup is running (you can enhance this with a global flag)
            await update.message.reply_text(
                "🔄 **Auto-Backup Configuration**\n\n"
                "Usage:\n"
                "`/autobackup on` - Enable daily auto-backup\n"
                "`/autobackup off` - Disable auto-backup\n"
                "`/autobackup now` - Trigger backup immediately\n"
                "`/autobackup status` - Check current status\n\n"
                "💡 Auto-backup sends JSON files to your Telegram daily",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        action = context.args[0].lower()
        
        if action == "on":
            # Enable auto-backup (you can implement with job_queue)
            await update.message.reply_text(
                "✅ **Auto-backup enabled**\n\n"
                "📅 JSON files will be sent to your Telegram daily at 2 AM UTC\n"
                "🔄 This ensures you always have the latest Railway data backed up"
            )
            
        elif action == "off":
            await update.message.reply_text("❌ Auto-backup disabled")
            
        elif action == "now":
            # Trigger immediate backup
            await telegram_backup_command(update, context)
            
        elif action == "status":
            # Show current status of the backup system
            current_time = datetime.now()
            if last_backup_time:
                time_since = current_time - last_backup_time
                hours_since = time_since.total_seconds() / 3600
                next_backup = last_backup_time + timedelta(hours=BACKUP_INTERVAL_HOURS)
                hours_until = (next_backup - current_time).total_seconds() / 3600
                
                status_msg = (
                    "📊 **Auto-Backup Status**\n\n"
                    f"🔄 Status: Active (Time-based monitoring)\n"
                    f"📅 Last backup: {last_backup_time.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                    f"⏰ Hours since last backup: {hours_since:.1f}\n"
                    f"📁 Next backup: {next_backup.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                    f"⏳ Hours until next backup: {max(0, hours_until):.1f}\n"
                    f"📤 Target: Admin Telegram chat\n"
                    f"💾 Files: All JSON databases\n"
                    f"🔄 Check interval: Every hour"
                )
            else:
                status_msg = (
                    "📊 **Auto-Backup Status**\n\n"
                    "🔄 Status: Active (First backup pending)\n"
                    "📅 Last backup: Never\n"
                    "⏰ Next backup: Within 1 hour\n"
                    "� Target: Admin Telegram chat\n"
                    "💾 Files: All JSON databases\n"
                    "🔄 Check interval: Every hour"
                )
            
            await update.message.reply_text(status_msg)
        else:
            await update.message.reply_text("❌ Invalid option. Use: on, off, now, or status")

    async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Create a backup of all database files"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not LOCAL_BACKUPS_ENABLED or backup_manager is None:
            await update.message.reply_text(
                "ℹ️ **Local Filesystem Backups: DISABLED**\n\n"
                "This bot uses Telegram-based backups instead:\n\n"
                "📤 **Manual Backup:** Use `/telegrambackup` to send all JSON files to Telegram now\n"
                "🔄 **Auto-Backup:** Daily backups are automatically sent to your Telegram\n\n"
                "💡 **To Enable Local Backups:**\n"
                "Use `/togglebackup on` to enable local filesystem backups to the `backups/` folder\n\n"
                "⚠️ Note: Local backups may be lost on Railway/cloud restarts. Telegram backups are safer!",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        await update.message.reply_text("💾 Creating backup...")
        
        # Create backup in background
        success, message = await asyncio.get_event_loop().run_in_executor(
            None, backup_manager.create_backup, None, "Manual backup via bot command"
        )
        
        if success:
            await update.message.reply_text(f"✅ {message}")
        else:
            await update.message.reply_text(f"❌ {message}")
    
    async def list_backups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all available backups"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not LOCAL_BACKUPS_ENABLED or backup_manager is None:
            import os
            backup_folder_exists = os.path.exists(LOCAL_BACKUP_DIR)
            folder_count = len(os.listdir(LOCAL_BACKUP_DIR)) if backup_folder_exists else 0
            
            status_msg = (
                "📊 **Local Backup System Status**\n\n"
                "🔴 **Status:** DISABLED\n"
                f"📁 **Backup Folder:** `{LOCAL_BACKUP_DIR}`\n"
                f"📂 **Folder Exists:** {'✅ Yes' if backup_folder_exists else '❌ No'}\n"
                f"📦 **Stored Backups:** {folder_count}\n\n"
                "ℹ️ **Why Disabled?**\n"
                "Local backups are disabled because Telegram-based backups are more reliable for cloud deployments.\n\n"
                "📤 **Available Backup Options:**\n"
                "• `/telegrambackup` - Send all JSON files to Telegram now\n"
                "• `/autobackup status` - Check daily auto-backup status\n"
                "• `/togglebackup on` - Enable local filesystem backups\n\n"
                "💡 **Recommendation:** Use `/telegrambackup` for manual backups. "
                "Auto-backups are sent daily to your Telegram automatically!"
            )
            await update.message.reply_text(status_msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        backups = await asyncio.get_event_loop().run_in_executor(None, backup_manager.list_backups)
        
        if not backups:
            await update.message.reply_text("📋 No backups found")
            return
        
        message = f"📋 <b>Available Backups ({len(backups)})</b>\n\n"
        
        for i, backup in enumerate(backups[:10], 1):  # Show first 10
            created = backup['created_at'][:19]  # Show date/time without seconds
            files = backup['total_files']
            size = backup['total_size']
            desc = backup.get('description', 'No description')[:50]
            
            message += f"{i}. <code>{backup['name']}</code>\n"
            message += f"   📅 {created} | 📁 {files} files | 💾 {size} bytes\n"
            if desc:
                message += f"   📝 {desc}\n"
            message += "\n"
        
        if len(backups) > 10:
            message += f"... and {len(backups) - 10} more backups"
        
        await update.message.reply_html(message)
    
    async def restore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Restore from a backup"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not LOCAL_BACKUPS_ENABLED or backup_manager is None:
            await update.message.reply_text(
                "ℹ️ **Local Backups are Disabled**\n\n"
                "Use `/telegrambackup` for manual backups or `/togglebackup on` to enable local backups.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /restore <backup_name>")
            return
        
        backup_name = context.args[0]
        await update.message.reply_text(f"🔄 Restoring from backup '{backup_name}'...")
        
        # Restore backup
        success, message = await asyncio.get_event_loop().run_in_executor(
            None, backup_manager.restore_backup, backup_name
        )
        
        if success:
            await update.message.reply_text(f"✅ {message}\n\n⚠️ Bot restart recommended to reload all data")
        else:
            await update.message.reply_text(f"❌ {message}")

    async def backup_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show statistics for a specific backup"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not LOCAL_BACKUPS_ENABLED or backup_manager is None:
            await update.message.reply_text(
                "ℹ️ **Local Backups are Disabled**\n\n"
                "Use `/telegrambackup` for manual backups or `/togglebackup on` to enable local backups.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /backupstats <backup_name>")
            return
        
        backup_name = context.args[0]
        
        stats = await asyncio.get_event_loop().run_in_executor(
            None, backup_manager.get_backup_stats, backup_name
        )
        
        if not stats or not stats.get('exists'):
            await update.message.reply_text(f"❌ Backup '{backup_name}' not found")
            return
        
        info = stats['info']
        message = f"📊 <b>Backup Statistics: {backup_name}</b>\n\n"
        message += f"📅 Created: {info['created_at'][:19]}\n"
        message += f"📁 Files: {info['total_files']}\n"
        message += f"💾 Size: {info['total_size']} bytes\n"
        
        if info.get('description'):
            message += f"📝 Description: {info['description']}\n"
        
        message += "\n📋 <b>File Details:</b>\n"
        
        for filename, file_info in stats.get('file_details', {}).items():
            if file_info.get('status') == 'not_found':
                message += f"❌ {filename}: Not found\n"
            else:
                size = file_info.get('size', 0)
                message += f"✅ {filename}: {size} bytes\n"
        
        await update.message.reply_html(message)

    async def delete_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Delete a backup"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not LOCAL_BACKUPS_ENABLED or backup_manager is None:
            await update.message.reply_text(
                "ℹ️ **Local Backups are Disabled**\n\n"
                "Use `/telegrambackup` for manual backups or `/togglebackup on` to enable local backups.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /deletebackup <backup_name>")
            return
        
        backup_name = context.args[0]
        
        # Confirm deletion with keyboard
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_backup_{backup_name}"),
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_delete_backup")
            ]
        ])
        
        await update.message.reply_text(
            f"🗑️ <b>Delete Backup</b>\n\n"
            f"Are you sure you want to delete backup '{backup_name}'?\n\n"
            f"⚠️ This action cannot be undone!",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    
    # Always register backup command handlers (they check LOCAL_BACKUPS_ENABLED internally)
    app.add_handler(CommandHandler("backup", backup_command))
    app.add_handler(CommandHandler("listbackups", list_backups_command))
    app.add_handler(CommandHandler("restore", restore_command))
    app.add_handler(CommandHandler("backupstats", backup_stats_command))
    app.add_handler(CommandHandler("deletebackup", delete_backup_command))
    app.add_handler(CommandHandler("togglebackup", toggle_backup_command))
    app.add_handler(CommandHandler("telegrambackup", telegram_backup_command))
    app.add_handler(CommandHandler("autobackup", auto_backup_command))
    
    # Add callback query handler for all button interactions
    app.add_handler(CallbackQueryHandler(handle_search_result_callback, pattern=r'^search_result_'))
    app.add_handler(CallbackQueryHandler(handle_search_page_callback, pattern=r'^search_page_'))
    app.add_handler(CallbackQueryHandler(handle_search_preview_callback, pattern=r'^search_preview_all$'))
    app.add_handler(CallbackQueryHandler(handle_continue_preview_callback, pattern=r'^continue_preview$'))
    app.add_handler(CallbackQueryHandler(handle_continue_top_videos_preview, pattern=r'^continue_top_videos_preview$'))
    
    # Handler for preview_from_rank callback
    async def handle_preview_from_rank_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle preview from specific rank button"""
        query = update.callback_query
        await query.answer()
        
        # Parse callback data: preview_from_rank_<index> or preview_from_rank_<index>_<range_limit>
        data = query.data.replace("preview_from_rank_", "")
        parts = data.split('_')
        
        try:
            start_index = int(parts[0])
            range_limit = int(parts[1]) if len(parts) > 1 else None
            
            # Start preview from this index
            await handle_top_videos_preview(update, context, start_index=start_index, range_limit=range_limit)
        except Exception as e:
            logging.error(f"Error in preview_from_rank callback: {str(e)}")
            await query.answer(f"Error: {str(e)}", show_alert=True)
    
    app.add_handler(CallbackQueryHandler(handle_preview_from_rank_callback, pattern=r'^preview_from_rank_'))
    app.add_handler(CallbackQueryHandler(handle_next_top_batch_callback, pattern=r'^next_top_batch_'))
    app.add_handler(CallbackQueryHandler(handle_random_from_tag_callback, pattern=r'^random_from_tag_'))
    app.add_handler(CallbackQueryHandler(handle_button_click))
    app.add_handler(CallbackQueryHandler(handle_resume_callback, pattern=r'^resume_'))
    app.add_handler(CallbackQueryHandler(return_home_callback, pattern=r'^return_home$'))

    # Universal handlers to auto-register all users
    app.add_handler(MessageHandler(filters.ALL, auto_register_user), group=1)
    app.add_handler(CallbackQueryHandler(auto_register_user), group=1)
    
    # Add a handler to save update offsets for all updates
    async def save_offset_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save the update offset for persistent tracking (batched to reduce I/O)"""
        global offset_save_counter
        if update.update_id:
            offset_save_counter += 1
            # Save every 5 updates or if it's been more than last saved + 10
            if offset_save_counter % 5 == 0 or update.update_id > last_update_offset + 10:
                save_update_offset(update.update_id)
    
    # Add this handler with lowest priority so it runs after all other handlers
    app.add_handler(MessageHandler(filters.ALL, save_offset_handler), group=99)
    app.add_handler(CallbackQueryHandler(save_offset_handler), group=99)
    
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VOICE | filters.ANIMATION | filters.Sticker.ALL, upload))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_rank_number_reply))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_shortcuts))

    # Add error handler for timeout exceptions
    async def error_handler(update: object, context) -> None:
        """Handle errors caused by timeout exceptions."""
        if isinstance(context.error, asyncio.TimeoutError):
            print(f"Timeout error caught: {context.error}")
            if update and hasattr(update, 'callback_query') and update.callback_query:
                try:
                    await safe_answer_callback_query(update.callback_query, "⏱️ Request timed out, please try again", show_alert=True)
                except Exception:
                    pass
        elif isinstance(context.error, Conflict):
            warning_message = (
                "⚠️ Telegram reported another active getUpdates session. "
                "This usually means a different bot instance is running."
            )
            print(warning_message)
            logging.warning("Conflict detected: %s", context.error)

            # Attempt to regain control by clearing webhooks and informing the admin
            try:
                await context.application.bot.delete_webhook(drop_pending_updates=False)
                logging.info("Cleared webhook after conflict; retrying polling")
            except Exception as e:
                logging.warning("Unable to clear webhook after conflict: %s", e)

            # Notify admin if available
            bot_data = getattr(context.application, "bot_data", {})
            now_ts = time.time()
            last_notice = bot_data.get("last_conflict_notice", 0)
            if now_ts - last_notice >= 300:  # Notify at most every 5 minutes
                try:
                    await context.application.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=(
                            "⚠️ <b>Conflict Detected</b>\n"
                            "Another instance of the bot appears to be polling.\n"
                            "Please ensure only one process is running."
                        ),
                        parse_mode=ParseMode.HTML
                    )
                    bot_data["last_conflict_notice"] = now_ts
                except Exception as e:
                    logging.debug("Failed to notify admin about conflict: %s", e)
        else:
            print(f"Update {update} caused error {context.error}")
    
    app.add_error_handler(error_handler)
    
    print("🚀 Bot running...")
    
    # Show initial counter line
    print_counter_line()
    
    # Use standard polling - pending updates are processed in post_init_callback
    app.run_polling()


if __name__ == "__main__":
    main()
