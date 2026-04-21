import os
import sys
import json
import time
import logging
import asyncio
import re
import traceback
import functools
import zipfile
import shutil
import urllib.parse
import io

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
from pyrogram.errors import MessageNotModified
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# Try importing Video Processing Libraries
try:
    import cv2
    from PIL import Image
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False

# ================= LOGGING SETUP =================
class MemoryLogHandler(logging.Handler):
    def __init__(self, capacity=20):
        super().__init__()
        self.capacity = capacity
        self.logs = []
    def emit(self, record):
        self.logs.append(self.format(record))
        if len(self.logs) > self.capacity:
            self.logs.pop(0)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)

memory_handler = MemoryLogHandler(capacity=20)
memory_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
logger.addHandler(memory_handler)

# ================= ENVIRONMENT VARIABLES =================
API_ID_STR = os.environ.get("API_ID", "").strip()
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_OAUTH_TOKEN = os.environ.get("GOOGLE_OAUTH_TOKEN", "").strip()
AUTH_USERS_STR = os.environ.get("AUTHORIZED_USERS", "")
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://gdriveuploadbot.onrender.com")
CF_WORKER_URL = os.environ.get("CF_WORKER_URL", "").strip().rstrip('/')

os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

if not all([API_ID_STR, API_HASH, BOT_TOKEN, DRIVE_FOLDER_ID, GOOGLE_CLIENT_SECRET]):
    logger.error("CRITICAL ERROR: API keys or GOOGLE_CLIENT_SECRET is missing!")
    sys.exit(1)

API_ID = int(API_ID_STR) if API_ID_STR.isdigit() else 0
AUTHORIZED_USERS = [int(u.strip()) for u in AUTH_USERS_STR.split(",") if u.strip().isdigit()]
SCOPES = ['https://www.googleapis.com/auth/drive']

oauth_flow = None

# ================= CACHE & STATES =================
LINK_CACHE = {}  
USER_STATES = {} 
MYFILES_CACHE = {} 
PREVIEW_CACHE = {} 
BOT_STATS = {"uploads": 0, "clones": 0, "bytes_uploaded": 0}
ACTIVE_TASKS = {} # Stores cancel status

os.makedirs("previews", exist_ok=True)

# ================= TELEGRAM BOT =================
app = Client(
    "my_drive_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

def get_drive_service():
    if not GOOGLE_OAUTH_TOKEN:
        return False, f"⚠️ Bot not authenticated. Visit {RENDER_URL}/login"
    try:
        creds_data = json.loads(GOOGLE_OAUTH_TOKEN)
        creds = Credentials.from_authorized_user_info(creds_data, SCOPES)
        return True, build('drive', 'v3', credentials=creds)
    except Exception as e:
        return False, f"OAuth Token Error: {str(e)}"

def format_size(bytes_size):
    if bytes_size >= 1024 ** 3: return f"{bytes_size / (1024 ** 3):.2f} GB"
    if bytes_size >= 1024 ** 2: return f"{bytes_size / (1024 ** 2):.2f} MB"
    return f"{bytes_size / 1024:.2f} KB"

def format_time(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    elif m > 0: return f"{m}m {s}s"
    return f"{s}s"

def get_safe_error(e):
    err_str = str(e) if str(e).strip() else repr(e)
    for char in ['<', '>', '`', '*', '_', '[', ']']:
        err_str = err_str.replace(char, '')
    return err_str[:800]

async def fetch_url_metadata(url):
    """Fetches real filename and size directly from the server headers."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, allow_redirects=True, timeout=5) as resp:
                size = int(resp.headers.get('Content-Length', 0))
                cd = resp.headers.get('Content-Disposition', '')
                name = None
                if 'filename=' in cd:
                    matches = re.findall(r'filename\*?=(?:UTF-8\'\')?([^;]+)', cd)
                    if matches:
                        name = urllib.parse.unquote(matches[0].strip('"\''))
                if not name:
                    name = urllib.parse.unquote(url.split('/')[-1].split('?')[0])
                if not name:
                    name = f"file_{int(time.time())}"
                return name, size
    except Exception:
        return url.split('/')[-1].split('?')[0], 0

def generate_video_preview(video_path, output_dir, prefix):
    """Extracts 10 frames and returns a list of file paths."""
    if not HAS_CV2: return []
    try:
        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames <= 0: return []
        paths = []
        for i in range(10):
            frame_id = int(total_frames * (0.05 + 0.09 * i))
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_id)
            ret, frame = cap.read()
            if ret:
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                img = Image.fromarray(frame)
                p = os.path.join(output_dir, f"{prefix}_{i}.jpg")
                img.save(p, format="JPEG", quality=85)
                paths.append(p)
        cap.release()
        return paths
    except Exception as e:
        logger.error(f"Preview gen error: {e}")
        return []

def extract_gdrive_id(url):
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), False
    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), False
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), True
    return None, False

def check_auth(user_id):
    if not AUTHORIZED_USERS: return True
    return user_id in AUTHORIZED_USERS

def generate_result_text(file_name, file_id, file_size, elapsed_time, is_folder=False):
    drive_link = f"https://drive.google.com/drive/folders/{file_id}" if is_folder else f"https://drive.google.com/file/d/{file_id}/view"
    safe_name = urllib.parse.quote(file_name)
    if CF_WORKER_URL:
        direct_link = f"{CF_WORKER_URL}/0:down/{safe_name}"
        if is_folder: direct_link += "/"
    else:
        direct_link = "https://t.me/c/NotConfigured"
        
    text = (
        f"✅ **Task Completed Successfully!**\n\n"
        f"📄 **Name:** `{file_name}`\n"
        f"📦 **Size:** `{format_size(file_size)}`\n"
        f"⏱️ **Time Taken:** `{format_time(elapsed_time)}`\n\n"
        f"🔗 [Google Drive Link]({drive_link})\n\n"
        f"⚡ [Direct Download Link]({direct_link})"
    )
    return text

async def update_progress(current, total, msg, start_time, action_text):
    if ACTIVE_TASKS.get(msg.id, {}).get('cancel'):
        raise Exception("TaskCancelled")
        
    if total == 0: return
    now = time.time()
    if not hasattr(msg, "last_update_time"): msg.last_update_time = start_time
    if not hasattr(msg, "last_text"): msg.last_text = ""

    if (now - msg.last_update_time > 3.0) or (current == total):
        msg.last_update_time = now
        percent = (current / total) * 100
        filled = int(percent / 10)
        bar = "🟩" * filled + "🟥" * (10 - filled)
        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        eta = (total - current) / speed if speed > 0 else 0
        
        text = (
            f"{action_text}\n\n"
            f"{bar} {percent:.1f}%\n"
            f"📦 Size: {format_size(current)} / {format_size(total)}\n"
            f"🚀 Speed: {format_size(speed)}/s\n"
            f"⏳ ETA: {format_time(eta)}"
        )
        if text != msg.last_text:
            try:
                btn = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_task|{msg.id}")]])
                await msg.edit_text(text, reply_markup=btn)
                msg.last_text = text
            except MessageNotModified:
                pass

async def upload_to_drive_async(file_path, file_name, msg, parent_id=DRIVE_FOLDER_ID):
    try:
        success, service = get_drive_service()
        if not success: return False, service, 0, 0
            
        file_size = os.path.getsize(file_path)
        file_metadata = {'name': file_name, 'parents': [parent_id]}
        
        media = MediaFileUpload(file_path, chunksize=20*1024*1024, resumable=True)
        request = service.files().create(body=file_metadata, media_body=media, fields='id')
        
        response = None
        start_time = time.time()
        
        while response is None:
            if ACTIVE_TASKS.get(msg.id, {}).get('cancel'):
                raise Exception("TaskCancelled")
            status, response = await asyncio.to_thread(functools.partial(request.next_chunk, num_retries=5))
            if status:
                await update_progress(status.resumable_progress, file_size, msg, start_time, "☁️ Uploading to Google Drive...")
                
        elapsed = time.time() - start_time
        BOT_STATS["uploads"] += 1
        BOT_STATS["bytes_uploaded"] += file_size
        return True, response.get('id'), file_size, elapsed
    except Exception as e:
        return False, get_safe_error(e), 0, 0

async def download_from_gdrive(file_id, file_path, msg):
    """Downloads a file from GDrive to local server (for extraction)."""
    success, service = get_drive_service()
    if not success: raise Exception("Auth Error")
    
    file_info = service.files().get(fileId=file_id, fields="size").execute()
    total_size = int(file_info.get('size', 0))
    
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request, chunksize=20*1024*1024)
    done = False
    start_time = time.time()
    
    while done is False:
        if ACTIVE_TASKS.get(msg.id, {}).get('cancel'):
            fh.close()
            raise Exception("TaskCancelled")
        status, done = await asyncio.to_thread(downloader.next_chunk)
        if status:
            await update_progress(int(status.progress() * total_size), total_size, msg, start_time, "📥 Downloading from GDrive for Extraction...")
    fh.close()
    return True

def create_gdrive_folder(folder_name, parent_id=DRIVE_FOLDER_ID):
    success, service = get_drive_service()
    if not success: return None
    metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = service.files().create(body=metadata, fields='id').execute()
    return folder.get('id')

async def clone_gdrive_item(item_id, is_folder=False, parent_id=DRIVE_FOLDER_ID, new_name=None):
    try:
        success, service = get_drive_service()
        if not success: return False, service
        
        if not is_folder:
            body = {'parents': [parent_id]}
            if new_name: body['name'] = new_name
            response = await asyncio.to_thread(
                lambda: service.files().copy(fileId=item_id, body=body, fields='id, name').execute()
            )
            BOT_STATS["clones"] += 1
            return True, response
        else:
            original_folder = service.files().get(fileId=item_id, fields='name').execute()
            folder_name = new_name if new_name else original_folder.get('name')
            new_folder_id = create_gdrive_folder(folder_name, parent_id)
            
            query = f"'{item_id}' in parents and trashed=false"
            results = service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType)", pageSize=1000).execute()
            items = results.get('files', [])
            
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    await clone_gdrive_item(item['id'], is_folder=True, parent_id=new_folder_id)
                else:
                    await clone_gdrive_item(item['id'], is_folder=False, parent_id=new_folder_id)
                    
            BOT_STATS["clones"] += 1
            return True, {"name": folder_name, "id": new_folder_id}
    except Exception as e:
        return False, get_safe_error(e)

def extract_zip(zip_path, extract_dir):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        return True, "Success"
    except RuntimeError as e:
        if 'password' in str(e).lower() or 'encrypted' in str(e).lower():
            return False, "This ZIP is password protected and cannot be extracted."
        return False, str(e)
    except Exception as e:
        return False, str(e)

# ================= NEW COMMANDS =================
@app.on_message(filters.command("stats"))
async def stats_command(client, message):
    if not check_auth(message.from_user.id): return
    text = (
        "📊 **Bot Current Session Stats:**\n\n"
        f"📤 Total Uploads: `{BOT_STATS['uploads']}`\n"
        f"🔄 Total Clones: `{BOT_STATS['clones']}`\n"
        f"💾 Data Uploaded: `{format_size(BOT_STATS['bytes_uploaded'])}`"
    )
    await message.reply_text(text)

@app.on_message(filters.command("logs"))
async def logs_command(client, message):
    if not check_auth(message.from_user.id): return
    logs_txt = "\n".join(memory_handler.logs) if memory_handler.logs else "No recent logs."
    await message.reply_text(f"📜 **Recent Logs:**\n`{logs_txt[-3000:]}`")

@app.on_message(filters.command("storage"))
async def storage_command(client, message):
    if not check_auth(message.from_user.id): return
    success, service = get_drive_service()
    if not success: return await message.reply_text("Auth Error.")
    try:
        about = service.about().get(fields="storageQuota").execute()
        quota = about.get('storageQuota', {})
        limit = int(quota.get('limit', 0))
        usage = int(quota.get('usage', 0))
        
        if limit == 0:
            text = f"💾 **Drive Storage:**\n\n**Used:** `{format_size(usage)}`\n**Total:** `Unlimited`"
        else:
            text = f"💾 **Drive Storage:**\n\n**Used:** `{format_size(usage)}`\n**Free:** `{format_size(limit - usage)}`\n**Total:** `{format_size(limit)}`"
        await message.reply_text(text)
    except Exception as e:
        await message.reply_text(f"Storage info fetch failed: {e}")

@app.on_message(filters.command("search"))
async def search_command(client, message):
    if not check_auth(message.from_user.id): return
    query = message.text.split(maxsplit=1)
    if len(query) < 2: 
        return await message.reply_text("⚠️ **Usage:** `/search <filename>`")
        
    keyword = query[1]
    success, service = get_drive_service()
    if not success: return await message.reply_text("Auth Error.")
    
    msg = await message.reply_text("🔍 Searching...")
    try:
        words = keyword.split()
        search_terms = " or ".join([f"name contains '{w}'" for w in words])
        q = f"trashed=false and ({search_terms})"
        
        results = service.files().list(q=q, fields="files(id, name, mimeType, size)", pageSize=20).execute()
        items = results.get('files', [])
        
        if not items:
            return await msg.edit_text("❌ No similar files found.")
            
        MYFILES_CACHE[message.from_user.id] = {
            "items": items, "page": 0, "parent": "search_results", "stack": []
        }
        await render_myfiles_page(msg, message.from_user.id)
    except Exception as e:
        await msg.edit_text(f"Search failed: {get_safe_error(e)}")

# ================= ADVANCED MYFILES =================
@app.on_message(filters.command("myfiles"))
async def myfiles_command(client, message):
    if not check_auth(message.from_user.id): return
    await fetch_and_render_folder(message, message.from_user.id, DRIVE_FOLDER_ID, init=True)

async def fetch_and_render_folder(message_obj_or_query, user_id, folder_id, init=False):
    success, service = get_drive_service()
    if not success: return
    msg = await message_obj_or_query.reply_text("Fetching files...") if init else message_obj_or_query.message
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, orderBy="folder, modifiedTime desc", fields="files(id, name, mimeType, size)", pageSize=100).execute()
        items = results.get('files', [])
        if init:
            MYFILES_CACHE[user_id] = {"items": items, "page": 0, "parent": folder_id, "stack": []}
        else:
            stack = MYFILES_CACHE[user_id]["stack"]
            MYFILES_CACHE[user_id] = {"items": items, "page": 0, "parent": folder_id, "stack": stack}
        await render_myfiles_page(msg, user_id)
    except Exception as e:
        await (msg.edit_text if not init else msg.reply_text)(f"Fetch failed: {get_safe_error(e)}")

async def render_myfiles_page(msg, user_id):
    cache = MYFILES_CACHE.get(user_id)
    if not cache: return await msg.edit_text("Session expired. Type /myfiles again.")
    items = cache["items"]
    page = cache["page"]
    per_page = 3
    total_pages = max(1, (len(items) + per_page - 1) // per_page)
    
    if not items:
        text = "📂 **Folder is empty!**"
        buttons = []
        if cache["stack"]: buttons.append([InlineKeyboardButton("🔼 Back to Parent", callback_data="mf_back")])
        return await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)
        
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, len(items))
    current_items = items[start_idx:end_idx]
    
    text = f"📁 **Files List (Page {page+1}/{total_pages}):**\n\n"
    buttons = []
    
    for i, item in enumerate(current_items):
        is_folder = item['mimeType'] == 'application/vnd.google-apps.folder'
        icon = "📁" if is_folder else "📄"
        idx_in_cache = start_idx + i
        text += f"{i+1}. {icon} `{item['name']}`\n"
        btn_text = f"📂 Open #{i+1}" if is_folder else f"⚙️ Options #{i+1}"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"mf_act|{idx_in_cache}")])

    nav_row = []
    if page > 0: nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="mf_nav|prev"))
    if page < total_pages - 1: nav_row.append(InlineKeyboardButton("Next ➡️", callback_data="mf_nav|next"))
    if nav_row: buttons.append(nav_row)
    if cache["stack"]: buttons.append([InlineKeyboardButton("🔼 Back to Parent", callback_data="mf_back")])
    
    await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# ================= GENERAL COMMANDS =================
@app.on_message(filters.command("start"))
async def start_command(client, message):
    if not check_auth(message.from_user.id): return
    if not GOOGLE_OAUTH_TOKEN:
        return await message.reply_text(f"⚠️ Connect Google Drive first:\n{RENDER_URL}/login")
    await message.reply_text(
        "Hello! 👋\n"
        "Send me a link or file. I support:\n"
        "- Direct Download Links (With Rename/Extract features)\n"
        "- Google Drive File/Folder Links (With Options)\n"
        "- Telegram Files\n\n"
        "**Useful Commands:**\n"
        "`/myfiles` - Advanced Drive Manager\n"
        "`/search <name>` - Search files\n"
        "`/stats` - Session Stats\n"
        "`/storage` - Check Drive space"
    )

@app.on_message(filters.text & ~filters.command(["start", "myfiles", "stats", "logs", "storage", "search"]))
async def handle_text_input(client, message):
    if not check_auth(message.from_user.id): return
    user_id = message.from_user.id
    
    state = USER_STATES.get(user_id)
    if state and state.get("action") == "wait_rename":
        new_name = message.text.strip()
        url = state.get("url")
        del USER_STATES[user_id]
        await message.reply_text(f"Renaming to: `{new_name}`...")
        await process_download(client, message, url, new_name, extract=False)
        return
        
    if state and state.get("action") == "wait_rename_clone":
        new_name = message.text.strip()
        g_id = state.get("gdrive_id")
        f_size = state.get("size", 0)
        del USER_STATES[user_id]
        
        msg = await message.reply_text(f"🔄 Cloning as `{new_name}`...")
        success, result = await clone_gdrive_item(g_id, is_folder=False, new_name=new_name)
        if success:
            file_id = result.get('id')
            await msg.edit_text(generate_result_text(new_name, file_id, f_size, 0))
        else:
            await msg.edit_text(f"❌ Clone Failed.\nReason:\n{result}")
        return

    if state and state.get("action") == "wait_drive_rename":
        new_name = message.text.strip()
        file_id = state.get("file_id")
        del USER_STATES[user_id]
        success, service = get_drive_service()
        if success:
            try:
                service.files().update(fileId=file_id, body={'name': new_name}).execute()
                await message.reply_text(f"✅ Successfully renamed to `{new_name}` in GDrive.")
            except Exception as e:
                await message.reply_text(f"❌ Rename Failed: {get_safe_error(e)}")
        return

    url = message.text
    if not re.match(r"http[s]?://", url): return

    # --- GDRIVE LINK HANDLING ---
    if "drive.google.com" in url:
        g_id, is_folder = extract_gdrive_id(url)
        if g_id:
            if is_folder:
                msg = await message.reply_text(f"🔄 Cloning Folder to Drive...")
                success, result = await clone_gdrive_item(g_id, is_folder=True)
                if success:
                    await msg.edit_text(f"✅ GDrive Folder Clone Complete!\n\nFolder: `{result.get('name')}`")
                else:
                    await msg.edit_text(f"❌ GDrive Clone Failed.\n\nReason:\n{result}")
            else:
                try:
                    success, service = get_drive_service()
                    file_info = service.files().get(fileId=g_id, fields="id, name, size").execute()
                    file_name = file_info.get('name', 'Unknown')
                    file_size = int(file_info.get('size', 0))
                    LINK_CACHE[message.id] = {"url": url, "name": file_name, "gdrive_id": g_id, "size": file_size}
                    
                    buttons = [
                        [InlineKeyboardButton("🔄 Clone Now", callback_data=f"dl_clone|{message.id}"),
                         InlineKeyboardButton("✏️ Rename & Clone", callback_data=f"dl_ren_clone|{message.id}")]
                    ]
                    if file_name.lower().endswith('.zip'):
                        buttons.append([InlineKeyboardButton("📦 Extract to Drive", callback_data=f"dl_ext|{message.id}")])
                        
                    await message.reply_text(
                        f"🔗 **GDrive File Detected!**\n📄 Name: `{file_name}`\n📦 Size: `{format_size(file_size)}`\n\nChoose an action:",
                        reply_markup=InlineKeyboardMarkup(buttons)
                    )
                except Exception as e:
                    await message.reply_text(f"❌ Could not fetch GDrive info. Error:\n{get_safe_error(e)}")
            return

    # --- NORMAL URL HANDLING ---
    wait_msg = await message.reply_text("⏳ Fetching file details...")
    file_name, file_size = await fetch_url_metadata(url)
    
    LINK_CACHE[message.id] = {"url": url, "name": file_name, "size": file_size}
    buttons = [
        [InlineKeyboardButton("⬇️ Download Now", callback_data=f"dl_now|{message.id}"),
         InlineKeyboardButton("✏️ Rename", callback_data=f"dl_ren|{message.id}")]
    ]
    if file_name.lower().endswith('.zip'):
        buttons.append([InlineKeyboardButton("📦 Extract & Upload", callback_data=f"dl_ext|{message.id}")])
        
    await wait_msg.edit_text(
        f"🔗 **Link Detected!**\n📄 Name: `{file_name}`\n📦 Size: `{format_size(file_size) if file_size > 0 else 'Unknown'}`\n\nChoose an action:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ================= CALLBACK HANDLERS =================
@app.on_callback_query()
async def callback_handler(client, query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data.split("|")
    action = data[0]

    if action == "cancel_task":
        task_id = int(data[1])
        if task_id in ACTIVE_TASKS:
            ACTIVE_TASKS[task_id]['cancel'] = True
            await query.answer("Cancelling task...", show_alert=True)
        else:
            await query.answer("Task not found or already finished.", show_alert=True)

    elif action in ["dl_now", "dl_ren", "dl_ext", "dl_clone", "dl_ren_clone"]:
        msg_id = int(data[1])
        link_data = LINK_CACHE.get(msg_id)
        if not link_data:
            return await query.answer("Session expired. Send link again.", show_alert=True)
            
        url = link_data["url"]
        default_name = link_data["name"]
        gdrive_id = link_data.get("gdrive_id")
        await query.message.delete()
        
        if action == "dl_clone":
            msg = await app.send_message(user_id, "🔄 Cloning File to Drive...")
            success, result = await clone_gdrive_item(gdrive_id, is_folder=False, new_name=default_name)
            if success:
                file_id = result.get('id')
                await msg.edit_text(generate_result_text(default_name, file_id, link_data.get("size", 0), 0))
            else:
                await msg.edit_text(f"❌ Clone Failed.\nReason:\n{result}")
                
        elif action == "dl_ren_clone":
            USER_STATES[user_id] = {"action": "wait_rename_clone", "gdrive_id": gdrive_id, "size": link_data.get("size", 0)}
            await app.send_message(user_id, "Please send the **new name** for the file (including extension):")

        elif action == "dl_now": 
            await process_download(client, query.message, url, default_name, extract=False, gdrive_id=gdrive_id)
        elif action == "dl_ext": 
            await process_download(client, query.message, url, default_name, extract=True, gdrive_id=gdrive_id)
        elif action == "dl_ren":
            USER_STATES[user_id] = {"action": "wait_rename", "url": url}
            await app.send_message(user_id, "Please send the **new name** for the file (including extension like .mp4, .zip):")

    elif action == "pv":
        preview_id = data[1]
        paths = PREVIEW_CACHE.get(preview_id, [])
        if paths:
            await query.answer("Uploading Preview Clips...")
            media_group = [InputMediaPhoto(p) for p in paths if os.path.exists(p)]
            if media_group:
                await app.send_media_group(query.message.chat.id, media_group)
            # Cleanup storage after sending!
            for p in paths:
                if os.path.exists(p): os.remove(p)
            del PREVIEW_CACHE[preview_id]
        else:
            await query.answer("Preview expired or not found.", show_alert=True)

    elif action == "mf_nav":
        cache = MYFILES_CACHE.get(user_id)
        if not cache: return await query.answer("Session expired.", show_alert=True)
        if data[1] == "next": cache["page"] += 1
        elif data[1] == "prev": cache["page"] -= 1
        await render_myfiles_page(query.message, user_id)
        
    elif action == "mf_back":
        cache = MYFILES_CACHE.get(user_id)
        if not cache or not cache["stack"]: return
        parent_id = cache["stack"].pop()
        await fetch_and_render_folder(query, user_id, parent_id, init=False)

    elif action == "mf_act":
        cache = MYFILES_CACHE.get(user_id)
        if not cache: return await query.answer("Session expired.", show_alert=True)
        idx = int(data[1])
        item = cache["items"][idx]
        
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            cache["stack"].append(cache["parent"])
            await fetch_and_render_folder(query, user_id, item['id'], init=False)
        else:
            size = int(item.get('size', 0))
            text = generate_result_text(item['name'], item['id'], size, 0)
            buttons = [
                [InlineKeyboardButton("✏️ Rename", callback_data=f"ren_file|{item['id']}"),
                 InlineKeyboardButton("🗑️ Remove", callback_data=f"del_file|{item['id']}")],
                [InlineKeyboardButton("🔙 Back to List", callback_data="mf_ret")]
            ]
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    elif action == "mf_ret":
        await render_myfiles_page(query.message, user_id)

    elif action == "del_file":
        file_id = data[1]
        success, service = get_drive_service()
        if success:
            try:
                service.files().delete(fileId=file_id).execute()
                await query.answer("File Deleted from GDrive!", show_alert=True)
                await render_myfiles_page(query.message, user_id)
            except Exception as e:
                await query.answer(f"Failed: {get_safe_error(e)}", show_alert=True)
                
    elif action == "ren_file":
        file_id = data[1]
        USER_STATES[user_id] = {"action": "wait_drive_rename", "file_id": file_id}
        await query.answer("Check your messages to rename.", show_alert=False)
        await app.send_message(user_id, "Please send the **new name** for this Google Drive file:")

async def process_download(client, message, url, file_name, extract=False, gdrive_id=None):
    msg = await app.send_message(message.chat.id, "📥 Starting process...")
    file_path = os.path.join(os.getcwd(), file_name)
    start_time = time.time()
    ACTIVE_TASKS[msg.id] = {'cancel': False}
    
    try:
        if gdrive_id:
            await download_from_gdrive(gdrive_id, file_path, msg)
        else:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded = 0
                    with open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(2 * 1024 * 1024):
                            if ACTIVE_TASKS.get(msg.id, {}).get('cancel'):
                                raise Exception("TaskCancelled")
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                await update_progress(downloaded, total_size, msg, start_time, "📥 Downloading to server...")
                                
        preview_id = None
        if file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
            await msg.edit_text("🎬 Extracting Video Clips...")
            preview_id = str(int(time.time()))
            paths = generate_video_preview(file_path, "./previews", preview_id)
            if paths:
                PREVIEW_CACHE[preview_id] = paths
            else:
                preview_id = None

        if extract and file_name.lower().endswith('.zip'):
            await msg.edit_text("📦 Extracting ZIP file...")
            extract_dir = os.path.join(os.getcwd(), file_name + "_extracted")
            os.makedirs(extract_dir, exist_ok=True)
            
            success, ext_result = await asyncio.to_thread(extract_zip, file_path, extract_dir)
            if not success:
                os.remove(file_path)
                shutil.rmtree(extract_dir, ignore_errors=True)
                return await msg.edit_text(f"❌ Extraction Failed:\n`{ext_result}`")
                
            await msg.edit_text("📁 Creating folder in Drive and uploading contents...")
            folder_name = file_name.replace(".zip", "")
            new_folder_id = create_gdrive_folder(folder_name)
            
            total_files_uploaded = 0
            for root, dirs, files in os.walk(extract_dir):
                for fname in files:
                    if ACTIVE_TASKS.get(msg.id, {}).get('cancel'): raise Exception("TaskCancelled")
                    fpath = os.path.join(root, fname)
                    await msg.edit_text(f"☁️ Uploading extracted file: `{fname}`...")
                    up_success, _, _, _ = await upload_to_drive_async(fpath, fname, msg, parent_id=new_folder_id)
                    if up_success: total_files_uploaded += 1
                        
            elapsed = time.time() - start_time
            text = generate_result_text(folder_name, new_folder_id, 0, elapsed, is_folder=True)
            await msg.edit_text(text)
            
            os.remove(file_path)
            shutil.rmtree(extract_dir, ignore_errors=True)
            return

        success, file_id, file_size, upload_time = await upload_to_drive_async(file_path, file_name, msg)
        
        if success:
            total_elapsed = time.time() - start_time
            text = generate_result_text(file_name, file_id, file_size, total_elapsed)
            reply_markup = None
            if preview_id:
                reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🎬 View Previews (10 Clips)", callback_data=f"pv|{preview_id}")]])
            await msg.edit_text(text, reply_markup=reply_markup)
        else:
            await msg.edit_text(f"❌ Upload failed.\n\n⚠️ Reason:\n{file_id}")
            
    except OSError as e: 
        await msg.edit_text(f"❌ Server Storage Full!\n⚠️ Reason:\n{get_safe_error(e)}")
    except Exception as e:
        if str(e) == "TaskCancelled":
            await msg.edit_text("🚫 **Task Cancelled by User.**")
        else:
            await msg.edit_text(f"❌ Error:\n{get_safe_error(e)}")
    finally:
        ACTIVE_TASKS.pop(msg.id, None)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.document | filters.video | filters.audio)
async def handle_telegram_files(client, message):
    if not check_auth(message.from_user.id): return
    if not GOOGLE_OAUTH_TOKEN: return await message.reply_text("Connect Google Drive first.")
    
    msg = await message.reply_text("📥 Preparing Telegram download...")
    start_time = time.time()
    ACTIVE_TASKS[msg.id] = {'cancel': False}
    file_path = None
    
    try:
        file_path = await message.download(progress=update_progress, progress_args=(msg, start_time, "📥 Downloading from Telegram..."))
        if not file_path: raise Exception("Download failed.")
        file_name = os.path.basename(file_path)
        
        preview_id = None
        if file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
            await msg.edit_text("🎬 Extracting Video Clips...")
            preview_id = str(int(time.time()))
            paths = generate_video_preview(file_path, "./previews", preview_id)
            if paths:
                PREVIEW_CACHE[preview_id] = paths
            else:
                preview_id = None

        success, file_id, file_size, up_time = await upload_to_drive_async(file_path, file_name, msg)
        
        if success:
            total_elapsed = time.time() - start_time
            text = generate_result_text(file_name, file_id, file_size, total_elapsed)
            reply_markup = None
            if preview_id:
                reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🎬 View Previews (10 Clips)", callback_data=f"pv|{preview_id}")]])
            await msg.edit_text(text, reply_markup=reply_markup)
        else:
            await msg.edit_text(f"❌ Upload failed.\nReason: {file_id}")
    except Exception as e:
        if str(e) == "TaskCancelled":
            await msg.edit_text("🚫 **Task Cancelled by User.**")
        else:
            await msg.edit_text(f"❌ Error:\n{get_safe_error(e)}")
    finally:
        ACTIVE_TASKS.pop(msg.id, None)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

# ================= WEB SERVER =================
async def handle_home(request): return web.Response(text="Bot is running! Go to /login to authorize.")
async def handle_login(request):
    global oauth_flow
    flow = Flow.from_client_config(json.loads(GOOGLE_CLIENT_SECRET), scopes=SCOPES)
    oauth_flow = flow
    flow.redirect_uri = f"https://{request.host}/callback"
    auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
    return web.HTTPFound(auth_url)

async def handle_callback(request):
    global oauth_flow
    code = request.query.get('code')
    oauth_flow.fetch_token(code=code)
    creds = oauth_flow.credentials
    token_data = {'token': creds.token, 'refresh_token': creds.refresh_token, 'token_uri': creds.token_uri, 'client_id': creds.client_id, 'client_secret': creds.client_secret, 'scopes': creds.scopes}
    oauth_flow = None
    return web.Response(text=f"SUCCESS! Copy & Paste in Render as GOOGLE_OAUTH_TOKEN:\n\n{json.dumps(token_data)}")

async def start_web_server():
    app_web = web.Application()
    app_web.router.add_get('/', handle_home)
    app_web.router.add_get('/login', handle_login)
    app_web.router.add_get('/callback', handle_callback)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 8080)))
    await site.start()

async def main():
    await start_web_server()
    await app.start()
    logger.info("Mega Update Bot is LIVE! 🚀")
    await idle()
    await app.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())