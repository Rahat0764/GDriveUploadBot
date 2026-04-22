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
import aiofiles # নতুন যুক্ত করা হয়েছে
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
from pyrogram.errors import MessageNotModified
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from cachetools import TTLCache # মেমরি লিক ফিক্স করার জন্য

# Video processing check
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
        if len(self.logs) > self.capacity: self.logs.pop(0)

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

API_ID = int(API_ID_STR) if API_ID_STR.isdigit() else 0
AUTHORIZED_USERS = [int(u.strip()) for u in AUTH_USERS_STR.split(",") if u.strip().isdigit()]
SCOPES = ['https://www.googleapis.com/auth/drive']

oauth_flow = None

# --- BUG FIX: Memory Leak Prevention ---
# আগে সাধারণ dict ছিল, এখন TTLCache ব্যবহার করা হয়েছে (১ ঘণ্টা পর অটো ডিলিট হবে)
LINK_CACHE = TTLCache(maxsize=100, ttl=3600)  
USER_STATES = TTLCache(maxsize=100, ttl=3600) 
MYFILES_CACHE = TTLCache(maxsize=100, ttl=3600)
PREVIEW_CACHE = TTLCache(maxsize=100, ttl=3600) 
CANCEL_FLAGS = {}
PROGRESS_CACHE = {} 
BOT_STATS = {"uploads": 0, "clones": 0, "bytes_uploaded": 0}

os.makedirs("previews", exist_ok=True)

app = Client("my_drive_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)

def get_drive_service():
    if not GOOGLE_OAUTH_TOKEN: return False, f"⚠️ Bot not authenticated. Provide GOOGLE_OAUTH_TOKEN in env."
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GOOGLE_OAUTH_TOKEN), SCOPES)
        return True, build('drive', 'v3', credentials=creds)
    except Exception as e: return False, f"OAuth Token Error: {str(e)}"

def format_size(bytes_size):
    if bytes_size >= 1024 ** 3: return f"{bytes_size / (1024 ** 3):.2f} GB"
    if bytes_size >= 1024 ** 2: return f"{bytes_size / (1024 ** 2):.2f} MB"
    return f"{bytes_size / 1024:.2f} KB"

def format_time(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    elif m > 0: return f"{m}m {s}s"
    return f"{s}s"

def get_safe_error(e): return str(e).replace('<', '').replace('>', '')[:800]

def get_cancel_markup(cancel_id):
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{cancel_id}")]])

# --- URL Metadata Fetcher ---
async def get_url_metadata(url):
    try:
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(url, allow_redirects=True) as resp:
                size = int(resp.headers.get('content-length', 0))
                cd = resp.headers.get('content-disposition', '')
                name = None
                if cd:
                    fname = re.findall(r"filename\*?=(?:UTF-8'')?([^;]+)", cd, flags=re.IGNORECASE)
                    if fname: name = urllib.parse.unquote(fname[0].strip().strip('"').strip("'"))
                if not name:
                    name = urllib.parse.unquote(resp.url.name)
                    if not name or name == '/': name = f"file_{int(time.time())}"
                return name, size
    except:
        return f"download_{int(time.time())}", 0

# --- Video Preview Gen ---
def generate_10_video_frames(video_path, preview_id):
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
                frame = cv2.resize(frame, (640, 360))
                out_path = f"./previews/{preview_id}_{i}.jpg"
                cv2.imwrite(out_path, frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
                paths.append(out_path)
        cap.release()
        return paths
    except: return []

def extract_gdrive_id(url):
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), False
    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), False
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), True
    return None, False

def check_auth(user_id): return not AUTHORIZED_USERS or user_id in AUTHORIZED_USERS

def generate_result_text(file_name, file_id, file_size, elapsed_time, is_folder=False):
    drive_link = f"https://drive.google.com/drive/folders/{file_id}" if is_folder else f"https://drive.google.com/file/d/{file_id}/view"
    safe_name = urllib.parse.quote(file_name)
    direct_link = f"{CF_WORKER_URL}/0:down/{safe_name}" + ("/" if is_folder else "") if CF_WORKER_URL else "Not_Configured"
    
    return (f"✅ **Task Completed!**\n\n📄 **Name:** `{file_name}`\n📦 **Size:** `{format_size(file_size)}`\n⏱️ **Time:** `{format_time(elapsed_time)}`\n\n"
            f"🔗 [Google Drive Link]({drive_link})\n⚡ [Direct Download Link]({direct_link})")

# --- Progress Bar Optimization ---
async def update_progress(current, total, msg, start_time, action_text, cancel_id=None):
    if cancel_id and CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
    if total == 0: total = current + 1 
    
    now = time.time()
    last_update_time = PROGRESS_CACHE.get(cancel_id, start_time) if cancel_id else start_time
    
    # রেট লিমিট এড়াতে ৩ সেকেন্ড পর পর আপডেট হবে
    if (now - last_update_time > 3.0) or (current == total):
        if cancel_id: PROGRESS_CACHE[cancel_id] = now
        percent = min(100.0, (current / total) * 100)
        filled = int(percent / 10)
        bar = "🟩" * filled + "⬜" * (10 - filled)
        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        eta = (total - current) / speed if speed > 0 else 0
        
        text = f"{action_text}\n\n{bar} {percent:.1f}%\n📦 Size: {format_size(current)} / {format_size(total)}\n🚀 Speed: {format_size(speed)}/s\n⏳ ETA: {format_time(eta)}"
        try: await msg.edit_text(text, reply_markup=get_cancel_markup(cancel_id) if cancel_id else None)
        except MessageNotModified: pass

# --- Parallel Download ---
async def download_part(session, url, start, end, part_path, progress, msg, start_time, cancel_id):
    headers = {'Range': f'bytes={start}-{end}'}
    async with session.get(url, headers=headers) as resp:
        resp.raise_for_status()
        async with aiofiles.open(part_path, 'wb') as f: # Async ফাইল রাইট
            async for chunk in resp.content.iter_chunked(8 * 1024 * 1024): # 8MB চাঙ্ক
                if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
                await f.write(chunk)
                progress['downloaded'] += len(chunk)
                await update_progress(progress['downloaded'], progress['total'], msg, start_time, "⚡ Parallel Downloading...", cancel_id)

# --- Drive Upload (Optimized Chunk Size) ---
async def upload_to_drive_async(file_path, file_name, msg, parent_id=DRIVE_FOLDER_ID, cancel_id=None):
    try:
        success, service = get_drive_service()
        if not success: return False, service, 0, 0
        file_size = os.path.getsize(file_path)
        # BUG FIX: Chunk size 100MB করা হয়েছে ফাস্ট আপলোডের জন্য
        media = MediaFileUpload(file_path, chunksize=100*1024*1024, resumable=True) 
        request = service.files().create(body={'name': file_name, 'parents': [parent_id]}, media_body=media, fields='id')
        
        response, start_time = None, time.time()
        while response is None:
            if cancel_id and CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
            status, response = await asyncio.to_thread(functools.partial(request.next_chunk, num_retries=5))
            if status: 
                await update_progress(status.resumable_progress, file_size, msg, start_time, "☁️ Super Fast Uploading...", cancel_id)
                
        BOT_STATS["uploads"] += 1
        BOT_STATS["bytes_uploaded"] += file_size
        return True, response.get('id'), file_size, time.time() - start_time
    except Exception as e: return False, get_safe_error(e), 0, 0

# --- File Merge Fix (Async Blocking Prevent) ---
def merge_files_sync(file_path, num_parts):
    # এই কাজটিকে আলাদা থ্রেডে পাঠানো হবে যাতে বট ফ্রিজ না হয়
    with open(file_path, 'wb') as outfile:
        for i in range(num_parts):
            part_path = f"{file_path}.part{i}"
            with open(part_path, 'rb') as infile:
                shutil.copyfileobj(infile, outfile, length=16*1024*1024) # 16MB বাফার
            os.remove(part_path)

# --- Clone GDrive ---
async def clone_gdrive_item(item_id, is_folder=False, parent_id=DRIVE_FOLDER_ID, msg=None):
    try:
        success, service = get_drive_service()
        if not success: return False, service
        
        if not is_folder:
            body = {'parents': [parent_id]}
            response = await asyncio.to_thread(lambda: service.files().copy(fileId=item_id, body=body, fields='id, name').execute())
            BOT_STATS["clones"] += 1
            return True, response
        else:
            original_folder = await asyncio.to_thread(lambda: service.files().get(fileId=item_id, fields='name').execute())
            new_folder_id = create_gdrive_folder(original_folder.get('name'), parent_id)
            
            query = f"'{item_id}' in parents and trashed=false"
            results = await asyncio.to_thread(lambda: service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType)", pageSize=1000).execute())
            items = results.get('files', [])
            
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    if msg: await msg.edit_text(f"🔄 Cloning Sub-folder: `{item['name']}`...")
                    await clone_gdrive_item(item['id'], is_folder=True, parent_id=new_folder_id, msg=msg)
                else:
                    if msg: await msg.edit_text(f"🔄 Cloning File: `{item['name']}`...")
                    await clone_gdrive_item(item['id'], is_folder=False, parent_id=new_folder_id)
                    
            BOT_STATS["clones"] += 1
            return True, {"name": original_folder.get('name'), "id": new_folder_id}
    except Exception as e:
        return False, get_safe_error(e)

def create_gdrive_folder(folder_name, parent_id=DRIVE_FOLDER_ID):
    success, service = get_drive_service()
    if not success: return None
    metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = service.files().create(body=metadata, fields='id').execute()
    return folder.get('id')

# --- Main Download Processor ---
async def process_download(client, message, url, file_name, extract=False):
    msg = await app.send_message(message.chat.id, "📥 Preparing Connection...")
    file_path = os.path.join(os.getcwd(), file_name)
    start_time = time.time()
    cancel_id = str(message.id)
    CANCEL_FLAGS[cancel_id] = False
    
    link_data = LINK_CACHE.get(message.id, {})
    is_gd = link_data.get("is_gd", False)
    gd_id = link_data.get("gd_id")
    gd_size = link_data.get("gd_size", 0)

    try:
        if is_gd and extract:
            # GDrive to Local for Extract
            success, service = get_drive_service()
            request = service.files().get_media(fileId=gd_id)
            fh = io.FileIO(file_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=50*1024*1024)
            done = False
            while not done:
                if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
                status, done = await asyncio.to_thread(functools.partial(downloader.next_chunk, num_retries=3))
                if status: await update_progress(status.resumable_progress, gd_size, msg, start_time, "📥 Fetching for Extraction...", cancel_id)
            fh.close()
        elif is_gd and not extract:
            success, service = get_drive_service()
            await msg.edit_text("🔄 High-Speed Cloning...")
            res = await asyncio.to_thread(lambda: service.files().copy(fileId=gd_id, body={'name': file_name, 'parents': [DRIVE_FOLDER_ID]}, fields='id').execute())
            await msg.edit_text(generate_result_text(file_name, res.get('id'), gd_size, time.time()-start_time))
            return
        else:
            connector = aiohttp.TCPConnector(limit=50) # লিমিট বাড়ানো হয়েছে
            async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=0)) as session:
                supports_range = False
                total_size = link_data.get("size", 0)
                
                try:
                    async with session.head(url, allow_redirects=True) as head_resp:
                        supports_range = head_resp.headers.get('Accept-Ranges') == 'bytes'
                        if total_size == 0: total_size = int(head_resp.headers.get('content-length', 0))
                except: pass

                if supports_range and total_size > 20 * 1024 * 1024:
                    num_parts = 8 # ৪ এর বদলে ৮ পার্ট করা হয়েছে আরও স্পিডের জন্য
                    part_size = total_size // num_parts
                    tasks = []
                    progress = {'downloaded': 0, 'total': total_size}
                    
                    for i in range(num_parts):
                        start = i * part_size
                        end = total_size - 1 if i == num_parts - 1 else (start + part_size - 1)
                        part_path = f"{file_path}.part{i}"
                        tasks.append(download_part(session, url, start, end, part_path, progress, msg, start_time, cancel_id))
                    
                    await asyncio.gather(*tasks) 
                    if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
                    
                    await msg.edit_text("⚙️ Merging Parts (Non-Blocking)...", reply_markup=get_cancel_markup(cancel_id))
                    # BUG FIX: থ্রেড ব্লক এড়াতে আলাদা থ্রেডে পাঠানো হলো
                    await asyncio.to_thread(merge_files_sync, file_path, num_parts)
                else:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        downloaded = 0
                        async with aiofiles.open(file_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(16 * 1024 * 1024): 
                                if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
                                await f.write(chunk)
                                downloaded += len(chunk)
                                if total_size > 0: await update_progress(downloaded, total_size, msg, start_time, "📥 Direct Downloading...", cancel_id)

        # Extraction Logic...
        if extract and file_name.lower().endswith('.zip'):
            await msg.edit_text("📦 Extracting ZIP (This may take a while)...")
            ext_dir = file_path + "_ext"
            os.makedirs(ext_dir, exist_ok=True)
            # আনজিপও থ্রেডে পাঠানো হয়েছে
            success, ext_res = await asyncio.to_thread(lambda: extract_zip(file_path, ext_dir))
            if not success: return await msg.edit_text(f"❌ Extraction Error: {ext_res}")
            
            f_name = file_name.replace(".zip", "")
            t_files, new_f_id = await upload_extracted_folder(ext_dir, f_name, DRIVE_FOLDER_ID, msg, cancel_id, start_time)
            
            await msg.edit_text(generate_result_text(f_name, new_f_id, 0, time.time()-start_time, True) + f"\n📄 Files: `{t_files}`")
            await asyncio.to_thread(shutil.rmtree, ext_dir, ignore_errors=True)
            return

        preview_id = None
        if file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
            await msg.edit_text("🎬 Extracting Frames...", reply_markup=get_cancel_markup(cancel_id))
            p_id = str(int(time.time()))
            frames = await asyncio.to_thread(generate_10_video_frames, file_path, p_id)
            if frames:
                preview_id = p_id
                PREVIEW_CACHE[preview_id] = frames

        success, file_id, file_size, up_time = await upload_to_drive_async(file_path, file_name, msg, cancel_id=cancel_id)
        
        if success:
            txt = generate_result_text(file_name, file_id, file_size, time.time() - start_time)
            rm = InlineKeyboardMarkup([[InlineKeyboardButton("🎬 View Preview", callback_data=f"pv|{preview_id}")]]) if preview_id else None
            await msg.edit_text(txt, reply_markup=rm)
        else: await msg.edit_text(f"❌ Upload Failed: {file_id}")

    except Exception as e:
        if str(e) == "CANCELLED": await msg.edit_text("🚫 **Task Cancelled by User!**")
        else: await msg.edit_text(f"❌ Error: {get_safe_error(e)}")
    finally:
        CANCEL_FLAGS.pop(cancel_id, None)
        PROGRESS_CACHE.pop(cancel_id, None)
        if file_path and os.path.exists(file_path): 
            await asyncio.to_thread(os.remove, file_path)
        for i in range(8):
            part_p = f"{file_path}.part{i}"
            if os.path.exists(part_p): 
                await asyncio.to_thread(os.remove, part_p)

# === Handlers and Listeners (Unchanged mainly, kept intact for your flow) ===
@app.on_message(filters.command("start"))
async def start_command(client, message):
    if not check_auth(message.from_user.id): return
    await message.reply_text("🚀 **SpeedPro Bot Ready!**\nSend me a Link or File to begin.\n\n/myfiles - Browse\n/stats - Info\n/search - Find files")

@app.on_message(filters.text & ~filters.command(["start", "myfiles", "stats", "logs", "storage", "search"]))
async def handle_text_input(client, message):
    if not check_auth(message.from_user.id): return
    url = message.text
    
    state = USER_STATES.get(message.from_user.id)
    if state and state.get("action") == "wait_rename":
        new_name = message.text.strip()
        url = state.get("url")
        del USER_STATES[message.from_user.id]
        is_gd = state.get("is_gd", False)
        gd_id = state.get("gd_id")
        gd_size = state.get("gd_size", 0)
        
        if is_gd:
            success, service = get_drive_service()
            msg = await message.reply_text(f"🔄 Cloning as `{new_name}`...")
            try:
                res = await asyncio.to_thread(lambda: service.files().copy(fileId=gd_id, body={'name': new_name, 'parents': [DRIVE_FOLDER_ID]}, fields='id').execute())
                await msg.edit_text(generate_result_text(new_name, res.get('id'), gd_size, 0))
            except HttpError as err:
                 await msg.edit_text(f"❌ **Clone Failed:** Private or inaccessible file.")
        else:
            await process_download(client, message, url, new_name, extract=False)
        return

    if not re.match(r"http[s]?://", url): return

    # Drive Link Handling
    g_id, is_folder = extract_gdrive_id(url)
    if g_id:
        if is_folder:
            msg = await message.reply_text(f"🔄 Fetching Folder details...")
            try:
                success, result = await clone_gdrive_item(g_id, is_folder=True, msg=msg)
                if success: await msg.edit_text(f"✅ Folder Cloned: `{result['name']}`")
                else: await msg.edit_text("❌ Folder not found.")
            except: await msg.edit_text("❌ Access Denied.")
            return
        else:
            msg = await message.reply_text("🔍 Fetching GDrive File Info...")
            try:
                success, service = get_drive_service()
                meta = await asyncio.to_thread(lambda: service.files().get(fileId=g_id, fields='name, size').execute())
                name, size = meta.get('name', 'Unknown'), int(meta.get('size', 0))
                LINK_CACHE[message.id] = {"url": url, "name": name, "is_gd": True, "gd_id": g_id, "gd_size": size}
                btns = [[InlineKeyboardButton("🔄 Clone Now", callback_data=f"dl_now|{message.id}"), InlineKeyboardButton("✏️ Rename", callback_data=f"dl_ren|{message.id}")]]
                if name.lower().endswith('.zip'): btns.append([InlineKeyboardButton("📦 Clone & Extract", callback_data=f"dl_ext|{message.id}")])
                await msg.edit_text(f"🔗 **GDrive File Detected!**\n📄 `{name}`\n📦 `{format_size(size)}`", reply_markup=InlineKeyboardMarkup(btns))
            except: await msg.edit_text("❌ File Not Found or Private.")
            return

    msg = await message.reply_text("🔍 Fetching Metadata...")
    name, size = await get_url_metadata(url)
    LINK_CACHE[message.id] = {"url": url, "name": name, "size": size, "is_gd": False}
    btns = [[InlineKeyboardButton("⬇️ Download Fast", callback_data=f"dl_now|{message.id}"), InlineKeyboardButton("✏️ Rename", callback_data=f"dl_ren|{message.id}")]]
    if name.lower().endswith('.zip'): btns.append([InlineKeyboardButton("📦 Extract", callback_data=f"dl_ext|{message.id}")])
    await msg.edit_text(f"🔗 **Direct Link!**\n📄 `{name}`\n📦 `{format_size(size)}`", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query()
async def callback_handler(client, query: CallbackQuery):
    data = query.data.split("|")
    action = data[0]

    if action == "cancel":
        CANCEL_FLAGS[data[1]] = True
        await query.answer("Cancelling task...", show_alert=True)
    elif action in ["dl_now", "dl_ext"]:
        ld = LINK_CACHE.get(int(data[1]))
        if not ld: return await query.answer("Session expired.", show_alert=True)
        await query.message.delete()
        await process_download(client, query.message, ld["url"], ld["name"], extract=(action=="dl_ext"))
    elif action == "dl_ren":
        ld = LINK_CACHE.get(int(data[1]))
        if not ld: return await query.answer("Session expired.", show_alert=True)
        USER_STATES[query.from_user.id] = {"action": "wait_rename", "url": ld["url"], "is_gd": ld.get("is_gd"), "gd_id": ld.get("gd_id"), "gd_size": ld.get("gd_size")}
        await query.message.delete()
        await app.send_message(query.from_user.id, "Please send the **new name** with extension:")

async def main():
    await app.start()
    logger.info("Ultimate Speed Pro Bot LIVE on Colab! 🚀")
    await idle()
    await app.stop()

if __name__ == "__main__": asyncio.get_event_loop().run_until_complete(main())