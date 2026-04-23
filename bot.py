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

try: asyncio.get_event_loop()
except RuntimeError: asyncio.set_event_loop(asyncio.new_event_loop())

import aiohttp
import aiofiles
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
from pyrogram.errors import MessageNotModified
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from cachetools import TTLCache

try:
    import cv2
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False

# ================= ENVIRONMENT VARIABLES =================
API_ID = int(os.environ.get("API_ID", "0").strip())
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_OAUTH_TOKEN = os.environ.get("GOOGLE_OAUTH_TOKEN", "").strip()
AUTH_USERS_STR = os.environ.get("AUTHORIZED_USERS", "")
CF_WORKER_URL = os.environ.get("CF_WORKER_URL", "").strip().rstrip('/')
PORT = int(os.environ.get("PORT", 8080))

AUTHORIZED_USERS = [int(u.strip()) for u in AUTH_USERS_STR.split(",") if u.strip().isdigit()]
SCOPES = ['https://www.googleapis.com/auth/drive']

# ================= LINUX CORE SETUP (100% RELIABLE) =================
def setup_binaries_and_config():
    print("⚙️ Initializing Linux Server Environment...")
    
    # 1. Rclone Config Generation
    if GOOGLE_OAUTH_TOKEN:
        try:
            td = json.loads(GOOGLE_OAUTH_TOKEN)
            rclone_token = {
                "access_token": td.get("token", ""),
                "token_type": "Bearer",
                "refresh_token": td.get("refresh_token", ""),
                "expiry": "2030-01-01T00:00:00.000000000Z" # Force refresh if needed
            }
            conf = f"[gdrive]\ntype = drive\nclient_id = {td.get('client_id','')}\nclient_secret = {td.get('client_secret','')}\nscope = drive\ntoken = {json.dumps(rclone_token)}\nroot_folder_id = {DRIVE_FOLDER_ID}\n"
            with open("rclone.conf", "w") as f: f.write(conf)
            print("✅ Rclone Config Generated!")
        except Exception as e: print("❌ Rclone Config Error:", e)

    # 2. Download and Setup Using Native Linux Shell (Fixes all 404/bzip2 errors)
    if not os.path.exists("./rclone"):
        print("⬇️ Shell Downloading Rclone...")
        os.system('wget -qO rclone.zip "https://downloads.rclone.org/rclone-current-linux-amd64.zip"')
        os.system('unzip -qo rclone.zip')
        os.system('mv rclone-*-linux-amd64/rclone ./rclone 2>/dev/null')
        os.system('chmod +x ./rclone')
        os.system('rm -rf rclone.zip rclone-*-linux-amd64')

    if not os.path.exists("./aria2c"):
        print("⬇️ Shell Downloading Aria2c...")
        os.system('wget -qO aria2.tar.bz2 "https://github.com/q3aql/aria2-static-builds/releases/download/v1.36.0/aria2-1.36.0-linux-gnu-64bit-build1.tar.bz2"')
        os.system('tar -xjf aria2.tar.bz2')
        os.system('mv aria2-1.36.0-linux-gnu-64bit-build1/aria2c ./aria2c 2>/dev/null')
        os.system('chmod +x ./aria2c')
        os.system('rm -rf aria2.tar.bz2 aria2-1.36.0-linux-gnu-64bit-build1')

    if os.path.exists("./aria2c"): print("✅ Aria2c Ready!")
    else: print("❌ Aria2c Setup Failed!")
        
    if os.path.exists("./rclone"): print("✅ Rclone Ready!")
    else: print("❌ Rclone Setup Failed!")

setup_binaries_and_config()

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

# Caches
LINK_CACHE = TTLCache(maxsize=100, ttl=3600)  
USER_STATES = TTLCache(maxsize=100, ttl=3600) 
MYFILES_CACHE = TTLCache(maxsize=100, ttl=3600)
PREVIEW_CACHE = TTLCache(maxsize=100, ttl=3600) 
CANCEL_FLAGS = {}
BOT_STATS = {"uploads": 0, "clones": 0, "bytes_uploaded": 0}

app = Client("my_drive_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
oauth_flow = None

def get_drive_service():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GOOGLE_OAUTH_TOKEN), SCOPES)
        return True, build('drive', 'v3', credentials=creds)
    except Exception as e: return False, f"OAuth Error: {str(e)}"

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
def get_cancel_markup(cancel_id): return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{cancel_id}")]])
def check_auth(user_id): return not AUTHORIZED_USERS or user_id in AUTHORIZED_USERS

def generate_result_text(file_name, file_id, file_size, elapsed_time, is_folder=False):
    drive_link = f"https://drive.google.com/drive/folders/{file_id}" if is_folder else f"https://drive.google.com/file/d/{file_id}/view"
    direct_link = "Not Configured"
    if CF_WORKER_URL:
        safe_name = urllib.parse.quote(file_name)
        base_url = CF_WORKER_URL if CF_WORKER_URL.endswith("/") else CF_WORKER_URL + "/"
        if "0:/" not in base_url: base_url += "0:/"
        direct_link = f"{base_url}{safe_name}" + ("/" if is_folder else "")
    
    return (f"✅ **Task Completed!**\n\n📄 **Name:** `{file_name}`\n📦 **Size:** `{format_size(file_size)}`\n⏱️ **Time:** `{format_time(elapsed_time)}`\n\n"
            f"🔗 [Google Drive Link]({drive_link})\n⚡ [Direct Download Link]({direct_link})")

async def get_url_metadata(url):
    try:
        connector = aiohttp.TCPConnector(limit=5)
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
    except: return f"download_{int(time.time())}", 0

def extract_gdrive_id(url):
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), False
    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), False
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), True
    return None, False

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

# ================= CORE: ENGINES & FALLBACKS =================
async def update_progress(current, total, msg, start_time, action_text, cancel_id=None):
    if cancel_id and CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
    if total == 0: total = current + 1 
    now = time.time()
    
    if (now - getattr(update_progress, "last_time", 0) > 3.0) or (current == total):
        update_progress.last_time = now
        percent = min(100.0, (current / total) * 100)
        filled = int(percent / 10)
        bar = "🟩" * filled + "⬜" * (10 - filled)
        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        eta = (total - current) / speed if speed > 0 else 0
        
        text = f"{action_text}\n\n{bar} {percent:.1f}%\n📦 Size: {format_size(current)} / {format_size(total)}\n🚀 Speed: {format_size(speed)}/s\n⏳ ETA: {format_time(eta)}"
        try: await msg.edit_text(text, reply_markup=get_cancel_markup(cancel_id) if cancel_id else None)
        except MessageNotModified: pass

# --- NATIVE DOWNLOAD ---
async def download_part(session, url, start, end, part_path, progress, msg, start_time, cancel_id):
    headers = {'Range': f'bytes={start}-{end}'}
    async with session.get(url, headers=headers) as resp:
        resp.raise_for_status()
        async with aiofiles.open(part_path, 'wb') as f:
            async for chunk in resp.content.iter_chunked(2 * 1024 * 1024):
                if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
                await f.write(chunk)
                progress['downloaded'] += len(chunk)
                await update_progress(progress['downloaded'], progress['total'], msg, start_time, "⚡ Native Downloading...", cancel_id)

def merge_files_sync(file_path, num_parts):
    with open(file_path, 'wb') as outfile:
        for i in range(num_parts):
            part_path = f"{file_path}.part{i}"
            with open(part_path, 'rb') as infile: shutil.copyfileobj(infile, outfile, length=4*1024*1024)
            os.remove(part_path)

async def download_with_python_native(url, file_path, msg, cancel_id, link_data):
    start_time = time.time()
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=0)) as session:
        supports_range = False
        total_size = link_data.get("size", 0)
        try:
            async with session.head(url, allow_redirects=True) as head_resp:
                supports_range = head_resp.headers.get('Accept-Ranges') == 'bytes'
                if total_size == 0: total_size = int(head_resp.headers.get('content-length', 0))
        except: pass

        if supports_range and total_size > 10 * 1024 * 1024:
            num_parts = 4 
            part_size = total_size // num_parts
            tasks, progress = [], {'downloaded': 0, 'total': total_size}
            for i in range(num_parts):
                start = i * part_size
                end = total_size - 1 if i == num_parts - 1 else (start + part_size - 1)
                part_path = f"{file_path}.part{i}"
                tasks.append(download_part(session, url, start, end, part_path, progress, msg, start_time, cancel_id))
            await asyncio.gather(*tasks) 
            if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
            await msg.edit_text("⚙️ Merging Parts...", reply_markup=get_cancel_markup(cancel_id))
            await asyncio.to_thread(merge_files_sync, file_path, num_parts)
        else:
            async with session.get(url) as response:
                response.raise_for_status()
                downloaded = 0
                async with aiofiles.open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(2 * 1024 * 1024): 
                        if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0: await update_progress(downloaded, total_size, msg, start_time, "📥 Native Downloading...", cancel_id)

# --- NATIVE UPLOAD ---
async def upload_with_python_native(file_path, file_name, msg, parent_id=DRIVE_FOLDER_ID, cancel_id=None, start_time=None):
    if not start_time: start_time = time.time()
    success, service = get_drive_service()
    if not success: return False, service, 0, 0
    file_size = os.path.getsize(file_path)
    media = MediaFileUpload(file_path, chunksize=20*1024*1024, resumable=True) 
    request = service.files().create(body={'name': file_name, 'parents': [parent_id]}, media_body=media, fields='id')
    
    response = None
    while response is None:
        if cancel_id and CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
        status, response = await asyncio.to_thread(functools.partial(request.next_chunk, num_retries=5))
        if status: await update_progress(status.resumable_progress, file_size, msg, start_time, "☁️ Native Uploading...", cancel_id)
            
    BOT_STATS["uploads"] += 1
    BOT_STATS["bytes_uploaded"] += file_size
    return True, response.get('id'), file_size, time.time() - start_time

# --- ARIA2 DOWNLOAD ---
async def download_with_aria2(url, file_path, msg, cancel_id):
    dir_name = os.path.dirname(file_path) if os.path.dirname(file_path) else "."
    file_name = os.path.basename(file_path)
    
    cmd = ["./aria2c", "--dir", dir_name, "--out", file_name, "--split=4", "--max-connection-per-server=4", "--min-split-size=10M", "--summary-interval=3", "-x4", url]
    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    
    last_update = time.time()
    while True:
        if CANCEL_FLAGS.get(cancel_id): process.terminate(); raise Exception("CANCELLED")
        line = await process.stdout.readline()
        if not line: break
        text = line.decode('utf-8').strip()
        
        if text.startswith("[#") and "DL:" in text and (time.time() - last_update > 3):
            try: 
                await msg.edit_text(f"⚡ **[Aria2c Engine] Downloading...**\n\n`{text}`", reply_markup=get_cancel_markup(cancel_id))
                last_update = time.time()
            except: pass

    await process.wait()
    if process.returncode != 0: raise Exception("Aria2 Process Failed")

# --- RCLONE UPLOAD ---
async def upload_with_rclone(file_path, file_name, msg, parent_id=DRIVE_FOLDER_ID, cancel_id=None, start_time=None):
    is_dir = os.path.isdir(file_path)
    
    # Rclone path configuration based on directory or single file
    if is_dir:
        cmd = ["./rclone", "copy", file_path, f"gdrive:{file_name}", "--config", "rclone.conf", "-P"]
    else:
        cmd = ["./rclone", "copyto", file_path, f"gdrive:{file_name}", "--config", "rclone.conf", "-P"]
        
    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    last_update = time.time()
    last_output = ""
    
    while True:
        if CANCEL_FLAGS.get(cancel_id): process.terminate(); raise Exception("CANCELLED")
        line = await process.stdout.readline()
        if not line: break
        text = line.decode('utf-8').strip()
        if text: last_output = text
        
        if "Transferred:" in text and "ETA" in text and (time.time() - last_update > 3):
            try:
                await msg.edit_text(f"☁️ **[Rclone Engine] Uploading...**\n\n`{text}`", reply_markup=get_cancel_markup(cancel_id))
                last_update = time.time()
            except: pass

    await process.wait()
    if process.returncode != 0: raise Exception(f"Rclone Failed. Last output: {last_output}")

    success, service = get_drive_service()
    if not success: return False, "Auth Error", 0, 0
    try:
        query = f"name='{file_name}' and '{parent_id}' in parents and trashed=false"
        results = await asyncio.to_thread(lambda: service.files().list(q=query, fields="files(id, size)", pageSize=1).execute())
        items = results.get('files', [])
        if items:
            f_id = items[0]['id']
            f_size = int(items[0].get('size', 0)) if not is_dir else 0
            BOT_STATS["uploads"] += 1
            BOT_STATS["bytes_uploaded"] += f_size
            return True, f_id, f_size, time.time() - (start_time or time.time())
        return False, "File ID not found after upload", 0, 0
    except Exception as e: return False, str(e), 0, 0

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
            nf = service.files().create(body={'name': original_folder.get('name'), 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}, fields='id').execute()
            new_folder_id = nf.get('id')
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
    except Exception as e: return False, get_safe_error(e)

def extract_zip(zip_path, extract_dir):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref: zip_ref.extractall(extract_dir)
        return True, "Success"
    except Exception as e: return False, str(e)

async def upload_extracted_folder(extract_dir, base_folder_name, parent_id, msg, cancel_id, start_time):
    success, service = get_drive_service()
    base_g_id = service.files().create(body={'name': base_folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}, fields='id').execute().get('id')
    folder_mapping = {extract_dir: base_g_id}
    total_files = 0
    for root, dirs, files in os.walk(extract_dir):
        if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
        current_parent = folder_mapping[root]
        for d in dirs:
            new_id = service.files().create(body={'name': d, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [current_parent]}, fields='id').execute().get('id')
            folder_mapping[os.path.join(root, d)] = new_id
        for f in files:
            if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
            fpath = os.path.join(root, f)
            await msg.edit_text(f"☁️ Uploading: `{f}`...\nFolder: `{os.path.basename(root)}`", reply_markup=get_cancel_markup(cancel_id))
            up_success, _, _, _ = await upload_with_python_native(fpath, f, msg, current_parent, cancel_id)
            if up_success: total_files += 1
    return total_files, base_g_id

# ================= MAIN PROCESS WITH FALLBACK =================
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
        # Phase 1: Download
        if is_gd and not extract:
            success, service = get_drive_service()
            await msg.edit_text("🔄 High-Speed GDrive Cloning...")
            res = await asyncio.to_thread(lambda: service.files().copy(fileId=gd_id, body={'name': file_name, 'parents': [DRIVE_FOLDER_ID]}, fields='id').execute())
            await msg.edit_text(generate_result_text(file_name, res.get('id'), gd_size, time.time()-start_time))
            return
            
        if not is_gd:
            if os.path.exists("./aria2c"):
                try:
                    await msg.edit_text("⚡ Starting Aria2c Downloader...", reply_markup=get_cancel_markup(cancel_id))
                    await download_with_aria2(url, file_path, msg, cancel_id)
                except Exception as e:
                    if str(e) == "CANCELLED": raise e
                    print("Aria2 Error, falling back:", e)
                    await msg.edit_text("⚡ [Fallback] Native Python Downloader...", reply_markup=get_cancel_markup(cancel_id))
                    await download_with_python_native(url, file_path, msg, cancel_id, link_data)
            else:
                await msg.edit_text("⚡ Starting Native Python Downloader...", reply_markup=get_cancel_markup(cancel_id))
                await download_with_python_native(url, file_path, msg, cancel_id, link_data)
        else:
            success, service = get_drive_service()
            request = service.files().get_media(fileId=gd_id)
            fh = io.FileIO(file_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=20*1024*1024)
            done = False
            while not done:
                if CANCEL_FLAGS.get(cancel_id): fh.close(); raise Exception("CANCELLED")
                status, done = await asyncio.to_thread(functools.partial(downloader.next_chunk, num_retries=3))
                if status:
                    try: await msg.edit_text(f"📥 Fetching from GDrive: {int(status.progress() * 100)}%", reply_markup=get_cancel_markup(cancel_id))
                    except: pass
            fh.close()

        # Phase 2: Extract
        if extract and file_name.lower().endswith('.zip'):
            await msg.edit_text("📦 Extracting ZIP...")
            ext_dir = file_path + "_ext"
            os.makedirs(ext_dir, exist_ok=True)
            success, ext_res = await asyncio.to_thread(extract_zip, file_path, ext_dir)
            if not success: return await msg.edit_text(f"❌ Extraction Error: {ext_res}")
            
            f_name = file_name.replace(".zip", "")
            up_success = False
            if os.path.exists("./rclone") and os.path.exists("rclone.conf"):
                try:
                    await msg.edit_text("☁️ [Rclone Engine] Uploading Extracted Folder...", reply_markup=get_cancel_markup(cancel_id))
                    up_success, new_f_id, _, _ = await upload_with_rclone(ext_dir, f_name, msg, DRIVE_FOLDER_ID, cancel_id, start_time)
                except Exception as e:
                    if str(e) == "CANCELLED": raise e
                    print("Rclone Folder Upload Error, falling back:", e)
                    
            if not up_success:
                await msg.edit_text("☁️ [Native] Uploading Extracted Folder...", reply_markup=get_cancel_markup(cancel_id))
                up_success, new_f_id = await upload_extracted_folder(ext_dir, f_name, DRIVE_FOLDER_ID, msg, cancel_id, start_time)

            if up_success:
                await msg.edit_text(generate_result_text(f_name, new_f_id, 0, time.time()-start_time, True))
            else: await msg.edit_text("❌ Extracted Folder Upload Failed")
            await asyncio.to_thread(shutil.rmtree, ext_dir, ignore_errors=True)
            return

        # Phase 3: Preview
        preview_id = None
        if file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
            await msg.edit_text("🎬 Extracting Frames...", reply_markup=get_cancel_markup(cancel_id))
            p_id = str(int(time.time()))
            frames = await asyncio.to_thread(generate_10_video_frames, file_path, p_id)
            if frames:
                preview_id = p_id
                PREVIEW_CACHE[preview_id] = frames

        # Phase 4: Upload
        up_success = False
        if os.path.exists("./rclone") and os.path.exists("rclone.conf"):
            try:
                await msg.edit_text("☁️ Starting Rclone Upload...", reply_markup=get_cancel_markup(cancel_id))
                success, file_id, file_size, up_time = await upload_with_rclone(file_path, file_name, msg, DRIVE_FOLDER_ID, cancel_id, start_time)
                up_success = success
            except Exception as e:
                if str(e) == "CANCELLED": raise e
                print("Rclone Upload Error, falling back:", e)
                
        if not up_success:
            await msg.edit_text("☁️ [Fallback] Starting Native Python Upload...", reply_markup=get_cancel_markup(cancel_id))
            success, file_id, file_size, up_time = await upload_with_python_native(file_path, file_name, msg, DRIVE_FOLDER_ID, cancel_id, start_time)
        
        if success:
            txt = generate_result_text(file_name, file_id, file_size, time.time() - start_time)
            rm = InlineKeyboardMarkup([[InlineKeyboardButton("🎬 View Preview", callback_data=f"pv|{preview_id}")]]) if preview_id else None
            await msg.edit_text(txt, reply_markup=rm)
        else: await msg.edit_text(f"❌ Upload Failed: {file_id}")

    except Exception as e:
        if str(e) == "CANCELLED": await msg.edit_text("🚫 **Task Cancelled!**")
        else: await msg.edit_text(f"❌ Error: {get_safe_error(e)}")
    finally:
        CANCEL_FLAGS.pop(cancel_id, None)
        if file_path and os.path.exists(file_path): await asyncio.to_thread(os.remove, file_path)
        for f in os.listdir():
            if f.endswith('.aria2'): os.remove(f)

# ================= COMMANDS & CALLBACKS =================
@app.on_message(filters.command("start"))
async def start_command(client, message):
    if not check_auth(message.from_user.id): return
    await message.reply_text("🚀 **SpeedPro Bot Ready!**\nSend me a Link, GDrive Link, or File.\n\n/myfiles, /search <name>, /stats, /storage, /logs")

@app.on_message(filters.command("stats"))
async def stats_command(client, message):
    if not check_auth(message.from_user.id): return
    text = f"📊 **Bot Current Session Stats:**\n\n📤 Uploads: `{BOT_STATS['uploads']}`\n🔄 Clones: `{BOT_STATS['clones']}`\n💾 Uploaded: `{format_size(BOT_STATS['bytes_uploaded'])}`"
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
        limit, usage = int(quota.get('limit', 0)), int(quota.get('usage', 0))
        text = f"💾 **Drive Storage:**\n\n**Used:** `{format_size(usage)}`\n" + (f"**Total:** `Unlimited`" if limit == 0 else f"**Free:** `{format_size(limit - usage)}`\n**Total:** `{format_size(limit)}`")
        await message.reply_text(text)
    except Exception as e: await message.reply_text(f"Storage info fetch failed: {e}")

@app.on_message(filters.command("search"))
async def search_command(client, message):
    if not check_auth(message.from_user.id): return
    query = message.text.split(maxsplit=1)
    if len(query) < 2: return await message.reply_text("⚠️ Usage: `/search <filename>`")
    success, service = get_drive_service()
    if not success: return await message.reply_text("Auth Error.")
    msg = await message.reply_text("🔍 Searching...")
    try:
        results = service.files().list(q=f"trashed=false and name contains '{query[1]}'", fields="files(id, name, mimeType, size)", pageSize=20).execute()
        items = results.get('files', [])
        if not items: return await msg.edit_text("❌ Not found.")
        MYFILES_CACHE[message.from_user.id] = {"items": items, "page": 0, "parent": "search_results", "stack": []}
        await render_myfiles_page(msg, message.from_user.id)
    except Exception as e: await msg.edit_text(f"Error: {e}")

@app.on_message(filters.command("myfiles"))
async def myfiles_command(client, message):
    if not check_auth(message.from_user.id): return
    await fetch_and_render_folder(message, message.from_user.id, DRIVE_FOLDER_ID, init=True)

async def fetch_and_render_folder(obj, user_id, folder_id, init=False):
    success, service = get_drive_service()
    if not success: return
    msg = await obj.reply_text("Fetching...") if init else obj.message
    try:
        results = service.files().list(q=f"'{folder_id}' in parents and trashed=false", orderBy="folder, modifiedTime desc", fields="files(id, name, mimeType, size)", pageSize=100).execute()
        MYFILES_CACHE[user_id] = {"items": results.get('files', []), "page": 0, "parent": folder_id, "stack": [] if init else MYFILES_CACHE[user_id]["stack"]}
        await render_myfiles_page(msg, user_id)
    except: await msg.edit_text("Error fetching files.")

async def render_myfiles_page(msg, user_id):
    cache = MYFILES_CACHE.get(user_id)
    if not cache: return await msg.edit_text("Session expired.")
    items, page = cache["items"], cache["page"]
    per_page = 4
    total_pages = max(1, (len(items) + per_page - 1) // per_page)
    if not items:
        btns = [[InlineKeyboardButton("🔼 Back", callback_data="mf_back")]] if cache["stack"] else None
        return await msg.edit_text("📂 Empty!", reply_markup=InlineKeyboardMarkup(btns) if btns else None)
    
    current_items = items[page * per_page : (page+1) * per_page]
    text, buttons = f"📁 **Files (Page {page+1}/{total_pages}):**\n\n", []
    
    for i, item in enumerate(current_items):
        is_f = item['mimeType'] == 'application/vnd.google-apps.folder'
        text += f"{i+1}. {'📁' if is_f else '📄'} `{item['name']}`\n"
        buttons.append([InlineKeyboardButton(f"📂 Open #{i+1}" if is_f else f"⚙️ Options #{i+1}", callback_data=f"mf_act|{page * per_page + i}")])

    nav_row = []
    if page > 0: nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="mf_nav|prev"))
    if page < total_pages - 1: nav_row.append(InlineKeyboardButton("Next ➡️", callback_data="mf_nav|next"))
    if nav_row: buttons.append(nav_row)
    if cache["stack"]: buttons.append([InlineKeyboardButton("🔼 Back", callback_data="mf_back")])
    await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex(r"^mf_"))
async def myfiles_callback(client, query):
    action, data = query.data.split("|")[0], query.data.split("|")[1] if "|" in query.data else None
    cache = MYFILES_CACHE.get(query.from_user.id)
    if not cache: return await query.answer("Expired.", show_alert=True)
    if action == "mf_nav":
        cache["page"] += 1 if data == "next" else -1
        await render_myfiles_page(query.message, query.from_user.id)
    elif action == "mf_back" and cache["stack"]:
        await fetch_and_render_folder(query, query.from_user.id, cache["stack"].pop(), False)
    elif action == "mf_act":
        item = cache["items"][int(data)]
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            cache["stack"].append(cache["parent"])
            await fetch_and_render_folder(query, query.from_user.id, item['id'], False)
        else:
            await query.message.edit_text(generate_result_text(item['name'], item['id'], int(item.get('size', 0)), 0), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="mf_ret")]]))
    elif action == "mf_ret": await render_myfiles_page(query.message, query.from_user.id)

@app.on_message(filters.document | filters.video | filters.audio)
async def handle_telegram_files(client, message):
    if not check_auth(message.from_user.id): return
    msg = await message.reply_text("📥 Preparing Telegram Download...")
    cancel_id = str(message.id)
    CANCEL_FLAGS[cancel_id] = False
    start_time = time.time()
    try:
        async def prog_cb(current, total):
            if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
            if time.time() - getattr(prog_cb, 'last_update', 0) > 3:
                pct = current/total * 100
                try: await msg.edit_text(f"📥 Telegram DL: {pct:.1f}%", reply_markup=get_cancel_markup(cancel_id))
                except: pass
                prog_cb.last_update = time.time()
                
        file_path = await message.download(progress=prog_cb)
        if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
        
        up_success = False
        if os.path.exists("./rclone") and os.path.exists("rclone.conf"):
            try:
                await msg.edit_text("☁️ [Rclone] Uploading...", reply_markup=get_cancel_markup(cancel_id))
                success, file_id, file_size, up_time = await upload_with_rclone(file_path, os.path.basename(file_path), msg, DRIVE_FOLDER_ID, cancel_id, start_time)
                up_success = success
            except Exception as e:
                print("Rclone telegram upload failed, falling back:", e)
                
        if not up_success:
            await msg.edit_text("☁️ [Native] Uploading...", reply_markup=get_cancel_markup(cancel_id))
            success, file_id, file_size, up_time = await upload_with_python_native(file_path, os.path.basename(file_path), msg, DRIVE_FOLDER_ID, cancel_id, start_time)
            
        if success:
            await msg.edit_text(generate_result_text(os.path.basename(file_path), file_id, file_size, time.time() - start_time))
        else: await msg.edit_text("❌ Upload Failed.")
    except Exception as e: await msg.edit_text(f"❌ Error: {e}")
    finally:
        if 'file_path' in locals() and file_path and os.path.exists(file_path): os.remove(file_path)

@app.on_message(filters.text & ~filters.command(["start", "myfiles", "stats", "logs", "storage", "search"]))
async def handle_text_input(client, message):
    if not check_auth(message.from_user.id): return
    url = message.text
    if state := USER_STATES.get(message.from_user.id):
        if state.get("action") == "wait_rename":
            new_name = url.strip()
            del USER_STATES[message.from_user.id]
            if state.get("is_gd"):
                msg = await message.reply_text("🔄 Cloning...")
                try:
                    res = await asyncio.to_thread(lambda: get_drive_service()[1].files().copy(fileId=state['gd_id'], body={'name': new_name, 'parents': [DRIVE_FOLDER_ID]}, fields='id').execute())
                    await msg.edit_text(generate_result_text(new_name, res.get('id'), state['gd_size'], 0))
                except Exception as e: await msg.edit_text(f"❌ Error: {e}")
            else: await process_download(client, message, state["url"], new_name, extract=False)
            return

    if not re.match(r"http[s]?://", url): return
    g_id, is_folder = extract_gdrive_id(url)
    if g_id:
        if is_folder:
            msg = await message.reply_text("🔄 Cloning Folder...")
            success, result = await clone_gdrive_item(g_id, True, msg=msg)
            if success: await msg.edit_text(f"✅ Folder Cloned: `{result['name']}`")
            else: await msg.edit_text("❌ Clone failed.")
            return
        else:
            msg = await message.reply_text("🔍 Fetching Info...")
            try:
                meta = await asyncio.to_thread(lambda: get_drive_service()[1].files().get(fileId=g_id, fields='name, size').execute())
                name, size = meta.get('name', 'Unknown'), int(meta.get('size', 0))
                LINK_CACHE[message.id] = {"url": url, "name": name, "is_gd": True, "gd_id": g_id, "gd_size": size}
                btns = [[InlineKeyboardButton("🔄 Clone Now", callback_data=f"dl_now|{message.id}"), InlineKeyboardButton("✏️ Rename", callback_data=f"dl_ren|{message.id}")]]
                if name.lower().endswith('.zip'): btns.append([InlineKeyboardButton("📦 Clone & Extract", callback_data=f"dl_ext|{message.id}")])
                await msg.edit_text(f"🔗 **GDrive File!**\n📄 `{name}`\n📦 `{format_size(size)}`", reply_markup=InlineKeyboardMarkup(btns))
            except: await msg.edit_text("❌ Not Found.")
            return

    msg = await message.reply_text("🔍 Fetching Metadata...")
    name, size = await get_url_metadata(url)
    LINK_CACHE[message.id] = {"url": url, "name": name, "size": size, "is_gd": False}
    btns = [[InlineKeyboardButton("⬇️ Download Fast", callback_data=f"dl_now|{message.id}"), InlineKeyboardButton("✏️ Rename", callback_data=f"dl_ren|{message.id}")]]
    if name.lower().endswith('.zip'): btns.append([InlineKeyboardButton("📦 Extract", callback_data=f"dl_ext|{message.id}")])
    await msg.edit_text(f"🔗 **Direct Link!**\n📄 `{name}`\n📦 `{format_size(size)}`", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(~filters.regex(r"^mf_"))
async def general_cb(client, query):
    action, data = query.data.split("|")[0], query.data.split("|")[1]
    if action == "cancel": CANCEL_FLAGS[data] = True; await query.answer("Cancelling...")
    elif action in ["dl_now", "dl_ext"]:
        if ld := LINK_CACHE.get(int(data)):
            await query.message.delete()
            await process_download(client, query.message, ld["url"], ld["name"], (action=="dl_ext"))
        else: await query.answer("Expired.", show_alert=True)
    elif action == "dl_ren":
        if ld := LINK_CACHE.get(int(data)):
            USER_STATES[query.from_user.id] = {"action": "wait_rename", "url": ld["url"], "is_gd": ld.get("is_gd"), "gd_id": ld.get("gd_id"), "gd_size": ld.get("gd_size")}
            await query.message.delete()
            await query.message.reply_text("Send **new name** with extension:")
    elif action == "pv":
        if paths := PREVIEW_CACHE.get(data):
            await query.answer("Sending...")
            await app.send_media_group(query.message.chat.id, [InputMediaPhoto(p) for p in paths])
        else: await query.answer("Expired.", show_alert=True)

# ================= WEB SERVER FOR RENDER =================
async def start_web_server():
    app_web = web.Application()
    app_web.router.add_get('/', lambda r: web.Response(text="🚀 Ultimate SpeedPro Bot is Running on Render!"))
    runner = web.AppRunner(app_web)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', PORT).start()

async def main():
    await start_web_server()
    await app.start()
    logger.info("Bot LIVE on Render with Aria2 & Rclone! 🚀")
    await idle()
    await app.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())