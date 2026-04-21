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
from googleapiclient.errors import HttpError

# ভিডিও প্রসেসিং লাইব্রেরি চেক করা হচ্ছে
try:
    import cv2
    from PIL import Image
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False

# ================= লগিং সেটআপ (মেমোরি সহ) =================
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

# ================= এনভায়রনমেন্ট ভেরিয়েবল =================
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
LINK_CACHE = {}  
USER_STATES = {} 
MYFILES_CACHE = {}
PREVIEW_CACHE = {} 
CANCEL_FLAGS = {}
BOT_STATS = {"uploads": 0, "clones": 0, "bytes_uploaded": 0}

os.makedirs("previews", exist_ok=True)

app = Client("my_drive_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)

def get_drive_service():
    if not GOOGLE_OAUTH_TOKEN: return False, f"⚠️ Bot not authenticated. Visit {RENDER_URL}/login"
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

# --- স্মার্ট ইউআরএল মেটাডেটা ফেচার (আসল নাম ডিটেকশন) ---
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

# --- ১০ ফ্রেমের ভিডিও প্রিভিউ জেনারেটর ---
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

# লাইভ প্রোগ্রেস আপডেট
async def update_progress(current, total, msg, start_time, action_text, cancel_id=None):
    if cancel_id and CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
    if total == 0: return
    now = time.time()
    if not hasattr(msg, "last_update_time"): msg.last_update_time = start_time
    
    if (now - msg.last_update_time > 2.5) or (current == total):
        msg.last_update_time = now
        percent = (current / total) * 100
        filled = int(percent / 10)
        bar = "🟩" * filled + "🟥" * (10 - filled)
        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        eta = (total - current) / speed if speed > 0 else 0
        
        text = f"{action_text}\n\n{bar} {percent:.1f}%\n📦 Size: {format_size(current)} / {format_size(total)}\n🚀 Speed: {format_size(speed)}/s\n⏳ ETA: {format_time(eta)}"
        try: await msg.edit_text(text, reply_markup=get_cancel_markup(cancel_id) if cancel_id else None)
        except MessageNotModified: pass

# --- স্পিড অপ্টিমাইজড মাল্টি-কানেকশন (প্যারালাল) ডাউনলোড লজিক ---
async def download_part(session, url, start, end, part_path, progress, msg, start_time, cancel_id):
    headers = {'Range': f'bytes={start}-{end}'}
    async with session.get(url, headers=headers) as resp:
        resp.raise_for_status()
        with open(part_path, 'wb') as f:
            async for chunk in resp.content.iter_chunked(4 * 1024 * 1024):
                if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
                f.write(chunk)
                progress['downloaded'] += len(chunk)
                await update_progress(progress['downloaded'], progress['total'], msg, start_time, "⚡ Parallel Downloading...", cancel_id)

# --- গুগল ড্রাইভ স্পিড আপলোড ---
async def upload_to_drive_async(file_path, file_name, msg, parent_id=DRIVE_FOLDER_ID, cancel_id=None):
    try:
        success, service = get_drive_service()
        if not success: return False, service, 0, 0
        file_size = os.path.getsize(file_path)
        media = MediaFileUpload(file_path, chunksize=50*1024*1024, resumable=True) # 50MB আপলোড চাঙ্ক
        request = service.files().create(body={'name': file_name, 'parents': [parent_id]}, media_body=media, fields='id')
        
        response, start_time = None, time.time()
        while response is None:
            if cancel_id and CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
            status, response = await asyncio.to_thread(functools.partial(request.next_chunk, num_retries=5))
            if status: 
                # BUG FIX: status.resumable_progress is a property, not a function!
                await update_progress(status.resumable_progress, file_size, msg, start_time, "☁️ Uploading to Drive...", cancel_id)
                
        BOT_STATS["uploads"] += 1
        BOT_STATS["bytes_uploaded"] += file_size
        return True, response.get('id'), file_size, time.time() - start_time
    except Exception as e: return False, get_safe_error(e), 0, 0

# --- গুগল ড্রাইভ আইটেম (ফোল্ডার সহ) ক্লোন লজিক ---
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

# --- স্ট্রাকচার অনুযায়ী জিপ এক্সট্র্যাক্ট এবং আপলোড ---
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
            up_success, _, _, _ = await upload_to_drive_async(fpath, f, msg, current_parent, cancel_id)
            if up_success: total_files += 1
    return total_files, base_g_id

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

# --- ড্রাইভ ফাইল সার্ভারে ডাউনলোড ---
async def download_gdrive_to_server(file_id, file_path, file_size, msg, cancel_id):
    success, service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request, chunksize=16*1024*1024)
    done, start_time = False, time.time()
    
    while not done:
        if CANCEL_FLAGS.get(cancel_id):
            fh.close()
            raise Exception("CANCELLED")
        status, done = await asyncio.to_thread(functools.partial(downloader.next_chunk, num_retries=3))
        # BUG FIX: status.resumable_progress is a property!
        if status: await update_progress(status.resumable_progress, file_size, msg, start_time, "📥 Downloading from GDrive...", cancel_id)
    fh.close()

# ================= মেসেজ হ্যান্ডলারস =================
@app.on_message(filters.command("start"))
async def start_command(client, message):
    if not check_auth(message.from_user.id): return
    await message.reply_text("Hello! 👋 Send me a Direct Link, GDrive Link, or a File.\nCommands: /myfiles, /stats, /search <name>, /logs, /storage")

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

    if state and state.get("action") == "wait_drive_rename":
        new_name = message.text.strip()
        file_id = state.get("file_id")
        del USER_STATES[message.from_user.id]
        success, service = get_drive_service()
        if success:
            try:
                service.files().update(fileId=file_id, body={'name': new_name}).execute()
                await message.reply_text(f"✅ Successfully renamed to `{new_name}` in GDrive.")
            except Exception as e:
                await message.reply_text(f"❌ Rename Failed: {get_safe_error(e)}")
        return

    if not re.match(r"http[s]?://", url): return

    # --- ড্রাইভ লিংক ম্যানেজমেন্ট (Private Link Bug Fixed) ---
    g_id, is_folder = extract_gdrive_id(url)
    if g_id:
        if is_folder:
            msg = await message.reply_text(f"🔄 Fetching Folder details...")
            try:
                success, result = await clone_gdrive_item(g_id, is_folder=True, msg=msg)
                if success:
                    await msg.edit_text(f"✅ GDrive Folder Cloned Successfully!\nName: `{result['name']}`\n[Open Folder](https://drive.google.com/drive/folders/{result['id']})")
                else:
                    await msg.edit_text(f"❌ **Access Denied or Folder Not Found!**\nMake sure the folder is public.")
            except Exception as e:
                await msg.edit_text("❌ **Access Denied or Folder Not Found!**\nMake sure the folder is public.")
            return
        else:
            msg = await message.reply_text("🔍 Fetching GDrive File Info...")
            try:
                success, service = get_drive_service()
                meta = await asyncio.to_thread(lambda: service.files().get(fileId=g_id, fields='name, size').execute())
                name, size = meta.get('name', 'Unknown'), int(meta.get('size', 0))
                
                LINK_CACHE[message.id] = {"url": url, "name": name, "is_gd": True, "gd_id": g_id, "gd_size": size}
                btns = [[InlineKeyboardButton("🔄 Clone Now", callback_data=f"dl_now|{message.id}"), InlineKeyboardButton("✏️ Rename & Clone", callback_data=f"dl_ren|{message.id}")]]
                if name.lower().endswith('.zip'): btns.append([InlineKeyboardButton("📦 Clone & Extract", callback_data=f"dl_ext|{message.id}")])
                
                await msg.edit_text(f"🔗 **GDrive File Detected!**\n📄 Name: `{name}`\n📦 Size: `{format_size(size)}`\n\nSelect action:", reply_markup=InlineKeyboardMarkup(btns))
            except HttpError as e:
                # প্রাইভেট ফাইল হলে এই ব্লকে আসবে
                await msg.edit_text("❌ **File Not Found or Access Denied!**\nPlease make sure the GDrive link is public and the file exists.")
            except Exception as e:
                await msg.edit_text("❌ **File Not Found!**")
            return

    # --- ডিরেক্ট লিংক ম্যানেজমেন্ট ---
    msg = await message.reply_text("🔍 Fetching Metadata...")
    name, size = await get_url_metadata(url)
    LINK_CACHE[message.id] = {"url": url, "name": name, "size": size, "is_gd": False}
    
    btns = [[InlineKeyboardButton("⬇️ Download Now", callback_data=f"dl_now|{message.id}"), InlineKeyboardButton("✏️ Rename", callback_data=f"dl_ren|{message.id}")]]
    if name.lower().endswith('.zip'): btns.append([InlineKeyboardButton("📦 Extract & Upload", callback_data=f"dl_ext|{message.id}")])
    
    size_str = format_size(size) if size > 0 else "Unknown"
    await msg.edit_text(f"🔗 **Direct Link Detected!**\n📄 Name: `{name}`\n📦 Size: `{size_str}`\n\nSelect action:", reply_markup=InlineKeyboardMarkup(btns))

# ================= প্রধান ডাউনলোড ও আপলোড লজিক =================
async def process_download(client, message, url, file_name, extract=False):
    msg = await app.send_message(message.chat.id, "📥 Preparing...")
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
            await download_gdrive_to_server(gd_id, file_path, gd_size, msg, cancel_id)
        elif is_gd and not extract:
            success, service = get_drive_service()
            await msg.edit_text("🔄 Cloning...")
            res = await asyncio.to_thread(lambda: service.files().copy(fileId=gd_id, body={'name': file_name, 'parents': [DRIVE_FOLDER_ID]}, fields='id').execute())
            await msg.edit_text(generate_result_text(file_name, res.get('id'), gd_size, time.time()-start_time))
            return
        else:
            # ⚡ স্মার্ট প্যারালাল (মাল্টি-কানেকশন) ডাউনলোড লজিক ⚡
            connector = aiohttp.TCPConnector(limit=20)
            async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=0)) as session:
                supports_range = False
                total_size = link_data.get("size", 0)
                
                try:
                    async with session.head(url, allow_redirects=True) as head_resp:
                        supports_range = head_resp.headers.get('Accept-Ranges') == 'bytes'
                        if total_size == 0: total_size = int(head_resp.headers.get('content-length', 0))
                except: pass

                if supports_range and total_size > 20 * 1024 * 1024:
                    num_parts = 4
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
                    
                    await msg.edit_text("⚙️ Merging downloaded parts...", reply_markup=get_cancel_markup(cancel_id))
                    with open(file_path, 'wb') as outfile:
                        for i in range(num_parts):
                            part_path = f"{file_path}.part{i}"
                            with open(part_path, 'rb') as infile:
                                shutil.copyfileobj(infile, outfile)
                            os.remove(part_path)
                else:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        downloaded = 0
                        with open(file_path, 'wb', buffering=10*1024*1024) as f:
                            async for chunk in response.content.iter_chunked(8 * 1024 * 1024): 
                                if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total_size > 0: await update_progress(downloaded, total_size, msg, start_time, "📥 Downloading...", cancel_id)

        # জিপ এক্সট্রাকশন হ্যান্ডলিং
        if extract and file_name.lower().endswith('.zip'):
            await msg.edit_text("📦 Extracting ZIP...")
            ext_dir = file_path + "_ext"
            os.makedirs(ext_dir, exist_ok=True)
            success, ext_res = await asyncio.to_thread(extract_zip, file_path, ext_dir)
            if not success:
                return await msg.edit_text(f"❌ Extraction Error: {ext_res}")
            
            f_name = file_name.replace(".zip", "")
            t_files, new_f_id = await upload_extracted_folder(ext_dir, f_name, DRIVE_FOLDER_ID, msg, cancel_id, start_time)
            
            await msg.edit_text(generate_result_text(f_name, new_f_id, 0, time.time()-start_time, True) + f"\n📄 Files: `{t_files}`")
            shutil.rmtree(ext_dir, ignore_errors=True)
            return

        # ভিডিও প্রিভিউ অ্যালবামের জন্য
        preview_id = None
        if file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
            await msg.edit_text("🎬 Extracting Frames...", reply_markup=get_cancel_markup(cancel_id))
            p_id = str(int(time.time()))
            frames = await asyncio.to_thread(generate_10_video_frames, file_path, p_id)
            if frames:
                preview_id = p_id
                PREVIEW_CACHE[preview_id] = frames

        # ড্রাইভ আপলোড
        success, file_id, file_size, up_time = await upload_to_drive_async(file_path, file_name, msg, cancel_id=cancel_id)
        
        if success:
            txt = generate_result_text(file_name, file_id, file_size, time.time() - start_time)
            rm = InlineKeyboardMarkup([[InlineKeyboardButton("🎬 View Preview", callback_data=f"pv|{preview_id}")]]) if preview_id else None
            await msg.edit_text(txt, reply_markup=rm)
        else:
            await msg.edit_text(f"❌ Upload Failed: {file_id}")

    except Exception as e:
        if str(e) == "CANCELLED": await msg.edit_text("🚫 **Task Cancelled by User!**")
        else: await msg.edit_text(f"❌ Error: {get_safe_error(e)}")
    finally:
        CANCEL_FLAGS.pop(cancel_id, None)
        if file_path and os.path.exists(file_path): os.remove(file_path)
        for i in range(4):
            part_p = f"{file_path}.part{i}"
            if os.path.exists(part_p): os.remove(part_p)

@app.on_message(filters.document | filters.video | filters.audio)
async def handle_telegram_files(client, message):
    if not check_auth(message.from_user.id): return
    if not GOOGLE_OAUTH_TOKEN: return await message.reply_text("Connect Google Drive first.")
    
    msg = await message.reply_text("📥 Preparing Telegram download...")
    start_time = time.time()
    cancel_id = str(message.id)
    CANCEL_FLAGS[cancel_id] = False
    
    try:
        file_path = await message.download(progress=update_progress, progress_args=(msg, start_time, "📥 Downloading from Telegram...", cancel_id))
        if CANCEL_FLAGS.get(cancel_id): raise Exception("CANCELLED")
        
        file_name = os.path.basename(file_path)
        
        preview_id = None
        if file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
            await msg.edit_text("🎬 Extracting Video Preview...", reply_markup=get_cancel_markup(cancel_id))
            p_id = str(int(time.time()))
            frames = await asyncio.to_thread(generate_10_video_frames, file_path, p_id)
            if frames:
                preview_id = p_id
                PREVIEW_CACHE[preview_id] = frames

        success, file_id, file_size, up_time = await upload_to_drive_async(file_path, file_name, msg, cancel_id=cancel_id)
        
        if success:
            total_elapsed = time.time() - start_time
            text = generate_result_text(file_name, file_id, file_size, total_elapsed)
            reply_markup = None
            if preview_id:
                reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🎬 View Preview", callback_data=f"pv|{preview_id}")]])
            await msg.edit_text(text, reply_markup=reply_markup)
        else:
            await msg.edit_text(f"❌ Upload failed.\nReason: {file_id}")
    except Exception as e:
        if str(e) == "CANCELLED": await msg.edit_text("🚫 **Task Cancelled by User!**")
        else: await msg.edit_text(f"❌ Error: {get_safe_error(e)}")
    finally:
        CANCEL_FLAGS.pop(cancel_id, None)
        if 'file_path' in locals() and file_path and os.path.exists(file_path): os.remove(file_path)

# ================= কলব্যাক বাটন হ্যান্ডলারস =================
@app.on_callback_query()
async def callback_handler(client, query: CallbackQuery):
    data = query.data.split("|")
    action = data[0]

    if action == "cancel":
        CANCEL_FLAGS[data[1]] = True
        await query.answer("Cancelling task, please wait...", show_alert=True)
        
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

    elif action == "pv":
        paths = PREVIEW_CACHE.get(data[1], [])
        if paths:
            await query.answer("Sending 10 frames...")
            media = [InputMediaPhoto(p) for p in paths]
            await app.send_media_group(query.message.chat.id, media)
        else:
            await query.answer("Preview expired or not available.", show_alert=True)

# ================= অতিরিক্ত কমান্ডস (Stats, Storage, MyFiles) =================
@app.on_message(filters.command("stats"))
async def stats_command(client, message):
    if not check_auth(message.from_user.id): return
    text = f"📊 **Bot Current Session Stats:**\n\n📤 Total Uploads: `{BOT_STATS['uploads']}`\n🔄 Total Clones: `{BOT_STATS['clones']}`\n💾 Data Uploaded: `{format_size(BOT_STATS['bytes_uploaded'])}`"
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
    if len(query) < 2: return await message.reply_text("⚠️ **Usage:** `/search <filename>`")
    keyword = query[1]
    success, service = get_drive_service()
    if not success: return await message.reply_text("Auth Error.")
    
    msg = await message.reply_text("🔍 Searching...")
    try:
        search_terms = " or ".join([f"name contains '{w}'" for w in keyword.split()])
        results = service.files().list(q=f"trashed=false and ({search_terms})", fields="files(id, name, mimeType, size)", pageSize=20).execute()
        items = results.get('files', [])
        if not items: return await msg.edit_text("❌ No similar files found.")
        MYFILES_CACHE[message.from_user.id] = {"items": items, "page": 0, "parent": "search_results", "stack": []}
        await render_myfiles_page(msg, message.from_user.id)
    except Exception as e: await msg.edit_text(f"Search failed: {get_safe_error(e)}")

@app.on_message(filters.command("myfiles"))
async def myfiles_command(client, message):
    if not check_auth(message.from_user.id): return
    await fetch_and_render_folder(message, message.from_user.id, DRIVE_FOLDER_ID, init=True)

async def fetch_and_render_folder(message_obj_or_query, user_id, folder_id, init=False):
    success, service = get_drive_service()
    if not success: return
    msg = await message_obj_or_query.reply_text("Fetching files...") if init else message_obj_or_query.message
    try:
        results = service.files().list(q=f"'{folder_id}' in parents and trashed=false", orderBy="folder, modifiedTime desc", fields="files(id, name, mimeType, size)", pageSize=100).execute()
        items = results.get('files', [])
        MYFILES_CACHE[user_id] = {"items": items, "page": 0, "parent": folder_id, "stack": [] if init else MYFILES_CACHE[user_id]["stack"]}
        await render_myfiles_page(msg, user_id)
    except Exception as e: await (msg.edit_text if not init else msg.reply_text)(f"Fetch failed: {get_safe_error(e)}")

async def render_myfiles_page(msg, user_id):
    cache = MYFILES_CACHE.get(user_id)
    if not cache: return await msg.edit_text("Session expired. Type /myfiles again.")
    items, page = cache["items"], cache["page"]
    per_page = 3
    total_pages = max(1, (len(items) + per_page - 1) // per_page)
    
    if not items:
        buttons = [[InlineKeyboardButton("🔼 Back to Parent", callback_data="mf_back")]] if cache["stack"] else None
        return await msg.edit_text("📂 **Folder is empty!**", reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)
        
    start_idx = page * per_page
    current_items = items[start_idx : start_idx + per_page]
    text, buttons = f"📁 **Files List (Page {page+1}/{total_pages}):**\n\n", []
    
    for i, item in enumerate(current_items):
        is_folder = item['mimeType'] == 'application/vnd.google-apps.folder'
        text += f"{i+1}. {'📁' if is_folder else '📄'} `{item['name']}`\n"
        buttons.append([InlineKeyboardButton(f"📂 Open #{i+1}" if is_folder else f"⚙️ Options #{i+1}", callback_data=f"mf_act|{start_idx+i}")])

    nav_row = []
    if page > 0: nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="mf_nav|prev"))
    if page < total_pages - 1: nav_row.append(InlineKeyboardButton("Next ➡️", callback_data="mf_nav|next"))
    if nav_row: buttons.append(nav_row)
    if cache["stack"]: buttons.append([InlineKeyboardButton("🔼 Back to Parent", callback_data="mf_back")])
    await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex(r"^mf_"))
async def myfiles_callback(client, query: CallbackQuery):
    user_id = query.from_user.id
    action, data = query.data.split("|")[0], query.data.split("|")[1] if "|" in query.data else None
    cache = MYFILES_CACHE.get(user_id)
    if not cache: return await query.answer("Session expired.", show_alert=True)
    
    if action == "mf_nav":
        cache["page"] += 1 if data == "next" else -1
        await render_myfiles_page(query.message, user_id)
    elif action == "mf_back" and cache["stack"]:
        await fetch_and_render_folder(query, user_id, cache["stack"].pop(), init=False)
    elif action == "mf_act":
        item = cache["items"][int(data)]
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            cache["stack"].append(cache["parent"])
            await fetch_and_render_folder(query, user_id, item['id'], init=False)
        else:
            text = generate_result_text(item['name'], item['id'], int(item.get('size', 0)), 0)
            buttons = [
                [InlineKeyboardButton("✏️ Rename", callback_data=f"ren_file|{item['id']}"), InlineKeyboardButton("🗑️ Remove", callback_data=f"del_file|{item['id']}")],
                [InlineKeyboardButton("🔙 Back to List", callback_data="mf_ret")]
            ]
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    elif action == "mf_ret": await render_myfiles_page(query.message, user_id)

async def start_web_server():
    app_web = web.Application()
    app_web.router.add_get('/', lambda r: web.Response(text="Bot is running! Go to /login for auth."))
    app_web.router.add_get('/login', handle_login)
    app_web.router.add_get('/callback', handle_callback)
    runner = web.AppRunner(app_web)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 8080))).start()

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

async def main():
    await start_web_server()
    await app.start()
    logger.info("Ultimate Speed Pro Bot LIVE! 🚀")
    await idle()
    await app.stop()

if __name__ == "__main__": asyncio.get_event_loop().run_until_complete(main())