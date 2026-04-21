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

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import MessageNotModified
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# ================= ENVIRONMENT VARIABLES =================
API_ID_STR = os.environ.get("API_ID", "").strip()
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_OAUTH_TOKEN = os.environ.get("GOOGLE_OAUTH_TOKEN", "").strip()
AUTH_USERS_STR = os.environ.get("AUTHORIZED_USERS", "")
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://gdriveuploadbot.onrender.com")

# Cloudflare GoIndex URL (e.g., https://gdrive.rahatx.workers.dev)
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

# Caching structures for interactive operations
LINK_CACHE = {}  
USER_STATES = {} 

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
    return f"{bytes_size / (1024 ** 2):.2f} MB"

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

async def update_progress(current, total, msg, start_time, action_text):
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
                await msg.edit_text(text)
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
            status, response = await asyncio.to_thread(functools.partial(request.next_chunk, num_retries=5))
            if status:
                await update_progress(status.resumable_progress, file_size, msg, start_time, "☁️ Uploading to Google Drive...")
                
        elapsed = time.time() - start_time
        return True, response.get('id'), file_size, elapsed
    except Exception as e:
        return False, get_safe_error(e), 0, 0

def create_gdrive_folder(folder_name, parent_id=DRIVE_FOLDER_ID):
    success, service = get_drive_service()
    if not success: return None
    metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = service.files().create(body=metadata, fields='id').execute()
    return folder.get('id')

async def clone_gdrive_item(item_id, is_folder=False, parent_id=DRIVE_FOLDER_ID):
    try:
        success, service = get_drive_service()
        if not success: return False, service
        
        if not is_folder:
            body = {'parents': [parent_id]}
            response = await asyncio.to_thread(
                lambda: service.files().copy(fileId=item_id, body=body, fields='id, name').execute()
            )
            return True, response
        else:
            original_folder = service.files().get(fileId=item_id, fields='name').execute()
            new_folder_id = create_gdrive_folder(original_folder.get('name'), parent_id)
            
            query = f"'{item_id}' in parents and trashed=false"
            results = service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType)", pageSize=1000).execute()
            items = results.get('files', [])
            
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    await clone_gdrive_item(item['id'], is_folder=True, parent_id=new_folder_id)
                else:
                    await clone_gdrive_item(item['id'], is_folder=False, parent_id=new_folder_id)
                    
            return True, {"name": original_folder.get('name'), "id": new_folder_id}
    except Exception as e:
        return False, get_safe_error(e)

def extract_zip(zip_path, extract_dir):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        return True, "Success"
    except RuntimeError as e:
        if 'password' in str(e).lower() or 'encrypted' in str(e).lower():
            return False, "Password protected ZIPs are not supported yet."
        return False, str(e)
    except Exception as e:
        return False, str(e)

def extract_gdrive_id(url):
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1)
    match = re.search(r"id=([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1)
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1), True
    return None, False

def check_auth(user_id):
    if not AUTHORIZED_USERS: return True
    return user_id in AUTHORIZED_USERS

def generate_result_text(file_name, file_id, file_size, elapsed_time):
    drive_link = f"https://drive.google.com/file/d/{file_id}/view"
    
    # URL Encode the filename for GoIndex direct link
    safe_file_name = urllib.parse.quote(file_name)
    direct_link = f"{CF_WORKER_URL}/{safe_file_name}" if CF_WORKER_URL else "Not Configured"
    
    text = (
        f"✅ **Task Completed Successfully!**\n\n"
        f"📄 **Name:** `{file_name}`\n"
        f"📦 **Size:** `{format_size(file_size)}`\n"
        f"⏱️ **Time Taken:** `{format_time(elapsed_time)}`\n\n"
        f"🔗 **Google Drive Link:**\n{drive_link}\n\n"
        f"⚡ **Direct Download Link (GoIndex):**\n{direct_link}"
    )
    return text

# ================= MESSAGE HANDLERS =================

@app.on_message(filters.command("start"))
async def start_command(client, message):
    if not check_auth(message.from_user.id): return
    if not GOOGLE_OAUTH_TOKEN:
        await message.reply_text(f"⚠️ Connect Google Drive first:\n{RENDER_URL}/login")
        return
    await message.reply_text(
        "Hello! 👋\n"
        "Send me a link or file. I support:\n"
        "- Direct Download Links (With Rename/Extract features)\n"
        "- Google Drive File/Folder Links (Instant Clone)\n"
        "- Telegram Files\n"
        "- Use `/myfiles` to manage your drive."
    )

@app.on_message(filters.command("myfiles"))
async def myfiles_command(client, message):
    if not check_auth(message.from_user.id): return
    success, service = get_drive_service()
    if not success: return await message.reply_text("Auth Error.")
    
    msg = await message.reply_text("Fetching your recent files...")
    try:
        query = f"'{DRIVE_FOLDER_ID}' in parents and trashed=false"
        results = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)", pageSize=10).execute()
        items = results.get('files', [])
        
        if not items:
            return await msg.edit_text("Your designated Drive folder is empty.")
            
        buttons = []
        text = "📁 **Recent 10 Files in your Drive:**\n\n"
        for i, item in enumerate(items, 1):
            text += f"{i}. `{item['name']}`\n"
            row = [
                InlineKeyboardButton(f"Rename #{i}", callback_data=f"ren_file|{item['id']}"),
                InlineKeyboardButton(f"Delete #{i}", callback_data=f"del_file|{item['id']}")
            ]
            buttons.append(row)
            
        await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        await msg.edit_text(f"Error fetching files: {get_safe_error(e)}")

@app.on_message(filters.text & ~filters.command(["start", "myfiles"]))
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

    if "drive.google.com" in url:
        g_id, is_folder = extract_gdrive_id(url)
        if g_id:
            msg = await message.reply_text(f"🔄 Cloning {'Folder' if is_folder else 'File'} to Drive...")
            success, result = await clone_gdrive_item(g_id, is_folder)
            if success:
                await msg.edit_text(f"✅ GDrive Clone Complete!\n\nName: `{result.get('name')}`")
            else:
                await msg.edit_text(f"❌ GDrive Clone Failed.\n\nReason:\n{result}")
            return

    file_name = url.split("/")[-1].split("?")[0]
    if not file_name: file_name = f"download_{int(time.time())}"
    
    LINK_CACHE[message.id] = {"url": url, "name": file_name}
    
    buttons = [
        [InlineKeyboardButton("⬇️ Download Now", callback_data=f"dl_now|{message.id}"),
         InlineKeyboardButton("✏️ Rename", callback_data=f"dl_ren|{message.id}")]
    ]
    if file_name.lower().endswith('.zip'):
        buttons.append([InlineKeyboardButton("📦 Extract & Upload", callback_data=f"dl_ext|{message.id}")])
        
    await message.reply_text(
        f"🔗 **Link Detected!**\nFile: `{file_name}`\n\nChoose an action below:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query()
async def callback_handler(client, query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data.split("|")
    action = data[0]

    if action in ["dl_now", "dl_ren", "dl_ext"]:
        msg_id = int(data[1])
        link_data = LINK_CACHE.get(msg_id)
        
        if not link_data:
            return await query.answer("Session expired. Send link again.", show_alert=True)
            
        url = link_data["url"]
        default_name = link_data["name"]
        
        await query.message.delete()
        
        if action == "dl_now":
            await process_download(client, query.message, url, default_name, extract=False)
        elif action == "dl_ext":
            await process_download(client, query.message, url, default_name, extract=True)
        elif action == "dl_ren":
            USER_STATES[user_id] = {"action": "wait_rename", "url": url}
            await app.send_message(user_id, "Please send the **new name** for the file (including extension like .mp4, .mkv):")
            
    elif action == "del_file":
        file_id = data[1]
        success, service = get_drive_service()
        if success:
            try:
                service.files().delete(fileId=file_id).execute()
                await query.answer("File Deleted from GDrive!", show_alert=True)
                await query.message.delete()
            except Exception as e:
                await query.answer(f"Failed: {get_safe_error(e)}", show_alert=True)
                
    elif action == "ren_file":
        file_id = data[1]
        USER_STATES[user_id] = {"action": "wait_drive_rename", "file_id": file_id}
        await query.answer("Check your messages to rename.", show_alert=False)
        await app.send_message(user_id, "Please send the **new name** for this Google Drive file:")

async def process_download(client, message, url, file_name, extract=False):
    msg = await app.send_message(message.chat.id, "📥 Starting download...")
    file_path = os.path.join(os.getcwd(), file_name)
    start_time = time.time()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                with open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(2 * 1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            await update_progress(downloaded, total_size, msg, start_time, "📥 Downloading to server...")
                            
        if extract and file_name.lower().endswith('.zip'):
            await msg.edit_text("📦 Extracting ZIP file...")
            extract_dir = os.path.join(os.getcwd(), file_name + "_extracted")
            os.makedirs(extract_dir, exist_ok=True)
            
            success, ext_result = await asyncio.to_thread(extract_zip, file_path, extract_dir)
            if not success:
                os.remove(file_path)
                shutil.rmtree(extract_dir, ignore_errors=True)
                return await msg.edit_text(f"❌ Extraction Failed: {ext_result}")
                
            await msg.edit_text("📁 Creating folder in Drive and uploading contents...")
            folder_name = file_name.replace(".zip", "")
            new_folder_id = create_gdrive_folder(folder_name)
            
            total_files_uploaded = 0
            for root, dirs, files in os.walk(extract_dir):
                for fname in files:
                    fpath = os.path.join(root, fname)
                    await msg.edit_text(f"☁️ Uploading extracted file: `{fname}`...")
                    up_success, _, _, _ = await upload_to_drive_async(fpath, fname, msg, parent_id=new_folder_id)
                    if up_success: total_files_uploaded += 1
                        
            elapsed = time.time() - start_time
            
            # GoIndex direct link for the new folder
            safe_folder_name = urllib.parse.quote(folder_name)
            folder_direct_link = f"{CF_WORKER_URL}/{safe_folder_name}/" if CF_WORKER_URL else "Not Configured"
            
            await msg.edit_text(f"✅ **Extraction & Upload Complete!**\n\n📁 **Folder:** `{folder_name}`\n📄 **Files Uploaded:** `{total_files_uploaded}`\n⏱️ **Time Taken:** `{format_time(elapsed)}`\n\n⚡ **Folder Direct Link:**\n{folder_direct_link}")
            
            os.remove(file_path)
            shutil.rmtree(extract_dir, ignore_errors=True)
            return

        success, file_id, file_size, upload_time = await upload_to_drive_async(file_path, file_name, msg)
        
        if success:
            total_elapsed = time.time() - start_time
            await msg.edit_text(generate_result_text(file_name, file_id, file_size, total_elapsed))
        else:
            await msg.edit_text(f"❌ Upload failed.\n\n⚠️ Reason:\n{file_id}")
            
    except OSError as e: 
        await msg.edit_text(f"❌ Server Storage Full!\n⚠️ Reason:\n{get_safe_error(e)}")
    except Exception as e:
        await msg.edit_text(f"❌ Error:\n{get_safe_error(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.document | filters.video | filters.audio)
async def handle_telegram_files(client, message):
    if not check_auth(message.from_user.id): return
    if not GOOGLE_OAUTH_TOKEN: return await message.reply_text("Connect Google Drive first.")
    
    msg = await message.reply_text("📥 Preparing Telegram download...")
    start_time = time.time()
    
    try:
        file_path = await message.download(progress=update_progress, progress_args=(msg, start_time, "📥 Downloading from Telegram..."))
        file_name = os.path.basename(file_path)
        
        success, file_id, file_size, up_time = await upload_to_drive_async(file_path, file_name, msg)
        
        if success:
            total_elapsed = time.time() - start_time
            await msg.edit_text(generate_result_text(file_name, file_id, file_size, total_elapsed))
        else:
            await msg.edit_text(f"❌ Upload failed.\nReason: {file_id}")
    finally:
        if 'file_path' in locals() and file_path and os.path.exists(file_path):
            os.remove(file_path)

# ================= WEB SERVER =================
async def handle_home(request): return web.Response(text="Bot running. Go to /login to auth.")
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
    return web.Response(text=f"SUCCESS! Render Env Var GOOGLE_OAUTH_TOKEN:\n\n{json.dumps(token_data)}")

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