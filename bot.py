import os
import sys
import json
import time
import logging
import asyncio
import re
import traceback
import functools

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
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

# New OAuth Variables
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_OAUTH_TOKEN = os.environ.get("GOOGLE_OAUTH_TOKEN", "").strip()

AUTH_USERS_STR = os.environ.get("AUTHORIZED_USERS", "")
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://gdriveuploadbot.onrender.com")

os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

if not all([API_ID_STR, API_HASH, BOT_TOKEN, DRIVE_FOLDER_ID, GOOGLE_CLIENT_SECRET]):
    logger.error("CRITICAL ERROR: API keys or GOOGLE_CLIENT_SECRET is missing!")
    sys.exit(1)

API_ID = int(API_ID_STR) if API_ID_STR.isdigit() else 0
AUTHORIZED_USERS = [int(u.strip()) for u in AUTH_USERS_STR.split(",") if u.strip().isdigit()]
SCOPES = ['https://www.googleapis.com/auth/drive']

# Global flow object to store the PKCE code verifier
oauth_flow = None

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
        return False, f"⚠️ Bot is not authenticated yet!\nPlease visit:\n{RENDER_URL}/login\nto link your Google Drive."
        
    try:
        creds_data = json.loads(GOOGLE_OAUTH_TOKEN)
        creds = Credentials.from_authorized_user_info(creds_data, SCOPES)
        return True, build('drive', 'v3', credentials=creds)
    except Exception as e:
        return False, f"OAuth Token Error: {str(e)}"

def format_size(bytes_size):
    if bytes_size >= 1024 * 1024 * 1024:
        return f"{bytes_size / (1024 * 1024 * 1024):.2f} GB"
    return f"{bytes_size / (1024 * 1024):.2f} MB"

def format_time(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    elif m > 0: return f"{m}m {s}s"
    return f"{s}s"

def get_safe_error(e):
    err_str = str(e)
    if not err_str.strip(): err_str = repr(e)
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

async def upload_to_drive_async(file_path, file_name, msg):
    try:
        success, service_or_error = get_drive_service()
        if not success: return False, service_or_error
            
        service = service_or_error
        file_size = os.path.getsize(file_path)
        file_metadata = {'name': file_name, 'parents': [DRIVE_FOLDER_ID]}
        
        media = MediaFileUpload(file_path, chunksize=20*1024*1024, resumable=True)
        request = service.files().create(body=file_metadata, media_body=media, fields='id')
        
        response = None
        start_time = time.time()
        
        while response is None:
            status, response = await asyncio.to_thread(functools.partial(request.next_chunk, num_retries=5))
            if status:
                await update_progress(status.resumable_progress, file_size, msg, start_time, "☁️ Uploading to Google Drive...")
                
        return True, response.get('id')
    except Exception as e:
        return False, get_safe_error(e)

def extract_gdrive_id(url):
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1)
    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if match: return match.group(1)
    return None

async def clone_gdrive_file(file_id):
    try:
        success, service_or_error = get_drive_service()
        if not success: return False, service_or_error
            
        service = service_or_error
        body = {'parents': [DRIVE_FOLDER_ID]}
        
        response = await asyncio.to_thread(
            lambda: service.files().copy(fileId=file_id, body=body, fields='id, name').execute()
        )
        return True, response
    except Exception as e:
        return False, get_safe_error(e)

def check_auth(user_id):
    if not AUTHORIZED_USERS: return True
    return user_id in AUTHORIZED_USERS

# ================= MESSAGE HANDLERS =================

@app.on_message(filters.command("start"))
async def start_command(client, message):
    if not check_auth(message.from_user.id):
        await message.reply_text("⛔ You are not authorized.")
        return
        
    if not GOOGLE_OAUTH_TOKEN:
        await message.reply_text(f"⚠️ Google Drive is NOT connected!\n\nPlease open this link in your browser to login:\n{RENDER_URL}/login\n\nAfter logging in, copy the text and paste it in Render as `GOOGLE_OAUTH_TOKEN`.")
        return
        
    await message.reply_text("Hello! 👋\nSend me any file, direct download link, or Google Drive link.\n- Normal links/files will be downloaded & uploaded.\n- GDrive links will be directly cloned instantly!")

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_files(client, message):
    if not check_auth(message.from_user.id): return
    if not GOOGLE_OAUTH_TOKEN:
        await message.reply_text(f"⚠️ Please authenticate first: {RENDER_URL}/login")
        return

    msg = await message.reply_text("📥 Preparing to download...")
    start_time = time.time()
    
    try:
        file_path = await message.download(
            progress=update_progress,
            progress_args=(msg, start_time, "📥 Downloading from Telegram...")
        )
        
        if not file_path:
            await msg.edit_text("❌ Failed to download from Telegram.")
            return
            
        file_name = os.path.basename(file_path)
        success, result = await upload_to_drive_async(file_path, file_name, msg)
        
        if success: await msg.edit_text(f"✅ Upload Complete!\n\n📄 File: {file_name}")
        else: await msg.edit_text(f"❌ Upload failed.\n\n⚠️ Reason:\n{result}")
            
    except Exception as e:
        await msg.edit_text(f"❌ Download Error:\n{get_safe_error(e)}")
    finally:
        if 'file_path' in locals() and file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.regex(r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"))
async def handle_links(client, message):
    if not check_auth(message.from_user.id): return
    if not GOOGLE_OAUTH_TOKEN:
        await message.reply_text(f"⚠️ Please authenticate first: {RENDER_URL}/login")
        return

    url = message.text
    
    if "drive.google.com" in url:
        g_id = extract_gdrive_id(url)
        if g_id:
            msg = await message.reply_text("🔄 Google Drive link detected!\nCloning directly to your Drive...")
            success, result = await clone_gdrive_file(g_id)
            if success:
                file_name = result.get('name', 'Unknown Name')
                await msg.edit_text(f"✅ GDrive Clone Complete (Instant)!\n\n📄 File: {file_name}")
            else:
                await msg.edit_text(f"❌ GDrive Clone Failed.\n\nReason:\n{result}")
            return

    msg = await message.reply_text("🔗 Link received! Starting download...")
    file_path = None
    
    try:
        file_name = url.split("/")[-1]
        if not file_name or '?' in file_name:
            file_name = f"download_{int(time.time())}"
            
        file_path = os.path.join(os.getcwd(), file_name)
        start_time = time.time()
        
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
                            
        success, result = await upload_to_drive_async(file_path, file_name, msg)
        
        if success: await msg.edit_text(f"✅ Link Upload Complete!\n\n📄 File: {file_name}")
        else: await msg.edit_text(f"❌ Upload failed.\n\n⚠️ Reason:\n{result}")
            
    except OSError as e: 
        await msg.edit_text(f"❌ Server Storage Full!\n\n⚠️ Reason:\n{get_safe_error(e)}")
    except Exception as e:
        await msg.edit_text(f"❌ Error handling link:\n{get_safe_error(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

# ================= WEB SERVER (HANDLES OAUTH) =================
async def handle_home(request):
    return web.Response(text="Bot is perfectly running! Go to /login to authenticate.")

async def handle_login(request):
    global oauth_flow
    if not GOOGLE_CLIENT_SECRET:
        return web.Response(text="Error: GOOGLE_CLIENT_SECRET is missing in Render variables!")
    try:
        oauth_flow = Flow.from_client_config(json.loads(GOOGLE_CLIENT_SECRET), scopes=SCOPES)
        redirect_uri = f"https://{request.host}/callback"
        oauth_flow.redirect_uri = redirect_uri
        auth_url, _ = oauth_flow.authorization_url(prompt='consent', access_type='offline')
        return web.HTTPFound(auth_url)
    except Exception as e:
        return web.Response(text=f"Login Error: {str(e)}")

async def handle_callback(request):
    global oauth_flow
    code = request.query.get('code')
    if not code:
        return web.Response(text="Error: No authorization code provided.")
    if not oauth_flow:
        return web.Response(text="Error: Session expired. Please go back to /login and try again.")
        
    try:
        # Fetch token using the SAME flow object to maintain the code_verifier
        oauth_flow.fetch_token(code=code)
        creds = oauth_flow.credentials
        
        token_data = {
            'token': creds.token,
            'refresh_token': creds.refresh_token,
            'token_uri': creds.token_uri,
            'client_id': creds.client_id,
            'client_secret': creds.client_secret,
            'scopes': creds.scopes
        }
        token_json = json.dumps(token_data)
        
        # Clear flow from memory after success
        oauth_flow = None 
        
        return web.Response(text=f"SUCCESS!\n\nCopy the entire text below and paste it into a NEW Environment Variable named GOOGLE_OAUTH_TOKEN in Render:\n\n{token_json}")
    except Exception as e:
        return web.Response(text=f"Callback Error: {str(e)}")

async def start_web_server():
    app_web = web.Application()
    app_web.router.add_get('/', handle_home)
    app_web.router.add_get('/login', handle_login)
    app_web.router.add_get('/callback', handle_callback)
    
    runner = web.AppRunner(app_web)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server started on port {port}")

# ================= MAIN RUNNER =================
async def main():
    logger.info("Initializing Web Server...")
    await start_web_server()
    logger.info("Starting Pyrogram Bot...")
    await app.start()
    logger.info("Bot is SUCCESSFULLY running! ✅ All Set!")
    await idle()
    await app.stop()

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.error(f"FATAL ERROR in main loop: {e}")