import os
import json
import time
import asyncio
import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Environment Variables
API_ID_STR = os.environ.get("API_ID")
API_ID = int(API_ID_STR) if API_ID_STR and API_ID_STR.isdigit() else None
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
GOOGLE_CREDS_STR = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")

# Authorized Users Setup
AUTH_USERS_STR = os.environ.get("AUTHORIZED_USERS", "")
AUTHORIZED_USERS = [int(u.strip()) for u in AUTH_USERS_STR.split(",") if u.strip().isdigit()]

# Google Drive API scopes
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Telegram Bot Initialization
app = Client(
    "my_drive_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

def get_drive_service():
    """Initializes and returns the Google Drive service."""
    try:
        if not GOOGLE_CREDS_STR:
            print("Credentials JSON is missing in Environment Variables.")
            return None
        creds_dict = json.loads(GOOGLE_CREDS_STR)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=SCOPES)
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"Credentials Error: {e}")
        return None

def format_size(bytes_size):
    """Formats bytes to MB."""
    return f"{bytes_size / (1024 * 1024):.2f} MB"

async def update_progress(current, total, msg, start_time, action_text):
    """Updates the progress message with a visual bar and speed."""
    if total == 0:
        return

    now = time.time()
    if not hasattr(msg, "last_update_time"):
        msg.last_update_time = start_time

    # Update every 3 seconds to avoid FloodWait limits
    if (now - msg.last_update_time > 3.0) or (current == total):
        msg.last_update_time = now
        
        percent = (current / total) * 100
        filled = int(percent / 10)
        bar = "🟩" * filled + "🟥" * (10 - filled)
        
        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        
        text = (
            f"{action_text}\n\n"
            f"{bar} {percent:.1f}%\n"
            f"Size: {format_size(current)} / {format_size(total)}\n"
            f"Speed: {format_size(speed)}/s"
        )
        try:
            await msg.edit_text(text)
        except Exception:
            pass 

async def upload_to_drive_async(file_path, file_name, msg):
    """Uploads file to Drive asynchronously, updating progress."""
    try:
        service = get_drive_service()
        if not service:
            return None
            
        file_size = os.path.getsize(file_path)
        file_metadata = {'name': file_name, 'parents': [DRIVE_FOLDER_ID]}
        
        media = MediaFileUpload(file_path, chunksize=2*1024*1024, resumable=True)
        request = service.files().create(body=file_metadata, media_body=media, fields='id')
        
        response = None
        start_time = time.time()
        
        while response is None:
            status, response = await asyncio.to_thread(request.next_chunk)
            if status:
                await update_progress(
                    status.resumable_progress, file_size, msg, start_time, "☁️ Uploading to Google Drive..."
                )
                
        return response.get('id')
    except Exception as e:
        print(f"Upload error: {e}")
        return None

def check_auth(user_id):
    """Checks if the user is authorized to use the bot."""
    if not AUTHORIZED_USERS:
        return True
    return user_id in AUTHORIZED_USERS

# Message Handlers

@app.on_message(filters.command("start"))
async def start_command(client, message):
    if not check_auth(message.from_user.id):
        await message.reply_text("⛔ You are not authorized to use this bot.")
        return
        
    await message.reply_text(
        "Hello! 👋\n"
        "Send me any file or direct download link, and I will upload it to your Google Drive."
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_files(client, message):
    if not check_auth(message.from_user.id):
        return

    msg = await message.reply_text("📥 Preparing to download...")
    start_time = time.time()
    
    try:
        file_path = await message.download(
            progress=update_progress,
            progress_args=(msg, start_time, "📥 Downloading from Telegram...")
        )
        file_name = os.path.basename(file_path)
        
        drive_file_id = await upload_to_drive_async(file_path, file_name, msg)
        
        if drive_file_id:
            await msg.edit_text(f"✅ Upload Complete!\n\nFile Name: {file_name}")
        else:
            await msg.edit_text("❌ Upload failed. Check Drive ID or Credentials.")
            
        if os.path.exists(file_path):
            os.remove(file_path)
            
    except Exception as e:
        await msg.edit_text(f"❌ An error occurred: {e}")

@app.on_message(filters.regex(r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"))
async def handle_links(client, message):
    if not check_auth(message.from_user.id):
        return

    url = message.text
    msg = await message.reply_text("🔗 Link received! Starting download...")
    
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
                            
        drive_file_id = await upload_to_drive_async(file_path, file_name, msg)
        
        if drive_file_id:
            await msg.edit_text(f"✅ Link Upload Complete!\n\nFile Name: {file_name}")
        else:
            await msg.edit_text("❌ Upload failed.")
            
        if os.path.exists(file_path):
            os.remove(file_path)
            
    except Exception as e:
        await msg.edit_text(f"❌ Error handling link: {e}")

# ================= MAIN RUNNER =================
async def main():
    print("Starting web server...")
    app_web = web.Application()
    app_web.router.add_get('/', lambda r: web.Response(text="Bot is perfectly running 24/7!"))
    runner = web.AppRunner(app_web)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"Web server started on port {port}")

    print("Starting Pyrogram Bot...")
    await app.start()
    print("Bot is successfully running!")
    
    # Keeps the script running to receive updates
    await idle()
    
    await app.stop()

if __name__ == "__main__":
    # Python 3.10+ / 3.14 safe asyncio runner
    asyncio.run(main())