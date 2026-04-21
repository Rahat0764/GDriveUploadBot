<h1 align="center">🚀 Ultimate GDrive Uploader & Manager Bot</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Python-v3.14.3-blue.svg?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Framework-Pyrogram-purple.svg" alt="Pyrogram">
  <img src="https://img.shields.io/badge/Cloud-Google%20Drive-yellow.svg?logo=googledrive" alt="Google Drive">
  <img src="https://img.shields.io/badge/CDN-Cloudflare%20Workers-orange.svg?logo=cloudflare" alt="Cloudflare">
</p>

<p align="center">
  A powerful, highly advanced Telegram Bot that acts as a bridge between Telegram, Google Drive, and Cloudflare. It allows high-speed uploads, instant cloning, ZIP extraction, video previews, and generates ultra-fast direct download links via GoIndex.
</p>

---

## ✨ Premium Features

- **📥 Direct Link Uploads:** Send any direct download URL and the bot will upload it straight to your Google Drive. Includes options to rename before downloading.
- **🔄 Instant GDrive Clone:** Send a public Google Drive File or Folder link to instantly clone it to your drive. Uses server-side API execution (No data consumed!).
- **📦 Smart ZIP Extraction:** Extract ZIP files directly to your Drive in a structured folder format. Safely handles and warns about password-protected archives.
- **🎬 Video Previews:** Automatically generates a beautiful 10-frame collage preview image for uploaded videos (`.mp4`, `.mkv`, etc.).
- **⚡ High-Speed Direct Links:** Integrated with Cloudflare Workers (GoIndex) to provide direct, shareable, high-speed download links.
- **📁 Advanced File Manager:** Use `/myfiles` to browse your Drive with pagination. Rename, Delete, and navigate through folders interactively via inline buttons.
- **🔍 Fuzzy Search:** Easily find files using the `/search <name>` command with advanced fuzzy matching.
- **📊 System Stats & Logs:** Check your Drive capacity with `/storage`, view session activities with `/stats`, and debug using `/logs`.

---

## 🛠️ Prerequisites & Requirements

Before deploying, ensure you have the following:

1. **Telegram API ID & Hash:** Get it from [my.telegram.org](https://my.telegram.org).
2. **Bot Token:** Create a bot via [@BotFather](https://t.me/BotFather).
3. **Google Drive Folder ID:** The ID of the folder where files will be uploaded.
4. **Google Cloud Console Setup:** You need a `client_secret.json` from Google Cloud (OAuth 2.0).
5. **Cloudflare Account:** (Optional but recommended) For GoIndex direct links.

### 📦 `requirements.txt`
```text
pyrogram
tgcrypto
aiohttp
google-api-python-client
google-auth-httplib2
google-auth-oauthlib
opencv-python-headless
Pillow
```

---

## 🚀 Installation & Deployment Guide

This bot is heavily optimized for deployment on Platforms like **Render**, Heroku, or VPS.

### 1. Environment Variables Config

| Variable | Description |
| :--- | :--- |
| `API_ID` | Your Telegram API ID. |
| `API_HASH` | Your Telegram API Hash. |
| `BOT_TOKEN` | Your Telegram Bot Token. |
| `AUTHORIZED_USERS` | Comma-separated User IDs (e.g., `12345,67890`). Leave empty to make it public. |
| `DRIVE_FOLDER_ID` | The target folder ID in your Google Drive. |
| `GOOGLE_CLIENT_SECRET` | The raw JSON content of your Google OAuth Client Secret file. |
| `RENDER_EXTERNAL_URL` | Your Web Service URL (e.g., `https://mybot.onrender.com`). |
| `CF_WORKER_URL` | Your Cloudflare Worker GoIndex URL (e.g., `https://gdrive.yourname.workers.dev`). |
| `GOOGLE_OAUTH_TOKEN` | Generated automatically. Follow the OAuth Setup below. |

### 2. Google Drive OAuth Setup (Crucial)
Unlike basic Service Accounts (which lack storage quota), this bot uses **OAuth 2.0** to act on behalf of your real Google account (utilizing your full 15GB/5TB quota).

1. Deploy the bot to Render with the `GOOGLE_CLIENT_SECRET` variable set.
2. Go to your bot's web URL: `https://your-bot-url.onrender.com/login`
3. Log in with your Google Account and grant permissions.
4. Copy the large JSON token displayed on the screen.
5. Add a new Environment Variable in Render named `GOOGLE_OAUTH_TOKEN` and paste the token.
6. Restart the bot!

### 3. Cloudflare GoIndex Setup (Direct Links)
To enable the lightning-fast `[Direct Download Link]`:
1. Go to [Cloudflare Workers](https://dash.cloudflare.com/?to=/:account/workers).
2. Create a new Worker (e.g., `gdrive-index`).
3. Copy the raw JavaScript code from [here](https://github.com/Rahat0764/GDriveUploadBot/blob/main/workers_demo.js) and paste it into your Worker's editor.
4. Replace the `client_id`, `client_secret`, `refresh_token`, and `folder_id` at the top of the script.
5. Deploy the worker and copy its `*.workers.dev` URL to the bot's `CF_WORKER_URL` variable.

---

## 🤖 Bot Commands List
Send this list to `@BotFather` -> `/setcommands`:
```text
start - Start the bot & view main menu
myfiles - Advanced Drive Manager (Rename, Delete, Explore)
search - Search files in Drive (Usage: /search filename)
stats - View session upload & clone statistics
storage - Check Google Drive storage limit and usage
logs - View recent system logs for debugging
```

---

## 👨‍💻 Author & Contact

Crafted with ❤️ by **Rahat Ahmed**.

- 💼 **LinkedIn:** [Rahat Ahmed](https://www.linkedin.com/in/RahatAhmedX)
- 🐙 **GitHub:** [Rahat0764](https://github.com/Rahat0764)

*If you found this project helpful, don't forget to ⭐ star the repository!*