import os, sys, json, time, logging, asyncio, re, functools, zipfile, shutil, urllib.parse, tarfile, io, requests

try: asyncio.get_event_loop()
except RuntimeError: asyncio.set_event_loop(asyncio.new_event_loop())

import aiohttp, aiofiles, aiosqlite
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
from pyrogram.errors import MessageNotModified
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from cachetools import TTLCache

try: import rarfile; HAS_RARFILE = True
except ImportError: HAS_RARFILE = False

try: import py7zr; HAS_7Z = True
except ImportError: HAS_7Z = False

try: import cv2; HAS_CV2 = True
except ImportError: HAS_CV2 = False

# ================= ENV =================
API_ID             = int(os.environ.get("API_ID", "0").strip())
API_HASH           = os.environ.get("API_HASH", "").strip()
BOT_TOKEN          = os.environ.get("BOT_TOKEN", "").strip()
DRIVE_FOLDER_ID    = os.environ.get("DRIVE_FOLDER_ID", "").strip()
GOOGLE_OAUTH_TOKEN = os.environ.get("GOOGLE_OAUTH_TOKEN", "").strip()
CF_WORKER_URL      = os.environ.get("CF_WORKER_URL", "").strip().rstrip('/')
AUTH_USERS_STR     = os.environ.get("AUTHORIZED_USERS", "")
PORT               = int(os.environ.get("PORT", 8080))

AUTHORIZED_USERS = [int(u.strip()) for u in AUTH_USERS_STR.split(",") if u.strip().isdigit()]
SCOPES           = ['https://www.googleapis.com/auth/drive']

# /tmp is safe on Render free tier
TMP_DIR     = "/tmp/bot_dl"
EXTRACT_DIR = "/tmp/bot_ext"
PREVIEW_DIR = "/tmp/bot_prev"
DB_PATH     = "/tmp/bot_data.db"
for _d in [TMP_DIR, EXTRACT_DIR, PREVIEW_DIR]: os.makedirs(_d, exist_ok=True)

ARCHIVE_EXTS = ('.zip', '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.rar', '.7z')

# ================= BINARY SETUP =================
def setup_binaries():
    print("Initializing binaries...")
    hdrs = {'User-Agent': 'Mozilla/5.0'}

    if GOOGLE_OAUTH_TOKEN:
        try:
            td = json.loads(GOOGLE_OAUTH_TOKEN)
            rclone_token = {"access_token": td.get("token",""), "token_type": "Bearer",
                            "refresh_token": td.get("refresh_token",""), "expiry": "2030-01-01T00:00:00.000000000Z"}
            conf = (f"[gdrive]\ntype = drive\nclient_id = {td.get('client_id','')}\n"
                    f"client_secret = {td.get('client_secret','')}\nscope = drive\n"
                    f"token = {json.dumps(rclone_token)}\nroot_folder_id = {DRIVE_FOLDER_ID}\n")
            open("rclone.conf","w").write(conf)
            print("rclone.conf OK")
        except Exception as e: print("rclone.conf error:", e)

    if not os.path.exists("./rclone"):
        try:
            r = requests.get("https://downloads.rclone.org/v1.65.0/rclone-v1.65.0-linux-amd64.zip", headers=hdrs, allow_redirects=True)
            open("rclone.zip","wb").write(r.content)
            with zipfile.ZipFile("rclone.zip",'r') as z:
                for info in z.infolist():
                    if info.filename.endswith("rclone") and not info.is_dir():
                        info.filename = "rclone"; z.extract(info,".")
            os.chmod("./rclone",0o755); os.remove("rclone.zip")
            print("rclone ready")
        except Exception as e: print("rclone download failed:", e)

    if not os.path.exists("./aria2c"):
        try:
            url = "https://github.com/P3TERX/Aria2-Pro-Core/releases/download/1.37.0/aria2-1.37.0-static-linux-amd64.tar.gz"
            r = requests.get(url, headers=hdrs, allow_redirects=True)
            open("aria2.tar.gz","wb").write(r.content)
            with tarfile.open("aria2.tar.gz","r:gz") as t:
                for m in t.getmembers():
                    if m.name.endswith("aria2c") and m.isfile(): m.name="aria2c"; t.extract(m,".")
            os.chmod("./aria2c",0o755); os.remove("aria2.tar.gz")
            print("aria2c ready")
        except Exception as e: print("aria2c failed:", e)

    if not os.path.exists("./unrar"):
        try:
            r = requests.get("https://www.rarlab.com/rar/rarlinux-x64-6.2.6.tar.gz", headers=hdrs, allow_redirects=True, timeout=30)
            open("unrar.tar.gz","wb").write(r.content)
            with tarfile.open("unrar.tar.gz","r:gz") as t:
                for m in t.getmembers():
                    if m.name.endswith("unrar") and m.isfile(): m.name="unrar"; t.extract(m,".")
            if os.path.exists("./unrar"): os.chmod("./unrar",0o755)
            if os.path.exists("unrar.tar.gz"): os.remove("unrar.tar.gz")
            print("unrar ready")
        except Exception as e: print("unrar failed (RAR support unavailable):", e)

setup_binaries()
if HAS_RARFILE and os.path.exists("./unrar"): rarfile.UNRAR_TOOL = os.path.abspath("./unrar")

# ================= LOGGING =================
class MemLog(logging.Handler):
    def __init__(self, cap=20): super().__init__(); self.cap=cap; self.logs=[]
    def emit(self, r):
        self.logs.append(self.format(r))
        if len(self.logs) > self.cap: self.logs.pop(0)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_ch)
mem_log = MemLog(20)
mem_log.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logger.addHandler(mem_log)

# ================= DATABASE =================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS task_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            file_name TEXT, file_size INTEGER, file_id TEXT,
            elapsed REAL, is_folder INTEGER DEFAULT 0,
            created_at INTEGER DEFAULT (strftime('%s','now')))""")
        await db.commit()

async def db_save(uid, name, size, fid, elapsed, is_folder=False):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO task_history (user_id,file_name,file_size,file_id,elapsed,is_folder) VALUES (?,?,?,?,?,?)",
                (uid, name, size, fid, elapsed, 1 if is_folder else 0))
            await db.commit()
    except Exception as e: logger.error(f"DB error: {e}")

async def db_history(uid, limit=10):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT file_name,file_size,file_id,elapsed,is_folder FROM task_history "
                "WHERE user_id=? ORDER BY id DESC LIMIT ?", (uid, limit)) as cur:
                return await cur.fetchall()
    except: return []

# ================= STATE =================
LINK_CACHE     = TTLCache(maxsize=100, ttl=86400)
USER_STATES    = TTLCache(maxsize=100, ttl=3600)
MYFILES_CACHE  = TTLCache(maxsize=100, ttl=86400)
PREVIEW_CACHE  = TTLCache(maxsize=50,  ttl=7200)
CANCEL_FLAGS   = {}
PROGRESS_TIMES = {}  # keyed by cancel_id — no shared state between concurrent tasks
BOT_STATS      = {"uploads": 0, "clones": 0, "bytes_uploaded": 0}

app = Client("speedpro_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)

# ================= HELPERS =================
def get_svc():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GOOGLE_OAUTH_TOKEN), SCOPES)
        if creds.expired and creds.refresh_token: creds.refresh(Request())
        return True, build('drive','v3',credentials=creds)
    except Exception as e: return False, f"OAuth Error: {e}"

def get_token():
    creds = Credentials.from_authorized_user_info(json.loads(GOOGLE_OAUTH_TOKEN), SCOPES)
    if creds.expired and creds.refresh_token: creds.refresh(Request())
    return creds.token

def fmt_sz(b):
    if b>=1024**3: return f"{b/1024**3:.2f} GB"
    if b>=1024**2: return f"{b/1024**2:.2f} MB"
    return f"{b/1024:.2f} KB"

def fmt_t(s):
    m,s=divmod(int(s),60); h,m=divmod(m,60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def safe_err(e): return str(e).replace('<','').replace('>','')[:800]
def cbtn(cid): return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{cid}")]])
def chk(uid): return not AUTHORIZED_USERS or uid in AUTHORIZED_USERS
def is_arc(n): return any(n.lower().endswith(e) for e in ARCHIVE_EXTS)

def res_txt(name, fid, size, elapsed, is_folder=False):
    lnk = (f"https://drive.google.com/drive/folders/{fid}" if is_folder
           else f"https://drive.google.com/file/d/{fid}/view")
    dl = "Not Configured"
    if CF_WORKER_URL:
        base = (CF_WORKER_URL if CF_WORKER_URL.endswith("/") else CF_WORKER_URL+"/")
        if "0:/" not in base: base += "0:/"
        dl = base + urllib.parse.quote(name) + ("/" if is_folder else "")
    return (f"✅ **Done!**\n\n📄 **Name:** `{name}`\n📦 **Size:** `{fmt_sz(size)}`\n"
            f"⏱️ **Time:** `{fmt_t(elapsed)}`\n\n🔗 [Google Drive]({lnk})\n⚡ [Direct Link]({dl})")

async def get_meta(url):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(url, allow_redirects=True) as r:
                size = int(r.headers.get('content-length',0))
                cd = r.headers.get('content-disposition','')
                name = None
                if cd:
                    m = re.findall(r"filename\*?=(?:UTF-8'')?([^;]+)", cd, re.IGNORECASE)
                    if m: name = urllib.parse.unquote(m[0].strip().strip('"').strip("'"))
                if not name:
                    name = urllib.parse.unquote(r.url.name)
                    if not name or name=='/': name = f"file_{int(time.time())}"
                return name, size
    except: return f"file_{int(time.time())}", 0

def gd_id(url):
    for pat,fld in [(r"/d/([a-zA-Z0-9_-]+)",False),(r"[?&]id=([a-zA-Z0-9_-]+)",False),(r"/folders/([a-zA-Z0-9_-]+)",True)]:
        m=re.search(pat,url)
        if m: return m.group(1),fld
    return None,False

def gen_frames(vpath, pid):
    if not HAS_CV2: return []
    try:
        cap=cv2.VideoCapture(vpath); total=int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total<=0: return []
        paths=[]
        for i in range(10):
            cap.set(cv2.CAP_PROP_POS_FRAMES,int(total*(0.05+0.09*i)))
            ret,frame=cap.read()
            if ret:
                p=f"{PREVIEW_DIR}/{pid}_{i}.jpg"
                cv2.imwrite(p,cv2.resize(frame,(640,360)),[int(cv2.IMWRITE_JPEG_QUALITY),80])
                paths.append(p)
        cap.release(); return paths
    except: return []

# ================= ARCHIVE EXTRACTION =================
def extract_sync(archive_path, dest):
    """Handles .zip .tar .tar.gz .tgz .tar.bz2 .rar .7z"""
    n = archive_path.lower()
    try:
        if n.endswith('.zip'):
            with zipfile.ZipFile(archive_path,'r') as z: z.extractall(dest)
        elif any(n.endswith(e) for e in ('.tar','.tar.gz','.tgz','.tar.bz2','.tbz2')):
            with tarfile.open(archive_path,'r:*') as t: t.extractall(dest)
        elif n.endswith('.rar'):
            if not HAS_RARFILE: return False,"rarfile not installed"
            if not os.path.exists("./unrar"): return False,"unrar binary missing"
            with rarfile.RarFile(archive_path) as r: r.extractall(dest)
        elif n.endswith('.7z'):
            if not HAS_7Z: return False,"py7zr not installed"
            with py7zr.SevenZipFile(archive_path,mode='r') as z: z.extractall(path=dest)
        else: return False,"Unsupported format"
        return True,"OK"
    except Exception as e: return False,str(e)

# ================= PROGRESS =================
async def prog(cur, tot, msg, t0, label, cid=None):
    if cid and CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
    if tot==0: tot=cur+1
    now=time.time(); key=cid or "default"
    if (now-PROGRESS_TIMES.get(key,0)>3.0) or (cur>=tot):
        PROGRESS_TIMES[key]=now
        pct=min(100.0,cur/tot*100)
        bar="🟩"*int(pct/10)+"⬜"*(10-int(pct/10))
        el=now-t0; spd=cur/el if el>0 else 0; eta=(tot-cur)/spd if spd>0 else 0
        txt=(f"{label}\n\n{bar} {pct:.1f}%\n📦 {fmt_sz(cur)} / {fmt_sz(tot)}\n"
             f"🚀 {fmt_sz(spd)}/s  ⏳ ETA: {fmt_t(eta)}")
        try: await msg.edit_text(txt, reply_markup=cbtn(cid) if cid else None)
        except MessageNotModified: pass

# ================= DOWNLOAD ENGINES =================
async def _part(ses, url, sb, eb, path, pg, msg, t0, cid):
    async with ses.get(url, headers={'Range':f'bytes={sb}-{eb}'}) as r:
        r.raise_for_status()
        async with aiofiles.open(path,'wb') as f:
            async for chunk in r.content.iter_chunked(2*1024*1024):
                if CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
                await f.write(chunk); pg['done']+=len(chunk)
                await prog(pg['done'],pg['total'],msg,t0,"⚡ Downloading...",cid)

def _merge(fp, n):
    with open(fp,'wb') as out:
        for i in range(n):
            p=f"{fp}.part{i}"
            with open(p,'rb') as inp: shutil.copyfileobj(inp,out,length=4*1024*1024)
            os.remove(p)

async def dl_native(url, fp, msg, cid, ld):
    t0=time.time(); total=ld.get("size",0)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=16),
                                     timeout=aiohttp.ClientTimeout(total=0)) as s:
        rng=False
        try:
            async with s.head(url,allow_redirects=True) as hr:
                rng=hr.headers.get('Accept-Ranges')=='bytes'
                if total==0: total=int(hr.headers.get('content-length',0))
        except: pass
        if rng and total>10*1024*1024:
            np=16 if total>2*1024**3 else (8 if total>500*1024**2 else 4)
            ps=total//np; pg={'done':0,'total':total}
            tasks=[_part(s,url,i*ps,total-1 if i==np-1 else i*ps+ps-1,
                         f"{fp}.part{i}",pg,msg,t0,cid) for i in range(np)]
            await asyncio.gather(*tasks)
            if CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
            await msg.edit_text("⚙️ Merging...", reply_markup=cbtn(cid))
            await asyncio.to_thread(_merge,fp,np)
        else:
            done=0
            async with aiofiles.open(fp,'wb') as f:
                async with s.get(url) as r:
                    r.raise_for_status()
                    async for chunk in r.content.iter_chunked(2*1024*1024):
                        if CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
                        await f.write(chunk); done+=len(chunk)
                        if total>0: await prog(done,total,msg,t0,"📥 Downloading...",cid)

async def dl_aria2(url, fp, msg, cid):
    d,fn=os.path.dirname(fp) or ".",os.path.basename(fp)
    cmd=[os.path.abspath("./aria2c"),"--dir",d,"--out",fn,
         "--split=16","--max-connection-per-server=16",
         "--min-split-size=5M","--file-allocation=none",
         "--max-tries=5","--retry-wait=3","--piece-length=1M",
         "--async-dns=false","--summary-interval=3",url]
    proc=await asyncio.create_subprocess_exec(*cmd,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
    last=time.time()
    while True:
        if CANCEL_FLAGS.get(cid): proc.terminate(); raise Exception("CANCELLED")
        line=await proc.stdout.readline()
        if not line: break
        txt=line.decode('utf-8').strip()
        if txt.startswith("[#") and "DL:" in txt and time.time()-last>3:
            try: await msg.edit_text(f"⚡ **[Aria2c]**\n\n`{txt}`",reply_markup=cbtn(cid)); last=time.time()
            except: pass
    await proc.wait()
    if proc.returncode!=0: raise Exception("Aria2c failed")

# ================= STREAMING PIPE UPLOAD =================
async def stream_upload(url, name, msg, cid, total_size, parent_id):
    """Zero-disk: downloads and uploads to Drive simultaneously via resumable API."""
    if total_size==0: return False,"Size unknown",0
    try: token=await asyncio.to_thread(get_token)
    except Exception as e: return False,f"Token error: {e}",0

    CHUNK=32*1024*1024  # 32MB — must be multiple of 256KB per Drive API
    t0=time.time()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0)) as ses:
        # initiate resumable upload session
        async with ses.post(
            'https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable',
            headers={'Authorization':f'Bearer {token}','Content-Type':'application/json',
                     'X-Upload-Content-Type':'application/octet-stream',
                     'X-Upload-Content-Length':str(total_size)},
            data=json.dumps({'name':name,'parents':[parent_id]})) as r:
            if r.status!=200: return False,f"Init failed: {r.status}",0
            upload_url=r.headers['Location']

        buf,done,offset=bytearray(),0,0
        async with ses.get(url,timeout=aiohttp.ClientTimeout(total=0)) as dl:
            dl.raise_for_status()
            async for chunk in dl.content.iter_chunked(2*1024*1024):
                if CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
                buf.extend(chunk); done+=len(chunk)
                is_last=done>=total_size
                if len(buf)>=CHUNK or is_last:
                    data=bytes(buf); c_end=offset+len(data)-1
                    async with ses.put(upload_url,
                        headers={'Authorization':f'Bearer {token}',
                                 'Content-Range':f'bytes {offset}-{c_end}/{total_size}',
                                 'Content-Length':str(len(data))}, data=data) as up:
                        if up.status in (200,201):
                            fid=(await up.json()).get('id')
                            BOT_STATS["uploads"]+=1; BOT_STATS["bytes_uploaded"]+=total_size
                            return True,fid,total_size
                        elif up.status==308:
                            offset+=len(data); buf=bytearray()
                            await prog(done,total_size,msg,t0,"🌊 Stream Uploading...",cid)
                        else:
                            err=await up.text()
                            return False,f"Chunk failed {up.status}: {err[:200]}",0
    return False,"Stream ended unexpectedly",0

# ================= UPLOAD ENGINES =================
async def ul_native(fp, name, msg, parent_id=DRIVE_FOLDER_ID, cid=None, t0=None):
    if not t0: t0=time.time()
    ok,svc=get_svc()
    if not ok: return False,svc,0,0
    fsz=os.path.getsize(fp)
    # 50MB chunks for Drive resumable API
    media=MediaFileUpload(fp,chunksize=50*1024*1024,resumable=True)
    req=svc.files().create(body={'name':name,'parents':[parent_id]},media_body=media,fields='id')
    resp=None
    while resp is None:
        if cid and CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
        status,resp=await asyncio.to_thread(functools.partial(req.next_chunk,num_retries=5))
        if status: await prog(status.resumable_progress,fsz,msg,t0,"☁️ Uploading...",cid)
    BOT_STATS["uploads"]+=1; BOT_STATS["bytes_uploaded"]+=fsz
    return True,resp.get('id'),fsz,time.time()-t0

async def ul_rclone(fp, name, msg, parent_id=DRIVE_FOLDER_ID, cid=None, t0=None):
    is_dir=os.path.isdir(fp)
    cmd=[os.path.abspath("./rclone"),"copy" if is_dir else "copyto",fp,f"gdrive:{name}",
         "--config",os.path.abspath("rclone.conf"),"-P",
         "--drive-chunk-size=128M","--transfers=4","--checkers=8","--buffer-size=256M"]  # 128MB chunks
    proc=await asyncio.create_subprocess_exec(*cmd,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
    last,last_out=time.time(),""
    while True:
        if CANCEL_FLAGS.get(cid): proc.terminate(); raise Exception("CANCELLED")
        line=await proc.stdout.readline()
        if not line: break
        txt=line.decode('utf-8').strip()
        if txt: last_out=txt
        if "Transferred:" in txt and "ETA" in txt and time.time()-last>3:
            try: await msg.edit_text(f"☁️ **[Rclone]**\n\n`{txt}`",reply_markup=cbtn(cid)); last=time.time()
            except: pass
    await proc.wait()
    if proc.returncode!=0: raise Exception(f"Rclone failed: {last_out}")
    ok,svc=get_svc()
    if not ok: return False,"Auth Error",0,0
    try:
        q=f"name='{name}' and '{parent_id}' in parents and trashed=false"
        r=await asyncio.to_thread(lambda: svc.files().list(q=q,fields="files(id,size)",pageSize=1).execute())
        items=r.get('files',[])
        if items:
            fid=items[0]['id']; fsz=int(items[0].get('size',0)) if not is_dir else 0
            BOT_STATS["uploads"]+=1; BOT_STATS["bytes_uploaded"]+=fsz
            return True,fid,fsz,time.time()-(t0 or time.time())
        return False,"File ID not found",0,0
    except Exception as e: return False,str(e),0,0

# ================= GDRIVE CLONE =================
async def gd_clone(item_id, is_folder=False, parent_id=DRIVE_FOLDER_ID, msg=None):
    try:
        ok,svc=get_svc()
        if not ok: return False,svc
        if not is_folder:
            r=await asyncio.to_thread(lambda: svc.files().copy(fileId=item_id,body={'parents':[parent_id]},fields='id,name').execute())
            BOT_STATS["clones"]+=1; return True,r
        orig=await asyncio.to_thread(lambda: svc.files().get(fileId=item_id,fields='name').execute())
        nf=await asyncio.to_thread(lambda: svc.files().create(
            body={'name':orig['name'],'mimeType':'application/vnd.google-apps.folder','parents':[parent_id]},fields='id').execute())
        new_id=nf['id']
        items=(await asyncio.to_thread(lambda: svc.files().list(
            q=f"'{item_id}' in parents and trashed=false",fields="files(id,name,mimeType)",pageSize=1000).execute())).get('files',[])
        for item in items:
            if msg:
                try: await msg.edit_text(f"🔄 Cloning: `{item['name']}`...")
                except: pass
            await gd_clone(item['id'],item['mimeType']=='application/vnd.google-apps.folder',new_id)
        BOT_STATS["clones"]+=1; return True,{"name":orig['name'],"id":new_id}
    except Exception as e: return False,safe_err(e)

# ================= FOLDER UPLOAD FALLBACK =================
async def ul_folder_native(ext_dir, fname, parent_id, msg, cid, t0):
    ok,svc=get_svc()
    if not ok: return False,None
    base_id=(await asyncio.to_thread(lambda: svc.files().create(
        body={'name':fname,'mimeType':'application/vnd.google-apps.folder','parents':[parent_id]},fields='id').execute()))['id']
    fm={ext_dir:base_id}; total=0
    for root,dirs,files in os.walk(ext_dir):
        if CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
        cp=fm[root]
        for d in dirs:
            nid=(await asyncio.to_thread(lambda dn=d,p=cp: svc.files().create(
                body={'name':dn,'mimeType':'application/vnd.google-apps.folder','parents':[p]},fields='id').execute()))['id']
            fm[os.path.join(root,d)]=nid
        for f in files:
            if CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
            fpath=os.path.join(root,f)
            try: await msg.edit_text(f"☁️ Uploading: `{f}`...",reply_markup=cbtn(cid))
            except: pass
            up_ok,_,_,_=await ul_native(fpath,f,msg,cp,cid,t0)
            if up_ok: total+=1
    return total>0,base_id  # (bool, folder_id)

# ================= MAIN PROCESS =================
async def process_dl(client, message, url, fname, extract=False, uid=None):
    msg=await app.send_message(message.chat.id,"📥 Preparing...")
    fp=os.path.join(TMP_DIR,fname); t0=time.time(); cid=str(message.id)
    CANCEL_FLAGS[cid]=False
    ld=LINK_CACHE.get(message.id,{}); is_gd=ld.get("is_gd",False)
    gid=ld.get("gd_id"); gsz=ld.get("gd_size",0)

    try:
        # GDrive server-side copy — no download
        if is_gd and not extract:
            ok,svc=get_svc()
            await msg.edit_text("🔄 GDrive Cloning...")
            r=await asyncio.to_thread(lambda: svc.files().copy(
                fileId=gid,body={'name':fname,'parents':[DRIVE_FOLDER_ID]},fields='id').execute())
            el=time.time()-t0
            await msg.edit_text(res_txt(fname,r['id'],gsz,el))
            await db_save(uid or 0,fname,gsz,r['id'],el); return

        # Streaming pipe upload — zero disk write
        if not is_gd and not extract and ld.get("size",0)>0:
            await msg.edit_text("🌊 Stream Uploading (disk-free)...",reply_markup=cbtn(cid))
            ok,fid,fsz=await stream_upload(url,fname,msg,cid,ld["size"],DRIVE_FOLDER_ID)
            if ok:
                el=time.time()-t0
                await msg.edit_text(res_txt(fname,fid,fsz,el))
                await db_save(uid or 0,fname,fsz,fid,el); return
            await msg.edit_text("⚠️ Stream failed — switching to disk download...",reply_markup=cbtn(cid))
            await asyncio.sleep(1)

        # Download to /tmp
        if not is_gd:
            if os.path.exists("./aria2c"):
                try:
                    await msg.edit_text("⚡ Aria2c Downloading...",reply_markup=cbtn(cid))
                    await dl_aria2(url,fp,msg,cid)
                except Exception as e:
                    if str(e)=="CANCELLED": raise
                    await msg.edit_text("⚡ [Fallback] Native...",reply_markup=cbtn(cid))
                    await dl_native(url,fp,msg,cid,ld)
            else:
                await msg.edit_text("⚡ Downloading...",reply_markup=cbtn(cid))
                await dl_native(url,fp,msg,cid,ld)
        else:
            ok,svc=get_svc(); req=svc.files().get_media(fileId=gid)
            fh=io.FileIO(fp,'wb'); dl=MediaIoBaseDownload(fh,req,chunksize=20*1024*1024); done=False
            while not done:
                if CANCEL_FLAGS.get(cid): fh.close(); raise Exception("CANCELLED")
                st,done=await asyncio.to_thread(functools.partial(dl.next_chunk,num_retries=3))
                if st:
                    try: await msg.edit_text(f"📥 GDrive: {int(st.progress()*100)}%",reply_markup=cbtn(cid))
                    except: pass
            fh.close()

        # Extract archive
        if extract and is_arc(fname):
            ext_dir=os.path.join(EXTRACT_DIR,f"e{int(time.time())}"); os.makedirs(ext_dir,exist_ok=True)
            await msg.edit_text(f"📦 Extracting `{fname}`...")
            ok,err=await asyncio.to_thread(extract_sync,fp,ext_dir)
            if not ok: return await msg.edit_text(f"❌ Extract Error: {err}")
            base=re.sub(r'\.(zip|tar\.gz|tgz|tar\.bz2|tbz2|tar|rar|7z)$','',fname,flags=re.IGNORECASE)
            up_ok,new_fid=False,None
            if os.path.exists("./rclone") and os.path.exists("rclone.conf"):
                try:
                    await msg.edit_text("☁️ [Rclone] Uploading folder...",reply_markup=cbtn(cid))
                    up_ok,new_fid,_,_=await ul_rclone(ext_dir,base,msg,DRIVE_FOLDER_ID,cid,t0)
                except Exception as e:
                    if str(e)=="CANCELLED": raise
            if not up_ok:
                await msg.edit_text("☁️ [Native] Uploading folder...",reply_markup=cbtn(cid))
                up_ok,new_fid=await ul_folder_native(ext_dir,base,DRIVE_FOLDER_ID,msg,cid,t0)
            await asyncio.to_thread(shutil.rmtree,ext_dir,ignore_errors=True)
            if up_ok:
                el=time.time()-t0
                await msg.edit_text(res_txt(base,new_fid,0,el,True))
                await db_save(uid or 0,base,0,new_fid,el,is_folder=True)
            else: await msg.edit_text("❌ Folder upload failed")
            return

        # Video preview
        prev_id=None
        if fname.lower().endswith(('.mp4','.mkv','.avi','.webm')):
            await msg.edit_text("🎬 Extracting frames...",reply_markup=cbtn(cid))
            pid=str(int(time.time()))
            frames=await asyncio.to_thread(gen_frames,fp,pid)
            if frames: prev_id=pid; PREVIEW_CACHE[pid]=frames

        # Upload
        up_ok=False; fid=fsz=None
        if os.path.exists("./rclone") and os.path.exists("rclone.conf"):
            try:
                await msg.edit_text("☁️ [Rclone] Uploading...",reply_markup=cbtn(cid))
                ok,fid,fsz,_=await ul_rclone(fp,fname,msg,DRIVE_FOLDER_ID,cid,t0); up_ok=ok
            except Exception as e:
                if str(e)=="CANCELLED": raise
        if not up_ok:
            await msg.edit_text("☁️ [Native] Uploading...",reply_markup=cbtn(cid))
            ok,fid,fsz,_=await ul_native(fp,fname,msg,DRIVE_FOLDER_ID,cid,t0); up_ok=ok

        if up_ok:
            el=time.time()-t0
            rm=InlineKeyboardMarkup([[InlineKeyboardButton("🎬 Preview",callback_data=f"pv|{prev_id}")]]) if prev_id else None
            await msg.edit_text(res_txt(fname,fid,fsz,el),reply_markup=rm)
            await db_save(uid or 0,fname,fsz,fid,el)
        else: await msg.edit_text(f"❌ Upload failed: {fid}")

    except Exception as e:
        if str(e)=="CANCELLED": await msg.edit_text("🚫 Cancelled!")
        else: logger.error(f"Error: {e}"); await msg.edit_text(f"❌ Error: {safe_err(e)}")
    finally:
        CANCEL_FLAGS.pop(cid,None); PROGRESS_TIMES.pop(cid,None)
        if os.path.exists(fp):
            try: os.remove(fp)
            except: pass
        for f in os.listdir(TMP_DIR):
            if f.endswith('.aria2'):
                try: os.remove(os.path.join(TMP_DIR,f))
                except: pass

# ================= COMMANDS =================
@app.on_message(filters.command("start"))
async def cmd_start(_,msg):
    if not chk(msg.from_user.id): return
    rar_ok="✅" if (HAS_RARFILE and os.path.exists("./unrar")) else "❌"
    z7_ok="✅" if HAS_7Z else "❌"
    await msg.reply_text(
        "🚀 **SpeedPro Bot**\n\n"
        "Send a URL, GDrive link, or upload a file.\n\n"
        f"📦 ZIP / TAR / TGZ / TBZ2: ✅\n📦 RAR: {rar_ok}\n📦 7z: {z7_ok}\n\n"
        "/myfiles · /search · /history · /stats · /storage · /cancel_all · /logs")

@app.on_message(filters.command("stats"))
async def cmd_stats(_,msg):
    if not chk(msg.from_user.id): return
    await msg.reply_text(f"📊 **Session Stats**\n\n📤 Uploads: `{BOT_STATS['uploads']}`\n"
                         f"🔄 Clones: `{BOT_STATS['clones']}`\n💾 Uploaded: `{fmt_sz(BOT_STATS['bytes_uploaded'])}`")

@app.on_message(filters.command("history"))
async def cmd_history(_,msg):
    if not chk(msg.from_user.id): return
    rows=await db_history(msg.from_user.id)
    if not rows: return await msg.reply_text("No history yet.")
    lines=["📜 **Last 10 Uploads:**\n"]
    for fn,fs,fid,el,isf in rows:
        lnk=(f"https://drive.google.com/drive/folders/{fid}" if isf else f"https://drive.google.com/file/d/{fid}/view")
        lines.append(f"• [{fn}]({lnk}) — `{fmt_sz(fs)}` in `{fmt_t(el)}`")
    await msg.reply_text("\n".join(lines),disable_web_page_preview=True)

@app.on_message(filters.command("cancel_all"))
async def cmd_cancel_all(_,msg):
    if not chk(msg.from_user.id): return
    n=len(CANCEL_FLAGS)
    for k in list(CANCEL_FLAGS): CANCEL_FLAGS[k]=True
    await msg.reply_text(f"🚫 Cancellation sent to {n} task(s).")

@app.on_message(filters.command("logs"))
async def cmd_logs(_,msg):
    if not chk(msg.from_user.id): return
    txt="\n".join(mem_log.logs) if mem_log.logs else "No logs."
    await msg.reply_text(f"📜 **Logs:**\n`{txt[-3000:]}`")

@app.on_message(filters.command("storage"))
async def cmd_storage(_,msg):
    if not chk(msg.from_user.id): return
    ok,svc=get_svc()
    if not ok: return await msg.reply_text("Auth error.")
    try:
        q=svc.about().get(fields="storageQuota").execute().get('storageQuota',{})
        lim,used=int(q.get('limit',0)),int(q.get('usage',0))
        await msg.reply_text(f"💾 **Drive Storage**\n\nUsed: `{fmt_sz(used)}`\n" +
                             ("Total: `Unlimited`" if lim==0 else f"Free: `{fmt_sz(lim-used)}`\nTotal: `{fmt_sz(lim)}`"))
    except Exception as e: await msg.reply_text(f"Error: {e}")

@app.on_message(filters.command("search"))
async def cmd_search(_,msg):
    if not chk(msg.from_user.id): return
    parts=msg.text.split(maxsplit=1)
    if len(parts)<2: return await msg.reply_text("Usage: `/search <n>`")
    ok,svc=get_svc()
    if not ok: return await msg.reply_text("Auth error.")
    m=await msg.reply_text("🔍 Searching...")
    try:
        r=svc.files().list(q=f"trashed=false and name contains '{parts[1]}'",
                           fields="files(id,name,mimeType,size)",pageSize=20).execute()
        items=r.get('files',[])
        if not items: return await m.edit_text("Nothing found.")
        MYFILES_CACHE[msg.from_user.id]={"items":items,"page":0,"parent":"search","stack":[]}
        await render_page(m,msg.from_user.id)
    except Exception as e: await m.edit_text(f"Error: {e}")

@app.on_message(filters.command("myfiles"))
async def cmd_myfiles(_,msg):
    if not chk(msg.from_user.id): return
    await fetch_folder(msg,msg.from_user.id,DRIVE_FOLDER_ID,init=True)

# ================= MYFILES UI =================
async def fetch_folder(obj, uid, folder_id, init=False):
    ok,svc=get_svc()
    if not ok: return
    m=await obj.reply_text("Fetching...") if init else obj.message
    try:
        r=svc.files().list(q=f"'{folder_id}' in parents and trashed=false",
                           orderBy="folder,modifiedTime desc",
                           fields="files(id,name,mimeType,size)",pageSize=100).execute()
        stack=MYFILES_CACHE.get(uid,{}).get("stack",[])  # safe fallback if cache expired
        MYFILES_CACHE[uid]={"items":r.get('files',[]),"page":0,"parent":folder_id,"stack":[] if init else stack}
        await render_page(m,uid)
    except: await m.edit_text("Error fetching files.")

async def render_page(msg, uid):
    cache=MYFILES_CACHE.get(uid)
    if not cache: return await msg.edit_text("Session expired.")
    items,pg=cache["items"],cache["page"]; per=4
    total=max(1,(len(items)+per-1)//per)
    if not items:
        btns=[[InlineKeyboardButton("🔼 Back",callback_data="mf_back")]] if cache["stack"] else None
        return await msg.edit_text("📂 Empty!",reply_markup=InlineKeyboardMarkup(btns) if btns else None)
    sl=items[pg*per:(pg+1)*per]; txt,btns=f"📁 **Files (Page {pg+1}/{total}):**\n\n",[]
    for i,item in enumerate(sl):
        is_f=item['mimeType']=='application/vnd.google-apps.folder'
        txt+=f"{i+1}. {'📁' if is_f else '📄'} `{item['name']}`\n"
        btns.append([InlineKeyboardButton(f"📂 Open #{i+1}" if is_f else f"⚙️ Options #{i+1}",
                                          callback_data=f"mf_act|{pg*per+i}")])
    nav=[]
    if pg>0: nav.append(InlineKeyboardButton("⬅️ Prev",callback_data="mf_nav|prev"))
    if pg<total-1: nav.append(InlineKeyboardButton("Next ➡️",callback_data="mf_nav|next"))
    if nav: btns.append(nav)
    if cache["stack"]: btns.append([InlineKeyboardButton("🔼 Back",callback_data="mf_back")])
    await msg.edit_text(txt,reply_markup=InlineKeyboardMarkup(btns))

# ================= CALLBACKS =================
@app.on_callback_query(filters.regex(r"^mf_"))
async def cb_myfiles(_,query):
    act=query.data.split("|")[0]; data=query.data.split("|")[1] if "|" in query.data else None
    cache=MYFILES_CACHE.get(query.from_user.id)

    # auto re-fetch root instead of showing "Expired" — handles bot restarts
    if not cache:
        await query.answer("Session refreshed — reloading...", show_alert=False)
        await fetch_folder(query, query.from_user.id, DRIVE_FOLDER_ID, init=False)
        return

    if act=="mf_nav":
        cache["page"]+=1 if data=="next" else -1
        await render_page(query.message,query.from_user.id)
    elif act=="mf_back":
        if cache["stack"]:
            await fetch_folder(query,query.from_user.id,cache["stack"].pop(),False)
        else:
            # already at root — just re-render
            await fetch_folder(query,query.from_user.id,DRIVE_FOLDER_ID,init=False)
    elif act=="mf_act":
        item=cache["items"][int(data)]
        if item['mimeType']=='application/vnd.google-apps.folder':
            cache["stack"].append(cache["parent"])
            await fetch_folder(query,query.from_user.id,item['id'],False)
        else:
            txt=res_txt(item['name'],item['id'],int(item.get('size',0)),0)
            btns=[[InlineKeyboardButton("✏️ Rename",callback_data=f"ren_file|{item['id']}"),
                   InlineKeyboardButton("🗑️ Remove",callback_data=f"del_file|{item['id']}")],
                  [InlineKeyboardButton("🔙 Back",callback_data="mf_ret")]]
            await query.message.edit_text(txt,reply_markup=InlineKeyboardMarkup(btns))
    elif act=="mf_ret": await render_page(query.message,query.from_user.id)

@app.on_callback_query(~filters.regex(r"^mf_"))
async def cb_general(_,query):
    parts=query.data.split("|"); act=parts[0]; data=parts[1] if len(parts)>1 else None  # safe split
    if act=="cancel":
        CANCEL_FLAGS[data]=True; await query.answer("Cancelling...")
    elif act in ("dl_now","dl_ext"):
        ld=LINK_CACHE.get(int(data))
        if not ld: return await query.answer("Expired.",show_alert=True)
        await query.message.delete()
        await process_dl(None,query.message,ld["url"],ld["name"],extract=(act=="dl_ext"),uid=query.from_user.id)
    elif act=="dl_ren":
        ld=LINK_CACHE.get(int(data))
        if not ld: return await query.answer("Expired.",show_alert=True)
        USER_STATES[query.from_user.id]={"action":"wait_rename","url":ld["url"],"is_gd":ld.get("is_gd"),
                                         "gd_id":ld.get("gd_id"),"gd_size":ld.get("gd_size"),"size":ld.get("size",0)}
        await query.message.delete(); await query.message.reply_text("Send **new name** with extension:")
    elif act=="ren_file":
        USER_STATES[query.from_user.id]={"action":"wait_drive_rename","file_id":data}
        await query.message.delete()
        await app.send_message(query.from_user.id,"Send **new name** with extension:")
    elif act=="del_file":
        try:
            ok,svc=get_svc(); svc.files().update(fileId=data,body={'trashed':True}).execute()
            await query.answer("✅ Moved to trash!",show_alert=True); await query.message.delete()
        except: await query.answer("❌ Delete failed.",show_alert=True)
    elif act=="pv":
        paths=PREVIEW_CACHE.get(data)
        if paths: await query.answer("Sending..."); await app.send_media_group(query.message.chat.id,[InputMediaPhoto(p) for p in paths])
        else: await query.answer("Preview expired.",show_alert=True)

# ================= TELEGRAM FILE HANDLER =================
@app.on_message(filters.document | filters.video | filters.audio)
async def handle_tg_file(_,message):
    if not chk(message.from_user.id): return
    msg=await message.reply_text("📥 Downloading from Telegram..."); cid=str(message.id)
    CANCEL_FLAGS[cid]=False; t0=time.time(); fpath=None
    try:
        async def pcb(cur,tot):
            if CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
            if time.time()-getattr(pcb,'_t',0)>3:
                try: await msg.edit_text(f"📥 TG Download: {cur/tot*100:.1f}%",reply_markup=cbtn(cid))
                except: pass
                pcb._t=time.time()
        fpath=await message.download(file_name=os.path.join(TMP_DIR,"tg_upload"),progress=pcb)
        if CANCEL_FLAGS.get(cid): raise Exception("CANCELLED")
        fname=os.path.basename(fpath); up_ok=False
        if os.path.exists("./rclone") and os.path.exists("rclone.conf"):
            try:
                await msg.edit_text("☁️ [Rclone] Uploading...",reply_markup=cbtn(cid))
                ok,fid,fsz,_=await ul_rclone(fpath,fname,msg,DRIVE_FOLDER_ID,cid,t0); up_ok=ok
            except Exception as e:
                if str(e)=="CANCELLED": raise
        if not up_ok:
            await msg.edit_text("☁️ [Native] Uploading...",reply_markup=cbtn(cid))
            ok,fid,fsz,_=await ul_native(fpath,fname,msg,DRIVE_FOLDER_ID,cid,t0); up_ok=ok
        if up_ok:
            el=time.time()-t0
            await msg.edit_text(res_txt(fname,fid,fsz,el))
            await db_save(message.from_user.id,fname,fsz,fid,el)
        else: await msg.edit_text("❌ Upload failed.")
    except Exception as e:
        if str(e)=="CANCELLED": await msg.edit_text("🚫 Cancelled.")
        else: await msg.edit_text(f"❌ Error: {safe_err(e)}")
    finally:
        CANCEL_FLAGS.pop(cid,None); PROGRESS_TIMES.pop(cid,None)
        if fpath and os.path.exists(fpath):
            try: os.remove(fpath)
            except: pass

# ================= TEXT / URL HANDLER =================
IGNORE=["start","myfiles","stats","logs","storage","search","history","cancel_all"]

@app.on_message(filters.text & ~filters.command(IGNORE))
async def handle_text(_,message):
    if not chk(message.from_user.id): return
    txt=message.text.strip()
    state=USER_STATES.get(message.from_user.id)
    if state:
        if state["action"]=="wait_rename":
            new_name=txt; del USER_STATES[message.from_user.id]
            if state.get("is_gd"):
                m=await message.reply_text("🔄 Cloning...")
                try:
                    ok,svc=get_svc()
                    r=await asyncio.to_thread(lambda: svc.files().copy(fileId=state['gd_id'],
                        body={'name':new_name,'parents':[DRIVE_FOLDER_ID]},fields='id').execute())
                    await m.edit_text(res_txt(new_name,r['id'],state.get('gd_size',0),0))
                except Exception as e: await m.edit_text(f"❌ Error: {e}")
            else:
                LINK_CACHE[message.id]={"url":state["url"],"name":new_name,"size":state.get("size",0),"is_gd":False}
                await process_dl(None,message,state["url"],new_name,uid=message.from_user.id)
            return
        elif state["action"]=="wait_drive_rename":
            fid=state["file_id"]; del USER_STATES[message.from_user.id]
            ok,svc=get_svc()
            if ok:
                try:
                    svc.files().update(fileId=fid,body={'name':txt}).execute()
                    await message.reply_text(f"✅ Renamed to `{txt}`")
                except Exception as e: await message.reply_text(f"❌ Error: {safe_err(e)}")
            return

    if not re.match(r"https?://",txt): return
    gid_val,is_folder=gd_id(txt)
    if gid_val:
        if is_folder:
            m=await message.reply_text("🔄 Cloning folder...")
            ok,r=await gd_clone(gid_val,True,msg=m)
            if ok:
                await m.edit_text(f"✅ Folder cloned: `{r['name']}`")
                await db_save(message.from_user.id,r['name'],0,r['id'],0,is_folder=True)
            else: await m.edit_text("❌ Clone failed.")
            return
        m=await message.reply_text("🔍 Fetching GDrive info...")
        try:
            ok,svc=get_svc()
            meta=await asyncio.to_thread(lambda: svc.files().get(fileId=gid_val,fields='name,size').execute())
            name=meta.get('name','Unknown'); size=int(meta.get('size',0))
            LINK_CACHE[message.id]={"url":txt,"name":name,"is_gd":True,"gd_id":gid_val,"gd_size":size}
            btns=[[InlineKeyboardButton("🔄 Clone Now",callback_data=f"dl_now|{message.id}"),
                   InlineKeyboardButton("✏️ Rename",callback_data=f"dl_ren|{message.id}")]]
            if is_arc(name): btns.append([InlineKeyboardButton("📦 Clone & Extract",callback_data=f"dl_ext|{message.id}")])
            await m.edit_text(f"🔗 **GDrive File**\n📄 `{name}`\n📦 `{fmt_sz(size)}`",reply_markup=InlineKeyboardMarkup(btns))
        except: await m.edit_text("❌ Not found / Access denied.")
        return

    m=await message.reply_text("🔍 Fetching metadata...")
    name,size=await get_meta(txt)
    LINK_CACHE[message.id]={"url":txt,"name":name,"size":size,"is_gd":False}
    btns=[[InlineKeyboardButton("⬇️ Download",callback_data=f"dl_now|{message.id}"),
           InlineKeyboardButton("✏️ Rename",callback_data=f"dl_ren|{message.id}")]]
    if is_arc(name): btns.append([InlineKeyboardButton("📦 Extract & Upload",callback_data=f"dl_ext|{message.id}")])
    note=" · 🌊 Stream ready" if size>0 else " · ⚠️ Size unknown"
    await m.edit_text(f"🔗 **Direct Link**\n📄 `{name}`\n📦 `{fmt_sz(size)}`{note}",reply_markup=InlineKeyboardMarkup(btns))

# ================= WEB SERVER =================
async def start_web():
    wa=web.Application()
    wa.router.add_get('/',lambda r: web.Response(text="🚀 SpeedPro Bot is alive!"))
    wa.router.add_get('/health',lambda r: web.Response(text="OK",status=200))  # UptimeRobot endpoint
    runner=web.AppRunner(wa)
    await runner.setup()
    await web.TCPSite(runner,'0.0.0.0',PORT).start()
    logger.info(f"Web server on port {PORT}")

# ================= MAIN =================
async def main():
    await init_db()
    await start_web()
    await app.start()
    logger.info("SpeedPro Bot LIVE — Aria2 + Rclone + Stream + TAR/RAR/7z + SQLite")
    await idle()
    await app.stop()

if __name__=="__main__":
    loop=asyncio.get_event_loop()
    loop.run_until_complete(main())