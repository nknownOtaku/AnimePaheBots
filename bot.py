import os
import asyncio
import json
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import yt_dlp
from datetime import datetime

# =====================
# ENV CONFIG
# =====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

MAIN_CHANNEL = int(os.getenv("MAIN_CHANNEL"))  # e.g., -1001234567890
PROGRESS_CHANNEL = int(os.getenv("PROGRESS_CHANNEL"))

MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", 700))
DOWNLOAD_DIR = "./downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

DB_FILE = "downloads.json"

# =====================
# LOAD LOCAL DB
# =====================
if os.path.exists(DB_FILE):
    with open(DB_FILE, "r") as f:
        db = json.load(f)
else:
    db = {"episodes": []}

def save_db():
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=2)

def is_duplicate(anime_id, episode, quality):
    return any(
        e["anime_id"] == anime_id and e["episode"] == episode and e["quality"] == quality
        for e in db["episodes"]
    )

def add_episode(anime_id, episode, quality, filename):
    db["episodes"].append({
        "anime_id": anime_id,
        "episode": episode,
        "quality": quality,
        "filename": filename,
        "timestamp": str(datetime.utcnow())
    })
    save_db()

# =====================
# DOWNLOAD QUEUE
# =====================
download_queue = asyncio.Queue()
is_downloading = False

async def process_queue(client):
    global is_downloading
    while True:
        anime_id, episode, url = await download_queue.get()
        is_downloading = True
        # Download qualities sequentially
        for quality in ["480p", "720p", "1080p"]:
            await download_episode(client, anime_id, episode, url, quality)
        is_downloading = False
        download_queue.task_done()

# =====================
# DOWNLOAD FUNCTION
# =====================
async def download_episode(client, anime_id, episode, url, quality):
    if is_duplicate(anime_id, episode, quality):
        await client.send_message(PROGRESS_CHANNEL, f"⚠️ Episode {episode} {quality} already downloaded.")
        return

    await client.send_message(PROGRESS_CHANNEL, f"⬇️ Downloading Episode {episode} - {quality}...")

    ydl_opts = {
        'outtmpl': os.path.join(DOWNLOAD_DIR, f'{anime_id}_EP{episode}_{quality}.%(ext)s'),
        'noplaylist': True,
        'progress_hooks': [lambda d: asyncio.create_task(progress_hook(client, anime_id, episode, quality, d))],
        'quiet': True,
        'format': f'bestvideo[height<={quality[:-1]}]+bestaudio/best'
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filesize_mb = info.get('filesize', 0) / (1024*1024)
            if filesize_mb > MAX_FILE_MB:
                await client.send_message(PROGRESS_CHANNEL, f"⚠️ Skipped Episode {episode} {quality} (>{MAX_FILE_MB}MB).")
                return
            filename = ydl.prepare_filename(info)
            add_episode(anime_id, episode, quality, filename)
            await client.send_message(PROGRESS_CHANNEL, f"✅ Downloaded Episode {episode} {quality}: {filename}")
            # Upload to main channel
            await client.send_document(MAIN_CHANNEL, filename, caption=f"Episode {episode} - {quality}")
    except Exception as e:
        await client.send_message(PROGRESS_CHANNEL, f"❌ Failed Episode {episode} {quality}: {e}")

async def progress_hook(client, anime_id, episode, quality, d):
    if d['status'] == 'downloading':
        percent = d.get('_percent_str', '0.0%')
        speed = d.get('_speed_str', '0 B/s')
        await client.send_message(PROGRESS_CHANNEL, f"⬇️ EP{episode} {quality} Downloading: {percent} at {speed}")

# =====================
# TELEGRAM BOT
# =====================
app = Client("anime_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

@app.on_message(filters.command("start"))
async def start(_, message):
    await message.reply_text("👋 Anime Downloader Bot is online.\nUse /download to download manually.")

@app.on_message(filters.command("download"))
async def manual_download(_, message):
    try:
        args = message.text.split(maxsplit=3)
        anime_id = args[1]
        episode = args[2]
        url = args[3]
    except:
        await message.reply_text("Usage: /download <anime_id> <episode> <url>")
        return

    await download_queue.put((anime_id, episode, url))
    await message.reply_text(f"✅ Added Episode {episode} to download queue (480p → 720p → 1080p).")

@app.on_message(filters.command("status"))
async def status(_, message):
    status_msg = "⏳ Download Queue:\n"
    queue_list = list(download_queue._queue)
    for i, item in enumerate(queue_list, 1):
        status_msg += f"{i}. Anime: {item[0]}, Episode: {item[1]}\n"
    status_msg += f"\nCurrently Downloading: {'Yes' if is_downloading else 'No'}"
    await message.reply_text(status_msg)

# =====================
# START BOT
# =====================
async def main():
    asyncio.create_task(process_queue(app))
    await app.start()
    print("Bot started...")
    from pyrogram import idle
    await idle()
    await app.stop()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())        if is_duplicate(anime_id, episode, resolution):
            continue

        filename = f"{anime_title} - Ep{episode} [{resolution}].mp4"
        filepath = os.path.join(DOWNLOAD_DIR, filename)

        await progress_msg.edit_text(
            f"⬇️ Downloading\n{anime_title}\nEp {episode}\n{resolution}"
        )

        # yt-dlp download
        cmd = [
            "yt-dlp",
            url,
            "-o", filepath,
            "--no-part"
        ]

        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()

        if not os.path.exists(filepath):
            continue

        size_mb = get_file_size_mb(filepath)
        if size_mb > MAX_FILE_MB:
            os.remove(filepath)
            await progress_msg.edit_text(
                f"⛔ Skipped >{MAX_FILE_MB}MB\n{anime_title}\nEp {episode}\n{resolution}"
            )
            continue

        await progress_msg.edit_text(
            f"⬆️ Uploading\n{anime_title}\nEp {episode}\n{resolution}\n{size_mb:.1f}MB"
        )

        sent = await app.send_video(
            MAIN_CHANNEL,
            video=filepath,
            caption=f"{anime_title}\nEpisode {episode}\nQuality {resolution}"
        )

        try:
            mark_processed(anime_id, anime_title, episode, resolution, size_mb)
        except DuplicateKeyError:
            pass

        os.remove(filepath)

        await progress_msg.edit_text(
            f"✅ Uploaded\n{anime_title}\nEp {episode}\n{resolution}"
        )

    await progress_msg.edit_text(
        f"🏁 Done\n{anime_title}\nEp {episode}"
    )

# ================= ANIMEPAHE API =================

def get_latest():
    return requests.get("http://api.soheru.in/animepahe/airing").json()["data"]

def get_episode_links(session):
    return requests.get(f"http://api.soheru.in/animepahe/download/{session}").json()

# ================= AUTO LATEST =================

async def auto_latest():
    for x in get_latest():
        anime_title = x["anime_title"]
        episode = str(x["episode"]).zfill(2)
        session = x["session"]

        anime_id = anime_title.lower().replace(" ", "_")

        details = get_episode_links(session)
        sources = details.get("sources", [])

        if not sources:
            continue

        await download_queue.put({
            "anime_id": anime_id,
            "anime_title": anime_title,
            "episode": episode,
            "sources": sources
        })

# ================= MANUAL COMMAND =================

@app.on_message(filters.command("download"))
async def manual_download(_, msg: Message):
    try:
        _, session = msg.text.split(maxsplit=1)
    except:
        return await msg.reply("Usage: /download <animepahe_session_id>")

    details = get_episode_links(session)
    anime_title = details.get("anime_title", "Manual")
    episode = details.get("episode", "00")
    sources = details.get("sources", [])

    anime_id = anime_title.lower().replace(" ", "_")

    await download_queue.put({
        "anime_id": anime_id,
        "anime_title": anime_title,
        "episode": episode,
        "sources": sources
    })

    await msg.reply(f"Queued: {anime_title} Ep {episode}")

# ================= SCHEDULER =================

scheduler = AsyncIOScheduler()
scheduler.add_job(lambda: asyncio.create_task(auto_latest()), "interval", minutes=3)
scheduler.start()

# ================= START =================

async def main():
    asyncio.create_task(queue_worker())
    await app.start()
    print("Otaku Syndicate Bot Running...")
    await asyncio.Event().wait()

asyncio.run(main())
