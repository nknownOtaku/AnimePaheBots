import os
import asyncio
import requests
import subprocess
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from pyrogram import Client, filters
from pyrogram.types import Message

# ================= CONFIG =================

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

MAIN_CHANNEL = int(os.getenv("MAIN_CHANNEL"))
PROGRESS_CHANNEL = int(os.getenv("PROGRESS_CHANNEL"))

MONGO_URI = os.getenv("MONGO_URI")

MAX_FILE_MB = 700
SOURCE = "animepahe"

DOWNLOAD_DIR = "./downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ================= MONGO =================

mongo = MongoClient(MONGO_URI)
db = mongo["otaku_syndicate"]
episodes_col = db["episodes"]

episodes_col.create_index(
    [("anime_id", ASCENDING),
     ("episode", ASCENDING),
     ("resolution", ASCENDING),
     ("source", ASCENDING)],
    unique=True
)

# ================= TELEGRAM =================

app = Client("otaku_syndicate", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ================= QUEUE =================

download_queue = asyncio.Queue()

# ================= HELPERS =================

def is_duplicate(anime_id, episode, resolution):
    return episodes_col.find_one({
        "anime_id": anime_id,
        "episode": episode,
        "resolution": resolution,
        "source": SOURCE
    }) is not None


def mark_processed(anime_id, anime_title, episode, resolution, file_size):
    episodes_col.insert_one({
        "anime_id": anime_id,
        "anime_title": anime_title,
        "episode": episode,
        "resolution": resolution,
        "file_size": file_size,
        "source": SOURCE,
        "uploaded_at": datetime.utcnow()
    })


def get_file_size_mb(path):
    return os.path.getsize(path) / 1024 / 1024


# ================= DOWNLOAD WORKER =================

async def queue_worker():
    while True:
        job = await download_queue.get()
        try:
            await process_job(job)
        except Exception as e:
            print("Job error:", e)
        download_queue.task_done()


# ================= CORE JOB =================

async def process_job(job):
    anime_id = job["anime_id"]
    anime_title = job["anime_title"]
    episode = job["episode"]
    sources = job["sources"]

    progress_msg = await app.send_message(
        PROGRESS_CHANNEL,
        f"⬇️ Starting\n{anime_title}\nEp {episode}"
    )

    for src in sources:
        url = src["url"]
        resolution = str(src["quality"])

        if is_duplicate(anime_id, episode, resolution):
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
