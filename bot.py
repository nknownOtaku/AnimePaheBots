# bot.py
import os
import asyncio
import re
import json
from pyrogram import Client, filters
from pyrogram.types import Message
from yt_dlp import YoutubeDL
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv

load_dotenv("config.env")

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PROGRESS_CHANNEL = os.getenv("PROGRESS_CHANNEL")
MAX_FILE_MB = min(int(os.getenv("MAX_FILE_MB", 2000)), 1000)  # Cap at 1000 MB due to 1GB disk
DOWNLOAD_DIR = "./downloads"
DUPLICATE_LOG = "./downloaded_episodes.txt"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Persistent duplicate tracking
if os.path.exists(DUPLICATE_LOG):
    with open(DUPLICATE_LOG, "r") as f:
        downloaded_episodes = set(line.strip() for line in f)
else:
    downloaded_episodes = set()

def save_duplicate_log():
    with open(DUPLICATE_LOG, "w") as f:
        for key in sorted(downloaded_episodes):
            f.write(key + "\n")

# Global queue and state
download_queue = asyncio.Queue()
is_downloading = False

app = Client("AnimePaheBot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

async def send_progress(text: str):
    if PROGRESS_CHANNEL:
        try:
            await app.send_message(PROGRESS_CHANNEL, text)
        except Exception as e:
            print(f"[Progress] Failed to send: {e}")

def extract_embed_and_info(animepahe_url: str) -> tuple[str, str, str]:    """
    Returns: (embed_url, anime_title, real_episode_number)
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(animepahe_url, headers=headers, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Get embed URL
    iframe = soup.find("iframe", src=True)
    if not iframe:
        raise ValueError("No embed found")
    embed_url = iframe["src"]

    # Get anime title from URL
    title_match = re.search(r"/play/([^/]+)", animepahe_url)
    anime_title = title_match.group(1).replace("-", " ").title() if title_match else "Unknown Anime"

    # Try to get real episode number
    ep_num = "0"

    # Method 1: Look for <h1> or similar
    h1 = soup.find("h1")
    if h1 and re.search(r"episode\s*\d+", h1.text, re.I):
        ep_match = re.search(r"\d+", h1.text)
        if ep_match:
            ep_num = ep_match.group()

    # Method 2: Look in script tags for JSON
    if ep_num == "0":
        for script in soup.find_all("script"):
            if script.string and "episode" in script.string and ("id" in script.string or "number" in script.string):
                try:
                    # Extract JSON from "var episode = {...};"
                    js_content = script.string
                    if "var episode =" in js_content:
                        json_str = js_content.split("var episode =")[1].split(";")[0].strip()
                        data = json.loads(json_str)
                        ep_num = str(data.get("episode") or data.get("number") or data.get("ep", "0"))
                        break
                except:
                    continue

    return embed_url, anime_title, ep_num

async def process_download_queue():
    global is_downloading
    while True:
        task = await download_queue.get()
        client, message, url = task        is_downloading = True
        try:
            await _download_episode(client, message, url)
        except Exception as e:
            await message.reply_text(f"💥 Fatal error: {str(e)[:500]}")
        finally:
            is_downloading = False
            download_queue.task_done()

async def _download_episode(client: Client, message: Message, anime_url: str):
    try:
        await send_progress(f"🔍 Parsing episode info from {anime_url}")
        embed_url, raw_title, ep_num = extract_embed_and_info(anime_url)
        clean_title = re.sub(r'[<>:"/\\|?*]', '_', raw_title)
        base_name = f"{clean_title} - Episode {ep_num}"

        await send_progress(f"🎬 Found: {base_name}")

        # Get video info
        with YoutubeDL({"quiet": True}) as ydl:
            info = ydl.extract_info(embed_url, download=False)
            duration = info.get("duration", 0)

        if duration > 10800:  # >3 hours
            await message.reply_text("⚠️ Video too long—skipping.")
            return

        success = False
        filename = None
        for res in ["480", "720", "1080"]:
            episode_key = f"{clean_title}_{ep_num}_{res}"
            if episode_key in downloaded_episodes:
                await send_progress(f"⏭️ Already downloaded: {episode_key}")
                continue

            filename = os.path.join(DOWNLOAD_DIR, f"{base_name} [{res}p].mp4")

            # Estimate size
            with YoutubeDL({"quiet": True}) as ydl:
                info = ydl.extract_info(embed_url, download=False)
                filesize = info.get("filesize") or info.get("filesize_approx") or 0
                filesize_mb = filesize / (1024 * 1024)

            if filesize_mb > MAX_FILE_MB:
                await send_progress(f"❌ {res}p too large ({filesize_mb:.1f} MB)")
                continue

            # Download
            await send_progress(f"📥 Downloading {base_name} [{res}p]...")
            ydl_opts = {                "format": f"bestvideo[height<={res}]+bestaudio/best[height<={res}]",
                "outtmpl": filename,
                "noplaylist": True,
                "quiet": False,
                "geo_bypass": True,
                "socket_timeout": 20,
            }
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([embed_url])

            if not os.path.exists(filename):
                await send_progress("❗ File not created")
                continue

            # Upload
            final_size_mb = os.path.getsize(filename) / (1024 * 1024)
            await send_progress(f"📤 Sending {base_name} [{res}p]...")

            await message.reply_document(
                document=filename,
                caption=f"✅ {base_name}\nResolution: {res}p\nSize: {final_size_mb:.1f} MB",
                force_document=True
            )

            # ✅ DELETE FILE IMMEDIATELY TO SAVE SPACE
            try:
                os.remove(filename)
                await send_progress("🗑️ File deleted after upload.")
            except Exception as e:
                await send_progress(f"⚠️ Failed to delete file: {e}")

            downloaded_episodes.add(episode_key)
            save_duplicate_log()
            success = True
            break

        if not success:
            await message.reply_text("❌ All resolutions skipped (too large or already downloaded).")
            # Clean up partial file if exists
            if filename and os.path.exists(filename):
                os.remove(filename)

    except Exception as e:
        # Cleanup on error
        if 'filename' in locals() and os.path.exists(filename):
            os.remove(filename)
        raise e

@app.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):    await message.reply_text(
        "👋 Send:\n`/download <AnimePahe episode URL>`\n"
        "Episodes are processed one-by-one. Files auto-deleted after upload."
    )

@app.on_message(filters.command("download") & filters.private)
async def manual_download(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("UsageId: `/download <AnimePahe episode URL>`")
        return

    url = message.command[1]
    if "animepahe" not in url:
        await message.reply_text("⚠️ Only AnimePahe URLs supported.")
        return

    await message.reply_text("✅ Added to queue. Processing one at a time...")

    await download_queue.put((client, message, url))

    # Start queue processor if not running
    if not any(t.get_name() == "QueueProcessor" for t in asyncio.all_tasks()):
        asyncio.create_task(process_download_queue(), name="QueueProcessor")

if __name__ == "__main__":
    print("🚀 Bot starting (1GB-safe mode)...")
    app.run()
