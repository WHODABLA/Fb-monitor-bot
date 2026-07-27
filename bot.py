"""
Facebook Recovery Monitor — Discord Bot
--------------------------------------
Tracks Facebook Pages/Profiles you flag as "banned/restricted" and posts a
Discord notification the moment they become reachable again.

- Only the server owner can use any command.
- Storage: Cloudflare Worker + D1 database (survives restarts).
- Checking: Apify's Facebook Pages Scraper.

*** IMPORTANT — READ BEFORE DEPLOYING ***
The field names in _check_via_apify() below (followers, title, pageId, etc.)
are based on that actor's public documentation, NOT a verified real test —
unlike the Instagram bot, this hasn't been confirmed against real output yet.
Before relying on this:
  1. Run the actor manually in Apify's console against a real Facebook Page
  2. Compare the real JSON response's field names against what's used below
  3. Fix any mismatches (this is exactly the mistake that broke the
     Instagram version repeatedly — don't skip this step)

Commands (slash commands, server owner only):
  /track pageurl          - start tracking a Facebook Page/Profile URL
  /untrack pageurl        - stop tracking one
  /list                   - show everything currently being tracked
  /setchannel             - set the channel this bot posts recovery alerts to
  /checknow pageurl       - debug: immediately check one page/profile

Setup: see README.md
"""

import os
import re
import asyncio
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks
import aiohttp
from dotenv import load_dotenv
from flask import Flask
from threading import Thread

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "60"))
PORT = int(os.getenv("PORT", "8080"))

D1_WORKER_URL = os.getenv("D1_WORKER_URL", "").rstrip("/")
if D1_WORKER_URL and not D1_WORKER_URL.startswith(("http://", "https://")):
    D1_WORKER_URL = f"https://{D1_WORKER_URL}"
D1_API_KEY = os.getenv("D1_API_KEY", "")

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "")  # set after you confirm the actor works — see README
APIFY_URL = f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/run-sync-get-dataset-items?token={APIFY_TOKEN}"

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


# ---------- owner-only check ----------

def is_owner():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message("This command only works in a server.", ephemeral=True)
            return False
        if interaction.user.id != interaction.guild.owner_id:
            await interaction.response.send_message("❌ Only the server owner can use this bot.", ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)


# ---------- keep-alive web server (for Render free tier + UptimeRobot) ----------

keep_alive_app = Flask(__name__)


@keep_alive_app.route("/")
def home():
    return "Facebook Recovery Monitor is running."


def run_keep_alive():
    keep_alive_app.run(host="0.0.0.0", port=PORT)


def start_keep_alive():
    t = Thread(target=run_keep_alive)
    t.daemon = True
    t.start()


# ---------- D1 storage (via Cloudflare Worker API) ----------

def _d1_headers():
    return {
        "Authorization": f"Bearer {D1_API_KEY}",
        "Content-Type": "application/json",
    }


async def api_get_tracked() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{D1_WORKER_URL}/tracked", headers=_d1_headers()) as resp:
            rows = await resp.json()
            return {
                row["username"]: {
                    "start_time": row["start_time"],
                    "recovered": bool(row["recovered"]),
                    "recovered_at": row.get("recovered_at"),
                }
                for row in rows
            }


async def api_add_tracked(page_key: str, start_time: str):
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{D1_WORKER_URL}/tracked",
            headers=_d1_headers(),
            json={"username": page_key, "start_time": start_time},
        )


async def api_mark_recovered(page_key: str, recovered_at: str):
    async with aiohttp.ClientSession() as session:
        await session.patch(
            f"{D1_WORKER_URL}/tracked/{page_key}",
            headers=_d1_headers(),
            json={"recovered_at": recovered_at},
        )


async def api_remove_tracked(page_key: str):
    async with aiohttp.ClientSession() as session:
        await session.delete(f"{D1_WORKER_URL}/tracked/{page_key}", headers=_d1_headers())


async def api_get_config() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{D1_WORKER_URL}/config", headers=_d1_headers()) as resp:
            return await resp.json()


async def api_set_config(key: str, value) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/config", headers=_d1_headers(), json={key: value}
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_set_config({key}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_set_config({key}) failed: {type(e).__name__}: {e}", flush=True)
        return False


# ---------- Facebook status check (via Apify) ----------

def _normalize_facebook_url(page_key: str) -> str:
    """Accepts a plain page name, a full URL, or an @handle and returns a
    full facebook.com URL — Facebook Pages don't have one consistent short
    handle format like Instagram usernames do."""
    page_key = page_key.strip()
    if page_key.startswith("http://") or page_key.startswith("https://"):
        return page_key
    page_key = page_key.lstrip("@")
    return f"https://www.facebook.com/{page_key}"


async def check_facebook_status(page_key: str):
    """Returns None if the page is unreachable (banned/restricted/not
    found). Returns a dict of page stats if reachable."""
    url = _normalize_facebook_url(page_key)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                APIFY_URL,
                headers={"Content-Type": "application/json"},
                json={"startUrls": [{"url": url}]},
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
                # Accept any 2xx status code (200, 201, etc.)
                if resp.status >= 300:
                    body = (await resp.text())[:300]
                    print(f"[apify-fb] {page_key}: HTTP {resp.status} — {body}", flush=True)
                    return None
                data = await resp.json(content_type=None)
                if not data or not isinstance(data, list) or len(data) == 0:
                    print(f"[apify-fb] {page_key}: 2xx OK but empty dataset — {data}", flush=True)
                    return None
                item = data[0]
                if item.get("error"):
                    print(f"[apify-fb] {page_key}: error in response — {item}", flush=True)
                    return None
                
                # Parse followers and likes from the info field
                followers = 0
                likes = 0
                if item.get("info") and isinstance(item["info"], list):
                    for info_text in item["info"]:
                        if "likes" in info_text.lower():
                            # Extract number from "13,315,381 likes"
                            match = re.search(r'([\d,]+)\s+likes', info_text)
                            if match:
                                likes = int(match.group(1).replace(',', ''))
                        if "talking about this" in info_text.lower():
                            # Extract number from "1,390 talking about this"
                            match = re.search(r'([\d,]+)\s+talking', info_text)
                            if match:
                                followers = int(match.group(1).replace(',', ''))
                
                # Get category from categories array
                category = ""
                if item.get("categories") and isinstance(item["categories"], list) and len(item["categories"]) > 0:
                    category = item["categories"][0]
                
                # Check if we have enough data to consider this a valid page
                if not item.get("facebookUrl") and not item.get("title"):
                    print(f"[apify-fb] {page_key}: no usable data — {item}", flush=True)
                    return None
                
                return {
                    "page_key": page_key,
                    "title": item.get("title") or page_key,
                    "followers": followers,
                    "likes": likes,
                    "category": category,
                    "profile_pic_url": "",  # Not available in current Apify response
                    "url": item.get("facebookUrl") or url,
                }
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as e:
        print(f"[apify-fb] {page_key}: {type(e).__name__}: {e}", flush=True)
        return None


# ---------- timer + embed ----------

def format_elapsed_long(start_iso: str) -> str:
    start = datetime.fromisoformat(start_iso)
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} hours, {minutes} minutes, {seconds} seconds"


def format_elapsed_short(start_iso: str) -> str:
    start = datetime.fromisoformat(start_iso)
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def build_recovery_message(info: dict, start_iso: str) -> str:
    return (
        f"[Account Recovered | {info['title']}]({info['url']}) 🏆✅ |\n"
        f"*Followers: {info['followers']} | Likes: {info['likes']}*\n"
        f"⏱️ *Time taken: {format_elapsed_long(start_iso)}*\n"
        f"{info['url']}"
    )


# ---------- background loop ----------

@tasks.loop(minutes=CHECK_INTERVAL_MINUTES)
async def check_tracked_pages():
    try:
        tracked = await api_get_tracked()
        if not tracked:
            print("check_tracked_pages: nothing tracked, skipping.", flush=True)
            return

        config = await api_get_config()
        channel_id = config.get("notify_channel_id")
        if not channel_id:
            print("check_tracked_pages: no notify channel set, skipping.", flush=True)
            return

        channel = bot.get_channel(int(channel_id))
        if channel is None:
            try:
                channel = await bot.fetch_channel(int(channel_id))
            except discord.HTTPException as e:
                print(f"check_tracked_pages: could not fetch channel {channel_id}: {e}", flush=True)
                return

        print(f"check_tracked_pages: checking {len(tracked)} page(s)...", flush=True)
        for page_key, meta in tracked.items():
            if meta.get("recovered"):
                continue
            info = await check_facebook_status(page_key)
            if info is not None:
                message = build_recovery_message(info, meta["start_time"])
                await channel.send(content=message)
                await api_mark_recovered(page_key, datetime.now(timezone.utc).isoformat())
                print(f"✅ Recovered: {page_key}", flush=True)
    except Exception as e:
        print(f"check_tracked_pages: UNHANDLED ERROR (loop continues): {type(e).__name__}: {e}", flush=True)


@check_tracked_pages.before_loop
async def before_check():
    await bot.wait_until_ready()


# ---------- slash commands (server owner only) ----------

@bot.event
async def on_ready():
    await bot.tree.sync()
    check_tracked_pages.start()
    print(f"Logged in as {bot.user} — checking every {CHECK_INTERVAL_MINUTES} min", flush=True)
    if not APIFY_TOKEN or not APIFY_ACTOR:
        print("WARNING: APIFY_TOKEN or APIFY_ACTOR not set — checks will fail.", flush=True)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        return
    print(f"[command error] /{interaction.command.name if interaction.command else '?'}: {error!r}", flush=True)
    message = f"⚠️ Something went wrong: `{error}`"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.HTTPException:
        pass


@bot.tree.command(name="checknow", description="Debug: immediately check a Facebook Page/Profile")
@app_commands.describe(pageurl="Facebook page name, @handle, or full URL")
@is_owner()
async def checknow(interaction: discord.Interaction, pageurl: str):
    await interaction.response.defer(thinking=True)
    info = await check_facebook_status(pageurl)
    if info:
        await interaction.followup.send(
            f"✅ **{info['title']}** is LIVE — Followers: {info['followers']}, Likes: {info['likes']}"
        )
    else:
        await interaction.followup.send(
            f"❌ `{pageurl}` is NOT reachable — would be treated as still restricted/banned."
        )


@bot.tree.command(name="track", description="Start tracking a Facebook Page/Profile for recovery")
@app_commands.describe(pageurl="Facebook page name, @handle, or full URL")
@is_owner()
async def track(interaction: discord.Interaction, pageurl: str):
    page_key = pageurl.strip()

    # Reject anything that isn't a plausible single entry — catches
    # accidental multi-value pastes, same bug we hit on the Instagram bot.
    if " " in page_key or page_key.count("http") > 1:
        await interaction.response.send_message(
            f"❌ `{page_key}` doesn't look like a single valid page URL/name "
            "(no spaces, one link only). Check for accidental pastes.",
            ephemeral=True,
        )
        return

    tracked = await api_get_tracked()
    if page_key in tracked and not tracked[page_key].get("recovered"):
        await interaction.response.send_message(f"⚠️ Already tracking `{page_key}`.", ephemeral=True)
        return
    await api_add_tracked(page_key, datetime.now(timezone.utc).isoformat())
    await interaction.response.send_message(f"⏱️ Started tracking **{page_key}**. I'll post here when it's back.")


@bot.tree.command(name="untrack", description="Stop tracking a Facebook Page/Profile")
@app_commands.describe(pageurl="Facebook page name, @handle, or full URL")
@is_owner()
async def untrack(interaction: discord.Interaction, pageurl: str):
    page_key = pageurl.strip()
    tracked = await api_get_tracked()
    if page_key in tracked:
        await api_remove_tracked(page_key)
        await interaction.response.send_message(f"✅ Stopped tracking `{page_key}`.")
    else:
        await interaction.response.send_message(f"❌ `{page_key}` isn't being tracked.", ephemeral=True)


@bot.tree.command(name="list", description="List all tracked Facebook Pages/Profiles")
@is_owner()
async def list_tracked(interaction: discord.Interaction):
    tracked = await api_get_tracked()
    if not tracked:
        await interaction.response.send_message("📭 Nothing is being tracked right now.")
        return

    pending = {k: m for k, m in tracked.items() if not m.get("recovered")}
    recovered = {k: m for k, m in tracked.items() if m.get("recovered")}

    lines = [
        "📊 **Tracked Pages:**",
        f"Active: {len(pending)} | Recovered: {len(recovered)}",
        "─" * 32,
    ]
    if pending:
        lines.append("")
        lines.append("**Currently Tracking:**")
        for page_key, meta in pending.items():
            lines.append(f"`{page_key}` — ⏳ {format_elapsed_short(meta['start_time'])}")
    if recovered:
        lines.append("")
        lines.append("**Recovered:**")
        for page_key, meta in recovered.items():
            lines.append(f"`{page_key}` — ✅")

    await interaction.response.send_message("\n".join(lines))


@bot.tree.command(name="setchannel", description="Set this channel as the recovery notification channel")
@is_owner()
async def setchannel(interaction: discord.Interaction):
    ok = await api_set_config("notify_channel_id", str(interaction.channel_id))
    if ok:
        config = await api_get_config()
        saved_id = config.get("notify_channel_id")
        if str(saved_id) == str(interaction.channel_id):
            await interaction.response.send_message(
                f"✅ Recovery notifications will be posted in {interaction.channel.mention}."
            )
        else:
            await interaction.response.send_message(
                f"⚠️ Save request succeeded but the database shows a different value "
                f"(`{saved_id}`) than expected (`{interaction.channel_id}`)."
            )
    else:
        await interaction.response.send_message(
            "❌ Failed to save the channel — the database write did not succeed. "
            "Check Render logs and verify D1_WORKER_URL/D1_API_KEY.",
            ephemeral=True,
        )


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("DISCORD_TOKEN not set.")
    if not D1_WORKER_URL or not D1_API_KEY:
        raise SystemExit("D1_WORKER_URL and D1_API_KEY must be set. See README.md.")
    start_keep_alive()
    bot.run(DISCORD_TOKEN)