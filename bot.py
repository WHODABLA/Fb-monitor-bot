"""
Facebook Recovery Monitor — Discord Bot
--------------------------------------
Tracks Facebook Pages/Profiles you flag as "banned/restricted" and posts a
Discord notification the moment they become reachable again.

- Only the server owner can use any command.
- Storage: Cloudflare Worker + D1 database (survives restarts).
- Checking: Apify's Facebook Pages Scraper.

Commands (slash commands, server owner only):
  /track pageurl          - start tracking a Facebook Page/Profile URL for recovery
  /untrack pageurl        - stop tracking one
  /list                   - show everything currently being tracked
  /setchannel             - set the channel this bot posts recovery alerts to
  /checknow pageurl       - debug: immediately check one page/profile
  /ban pageurl            - track a page for ban (send notification when banned)
  /unban pageurl          - stop tracking ban for a page

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
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "")
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


# ---------- keep-alive web server ----------

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


# ---------- D1 storage ----------

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
                    "recovered": bool(row.get("recovered", False)),
                    "recovered_at": row.get("recovered_at"),
                    "banned": bool(row.get("banned", False)),
                    "banned_at": row.get("banned_at"),
                    "track_type": row.get("track_type", "recovery"),
                }
                for row in rows
            }


async def api_add_tracked(page_key: str, start_time: str, track_type: str = "recovery"):
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{D1_WORKER_URL}/tracked",
            headers=_d1_headers(),
            json={
                "username": page_key, 
                "start_time": start_time,
                "track_type": track_type,
                "banned": False,
                "recovered": False
            },
        )


async def api_mark_recovered(page_key: str, recovered_at: str):
    async with aiohttp.ClientSession() as session:
        await session.patch(
            f"{D1_WORKER_URL}/tracked/{page_key}",
            headers=_d1_headers(),
            json={"recovered_at": recovered_at, "recovered": True},
        )


async def api_mark_banned(page_key: str, banned_at: str):
    async with aiohttp.ClientSession() as session:
        await session.patch(
            f"{D1_WORKER_URL}/tracked/{page_key}",
            headers=_d1_headers(),
            json={"banned_at": banned_at, "banned": True},
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


# ---------- Facebook status check ----------

def _normalize_facebook_url(page_key: str) -> str:
    page_key = page_key.strip()
    if page_key.startswith("http://") or page_key.startswith("https://"):
        return page_key
    page_key = page_key.lstrip("@")
    return f"https://www.facebook.com/{page_key}"


def _extract_page_name_from_url(url: str) -> str:
    """Extract the page name/ID from a Facebook URL."""
    url = url.replace("https://", "").replace("http://", "")
    url = url.replace("www.facebook.com/", "").replace("facebook.com/", "")
    url = url.split("?")[0]
    url = url.rstrip("/")
    
    if url.startswith("share/"):
        return url.split("/")[-1] if "/" in url else url
    if "profile.php" in url:
        match = re.search(r'id=(\d+)', url)
        if match:
            return match.group(1)
    if url and not url.startswith("profile.php") and not url.startswith("share/"):
        return url.split("/")[0] if "/" in url else url
    return url


def _get_profile_pic_url(page_key: str) -> str:
    """Get Facebook profile picture URL using Open Graph."""
    # Clean up the page key
    page_key = page_key.strip().lstrip("@")
    # If it's a full URL, extract the page name/ID
    if page_key.startswith("http://") or page_key.startswith("https://"):
        # Try to extract from URL
        page_key = page_key.replace("https://", "").replace("http://", "")
        page_key = page_key.replace("www.facebook.com/", "").replace("facebook.com/", "")
        
        # If it's a share URL, extract the ID
        if "share/" in page_key:
            page_key = page_key.split("share/")[-1].split("?")[0].rstrip("/")
        # If it's a profile.php URL, extract the ID
        elif "profile.php" in page_key:
            match = re.search(r'id=(\d+)', page_key)
            if match:
                page_key = match.group(1)
        # Otherwise, just use the first part
        else:
            page_key = page_key.split("/")[0].split("?")[0]
    
    # If page_key is still a URL, try one more time
    if page_key.startswith("http://") or page_key.startswith("https://"):
        # Last resort: try to get from URL
        page_key = page_key.split("/")[-1].split("?")[0].rstrip("/")
    
    # Only use if we have a valid ID
    if page_key and page_key != "":
        return f"https://graph.facebook.com/{page_key}/picture?type=large"
    return None


def _format_number(num: int) -> str:
    """Format large numbers with K, M, B suffixes."""
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    else:
        return str(num)


async def check_facebook_status(page_key: str):
    """Returns None if unreachable. Returns dict with page stats if reachable."""
    url = _normalize_facebook_url(page_key)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                APIFY_URL,
                headers={"Content-Type": "application/json"},
                json={"startUrls": [{"url": url}]},
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
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
                
                # Parse followers from the info field
                followers = 0
                info_text = ""
                if item.get("info") and isinstance(item["info"], list):
                    info_text = " ".join(item["info"])
                    for text in item["info"]:
                        if "likes" in text.lower():
                            match = re.search(r'([\d,]+)\s+likes', text)
                            if match:
                                followers = int(match.group(1).replace(',', ''))
                                break
                
                # Try to extract talking about count (engagement)
                talking_about = 0
                if info_text:
                    match = re.search(r'([\d,]+)\s+talking about this', info_text)
                    if match:
                        talking_about = int(match.group(1).replace(',', ''))
                
                # Get category
                category = ""
                if item.get("categories") and isinstance(item["categories"], list) and len(item["categories"]) > 0:
                    category = item["categories"][0]
                
                if not item.get("facebookUrl") and not item.get("title"):
                    print(f"[apify-fb] {page_key}: no usable data — {item}", flush=True)
                    return None
                
                title = item.get("title") or page_key
                
                return {
                    "page_key": page_key,
                    "title": title,
                    "display_name": title,
                    "followers": followers,
                    "followers_formatted": _format_number(followers),
                    "talking_about": talking_about,
                    "category": category,
                    "profile_pic_url": _get_profile_pic_url(page_key),
                    "url": item.get("facebookUrl") or url,
                }
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as e:
        print(f"[apify-fb] {page_key}: {type(e).__name__}: {e}", flush=True)
        return None


# ---------- timer formatting ----------

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


# ---------- Discord Embeds ----------

def get_display_name(page_key: str) -> str:
    """Get a clean display name from a page key."""
    if page_key.startswith("http://") or page_key.startswith("https://"):
        return _extract_page_name_from_url(page_key)
    return page_key


def build_recovery_embed(info: dict, start_iso: str) -> discord.Embed:
    """Creates an embed with profile picture thumbnail for recovery notifications."""
    
    followers_str = info.get('followers_formatted', f"{info['followers']:,}")
    page_name = info.get('display_name') or info['title']
    
    embed = discord.Embed(
        title=f"✅ Account Recovered | {page_name}",
        url=info['url'],
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc)
    )
    
    # Create description like Instagram monitor style
    description = f"**👥 Followers:** {info['followers']:,}"
    
    if info.get('talking_about', 0) > 0:
        description += f"\n**💬 Talking About:** {info['talking_about']:,}"
    
    if info.get('category'):
        description += f"\n**📂 Category:** {info['category']}"
    
    description += f"\n⏱️ *Time taken: {format_elapsed_long(start_iso)}*\n\n"
    
    # Page stats repeated (like Instagram monitor)
    description += f"**{page_name}**\n"
    description += f"👥 {info['followers']:,} followers"
    
    if info.get('talking_about', 0) > 0:
        description += f"\n💬 {info['talking_about']:,} talking about this"
    
    if info.get('category'):
        description += f"\n📂 {info['category']}"
    
    embed.description = description
    
    if info.get('profile_pic_url'):
        embed.set_thumbnail(url=info['profile_pic_url'])
    
    embed.set_footer(
        text=f"Facebook Recovery Monitor",
        icon_url="https://cdn.jsdelivr.net/npm/simple-icons@v9/icons/facebook.svg"
    )
    
    return embed


def build_ban_embed(page_key: str) -> discord.Embed:
    """Creates an embed with profile picture for ban notifications."""
    
    display_name = get_display_name(page_key)
    profile_pic = _get_profile_pic_url(page_key)  # Get profile picture
    page_url = _normalize_facebook_url(page_key)
    
    embed = discord.Embed(
        title=f"🚫 Account Banned | {display_name}",
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc)
    )
    
    description = f"**{display_name}** has been banned or restricted on Facebook!\n\n"
    description += f"📌 The page is no longer accessible.\n"
    description += f"🔗 [{page_url}]({page_url})"
    
    embed.description = description
    
    # Add profile picture thumbnail if available
    if profile_pic:
        embed.set_thumbnail(url=profile_pic)
    
    embed.set_footer(
        text=f"Facebook Ban Monitor",
        icon_url="https://cdn.jsdelivr.net/npm/simple-icons@v9/icons/facebook.svg"
    )
    
    return embed


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
            track_type = meta.get("track_type", "recovery")
            
            info = await check_facebook_status(page_key)
            
            if track_type == "ban":
                if info is None and not meta.get("banned", False):
                    embed = build_ban_embed(page_key)
                    await channel.send(embed=embed)
                    await api_mark_banned(page_key, datetime.now(timezone.utc).isoformat())
                    print(f"🚫 Banned: {page_key}", flush=True)
            
            elif track_type == "recovery":
                if info is not None and not meta.get("recovered", False):
                    embed = build_recovery_embed(info, meta["start_time"])
                    await channel.send(embed=embed)
                    await api_mark_recovered(page_key, datetime.now(timezone.utc).isoformat())
                    print(f"✅ Recovered: {page_key}", flush=True)
            
    except Exception as e:
        print(f"check_tracked_pages: UNHANDLED ERROR (loop continues): {type(e).__name__}: {e}", flush=True)


@check_tracked_pages.before_loop
async def before_check():
    await bot.wait_until_ready()


# ---------- slash commands ----------

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


# ---------- Recovery Tracking Commands ----------

@bot.tree.command(name="track", description="Start tracking a Facebook Page/Profile for recovery")
@app_commands.describe(pageurl="Facebook page name, @handle, or full URL")
@is_owner()
async def track(interaction: discord.Interaction, pageurl: str):
    page_key = pageurl.strip()

    if " " in page_key or page_key.count("http") > 1:
        await interaction.response.send_message(
            f"❌ `{page_key}` doesn't look like a single valid page URL/name "
            "(no spaces, one link only). Check for accidental pastes.",
            ephemeral=True,
        )
        return

    tracked = await api_get_tracked()
    if page_key in tracked and not tracked[page_key].get("recovered", False):
        await interaction.response.send_message(f"⚠️ Already tracking `{page_key}`.", ephemeral=True)
        return
    await api_add_tracked(page_key, datetime.now(timezone.utc).isoformat(), "recovery")
    
    display_name = get_display_name(page_key)
    embed = discord.Embed(
        title="⏱️ Recovery Tracking Started",
        description=f"Now tracking **{display_name}** for recovery\n\n📌 Will notify here when it's back online",
        color=discord.Color.blue()
    )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="untrack", description="Stop tracking a Facebook Page/Profile")
@app_commands.describe(pageurl="Facebook page name, @handle, or full URL")
@is_owner()
async def untrack(interaction: discord.Interaction, pageurl: str):
    page_key = pageurl.strip()
    tracked = await api_get_tracked()
    if page_key in tracked:
        await api_remove_tracked(page_key)
        embed = discord.Embed(
            title="✅ Stopped Tracking",
            description=f"No longer tracking `{page_key}`",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
    else:
        await interaction.response.send_message(f"❌ `{page_key}` isn't being tracked.", ephemeral=True)


# ---------- Ban Tracking Commands ----------

@bot.tree.command(name="ban", description="Start tracking a Facebook Page/Profile for bans")
@app_commands.describe(pageurl="Facebook page name, @handle, or full URL")
@is_owner()
async def ban(interaction: discord.Interaction, pageurl: str):
    page_key = pageurl.strip()

    if " " in page_key or page_key.count("http") > 1:
        await interaction.response.send_message(
            f"❌ `{page_key}` doesn't look like a single valid page URL/name "
            "(no spaces, one link only). Check for accidental pastes.",
            ephemeral=True,
        )
        return

    tracked = await api_get_tracked()
    if page_key in tracked:
        await interaction.response.send_message(f"⚠️ `{page_key}` is already being tracked.", ephemeral=True)
        return
    
    await api_add_tracked(page_key, datetime.now(timezone.utc).isoformat(), "ban")
    
    display_name = get_display_name(page_key)
    embed = discord.Embed(
        title="🚫 Ban Tracking Started",
        description=f"Now monitoring **{display_name}** for bans\n\n📌 Will notify here if it gets banned",
        color=discord.Color.red()
    )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="unban", description="Stop tracking a Facebook Page/Profile for bans")
@app_commands.describe(pageurl="Facebook page name, @handle, or full URL")
@is_owner()
async def unban(interaction: discord.Interaction, pageurl: str):
    page_key = pageurl.strip()
    tracked = await api_get_tracked()
    if page_key in tracked:
        await api_remove_tracked(page_key)
        embed = discord.Embed(
            title="✅ Stopped Ban Tracking",
            description=f"No longer monitoring `{page_key}` for bans",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
    else:
        await interaction.response.send_message(f"❌ `{page_key}` isn't being tracked.", ephemeral=True)


# ---------- List Command ----------

@bot.tree.command(name="list", description="List all tracked Facebook Pages/Profiles")
@is_owner()
async def list_tracked(interaction: discord.Interaction):
    tracked = await api_get_tracked()
    if not tracked:
        await interaction.response.send_message("📭 Nothing is being tracked right now.")
        return

    recovery_pending = {k: m for k, m in tracked.items() if m.get("track_type") == "recovery" and not m.get("recovered", False)}
    recovery_done = {k: m for k, m in tracked.items() if m.get("track_type") == "recovery" and m.get("recovered", False)}
    ban_tracking = {k: m for k, m in tracked.items() if m.get("track_type") == "ban" and not m.get("banned", False)}
    ban_done = {k: m for k, m in tracked.items() if m.get("track_type") == "ban" and m.get("banned", False)}

    embed = discord.Embed(
        title="📊 Tracked Pages",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc)
    )
    
    summary = f"🔄 Recovery Tracking: {len(recovery_pending)} active, {len(recovery_done)} recovered\n"
    summary += f"🚫 Ban Monitoring: {len(ban_tracking)} active, {len(ban_done)} banned"
    embed.add_field(name="📈 Summary", value=summary, inline=False)
    
    if recovery_pending:
        pending_list = []
        for page_key, meta in recovery_pending.items():
            display_name = get_display_name(page_key)
            pending_list.append(f"`{display_name}` — ⏳ {format_elapsed_short(meta['start_time'])}")
        embed.add_field(
            name="🔄 Recovery Tracking (Active)",
            value="\n".join(pending_list) if pending_list else "None",
            inline=False
        )
    
    if recovery_done:
        done_list = []
        for page_key in recovery_done.keys():
            display_name = get_display_name(page_key)
            done_list.append(f"`{display_name}` — ✅")
        embed.add_field(
            name="✅ Recovery Tracking (Recovered)",
            value="\n".join(done_list) if done_list else "None",
            inline=False
        )
    
    if ban_tracking:
        ban_list = []
        for page_key, meta in ban_tracking.items():
            display_name = get_display_name(page_key)
            ban_list.append(f"`{display_name}` — 📡 {format_elapsed_short(meta['start_time'])}")
        embed.add_field(
            name="🚫 Ban Monitoring (Active)",
            value="\n".join(ban_list) if ban_list else "None",
            inline=False
        )
    
    if ban_done:
        banned_list = []
        for page_key in ban_done.keys():
            display_name = get_display_name(page_key)
            banned_list.append(f"`{display_name}` — 🚫")
        embed.add_field(
            name="🚫 Ban Monitoring (Banned)",
            value="\n".join(banned_list) if banned_list else "None",
            inline=False
        )

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="setchannel", description="Set this channel as the notification channel")
@is_owner()
async def setchannel(interaction: discord.Interaction):
    ok = await api_set_config("notify_channel_id", str(interaction.channel_id))
    if ok:
        config = await api_get_config()
        saved_id = config.get("notify_channel_id")
        if str(saved_id) == str(interaction.channel_id):
            embed = discord.Embed(
                title="✅ Channel Set",
                description=f"Notifications will be posted in {interaction.channel.mention}",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
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


@bot.tree.command(name="checknow", description="Debug: immediately check a Facebook Page/Profile")
@app_commands.describe(pageurl="Facebook page name, @handle, or full URL")
@is_owner()
async def checknow(interaction: discord.Interaction, pageurl: str):
    await interaction.response.defer(thinking=True)
    info = await check_facebook_status(pageurl)
    if info:
        display_name = info.get('display_name') or info['title']
        embed = discord.Embed(
            title=f"✅ {display_name} is LIVE!",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        description = f"**👥 Followers:** {info['followers']:,}"
        if info.get('talking_about', 0) > 0:
            description += f"\n**💬 Talking About:** {info['talking_about']:,}"
        if info.get('category'):
            description += f"\n**📂 Category:** {info['category']}"
        description += f"\n🔗 [{info['url']}]({info['url']})"
        embed.description = description
        if info.get('profile_pic_url'):
            embed.set_thumbnail(url=info['profile_pic_url'])
        await interaction.followup.send(embed=embed)
    else:
        display_name = get_display_name(pageurl)
        embed = discord.Embed(
            title=f"🚫 {display_name} is BANNED or UNREACHABLE",
            description="The page is not accessible (banned, restricted, or doesn't exist).",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("DISCORD_TOKEN not set.")
    if not D1_WORKER_URL or not D1_API_KEY:
        raise SystemExit("D1_WORKER_URL and D1_API_KEY must be set. See README.md.")
    start_keep_alive()
    bot.run(DISCORD_TOKEN)