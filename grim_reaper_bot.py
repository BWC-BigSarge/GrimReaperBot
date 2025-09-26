# grim_reaper_bot.py

import os
import sqlite3
import logging
import asyncio
from datetime import datetime
from collections import defaultdict

import discord
from discord.ext import commands
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from flask import Flask, request, jsonify
import threading
import secrets

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("GrimReaperBot")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "your_discord_token_here")

# Replace with your actual channel IDs
CHANNEL_SC_PUBLIC = 123456789012345678
CHANNEL_SC_ANNOUNCEMENTS = 987654321098765432

# Role allowed to trigger admin commands
ADMIN_ROLE_NAME = os.getenv("ADMIN_ROLE_NAME", "Admin")

# SQLite database (hosted on SparkedHost or local dev)
DB_PATH = "grimreaperbot.db"

# Shared secret for API requests from the BWC website
API_SHARED_SECRET = os.getenv("API_SHARED_SECRET", "super-secret")

# ---------------------------------------------------------------------------
# Database Setup
# ---------------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS api_keys (
            discord_id TEXT PRIMARY KEY,
            api_key TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    conn.commit()
    conn.close()

init_db()

# ---------------------------------------------------------------------------
# Discord Bot
# ---------------------------------------------------------------------------
intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Track kills in memory
kill_history = defaultdict(list)  # {player: [timestamps]}
player_kills = defaultdict(int)   # {player: total kills}

# ---------------------------------------------------------------------------
# APScheduler Setup
# ---------------------------------------------------------------------------
jobstores = {
    "default": SQLAlchemyJobStore(url=f"sqlite:///{DB_PATH}")
}
scheduler = AsyncIOScheduler(jobstores=jobstores)


async def weekly_tally():
    """Generate and post the weekly tally."""
    logger.info("Generating weekly tally...")

    if not player_kills:
        logger.info("No kills recorded yet. Skipping tally.")
        return

    sorted_kills = sorted(player_kills.items(), key=lambda x: x[1], reverse=True)
    lines = [f"**Weekly Kill Tally** ({datetime.utcnow().strftime('%Y-%m-%d')}):"]
    for player, count in sorted_kills:
        lines.append(f"- {player}: {count} kills")

    channel = bot.get_channel(CHANNEL_SC_ANNOUNCEMENTS)
    if channel:
        await channel.send("\n".join(lines))
    else:
        logger.warning("Announcements channel not found!")


# Add job if it doesn’t exist
if not scheduler.get_job("weekly_tally"):
    scheduler.add_job(
        weekly_tally,
        "interval",
        hours=168,  # 7 days
        id="weekly_tally",
        replace_existing=True,
    )

# ---------------------------------------------------------------------------
# Bot Events
# ---------------------------------------------------------------------------
@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    if not scheduler.running:
        scheduler.start()

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
@bot.command()
async def kill(ctx, player: str):
    """Simulate recording a PvP kill (testing only)."""
    now = datetime.utcnow().timestamp()
    kill_history[player].append(now)
    player_kills[player] += 1

    channel = bot.get_channel(CHANNEL_SC_PUBLIC)
    if channel:
        await channel.send(f"☠️ **{player}** scored a kill!")

    # Kill streaks
    recent = [t for t in kill_history[player] if now - t <= 120]
    if len(recent) >= 5:
        await channel.send(f"🔥 {player} is on a **5-kill streak in 120s!**")
    elif len([t for t in kill_history[player] if now - t <= 60]) >= 3:
        await channel.send(f"⚡ {player} is on a **3-kill streak in 60s!**")

    # Milestones
    if player_kills[player] % 100 == 0:
        await channel.send(f"🏆 {player} reached **{player_kills[player]} kills!**")


@bot.command(name="weeklytally")
@commands.has_role(ADMIN_ROLE_NAME)
async def manual_weekly_tally(ctx):
    """Manually trigger the weekly tally (Admin only)."""
    await weekly_tally()
    await ctx.send("✅ Weekly tally triggered manually.")


@manual_weekly_tally.error
async def weeklytally_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have permission to run this command.")

# ---------------------------------------------------------------------------
# API Server for Website Requests
# ---------------------------------------------------------------------------
app = Flask("GrimReaperBotAPI")

@app.route("/get_api_key", methods=["POST"])
def get_api_key():
    data = request.json
    if not data or data.get("secret") != API_SHARED_SECRET:
        return jsonify({"error": "Unauthorized"}), 403

    discord_id = data.get("discord_id")
    if not discord_id:
        return jsonify({"error": "Missing discord_id"}), 400

    api_key = secrets.token_hex(16)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO api_keys (discord_id, api_key) VALUES (?, ?)",
        (discord_id, api_key),
    )
    conn.commit()
    conn.close()

    logger.info(f"Issued API key for {discord_id}")
    return jsonify({"api_key": api_key})


def run_api():
    app.run(host="0.0.0.0", port=5000)


api_thread = threading.Thread(target=run_api, daemon=True)
api_thread.start()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
