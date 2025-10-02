# main.py - Discord bot for tracking kills/deaths in Star Citizen for BWC members
import os
from time import sleep
from dotenv import load_dotenv
import discord
from discord.ext import commands
import mysql.connector
import logging
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict

#from apscheduler.schedulers.asyncio import AsyncIOScheduler
#from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from flask import Flask, request, jsonify
import threading
import secrets

import data_map # Human readable mappings for various log entries

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
description = """
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                       BWC-KillTracker for Star Citizen
     (Tracks kills/deaths and other metrics that occur for members of BWC)
     
     Vibe coded by: BWC-Firely
         (https://robertsspaceindustries.com/en/citizens/BWC-Firefly)
     Re-Coded by: Game_Overture
         (https://robertsspaceindustries.com/citizens/Game_Overture)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
"""
API_SHARED_SECRET = os.getenv("API_SHARED_SECRET") # Shared secret for API requests from the BWC website
ADMIN_ROLE_NAME = os.getenv("ADMIN_ROLE_NAME", 480372823454384138) # Default is "NBR NCO" id number - this role allows manual_weekly_tally

# NOTE: These channel IDs currently point to my test server (Game_Overture)
CHANNEL_SC_PUBLIC = 1420804944075689994
CHANNEL_SC_ANNOUNCEMENTS = 1421936341486145678
#CHANNEL_SC_PUBLIC = 480367983558918174
#CHANNEL_SC_ANNOUNCEMENTS = 827312889890471957

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("GrimReaperBot")

# ---------------------------------------------------------------------------
# Discord Bot
# ---------------------------------------------------------------------------
intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", description=description, intents=intents)

# Track kills in memory
kill_history = defaultdict(list)  # {player: [timestamps]}
player_kills = defaultdict(int)   # {player: total kills}

# ---------------------------------------------------------------------------
# Bot Events
# ---------------------------------------------------------------------------
@bot.event
async def on_ready():
    # Database Setup
    global conn
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE")
    )
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS api_keys (
        discord_id VARCHAR(24) PRIMARY KEY,
        api_key VARCHAR(42) NOT NULL,
        rsi_handle VARCHAR(42),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS kill_feed (
        id INT AUTO_INCREMENT PRIMARY KEY,
        discord_id VARCHAR(24) NOT NULL,
        rsi_handle VARCHAR(42) NOT NULL,
        victim VARCHAR(42) NOT NULL,
        time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        zone VARCHAR(64),
        weapon VARCHAR(64),
        game_mode VARCHAR(42),
        client_ver VARCHAR(10),
        killers_ship VARCHAR(64)
    )
    """)

    # Logger Setup
    logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")

    #if not scheduler.running:
    #    scheduler.start()

# ---------------------------------------------------------------------------
# APScheduler Setup
# ---------------------------------------------------------------------------
# jobstores = {
#     "default": SQLAlchemyJobStore(url=f"sqlite:///{DB_PATH}")
# }
# scheduler = AsyncIOScheduler(jobstores=jobstores)


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


# # Add job if it doesn’t exist
# if not scheduler.get_job("weekly_tally"):
#     scheduler.add_job(
#         weekly_tally,
#         "interval",
#         hours=168,  # 7 days
#         id="weekly_tally",
#         replace_existing=True,
#     )

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
@bot.command()
async def kill(ctx, player:str):
    """Simulate recording a PvP kill (testing only)."""
    details = {
        'discord_id': ctx.author.id,
        'player': "BWC",
        'victim': player,
        'time': datetime.utcnow().timestamp(),
        'zone': "Zone Name",
        'weapon': "Weapon Name",
        'game_mode': "Test",
        'client_ver': "N/A",
        'killers_ship': "Ship Name",
        'anonymize_state': {'enabled': False }
    }
    process_kill("killer", details, store_in_db=False)

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

def process_kill(result:str, details:object, store_in_db:bool):
    anonymize_state = details.get("anonymize_state")
    discord_id = "N/A"
    player = "BWC" # Default to "BWC" if anonymized
    if(anonymize_state["enabled"]):
        discord_id = details.get("discord_id")
        player = details.get("player")
        logger.info("Anonymized kill reported")
    
    if result == "killer":
        victim = details.get("victim")
        kill_time = details.get("time")
        zone = details.get("zone")
        weapon = details.get("weapon")
        game_mode = details.get("game_mode")
        client_ver = details.get("client_ver")
        killers_ship = details.get("killers_ship")

        # kill_time is formatted something like "<2025-10-02T22:57:03.975Z>" convert it to a datetime object
        kill_time = kill_time.strip("<>")
        kill_time = datetime.strptime(kill_time, "%Y-%m-%dT%H:%M:%S.%fZ")

        # Record kill in memory
        kill_history[player].append(kill_time.timestamp())
        player_kills[player] += 1

        # Record kill in database
        if store_in_db:
            logger.info(f"Recording DB kill: {player} killed {victim} with {weapon} in {zone}")
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO kill_feed (discord_id, rsi_handle, victim, time, zone, weapon, game_mode, client_ver, killers_ship)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (discord_id, player, victim, kill_time, zone, weapon, game_mode, client_ver, killers_ship)
            )
            conn.commit()
        else:
            logger.info(f"Test kill: {player} killed {victim} with {weapon} in {zone}")

        # Announce kill in Discord
        channel = bot.get_channel(CHANNEL_SC_PUBLIC)
        if channel:
            asyncio.run_coroutine_threadsafe(
                channel.send(f"☠️ **{player}** killed **{victim}** using **{weapon}** in **{zone}**."),
                bot.loop
            )
            now = datetime.utcnow().timestamp()
            # Kill streaks
            recent = [t for t in kill_history[player] if now - t <= 120]
            if len(recent) >= 5:
                asyncio.run_coroutine_threadsafe(
                    channel.send(f"🔥 {player} is on a **5-kill streak in 120s!**"),
                    bot.loop
                )
            elif len([t for t in kill_history[player] if now - t <= 60]) >= 3:
                asyncio.run_coroutine_threadsafe(
                    channel.send(f"⚡ {player} is on a **3-kill streak in 60s!**"),
                    bot.loop
                )

            # Milestones
            if player_kills[player] % 10 == 0:
                asyncio.run_coroutine_threadsafe(
                    channel.send(f"🏆 {player} reached **{player_kills[player]} kills!**"),
                    bot.loop
                )

# ---------------------------------------------------------------------------
# API Server for Website Requests
# ---------------------------------------------------------------------------
app = Flask("GrimReaperBotAPI")

@app.route("/get_api_key", methods=["POST"])
def get_api_key():
    # Authenticate request
    data = request.json
    if not data or data.get("secret") != API_SHARED_SECRET:
        return jsonify({"error": "Unauthorized"}), 403

    discord_id = data.get("discord_id")
    if not discord_id:
        return jsonify({"error": "Missing discord_id"}), 400

    # Check if user already has an API key
    cur = conn.cursor()
    cur.execute("SELECT api_key FROM api_keys WHERE discord_id = %s", (discord_id,))
    ret = cur.fetchone()
    api_key = None
    if ret:
        api_key = ret[0]
        print("API key found")
        logger.info(f"Existing API key for {discord_id} retrieved")
    else:
        print("No API key found, generating new one")
        api_key = secrets.token_hex(16)
        print(f"Generated API key: { api_key }")
        cur.execute(
            """
            INSERT INTO api_keys (discord_id, api_key)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE api_key = VALUES(api_key),
                                    created_at = CURRENT_TIMESTAMP
            """,
            (discord_id, api_key),
        )
        conn.commit()
        print("Generated new API key")
        logger.info(f"Generated new API key for {discord_id}")

    return jsonify({"api_key": api_key})

# JSON Payload Example:
# { "api_key": key,
#   "player_name": self.rsi_handle["current"] }
@app.route("/validateKey", methods=["POST"])
def validate_key():
    data = request.json
    if not data:
        return jsonify({"error": "Unauthorized"}), 403

    api_key = data.get("api_key")
    player_name = data.get("player_name")

    # Determine if API key exists, then update rsi_handle if needed
    cur = conn.cursor()
    cur.execute("SELECT discord_id, rsi_handle FROM api_keys WHERE api_key = %s", (api_key,))
    ret = cur.fetchone()
    if not ret:
        return jsonify({"error": "Invalid API key"}), 403
    discord_id, rsi_handle = ret
    if player_name and rsi_handle != player_name:
        cur.execute("UPDATE api_keys SET rsi_handle = %s WHERE api_key = %s", (player_name, api_key))
        conn.commit()

    return jsonify({"success": True, "discord_id": discord_id}), 200

# JSON Payload Example:
# { "api_key": key,
#   "player_name": self.rsi_handle["current"] }
@app.route("/get_expiration", methods=["POST"])
def get_expiration():
    data = request.json
    if not data:
        return jsonify({"error": "Unauthorized"}), 403

    api_key = data.get("api_key")
    player_name = data.get("player_name")

    # Determine if API key exists, then update rsi_handle if needed
    cur = conn.cursor()
    cur.execute("SELECT discord_id, rsi_handle, created_at FROM api_keys WHERE api_key = %s", (api_key,))
    ret = cur.fetchone()
    if not ret:
        return jsonify({"error": "Invalid API key"}), 403
    discord_id, rsi_handle, created_at = ret
    if player_name and rsi_handle != player_name:
        cur.execute("UPDATE api_keys SET rsi_handle = %s WHERE api_key = %s", (player_name, api_key))
        conn.commit()
    expiration_date = created_at + timedelta(days=180)
    # Convert expiration_date to work with datetime.strptime()
    expiration_date = expiration_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return jsonify({"success": True, "expires_at": expiration_date}), 200

# GET requests for various data maps
@app.route("/data_map/weapons", methods=["GET"])
def get_data_map_weapons():
    ret_json = {}
    ret_json["weapons"] = data_map.weaponMapping
    return jsonify(ret_json)

@app.route("/data_map/locations", methods=["GET"])
def get_data_map_locations():
    ret_json = {}
    ret_json["locations"] = data_map.locationMapping
    return jsonify(ret_json)

@app.route("/data_map/vehicles", methods=["GET"])
def get_data_map_vehicles():
    ret_json = {}
    ret_json["vehicles"] = data_map.vehicleMapping
    return jsonify(ret_json)

@app.route("/data_map/ignoredVictimRules", methods=["GET"])
def get_data_map_ignoredVictimRules():
    ret_json = {}
    ret_json["ignoredVictimRules"] = data_map.ignoredVictimRules
    return jsonify(ret_json)

# Sample JSON Payloads:
# Current user killed themselves
#{
#    "result": "suicide",
#    "data": {
#        "discord_id": discord_id,
#        "player": curr_user,
#        "weapon": weapon,
#        "zone": killed_zone
#        "anonymize_state": self.anonymize_state
#    }
#}
#
# Current user died
#{
#    "result": "killed",
#    "data": {
#        "discord_id": discord_id,
#        "player": curr_user,
#        "victim": curr_user,
#        "killer": killer,
#        "weapon": mapped_weapon,
#        "zone": self.active_ship["current"]
#        "anonymize_state": self.anonymize_state
#    }
#}
#
# Current user killed other player
#{
#    "result": "killer",
#    "data": {
#        "discord_id": discord_id,
#        "player": curr_user,
#        "victim": killed,
#        "time": kill_time,
#        "zone": killed_zone,
#        "weapon": weapon,
#        "game_mode": self.game_mode,
#        "client_ver": self.local_version,
#        "killers_ship": self.active_ship["current"],
#        "anonymize_state": self.anonymize_state
#     }
#}
@app.route("/reportKill", methods=["POST"])
def report_kill():
    data = request.json
    result = data.get("result")
    details = data.get("data", {})

    process_kill(result, details, store_in_db=True)
    return jsonify({"success": True}), 200

#@app.route("/reportACKill", methods=["POST"])

def run_api():
    app.run(host="0.0.0.0", port=25219)

api_thread = threading.Thread(target=run_api, daemon=True)
api_thread.start()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    load_dotenv()
    bot.run(os.getenv("DISCORD_TOKEN"))
