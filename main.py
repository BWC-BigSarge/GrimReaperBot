# main.py - Discord bot for tracking kills/deaths in Star Citizen for BWC members
import dis
import os
from time import sleep
from dotenv import load_dotenv
import discord
from discord.ext import commands, tasks
import mysql.connector
from mysql.connector import pooling, errors
import logging
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
from flask import Flask, request, jsonify
from waitress import serve
import threading
import secrets
from fuzzywuzzy import fuzz

import data_map # Human readable mappings for various log entries

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
description = """
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                       BWC-KillTracker for Star Citizen
     (Tracks kills/deaths and other metrics that occur for members of BWC)
     Developed by Game_Overture
         (https://robertsspaceindustries.com/citizens/Game_Overture)
     Original concept BWC-Firefly
         (https://robertsspaceindustries.com/citizens/BWC-Firefly)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
"""
API_SHARED_SECRET = os.getenv("API_SHARED_SECRET") # Shared secret for API requests from the BWC website

ROLE_GrimReaperAdmin = 1429598849173032990
ROLE_BWC = 480372977452580874
ROLE_SC = 480372006806618114
ROLE_TEST_ADMIN = 1427357824597360671

CHANNEL_SC_PUBLIC = 480367983558918174
CHANNEL_SC_ANNOUNCEMENTS = 827312889890471957
CHANNEL_TEST_SERVER_PUBLIC = 1420804944075689994

DISCORD_ID_TEST = "123456789012345678"

# ERROR CODES:
ERRORCODE_Void = 469
ERRORCODE_Expired = 470
ERRORCODE_Revoked = 471
ERRORCODE_Banned = 472

# Status string identifiers for api_keys table
STATUS_Active = "active" # NOTE: 'active' is hardcoded as the default value in the table schema
STATUS_Expired = "expired"
STATUS_Revoked = "revoked"
STATUS_Banned = "banned"

EXPIRATION_DURATION = timedelta(days=180) # API keys expire after 180 days

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
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", description=description, intents=intents)

# Track kills in memory
g_kill_timestamps = defaultdict(list)  # {discord_id: [timestamps]}
g_kill_streaks = defaultdict(int)      # {discord_id: total kills}

# ---------------------------------------------------------------------------
# Database Connection Pool
# ---------------------------------------------------------------------------
cnxpool = None # Pool will be initialized within bot's on_ready() event

def get_connection():
    global cnxpool
    if cnxpool is None:
        raise RuntimeError("Database connection pool not initialized yet")
    try:
        conn = cnxpool.get_connection()
        conn.ping(reconnect=True, attempts=3, delay=5)
        return conn
    except errors.InterfaceError as e:
        logger.error(f"InterfaceError Exception - Database connection pool might be dead, rebuilding it. Error was: {e}")
        init_cnxpool()
        return cnxpool.get_connection()
    except errors.OperationalError as e:
        logger.error(f"OperationalError Exception - Database connection pool might be dead, rebuilding it. Error was: {e}")
        init_cnxpool()
        return cnxpool.get_connection()
    except Exception as e:
        logger.error(f"Exception - Database connection pool might be dead, rebuilding it. Error was: {e}")
        init_cnxpool()
        return cnxpool.get_connection()

def init_cnxpool():
    global cnxpool
    dbconfig = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_DATABASE")
        }
    cnxpool = pooling.MySQLConnectionPool(pool_name="grim_db_pool",
                                          pool_size=8,
                                          **dbconfig)
    logger.info("Database connection pool established")

# ---------------------------------------------------------------------------
# Bot Events
# ---------------------------------------------------------------------------
@tasks.loop(hours=1)
async def hourly_task_check():
    logger.info("TASK LOOP - hourly_task_check() invoked")
    await asyncio.to_thread(run_db_check)

def run_db_check():
    global cnxpool
    if not cnxpool:
        logger.warning("hourly_task_check: Database connection pool not initialized yet")
        return

    logger.info("TASK LOOP - before should_task_run() invoked")
    if should_task_run("weekly_kill_tally", 7):
        post_weekly_tally(CHANNEL_SC_ANNOUNCEMENTS)
    logger.info("TASK LOOP - after should_task_run() invoked")

@bot.event
async def on_ready():
    global cnxpool

    # Database Setup
    try:
        init_cnxpool()
        conn = get_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    discord_id VARCHAR(24) PRIMARY KEY,
                    api_key VARCHAR(42) NOT NULL,
                    rsi_handle VARCHAR(42),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(32) DEFAULT 'active'
                )
                """)
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS kill_feed (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    discord_id VARCHAR(24) NOT NULL,
                    rsi_handle VARCHAR(42) NOT NULL,
                    victim VARCHAR(42) NOT NULL,
                    weapon VARCHAR(64),
                    zone VARCHAR(64),
                    current_ship VARCHAR(64),
                    game_mode VARCHAR(42),
                    time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    client_ver VARCHAR(10)
                )
                """)
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS scheduled_tasks (
                    task_name VARCHAR(64) PRIMARY KEY,
                    last_run DATETIME NOT NULL
                )
                """)
            except mysql.connector.Error as err:
                logger.error(f"Fatal Error creating tables: {err}")
                return
            finally:
                cursor.close()
        except mysql.connector.Error as err:
            logger.error(f"Fatal Error: initializing database: {err}")
            return
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Fatal Error: initializing database: {e}")
        return

    # Logger Setup
    logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")

    if not hourly_task_check.is_running():
        hourly_task_check.start()

# ---------------------------------------------------------------------------
# Bot Commands
# ---------------------------------------------------------------------------
@bot.command(name="grimreaper_totalkills")
@commands.has_role(ROLE_BWC)
@commands.has_role(ROLE_SC)
async def cmd_total_kills(ctx, discord_id:str=""):
    if discord_id == "":
        discord_id = str(ctx.author.id)
    kill_buckets = db_get_kill_buckets("WHERE discord_id = %s", (discord_id,))
    if kill_buckets == {}:
        await ctx.send("❌ Unable to retrieve your kill count.")
    else:
        pu_fps_kills = len(kill_buckets['PU_FPS'])
        pu_ship_kills = len(kill_buckets['PU_Ship'])
        ac_fps_kills = len(kill_buckets['AC_FPS'])
        ac_ship_kills = len(kill_buckets['AC_Ship'])
        total_kills = pu_fps_kills + pu_ship_kills + ac_fps_kills + ac_ship_kills
        bwc_name = get_bwc_name(discord_id, False)
        message = f"✅ {bwc_name} has a total of {total_kills} recorded kills:\n"
        message += f"> PU FPS Kills: `{pu_fps_kills}`\n"
        message += f"> PU Ship Kills: `{pu_ship_kills}`\n"
        message += f"> AC FPS Kills: `{ac_fps_kills}`\n"
        message += f"> AC Ship Kills: `{ac_ship_kills}`"
        await ctx.send(message)

@cmd_total_kills.error
async def cmd_total_kills_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have permission to run this command.")
    else:
        await ctx.send("❌ An error occurred while processing your request.")

# ---------------------------------------------------------------------------

@bot.command(name="grimreaper_weeklytally")
@commands.has_role(ROLE_GrimReaperAdmin)
async def cmd_weekly_tally(ctx):
    """Manually trigger the weekly tally (Admin only)."""
    await post_weekly_tally(CHANNEL_SC_ANNOUNCEMENTS)
    await ctx.send("✅ Weekly tally triggered manually.")

@cmd_weekly_tally.error
async def cmd_weekly_tally_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have permission to run this command.")

# ---------------------------------------------------------------------------

@bot.command(name="grimreaper_ban")
@commands.has_role(ROLE_GrimReaperAdmin)
async def cmd_ban_user(ctx, discord_id: str):
    """Ban a user from using the API (Admin only)."""
    set_api_status(ctx, discord_id, STATUS_Banned)

@cmd_ban_user.error
async def cmd_ban_user_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have permission to run this command.")

# ---------------------------------------------------------------------------

@bot.command(name="grimreaper_activate")
@commands.has_role(ROLE_GrimReaperAdmin)
async def cmd_activate_user(ctx, discord_id: str):
    """Unban/Activate a user from using the API (Admin only)."""
    set_api_status(ctx, discord_id, STATUS_Active)

@cmd_activate_user.error
async def cmd_activate_user_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have permission to run this command.")

# ---------------------------------------------------------------------------

@bot.command(name="grimreaper_revoke")
@commands.has_role(ROLE_GrimReaperAdmin)
async def cmd_revoke_key(ctx, discord_id: str):
    """Revoke a user's API key (Admin only)."""
    set_api_status(ctx, discord_id, STATUS_Revoked)
    
@cmd_revoke_key.error
async def cmd_revoke_key_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have permission to run this command.")

# ---------------------------------------------------------------------------

@bot.command(name="grimreaper_testkill")
@commands.has_role(ROLE_TEST_ADMIN)
async def cmd_test_kill(ctx, victim:str=""):
    """Simulate recording a PvP kill (testing only)."""
    if victim == "":
        victim = "Test_Victim"
    details = {
        'discord_id': DISCORD_ID_TEST,
        'player': "Test_RSI_Name",
        'victim': victim,
        'time': "<2025-10-02T22:57:03.975Z>",
        'zone': "Test_Zone",
        'weapon': "Test_Weapon",
        'game_mode': "SC_Default",
        'current_ship': "Test_Ship",
        'client_ver': "N/A",
        'anonymize_state': {'enabled': False }
    }
    process_kill("killer", details, store_in_db=False)

@cmd_test_kill.error
async def cmd_test_kill_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have permission to run this command.")

# ---------------------------------------------------------------------------

@bot.command(name="grimreaper_testtally")
@commands.has_role(ROLE_TEST_ADMIN)
async def cmd_test_tally(ctx):
    """Test weekly tally embed"""
    await post_weekly_tally(CHANNEL_TEST_SERVER_PUBLIC)

@cmd_test_tally.error
async def cmd_test_tally_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have permission to run this command.")

# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------
def set_api_status(ctx, discord_id:str, new_status:str):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE api_keys SET status = %s WHERE discord_id = %s", (new_status, discord_id))
        if cursor.rowcount == 0:
            asyncio.run_coroutine_threadsafe(
                ctx.send(f"❌ No API key found for Discord ID {discord_id}."),
                bot.loop
                )
            logger.warning(f"ban_user: No API key found for Discord ID {discord_id}.")
        else:
            conn.commit()
            if new_status == STATUS_Active:
                asyncio.run_coroutine_threadsafe(
                    ctx.send(f"✅ Discord ID <@{discord_id}> has been activated and can now use the Kill Tracker."),
                    bot.loop
                    )
            elif new_status == STATUS_Banned:
                asyncio.run_coroutine_threadsafe(
                    ctx.send(f"✅ Discord ID <@{discord_id}> has been banned from using the Kill Tracker."),
                    bot.loop
                    )
            elif new_status == STATUS_Revoked:
                asyncio.run_coroutine_threadsafe(
                    ctx.send(f"✅ Kill Tracker API key for Discord ID <@{discord_id}> has been revoked."),
                    bot.loop
                    )
            else:
                asyncio.run_coroutine_threadsafe(
                    ctx.send(f"✅ API key status for Discord ID <@{discord_id}> set to '{new_status}'."),
                    bot.loop
                    )
    except mysql.connector.Error as err:
        logger.error(f"Database error in ban_user: {err}")
        asyncio.run_coroutine_threadsafe(
            ctx.send(f"❌ Database error occurred while '{new_status}' the user."),
            bot.loop
            )
    except Exception as e:
        logger.error(f"Unexpected error in ban_user: {e}")
        asyncio.run_coroutine_threadsafe(
            ctx.send(f"❌ An unexpected error occurred while '{new_status}' the user."),
            bot.loop
            )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_bwc_name(discord_id:str, ping_user=False, fallback_name="Unknown") -> str:
    """ Reassign bwc_name to how their name is formatted in Discord. Using their discord_id """
    if discord_id == DISCORD_ID_TEST:
        return fallback_name

    if ping_user:
        bwc_name = f"<@{discord_id}>"
        return bwc_name

    bwc_name = None
    # First look through all guilds the bot is in to find the member
    for guild in bot.guilds:
        for member in guild.members:
            if str(member.id) == discord_id:
                logger.info(f"Found Discord member in guild {guild.name}")
                bwc_name = member.display_name
                break
    # If we can't find them in any guild, try fetching the user directly
    if bwc_name == None:
        discord_id_as_int = int(discord_id) if discord_id.isdigit() else None
        if discord_id_as_int:
            member = bot.get_user(discord_id_as_int)
            if member:
                logger.info(f"Found cached Discord member: {member}")
                bwc_name = member.display_name
            else:
                try:
                    future = asyncio.run_coroutine_threadsafe(bot.fetch_user(discord_id_as_int), bot.loop)
                    member = future.result(timeout=3)
                    logger.info(f"Fetched Discord member: {member}")
                    if member:
                        bwc_name = member.display_name
                except Exception as e:
                    logger.error(f"Error fetching Discord member: {e}")
    if bwc_name == None:
        logger.warning(f"Could not find Discord member for ID: {discord_id}, using fallback name: {fallback_name}")
        bwc_name = fallback_name
    if bwc_name == "Unknown":
        logger.info(f"Final Attempt to fetch RSI handle for Discord ID: {discord_id} from database")
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT rsi_handle FROM api_keys WHERE discord_id = %s", (discord_id,))
            ret = cursor.fetchone()
            if ret and ret[0]:
                bwc_name = ret[0]
        except mysql.connector.Error as err:
            logger.error(f"Database error in get_bwc_name fetching rsi_handle: {err}")
        except Exception as e:
            logger.error(f"Unexpected error in get_bwc_name fetching rsi_handle: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    return bwc_name

# NOTE: This is a synomous function used in BlackWidowCompanyKilltracker (LogParser class) - Changes or enhancements should be mirrored to it
def convert_string(data_map, src_string:str, fuzzy_search=bool) -> str:
    """Get the best human readable string from the established data maps"""
    try:
        if fuzzy_search:
            fuzzy_found_dict = {}
            for key, value in data_map.items():
                pts = fuzz.ratio(key, src_string)
                if pts >= 90:
                    fuzzy_found_dict[value] = pts
        
            if len(fuzzy_found_dict) > 0:
                # Sort the fuzzy matches by their score and return the best match
                sorted_fuzzy = dict(sorted(fuzzy_found_dict.items(), key=lambda item: item[1], reverse=True))
                return list(sorted_fuzzy.keys())[0]
        else:
            best_key_match = ""
            for key in data_map.keys():
                # if src_string contains key
                if src_string.startswith(key):
                    if len(key) > len(best_key_match):
                        best_key_match = key
            if best_key_match:
                return data_map[best_key_match]
    except Exception as e:
        print(f"Error in convert_string: {e}")
    return src_string

def should_task_run(task_name:str, interval_days:int = 7):
    """Return True if database scheduled task should run now."""
    try:
        now = datetime.utcnow()

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT last_run FROM scheduled_tasks WHERE task_name = %s", (task_name,))
        row = cursor.fetchone()
        if not row:
            logger.info(f"Scheduling first run of task: {task_name}")
            cursor.execute("INSERT INTO scheduled_tasks (task_name, last_run) VALUES (%s, %s)", (task_name, now))
            conn.commit()
            return True  # First run ever

        last_run = row[0]
        delta = now - last_run
        if delta.days >= interval_days:
            logger.info(f"Scheduling task: {task_name} to run now (last run was {delta.days} days ago)")
            cursor.execute("UPDATE scheduled_tasks SET last_run = %s WHERE task_name = %s", (now, task_name))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error in should_task_run for {task_name}: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    logger.info("TASK LOOP - should_task_run() returning False")
    return False

def db_get_kill_buckets(sql_where_query:str="", sql_where_params:tuple=()) -> dict:
    try:
        conn = get_connection()
        cursor = conn.cursor()

        sql_query = """
            SELECT discord_id, victim, weapon, current_ship, zone, game_mode
            FROM kill_feed 
            """ + sql_where_query

        cursor.execute(sql_query, sql_where_params)
        kill_list = cursor.fetchall() # List of tuples (discord_id, victim, weapon, current_ship, zone, game_mode)

        # Sort into 4 buckets based on PU vs AC and weapon type (FPS vs Ship)
        kill_buckets = {
            'PU_FPS': [],
            'PU_Ship': [],
            'AC_FPS': [],
            'AC_Ship': []
        }
        for discord_id, victim, weapon, current_ship, zone, game_mode in kill_list:
            is_ac = game_mode != 'SC_Default'
            is_ship = False
            if weapon.split('_')[0].isupper(): # If the first part of weapon up to the first underscore is in all capital letters, consider it a ship weapon
                is_ship = True

            if is_ac and is_ship:
                kill_buckets['AC_Ship'].append((discord_id, victim, weapon, current_ship, zone, game_mode))
            elif is_ac and not is_ship:
                kill_buckets['AC_FPS'].append((discord_id, victim, weapon, current_ship, zone, game_mode))
            elif not is_ac and is_ship:
                kill_buckets['PU_Ship'].append((discord_id, victim, weapon, current_ship, zone, game_mode))
            else:
                kill_buckets['PU_FPS'].append((discord_id, victim, weapon, current_ship, zone, game_mode))
        return kill_buckets
    except mysql.connector.Error as err:
        logger.error(f"Database error in db_get_kill_buckets: {err}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error in db_get_kill_buckets: {e}")
        return {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ---------------------------------------------------------------------------
# API Server for Website Requests
# ---------------------------------------------------------------------------
app = Flask("GrimReaperBotAPI")

@app.route("/get_api_key", methods=["POST"])
def get_api_key():
    logger.info("entering get_api_key")
    discord_id = None
    try:
        # Authenticate request
        data = request.json
        if not data or data.get("secret") != API_SHARED_SECRET:
            return jsonify({"error": "Unauthorized"}), 401
        discord_id = data.get("discord_id")
        if not discord_id:
            return jsonify({"error": "Missing discord_id"}), ERRORCODE_Void
    except Exception as e:
        logger.error(f"Unexpected error in get_api_key: {e}")
        return jsonify({"error": "Internal server error"}), 500

    logger.info(f"get_api_key called for discord_id: {discord_id}")

    try:
        api_key = None
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT api_key, created_at, status FROM api_keys WHERE discord_id = %s", (discord_id,))
        ret = cursor.fetchone()
        api_key_found = False
        if ret: # Check if user already has an API key
            api_key, created_at, status = ret
            if status == STATUS_Active:
                if created_at < datetime.utcnow() - EXPIRATION_DURATION: # Check if key is expired
                    logger.info(f"API key for {discord_id} has become expired, generating new key")
                else:
                    api_key_found = True
                    logger.info(f"Existing API key for {discord_id} retrieved")
            else:
                logger.warning(f"API key {api_key} is void: {status}")
                if status == STATUS_Banned:
                    return jsonify({"error": "User Discord ID is banned"}), ERRORCODE_Banned
        if not api_key_found:
            logger.info("Generating new API Key...")
            api_key = secrets.token_hex(16)
            cursor.execute(
                """
                INSERT INTO api_keys (discord_id, api_key)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE api_key = VALUES(api_key),
                                        created_at = CURRENT_TIMESTAMP,
                                        status = 'active'
                """,
                (discord_id, api_key),
            )
            conn.commit()
            logger.info(f"Generated new API key for {discord_id}")
        return jsonify({"api_key": api_key}), 200
    except mysql.connector.Error as err:
        logger.error(f"Database error in get_api_key: {err}")
        return jsonify({"error": "Database error"}), 500
    except Exception as e:
        logger.error(f"Unexpected error in get_api_key: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# JSON Payload Example:
# { "api_key": key,
#   "player_name": self.rsi_handle["current"] }
@app.route("/validateKey", methods=["POST"])
def validate_key():
    api_key = None
    player_name = None
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Bad Request"}), 400
        api_key = data.get("api_key")
        player_name = data.get("player_name")
        if not api_key:
            return jsonify({"error": "Missing api_key"}), ERRORCODE_Void
    except Exception as e:
        logger.error(f"Unexpected error in validate_key: {e}")
        return jsonify({"error": "Internal server error"}), 500

    # Determine if API key exists, then update rsi_handle if needed
    try:
        dicord_id = None
        rsi_handle = None
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT discord_id, rsi_handle, created_at, status FROM api_keys WHERE api_key = %s", (api_key,))
        ret = cursor.fetchone()
        if ret:
            discord_id, rsi_handle, created_at, status = ret
            if player_name and rsi_handle != player_name:
                cursor.execute("UPDATE api_keys SET rsi_handle = %s WHERE api_key = %s", (player_name, api_key))
                conn.commit()
            if status == STATUS_Active:
                # Check if key is expired
                if created_at < datetime.utcnow() - EXPIRATION_DURATION:
                    logger.info(f"API key for {discord_id} has become expired")
                    cursor.execute("UPDATE api_keys SET status = %s WHERE api_key = %s", (STATUS_Expired, api_key))
                    conn.commit()
                    return jsonify({"error": "API key is expired"}), ERRORCODE_Expired
                return jsonify({"success": True, "discord_id": discord_id}), 200
            else:
                logger.warning(f"API key {api_key} is void: {status}")
                if status == STATUS_Banned:
                    return jsonify({"error": "User Discord ID is banned"}), ERRORCODE_Banned
                elif status == STATUS_Revoked:
                    return jsonify({"error": "API key is revoked"}), ERRORCODE_Revoked
                elif status == STATUS_Expired:
                    return jsonify({"error": "API key is expired"}), ERRORCODE_Expired
                else:
                    return jsonify({"error": "API key is void"}), ERRORCODE_Void
        else:
            logger.warning(f"API key {api_key} not found in database")
            return jsonify({"error": "API key not found in database"}), ERRORCODE_Void
    except mysql.connector.Error as err:
        logger.error(f"Database error in validate_key: {err}")
        return jsonify({"error": "Database error"}), 500
    except Exception as e:
        logger.error(f"Unexpected error in validate_key: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# JSON Payload Example:
# { "api_key": key,
#   "player_name": self.rsi_handle["current"] }
@app.route("/get_expiration", methods=["POST"])
def get_expiration():
    api_key = None
    player_name = None
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Bad Request"}), 400
        api_key = data.get("api_key")
        player_name = data.get("player_name")
        if not api_key:
            return jsonify({"error": "Missing api_key"}), ERRORCODE_Void
    except Exception as e:
        logger.error(f"Unexpected error in get_expiration: {e}")
        return jsonify({"error": "Internal server error"}), 500

    # Determine if API key exists, then update rsi_handle if needed
    expiration_date = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT discord_id, rsi_handle, created_at, status FROM api_keys WHERE api_key = %s", (api_key,))
        ret = cursor.fetchone()
        if ret:
            discord_id, rsi_handle, created_at, status = ret
            if player_name and rsi_handle != player_name:
                cursor.execute("UPDATE api_keys SET rsi_handle = %s WHERE api_key = %s", (player_name, api_key))
                conn.commit()
            if status == STATUS_Active:
                # Check if key is expired
                if created_at < datetime.utcnow() - EXPIRATION_DURATION:
                    logger.info(f"API key for {discord_id} has become expired")
                    cursor.execute("UPDATE api_keys SET status = %s WHERE api_key = %s", (STATUS_Expired, api_key))
                    conn.commit()
                    return jsonify({"error": "API key is expired"}), ERRORCODE_Expired
            elif status == STATUS_Banned:
                return jsonify({"error": "User Discord ID is banned"}), ERRORCODE_Banned
            elif status == STATUS_Revoked:
                return jsonify({"error": "API key is revoked"}), ERRORCODE_Revoked
            elif status == STATUS_Expired:
                return jsonify({"error": "API key is expired"}), ERRORCODE_Expired
            else:
                return jsonify({"error": "API key is void"}), ERRORCODE_Void
            expiration_date = created_at + EXPIRATION_DURATION
            # Convert expiration_date to work with datetime.strptime()
            expiration_date = expiration_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            logger.info(f"{player_name} pinged host")
        return jsonify({"success": True, "expires_at": expiration_date}), 200
    except mysql.connector.Error as err:
        logger.error(f"Database error in get_expiration: {err}")
        return jsonify({"error": "Database error"}), 500
    except Exception as e:
        logger.error(f"Unexpected error in get_expiration: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# GET requests for various data maps
@app.route("/data_map/weapons", methods=["GET"])
def get_data_map_weapons():
    ret_json = {}
    ret_json["weapons"] = data_map.weaponMapping
    return jsonify(ret_json)

@app.route("/data_map/zones", methods=["GET"])
def get_data_map_zones():
    ret_json = {}
    ret_json["zones"] = data_map.zonesMapping
    return jsonify(ret_json)

@app.route("/data_map/vehicles", methods=["GET"])
def get_data_map_vehicles():
    ret_json = {}
    ret_json["vehicles"] = data_map.vehicleMapping
    return jsonify(ret_json)

@app.route("/data_map/gameModes", methods=["GET"])
def get_data_map_game_modes():
    ret_json = {}
    ret_json["gameModes"] = data_map.gameModeMapping
    return jsonify(ret_json)

@app.route("/data_map/ignoredVictimRules", methods=["GET"])
def get_data_map_ignoredVictimRules():
    ret_json = {}
    ret_json["ignoredVictimRules"] = data_map.ignoredVictimRules
    return jsonify(ret_json)

@app.route("/reportKill", methods=["POST"])
def report_kill():
    data = request.json
    headers = request.headers
    if not data or not headers:
        return jsonify({"error": "Bad Request"}), 400
    api_key = headers.get("Authorization")
    if not api_key:
        return jsonify({"error": "Missing api_key"}), ERRORCODE_Void

    # Validate API key
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM api_keys WHERE api_key = %s", (api_key,))
        ret = cursor.fetchone()
        if ret:
            status = ret[0]
            if status == STATUS_Banned:
                return jsonify({"error": "User Discord ID is banned"}), ERRORCODE_Banned
            elif status == STATUS_Revoked:
                return jsonify({"error": "API key is revoked"}), ERRORCODE_Revoked
            elif status == STATUS_Expired:
                return jsonify({"error": "API key is expired"}), ERRORCODE_Expired
            elif status != STATUS_Active:
                return jsonify({"error": "API key is void"}), ERRORCODE_Void
        else:
            return jsonify({"error": "Invalid API key"}), ERRORCODE_Void
    except mysql.connector.Error as err:
        logger.error(f"Database error in report_kill: {err}")
        return jsonify({"error": "Database error"}), 500
    except Exception as e:
        logger.error(f"Unexpected error in report_kill: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    result = data.get("result")
    details = data.get("data", {})
    if process_kill(result, details, store_in_db=True):
        return jsonify({"success": True}), 200
    else:
        logger.error("Failed to process kill")
        return jsonify({"error": "Failed to process kill"}), 500

# ---------------------------------------------------------------------------
# Perform Actions
# ---------------------------------------------------------------------------
async def post_weekly_tally(channel_id:int):
    """Generate and post the weekly tally."""
    logger.info("Generating weekly tally...")

    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            logger.error(f"Post Weekly Tally Channel (ID: {channel_id}) not found.")
            return

        # Get kills from the past week from the kill_feed table
        kill_buckets = db_get_kill_buckets("WHERE time_stamp >= NOW() - INTERVAL 7 DAY AND discord_id != '[BWC]'")

        pu_fps_total_kills = len(kill_buckets['PU_FPS'])
        pu_fps_weapon_usage = {}
        pu_fps_member_kills = {}
        for discord_id, victim, weapon, current_ship, zone, game_mode in kill_buckets['PU_FPS']:
            if weapon in pu_fps_weapon_usage:
                pu_fps_weapon_usage[weapon] += 1
            else:
                pu_fps_weapon_usage[weapon] = 1
            if discord_id in pu_fps_member_kills:
                pu_fps_member_kills[discord_id] += 1
            else:
                pu_fps_member_kills[discord_id] = 1
    
        pu_ship_total_kills = len(kill_buckets['PU_Ship'])
        pu_ship_weapon_usage = {}
        pu_ship_curship_usage = {}
        pu_ship_member_kills = {}
        for discord_id, victim, weapon, current_ship, zone, game_mode in kill_buckets['PU_Ship']:
            if weapon in pu_ship_weapon_usage:
                pu_ship_weapon_usage[weapon] += 1
            else:
                pu_ship_weapon_usage[weapon] = 1
            if current_ship in pu_ship_curship_usage:
                pu_ship_curship_usage[current_ship] += 1
            else:
                pu_ship_curship_usage[current_ship] = 1
            if discord_id in pu_ship_member_kills:
                pu_ship_member_kills[discord_id] += 1
            else:
                pu_ship_member_kills[discord_id] = 1

        ac_fps_total_kills = len(kill_buckets['AC_FPS'])
        ac_fps_weapon_usage = {}
        ac_fps_member_kills = {}
        for discord_id, victim, weapon, current_ship, zone, game_mode in kill_buckets['AC_FPS']:
            if weapon in ac_fps_weapon_usage:
                ac_fps_weapon_usage[weapon] += 1
            else:
                ac_fps_weapon_usage[weapon] = 1
            if discord_id in ac_fps_member_kills:
                ac_fps_member_kills[discord_id] += 1
            else:
                ac_fps_member_kills[discord_id] = 1

        ac_ship_total_kills = len(kill_buckets['AC_Ship'])
        ac_ship_weapon_usage = {}
        ac_ship_curship_usage = {}
        ac_ship_member_kills = {}
        for discord_id, victim, weapon, current_ship, zone, game_mode in kill_buckets['AC_Ship']:
            if weapon in ac_ship_weapon_usage:
                ac_ship_weapon_usage[weapon] += 1
            else:
                ac_ship_weapon_usage[weapon] = 1
            if current_ship in ac_ship_curship_usage:
                ac_ship_curship_usage[current_ship] += 1
            else:
                ac_ship_curship_usage[current_ship] = 1
            if discord_id in ac_ship_member_kills:
                ac_ship_member_kills[discord_id] += 1
            else:
                ac_ship_member_kills[discord_id] = 1
    except Exception as e:
        logger.error(f"Error generating weekly tally data: {e}")
        return
    

    # Create an embed message (NOTE: every third `embed_var.add_field()` is blank to create a 2 column format)
    embed_desc = "Black Widow Company's Star Citizen kill report from **" + (datetime.utcnow() - timedelta(days=7)).strftime("%B %d") + "** to **" + datetime.utcnow().strftime("%B %d") + "**\n\u3164"
    embed_var = discord.Embed(title="Weekly Kill Tally", color=0xff0000, description=embed_desc, timestamp=datetime.utcnow())
    embed_var.set_author(name="GrimReaperBot", icon_url="https://media.discordapp.net/attachments/1079475596314280066/1427308241796333691/5ae5886122e57b7510cc31a69b9b2dca.png?ex=68ee63e2&is=68ed1262&hm=fb4fd804a994eb6ec1d7c6b62bb55a877441934ae273e2f05816a51be9ff2e51&=&format=webp&quality=lossless")

    pu_total_desc = f"**Total PU Kills:** `{pu_fps_total_kills + pu_ship_total_kills}`\n"
    pu_total_desc += f"> FPS kills: `{pu_fps_total_kills}`\n"
    pu_total_desc += f"> Ship kills: `{pu_ship_total_kills}`\n\u3164"
    pu_total_desc += "\n**Top FPS Weapons:**\n"
    sorted_pu_fps_weapons = sorted(pu_fps_weapon_usage.items(), key=lambda x: x[1], reverse=True)
    pad_count = 3
    for weapon, count in sorted_pu_fps_weapons[:3]: # Limit to 3
        weapon_human_readable = convert_string(data_map.weaponMapping, weapon, fuzzy_search=False)
        pu_total_desc += f"> {weapon_human_readable}: `{count}`\n"
        pad_count -= 1
    for _ in range(pad_count):
        pu_total_desc += f"> \u3164\n"
    pu_total_desc += "\n**Top Ship Weapons:**\n"
    sorted_pu_ship_weapons = sorted(pu_ship_weapon_usage.items(), key=lambda x: x[1], reverse=True)
    pad_count = 3
    for weapon, count in sorted_pu_ship_weapons[:3]: # Limit to 3
        weapon_human_readable = convert_string(data_map.weaponMapping, weapon, fuzzy_search=False)
        pu_total_desc += f"> {weapon_human_readable}: `{count}`\n"
        pad_count -= 1
    for _ in range(pad_count):
        pu_total_desc += f"> \u3164\n"
    pu_total_desc += "\n**Top Ships:**\n"
    sorted_pu_ship_curships = sorted(pu_ship_curship_usage.items(), key=lambda x: x[1], reverse=True)
    pad_count = 3
    for ship, count in sorted_pu_ship_curships[:3]: # Limit to 3
        ship_human_readable = convert_string(data_map.vehicleMapping, ship, fuzzy_search=False)
        if ship_human_readable == "FPS":
            ship_human_readable = "Undetermined"
        pu_total_desc += f"> {ship_human_readable}: `{count}`\n"
        pad_count -= 1
    for _ in range(pad_count):
        pu_total_desc += f"> \u3164\n"
    #pu_total_desc += "\n\u3164"
    embed_var.add_field(name="🚀 Persistent Universe", value=pu_total_desc, inline=True)

    ac_total_desc = f"**Total AC Kills:** `{ac_fps_total_kills + ac_ship_total_kills}`\n"
    ac_total_desc += f"> FPS kills: `{ac_fps_total_kills}`\n"
    ac_total_desc += f"> Ship kills: `{ac_ship_total_kills}`\n\u3164"
    ac_total_desc += "\n**Top FPS Weapons:**\n"
    sorted_ac_fps_weapons = sorted(ac_fps_weapon_usage.items(), key=lambda x: x[1], reverse=True)
    pad_count = 3
    for weapon, count in sorted_ac_fps_weapons[:3]: # Limit to 3
        weapon_human_readable = convert_string(data_map.weaponMapping, weapon, fuzzy_search=False)
        ac_total_desc += f"> {weapon_human_readable}: `{count}`\n"
        pad_count -= 1
    for _ in range(pad_count):
        ac_total_desc += f"> \u3164\n"
    ac_total_desc += "\n**Top Ship Weapons:**\n"
    sorted_ac_ship_weapons = sorted(ac_ship_weapon_usage.items(), key=lambda x: x[1], reverse=True)
    pad_count = 3
    for weapon, count in sorted_ac_ship_weapons[:3]: # Limit to 3
        weapon_human_readable = convert_string(data_map.weaponMapping, weapon, fuzzy_search=False)
        ac_total_desc += f"> {weapon_human_readable}: `{count}`\n"
        pad_count -= 1
    for _ in range(pad_count):
        ac_total_desc += f"> \u3164\n"
    ac_total_desc += "\n**Top Ships:**\n"
    sorted_ac_ship_curships = sorted(ac_ship_curship_usage.items(), key=lambda x: x[1], reverse=True)
    pad_count = 3
    for ship, count in sorted_ac_ship_curships[:3]: # Limit to 3
        ship_human_readable = convert_string(data_map.vehicleMapping, ship, fuzzy_search=False)
        if ship_human_readable == "FPS":
            ship_human_readable = "Undetermined"
        ac_total_desc += f"> {ship_human_readable}: `{count}`\n"
        pad_count -= 1
    for _ in range(pad_count):
        ac_total_desc += f"> \u3164\n"
    #ac_total_desc += "\n\u3164"
    embed_var.add_field(name="🕹 Arena Commander", value=ac_total_desc, inline=True)

    embed_var.add_field(name="\u200b", value="\u200b", inline=True)

    embed_var.add_field(name="\u200b", value="~~-----~~ **Top 10 - FPS Combat** ~~-----~~", inline=True)
    embed_var.add_field(name="\u200b", value="\u200b", inline=True)
    embed_var.add_field(name="\u200b", value="\u200b", inline=True)

    pu_top10_desc = ""
    rank = 1
    sorted_pu_fps_members = sorted(pu_fps_member_kills.items(), key=lambda x: x[1], reverse=True)
    for discord_id, kill_count in sorted_pu_fps_members[:10]: # Top 10 PU FPS killers
        bwc_name = get_bwc_name(discord_id, True)
        pu_top10_desc += f"**{rank}.** {bwc_name} "
        if rank == 1:
            pu_top10_desc += "🥇"
        elif rank == 2:
            pu_top10_desc += "🥈"
        elif rank == 3:
            pu_top10_desc += "🥉"
        pu_top10_desc += f"\n> {kill_count} kills\n"
        rank += 1
    #pu_top10_desc += "\n\u3164"
    embed_var.add_field(name="🚀 Persistent Universe", value=pu_top10_desc, inline=True)

    ac_top10_desc = ""
    rank = 1
    sorted_ac_fps_members = sorted(ac_fps_member_kills.items(), key=lambda x: x[1], reverse=True)
    for discord_id, kill_count in sorted_ac_fps_members[:10]: # Top 10 AC FPS killers
        bwc_name = get_bwc_name(discord_id, True)
        ac_top10_desc += f"**{rank}.** {bwc_name} "
        if rank == 1:
            ac_top10_desc += "🥇"
        elif rank == 2:
            ac_top10_desc += "🥈"
        elif rank == 3:
            ac_top10_desc += "🥉"
        ac_top10_desc += f"\n> {kill_count} kills\n"
        rank += 1
    #ac_top10_desc += "\n\u3164"
    embed_var.add_field(name="🕹 Arena Commander", value=ac_top10_desc, inline=True)

    embed_var.add_field(name="\u200b", value="\u200b", inline=True)

    embed_var.add_field(name="\u200b", value="~~-----~~ **Top 10 - Ship Combat** ~~-----~~", inline=True)
    embed_var.add_field(name="\u200b", value="\u200b", inline=True)
    embed_var.add_field(name="\u200b", value="\u200b", inline=True)

    pu_ship_top10_desc = ""
    rank = 1
    sorted_pu_ship_members = sorted(pu_ship_member_kills.items(), key=lambda x: x[1], reverse=True)
    for discord_id, kill_count in sorted_pu_ship_members[:10]: # Top 10 PU Ship killers
        bwc_name = get_bwc_name(discord_id, True)
        pu_ship_top10_desc += f"**{rank}.** {bwc_name} "
        if rank == 1:
            pu_ship_top10_desc += "🥇"
        elif rank == 2:
            pu_ship_top10_desc += "🥈"
        elif rank == 3:
            pu_ship_top10_desc += "🥉"
        pu_ship_top10_desc += f"\n> {kill_count} kills\n"
        rank += 1
    #pu_ship_top10_desc += "\n\u3164"
    embed_var.add_field(name="🚀 Persistent Universe", value=pu_ship_top10_desc, inline=True)

    ac_ship_top10_desc = ""
    rank = 1
    sorted_ac_ship_members = sorted(ac_ship_member_kills.items(), key=lambda x: x[1], reverse=True)
    for discord_id, kill_count in sorted_ac_ship_members[:10]: # Top 10 AC Ship killers
        bwc_name = get_bwc_name(discord_id, True)
        ac_ship_top10_desc += f"**{rank}.** {bwc_name} "
        if rank == 1:
            ac_ship_top10_desc += "🥇"
        elif rank == 2:
            ac_ship_top10_desc += "🥈"
        elif rank == 3:
            ac_ship_top10_desc += "🥉"
        ac_ship_top10_desc += f"\n> {kill_count} kills\n"
        rank += 1
    #ac_ship_top10_desc += "\n\u3164"
    embed_var.add_field(name="🕹 Arena Commander", value=ac_ship_top10_desc, inline=True)

    embed_var.add_field(name="\u200b", value="\u200b", inline=True)

    embed_var.add_field(name="\u200b", value="Download the Kill Tracker client [here](https://discord.com/channels/378419940027269130/480367983558918174/1429925997007999036)\n\u3164", inline=True)

    #embed_var.set_image(url="https://cdn.discordapp.com/attachments/1079475596314280066/1430355324287844505/resized_BannerStandard.png?ex=68f979b4&is=68f82834&hm=dfd739ab373f667943af6f7f75b03d13245e36ca2a81d63c169fbb362ea50d4b&") # This image is shown at the bottom of the embed
    embed_var.set_footer(text="[BWC] Star Citizen Kill Tracker Weekly Kill Tally Report")
    try:
        await channel.send(embed=embed_var)
        logger.info("Weekly tally posted successfully.")
    except Exception as e:
        logger.error(f"Error posting weekly tally: {e}")

# Sample JSON Payloads:
# Current user killed themselves
#{
#    "result": "suicide",
#    "data": {
#        'discord_id': self.discord_id["current"],
#        'player': curr_user,
#        'victim': curr_user,
#        'time': "<2025-10-02T22:57:03.975Z>",
#        'zone': zone,
#        'weapon': weapon,
#        'game_mode': self.game_mode,
#        'current_ship': self.active_ship["current"],
#        'client_ver': self.local_version,
#        'anonymize_state': {'enabled': False }
#    }
#}
#
# Current user died
#{
#    "result": "killed",
#    "data": {
#        'discord_id': self.discord_id["current"],
#        'player': killer,
#        'victim': curr_user,
#        'time': "<2025-10-02T22:57:03.975Z>",
#        'zone': self.active_ship["current"],
#        'weapon': weapon,
#        'game_mode': self.game_mode,
#        'current_ship': self.active_ship["current"],
#        'client_ver': self.local_version,
#        'anonymize_state': {'enabled': False }
#    }
#}
#
# Current user killed other player
#{
#    "result": "killer",
#    "data": {
#        "discord_id": self.discord_id["current"],
#        "player": curr_user,
#        "victim": killed,
#        "time": "<2025-10-02T22:57:03.975Z>",
#        "zone": zone,
#        "weapon": weapon,
#        "game_mode": self.game_mode,
#        "current_ship": self.active_ship["current"],
#        "client_ver": self.local_version,
#        "anonymize_state": {'enabled': False }
#     }
#}
def process_kill(result:str, details:object, store_in_db:bool):
    discord_id = details.get("discord_id")
    ping_self = details.get("ping_self", False)
    player = details.get("player")
    victim = details.get("victim")
    kill_time = details.get("time") # kill_time is formatted something like "<2025-10-02T22:57:03.975Z>" convert it to a datetime object
    if kill_time:
        kill_time = kill_time.strip("<>")
        kill_time = datetime.strptime(kill_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    zone = details.get("zone")
    weapon = details.get("weapon")
    game_mode = details.get("game_mode")
    client_ver = details.get("client_ver")
    current_ship = details.get("current_ship")
    anonymize_state = details.get("anonymize_state")
    if(anonymize_state.get("enabled")):
        logger.info("Reporting anonymized kill")
        discord_id = "[BWC]"
        player = "[BWC]"

    weapon_human_readable = convert_string(data_map.weaponMapping, weapon, fuzzy_search=False)

    success = True
    if result == "killer":
        channel = None

        # ------------------------------------------------------------------------------------------
        # Record kill in database
        # ------------------------------------------------------------------------------------------
        if store_in_db:
            channel = bot.get_channel(CHANNEL_SC_PUBLIC)
            logger.info(f"Recording DB kill: {player} killed {victim} with {weapon} in {zone} with ship {current_ship} playing {game_mode}")
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(
                        """
                        INSERT INTO kill_feed (discord_id, rsi_handle, victim, weapon, zone, current_ship, game_mode, time_stamp, client_ver)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (discord_id, player, victim, weapon, zone, current_ship, game_mode, kill_time, client_ver)
                    )
                conn.commit()
            except mysql.connector.Error as err:
                logger.error(f"Error recording kill in database: {err}")
                success = False
            except Exception as e:
                logger.error(f"Unexpected error recording kill in database: {e}")
                success = False
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        else:
            channel = bot.get_channel(CHANNEL_TEST_SERVER_PUBLIC)
            logger.info(f"Test kill: {player} killed {victim} with {weapon} in {zone} with ship {current_ship} playing {game_mode}")

        # ------------------------------------------------------------------------------------------
        # Announce kill in Discord
        # ------------------------------------------------------------------------------------------
        if game_mode == "SC_Default" and success and channel:
            try:
                # Record kill in memory used to track kill streaks and multi-kills
                now = datetime.utcnow().timestamp()
                g_kill_timestamps[discord_id].append(now)
                g_kill_streaks[discord_id] += 1

                bwc_name = get_bwc_name(discord_id, ping_self, player) # Reassign bwc_name to how their name is formatted in Discord. Using their discord_id. Fallback to their RSI handle if not found

                victim_link = f"[{victim}](https://robertsspaceindustries.com/citizens/{victim})"
                kill_message = f"> **{bwc_name}** killed ☠️ **{victim_link}** ☠️ using {weapon_human_readable}"

                # Kill streaks
                if g_kill_streaks[discord_id] == 50:
                    kill_message += f"\n 🔥🔥🔥🔥🔥 **{bwc_name}** is on a **50-kill streak!** 🔥🔥🔥🔥🔥"
                elif g_kill_streaks[discord_id] == 20:
                    kill_message += f"\n 🔥🔥🔥🔥 **{bwc_name}** is on a **20-kill streak!** 🔥🔥🔥🔥"
                elif g_kill_streaks[discord_id] == 10:
                    kill_message += f"\n 🔥🔥🔥 **{bwc_name}** is on a **10-kill streak!** 🔥🔥🔥"
                elif g_kill_streaks[discord_id] == 5:
                    kill_message += f"\n 🔥🔥 **{bwc_name}** is on a **5-kill streak!** 🔥🔥"
                elif g_kill_streaks[discord_id] == 3:
                    kill_message += f"\n 🔥 **{bwc_name}** is on a **3-kill streak!** 🔥"

                # Clean up any kills that are older than 60 seconds
                g_kill_timestamps[discord_id] = [t for t in g_kill_timestamps[discord_id] if now - t <= 60]

                # Chain Multiple kills
                if len([t for t in g_kill_timestamps[discord_id] if now - t <= 50]) >= 6:
                    kill_message += "\n ⚡⚡⚡⚡⚡⚡ Killimanjaro! ⚡⚡⚡⚡⚡⚡"
                elif len([t for t in g_kill_timestamps[discord_id] if now - t <= 40]) >= 5:
                    kill_message += "\n ⚡⚡⚡⚡⚡ Killtacular! ⚡⚡⚡⚡⚡"
                elif len([t for t in g_kill_timestamps[discord_id] if now - t <= 30]) >= 4:
                    kill_message += "\n ⚡⚡⚡⚡ OverKill! ⚡⚡⚡⚡"
                elif len([t for t in g_kill_timestamps[discord_id] if now - t <= 20]) >= 3:
                    kill_message += "\n ⚡⚡⚡ Triple Kill! ⚡⚡⚡"
                elif len([t for t in g_kill_timestamps[discord_id] if now - t <= 10]) >= 2:
                    kill_message += "\n ⚡⚡ Double Kill! ⚡⚡"

                # Milestones
                kill_buckets = db_get_kill_buckets("WHERE discord_id = %s", (discord_id,))
                if not kill_buckets == {}:
                    total_kills = 0
                    total_kills += len(kill_buckets['PU_FPS'])
                    total_kills += len(kill_buckets['PU_Ship'])
                    if total_kills > 0 and total_kills % 50 == 0:
                        kill_message += f"\n 🏆 {bwc_name} reached **{total_kills} total PU kills!** 🏆\n"

                # Send kill announcement
                asyncio.run_coroutine_threadsafe(
                    channel.send(kill_message),
                    bot.loop
                )
            except Exception as e:
                logger.error(f"Unexpected error sending kill announcement: {e}")
    elif result == "killed":
        g_kill_streaks[discord_id] = 0
        logger.info(f"Reporting killed player: {victim} with ship {current_ship} playing {game_mode}, killed by {player} with {weapon} in {zone}")
    elif result == "suicide":
        g_kill_streaks[discord_id] = 0
        logger.info(f"Reporting suicide: {victim} with ship {current_ship} playing {game_mode}, died by {weapon} in {zone}")
    else:
        logger.warning(f"Unhandled kill result type: {result}")
        success = False
    return success

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_waitress():
    serve(app, host='0.0.0.0', port=25219)

if __name__ == "__main__":
    load_dotenv()

    waitress_thread = threading.Thread(target=run_waitress, daemon=True)
    waitress_thread.start()

    bot.run(os.getenv("DISCORD_TOKEN"))
