# main.py - Discord bot for tracking kills/deaths in Star Citizen for BWC members
import dis
import os
from time import sleep
from dotenv import load_dotenv
import discord
from discord.ext import commands, tasks
import mysql.connector
from mysql.connector import pooling
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
         (https://robertsspaceindustries.com/en/citizens/BWC-Firefly)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
"""
API_SHARED_SECRET = os.getenv("API_SHARED_SECRET") # Shared secret for API requests from the BWC website
ROLE_GrimReaperAdmin = 1427357824597360671 #os.getenv("ADMIN_ROLE_NAME", 480372823454384138)
ROLE_BWC = 480372977452580874

CHANNEL_SC_PUBLIC = 1420804944075689994 # Test server (Game_Overture)
CHANNEL_SC_ANNOUNCEMENTS = 1421936341486145678 # Test server (Game_Overture)
#CHANNEL_SC_PUBLIC = 480367983558918174 # BWC server
#CHANNEL_SC_ANNOUNCEMENTS = 827312889890471957 # BWC server

# ERROR CODES:
ERRORCODE_Void = 469
ERRORCODE_Expired = 470
ERRORCODE_Revoked = 471
ERRORCODE_Banned = 472

# Status string identifiers for api_keys table
STATUS_Active = "active" # This is hardcoded as the default value in the table schema
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
    return cnxpool.get_connection()

# ---------------------------------------------------------------------------
# Bot Events
# ---------------------------------------------------------------------------
@tasks.loop(hours=1)
async def hourly_task_check():
    if should_task_run("weekly_kill_tally", 7):
        await weekly_tally()

@bot.event
async def on_ready():
    global cnxpool

    # Database Setup
    try:
        dbconfig = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_DATABASE")
        }
        cnxpool = pooling.MySQLConnectionPool(pool_name="mypool",
                                              pool_size=8,
                                              **dbconfig)
        logger.info("Database connection pool established")
        
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
# APScheduler Setup
# ---------------------------------------------------------------------------
# jobstores = {
#     "default": SQLAlchemyJobStore(url=f"sqlite:///{DB_PATH}")
# }
# scheduler = AsyncIOScheduler(jobstores=jobstores)


async def weekly_tally():
    """Generate and post the weekly tally."""
    logger.info("Generating weekly tally...")

    try:
        # Post the message to the announcements channel
        channel = bot.get_channel(CHANNEL_SC_ANNOUNCEMENTS)
        if not channel:
            logger.error(f"Announcements Channel (ID: {CHANNEL_SC_ANNOUNCEMENTS}) not found.")
            return

        conn = get_connection()
        cursor = conn.cursor()
        # Get kills from the past week from the kill_feed table, by counting number of rows with discord_id and time_stamp column
        cursor.execute("""
            SELECT discord_id, COUNT(*) as kill_count_pu
            FROM kill_feed
            WHERE time_stamp >= NOW() - INTERVAL 7 DAY AND discord_id != '[BWC]' AND game_mode = 'SC_Default'
            GROUP BY discord_id
            """)
        results_PU = cursor.fetchall() # List of tuples (discord_id, kill_count)
        cursor.execute("""
            SELECT discord_id, COUNT(*) as kill_count_ac
            FROM kill_feed
            WHERE time_stamp >= NOW() - INTERVAL 7 DAY AND discord_id != '[BWC]' AND game_mode != 'SC_Default'
            GROUP BY discord_id
            """)
        results_AC = cursor.fetchall() # List of tuples (discord_id, kill_count)
    except mysql.connector.Error as err:
        logger.error(f"Database error in weekly_tally: {err}")
        return
    except Exception as e:
        logger.error(f"Unexpected error in weekly_tally: {e}")
        return
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    if not results_PU:
        logger.info("No PU kills recorded in the past week.")
    if not results_AC:
        logger.info("No AC kills recorded in the past week.")
    logger.info(f"Weekly tally results PU: {results_PU}")
    logger.info(f"Weekly tally results AC: {results_AC}")

    results_PU = sorted(results_PU, key=lambda x: x[1], reverse=True) # Sort PU results by kill_count descending
    results_AC = sorted(results_AC, key=lambda x: x[1], reverse=True) # Sort AC results by kill_count descending

    # Create an embed message with the top 10 killers for both PU and AC, each in their own section
    embed_desc = "Top 10 BWC members with most kills in the PU and AC for the week of **" + (datetime.utcnow() - timedelta(days=7)).strftime("%m-%d") + "**\n\u3164"
    embed_var = discord.Embed(title="Weekly Kill Tally", color=0xff0000, description=embed_desc, timestamp=datetime.utcnow())
    embed_var.set_author(name="GrimReaperBot", icon_url="https://media.discordapp.net/attachments/1079475596314280066/1427308241796333691/5ae5886122e57b7510cc31a69b9b2dca.png?ex=68ee63e2&is=68ed1262&hm=fb4fd804a994eb6ec1d7c6b62bb55a877441934ae273e2f05816a51be9ff2e51&=&format=webp&quality=lossless")
    #embed_var.set_image(url="")
    embed_var.set_footer(text="[BWC] Kill Tracker")
    if results_PU:
        pu_description = ""
        rank = 1
        for discord_id, kill_count in results_PU[:10]: # Top 10 PU killers
            bwc_name = get_bwc_name(discord_id, True)
            pu_description += f"**{rank}.** {bwc_name} "
            if rank == 1:
                pu_description += "🥇"
            elif rank == 2:
                pu_description += "🥈"
            elif rank == 3:
                pu_description += "🥉"
            pu_description += f"\n> {kill_count} kills\n"
            rank += 1
        pu_description += "\u3164"
        embed_var.add_field(name="🚀 Persistent Universe", value=pu_description, inline=True)
    if results_AC:
        ac_description = ""
        rank = 1
        for discord_id, kill_count in results_AC[:10]: # Top 10 AC killers
            bwc_name = get_bwc_name(discord_id, True)
            ac_description += f"**{rank}.** {bwc_name} "
            if rank == 1:
                ac_description += "🥇"
            elif rank == 2:
                ac_description += "🥈"
            elif rank == 3:
                ac_description += "🥉"
            ac_description += f"\n> {kill_count} kills\n"
            rank += 1
        ac_description += "\u3164"
        embed_var.add_field(name="🕹 Arena Commander", value=ac_description, inline=True)
    try:
        await channel.send(embed=embed_var)
        logger.info("Weekly tally posted successfully.")
    except Exception as e:
        logger.error(f"Error posting weekly tally: {e}")

def db_total_kills(discord_id:str) -> int: # Returns -1 on error, or total kills if successful
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM kill_feed WHERE discord_id = %s", (discord_id,))
        ret = cursor.fetchone()
        if ret:
            total = ret[0]
            return total
    except mysql.connector.Error as err:
        logger.error(f"Database error in total_kills: {err}")
        return -1
    except Exception as e:
        logger.error(f"Unexpected error in total_kills: {e}")
        return -1
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return -1

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
@bot.command(name="grimreaper_totalkills")
@commands.has_role(ROLE_BWC)
async def cmd_total_kills(ctx, discord_id:str=""):
    if discord_id == "":
        discord_id = str(ctx.author.id)
    total_kills = db_total_kills(discord_id)
    bwc_name = get_bwc_name(discord_id, False)
    if total_kills >= 0:
        await ctx.send(f"✅ {bwc_name} has a total of {total_kills} recorded kills.")
    else:
        await ctx.send("❌ Unable to retrieve your kill count.")

@cmd_total_kills.error
async def cmd_total_kills_error(ctx, error):
    await ctx.send("❌ An error occurred while processing your request.")

# ---------------------------------------------------------------------------

@bot.command(name="grimreaper_weeklytally")
@commands.has_role(ROLE_GrimReaperAdmin)
async def cmd_weekly_tally(ctx):
    """Manually trigger the weekly tally (Admin only)."""
    await weekly_tally()
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
@commands.has_role(ROLE_BWC)
async def cmd_test_kill(ctx, victim:str=""):
    """Simulate recording a PvP kill (testing only)."""
    if victim == "":
        victim = "Test_Victim"
    details = {
        'discord_id': str(ctx.author.id),
        'player': "Test_RSI_Name",
        'victim': victim,
        'time': "<2025-10-02T22:57:03.975Z>",
        'zone': "Test_Zone",
        'weapon': "Test_Weapon",
        'game_mode': "Test_Mode",
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
        # Record kill in memory
        g_kill_timestamps[discord_id].append(kill_time.timestamp())
        g_kill_streaks[discord_id] += 1

        # ------------------------------------------------------------------------------------------
        # Record kill in database
        # ------------------------------------------------------------------------------------------
        if store_in_db:
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
            logger.info(f"Test kill: {player} killed {victim} with {weapon} in {zone} with ship {current_ship} playing {game_mode}")

        # ------------------------------------------------------------------------------------------
        # Announce kill in Discord
        # ------------------------------------------------------------------------------------------
        bwc_name = get_bwc_name(discord_id, False, player) # Reassign bwc_name to how their name is formatted in Discord. Using their discord_id. Fallback to their RSI handle if not found

        #zone_human_readable = convert_string(data_map.zonesMapping, zone, fuzzy_search=True)
        # game_mode == "SC_Default"
        channel = bot.get_channel(CHANNEL_SC_PUBLIC)
        if success and channel:
            try:
                # Regular kill announcement
                asyncio.run_coroutine_threadsafe(
                    channel.send(f"**{bwc_name}** killed ☠️ **{victim}** ☠️ using {weapon_human_readable}"),
                    bot.loop
                )

                # Kill streaks
                if g_kill_streaks[discord_id] == 50:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"🔥🔥🔥🔥🔥 **{bwc_name}** is on a **50-kill streak!** 🔥🔥🔥🔥🔥"),
                        bot.loop
                    )
                elif g_kill_streaks[discord_id] == 20:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"🔥🔥🔥🔥 **{bwc_name}** is on a **20-kill streak!** 🔥🔥🔥🔥"),
                        bot.loop
                    )
                elif g_kill_streaks[discord_id] == 10:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"🔥🔥🔥 **{bwc_name}** is on a **10-kill streak!** 🔥🔥🔥"),
                        bot.loop
                    )
                elif g_kill_streaks[discord_id] == 5:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"🔥🔥 **{bwc_name}** is on a **5-kill streak!** 🔥🔥"),
                        bot.loop
                    )
                elif g_kill_streaks[discord_id] == 3:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"🔥 **{bwc_name}** is on a **3-kill streak!** 🔥"),
                        bot.loop
                    )

                # Clean up any kills that are older than 60 seconds
                now = datetime.utcnow().timestamp()
                g_kill_timestamps[discord_id] = [t for t in g_kill_timestamps[discord_id] if now - t <= 60]

                # Chain Multiple kills
                if len([t for t in g_kill_timestamps[discord_id] if now - t <= 50]) >= 6:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"⚡ Killimanjaro! ⚡ **{bwc_name}**"),
                        bot.loop
                    )
                elif len([t for t in g_kill_timestamps[discord_id] if now - t <= 40]) >= 5:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"⚡ Killtacular! ⚡ **{bwc_name}**"),
                        bot.loop
                    )
                elif len([t for t in g_kill_timestamps[discord_id] if now - t <= 30]) >= 4:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"⚡ OverKill! ⚡ **{bwc_name}**"),
                        bot.loop
                    )
                elif len([t for t in g_kill_timestamps[discord_id] if now - t <= 20]) >= 3:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"⚡ Triple Kill ⚡ **{bwc_name}**"),
                        bot.loop
                    )
                elif len([t for t in g_kill_timestamps[discord_id] if now - t <= 10]) >= 2:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"⚡ Double Kill ⚡ **{bwc_name}**"),
                        bot.loop
                    )

                # Milestones
                total_kills = db_total_kills(discord_id)
                if total_kills > 0 and total_kills % 50 == 0:
                    asyncio.run_coroutine_threadsafe(
                        channel.send(f"🏆 {bwc_name} reached **{total_kills} kills!**"),
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

def get_bwc_name(discord_id:str, ping_user=False, fallback_name="Unknown") -> str:
    """ Reassign bwc_name to how their name is formatted in Discord. Using their discord_id """
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
            logger.error(f"Database error in weekly_tally fetching rsi_handle: {err}")
        except Exception as e:
            logger.error(f"Unexpected error in weekly_tally fetching rsi_handle: {e}")
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
    return False

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
# Main
# ---------------------------------------------------------------------------
def run_waitress():
    serve(app, host='0.0.0.0', port=25219)

if __name__ == "__main__":
    load_dotenv()

    waitress_thread = threading.Thread(target=run_waitress, daemon=True)
    waitress_thread.start()

    bot.run(os.getenv("DISCORD_TOKEN"))
