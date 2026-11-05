
# bot.py
import asyncio
import os
import getpass
import platform
import json
import discord
from discord import Embed, app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
import random
import time
import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import firebase_admin
from firebase_admin import credentials, firestore
import firebase_admin
from firebase_admin import credentials, firestore

# Ensure environment variables from .env are loaded before use
# Ensure environment variables from .env are loaded before use
load_dotenv()

# Load credentials from your service account file
FIREBASE_KEY_PATH = os.getenv("FIREBASE_KEY_PATH")
if not FIREBASE_KEY_PATH:
    raise RuntimeError(
        "FIREBASE_KEY_PATH is not set. Add it to your .env (e.g., FIREBASE_KEY_PATH=/absolute/path/service-account.json)."
    )
if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_KEY_PATH)
    firebase_admin.initialize_app(cred)
db = firestore.client()





FOUNDER_ID = 826784147606470656

# Load credentials from your service account file
FIREBASE_KEY_PATH = os.getenv("FIREBASE_KEY_PATH")
if not FIREBASE_KEY_PATH:
    raise RuntimeError(
        "FIREBASE_KEY_PATH is not set. Add it to your .env (e.g., FIREBASE_KEY_PATH=/absolute/path/service-account.json)."
    )
if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_KEY_PATH)
    firebase_admin.initialize_app(cred)
db = firestore.client()





FOUNDER_ID = 826784147606470656
TOKEN = os.getenv('DISCORD_TOKEN')
checking = 0
intents = discord.Intents.all()

intents = discord.Intents.default()
intents.members = True          # for on_member_join
intents.message_content = True  # IMPORTANT: lets on_message receive messages
bot = commands.Bot(command_prefix="!", intents=intents)
slash_commands_synced = False

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
json_dir = os.path.join(project_dir, "json")
config_file = "config.json"
balances_file = "balances.json"
chao_folder = "wildchao"
chao_data_file = "chao_data.json"
local_age_queue_file = os.path.join(json_dir, "local_age_queue.json")
# In-memory cached queue to avoid frequent disk reads
local_age_queue = []

ALIGNMENT_EMOJIS = {
    "neutral": "<:bubblenew:1408217651116773376>",
    "dark": "<:spikenew:1408217572733882509>",
    "hero": "<:halonew:1408217567943987380> "
}

# Custom converter: resolves to a Member if possible, otherwise returns None
class MaybeMember(commands.MemberConverter):
    async def convert(self, ctx, argument):
        try:
            return await super().convert(ctx, argument)
        except commands.MemberNotFound:
            return None

def save_user_chao_data(user_ids=None):
    try:
        if not user_chao_data:
            # nothing to save
            return

        # If no specific users requested, persist all (legacy behaviour)
        if user_ids is None:
            # validate shape quickly
            if not all(isinstance(u.get("chao", []), list) for u in user_chao_data.values() if u.get("chao") is not None):
                print("[ERROR] Not saving chao_data: data is empty or malformed!")
                return
            batch = db.batch()
            for uid, payload in user_chao_data.items():
                ref = db.collection("chao_data").document(str(uid))
                batch.set(ref, payload)
            batch.commit()
            return

        # Normalize user_ids to list of strings
        if isinstance(user_ids, (str, int)):
            user_ids = [str(user_ids)]
        else:
            user_ids = [str(u) for u in user_ids]

        # Write only requested user documents
        batch = db.batch()
        for uid in user_ids:
            payload = user_chao_data.get(str(uid), {})
            ref = db.collection("chao_data").document(str(uid))
            batch.set(ref, payload)
        batch.commit()
    except Exception as e:
        print(f"[ERROR] Failed to save chao_data to Firestore: {e}")


def load_local_age_queue():
    """Load persistent local age queue from disk."""
    try:
        global local_age_queue
        if os.path.exists(local_age_queue_file):
            with open(local_age_queue_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    local_age_queue = data
                    return local_age_queue
        local_age_queue = []
        return local_age_queue
    except Exception as e:
        print(f"[WARN] Failed to load local age queue: {e}")
        return []


def save_local_age_queue(queue):
    """Persist the local age queue to disk."""
    try:
        global local_age_queue
        local_age_queue = queue
        with open(local_age_queue_file, "w", encoding="utf-8") as f:
            json.dump(local_age_queue, f, indent=2)
    except Exception as e:
        print(f"[ERROR] Failed to save local age queue: {e}")


def enqueue_age_write(user_id, chao_index=None, scheduled_time=None, merge=True):
    """Add an age write event to the local queue. scheduled_time should be a unix timestamp.
    If merge=True, attempt to merge multiple events for the same user and scheduled_time.
    """
    # Default scheduled_time: next top-of-hour
    if scheduled_time is None:
        now = time.time()
        # compute next top-of-hour timestamp
        t = datetime.datetime.fromtimestamp(now)
        next_hour = (t.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1))
        scheduled_time = next_hour.timestamp()

    # use in-memory queue to reduce disk I/O
    queue = local_age_queue if local_age_queue is not None else load_local_age_queue()
    # Try to merge: find an existing entry for the same user and scheduled_time (within 60s)
    matched = None
    for item in queue:
        if str(item.get("user_id")) == str(user_id) and abs(item.get("scheduled_time", 0) - scheduled_time) <= 60:
            matched = item
            break
    if matched and merge:
        # mark chao_index if provided
        if chao_index is not None and chao_index not in matched.get("chao_indices", []):
            matched.setdefault("chao_indices", []).append(chao_index)
    else:
        entry = {
            "user_id": str(user_id),
            "chao_indices": [chao_index] if chao_index is not None else [],
            "scheduled_time": scheduled_time
        }
        queue.append(entry)
    save_local_age_queue(queue)


async def flush_age_queue_once():
    """Flush due queue entries now: apply queued aging changes to user_chao_data and persist affected users once."""
    # operate on in-memory queue
    global local_age_queue
    if local_age_queue is None:
        queue = load_local_age_queue()
    else:
        queue = local_age_queue
    if not queue:
        return
    now = time.time()
    to_process = [q for q in queue if q.get("scheduled_time", 0) <= now]
    if not to_process:
        return

    # Group by user_id
    by_user = {}
    for item in to_process:
        uid = str(item["user_id"])
        by_user.setdefault(uid, []).extend(item.get("chao_indices", []))

    # Apply changes per user in-memory, then persist
    for uid, indices in by_user.items():
        user_data = user_chao_data.get(uid)
        if not user_data or not isinstance(user_data, dict):
            continue
        chao_list = user_data.get("chao", [])
        # If indices empty -> persist whole user data (conservative)
        affected_chao = set(i for i in indices if isinstance(i, int))
        # Here we don't change the aging logic; instead we ensure the current in-memory state is persisted.
        # (Aging updates are already applied when they happen; the queue only controls when we write.)
        print(f"[AGE-FLUSH] Persisting chao data for user {uid} (chao indices: {sorted(list(affected_chao))})")

    # Persist all affected users in one batch
    try:
        batch = db.batch()
        for uid in by_user.keys():
            ref = db.collection("chao_data").document(str(uid))
            batch.set(ref, user_chao_data.get(uid, {}))
        batch.commit()
        print(f"[AGE-FLUSH] Successfully flushed {len(by_user)} user(s) to Firestore.")
    except Exception as e:
        print(f"[ERROR] Failed to flush age queue to Firestore: {e}")
        return

    # Remove processed entries and persist queue
    remaining = [q for q in queue if q not in to_process]
    save_local_age_queue(remaining)


@tasks.loop(seconds=60)
async def age_queue_flush_task():
    """Task that checks the local age queue every minute and flushes due entries."""
    try:
        await flush_age_queue_once()
    except Exception as e:
        print(f"[ERROR] Exception in age_queue_flush_task: {e}")



def load_race_scores():
    try:
        data = {}
        for doc in db.collection("race_scores").stream():
            data[doc.id] = doc.to_dict() or {}
        return data
    except Exception as e:
        print(f"[WARN] Failed to load race_scores from Firestore: {e}")
        data = {}
        for doc in db.collection("race_scores").stream():
            data[doc.id] = doc.to_dict() or {}
        return data
    except Exception as e:
        print(f"[WARN] Failed to load race_scores from Firestore: {e}")
        return {}

def save_race_scores(user_ids=None):
    try:
        if not race_scores:
            return
        if user_ids is None:
            batch = db.batch()
            for uid, payload in race_scores.items():
                ref = db.collection("race_scores").document(str(uid))
                batch.set(ref, payload)
            batch.commit()
            return

        if isinstance(user_ids, (str, int)):
            user_ids = [str(user_ids)]
        else:
            user_ids = [str(u) for u in user_ids]

        batch = db.batch()
        for uid in user_ids:
            payload = race_scores.get(str(uid), {})
            ref = db.collection("race_scores").document(str(uid))
            batch.set(ref, payload)
        batch.commit()
    except Exception as e:
        print(f"[ERROR] Failed to save race_scores to Firestore: {e}")



race_scores = load_race_scores()  # Load from JSON at startup

command_categories = {
    "Chao": [
        "chao", "mychao", "spawnchao", "release", "chaorace", "chaoage", "nickname", "fortuneteller",
        "play", "pet", "chaoenergy", "chaohappiness", "chaohunger"
    ],
    "Economy": ["balance", "earn", "pay", "balancetop", "setbalance"],
    "Items": ["use", "shop", "inventory"],
    "General": ["commandslist", "setchannel"],  
}


class ChaoHelpCommand(commands.HelpCommand):
    """Branded help pages for the bot's commands."""

    category_emojis = {
        "Chao": "<:bubblenew:1408217651116773376>",
        "Economy": "<:yellowchao:1407492531046060032>",
        "Items": "🛍️",
        "General": "🧭",
    }

    async def send_bot_help(self, mapping):
        ctx = self.context
        is_admin = ctx.author.guild_permissions.administrator
        embed = discord.Embed(
            title="<:bubblenew:1408217651116773376> Chao Garden Help",
            description=(
                "Raise, play with, and care for your Chao.\n"
                "Use `!help <command>` for detailed instructions."
            ),
            color=discord.Color.from_rgb(74, 194, 179),
        )

        for category, command_names in command_categories.items():
            if category == "Admin" and not is_admin:
                continue

            visible_commands = []
            for command_name in command_names:
                command = bot.get_command(command_name)
                if command and not command.hidden:
                    visible_commands.append(
                        f"`!{command.name}` — {command.short_doc or 'No description'}"
                    )

            if visible_commands:
                emoji = self.category_emojis.get(category, "•")
                embed.add_field(
                    name=f"{emoji} {category}",
                    value="\n".join(visible_commands),
                    inline=False,
                )

        embed.set_footer(text="Tip: command names and arguments are shown with !")
        await ctx.send(embed=embed)

    async def send_command_help(self, command):
        ctx = self.context
        embed = discord.Embed(
            title=f"📖 !{command.qualified_name}",
            description=command.help or "No description available.",
            color=discord.Color.from_rgb(74, 194, 179),
        )

        if command.signature:
            embed.add_field(name="Usage", value=f"`!{command.qualified_name} {command.signature}`", inline=False)
        if command.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`!{alias}`" for alias in command.aliases), inline=False)

        embed.set_footer(text="Use !help to return to the command list")
        await ctx.send(embed=embed)


bot.help_command = ChaoHelpCommand()

# --- Shop Data ---
shop_items = {
    "Food": [
        {"name": "Dark Fruit", "price": 200, "emoji": "<:darkfruit:1408217669018325162>"},
        {"name": "Hero Fruit", "price": 200, "emoji": "<:herofruit:1408217672008863744>"},
        {"name": "Heart Fruit", "price": 400, "emoji": "<:heartfruit:1408217670607962232>"},
        {"name": "Chao Fruit", "price": 300, "emoji": "<:chaofruit:1408217558875766804>"},
        {"name": "Triangle Fruit", "price": 250, "emoji": "<:trianglefruit:1410635146675359866>"},
        {"name": "Square Fruit", "price": 250, "emoji": " <:squarefruit:1410635145257553930>"},
        {"name": "Round Fruit", "price": 250, "emoji": "<:roundfruit:1410635143219249314>"}
    ],
    "Activities": [
        {"name": "Chao Race Ticket", "price": 2000, "emoji": "🎟️"},
        {"name": "Chao Karate Ticket [NOT IMPLEMENTED YET]", "price": 2000, "emoji": "🥋"}
    ],
    "Chao": [
        {"name": "Egg", "price": 20000, "emoji": "<:chaoegg:1408215309592035329>"}
    ]
}


last_earned = {}   # user_id : timestamp of last earn
message_counts = {}  # user_id : messages since last earn
chao_message_counts = {}  # user_id : messages to age up Chao
last_chao_message = {}  # user_id : timestamp
last_message_times = {}  # user_id : last message time for aging

EXEMPT_GUILD_ID = 1407517059797094535
EXEMPT_CHANNEL_ID = 1407521665276969001

def load_config():
    try:
        out = {}
        for doc in db.collection("config").stream():
            out[doc.id] = doc.to_dict() or {}
        return out
    except Exception as e:
        print(f"[WARN] Failed to load config from Firestore: {e}")
        return {}
    try:
        out = {}
        for doc in db.collection("config").stream():
            out[doc.id] = doc.to_dict() or {}
        return out
    except Exception as e:
        print(f"[WARN] Failed to load config from Firestore: {e}")
        return {}

def save_config():
    try:
        batch = db.batch()
        for gid, payload in (config or {}).items():
            ref = db.collection("config").document(str(gid))
            batch.set(ref, payload)
        batch.commit()
    except Exception as e:
        print(f"[ERROR] Failed to save config to Firestore: {e}")

try:
    user_chao_data = {doc.id: (doc.to_dict() or {}) for doc in db.collection("chao_data").stream()}
except Exception:
    user_chao_data = {}
changed = False
for user in user_chao_data.values():
    chao_list = user.get("chao", [])
    for chao in chao_list:
        if "favourite_fruit" not in chao:
            chao["favourite_fruit"] = random.choice(["Triangle Fruit", "Square Fruit", "Round Fruit"])
            changed = True
if changed:
    # Save only changed users (all users changed here during migration)
    save_user_chao_data()

# --- Per-user save debounce queue ---
# pending_chao_saves stores user IDs that need to be flushed
pending_chao_saves = set()
# last_chao_save_time records the last firestore write timestamp per user
last_chao_save_time = {}

def enqueue_chao_save(user_id):
    """Mark a user's chao data to be saved on the next flush tick."""
    try:
        pending_chao_saves.add(str(user_id))
    except Exception:
        pass


def flush_pending_chao_saves():
    """Synchronously flush pending per-user chao saves to Firestore.
    This is used during shutdown to ensure no pending changes are lost.
    """
    if not pending_chao_saves:
        return
    to_flush = list(pending_chao_saves)
    pending_chao_saves.clear()
    try:
        save_user_chao_data(to_flush)
        now = time.time()
        for uid in to_flush:
            last_chao_save_time[uid] = now
        print(f"[CHAO-FLUSH] Flushed {len(to_flush)} user(s) to Firestore (sync).")
    except Exception as e:
        print(f"[ERROR] Failed to flush pending chao saves synchronously: {e}")


@tasks.loop(seconds=60)
async def chao_save_flush_task():
    """Flush pending per-user chao saves every 60 seconds with simple debounce."""
    if not pending_chao_saves:
        return
    # Snapshot and clear set to avoid blocking enqueues
    to_flush = list(pending_chao_saves)
    pending_chao_saves.clear()

    # Optionally filter users that were saved <60s ago
    now = time.time()
    final_flush = []
    for uid in to_flush:
        last = last_chao_save_time.get(uid, 0)
        if now - last >= 55:  # allow ~1 min interval
            final_flush.append(uid)

    if not final_flush:
        return

    try:
        save_user_chao_data(final_flush)
        for uid in final_flush:
            last_chao_save_time[uid] = now
        print(f"[CHAO-FLUSH] Flushed {len(final_flush)} user(s) to Firestore.")
    except Exception as e:
        print(f"[ERROR] Failed to flush per-user chao saves: {e}")

config = load_config()

try:
    user_inventory = {doc.id: (doc.to_dict() or {}) for doc in db.collection("user_inventory").stream()}
except Exception:
    user_inventory = {}


colour_map = {
    "blue": discord.Color.blue(),
    "green": discord.Color.green(),
    "red": discord.Color.red(),
    "black": discord.Color.dark_gray(),
    "yellow": discord.Color.gold(),
    "white": discord.Color.light_gray(),
    "purple": discord.Color.purple()
}

CHAO_EMOJI_FILE = os.path.join(json_dir, "chao_emojis.json")
chao_emojis = {}  # This will always have the latest emojis

def load_chao_emojis():
    global chao_emojis
    try:
        with open(CHAO_EMOJI_FILE, "r", encoding="utf-8") as f:
            chao_emojis = json.load(f)
        print(f"Chao emojis loaded at {time.strftime('%X')}")
    except Exception as e:
        print(f"Failed to load chao_emojis.json: {e}")

# Initial load
load_chao_emojis()

class EmojiFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if os.path.abspath(event.src_path) == os.path.abspath(CHAO_EMOJI_FILE):
            load_chao_emojis()

def start_emoji_watcher():
    global emoji_observer
    emoji_observer = Observer()
    handler = EmojiFileHandler()
    emoji_observer.schedule(handler, json_dir, recursive=False)
    emoji_observer.start()
    return emoji_observer


@bot.event
async def on_ready():
    global slash_commands_synced

    print(f"[EVENT] on_ready: Bot {bot.user} is now online.")

    if not slash_commands_synced:
        synced_commands = await bot.tree.sync()
        slash_commands_synced = True
        print(f"Synced {len(synced_commands)} slash commands.")
    
    # Start your tasks
    chao_encounter_event.start()
    chao_aging_task.start()
    energy_regen_task.start()
    # start and keep reference to emoji observer
    try:
        start_emoji_watcher()
    except Exception as e:
        print(f"[WARN] Failed to start emoji watcher: {e}")
    print("Chao encounter event started.")
    print("Daily Chao aging event started.")
    print("Chao emojis auto-reload task started.")

    # System info
    system_info = platform.uname()
    print(f"Bot running on system: {system_info.system}, node name: {system_info.node}")
    
    username = getpass.getuser()
    print(f"Username: {username}")

    # --- Set bot status if username is not 'sbrr' ---
    if username.lower() != 'sbrr':
        activity = discord.Game(name="Backup system - expect slow responses.")
        # activity = discord.Game(name=f"Running on {system_info.node}, {system_info.system}.")
        # activity = discord.Game(name=f"Running on {system_info.node}, {system_info.system}.")
        await bot.change_presence(status=discord.Status.online, activity=activity)
        print("Bot status set due to backup PC.")
    else:
        print("Username is 'sbrr', bot status not changed.")

    # Start age queue flush task
    try:
        # Ensure queue is loaded into memory first
        try:
            load_local_age_queue()
            print(f"Local age queue loaded: {len(local_age_queue)} entries.")
        except Exception as e:
            print(f"[WARN] Failed to pre-load local age queue: {e}")

        age_queue_flush_task.start()
        print("Local age queue flush task started.")
        # Start chao per-user save flush task
        try:
            chao_save_flush_task.start()
            print("Chao per-user save flush task started.")
        except Exception as e:
            print(f"[WARN] Could not start chao_save_flush_task: {e}")
    except Exception as e:
        print(f"[WARN] Could not start age_queue_flush_task: {e}")



    # Check all guilds for configured channels
    for guild in bot.guilds:
        guild_id = str(guild.id)
        guild_config = config.get(guild_id)
        channel = None

        if not guild_config or "channel_id" not in guild_config:
            print(f"No channel set for guild {guild.name} ({guild.id})")
        else:
            # Check if the saved channel still exists
            channel_id = guild_config["channel_id"]
            channel = guild.get_channel(channel_id)
            if not channel:
                print(f"Configured channel {channel_id} for guild {guild.name} no longer exists!")
                channel = None

        # Send a message if no valid channel
        if not channel:
            # Try system channel
            if guild.system_channel:
                await guild.system_channel.send(
                    "<:twisty:1407499302301073418> No valid Chao channel is set for this server! "
                    "Use `/setchannel` or `!setchannel` in the channel you want Chao events to appear."
                )
                print(f"Reminder message sent in system channel for guild {guild.name}")
            else:
                print(f"No valid channel to send message in guild {guild.name} ({guild.id})")


@bot.event
async def on_guild_join(guild):
    # Find a text channel where the bot has permission to send messages
    channel = None
    for c in guild.text_channels:
        if c.permissions_for(guild.me).send_messages:
            channel = c
            break

    # Send a welcome message
    if channel:
        await channel.send(
            f"Hello **{guild.name}**! <:happychao:1407671142290034829>\n"
            "Use `/commandslist` or `!commandslist` to list all available commands. "
            "Set a channel for Chao events using `/setchannel` or `!setchannel`!"
        )

    # (Optional) Log it in your console
    print(f"Joined a new server: {guild.name} (ID: {guild.id})")


# Passive coin system for messages
@bot.event
async def on_message(message):
    # print(f"[EVENT] on_message: {message.author} in {message.channel} - '{message.content}'")
    if message.author.bot:
        return  # Ignore other bots

    # Skip empty messages
    if not message.content or message.content.strip() == "":
        return

    user_id = message.author.id
    now = time.time()

    # --- Skip messages that are commands but still process them ---
    if message.content.startswith(bot.command_prefix):
        await bot.process_commands(message)
        return  # Don't give passive coins or count messages for commands

    # --- Passive coin logic ---
    cooldown = 300  # seconds
    required_messages = 20
    message_counts[user_id] = message_counts.get(user_id, 0) + 1

    if (
        user_id not in last_earned
        or (now - last_earned[user_id] >= cooldown and message_counts[user_id] >= required_messages)
    ):
        amount = random.randint(5, 20)
        add_balance(user_id, amount)
        last_earned[user_id] = now
        message_counts[user_id] = 0
        emote = [
            "<a:chaosweeping:1407516449911869632>",
            "<a:chaocar:1407515795608834210>",
            "<a:chaosonicchaodance:1407507437010944121>"
        ]
        await message.channel.send(
            f"-# {random.choice(emote)} {message.author.display_name} earned {amount} rings for being active!"
        )

    # --- Chao aging + egg hatching (15 seconds restriction) ---
    if str(user_id) in user_chao_data and isinstance(user_chao_data[str(user_id)], dict) and "chao" in user_chao_data[str(user_id)]:
        last_msg_time = last_message_times.get(user_id, 0)
        if now - last_msg_time >= 15:  # 15 seconds cooldown
            for chao in user_chao_data[str(user_id)]["chao"]:
                # --- Handle Egg ---
                if chao.get("is_egg", False):
                    chao["messages_until_hatch"] = chao.get("messages_until_hatch", 20) - 1

                    if chao["messages_until_hatch"] <= 0 or now >= chao.get("hatch_time", now + 1):
                        chao["is_egg"] = False
                        old_name = chao.get("nickname", "egg")

                        # --- Assign nickname if not a reincarnation ---
                        if chao.get("reincarnations", 0) == 0:
                            colour = chao.get("colour", "Unknown").capitalize()
                            ctype = chao.get("type_", chao.get("type", "Unknown")).capitalize()
                            chao["nickname"] = f"{colour} {ctype} Chao"
                        elif not chao.get("nickname"):
                            # fallback if no nickname exists
                            chao["nickname"] = f"{chao.get('colour', 'Unknown').capitalize()} {chao.get('type_', chao.get('type', 'Unknown')).capitalize()} Chao"

                        # cleanup
                        chao.pop("messages_until_hatch", None)
                        chao.pop("hatch_time", None)

                        #await message.channel.send(
                        #   f"<:chaoegg:1408215309592035329>✨ Your egg has hatched into a **{chao['nickname']}**!"
                        #)


                else:
                    # --- Normal chao growth ---
                    chao["messages"] = chao.get("messages", 0) + 1

            last_message_times[user_id] = now

            # Save Chao after increment (debounced per-user)
            enqueue_chao_save(user_id)

    # --- Always process commands at the end ---
    await bot.process_commands(message)


def get_chao_emoji(chao):
    """Return the correct emoji for a Chao based on colour and type."""
    # Handle egg separately
    if chao.get("is_egg", False):
        return chao_emojis.get("egg", "<:chaoegg:1408215309592035329>")

    colour = chao.get("colour", "default").lower()
    ctype = chao.get("type", "monotone").lower()  # default to monotone

    colour_data = chao_emojis.get(colour, {})

    # Lookup colour → type
    emoji = colour_data.get(ctype, None)

    # Fallback to default monotone
    if not emoji:
        emoji = chao_emojis.get("default", {}).get("monotone", "<:happychao:1407671142290034829>")

    return emoji

## Inventory already loaded from Firestore above
## Inventory already loaded from Firestore above

def get_user_inventory(user_id):
    user_id = str(user_id)
    return user_inventory.get(user_id, {})

def save_user_inventory(user_ids=None):
    try:
        if not user_inventory:
            return
        if user_ids is None:
            batch = db.batch()
            for uid, inv in (user_inventory or {}).items():
                ref = db.collection("user_inventory").document(str(uid))
                batch.set(ref, inv)
            batch.commit()
            return

        if isinstance(user_ids, (str, int)):
            user_ids = [str(user_ids)]
        else:
            user_ids = [str(u) for u in user_ids]

        batch = db.batch()
        for uid in user_ids:
            inv = user_inventory.get(str(uid), {})
            ref = db.collection("user_inventory").document(str(uid))
            batch.set(ref, inv)
        batch.commit()
    except Exception as e:
        print(f"[ERROR] Failed to save user_inventory to Firestore: {e}")

def add_to_inventory(user_id, item_name, quantity=1):
    user_id = str(user_id)
    if user_id not in user_inventory:
        user_inventory[user_id] = {}
    if item_name in user_inventory[user_id]:
        user_inventory[user_id][item_name] += quantity
    else:
        user_inventory[user_id][item_name] = quantity
    save_user_inventory()

def get_inventory(user_id):
    return user_inventory.get(str(user_id), {})


# --- Egg helpers ---
def build_egg(colour: str, egg_type: str) -> dict:
    """Create an egg dict with default hatch settings."""
    egg_colour = (colour or "Blue")
    egg_type_ = (egg_type or "monotone")
    egg_nickname = f"{egg_colour.capitalize()} {egg_type_.capitalize()} Chao"
    return {
        "nickname": egg_nickname,
        "colour": egg_colour,
        "type": egg_type_,
        "age": 0,
        "alignment": 0,
        "is_egg": True,
        "messages_until_hatch": 20,
        "hatch_time": time.time() + 1200,
        "favourite_fruit": random.choice(["Triangle Fruit", "Square Fruit", "Round Fruit"])
    }

def add_egg_to_user(user_id: str, egg: dict):
    """Append an egg to the user's chao list and persist."""
    user_id = str(user_id)
    if user_id not in user_chao_data or not isinstance(user_chao_data[user_id], dict):
        user_chao_data[user_id] = {"username": None, "chao": []}
    user_chao_data[user_id].setdefault("chao", []).append(egg)
    enqueue_chao_save(user_id)


def user_shares_another_bot_guild(user_id, excluded_guild_id=None):
    """Return whether a user is still in another guild containing the bot."""
    return any(
        guild.id != excluded_guild_id and guild.get_member(user_id) is not None
        for guild in bot.guilds
    )


@bot.event
async def on_member_remove(member):
    print(f"[EVENT] on_member_remove: {member.display_name} ({member.id}) left.")
    user_id = str(member.id)

    if (
        user_id in balances
        and not user_shares_another_bot_guild(
            member.id, excluded_guild_id=member.guild.id
        )
    ):
        add_balance(user_id, -200)


from discord.ext import commands

@bot.hybrid_command()
@commands.has_permissions(administrator=True)
async def setchannel(ctx):
    """Sets the current channel for Chao events in this server. Administrator only."""
    print(f"[COMMAND] !setchannel by {ctx.author} in {ctx.channel}")
    guild_id = str(ctx.guild.id)
    channel_id = ctx.channel.id

    config[guild_id] = {"channel_id": channel_id}
    save_config()
    await ctx.send(f"Channel set for this server: {ctx.channel.name}")

@setchannel.error
async def setchannel_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("<:twisty:1408217575816691835> You need **Administrator** permissions to use this command.")


@bot.hybrid_command()
@app_commands.default_permissions(administrator=True)
async def shutdown(ctx):
    """Shuts Chaobot down cleanly."""
    if ctx.author.id != FOUNDER_ID:
        return await ctx.send("<:twisty:1407499302301073418> Only the bot founder can shut me down.")
    print("Shutting down: flushing age queue and saving state...")
    try:
        # flush any due entries now
        await flush_age_queue_once()
    except Exception as e:
        print(f"[WARN] flush_age_queue_once on shutdown failed: {e}")

    # Save data stores
    try:
        # Persist any pending per-user chao saves synchronously
        flush_pending_chao_saves()
    except Exception as e:
        print(f"[WARN] flush_pending_chao_saves on shutdown failed: {e}")
    try:
        save_balances()
    except Exception as e:
        print(f"[WARN] save_balances on shutdown failed: {e}")
    try:
        save_user_inventory()
    except Exception as e:
        print(f"[WARN] save_user_inventory on shutdown failed: {e}")

    # Stop tasks
    try:
        chao_encounter_event.cancel()
        chao_aging_task.cancel()
        energy_regen_task.cancel()
        age_queue_flush_task.cancel()
    except Exception as e:
        print(f"[WARN] Failed to cancel tasks: {e}")

    # Stop emoji watcher if present
    try:
        if 'emoji_observer' in globals() and emoji_observer:
            emoji_observer.stop()
            emoji_observer.join(timeout=2)
    except Exception as e:
        print(f"[WARN] Stopping emoji_observer failed: {e}")

    await ctx.send("See you later! <:sadchaoo:1408217554064773291>")
    await bot.close()


@bot.event
async def on_command_error(ctx, error):
    """Global command error handler for common cases like MemberNotFound."""
    if isinstance(error, commands.MemberNotFound):
        await ctx.send(
            "<:fa:1408217566660395028> I couldn't find that user. "
            "Try mentioning them (e.g., @User) or use their ID."
        )
        return
    # Let other errors bubble up to default handler/logs
    raise error



script_dir = os.path.dirname(os.path.abspath(__file__))
chao_folder = os.path.join(script_dir, "botimages", "wildchao")

ADULT_IMAGE = os.path.join(script_dir, "botimages", "cocoon.png")  # image for adult Chao

def get_random_chao():
    # Pick a random file from wildchao folder
    files = [
        f for f in os.listdir(chao_folder)
        if f.lower().endswith(".png")
        and not f.startswith("._")  # ignore macOS AppleDouble files
        and not f.startswith(".")    # ignore hidden files like .DS_Store
        and os.path.isfile(os.path.join(chao_folder, f))
    ]
    if not files:
        raise RuntimeError(f"No valid .png files found in {chao_folder}")
    files = [
        f for f in os.listdir(chao_folder)
        if f.lower().endswith(".png")
        and not f.startswith("._")  # ignore macOS AppleDouble files
        and not f.startswith(".")    # ignore hidden files like .DS_Store
        and os.path.isfile(os.path.join(chao_folder, f))
    ]
    if not files:
        raise RuntimeError(f"No valid .png files found in {chao_folder}")
    chosen_file = random.choice(files)
    
    # Full path to the image
    image_path = os.path.join(chao_folder, chosen_file)

    # Extract parts from filename
    name = os.path.splitext(chosen_file)[0]  # remove .png
    parts = name.split("_")  # e.g. "Neutral_Normal_TwoTone"

    alignment_ = parts[0] if len(parts) > 0 else "Unknown"
    colour  = parts[1] if len(parts) > 1 else "Unknown"
    type_   = parts[2] if len(parts) > 2 else "Unknown"

    return alignment_, colour, type_, image_path, chosen_file


current_wild_chao = {}

@tasks.loop(minutes=random.randint(10, 60))
async def chao_encounter_event():
    await bot.wait_until_ready()  # Ensure bot is ready

    # --- Wait a random initial delay before first spawn ---
    initial_delay = random.randint(10, 60) * 60  # convert minutes to seconds
    await asyncio.sleep(initial_delay)

    while True:
        print("[TASK] chao_encounter_event tick.")
        print("Spawning new Chao!")

        for guild_id, guild_config in config.items():
            guild_id = int(guild_id)
            channel_id = guild_config.get("channel_id")
            channel = bot.get_channel(int(channel_id)) if channel_id else None

            if channel:
                alignment_, colour, type_, image_path, chosen_file = get_random_chao()

                current_wild_chao[channel.id] = {
                    "alignment_": alignment_,
                    "colour": colour,
                    "type": type_,
                    "image_path": image_path,
                    "filename": chosen_file
                }

                ctype_lower = type_.lower()
                colour_lower = colour.lower()
                emoji = chao_emojis.get(colour_lower, {}).get(ctype_lower, chao_emojis["default"]["monotone"])

                embed = discord.Embed(
                    title=f"{emoji} A Chao has entered your garden!",
                    description="Type **!chao** to interact with it!",
                    color=colour_map.get(colour_lower, discord.Color.blue())
                )
                embed.set_image(url=f"attachment://{chosen_file}")

                await channel.send(
                    embed=embed,
                    file=discord.File(image_path, filename=chosen_file)
                )

        # Wait a new random interval before next spawn
        await asyncio.sleep(random.randint(10, 60) * 60)


# Bot Commands
@bot.hybrid_command()
async def chao(ctx):
    """Catches any present wild Chao in the channel."""
    print(f"[COMMAND] !chao by {ctx.author} in {ctx.channel}")
    
    channel_id = ctx.channel.id

    # Check if a Chao is currently available
    if channel_id not in current_wild_chao:
        await ctx.send("<:fa:1408217566660395028> No wild Chao to catch right now!")
        return

    wild_chao = current_wild_chao[channel_id]

    # Get correct emoji using colour + type
    colour = wild_chao["colour"].lower()
    ctype = wild_chao.get("type", "monotone").lower()
    emoji = chao_emojis.get(colour, {}).get(ctype, "<:happychao:1407671142290034829>")

    # Prompt the user for a nickname
    await ctx.send(
        f"{emoji} You found a(n) {wild_chao['colour']} {wild_chao['type']} Chao! "
        "What nickname do you want to give it?"
    )

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel

    try:
        msg = await bot.wait_for("message", check=check, timeout=60)
        nickname = msg.content.strip()
    except asyncio.TimeoutError:
        await ctx.send("You took too long! The Chao ran away... <:sadchao:1407501406260297869>")
        return

    # Prepare Chao data
    chao_info = {
        "nickname": nickname,
        "alignment_": wild_chao["alignment_"],
        "colour": wild_chao["colour"],
        "type": wild_chao["type"],
        "filename": wild_chao["filename"],
        "image_path": wild_chao["image_path"],
        "age": 0,  # 0 = newborn, 1 = child, 2 = adult
        "last_aged": time.time(),  # timestamp of last age check
        "message_count": 0,  # counts messages toward aging
        "favourite_fruit": random.choice(["Triangle Fruit", "Square Fruit", "Round Fruit"])
    }

    user_id = str(ctx.author.id)

    if user_id not in user_chao_data or not isinstance(user_chao_data[user_id], dict):
        user_chao_data[user_id] = {"username": ctx.author.display_name, "chao": []}
    user_chao_data[user_id]["username"] = ctx.author.display_name
    user_chao_data[user_id]["chao"].append(chao_info)

    # Save to Firestore (debounced)
    enqueue_chao_save(user_id)

    # Remove the wild Chao from the channel
    del current_wild_chao[channel_id]

    await ctx.send(
        f"{emoji} Congratulations! **{nickname}** the {chao_info['colour']} {chao_info['type']} has joined your garden!"
    )


# Alignment emojis defined once at the top of your bot script
ALIGNMENT_EMOJIS = {
    "neutral": "<:bubblenew:1408217651116773376>",
    "dark": "<:spikenew:1408217572733882509>",
    "hero": "<:halonew:1408217567943987380> "
}

@bot.hybrid_command()
async def mychao(ctx):
    """Lists all Chao owned by the user."""
    print(f"[COMMAND] !mychao by {ctx.author} in {ctx.channel}")
    user_id = str(ctx.author.id)
    if user_id not in user_chao_data or not isinstance(user_chao_data[user_id], dict) or not user_chao_data[user_id].get("chao"):
        return await ctx.send("You don't have any Chao yet!")

    chao_list = user_chao_data[user_id]["chao"]
    per_page = 5
    pages = (len(chao_list) - 1) // per_page + 1
    current_page = 0

    def make_embed(page):
        embed = discord.Embed(
            title=f"{ctx.author.display_name}'s Chao Garden (Page {page+1}/{pages})",
            color=discord.Color.green()
        )
        start, end = page * per_page, (page + 1) * per_page
        for i, chao in enumerate(chao_list[start:end], start=start+1):
            if chao.get("is_egg", False):
                # Egg handling
                nickname = "Egg"
                emoji = chao_emojis.get("egg", "<:chaoegg:1408215309592035329>")
                age_text = "Egg"
                alignment_text = "-"
                semoji = ""
                colour_text = "-"
                type_text = "-"
                reinc_text = chao.get("reincarnations", 0)
            else:
                # Chao handling
                nickname = chao.get("nickname", "Unknown")
                alignment_text = chao.get("alignment_", "Unknown")
                colour_text = chao.get("colour", "Unknown")
                type_text = chao.get("type", "Unknown")
                age_val = chao.get("age", "Unknown")
                age_text = (
                    ["Baby", "Child", "Adult"][age_val]
                    if isinstance(age_val, int) and 0 <= age_val <= 2
                    else "Unknown"
                )
                emoji = get_chao_emoji(chao)  # uses your new function
                # Determine alignment emoji
                age_val = chao.get("age", "Unknown")
                if isinstance(age_val, int) and age_val == 2:  # Adult
                    semoji = ALIGNMENT_EMOJIS.get(alignment_text.lower(), "❓")
                else:
                    semoji = "❓"

                reinc_text = chao.get("reincarnations", 0)

            embed.add_field(
                name=f"{i}. {nickname} {emoji}",
                value=(
                    f"Alignment: {alignment_text if not chao.get('is_egg', False) else '-'} "
                    f"{semoji if not chao.get('is_egg', False) else ''}\n"
                    f"Colour: {colour_text}\n"
                    f"Type: {type_text}\n"
                    f"Age: {age_text}\n"
                    f"Reincarnations: {reinc_text}"
                ),
                inline=False
            )
        embed.set_footer(
            text=f"Total Chao: {len(chao_list)}\n"
                 f"Note: Baby Chao (those under the age of 2) may align differently as they age."
        )
        return embed

    message = await ctx.send(embed=make_embed(current_page))
    if pages == 1:
        return  # no need for reactions if only 1 page

    # Add reaction arrows
    await message.add_reaction("⬅️")
    await message.add_reaction("➡️")

    def check(reaction, user):
        return (
            user == ctx.author
            and str(reaction.emoji) in ["⬅️", "➡️"]
            and reaction.message.id == message.id
        )

    while True:
        try:
            reaction, user = await bot.wait_for(
                "reaction_add", timeout=60.0, check=check
            )
            if str(reaction.emoji) == "➡️" and current_page < pages - 1:
                current_page += 1
                await message.edit(embed=make_embed(current_page))
            elif str(reaction.emoji) == "⬅️" and current_page > 0:
                current_page -= 1
                await message.edit(embed=make_embed(current_page))
            await message.remove_reaction(reaction, user)
        except asyncio.TimeoutError:
            break




@bot.hybrid_command()
async def spawnchao(ctx):
    """Manually spawn a wild Chao in the configured channel (or current if none set). Owner only."""
    print(f"[COMMAND] !spawnchao by {ctx.author} in {ctx.channel}")

    # --- Restrict to bot owner only ---
    if ctx.author.id != FOUNDER_ID:
        await ctx.send("<:twisty:1408217575816691835> Only the bot owner can use this command.")
        return

    guild_id = str(ctx.guild.id)
    # Get the configured channel ID from config (if any)
    target_channel_id = config.get(guild_id, {}).get("channel_id", ctx.channel.id)
    target_channel = ctx.guild.get_channel(target_channel_id)

    if not target_channel:
        await ctx.send("<:fa:1408217566660395028> Configured channel not found. Using this channel instead.")
        target_channel = ctx.channel

    # Get a random Chao
    alignment_, colour, type_, image_path, chosen_file = get_random_chao()

    # Update current wild Chao for this channel
    current_wild_chao[target_channel.id] = {
        "alignment_": alignment_,
        "colour": colour,
        "type": type_,
        "image_path": image_path,
        "filename": chosen_file
    }

    # --- Get correct emoji ---
    ctype_lower = type_.lower()
    colour_lower = colour.lower()
    emoji = chao_emojis.get(colour_lower, {}).get(ctype_lower, chao_emojis["default"]["monotone"])

    # --- Create embed ---
    embed = discord.Embed(
        title=f"{emoji} A Chao has entered your garden! [Test]",
        description="Type **!chao** to interact with it!",
        color=colour_map.get(colour_lower, discord.Color.blue())
    )
    embed.set_image(url=f"attachment://{chosen_file}")

    # Send to the configured channel (or fallback channel)
    await target_channel.send(
        embed=embed,
        file=discord.File(image_path, filename=chosen_file)
    )

    # Optional confirmation if executed in a different channel
    if target_channel.id != ctx.channel.id:
        await ctx.send(f"✅ Test Chao spawned in {target_channel.mention}.")



# --- Helper Functions ---
def save_balances(user_ids=None):
    try:
        if not balances:
            return
        if user_ids is None:
            batch = db.batch()
            for uid, bal in (balances or {}).items():
                ref = db.collection("balances").document(str(uid))
                batch.set(ref, {"balance": int(bal)})
            batch.commit()
            return

        if isinstance(user_ids, (str, int)):
            user_ids = [str(user_ids)]
        else:
            user_ids = [str(u) for u in user_ids]

        batch = db.batch()
        for uid in user_ids:
            bal = int(balances.get(str(uid), 0))
            ref = db.collection("balances").document(str(uid))
            batch.set(ref, {"balance": bal})
        batch.commit()
    except Exception as e:
        print(f"[ERROR] Failed to save balances to Firestore: {e}")

def get_balance(user_id):
    uid = str(user_id)
    if uid in balances:
        return balances.get(uid, 0)
    try:
        snap = db.collection("balances").document(uid).get()
        if snap.exists:
            val = int((snap.to_dict() or {}).get("balance", 0))
            balances[uid] = val
            return val
    except Exception:
        pass
    return 0
    uid = str(user_id)
    if uid in balances:
        return balances.get(uid, 0)
    try:
        snap = db.collection("balances").document(uid).get()
        if snap.exists:
            val = int((snap.to_dict() or {}).get("balance", 0))
            balances[uid] = val
            return val
    except Exception:
        pass
    return 0

def add_balance(user_id, amount):
    uid = str(user_id)
    try:
        amt = int(amount)
    except Exception:
        # invalid amount; ignore
        return

    ref = db.collection("balances").document(uid)

    @firestore.transactional
    def do_txn(transaction, doc_ref, delta):
        snap = doc_ref.get(transaction=transaction)
        cur = 0
        if snap.exists:
            try:
                cur = int((snap.to_dict() or {}).get("balance", 0))
            except Exception:
                cur = 0
        new_bal = cur + delta
        # Prevent balances going negative: clamp to 0
        if new_bal < 0:
            new_bal = 0
        transaction.set(doc_ref, {"balance": new_bal})
        return new_bal

    try:
        transaction = db.transaction()
        new_bal = do_txn(transaction, ref, amt)
        # Update in-memory cache to the authoritative value from the transaction
        balances[uid] = int(new_bal)
    except Exception as e:
        print(f"[ERROR] Failed to update balance for {uid}: {e}")
        # Best-effort fallback: update cache without going negative
        try:
            balances[uid] = max(0, get_balance(uid) + amt)
        except Exception:
            balances[uid] = max(0, balances.get(uid, 0) + (amt if isinstance(amt, int) else 0))
    ref = db.collection("balances").document(uid)
    @firestore.transactional
    def do_txn(transaction, doc_ref, delta):
        snap = doc_ref.get(transaction=transaction)
        cur = 0
        if snap.exists:
            cur = int((snap.to_dict() or {}).get("balance", 0))
        transaction.set(doc_ref, {"balance": cur + delta})
    try:
        transaction = db.transaction()
        do_txn(transaction, ref, amt)
    except Exception as e:
        print(f"[ERROR] Failed to update balance for {uid}: {e}")

# --- Load balances ---
balances = {}
try:
    for doc in db.collection("balances").stream():
        try:
            balances[doc.id] = int((doc.to_dict() or {}).get("balance", 0))
        except Exception:
            balances[doc.id] = 0
except Exception as e:
    print(f"[WARN] Failed to load balances from Firestore: {e}")
balances = {}
try:
    for doc in db.collection("balances").stream():
        try:
            balances[doc.id] = int((doc.to_dict() or {}).get("balance", 0))
        except Exception:
            balances[doc.id] = 0
except Exception as e:
    print(f"[WARN] Failed to load balances from Firestore: {e}")

async def age_chao(user_id, chao_index, guild=None):
    """Ages a user's chao by 1 stage if eligible"""
    chao = user_chao_data[user_id]["chao"][chao_index]
    if chao["age"] >= 2:
        return  # Already adult

    chao["age"] += 1
    chao["message_count"] = 0
    chao["last_aged"] = time.time()

    # Save data
    enqueue_chao_save(user_id)

    # Only notify when reaching adult (age == 2)
    if chao["age"] == 2:
        # Determine the channel to send the message
        guild_id = str(guild.id)
        chao_channel_id = config.get(guild_id, {}).get("channel_id")
        chao_channel = bot.get_channel(chao_channel_id) if chao_channel_id else None

        # DM if no channel
        if chao_channel:
            user = guild.get_member(int(user_id))
            await chao_channel.send(
                f"{chao_emojis} {user.mention}, your Chao **{chao['nickname']}** has grown into an **adult**!"
            )


REINCARNATION_MESSAGES = 500  # Messages after adult before reincarnation

@tasks.loop(seconds=60)
async def chao_aging_task():
    try:
        global checking
        now = time.time()  # use timestamps consistently

        changed_users = set()
        for user_id, user_data in user_chao_data.items():
            if not isinstance(user_data, dict) or "chao" not in user_data or not isinstance(user_data["chao"], list):
                print(f"[WARN] Malformed chao data for user {user_id}, skipping.")
                continue
            try:
                member = await bot.fetch_user(int(user_id))
            except Exception as e:
                print(f"[ERROR] Could not fetch user {user_id}: {e}")
                continue
            # Snapshot user data to detect changes during processing
            try:
                before = json.dumps(user_data, sort_keys=True)
            except Exception:
                before = None

            for chao in user_data["chao"]:
                try:
                    if not isinstance(chao, dict):
                        print(f"[WARN] Malformed chao entry for user {user_id}, skipping.")
                        continue
                    # --- Initialize missing attributes ---
                    chao.setdefault("age", 0)
                    chao.setdefault("messages", 0)
                    chao.setdefault("last_age_time", now)
                    chao.setdefault("alignment", 0)
                    chao.setdefault("alignment_", "Neutral")
                    chao.setdefault("reincarnations", 0)
                    # Ensure energy exists and track last energy timestamp for gradual regen
                    chao.setdefault("energy", 100)
                    # Hunger: 0% = full, 100% = starving. Ensure default exists.
                    chao.setdefault("hunger", 0)
                    chao.setdefault("last_energy_update", now)

                    aged = False

                    # --- Auto-hatch stuck eggs ---
                    if chao.get("is_egg", False) and chao.get("hatch_time", 0) <= now:
                        chao["is_egg"] = False
                        chao["age"] = 0
                        chao["last_age_time"] = now
                        chao_name = chao.get("nickname", "Unnamed")
                        print(f"[FIX] Chao '{chao_name}' for user {user_id} auto-hatched.")

                        # Send DMhg
                        try:
                            await member.send(f"<:chaoegg:1408215309592035329> Your Chao has hatched into **{chao_name}**!")
                        except discord.Forbidden:
                            print(f"[WARN] Cannot DM user {user_id} (DMs closed).")
                        except Exception as e:
                            print(f"[ERROR] Failed to DM user {user_id}: {e}")

                        # Send guild message if possible
                        guild_id = str(chao.get("guild_id", None))
                        if guild_id and guild_id in config:
                            channel_id = config[guild_id].get("channel_id")
                            if channel_id:
                                try:
                                    channel = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
                                    await channel.send(f"<:chaoegg:1408215309592035329>✨ <@{user_id}>'s Chao has hatched into **{chao_name}**!")
                                except discord.Forbidden:
                                    print(f"[WARN] Cannot send hatch message in guild {guild_id}.")
                                except Exception as e:
                                    print(f"[ERROR] Sending to guild channel failed: {e}")
                        # Enqueue a flush for this user's chao an hour from now instead of immediate save
                        try:
                            enqueue_age_write(user_id, chao_index=None, scheduled_time=now + 3600)
                        except Exception as e:
                            print(f"[WARN] Failed to enqueue age write after hatch for user {user_id}: {e}")
                    # --- Age by time (8h) ---
                    last_age_time = chao.get("last_age_time", now)
                    if (now - last_age_time) >= 8 * 3600 and chao["age"] < 2:
                        chao["age"] += 1
                        chao["last_age_time"] = now
                        chao["messages"] = 0
                        aged = True
                        print(f"[AGE-UP] {user_id}'s Chao '{chao.get('nickname', 'Unnamed')}' is now age {chao['age']}.")
                        # Enqueue a flush rather than saving immediately
                        try:
                            enqueue_age_write(user_id, chao_index=None, scheduled_time=now + 3600)
                        except Exception as e:
                            print(f"[WARN] Failed to enqueue age write for user {user_id}: {e}")

                    # --- Age by messages ---
                    messages_needed = max(10, 100 - chao.get("messages_needed_modifier", 0))
                    if chao["messages"] >= messages_needed and chao["age"] < 2:
                        chao["age"] += 1
                        chao["messages"] = 0
                        chao["last_age_time"] = now
                        chao["messages_needed_modifier"] = 0
                        aged = True
                        print(f"[AGE-UP] {user_id}'s Chao '{chao.get('nickname', 'Unnamed')}' is now age {chao['age']}.")
                        try:
                            enqueue_age_write(user_id, chao_index=None, scheduled_time=now + 3600)
                        except Exception as e:
                            print(f"[WARN] Failed to enqueue age write for user {user_id}: {e}")

                    # --- Adult Hero/Dark transformation ---
                    if aged and chao["age"] == 2 and chao["alignment_"] == "Neutral":
                        if chao["alignment"] == 10:
                            chao["alignment_"] = "Hero"
                        elif chao["alignment"] == -10:
                            chao["alignment_"] = "Dark"
                        else:
                            if chao["alignment"] > 0 and random.random() <= (chao["alignment"] / 10):
                                chao["alignment_"] = "Hero"
                                chao["alignment"] = 10
                            elif chao["alignment"] < 0 and random.random() <= (-chao["alignment"] / 10):
                                chao["alignment_"] = "Dark"
                                chao["alignment"] = -10

                    # --- Determine announcement channel ---
                    channel = None
                    try:
                        for guild in bot.guilds:
                            if guild.get_member(member.id):
                                guild_config = config.get(str(guild.id))
                                if guild_config and guild_config.get("channel_id"):
                                    channel = bot.get_channel(int(guild_config["channel_id"])) or await bot.fetch_channel(int(guild_config["channel_id"]))
                                    break
                        if not channel:
                            channel = await member.create_dm()
                    except Exception as e:
                        print(f"[ERROR] Could not fetch channel for user {user_id}: {e}")
                        continue

                    # --- Notify adult growth ---
                    if aged and chao["age"] == 2:
                        chao_colour = chao.get("colour", "Unknown")
                        chao_type = chao.get("type", "Chao")
                        title = f"<:happychao:1407671142290034829> {chao.get('nickname', 'Your Chao')} has grown into an Adult Chao!"
                        description = f"<@{user_id}>, your {chao_colour} {chao_type} has reached adulthood!"
                        await send_cocoon_embed(member, channel, title, description)

                        if chao["alignment_"] in ["Hero", "Dark"]:
                            color = discord.Color.blue() if chao["alignment_"] == "Hero" else discord.Color.red()
                            transform_title = f"✨ {chao.get('nickname', 'Your Chao')} has transformed!"
                            transform_desc = f"<@{user_id}>, your {chao_colour} {chao_type} has become a **{chao['alignment_']} Chao**!"
                            embed = discord.Embed(title=transform_title, description=transform_desc, color=color)
                            try:
                                await channel.send(embed=embed)
                                await member.send(embed=embed)
                            except discord.Forbidden:
                                print(f"[WARN] Cannot DM user {user_id}.")
                            except Exception as e:
                                print(f"[ERROR] Sending transformation embed failed: {e}")

                    # --- Reincarnation ---
                    if chao["age"] == 2 and chao["messages"] >= REINCARNATION_MESSAGES:
                        chao.update({
                            "age": 0,
                            "messages": 0,
                            "last_age_time": now,
                            "alignment": 0,
                            "alignment_": "Neutral",
                            "is_egg": True,
                            "messages_until_hatch": 20,
                            "hatch_time": now + 1200,
                            "reincarnations": chao.get("reincarnations", 0) + 1
                        })
                        print(f"[REINCARNATE] {user_id}'s Chao '{chao.get('nickname', 'Unnamed')}' has reincarnated.")
                        title = f"💫 {chao.get('nickname', 'Your Chao')} has reincarnated!"
                        description = f"<@{user_id}>, your Chao has been reborn and is starting its journey anew!"
                        try:
                            await send_cocoon_embed(member, channel, title, description, color=discord.Color.purple())
                        except discord.Forbidden:
                            print(f"[WARN] Cannot DM user {user_id}.")
                        except Exception as e:
                            print(f"[ERROR] Sending reincarnation embed failed: {e}")

                        # Enqueue persistence for reincarnation (one-hour delay)
                        try:
                            enqueue_age_write(user_id, chao_index=None, scheduled_time=now + 3600)
                        except Exception as e:
                            print(f"[WARN] Failed to enqueue age write after reincarnation for user {user_id}: {e}")

                    # --- Gradual energy regeneration: full recovery over 12 hours ---
                    try:
                        last_update = chao.get("last_energy_update", now)
                        # If energy already full, refresh the timestamp
                        if chao.get("energy", 100) >= 100:
                            chao["energy"] = 100
                            chao["last_energy_update"] = now
                        else:
                            if now > last_update:
                                elapsed = now - last_update
                                # fraction of full energy to regenerate this tick
                                frac = elapsed / (12 * 3600)
                                if frac > 0:
                                    energy_before = float(chao.get("energy", 0))
                                    energy_after = min(100.0, energy_before + frac * 100.0)
                                    chao["energy"] = int(round(energy_after))
                                    chao["last_energy_update"] = now
                    except Exception as e:
                        # Defensive defaults
                        chao.setdefault("energy", 100)
                        chao.setdefault("last_energy_update", now)
                except Exception as e:
                    print(f"[ERROR] Exception in chao aging for user {user_id}: {e}")
                    continue
            # Detect if user_data changed and enqueue a save
            try:
                after = json.dumps(user_data, sort_keys=True)
            except Exception:
                after = None
            if before is None or after is None or before != after:
                changed_users.add(user_id)

        # Enqueue saves for changed users only
        for uid in changed_users:
            enqueue_chao_save(uid)
        checking += 1
        if checking % 5 == 0:
            print(f"Checked {checking} Chao aging processes since bot start.")
    except Exception as e:
        print(f"[FATAL] Exception in chao_aging_task: {e}")


@tasks.loop(minutes=30)
async def energy_regen_task():
    """Add 5% energy to every non-egg Chao every 30 minutes, up to 100%."""
    global checking
    changed = False
    changed_users = set()
    for user_id, user_data in user_chao_data.items():
        if not isinstance(user_data, dict):
            continue
        chao_list = user_data.get("chao", []) if isinstance(user_data.get("chao", []), list) else []
        for chao in chao_list:
            if not isinstance(chao, dict):
                continue
            if chao.get("is_egg", False):
                continue
            chao.setdefault("energy", 100)
            energy_before = int(chao.get("energy", 100))
            if energy_before < 100:
                energy_after = min(100, energy_before + 5)
                chao["energy"] = energy_after
                changed = True
                changed_users.add(user_id)
            # Increase hunger by 5% every 30 minutes (clamp to 100)
            chao.setdefault("hunger", 0)
            hunger_before = int(chao.get("hunger", 0))
            if hunger_before < 100:
                hunger_after = min(100, hunger_before + 5)
                chao["hunger"] = hunger_after
                changed = True
                changed_users.add(user_id)

            # Decrease happiness by 10% every 30 minutes (clamp to 0)
            chao.setdefault("happiness", 100)
            happiness_before = int(chao.get("happiness", 100))
            if happiness_before > 0:
                happiness_after = max(0, happiness_before - 10)
                chao["happiness"] = happiness_after
                changed = True
                changed_users.add(user_id)
    if changed:
        try:
            # enqueue only users that were modified
            for uid in changed_users:
                enqueue_chao_save(uid)
        except Exception as e:
            print(f"[ERROR] Failed to enqueue chao data after regen: {e}")
    # Avoid double-save: already persisted if changed
    checking += 1
    if checking % 5 == 0:
        print(f"Checked {checking} Chao aging processes since bot start.")


async def send_cocoon_embed(user, channel, title, description, color=discord.Color.gold()):
    """Helper to send cocoon embed to both DM and server channel"""
    cocoon_path = os.path.join(script_dir, "botimages", "cocoon.png")
    embed = discord.Embed(title=title, description=description, color=color)

    if os.path.exists(cocoon_path):
        # Send to channel (if exists)
        if channel:
            file1 = discord.File(cocoon_path, filename="cocoon.png")
            embed.set_image(url="attachment://cocoon.png")
            await channel.send(embed=embed, file=file1)
        # Send to DM
        dm_channel = await user.create_dm()
        file2 = discord.File(cocoon_path, filename="cocoon.png")
        embed.set_image(url="attachment://cocoon.png")
        await dm_channel.send(embed=embed, file=file2)
    else:
        if channel:
            await channel.send(embed=embed)
        dm_channel = await user.create_dm()
        await dm_channel.send(embed=embed)



# Give coins on member join
@bot.event
async def on_member_join(member):
    print(f"[EVENT] on_member_join: {member.display_name} ({member.id}) joined.")
    user_id = member.id
    amount = 200

    if user_shares_another_bot_guild(user_id, excluded_guild_id=member.guild.id):
        return

    add_balance(user_id, amount)

    # Send a welcome image
    script_dir = os.path.dirname(os.path.abspath(__file__))
    image_number = random.randint(1, 4)
    image_path = os.path.join(script_dir, "botimages", f"{image_number}.png")

    if member.guild.system_channel:  # send to system channel if set
        await member.guild.system_channel.send(
            f"Welcome to the server, {member.display_name}! You have been awarded {amount} rings.",
            file=discord.File(image_path)
        )




""" @bot.event NO LONGER USED
async def on_message(message): # For Chao aging
    if message.author.bot:
        return

    user_id = str(message.author.id)
    now = time.time()

    # --- Increment Chao message counter every 2 minutes ---
    if user_id in user_chao_data:
        last_time = last_chao_message.get(user_id, 0)
        if now - last_time >= 120:  # 2 minutes cooldown
            for chao in user_chao_data[user_id]:
                if "age" in chao and chao["age"] < 2:  # Only count messages if not adult
                    chao["messages"] = chao.get("messages", 0) + 1
            last_chao_message[user_id] = now

    if message.is_system():
        return

    if not message.content or message.content.strip() == "":
        return

    if message.content.startswith(bot.command_prefix):
        await bot.process_commands(message)
        return

    user_id_int = message.author.id

    await bot.process_commands(message) """




# --- Commands --- 
@bot.hybrid_command(aliases=["bal"])
async def balance(ctx, member: MaybeMember = None):
    """Shows the balance of a user."""
    print(f"[COMMAND] !balance by {ctx.author} for {member or ctx.author}")
    member = member or ctx.author
    bal = get_balance(member.id)
    await ctx.send(f"<:yellowchao:1407492531046060032> {member.display_name} has {bal} rings!")

@bot.hybrid_command()
@commands.cooldown(1, 1800, commands.BucketType.user)  # 1 use every 30 min per user
async def earn(ctx):
    """Allows the user to earn rings. 30-minute cooldown."""
    print(f"[COMMAND] !earn by {ctx.author}")
    
    user_id = str(ctx.author.id)  # Ensure string for balance functions

    # --- Bypass cooldown if in exempt guild/channel ---
    if ctx.guild and ctx.guild.id == EXEMPT_GUILD_ID and ctx.channel.id == EXEMPT_CHANNEL_ID:
        ctx.command.reset_cooldown(ctx)
        await ctx.send("Bypassing cooldown in exempt channel!")
        await asyncio.sleep(1)  # simulate processing time

    activity = random.randint(0, 5)

    if activity == 0:
        amount = 0
        balance = get_balance(user_id)
        await ctx.send(
            f"<:berryeeping:1407500809502982297> Uh oh, you fell asleep and missed out on some rings! "
            f"<:sadchao:1407501406260297869>\n-# {ctx.author.display_name} now has {balance} rings."
        )

    elif activity == 1:
        amount = random.choice([300, 600, 900])
        add_balance(user_id, amount)
        balance = get_balance(user_id)
        await ctx.send(
            f"<:chaofruit:1408217558875766804> You managed to shake some fruit off a tree! "
            f"You earned {amount} rings!\n-# {ctx.author.display_name} now has {balance} rings."
        )

    elif activity == 2:
        amount = 5000
        add_balance(user_id, amount)
        balance = get_balance(user_id)
        await ctx.send(
            f"<:beanie:1407510999107375134> You managed to do the beanie exploit! "
            f"You earned {amount} rings!\n-# {ctx.author.display_name} now has {balance} rings."
        )

    elif activity == 3:
        amount = random.randint(10, 100)
        add_balance(user_id, amount)
        balance = get_balance(user_id)
        await ctx.send(
            f"<:yippee:1407493289816625293> You found some rings lying around the garden! "
            f"You earned {amount} rings!\n-# {ctx.author.display_name} now has {balance} rings."
        )

    elif activity == 4:
        amount = random.randint(100, 500)
        add_balance(user_id, amount)
        balance = get_balance(user_id)
        await ctx.send(
            f"Gleepub thought you were sad and gifted you {amount} rings! "
            f"<:gleepubthumbsup:1407505516527226964>\n-# {ctx.author.display_name} now has {balance} rings."
        )

    elif activity == 5:
        await ctx.send("<a:chaosonicchaodance:1407507437010944121> You were challenged to a dance-off by one of the Chao!")
        await asyncio.sleep(1)
        danceoff = random.randint(0, 1)
        if danceoff == 0:
            amount = 0
            add_balance(user_id, amount)
            balance = get_balance(user_id)
            await ctx.send(
                f"<:twisty:1407499302301073418> You lost the dance-off and earned no rings.\n"
                f"-# {ctx.author.display_name} now has {balance} rings."
            )
        else:
            amount = random.randint(1000, 5000)
            add_balance(user_id, amount)
            balance = get_balance(user_id)
            await ctx.send(
                f"<:yippee:1407493289816625293> You won the dance-off and earned {amount} rings!\n"
                f"-# {ctx.author.display_name} now has {balance} rings."
            )

    #await ctx.send(f"<:yippee:1407493289816625293> You earned {amount} rings!"
    


@bot.hybrid_command()
async def pay(ctx, member: discord.Member, amount: int):
    '''Pay another user an amount of rings.'''
    print(f"[COMMAND] !pay by {ctx.author} to {member} amount {amount}")
    
    if ctx.author.id == member.id:
        await ctx.send("<:fa:1408217566660395028> You cannot pay yourself!")
        return

    if amount <= 0:
        await ctx.send("<:fa:1408217566660395028> Amount must be positive!")
        return
    
    sender_balance = get_balance(ctx.author.id)
    if sender_balance < amount:
        await ctx.send("<:twisty:1407499302301073418> You don't have enough rings!")
        return

    # Subtract from sender
    add_balance(ctx.author.id, -amount)
    # Add to receiver
    add_balance(member.id, amount)

    await ctx.send(f"<:yellowchao:1407492531046060032> {ctx.author.display_name} paid {amount} rings to {member.display_name}!")


@bot.hybrid_command(aliases=["topbalance", "baltop"])
async def balancetop(ctx, top_n: int = 10):
    """Check the top ring balances."""
    print(f"[COMMAND] !balancetop by {ctx.author}")
    if not balances:
        await ctx.send("No balances to display!")
        return

    # Sort balances descending
    sorted_balances = sorted(balances.items(), key=lambda x: x[1], reverse=True)

    # Filter out the bot itself
    bot_id = bot.user.id
    sorted_balances = [(uid, bal) for uid, bal in sorted_balances if int(uid) != bot_id]

    # Split into pages of 10 users each
    pages = []
    page_size = 10
    for i in range(0, len(sorted_balances), page_size):
        page_items = sorted_balances[i:i+page_size]
        description = ""
        for rank, (user_id, bal) in enumerate(page_items, start=i+1):
            user = ctx.guild.get_member(int(user_id))
            if user:  # Only show existing users
                description += f"**{rank}. {user.display_name}** — {bal} rings\n"
        embed = discord.Embed(
            title=f"<:yellowchao:1407492531046060032> Balance Top {i+1}-{min(i+page_size, len(sorted_balances))}",
            description=description,
            color=discord.Color.gold()
        )
        pages.append(embed)

    # Send the first page
    message = await ctx.send(embed=pages[0])
    
    if len(pages) == 1:
        return  # no need for reactions if only one page

    # Add navigation reactions
    await message.add_reaction("⬅️")
    await message.add_reaction("➡️")

    current_page = 0

    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["⬅️", "➡️"] and reaction.message.id == message.id

    while True:
        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check)
            
            # Remove the user's reaction for a cleaner UI
            await message.remove_reaction(reaction.emoji, user)

            if str(reaction.emoji) == "➡️":
                current_page = (current_page + 1) % len(pages)
            elif str(reaction.emoji) == "⬅️":
                current_page = (current_page - 1) % len(pages)

            await message.edit(embed=pages[current_page])
        except asyncio.TimeoutError:
            break  # Stop listening after 60 seconds



@bot.hybrid_command()
@app_commands.default_permissions(administrator=True)
async def setbalance(ctx, member: discord.Member, amount: int):
    """Set a user's balance to a specific amount. Only the bot founder can do this."""
    print(f"[COMMAND] !setbalance by {ctx.author} for {member} amount {amount}")

    # Check if user is the founder
    if ctx.author.id != FOUNDER_ID:
        await ctx.send("<:twisty:1407499302301073418> Only the bot founder can use this command!")
        return

    bal = get_balance(member.id)
    if bal is None:
        await ctx.send("<:fa:1408217566660395028> User not found!")
        return
    else:
        balances[str(member.id)] = amount
        save_balances()
        await ctx.send(f"<:yellowchao:1407492531046060032> Set {member.display_name}'s balance to {amount} rings!")


# Helper to safely get user's inventory
def get_user_inventory(user_id):
    user_id = str(user_id)
    if user_id not in user_inventory:
        user_inventory[user_id] = {}
    return user_inventory[user_id]

active_shops = set()   # Tracks active shop sessions per user

@bot.hybrid_command()
@app_commands.default_permissions(administrator=True)
async def shop(ctx):
    """Welcome to the Black Market!"""
    print(f"[COMMAND] !shop by {ctx.author}")
    user_id = ctx.author.id
    if user_id in active_shops:
        await ctx.send("<:twisty:1408217575816691835> You already have an open shop session!")
        return

    active_shops.add(user_id)
    embed = discord.Embed(
        title="Black Market <:blackmarket:1407707350063648910>",
        description="Welcome to the Black Market! Type the name of the item you want to buy.",
        color=0x000000
    )

    for category, items in shop_items.items():
        value = ""
        for item in items:
            disp_emoji = item.get('emoji', '')
            if item.get('name', '').lower() == 'egg':
                disp_emoji = chao_emojis.get('egg', disp_emoji)
            value += f"{disp_emoji} **{item['name']}** — {item['price']} rings\n"
        embed.add_field(name=category, value=value, inline=False)

    await ctx.send(embed=embed)

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel

    try:
        msg = await bot.wait_for("message", check=check, timeout=60)
        choice = msg.content.strip().lower()
    except asyncio.TimeoutError:
        await ctx.send("⏳ You took too long to choose an item!")
        active_shops.remove(user_id)
        return

    # Find matching items (case-insensitive)
    matches = []
    for category_items in shop_items.values():
        for item in category_items:
            if choice in item['name'].lower():
                matches.append(item)

    if not matches:
        await ctx.send(f"<:twisty:1407499302301073418> `{choice}` is not a valid item.")
        active_shops.remove(user_id)
        return
    elif len(matches) > 1:
        await ctx.send("<:twisty:1407499302301073418> Multiple items match that name. Please type the full name next time.")
        active_shops.remove(user_id)
        return

    item_found = matches[0]

    # Ask for quantity
    await ctx.send(f"How many **{item_found['name']}** do you want to buy? (type a number)")

    try:
        qty_msg = await bot.wait_for("message", check=check, timeout=30)
        quantity = int(qty_msg.content)
        if quantity <= 0:
            await ctx.send("<:twisty:1407499302301073418> Quantity must be at least 1.")
            active_shops.remove(user_id)
            return
    except (ValueError, asyncio.TimeoutError):
        await ctx.send("<:twisty:1407499302301073418> Invalid quantity or timeout.")
        active_shops.remove(user_id)
        return

    # Check balance
    total_price = item_found['price'] * quantity
    user_balance = get_balance(user_id)
    if user_balance < total_price:
        await ctx.send(f"<:twisty:1407499302301073418> You don't have enough rings for {quantity}x {item_found['name']}! You need {total_price} and you only have {user_balance} rings.")
        active_shops.remove(user_id)
        return

    # Special handling: Egg is created directly in the user's garden
    if item_found['name'].lower() == 'egg':
        add_balance(user_id, -total_price)
        uid_str = str(user_id)
        if uid_str not in user_chao_data or not isinstance(user_chao_data[uid_str], dict):
            user_chao_data[uid_str] = {"username": ctx.author.display_name, "chao": []}
        user_chao_data[uid_str]["username"] = user_chao_data[uid_str].get("username") or ctx.author.display_name
        chao_list = user_chao_data[uid_str].setdefault("chao", [])

        for _ in range(quantity):
            # Shop eggs are always Normal TwoTone
            egg = build_egg("Normal", "TwoTone")
            chao_list.append(egg)

        enqueue_chao_save(uid_str)
        egg_emoji = chao_emojis.get("egg", "🥚")
        await ctx.send(f"{egg_emoji} You bought {quantity}x **Egg** for {total_price} rings! Added to your garden.")
        active_shops.remove(user_id)
        return

    # Default: Deduct and add to inventory
    add_balance(user_id, -total_price)
    inv = get_user_inventory(user_id)
    key = item_found['name'].lower()
    inv[key] = inv.get(key, 0) + quantity
    user_inventory[str(user_id)] = inv
    save_user_inventory()

    await ctx.send(f"<:yellowchao:1407492531046060032> You bought {quantity}x **{item_found['name']}** for {total_price} rings!")
    active_shops.remove(user_id)



@bot.hybrid_command()
async def use(ctx, *, item_name=None):
    """Use an item from your inventory. Note: Chao Race Tickets must be used through !chaorace."""
    print(f"[COMMAND] !use by {ctx.author} item_name: {item_name}")
    if not item_name or item_name.strip() == "":
        await ctx.send("<:fa:1408217566660395028> Usage: `/use item_name:<item name>` or `!use <item name>` — specify the item you want to use from your inventory.")
        return

    user_id = str(ctx.author.id)
    inv = get_user_inventory(user_id)
    chao_list = []
    if user_id in user_chao_data and isinstance(user_chao_data[user_id], dict):
        chao_list = user_chao_data[user_id].get("chao", [])
    # Parse optional quantity at the end (e.g., "hero fruit 3")
    quantity = 1
    base_name = item_name
    if item_name:
        parts = item_name.rsplit(" ", 1)
        if len(parts) == 2 and parts[1].isdigit():
            base_name, qty_str = parts[0], parts[1]
            quantity = max(1, int(qty_str))
    key = base_name.lower()

    if key not in inv or inv[key] <= 0:
        await ctx.send("<:twisty:1407499302301073418> You don't have that item!")
        return
    
    # Only these items can be used multiple times at once
    multi_allowed = {"dark fruit", "hero fruit", "chao fruit", "square fruit", "triangle fruit", "round fruit"}
    if quantity > 1 and key not in multi_allowed:
        await ctx.send("<:twisty:1407499302301073418> Only Dark Fruit, Hero Fruit, and Chao Fruit can be used multiple times at once.")
        return
    
    # Ensure sufficient quantity when multi-using
    if quantity > inv[key]:
        await ctx.send(f"<:twisty:1407499302301073418> You only have {inv[key]}x {base_name.title()}!")
        return
    if not chao_list:
        await ctx.send("<:twisty:1407499302301073418> You don't have any Chao to use this on!")
        return

    # --- Helpers ---
    def get_chao_type(chao):
        return chao.get("type_") or chao.get("type") or "monotone"

    def get_chao_emoji(chao):
        if chao.get("is_egg", False):
            return chao_emojis.get("egg", "<:chaoegg:1408215309592035329>")
        colour = chao.get("colour", "default").lower()
        ctype = get_chao_type(chao).lower()
        return chao_emojis.get(colour, {}).get(ctype, chao_emojis["default"]["monotone"])

    async def prompt_chao_selection(title, chao_options):
        def type_display(c):
            t = c.get('type_') or c.get('type') or 'Monotone'
            # Normalize common casing
            return t if any(ch.isupper() for ch in t) else t.capitalize()

        description = "\n".join(
            f"{i+1}. {get_chao_emoji(c)} {c['nickname']} ({c.get('colour','Unknown')} {type_display(c)})"
            for i, c in enumerate(chao_options)
        )
        await ctx.send(embed=discord.Embed(title=title, description=description, color=discord.Color.green()))
        try:
            reply = await bot.wait_for(
                "message",
                check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                timeout=30
            )
            choice = int(reply.content) - 1
            if choice < 0 or choice >= len(chao_options):
                await ctx.send("<:twisty:1407499302301073418> Invalid choice!")
                return None
            return chao_options[choice]
        except (ValueError, asyncio.TimeoutError):
            await ctx.send("<:twisty:1407499302301073418> You did not choose a valid Chao in time!")
            return None

    msg = ""
    chosen_chao = None

    # --- HEART FRUIT ---
    if key == "heart fruit":
        adults = [c for c in chao_list if c.get("age", 0) == 2 and not c.get("is_egg", False)]
        if len(adults) < 2:
            await ctx.send("<:twisty:1407499302301073418> You need at least two adult Chao to use the Heart Fruit!")
            return

        # Prompt for first Chao, but do not allow selections with energy < 30%
        while True:
            chosen1 = await prompt_chao_selection("Select the first Chao for the Heart Fruit", adults)
            if not chosen1:
                return  # safe exit, inventory untouched (timeout or invalid)
            try:
                energy1 = int(chosen1.get("energy", 100))
            except Exception:
                energy1 = 100
            if energy1 < 30:
                await ctx.send(f"<:twisty:1407499302301073418> {chosen1.get('nickname','That Chao')} doesn't have enough energy ({energy1}%). Please select a different Chao.")
                # reissue prompt
                continue
            break

        # Second selection: exclude the first chosen chao and re-prompt if energy < 30%
        adults_second = [c for c in adults if c != chosen1]
        while True:
            chosen2 = await prompt_chao_selection("Select the second Chao for the Heart Fruit", adults_second)
            if not chosen2:
                return  # safe exit, inventory untouched (timeout or invalid)
            try:
                energy2 = int(chosen2.get("energy", 100))
            except Exception:
                energy2 = 100
            if energy2 < 30:
                await ctx.send(f"<:twisty:1407499302301073418> {chosen2.get('nickname','That Chao')} doesn't have enough energy ({energy2}%). Please select a different Chao.")
                continue
            break

        # --- Deduct item only after successful selection ---
        inv[key] -= 1
        if inv[key] <= 0:
            del inv[key]
        user_inventory[user_id] = inv
        save_user_inventory()

        # --- Create egg via helper ---
        egg_colour = random.choice([chosen1.get("colour", "Blue"), chosen2.get("colour", "Blue")])
        egg_type_ = random.choice([get_chao_type(chosen1), get_chao_type(chosen2)])
        egg = build_egg(egg_colour, egg_type_)
        chao_list.append(egg)

        # Deduct 20% energy (20 points) from both participating Chao
        try:
            chosen1['energy'] = max(0, int(chosen1.get('energy', 100) - 20))
        except Exception:
            chosen1['energy'] = max(0, 100 - 20)
        try:
            chosen2['energy'] = max(0, int(chosen2.get('energy', 100) - 20))
        except Exception:
            chosen2['energy'] = max(0, 100 - 20)

        msg = (
            f"💖 {chosen1['nickname']} and {chosen2['nickname']} have used a Heart Fruit! "
            f"A new egg has appeared in your garden!\n{chosen1.get('nickname','Chao')}: {chosen1.get('energy', 100)}% energy remaining\n"
            f"{chosen2.get('nickname','Chao')}: {chosen2.get('energy', 100)}% energy remaining."
        )

    # --- OTHER ITEMS ---
    elif key in ["dark fruit", "hero fruit", "chao fruit", "square fruit", "triangle fruit", "round fruit"]:
        non_eggs = [c for c in chao_list if not c.get("is_egg", False)]
        if not non_eggs:
            await ctx.send("<:twisty:1407499302301073418> You can't use that item on eggs!")
            return

        chosen_chao = await prompt_chao_selection(f"Which Chao do you want to use the {base_name.title()} on?", non_eggs)
        if not chosen_chao:
            return  # safe exit, inventory untouched

        # If user didn't specify a quantity (defaults to 1) and owns more, prompt how many to use
        if quantity == 1 and inv.get(key, 0) > 1:
            await ctx.send(f"How many {base_name.title()} would you like to use? (1 - {inv[key]})")
            try:
                reply = await bot.wait_for(
                    "message",
                    check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                    timeout=30
                )
                qty = int(reply.content)
                if qty < 1 or qty > inv[key]:
                    await ctx.send(f"<:twisty:1407499302301073418> Please enter a number between 1 and {inv[key]}.")
                    return
                quantity = qty
            except (ValueError, asyncio.TimeoutError):
                await ctx.send("<:twisty:1407499302301073418> You did not provide a valid quantity in time.")
                return

        # Ensure sufficient quantity when multi-using (covers explicit quantity case too)
        if quantity > inv.get(key, 0):
            await ctx.send(f"<:twisty:1407499302301073418> You only have {inv.get(key, 0)}x {base_name.title()}!")
            return

        # --- Deduct item(s) only after successful selection ---
        inv[key] -= quantity
        if inv[key] <= 0:
            del inv[key]
        user_inventory[user_id] = inv
        save_user_inventory()

        # --- Apply item effects ---
        if key == "dark fruit":
            # Stackable: decrease alignment quantity times (clamped)
            current = chosen_chao.get("alignment", 0)
            chosen_chao["alignment"] = max(-10, current - quantity)
            msg = f"{chosen_chao['nickname']} ate {quantity}x Dark Fruit and feels darker... <:spikenew:1408217572733882509>"
        elif key == "hero fruit":
            # Stackable: increase alignment quantity times (clamped)
            current = chosen_chao.get("alignment", 0)
            chosen_chao["alignment"] = min(10, current + quantity)
            msg = f"{chosen_chao['nickname']} ate {quantity}x Hero Fruit and feels more heroic! <:halonew:1408217567943987380> "
        elif key == "chao fruit":
            # Stackable: +10 messages per fruit and -1 hour per fruit towards aging (if < adult)
            chosen_chao["messages"] = chosen_chao.get("messages", 0) + (10 * quantity)
            if chosen_chao.get("age", 0) < 2:
                chosen_chao["last_age_time"] = chosen_chao.get("last_age_time", time.time()) - (3600 * quantity)
            msg = f"{chosen_chao['nickname']} has eaten {quantity}x Chao Fruit! They will grow a little faster now! <:chaofruit:1408217558875766804>"
        elif key in ["square fruit", "triangle fruit", "round fruit"]:
            # Stackable: -25% hunger per fruit, -50% if favourite fruit
            fav = chosen_chao.get("favourite_fruit", "").lower()
            fruit_name = base_name.lower()
            hunger_reduction = 50 if fav == base_name.lower() else 25
            total_reduction = hunger_reduction * quantity
            before = int(chosen_chao.get("hunger", 0))
            after = max(0, before - total_reduction)
            chosen_chao["hunger"] = after
            # Get emoji from shop_items
            fruit_emoji = None
            for item in shop_items["Food"]:
                if item["name"].lower() == base_name.lower():
                    fruit_emoji = item.get("emoji", "")
                    break
            fruit_emoji = fruit_emoji or ""
            if hunger_reduction == 50:
                msg = f"{chosen_chao['nickname']} ate {quantity}x {base_name.title()} (favourite!) {fruit_emoji} and is much less hungry! ({before}% → {after}%)"
            else:
                msg = f"{chosen_chao['nickname']} ate {quantity}x {base_name.title()} {fruit_emoji} and is less hungry! ({before}% → {after}%)"
    else:
        await ctx.send("<:twisty:1407499302301073418> You can't use that item on a Chao!")
        return

    # --- Save Chao data ---
    enqueue_chao_save(user_id)

    # --- Notify user ---
    if chosen_chao:
        await ctx.send(f"{get_chao_emoji(chosen_chao)} {msg}")
    else:
        await ctx.send(msg)




@bot.hybrid_command(aliases=["cmds", "listcommands"])
async def commandslist(ctx):
    """List all available commands."""
    print(f"[COMMAND] !commandslist by {ctx.author}")
    
    
    # Determine user's permissions
    is_admin = ctx.author.guild_permissions.administrator
    
    # Build pages
    pages = []
    page_size = 5  # Number of commands per page per category
    
    for category, commands_in_cat in command_categories.items():
        # Filter admin commands for non-admins
        if category == "Admin" and not is_admin:
            continue

        # Split into chunks/pages
        for i in range(0, len(commands_in_cat), page_size):
            chunk = commands_in_cat[i:i+page_size]
            description = ""
            for cmd_name in chunk:
                cmd = bot.get_command(cmd_name)
                if not cmd:
                    continue
                # Skip hidden commands
                if cmd.hidden:
                    continue
                description += f"**!{cmd.name}** — {cmd.help or 'No description'}\n"
            
            embed = discord.Embed(
                title=f"{category} Commands ({i+1}-{i+len(chunk)})",
                description=description,
                color=discord.Color.blurple()
            )
            pages.append(embed)
    
    if not pages:
        await ctx.send("No commands available for you.")
        return
    
    # Send first page
    message = await ctx.send(embed=pages[0])
    
    if len(pages) == 1:
        return  # No pagination needed
    
    # Add navigation reactions
    await message.add_reaction("⬅️")
    await message.add_reaction("➡️")
    
    current_page = 0

    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["⬅️", "➡️"] and reaction.message.id == message.id

    while True:
        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check)
            await message.remove_reaction(reaction.emoji, user)

            if str(reaction.emoji) == "➡️":
                current_page = (current_page + 1) % len(pages)
            elif str(reaction.emoji) == "⬅️":
                current_page = (current_page - 1) % len(pages)

            await message.edit(embed=pages[current_page])
        except asyncio.TimeoutError:
            break  # Stop listening after timeout

@bot.hybrid_command()
async def release(ctx):
    """Chao will have a happy life in a faraway forest. You will never see your Chao again."""
    print(f"[COMMAND] !release by {ctx.author}")
    
    user_id = str(ctx.author.id)
    if user_id not in user_chao_data or not isinstance(user_chao_data[user_id], dict) or not user_chao_data[user_id].get("chao"):
        return await ctx.send("You don't have any Chao to release!")

    # Filter out eggs
    chao_list = [c for c in user_chao_data[user_id]["chao"] if not c.get("is_egg", False)]
    if not chao_list:
        return await ctx.send("You don't have any Chao eligible for release! Eggs cannot be released.")

    # List Chao with indices
    message_text = "Which Chao would you like to release? Reply with the number or type `cancel` to cancel.\n"
    for i, chao in enumerate(chao_list, start=1):
        nickname = chao.get("nickname", "Unknown")
        colour = chao.get("colour", "Unknown")
        message_text += f"{i}. {nickname} ({colour})\n"
    await ctx.send(message_text)

    def check_number_or_cancel(m):
        return (
            m.author == ctx.author
            and m.channel == ctx.channel
            and (m.content.lower() == "cancel" or (m.content.isdigit() and 1 <= int(m.content) <= len(chao_list)))
        )

    try:
        reply = await bot.wait_for("message", check=check_number_or_cancel, timeout=30)

        if reply.content.lower() == "cancel":
            return await ctx.send("Release canceled.")

        index = int(reply.content) - 1
        chao_to_release = chao_list[index]

        # Ask for confirmation
        await ctx.send(
            f"<:sadchao:1407501406260297869> Once you say good-bye to {chao_to_release.get('nickname', 'Unknown')}, you will never see that Chao again. Are you sure? Type `yes` to confirm or `cancel` to cancel."
        )

        def check_confirmation_or_cancel(m):
            return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() in ["yes", "no", "cancel"]

        confirm = await bot.wait_for("message", check=check_confirmation_or_cancel, timeout=30)
        if confirm.content.lower() == "yes":
            # Remove from the original user_chao_data list
            original_list = user_chao_data[user_id]["chao"]
            original_index = original_list.index(chao_to_release)
            released_chao = original_list.pop(original_index)
            enqueue_chao_save(user_id)  # persist change on next flush
            await ctx.send(f"All right. Say good-bye!\n<:goodbyechao:1407763148244648118> {released_chao.get('nickname', 'Chao')} has returned to a faraway forest. It will surely have a happy life.")
        else:
            await ctx.send("Release canceled.")

    except asyncio.TimeoutError:
        await ctx.send("You took too long to respond. Release canceled.")


@bot.hybrid_command()
async def chaorace(ctx):
    """Start a Chao race! Requires a Chao Race Ticket."""
    print(f"[COMMAND] !chaorace by {ctx.author}")
    user_id = str(ctx.author.id)

    # --- Load user's inventory ---
    inv = get_user_inventory(user_id)
    ticket_key = next((k for k in inv if k.lower() == "chao race ticket"), None)
    ticket_count = inv.get(ticket_key, 0) if ticket_key else 0
    if ticket_count <= 0:
        await ctx.send("<:twisty:1407499302301073418> You need a Chao Race Ticket to enter a race!")
        return

    # --- Filter out eggs ---
    user_chao_list = [
        c for c in user_chao_data.get(user_id, {}).get("chao", [])
        if not c.get("is_egg", False)
    ]
    if not user_chao_list:
        await ctx.send("<:twisty:1407499302301073418> You don't have any Chao eligible for racing!")
        return

    # --- Helper functions ---
    def get_chao_type_display(chao):
        ctype = chao.get("type_") or chao.get("type") or "Monotone"
        return ctype.capitalize() if ctype.islower() else ctype

    def get_chao_emoji(chao):
        if chao.get("is_egg", False):
            return chao_emojis.get("egg", "<:chaoegg:1408215309592035329>")
        colour = chao.get("colour", "default").lower()
        ctype = (chao.get("type_", chao.get("type", "monotone"))).lower()
        return chao_emojis.get(colour, {}).get(ctype, chao_emojis["default"]["monotone"])

    # --- Eligibility helper (energy and hunger thresholds) ---
    def chao_is_eligible_for_race(c):
        try:
            energy = int(c.get("energy", 100))
        except Exception:
            energy = 100
        try:
            hunger = int(c.get("hunger", 100))
        except Exception:
            hunger = 100
        return energy, hunger, (energy >= 30 and hunger < 70)

    # --- Let user pick their Chao if multiple ---
    if len(user_chao_list) > 1:
        desc_lines = [
            f"{i+1}. {get_chao_emoji(c)} {c['nickname']} ({c['colour']} {get_chao_type_display(c)})"
            for i, c in enumerate(user_chao_list)
        ]
        embed = discord.Embed(title="Choose your Chao to race:", description="\n".join(desc_lines), color=discord.Color.green())
        await ctx.send(embed=embed)

        def check(m): return m.author == ctx.author and m.channel == ctx.channel
        try:
            reply = await bot.wait_for("message", check=check, timeout=30)
            choice = int(reply.content) - 1
            if choice < 0 or choice >= len(user_chao_list):
                await ctx.send("Invalid choice!")
                return
            user_chao = user_chao_list[choice]
        except (ValueError, asyncio.TimeoutError):
            await ctx.send("You did not choose a valid Chao in time!")
            return
    else:
        user_chao = user_chao_list[0]

    # --- Ensure the selected chao meets energy/hunger requirements ---
    energy, hunger, eligible = chao_is_eligible_for_race(user_chao)
    if not eligible:
        if energy < 30 and hunger >= 70:
            msg = f"<:twisty:1407499302301073418> Your Chao can't race: Energy is too low ({energy}/30+) and Hunger is too high ({hunger}/<70)."
        elif energy < 30:
            msg = f"<:twisty:1407499302301073418> Your Chao can't race: Energy is too low ({energy}/30+)."
        elif hunger >= 70:
            msg = f"<:twisty:1407499302301073418> Your Chao can't race: Hunger is too high ({hunger}/<70)."
        else:
            msg = f"<:twisty:1407499302301073418> Your Chao can't race due to unknown eligibility issue. (Energy: {energy}, Hunger: {hunger})"
        await ctx.send(msg)
        return

    # --- Deduct ticket ---
    if ticket_key:
        inv[ticket_key] -= 1
        if inv[ticket_key] <= 0: del inv[ticket_key]
        user_inventory[user_id] = inv
        save_user_inventory()
    await ctx.send("🎟️ Chao Race Ticket used. Good luck!")

    # --- Prepare participants ---
    participants = [(user_id, user_chao)]
    other_users = [uid for uid in user_chao_data if uid != user_id]
    random.shuffle(other_users)
    for uid in other_users:
        if len(participants) >= 8: break
        chao_pool = [c for c in user_chao_data.get(uid, {}).get("chao", []) if not c.get("is_egg", False)]
        eligible = [c for c in chao_pool if chao_is_eligible_for_race(c)]
        pick_from = eligible if eligible else chao_pool
        if pick_from:
            participants.append((uid, random.choice(pick_from)))

    npc_names = ["Fluffy", "Sparky", "Shadow", "Glimmer", "Twinkle", "Storm", "Blaze", "Coco"]
    while len(participants) < 8 and npc_names:
        name = npc_names.pop(0)
        participants.append((f"npc_{name}", {"nickname": name, "colour": "Default", "type_": "Monotone"}))

    # --- Show participants ---
    msg = "**🏁 Participants 🏁**\n"
    for i, (uid, chao) in enumerate(participants, 1):
        emoji = get_chao_emoji(chao)
        username = "NPC" if str(uid).startswith("npc_") else user_chao_data.get(uid, {}).get("username", ctx.guild.get_member(int(uid)).display_name if ctx.guild.get_member(int(uid)) else "Unknown")
        msg += f"{i}. {emoji} {chao['nickname']} ({username})\n"
    await ctx.send(msg)

    # --- Countdown ---
    for i in range(3, 0, -1):
        await ctx.send(f"Race starting in {i}...")
        await asyncio.sleep(1)
    await ctx.send("🏁 GO! 🏁")

    # --- Race mechanics ---
    finish_line = 10
    positions = [0] * len(participants)
    finished = [False] * len(participants)
    winners = []
    start_time = time.time()
    finish_times = {}

    def render_track():
        rows = []
        for r in range(finish_line):
            row = ""
            for i, (_, chao) in enumerate(participants):
                if positions[i] == r: row += get_chao_emoji(chao)
                elif positions[i] > r: row += ":black_large_square:"
                else: row += "⬜"
            rows.append(row)
        rows.append("🏁" * len(participants))
        return "\n".join(rows)

    race_msg = await ctx.send(render_track())

    while not all(finished):
        await asyncio.sleep(0.4)
        for i in range(len(participants)):
            if not finished[i]:
                positions[i] += random.randint(0, 2)
                if positions[i] >= finish_line - 1:
                    positions[i] = finish_line - 1
                    finished[i] = True
                    winners.append(participants[i])
                    uid, chao = participants[i]
                    if not str(uid).startswith("npc_"):
                        elapsed = round(time.time() - start_time, 2)
                        finish_times[uid] = elapsed
                        place = len(winners)
                        suffix = "st" if place==1 else "nd" if place==2 else "rd" if place==3 else "th"
                        username = user_chao_data.get(uid, {}).get("username", ctx.guild.get_member(int(uid)).display_name if ctx.guild.get_member(int(uid)) else "Unknown")
                        await ctx.send(f"🎉 {get_chao_emoji(chao)} {chao['nickname']} ({username}) finished {place}{suffix} in {elapsed}s!")

        await race_msg.edit(content=render_track())

    # --- Rewards and stats ---
    rewards = [3000, 2500, 2000] + [300] * 5
    results = "**🏁 Results 🏁**\n"
    for i, (uid, chao) in enumerate(winners):
        reward = rewards[i] if i < len(rewards) else 0
        if not str(uid).startswith("npc_"):
            add_balance(uid, reward)
            elapsed = finish_times.get(uid)
            username = user_chao_data.get(uid, {}).get("username") or (ctx.guild.get_member(int(uid)).display_name if ctx.guild.get_member(int(uid)) else "Unknown")
            chao_name = chao.get("nickname", "Unnamed")
            stats = race_scores.get(uid, {"wins": 0, "races": 0, "best_time": None, "chao_name": chao_name, "username": username})
            stats["races"] += 1
            if i == 0:
                stats["wins"] += 1
            if elapsed is not None and (stats.get("best_time") is None or elapsed < stats["best_time"]):
                stats["best_time"] = elapsed
                stats["chao_name"] = chao_name
            stats["username"] = username
            race_scores[uid] = stats
            save_race_scores()
            results += f"{i+1}. {get_chao_emoji(chao)} {chao_name} ({username}) — {reward} rings\n"
        else:
            results += f"{i+1}. {get_chao_emoji(chao)} {chao['nickname']} (NPC)\n"

    # --- Energy and happiness adjustments ---
    for place, (uid, ch) in enumerate(winners, 1):
        if str(uid).startswith("npc_"):
            continue

        # --- Energy ---
        try:
            energy_before = int(ch.get("energy", 100))
        except Exception:
            energy_before = 100
        if uid == user_id:
            energy_after = max(0, int(energy_before * 0.5))
        else:
            energy_after = max(0, int(round(energy_before * 0.9)))
        ch["energy"] = energy_after

        # --- Happiness ---
        current_happy = int(ch.get("happiness", 100))
        if place == 1:
            change = +75
        elif place == 2:
            change = +50
        elif place == 3:
            change = +40
        elif place == 4:
            change = +20   # <-- 4th place update
        elif place == 5:
            change = 0
        elif place == 6:
            change = -10
        elif place == 7:
            change = -20
        elif place == 8:
            change = -30
        else:
            change = 0
        ch["happiness"] = max(0, min(100, current_happy + change))

    # --- Save changes ---
    try:
        # Enqueue saves for affected users from this race
        affected = set()
        for _, ch in winners:
            try:
                uid = str(_)
                if not uid.startswith("npc_"):
                    affected.add(uid)
            except Exception:
                pass
        if affected:
            save_user_chao_data(list(affected))
    except Exception as e:
        print(f"Warning: failed to save chao data after race updates: {e}")

    await ctx.send(results)

@bot.hybrid_command()
async def racetop(ctx):
    """Shows Chao Race leaderboard with pagination."""
    if not race_scores:
        await ctx.send("No race stats yet!")
        return

    # Sort scores: primary by wins, secondary by best_time
    sorted_scores = sorted(
        race_scores.items(),
        key=lambda x: (x[1].get("wins", 0), -(x[1].get("best_time") or float('inf'))),
        reverse=True
    )

    page_size = 10
    pages = [sorted_scores[i:i+page_size] for i in range(0, len(sorted_scores), page_size)]
    current_page = 0

    def get_chao_emoji_for_leaderboard(uid, data):
        # Try to get the user's chao info from user_chao_data
        chao_name = data.get("chao_name", "Unnamed")
        chao_list = user_chao_data.get(uid, {}).get("chao", [])
        for chao in chao_list:
            if chao.get("nickname") == chao_name:
                # Use the same emoji logic as in chaorace
                colour = chao.get("colour", "default").lower()
                ctype = (chao.get("type_", chao.get("type", "monotone"))).lower()
                return chao_emojis.get(colour, {}).get(ctype, chao_emojis["default"]["monotone"])
        # fallback
        return chao_emojis["default"]["monotone"]

    def create_embed(page_idx):
        embed = Embed(title="🏆 Chao Race Leaderboard 🏆", color=0xFFD700)
        medals = ["🥇", "🥈", "🥉"]

        for idx, (uid, data) in enumerate(pages[page_idx], 1 + page_idx*page_size):
            chao_name = data.get("chao_name", "Unnamed")
            username = data.get("username") or user_chao_data.get(uid, {}).get("username") or (ctx.guild.get_member(int(uid)).display_name if ctx.guild.get_member(int(uid)) else "Unknown")
            wins = data.get("wins", 0)
            races = data.get("races", 0)
            best_time = data.get("best_time")
            medal = medals[idx-1] if idx <= 3 else f"{idx}."
            best_time_str = f"{best_time:.2f}s" if best_time is not None else "N/A"
            emoji = get_chao_emoji_for_leaderboard(uid, data)
            embed.add_field(
                name=f"{medal} {emoji} {chao_name} - {username}",
                value=f"{wins}/{races} Races won (Fastest: {best_time_str})",
                inline=False
            )

        embed.set_footer(text=f"Page {page_idx+1}/{len(pages)}")
        return embed

    message = await ctx.send(embed=create_embed(current_page))

    emojis = ["◀️", "▶️"]
    for emoji in emojis:
        await message.add_reaction(emoji)

    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in emojis and reaction.message.id == message.id

    while True:
        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=120.0, check=check)

            if str(reaction.emoji) == "◀️" and current_page > 0:
                current_page -= 1
            elif str(reaction.emoji) == "▶️" and current_page < len(pages) - 1:
                current_page += 1

            await message.edit(embed=create_embed(current_page))
            await message.remove_reaction(reaction, user)
        except asyncio.TimeoutError:
            await message.clear_reactions()
            break




@bot.hybrid_command()
async def racestats(ctx, member: MaybeMember = None):
    """Show race stats for yourself or another user."""
    member = member or ctx.author
    user_id = str(member.id)

    stats = race_scores.get(user_id)
    if not stats:
        await ctx.send(f"{member.display_name} has no recorded race stats yet.")
        return

    wins = stats.get("wins", 0)
    races = stats.get("races", 0)
    best_time = stats.get("best_time")
    best_time_str = f"{best_time:.2f}s" if best_time is not None else "N/A"

    embed = discord.Embed(
        title=f"🏆 Race Stats for {member.display_name}",
        color=discord.Color.blue()
    )
    embed.add_field(name="Total Wins", value=str(wins), inline=True)
    embed.add_field(name="Total Races", value=str(races), inline=True)
    embed.add_field(name="Best Time", value=best_time_str, inline=True)

    await ctx.send(embed=embed)



@bot.hybrid_command()
async def chaoage(ctx):
    """Check how long until your Chao ages up, hatches, or reincarnates."""
    print(f"[COMMAND] !chaoage by {ctx.author}")

    user_id = str(ctx.author.id)
    user_chao_list = []
    if user_id in user_chao_data and isinstance(user_chao_data[user_id], dict):
        user_chao_list = user_chao_data[user_id].get("chao", [])

    if not user_chao_list:
        await ctx.send("You don't have any Chao!")
        return

    # --- Helper functions ---
    def get_chao_type(chao):
        return chao.get("type_") or chao.get("type") or "Monotone"

    def get_chao_emoji(chao):
        if chao.get("is_egg", False):
            return chao_emojis.get("egg", "<:chaoegg:1408215309592035329>")
        colour = chao.get("colour", "default").lower()
        ctype = get_chao_type(chao).lower()
        return chao_emojis.get(colour, {}).get(ctype, chao_emojis["default"]["monotone"])

    def get_chao_display_name(chao, for_menu=False):
        """Return display name: 'Egg' for eggs, Colour + Type for others."""
        if chao.get("is_egg", False):
            return "Egg"
        colour = chao.get("colour", "Unknown")
        ctype = get_chao_type(chao)
        return f"{colour} {ctype.capitalize()}"

    def get_chao_title_name(chao):
        """Use nickname for non-eggs, 'Egg' for eggs."""
        if chao.get("is_egg", False):
            return "Egg"
        return chao.get("nickname", get_chao_display_name(chao))

    # --- Let user pick their Chao if multiple ---
    if len(user_chao_list) > 1:
        description_lines = []
        for i, c in enumerate(user_chao_list):
            if c.get("is_egg", False):
                display_name = "Egg"
            else:
                nickname = c.get("nickname")
                display_name = nickname if nickname else get_chao_display_name(c)
            description_lines.append(f"{i+1}. {get_chao_emoji(c)} {display_name}")

        embed = discord.Embed(
            title="Which Chao do you want to check?",
            description="\n".join(description_lines),
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)

        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel

        try:
            reply = await bot.wait_for("message", check=check, timeout=30)
            choice = int(reply.content) - 1
            if choice < 0 or choice >= len(user_chao_list):
                await ctx.send("Invalid choice!")
                return
        except (ValueError, asyncio.TimeoutError):
            await ctx.send("You did not choose a valid Chao in time!")
            return

        chao = user_chao_list[choice]
    else:
        chao = user_chao_list[0]

    now = time.time()
    age = chao.get("age", 0)
    last_age_time = chao.get("last_age_time", now)
    messages = chao.get("messages", 0)
    reincarnations = chao.get("reincarnations", 0)
    emoji = get_chao_emoji(chao)
    title_name = get_chao_title_name(chao)

    # --- Determine description ---
    if chao.get("is_egg", False):
        messages_remaining = max(0, chao.get("messages_until_hatch", 0))
        hatch_time = chao.get("hatch_time", now)
        time_remaining = max(0, hatch_time - now)
        # Discord timestamp formatting: use unix timestamp integers
        hatch_ts = int(hatch_time)
        desc_text = (
            f"Current state: {emoji} Egg\n"
            f"Messages until hatching: {messages_remaining}\n"
            f"Hatch time: <t:{hatch_ts}:F> (<t:{hatch_ts}:R>)"
        )
        footer = "Your Chao will hatch once the message or time requirement is reached."
    elif age < 2:
        time_remaining = max(0, 8*3600 - (now - last_age_time))
        messages_remaining = max(0, 100 - messages)
        messages_remaining_seconds = messages_remaining * 120
        age_desc = ['Baby', 'Child'][age]
        # compute absolute age-up time and show Discord timestamp
        ageup_time = now + time_remaining
        ageup_ts = int(ageup_time)
        desc_text = (
            f"Current age: {age_desc}\n"
            f"ETA to age-up: <t:{ageup_ts}:F> (<t:{ageup_ts}:R>)\n"
            f"Messages until age-up: {messages_remaining} (~{int(messages_remaining_seconds//60)}m)"
        )
        footer = ("This Chao will most likely age up by TIME first."
                  if time_remaining < messages_remaining_seconds
                  else "This Chao will most likely age up by MESSAGES first. Messages are only counted every 2 minutes.")
    else:
        REINCARNATION_MESSAGES = 500
        messages_remaining = max(0, REINCARNATION_MESSAGES - messages)
        messages_remaining_seconds = messages_remaining * 120
        eta_messages = datetime.timedelta(seconds=int(messages_remaining_seconds))
        desc_text = (
            f"Current age: Adult\n"
            f"Messages until reincarnation: {messages_remaining} (~{str(eta_messages)})\n"
            f"Reincarnation count: {reincarnations}"
        )
        footer = "This Chao will reincarnate once the message requirement is reached."

    embed = discord.Embed(
        title=f"{title_name}'s Progress {emoji}",
        description=desc_text,
        color=colour_map.get(chao.get("colour", "").lower(), discord.Color.green())
    )
    embed.set_footer(text=footer)

    await ctx.send(embed=embed)


@bot.hybrid_command()
async def agequeue(ctx):
    """Founder-only: view the pending local age queue."""
    if ctx.author.id != FOUNDER_ID:
        return await ctx.send("<:twisty:1407499302301073418> Only the bot founder can view the age queue.")
    queue = load_local_age_queue()
    if not queue:
        return await ctx.send("Local age queue is empty.")
    lines = []
    for i, item in enumerate(queue, start=1):
        uid = item.get('user_id')
        st = item.get('scheduled_time', 0)
        t = datetime.datetime.fromtimestamp(st).isoformat()
        chs = item.get('chao_indices', [])
        lines.append(f"{i}. User {uid} — scheduled: {t} — chao_indices: {chs}")
    # Send as a file if too long
    text = "\n".join(lines)
    if len(text) > 1900:
        path = os.path.join(script_dir, "local_age_queue_preview.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        await ctx.send(file=discord.File(path))
        try:
            os.remove(path)
        except Exception:
            pass
    else:
        await ctx.send(f"Pending age queue:\n{text}")


@bot.hybrid_command()
async def flushage(ctx):
    """Founder-only: force flush due entries in the local age queue immediately."""
    if ctx.author.id != FOUNDER_ID:
        return await ctx.send("<:twisty:1407499302307171835> Only the bot founder can flush the age queue.")
    await ctx.send("Flushing local age queue now...")
    try:
        await flush_age_queue_once()
        await ctx.send("Flush complete. Check logs for details.")
    except Exception as e:
        await ctx.send(f"Flush failed: {e}")

@bot.hybrid_command()
async def nickname(ctx, *, new_name=None):
    """Change the nickname of one of your Chao."""
    print(f"[COMMAND] !nickname by {ctx.author} new_name: {new_name}")
    user_id = str(ctx.author.id)
    chao_list = []
    if user_id in user_chao_data and isinstance(user_chao_data[user_id], dict):
        chao_list = user_chao_data[user_id].get("chao", [])

    if not chao_list:
        await ctx.send("<:twisty:1407499302301073418> You don't have any Chao to nickname!")
        return

    if not new_name or new_name.strip() == "":
        await ctx.send(f"<:fa:1408217566660395028> Usage: `/nickname new_name:<new name>` or `!nickname <new name>` — specify the new nickname.")
        return

    # --- Helper to get type safely ---
    def get_chao_type(chao):
        return chao.get("type_") or chao.get("type") or "monotone"

    # --- Helper to get emoji ---
    def get_chao_emoji(chao):
        if chao.get("is_egg", False):
            return chao_emojis.get("egg", "<:chaoegg:1408215309592035329>")
        colour = chao.get("colour", "default").lower()
        ctype = get_chao_type(chao).lower()
        return chao_emojis.get(colour, {}).get(ctype, chao_emojis["default"]["monotone"])

    # Filter out eggs for renaming
    hatched_chao = [c for c in chao_list if not c.get("is_egg", False)]
    if not hatched_chao:
        await ctx.send("<:twisty:1407499302301073418> You don't have any hatched Chao to rename!")
        return

    # Show Chao list for user to pick one
    description_lines = []
    for i, c in enumerate(hatched_chao):
        emoji = get_chao_emoji(c)
        chao_name = c.get("nickname", "Unknown")
        status = f"{c.get('colour', 'Unknown')} {c.get('alignment_', 'Unknown')}"
        description_lines.append(f"{i+1}. {emoji} {chao_name} ({status})")

    embed = discord.Embed(
        title="Which Chao do you want to rename?",
        description="\n".join(description_lines),
        color=discord.Color.green()
    )
    await ctx.send(embed=embed)

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel

    try:
        reply = await bot.wait_for("message", check=check, timeout=30)
        choice = int(reply.content) - 1
        if choice < 0 or choice >= len(hatched_chao):
            await ctx.send("<:twisty:1407499302301073418> Invalid choice!")
            return
    except (ValueError, asyncio.TimeoutError):
        await ctx.send("<:twisty:1407499302301073418> You did not choose a valid Chao in time!")
        return

    chosen_chao = hatched_chao[choice]
    old_name = chosen_chao.get("nickname", "Unknown")
    chosen_chao["nickname"] = new_name.strip()

    # Save updated Chao data
    enqueue_chao_save(user_id)

    await ctx.send(f"✅ Your Chao **{old_name}** has been renamed to **{chosen_chao['nickname']}**!")


# --- INVENTORY COMMAND ---
@bot.hybrid_command()
async def inventory(ctx):
    """Check your item inventory."""
    print(f"[COMMAND] !inventory by {ctx.author}")
    user_id = ctx.author.id
    inv = get_user_inventory(user_id)

    if not inv or all(v <= 0 for v in inv.values()):
        await ctx.send("<:sadchao:1407501406260297869> Your inventory is empty!")
        return

    embed = discord.Embed(
        title=f"{ctx.author.display_name}'s Inventory",
        color=discord.Color.green()
    )

    for category, items in shop_items.items():
        value = ""
        for item in items:
            key = item['name'].lower()
            if key in inv and inv[key] > 0:
                value += f"{item['emoji']} **{item['name']}** — {inv[key]} owned\n"
        if value:
            embed.add_field(name=category, value=value, inline=False)

    await ctx.send(embed=embed)



@bot.hybrid_command()
async def fortuneteller(ctx):
    """Consult a fortune teller for alignment chances of a baby/child Chao (not adults)."""
    user_id = str(ctx.author.id)
    data = user_chao_data.get(user_id, {})
    chao_list = data.get("chao", []) if isinstance(data, dict) else []
    if not chao_list:
        await ctx.send("<:sadchao:1407501406260297869> You have no Chao.")
        return

    # All non-egg Chao
    candidates = [c for c in chao_list if not c.get("is_egg", False)]
    if not candidates:
        await ctx.send("<:sadchao:1407501406260297869> You have no Chao.")
        return

    def get_alignment(c):
        """Return numeric alignment in [-10,10] based on c['alignment'] if possible.
        If it's a label string, map hero/dark/neutral to 10/-10/0 respectively.
        """
        val = c.get("alignment", 0)
        try:
            a = int(val)
        except (ValueError, TypeError):
            label = str(val if val is not None else c.get("alignment_", "")).strip().lower()
            if label == "dark":
                a = -10
            elif label == "hero":
                a = 10
            else:
                a = 0
        return max(-10, min(10, a))

    options = "\n".join(
        f"{i+1}. {get_chao_emoji(c)} {c.get('nickname','Unnamed')}"
        for i, c in enumerate(candidates)
    )
    await ctx.send(embed=discord.Embed(title="<:fortuneteller:1410058038492139672> Choose a Chao for the reading", description=options, color=discord.Color.purple()))

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel

    try:
        reply = await bot.wait_for("message", check=check, timeout=30)
        idx = int(reply.content) - 1
        if idx < 0 or idx >= len(candidates):
            await ctx.send("<:fortuneteller:1410058038492139672> That's not a valid choice.")
            return
    except (ValueError, asyncio.TimeoutError):
        await ctx.send("<:fortuneteller:1410058038492139672> No reading was made.")
        return

    ch = candidates[idx]
    nickname = ch.get('nickname', 'Unnamed')
    fav_fruit = ch.get('favourite_fruit', 'Unknown')
    if ch.get('age', 0) < 2:
        a = get_alignment(ch)
        # Use the same model as adult transformation (lines 853-865):
        p_hero = max(0, a) / 10.0
        p_dark = max(0, -a) / 10.0
        p_neutral = max(0.0, 1.0 - p_hero - p_dark)
        dark_p = round(p_dark * 100)
        neutral_p = round(p_neutral * 100)
        hero_p = round(p_hero * 100)
        embed = discord.Embed(
            title=f"<:fortuneteller:1410058038492139672> Fortune for {nickname}",
            description=(
                f"Dark: {dark_p}%\nNeutral: {neutral_p}%\nHero: {hero_p}%\n\n"
                f"Your Chao's favourite fruit is {fav_fruit}!"
            ),
            color=discord.Color.dark_purple(),
        )
    else:
        embed = discord.Embed(
            title=f"<:fortuneteller:1410058038492139672> Fortune for {nickname}",
            description=(
                f"Your Chao's favourite fruit is {fav_fruit}!"
            ),
            color=discord.Color.dark_purple(),
        )
    await ctx.send(embed=embed)


# --- PET COMMAND STAT LOGIC ---
@bot.hybrid_command()
async def pet(ctx):
    """Pet one of your Chao to increase its happiness and earn rings. 30-minute cooldown per Chao."""
    user_id = str(ctx.author.id)
    now = time.time()
    data = user_chao_data.get(user_id, {})
    chao_list = data.get("chao", []) if isinstance(data, dict) else []

    # Exclude eggs from being pettable
    pettable = [c for c in chao_list if not c.get('is_egg', False)]
    if not pettable:
        await ctx.send("<:sadchao:1407501406260297869> You have no Chao that can be petted right now (eggs cannot be petted).")
        return

    # Let them pick which Chao to pet
    options = []
    for i, c in enumerate(pettable):
        last_pet = c.get('last_pet', 0)
        cd = max(0, 1800 - (now - last_pet))
        opt = f"{i+1}. {get_chao_emoji(c)} {c.get('nickname','Unnamed')}"
        if cd > 0:
            h = int(cd // 3600)
            m = int((cd % 3600) // 60)
            if h > 0:
                opt += f" (pettable in {h}h {m}m)"
            else:
                opt += f" (pettable in {m}m)"
        options.append(opt)
    await ctx.send(embed=discord.Embed(
        title="<a:petchao:1410396660001013841> Which Chao do you want to pet?",
        description="\n".join(options),
        color=discord.Color.green()
    ))

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel

    try:
        reply = await bot.wait_for("message", check=check, timeout=30)
        idx = int(reply.content) - 1
        if idx < 0 or idx >= len(pettable):
            await ctx.send("<:twisty:1407499302301073418> Invalid choice.")
            return
    except (ValueError, asyncio.TimeoutError):
        await ctx.send("<:twisty:1407499302301073418> No Chao was petted.")
        return

    ch = pettable[idx]
    last_pet = ch.get('last_pet', 0)
    cd = max(0, 1800 - (now - last_pet))
    if cd > 0:
        h = int(cd // 3600)
        m = int((cd % 3600) // 60)
        if h > 0:
            await ctx.send(f"<:twisty:1407499302301073418> You can pet **{ch.get('nickname','Unnamed')}** again in {h}h {m}m.")
        else:
            await ctx.send(f"<:twisty:1407499302301073418> You can pet **{ch.get('nickname','Unnamed')}** again in {m} minutes.")
        return

    nickname = ch.get("nickname", "Unnamed")
    ch['last_pet'] = now
    # Reward rings
    rings = random.randint(1, 500)
    # Award rings into the global balances (persisted by save_balances)
    add_balance(user_id, rings)
    # Increase happiness by 20%, clamp to 100
    ch["happiness"] = min(100, ch.get("happiness", 100) + 20)
    # Persist chao changes to disk
    enqueue_chao_save(user_id)
    await ctx.send(
        f"<a:petchao:1410396660001013841> You petted **{nickname}** and got **{rings} rings**! (+20% happiness)"
    )

# --- PLAY COMMAND STAT LOGIC ---
@bot.hybrid_command()
async def play(ctx):
    """Play with one of your Chao to boost happiness. 6-hour cooldown per Chao."""
    user_id = str(ctx.author.id)
    now = time.time()
    data = user_chao_data.get(user_id, {})
    chao_list = data.get("chao", []) if isinstance(data, dict) else []

    # Exclude eggs from being playable
    non_eggs = [c for c in chao_list if not c.get('is_egg', False)]
    if not non_eggs:
        await ctx.send("<:sadchao:1407501406260297869> You have no Chao that can be played right now (eggs cannot be played).")
        return

    # Let them pick which Chao to play with
    options = []
    cooldowns = []
    for i, c in enumerate(non_eggs):
        last_play = c.get('last_play', 0)
        cd = max(0, 21600 - (now - last_play))
        cooldowns.append(cd)
        opt = f"{i+1}. {get_chao_emoji(c)} {c.get('nickname','Unnamed')}"
        if cd > 0:
            h = int(cd // 3600)
            m = int((cd % 3600) // 60)
            if h > 0:
                opt += f" (playable in {h}h {m}m)"
            else:
                opt += f" (playable in {m}m)"
        options.append(opt)
    play_emotes = [
        "<a:chaobox:1410399344523214848>",
        "<a:chaoddr:1410399346389811261>",
        "<a:chaoexplode:1410399336373948426>",
        "<a:chaoguitar:1410399337812594719>",
        "<a:chaohop:1410399340022726727>",
        "<a:chaohyper:1410399341604245518>",
        "<a:chaotouchgrass:1410399343130841088>"
    ]
    import random
    play_title_emote = random.choice(play_emotes)
    await ctx.send(embed=discord.Embed(
        title=f"{play_title_emote} Which Chao do you want to play with?",
        description="\n".join(options),
        color=discord.Color.blue()
    ))

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel

    try:
        reply = await bot.wait_for("message", check=check, timeout=30)
        idx = int(reply.content) - 1
        if idx < 0 or idx >= len(non_eggs):
            await ctx.send("<:twisty:1407499302301073418> Invalid choice.")
            return
    except (ValueError, asyncio.TimeoutError):
        await ctx.send("<:twisty:1407499302301073418> No Chao was played with.")
        return

    ch = non_eggs[idx]
    last_play = ch.get('last_play', 0)
    cd = max(0, 21600 - (now - last_play))
    if cd > 0:
        h = int(cd // 3600)
        m = int((cd % 3600) // 60)
        if h > 0:
            await ctx.send(f"<:twisty:1407499302301073418> You can play with **{ch.get('nickname','Unnamed')}** again in {h}h {m}m.")
        else:
            await ctx.send(f"<:twisty:1407499302301073418> You can play with **{ch.get('nickname','Unnamed')}** again in {m} minutes.")
        return

    nickname = ch.get("nickname", "Unnamed")
    ch['last_play'] = now
    # Reward rings (larger for play)
    rings = random.randint(250, 5000)
    # Award rings into the global balances (persisted by save_balances)
    add_balance(user_id, rings)
    # Increase happiness by 50%, clamp to 100
    ch["happiness"] = min(100, ch.get("happiness", 100) + 50)
    # Decrease energy by 20%, clamp to 0
    ch["energy"] = max(0, ch.get("energy", 100) - 20)
    # Persist chao changes to disk
    enqueue_chao_save(user_id)
    await ctx.send(
        f"{play_title_emote} You played with **{nickname}** and earned **{rings} rings**! (+50% happiness, -20% energy)"
    )

@bot.hybrid_command()
async def checkchao(ctx):
    """Check all your Chao's stats (energy, happiness, hunger) with reaction-based pages."""
    user_id = str(ctx.author.id)
    data = user_chao_data.get(user_id, {})
    chao_list = data.get("chao", []) if isinstance(data, dict) else []

    if not chao_list:
        await ctx.send("<:sadchao:1407501406260297869> You have no Chao.")
        return

    # Helper to create embeds for pages
    def make_embed(page):
        start = page * 5
        end = start + 5
        chao_slice = chao_list[start:end]
        embed = discord.Embed(
            title=f"Your Chao (Page {page+1}/{(len(chao_list)-1)//5+1})",
            color=discord.Color.blue()
        )
        for i, c in enumerate(chao_slice, start=start+1):
            embed.add_field(
                name=f"{i}. {get_chao_emoji(c)} {c.get('nickname', 'Unnamed')}",
                value=(
                    f"**Energy:** {c.get('energy', 100)}%\n"
                    f"**Happiness:** {c.get('happiness', 100)}%\n"
                    f"**Hunger:** {c.get('hunger', 0)}%"
                ),
                inline=False
            )
        return embed

    page = 0
    message = await ctx.send(embed=make_embed(page))

    # Add navigation reactions
    if len(chao_list) > 5:
        await message.add_reaction("⬅️")
        await message.add_reaction("➡️")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == message.id
            and str(reaction.emoji) in ["⬅️", "➡️"]
        )

    while True:
        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check)
            if str(reaction.emoji) == "⬅️" and page > 0:
                page -= 1
                await message.edit(embed=make_embed(page))
            elif str(reaction.emoji) == "➡️" and (page + 1) * 5 < len(chao_list):
                page += 1
                await message.edit(embed=make_embed(page))

            # Remove user's reaction to keep UI clean
            await message.remove_reaction(reaction, user)
        except asyncio.TimeoutError:
            # Timeout -> stop listening, clear reactions
            await message.clear_reactions()
            break


@bot.hybrid_command(aliases=["energy"])
async def chaoenergy(ctx, index: int = None):
    """Show your Chao energy levels and ETA to full."""
    user_id = str(ctx.author.id)
    data = user_chao_data.get(user_id, {}) if isinstance(user_chao_data.get(user_id, {}), dict) else {}
    chao_list = data.get("chao", []) if isinstance(data.get("chao", []), list) else []

    if not chao_list:
        await ctx.send("<:sadchao:1407501406260297869> You have no Chao.")
        return

    def eta_text(ch):
        energy = int(ch.get("energy", 100))
        if energy >= 100:
            return "Full"
        remaining = 100 - energy
        ticks = (remaining + 4) // 5  # each tick = 5%
        minutes = ticks * 30
        hours = minutes // 60
        mins = minutes % 60
        if hours > 0:
            return f"{hours}h {mins}m until full."
        return f"{mins}m until full."

    if index is None:
        lines = []
        for i, c in enumerate(chao_list, start=1):
            if c.get("is_egg", False):
                lines.append(f"{i}. <:chaoegg:1408215309592035329> Egg — hatching")
                continue
            name = c.get("nickname", "Unnamed")
            energy = int(c.get("energy", 100))
            lines.append(f"{i}. {get_chao_emoji(c)} {name}: {energy}% — {eta_text(c)}")

        embed = discord.Embed(title=f"{ctx.author.display_name}'s Chao Energy", description="\n".join(lines), color=discord.Color.green())
        await ctx.send(embed=embed)
        return

    # Specific index requested
    idx = index - 1
    if idx < 0 or idx >= len(chao_list):
        await ctx.send("<:twisty:1407499302301073418> Invalid Chao index.")
        return

    c = chao_list[idx]
    if c.get("is_egg", False):
        await ctx.send("That entry is an egg — it has no energy yet.")
        return

    energy = int(c.get("energy", 100))
    eta = eta_text(c)
    embed = discord.Embed(title=f"{c.get('nickname','Unnamed')}'s Energy {get_chao_emoji(c)}", color=discord.Color.blue())
    embed.add_field(name="Energy", value=f"{energy}%", inline=True)
    embed.add_field(name="ETA to full", value=eta, inline=True)
    last_update = c.get("last_energy_update")
    if last_update:
        try:
            delta = int(time.time() - float(last_update))
            embed.set_footer(text=f"Last regen check {delta}s ago")
        except Exception:
            pass

    await ctx.send(embed=embed)


@bot.hybrid_command(aliases=["hunger"])
async def chaohunger(ctx, index: int = None):
    """Show your Chao hunger levels. Usage: !chaohunger [index]"""
    user_id = str(ctx.author.id)
    data = user_chao_data.get(user_id, {}) if isinstance(user_chao_data.get(user_id, {}), dict) else {}
    chao_list = data.get("chao", []) if isinstance(data.get("chao", []), list) else []

    if not chao_list:
        await ctx.send("<:sadchao:1407501406260297869> You have no Chao.")
        return

    def eta_text_hunger(ch):
        # No global hunger regen rule defined; report N/A
        return "N/A"

    if index is None:
        lines = []
        for i, c in enumerate(chao_list, start=1):
            if c.get("is_egg", False):
                lines.append(f"{i}. Egg — hatching")
                continue
            name = c.get("nickname", "Unnamed")
            hunger = int(c.get("hunger", 0))
            lines.append(f"{i}. {get_chao_emoji(c)} {name}: {hunger}%.")

        embed = discord.Embed(title=f"{ctx.author.display_name}'s Chao Hunger", description="\n".join(lines), color=discord.Color.orange())
        await ctx.send(embed=embed)
        return

    idx = index - 1
    if idx < 0 or idx >= len(chao_list):
        await ctx.send("<:twisty:1407499302301073418> Invalid Chao index.")
        return

    c = chao_list[idx]
    if c.get("is_egg", False):
        await ctx.send("That entry is an egg — it has no hunger yet.")
        return

    hunger = int(c.get("hunger", 0))
    eta = eta_text_hunger(c)
    embed = discord.Embed(title=f"{c.get('nickname','Unnamed')}'s Hunger {get_chao_emoji(c)}", color=discord.Color.orange())
    embed.add_field(name="Hunger", value=f"{hunger}%", inline=True)
    embed.add_field(name="ETA", value=eta, inline=True)
    await ctx.send(embed=embed)

@bot.hybrid_command(aliases=["happiness"])
async def chaohappiness(ctx, index: int = None):
    """Show your Chao happiness levels. Usage: !chaohappiness [index]"""
    user_id = str(ctx.author.id)
    data = user_chao_data.get(user_id, {}) if isinstance(user_chao_data.get(user_id, {}), dict) else {}
    chao_list = data.get("chao", []) if isinstance(data.get("chao", []), list) else []

    if not chao_list:
        await ctx.send("<:sadchao:1407501406260297869> You have no Chao.")
        return

    def eta_text_happiness(ch):
        # No global happiness regen rule defined; report N/A
        return "N/A"

    if index is None:
        lines = []
        for i, c in enumerate(chao_list, start=1):
            if c.get("is_egg", False):
                lines.append(f"{i}. Egg — hatching")
                continue
            name = c.get("nickname", "Unnamed")
            happiness = int(c.get("happiness", 100))
            lines.append(f"{i}. {get_chao_emoji(c)} {name}: {happiness}%.")

        embed = discord.Embed(title=f"{ctx.author.display_name}'s Chao Happiness", description="\n".join(lines), color=discord.Color.purple())
        await ctx.send(embed=embed)
        return

    idx = index - 1
    if idx < 0 or idx >= len(chao_list):
        await ctx.send("<:twisty:1407499302301073418> Invalid Chao index.")
        return

    c = chao_list[idx]
    if c.get("is_egg", False):
        await ctx.send("That entry is an egg — it has no happiness yet.")
        return

    happiness = int(c.get("happiness", 100))
    eta = eta_text_happiness(c)
    embed = discord.Embed(title=f"{c.get('nickname','Unnamed')}'s Happiness {get_chao_emoji(c)}", color=discord.Color.purple())
    embed.add_field(name="Happiness", value=f"{happiness}%", inline=True)
    embed.add_field(name="ETA", value=eta, inline=True)
    await ctx.send(embed=embed)

@bot.hybrid_command()
async def feedall(ctx):
    """Feed all your hatched Chao at once.
    Costs 2000 rings per Chao and uses 4 fruit (Triangle/Square/Round) per Chao.
    Fails if you don't have enough of those fruits combined or not enough rings.
    """
    user_id = str(ctx.author.id)
    # Get user's chao
    user_data = user_chao_data.get(user_id, {}) if isinstance(user_chao_data.get(user_id, {}), dict) else {}
    chao_list = user_data.get("chao", []) if isinstance(user_data.get("chao", []), list) else []

    # Only hatched (non-egg) Chao are feedable
    feedable = [c for c in chao_list if not c.get("is_egg", False)]
    count = len(feedable)
    if count == 0:
        await ctx.send("<:sadchao:1407501406260297869> You have no hatched Chao to feed!")
        return

    # Requirements
    fruits_needed = count * 4
    cost = count * 2000

    # Inventory keys for the allowed fruits
    fruit_keys = ["triangle fruit", "square fruit", "round fruit"]
    inv = get_user_inventory(user_id)
    available_fruits = sum(int(inv.get(k, 0)) for k in fruit_keys)

    if available_fruits < fruits_needed:
        await ctx.send(f"<:twisty:1407499302301073418> You need {fruits_needed} total Triangle/Square/Round Fruit to feed {count} Chao, but you only have {available_fruits}.")
        return

    # Check rings
    user_balance = get_balance(ctx.author.id)
    if user_balance < cost:
        await ctx.send(f"<:twisty:1407499302301073418> You need {cost} rings to feed {count} Chao, but you only have {user_balance} rings.")
        return

    # Ask for confirmation before spending
    await ctx.send(f"This will cost **{cost} rings** and **{fruits_needed}** Triangle/Square/Round Fruit to feed your {count} Chao(s). Type `yes` to confirm or `no` to cancel.")

    def _check_confirm(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() in ["yes", "y", "no", "n"]

    try:
        reply = await bot.wait_for("message", check=_check_confirm, timeout=30)
    except asyncio.TimeoutError:
        await ctx.send("⏳ Feed canceled (no response).")
        return

    if reply.content.lower() not in ("yes", "y"):
        await ctx.send("❌ Feed canceled.")
        return

    # Re-load inventory and balance to ensure availability hasn't changed
    inv = get_user_inventory(user_id)
    available_fruits = sum(int(inv.get(k, 0)) for k in fruit_keys)
    user_balance = get_balance(ctx.author.id)
    if available_fruits < fruits_needed:
        await ctx.send(f"<:twisty:1407499302301073418> You no longer have enough fruit ({available_fruits}) to complete this action.")
        return
    if user_balance < cost:
        await ctx.send(f"<:twisty:1407499302301073418> You no longer have enough rings ({user_balance}) to complete this action.")
        return

    # Deduct rings
    add_balance(ctx.author.id, -cost)

    # Deduct fruits (consume in triangle -> square -> round order)
    to_consume = fruits_needed
    for key in fruit_keys:
        have = int(inv.get(key, 0))
        use = min(have, to_consume)
        if use > 0:
            inv[key] = have - use
            if inv[key] <= 0:
                inv.pop(key, None)
            to_consume -= use
        if to_consume <= 0:
            break

    # Sanity: to_consume should be zero here because we checked availability
    if to_consume > 0:
        # Unexpected; refund rings and abort
        add_balance(ctx.author.id, cost)
        await ctx.send("<:twisty:1407499302301073418> An error occurred while consuming fruits. No changes were made.")
        return

    # Save updated inventory
    user_inventory[user_id] = inv
    save_user_inventory()

    # Apply feed effects: set hunger to 0 for each feedable Chao
    for c in feedable:
        try:
            c["hunger"] = 0
        except Exception:
            c.setdefault("hunger", 0)

    # Persist chao data
    enqueue_chao_save(user_id)

    await ctx.send(f"🍎 Fed {count} Chao(s). Spent {cost} rings and used {fruits_needed} Triangle/Square/Round Fruit. All fed Chao are now full.")


@bot.hybrid_command()
async def playall(ctx):
    """Play with all your hatched Chao.
    Costs 3000 rings per Chao. Does not award rings. Sets happiness to 100 for all hatched Chao.
    """
    user_id = str(ctx.author.id)
    user_data = user_chao_data.get(user_id, {}) if isinstance(user_chao_data.get(user_id, {}), dict) else {}
    chao_list = user_data.get("chao", []) if isinstance(user_data.get("chao", []), list) else []

    playables = [c for c in chao_list if not c.get("is_egg", False)]
    count = len(playables)
    if count == 0:
        await ctx.send("<:sadchao:1407501406260297869> You have no hatched Chao to play with!")
        return

    cost = count * 3000
    user_balance = get_balance(ctx.author.id)
    if user_balance < cost:
        await ctx.send(f"<:twisty:1407499302301073418> You need {cost} rings to play with {count} Chao(s), but you only have {user_balance} rings.")
        return

    # Confirmation
    await ctx.send(f"This will cost **{cost} rings** to play with your {count} Chao(s). Type `yes` to confirm or `no` to cancel.")

    def _check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() in ["yes", "y", "no", "n"]

    try:
        reply = await bot.wait_for("message", check=_check, timeout=30)
    except asyncio.TimeoutError:
        await ctx.send("⏳ Play canceled (no response).")
        return

    if reply.content.lower() not in ("yes", "y"):
        await ctx.send("❌ Play canceled.")
        return

    # Re-check balance
    user_balance = get_balance(ctx.author.id)
    if user_balance < cost:
        await ctx.send(f"<:twisty:1407499302301073418> You no longer have enough rings ({user_balance}) to complete this action.")
        return

    # Deduct rings
    add_balance(ctx.author.id, -cost)

    # Apply play effects: set happiness to 100 and update last_play timestamp
    now = time.time()
    for c in playables:
        try:
            c["happiness"] = 100
            c["last_play"] = now
        except Exception:
            c.setdefault("happiness", 100)
            c["last_play"] = now

    # Use the same playful emotes as the single-play command for consistency
    play_emotes = [
        "<a:chaobox:1410399344523214848>",
        "<a:chaoddr:1410399346389811261>",
        "<a:chaoexplode:1410399336373948426>",
        "<a:chaoguitar:1410399337812594719>",
        "<a:chaohop:1410399340022726727>",
        "<a:chaohyper:1410399341604245518>",
        "<a:chaotouchgrass:1410399343130841088>"
    ]
    play_title_emote = random.choice(play_emotes)

    enqueue_chao_save(user_id)

    await ctx.send(f"{play_title_emote} Played with {count} Chao(s). Spent {cost} rings. All played Chao are now at 100% happiness.")



@earn.error
async def earn_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        minutes = int(error.retry_after // 60)
        seconds = int(error.retry_after % 60)
        await ctx.send(f"⏳ You need to wait {minutes}m {seconds}s before using `/earn` or `!earn` again.")

@spawnchao.error
async def spawnchao_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("<:twisty:1407499302301073418> You must be an administrator to use this command!")

@setbalance.error
async def setbalance_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("<:twisty:1407499302301073418> You must be an administrator to use this command!")

bot.run(TOKEN)