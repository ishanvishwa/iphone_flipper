"""
Facebook Marketplace iPhone Scraper for Perth, Australia

This script uses Playwright with stealth techniques to scrape iPhone listings
from Facebook Marketplace within a 100km radius of Perth.

IMPORTANT: This script is for educational purposes only. Use at your own risk.
Facebook's Terms of Service may prohibit automated scraping.
"""

import asyncio
import ipaddress
import json
import os
import random
import re
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any, Callable, Awaitable
from urllib.parse import urlparse, urlunparse

# Configuration
SEARCH_LOCATION = "Perth, Western Australia, Australia"
SEARCH_RADIUS_KM = 100
SEARCH_QUERIES = [
    "iPhone 12",
    "iPhone 12 Mini",
    "iPhone 12 Pro",
    "iPhone 12 Pro Max",
    "iPhone 13",
    "iPhone 13 Mini",
    "iPhone 13 Pro",
    "iPhone 13 Pro Max",
    "iPhone 14",
    "iPhone 14 Plus",
    "iPhone 14 Pro",
    "iPhone 14 Pro Max",
    "iPhone 15",
    "iPhone 15 Plus",
    "iPhone 15 Pro",
    "iPhone 15 Pro Max",
    "iPhone cracked screen",
    "iPhone broken",
    "iPhone repairable",
]

# Accessory-only suppression signals.
ACCESSORY_SIGNAL_KEYWORDS = (
    "iphone case",
    "phone case",
    "silicone case",
    "clear case",
    "wallet case",
    "otterbox",
    "screen protector",
    "tempered glass",
    "camera protector",
    "camera lens protector",
    "charging cable",
    "lightning cable",
    "usb c cable",
    "usb-c cable",
    "charger",
    "power adapter",
    "wall adapter",
    "car charger",
    "phone holder",
    "phone mount",
)

DEFAULT_ACCESSORY_KEYWORD_CSV = ", ".join(ACCESSORY_SIGNAL_KEYWORDS)
DEFAULT_ACCESSORY_MAX_PRICE = 120.0

ACCESSORY_ONLY_PHRASES = (
    "case only",
    "cover only",
    "accessory only",
    "phone not included",
    "iphone not included",
    "no phone included",
    "not a phone",
)

DEVICE_SALE_SIGNALS = (
    "battery health",
    "face id",
    "true tone",
    "truetone",
    "icloud",
    "unlocked",
    "for parts",
    "not working",
    "won't turn on",
    "imei",
    "screen cracked",
    "cracked screen",
    "back glass",
    "box included",
    "with box",
    "receipt",
)

MANUAL_LOGIN_REQUIRED_PREFIX = "MANUAL_LOGIN_REQUIRED:"
CHECKPOINT_URL_MARKERS = (
    "facebook.com/checkpoint",
    "/checkpoint/",
    "/checkpoint",
    "facebook.com/login",
    "/login/?next=",
    "facebook.com/two_factor",
    "facebook.com/security",
)
CHECKPOINT_TEXT_MARKERS = (
    "checkpoint",
    "confirm it's you",
    "confirm your identity",
    "security check",
    "we need to make sure",
    "your account is temporarily locked",
    "your account has been disabled",
    "log in to continue",
    "enter the code from your authentication app",
)

_ACCESSORY_FILTER_CACHE_TTL_SECONDS = 10.0
_accessory_filter_cache: Dict[str, Any] = {
    "loaded_at": 0.0,
    "keywords": tuple(ACCESSORY_SIGNAL_KEYWORDS),
    "max_price": float(DEFAULT_ACCESSORY_MAX_PRICE),
}


def _parse_env_int(raw_value: str | None, default: int, minimum: int = 1) -> int:
    try:
        parsed = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), parsed)


def _parse_env_bool(raw_value: str | None, default: bool = False) -> bool:
    value = str(raw_value or "").strip().lower()
    if not value:
        return bool(default)
    return value in {"1", "true", "yes", "on"}


def _parse_blocked_resource_types(raw_value: str | None) -> set[str]:
    default_types: set[str] = set()
    text = str(raw_value or "").strip()
    if not text:
        return default_types
    parsed = {token.strip().lower() for token in text.split(",") if token.strip()}
    return parsed or default_types


BROWSER_MAX_CONNECTIONS_PER_PROXY = _parse_env_int(
    os.getenv("BROWSER_MAX_CONNECTIONS_PER_PROXY"),
    default=32,
    minimum=1,
)
BROWSER_MAX_CONNECTIONS_PER_HOST = _parse_env_int(
    os.getenv("BROWSER_MAX_CONNECTIONS_PER_HOST"),
    default=6,
    minimum=1,
)
BROWSER_BLOCK_RESOURCE_TYPES = _parse_blocked_resource_types(
    os.getenv("BROWSER_BLOCK_RESOURCE_TYPES"),
)
FORCE_LOCAL_PROXY_BRIDGE = _parse_env_bool(
    os.getenv("FORCE_LOCAL_PROXY_BRIDGE"),
    default=False,
)
PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS = _parse_env_int(
    os.getenv("PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS"),
    default=1,
    minimum=1,
)
PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS = _parse_env_int(
    os.getenv("PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS"),
    default=30,
    minimum=1,
)
PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS = _parse_env_int(
    os.getenv("PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS"),
    default=15,
    minimum=1,
)
PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS = _parse_env_int(
    os.getenv("PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS"),
    default=8,
    minimum=2,
)


async def _detect_manual_login_required_state(page) -> Optional[str]:
    """Detect Facebook checkpoint/login challenge states that require operator intervention."""
    url_text = ""
    try:
        url_text = str(getattr(page, "url", "") or "").strip()
    except Exception:
        url_text = ""
    lowered_url = url_text.lower()
    for marker in CHECKPOINT_URL_MARKERS:
        if marker in lowered_url:
            return f"Facebook checkpoint/login URL detected: {url_text or marker}"

    try:
        body_text = await page.evaluate(
            "() => (document && document.body && document.body.innerText) ? document.body.innerText.slice(0, 12000) : ''"
        )
    except Exception:
        body_text = ""
    lowered_body = str(body_text or "").lower()
    for marker in CHECKPOINT_TEXT_MARKERS:
        if marker in lowered_body:
            return f"Facebook challenge text detected ('{marker}') at {url_text or 'unknown URL'}"
    return None

# Database setup
DB_PATH = Path(
    os.getenv("IPHONE_FLIPPER_DB_PATH", str(Path(__file__).parent / "listings.db"))
)

# Path to your real Chrome/Chromium user data directory
# This is the key to avoiding detection - use your REAL browser profile
USER_DATA_DIR = Path(__file__).parent / "browser_profile"


def init_db():
    """Initialize the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            id TEXT PRIMARY KEY,
            title TEXT,
            price REAL,
            location TEXT,
            url TEXT,
            description TEXT,
            seller_name TEXT,
            model TEXT,
            condition TEXT,
            max_buy_price REAL,
            potential_profit REAL,
            status TEXT DEFAULT 'new',
            created_at TEXT,
            updated_at TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT,
            message_type TEXT,
            message_text TEXT,
            timestamp TEXT,
            FOREIGN KEY (listing_id) REFERENCES listings(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS proxies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            proxy_type TEXT DEFAULT 'http',
            host TEXT NOT NULL,
            port INTEGER NOT NULL,
            username TEXT,
            password TEXT,
            country TEXT,
            status TEXT DEFAULT 'active',
            notes TEXT,
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(host, port, username)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fb_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_name TEXT,
            email TEXT,
            status TEXT DEFAULT 'ACTIVE',
            profile_path TEXT,
            user_agent TEXT,
            proxy_id INTEGER,
            cookies_json TEXT,
            last_login_at TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (proxy_id) REFERENCES proxies(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scraper_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT,
            updated_at TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS search_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            keywords TEXT NOT NULL UNIQUE,
            latitude REAL,
            longitude REAL,
            radius_km INTEGER,
            min_price INTEGER,
            max_price INTEGER,
            category TEXT,
            sort_by TEXT DEFAULT 'CREATION_TIME_DESCEND',
            is_active INTEGER DEFAULT 1,
            poll_interval_sec INTEGER DEFAULT 60,
            last_polled TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    # Lightweight schema migration for backwards compatibility.
    cursor.execute("PRAGMA table_info(listings)")
    listing_columns = [col[1] for col in cursor.fetchall()]
    listing_column_types = {
        "location": "TEXT",
        "description": "TEXT",
        "seller_name": "TEXT",
        "user_flag": "TEXT",
        "opened_at": "TEXT",
    }

    for column_name, column_type in listing_column_types.items():
        if column_name not in listing_columns:
            cursor.execute(f"ALTER TABLE listings ADD COLUMN {column_name} {column_type}")

    # Lightweight schema migration for account/proxy management.
    cursor.execute("PRAGMA table_info(fb_accounts)")
    account_columns = [col[1] for col in cursor.fetchall()]
    if account_columns and "proxy_id" not in account_columns:
        cursor.execute("ALTER TABLE fb_accounts ADD COLUMN proxy_id INTEGER")

    account_column_types = {
        "failure_count": "INTEGER DEFAULT 0",
        "cooldown_until": "TEXT",
        "last_scrape_started_at": "TEXT",
    }
    for column_name, column_type in account_column_types.items():
        if column_name not in account_columns:
            cursor.execute(f"ALTER TABLE fb_accounts ADD COLUMN {column_name} {column_type}")

    now_iso = datetime.now().isoformat()
    default_settings = {
        "monitor_interval_minutes": "30",
        "monitor_jitter_seconds": "45",
        "account_min_reuse_seconds": "30",
        "scrape_delay_min_seconds": "5",
        "scrape_delay_max_seconds": "12",
        "scrape_scroll_target_cards": "180",
        "scrape_scroll_max_rounds": "14",
        "notify_profitable_only": "1",
        "max_queries_per_run": str(len(SEARCH_QUERIES)),
        "accessory_filter_keywords": DEFAULT_ACCESSORY_KEYWORD_CSV,
        "accessory_filter_max_price": str(int(DEFAULT_ACCESSORY_MAX_PRICE)),
        "proxy_api_url": "",
        "proxy_api_key": "",
        "proxy_api_auth_header": "Authorization",
        "proxy_api_timeout_seconds": "20",
        "proxy_api_default_type": "http",
        "proxy_api_default_country": "",
        "active_scraper_account_id": "",
        "last_scraper_account_id": "",
        "server_sync_enabled": "0",
        "server_api_base_url": "https://api.iphoneguy.com.au",
        "server_api_token": "",
        "server_sync_poll_seconds": "2",
        "server_sync_since_id": "0",
        "server_max_concurrent_proxy_connections": "5",
        "server_ssh_enabled": "1",
        "server_ssh_user": "ubuntu",
        "server_ssh_host": "15.235.185.32",
        "server_ssh_project_dir": "/home/ubuntu/iphone-flipper-server/server",
        "server_monitor_worker_services": "worker worker_2 worker_3",
    }
    for key, value in default_settings.items():
        cursor.execute(
            """
            INSERT OR IGNORE INTO scraper_settings (setting_key, setting_value, updated_at)
            VALUES (?, ?, ?)
            """,
            (key, value, now_iso),
        )

    cursor.execute("SELECT keywords FROM search_queries")
    existing_keywords = {str(row[0]).strip().lower() for row in cursor.fetchall() if row[0]}
    for query in SEARCH_QUERIES:
        normalized_query = str(query).strip()
        if not normalized_query or normalized_query.lower() in existing_keywords:
            continue
        cursor.execute(
            """
            INSERT INTO search_queries (
                name, keywords, latitude, longitude, radius_km, category, sort_by,
                is_active, poll_interval_sec, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_query,
                normalized_query,
                -31.9505,
                115.8605,
                SEARCH_RADIUS_KM,
                "electronics",
                "CREATION_TIME_DESCEND",
                1,
                60,
                now_iso,
                now_iso,
            ),
        )

    conn.commit()
    conn.close()


def load_runtime_scraper_settings() -> Dict[str, int]:
    """Load scraper runtime settings persisted in SQLite."""
    defaults = {
        "scrape_delay_min_seconds": 5,
        "scrape_delay_max_seconds": 12,
        "scrape_scroll_target_cards": 180,
        "scrape_scroll_max_rounds": 14,
        "max_queries_per_run": len(SEARCH_QUERIES),
    }

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT setting_key, setting_value
        FROM scraper_settings
        WHERE setting_key IN (?, ?, ?, ?, ?)
        """,
        tuple(defaults.keys()),
    )
    rows = cursor.fetchall()
    conn.close()

    settings = dict(defaults)
    for key, raw_value in rows:
        try:
            settings[key] = int(str(raw_value).strip())
        except (TypeError, ValueError):
            continue

    settings["scrape_delay_min_seconds"] = max(0, settings["scrape_delay_min_seconds"])
    settings["scrape_delay_max_seconds"] = max(settings["scrape_delay_min_seconds"], settings["scrape_delay_max_seconds"])
    settings["scrape_scroll_target_cards"] = max(40, min(settings["scrape_scroll_target_cards"], 1000))
    settings["scrape_scroll_max_rounds"] = max(2, min(settings["scrape_scroll_max_rounds"], 50))
    settings["max_queries_per_run"] = max(1, min(settings["max_queries_per_run"], len(SEARCH_QUERIES)))
    return settings


def get_scraper_setting(key: str, default: str = "") -> str:
    """Get a persisted scraper setting value from SQLite."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT setting_value FROM scraper_settings WHERE setting_key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return str(row[0]) if row and row[0] is not None else str(default)


def set_scraper_setting(key: str, value: str):
    """Upsert scraper setting value in SQLite."""
    init_db()
    now_iso = datetime.now().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO scraper_settings (setting_key, setting_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
        """,
        (key, str(value), now_iso),
    )
    conn.commit()
    conn.close()
    if key in {"accessory_filter_keywords", "accessory_filter_max_price"}:
        _accessory_filter_cache["loaded_at"] = 0.0


def _parse_accessory_keywords_csv(raw_keywords: Any) -> Tuple[str, ...]:
    text = str(raw_keywords or "").strip().lower()
    if not text:
        return tuple()

    keywords: List[str] = []
    seen: set[str] = set()
    for token in re.split(r"[,;\n]+", text):
        normalized = re.sub(r"\s+", " ", token.strip())
        if len(normalized) < 3 or normalized in seen:
            continue
        keywords.append(normalized)
        seen.add(normalized)
    return tuple(keywords)


def load_accessory_filter_settings(force_refresh: bool = False) -> Dict[str, Any]:
    """Load accessory suppression settings from scraper_settings with short-lived cache."""
    now_ts = time.time()
    loaded_at = float(_accessory_filter_cache.get("loaded_at") or 0.0)
    if not force_refresh and (now_ts - loaded_at) < _ACCESSORY_FILTER_CACHE_TTL_SECONDS:
        return {
            "keywords": tuple(_accessory_filter_cache.get("keywords") or ACCESSORY_SIGNAL_KEYWORDS),
            "max_price": float(_accessory_filter_cache.get("max_price") or DEFAULT_ACCESSORY_MAX_PRICE),
        }

    keywords = tuple(ACCESSORY_SIGNAL_KEYWORDS)
    max_price = float(DEFAULT_ACCESSORY_MAX_PRICE)

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT setting_key, setting_value
            FROM scraper_settings
            WHERE setting_key IN (?, ?)
            """,
            ("accessory_filter_keywords", "accessory_filter_max_price"),
        )
        rows = {str(key): value for key, value in cursor.fetchall()}
        conn.close()

        parsed_keywords = _parse_accessory_keywords_csv(rows.get("accessory_filter_keywords", ""))
        if parsed_keywords:
            keywords = parsed_keywords

        raw_max = str(rows.get("accessory_filter_max_price", "") or "").strip()
        if raw_max:
            parsed_max = float(raw_max)
            if parsed_max > 0:
                max_price = min(parsed_max, 5000.0)
    except Exception:
        pass

    _accessory_filter_cache["loaded_at"] = now_ts
    _accessory_filter_cache["keywords"] = keywords
    _accessory_filter_cache["max_price"] = max_price
    return {"keywords": keywords, "max_price": max_price}


def _parse_iso_timestamp(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def load_active_search_queries(max_queries: Optional[int] = None) -> List[str]:
    """Load enabled search query strings from DB with fallback to defaults."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT keywords
        FROM search_queries
        WHERE COALESCE(is_active, 1) = 1
        ORDER BY id ASC
        """
    )
    rows = cursor.fetchall()
    conn.close()

    queries = [str(row[0]).strip() for row in rows if row and row[0] and str(row[0]).strip()]
    if not queries:
        queries = list(SEARCH_QUERIES)

    if max_queries is not None:
        try:
            limit = max(1, int(max_queries))
            queries = queries[:limit]
        except (TypeError, ValueError):
            pass
    return queries


def mark_search_query_polled(keyword: str):
    """Update last_polled for a query keyword when it is processed."""
    normalized = str(keyword or "").strip()
    if not normalized:
        return
    init_db()
    now_iso = datetime.now().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE search_queries
        SET last_polled = ?, updated_at = ?
        WHERE LOWER(TRIM(keywords)) = LOWER(TRIM(?))
        """,
        (now_iso, now_iso, normalized),
    )
    conn.commit()
    conn.close()


def update_fb_account_runtime_status(account_id: int, success: bool, reason: str = ""):
    """Track account runtime health for cooldown/login recovery decisions."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT UPPER(COALESCE(status, 'ACTIVE')), COALESCE(failure_count, 0), COALESCE(cooldown_until, '')
        FROM fb_accounts
        WHERE id = ?
        """,
        (account_id,),
    )
    row = cursor.fetchone()
    if not row:
        conn.close()
        return

    current_status = row[0] or "ACTIVE"
    try:
        failure_count = int(row[1] or 0)
    except (TypeError, ValueError):
        failure_count = 0
    cooldown_until_raw = row[2] or ""
    now = datetime.now()
    now_iso = now.isoformat()

    # Preserve explicit terminal bans.
    if current_status == "BANNED":
        conn.close()
        return

    if success:
        cursor.execute(
            """
            UPDATE fb_accounts
            SET status = 'ACTIVE', failure_count = 0, cooldown_until = NULL, updated_at = ?
            WHERE id = ?
            """,
            (now_iso, account_id),
        )
        conn.commit()
        conn.close()
        return

    next_failure = failure_count + 1
    reason_text = str(reason or "").lower()
    next_status = current_status if current_status else "ACTIVE"
    next_cooldown_until = cooldown_until_raw or None

    login_signals = ("401", "unauthorized", "login", "checkpoint", "challenge")
    rate_limit_signals = ("429", "too many requests", "rate limit")

    if any(token in reason_text for token in login_signals):
        next_status = "NEEDS_LOGIN"
        next_cooldown_until = None
    elif any(token in reason_text for token in rate_limit_signals):
        next_status = "COOLDOWN"
        next_cooldown_until = (now + timedelta(hours=6)).isoformat()
    elif next_failure >= 3:
        next_status = "COOLDOWN"
        next_cooldown_until = (now + timedelta(hours=1)).isoformat()

    cursor.execute(
        """
        UPDATE fb_accounts
        SET status = ?, failure_count = ?, cooldown_until = ?, updated_at = ?
        WHERE id = ?
        """,
        (next_status, next_failure, next_cooldown_until, now_iso, account_id),
    )
    conn.commit()
    conn.close()


def load_notify_profitable_only_setting() -> bool:
    """Return whether notifications should be limited to positive-profit listings."""
    raw = get_scraper_setting("notify_profitable_only", "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _get_account_context_by_id(account_id: int) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT a.id, a.account_name, a.email, a.profile_path,
               p.id, p.proxy_type, p.host, p.port, p.username, p.password
        FROM fb_accounts a
        LEFT JOIN proxies p ON p.id = a.proxy_id
        WHERE a.id = ?
        """,
        (account_id,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "account_id": row[0],
        "account_name": row[1] or "",
        "email": row[2] or "",
        "profile_path": row[3] or "",
        "proxy_id": row[4],
        "proxy_type": (row[5] or "").lower(),
        "host": row[6],
        "port": row[7],
        "username": row[8],
        "password": row[9],
    }


def _get_active_scraper_account_context() -> Optional[Dict[str, Any]]:
    raw = get_scraper_setting("active_scraper_account_id", "").strip()
    if not raw:
        return None
    try:
        account_id = int(raw)
    except ValueError:
        return None
    return _get_account_context_by_id(account_id)


def list_eligible_scraper_account_contexts() -> List[Dict[str, Any]]:
    """Return eligible fb accounts with ACTIVE/expired-COOLDOWN + ACTIVE SOCKS5 proxy."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT a.id, a.account_name, a.email, a.profile_path,
               UPPER(COALESCE(a.status, 'ACTIVE')),
               COALESCE(a.cooldown_until, ''),
               COALESCE(a.last_scrape_started_at, ''),
               COALESCE(a.updated_at, ''),
               p.id, p.proxy_type, p.host, p.port, p.username, p.password
        FROM fb_accounts a
        INNER JOIN proxies p ON p.id = a.proxy_id
        WHERE UPPER(COALESCE(a.status, '')) IN ('ACTIVE', 'COOLDOWN')
          AND LOWER(COALESCE(p.proxy_type, '')) = 'socks5'
          AND LOWER(COALESCE(p.status, 'active')) = 'active'
          AND COALESCE(TRIM(p.host), '') != ''
          AND p.port IS NOT NULL
        ORDER BY a.id
        """
    )
    rows = cursor.fetchall()
    conn.close()

    accounts: List[Dict[str, Any]] = []
    now = datetime.now()
    for row in rows:
        account_status = (row[4] or "ACTIVE").upper()
        cooldown_until_raw = row[5] or ""
        if account_status == "COOLDOWN":
            cooldown_until = _parse_iso_timestamp(cooldown_until_raw)
            if cooldown_until and cooldown_until > now:
                continue
        try:
            port = int(row[11])
        except (TypeError, ValueError):
            continue
        if port <= 0:
            continue
        accounts.append(
            {
                "account_id": row[0],
                "account_name": row[1] or "",
                "email": row[2] or "",
                "profile_path": row[3] or "",
                "status": account_status,
                "cooldown_until": cooldown_until_raw,
                "last_scrape_started_at": row[6] or "",
                "updated_at": row[7] or "",
                "proxy_id": row[8],
                "proxy_type": (row[9] or "").lower(),
                "host": row[10],
                "port": port,
                "username": row[12],
                "password": row[13],
            }
        )
    return accounts


def _select_next_scraper_account_context() -> Optional[Dict[str, Any]]:
    eligible_accounts = list_eligible_scraper_account_contexts()
    if not eligible_accounts:
        return None

    try:
        min_reuse_seconds = max(0, int(get_scraper_setting("account_min_reuse_seconds", "30").strip()))
    except ValueError:
        min_reuse_seconds = 30

    reusable_accounts = eligible_accounts
    if min_reuse_seconds > 0:
        cutoff = datetime.now().timestamp() - min_reuse_seconds
        candidate_accounts: List[Dict[str, Any]] = []
        for account in eligible_accounts:
            last_started_raw = str(account.get("last_scrape_started_at") or "").strip()
            if not last_started_raw:
                candidate_accounts.append(account)
                continue
            try:
                last_started_ts = datetime.fromisoformat(last_started_raw).timestamp()
            except ValueError:
                candidate_accounts.append(account)
                continue
            if last_started_ts <= cutoff:
                candidate_accounts.append(account)
        if candidate_accounts:
            reusable_accounts = candidate_accounts

    ids = [acc["account_id"] for acc in reusable_accounts]
    preferred_ctx = _get_active_scraper_account_context()
    preferred_id = preferred_ctx["account_id"] if preferred_ctx and preferred_ctx["account_id"] in ids else ids[0]

    raw_last = get_scraper_setting("last_scraper_account_id", "").strip()
    try:
        last_id = int(raw_last) if raw_last else None
    except ValueError:
        last_id = None

    if last_id in ids:
        next_index = (ids.index(last_id) + 1) % len(ids)
        return reusable_accounts[next_index]

    for account in reusable_accounts:
        if account["account_id"] == preferred_id:
            return account
    return reusable_accounts[0]


def reserve_next_scraper_account_for_run() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Reserve next account in round-robin and persist the reservation pointer."""
    account_ctx = _select_next_scraper_account_context()
    if not account_ctx:
        return None, (
            "No eligible scraper account found. Add at least one ACTIVE Facebook account "
            "with an assigned ACTIVE SOCKS5 proxy."
        )
    now_iso = datetime.now().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE fb_accounts
        SET last_scrape_started_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (now_iso, now_iso, account_ctx["account_id"]),
    )
    conn.commit()
    conn.close()
    set_scraper_setting("last_scraper_account_id", str(account_ctx["account_id"]))
    return account_ctx, None


def _find_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


LOCAL_SOCKS5_BRIDGE_SCRIPT = r"""
import argparse
import asyncio
import contextlib
import os
import socket
import struct
from typing import Optional


async def _read_exact(reader: asyncio.StreamReader, size: int) -> bytes:
    data = await reader.readexactly(size)
    if len(data) != size:
        raise asyncio.IncompleteReadError(data, size)
    return data


def _encode_socks5_addr_port(host: str, port: int) -> bytes:
    try:
        packed = socket.inet_aton(host)
        return b"\x01" + packed + struct.pack("!H", int(port))
    except OSError:
        pass
    host_bytes = host.encode("utf-8", errors="ignore")
    host_bytes = host_bytes[:255]
    return b"\x03" + bytes([len(host_bytes)]) + host_bytes + struct.pack("!H", int(port))


async def _drain_or_raise(writer: asyncio.StreamWriter) -> None:
    await writer.drain()


async def _socks5_connect_upstream(
    upstream_host: str,
    upstream_port: int,
    upstream_username: str,
    upstream_password: str,
    connect_payload: bytes,
    connect_timeout_seconds: float,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, bytes]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(upstream_host, upstream_port),
        timeout=connect_timeout_seconds,
    )
    methods = [0x00]
    if upstream_username or upstream_password:
        methods.append(0x02)
    writer.write(bytes([0x05, len(methods), *methods]))
    await _drain_or_raise(writer)

    server_choice = await _read_exact(reader, 2)
    if server_choice[0] != 0x05:
        raise RuntimeError("Upstream proxy returned invalid SOCKS version.")
    selected_method = int(server_choice[1])
    if selected_method == 0xFF:
        raise RuntimeError("Upstream proxy rejected auth methods.")

    if selected_method == 0x02:
        username_bytes = upstream_username.encode("utf-8", errors="ignore")[:255]
        password_bytes = upstream_password.encode("utf-8", errors="ignore")[:255]
        writer.write(
            bytes([0x01, len(username_bytes)])
            + username_bytes
            + bytes([len(password_bytes)])
            + password_bytes
        )
        await _drain_or_raise(writer)
        auth_reply = await _read_exact(reader, 2)
        if auth_reply[1] != 0x00:
            raise RuntimeError("Upstream proxy authentication failed.")
    elif selected_method != 0x00:
        raise RuntimeError("Upstream proxy selected unsupported auth method.")

    writer.write(connect_payload)
    await _drain_or_raise(writer)

    reply_head = await _read_exact(reader, 4)
    if reply_head[0] != 0x05:
        raise RuntimeError("Upstream connect reply version mismatch.")
    reply_code = int(reply_head[1])
    atyp = int(reply_head[3])
    if atyp == 0x01:
        addr_tail = await _read_exact(reader, 4 + 2)
    elif atyp == 0x03:
        name_len = await _read_exact(reader, 1)
        addr_tail = name_len + await _read_exact(reader, int(name_len[0]) + 2)
    elif atyp == 0x04:
        addr_tail = await _read_exact(reader, 16 + 2)
    else:
        raise RuntimeError("Upstream connect reply had unsupported address type.")
    full_reply = reply_head + addr_tail
    if reply_code != 0x00:
        raise RuntimeError(f"Upstream connect failed with SOCKS code={reply_code}")
    return reader, writer, full_reply


async def _pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    idle_timeout_seconds: float,
) -> None:
    while True:
        try:
            chunk = await asyncio.wait_for(reader.read(65536), timeout=idle_timeout_seconds)
        except asyncio.TimeoutError:
            # Release bridge slot if connection is idle for too long.
            break
        if not chunk:
            break
        writer.write(chunk)
        await writer.drain()


async def _handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    upstream_host: str,
    upstream_port: int,
    upstream_username: str,
    upstream_password: str,
    semaphore: asyncio.Semaphore,
    queue_timeout_seconds: float,
    connect_timeout_seconds: float,
    idle_timeout_seconds: float,
) -> None:
    upstream_reader: Optional[asyncio.StreamReader] = None
    upstream_writer: Optional[asyncio.StreamWriter] = None
    acquired = False
    try:
        hello = await _read_exact(client_reader, 2)
        if hello[0] != 0x05:
            return
        method_count = int(hello[1])
        client_methods = await _read_exact(client_reader, method_count)
        if 0x00 not in client_methods:
            client_writer.write(b"\x05\xFF")
            await client_writer.drain()
            return
        client_writer.write(b"\x05\x00")
        await client_writer.drain()

        request_head = await _read_exact(client_reader, 4)
        if request_head[0] != 0x05:
            return
        cmd = int(request_head[1])
        atyp = int(request_head[3])
        if cmd != 0x01:
            client_writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_writer.drain()
            return
        if atyp == 0x01:
            addr_port_payload = await _read_exact(client_reader, 4 + 2)
        elif atyp == 0x03:
            name_len = await _read_exact(client_reader, 1)
            addr_port_payload = name_len + await _read_exact(client_reader, int(name_len[0]) + 2)
        elif atyp == 0x04:
            addr_port_payload = await _read_exact(client_reader, 16 + 2)
        else:
            client_writer.write(b"\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_writer.drain()
            return
        connect_payload = request_head + addr_port_payload

        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=queue_timeout_seconds)
            acquired = True
        except asyncio.TimeoutError:
            client_writer.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_writer.drain()
            return

        upstream_reader, upstream_writer, upstream_reply = await _socks5_connect_upstream(
            upstream_host=upstream_host,
            upstream_port=upstream_port,
            upstream_username=upstream_username,
            upstream_password=upstream_password,
            connect_payload=connect_payload,
            connect_timeout_seconds=connect_timeout_seconds,
        )

        client_writer.write(upstream_reply)
        await client_writer.drain()

        to_upstream = asyncio.create_task(
            _pipe(client_reader, upstream_writer, idle_timeout_seconds=idle_timeout_seconds)
        )
        to_client = asyncio.create_task(
            _pipe(upstream_reader, client_writer, idle_timeout_seconds=idle_timeout_seconds)
        )
        done, pending = await asyncio.wait(
            {to_upstream, to_client},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in done:
            with contextlib.suppress(Exception):
                task.result()
    except asyncio.IncompleteReadError:
        pass
    except Exception:
        with contextlib.suppress(Exception):
            client_writer.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_writer.drain()
    finally:
        if acquired:
            semaphore.release()
        if upstream_writer is not None:
            upstream_writer.close()
            with contextlib.suppress(Exception):
                await upstream_writer.wait_closed()
        client_writer.close()
        with contextlib.suppress(Exception):
            await client_writer.wait_closed()


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-port", type=int, required=True)
    parser.add_argument("--max-upstream-connections", type=int, default=1)
    parser.add_argument("--queue-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--idle-timeout-seconds", type=float, default=8.0)
    args = parser.parse_args()

    upstream_host = os.getenv("BRIDGE_UPSTREAM_HOST", "").strip()
    upstream_port = int(os.getenv("BRIDGE_UPSTREAM_PORT", "0") or 0)
    upstream_username = os.getenv("BRIDGE_UPSTREAM_USERNAME", "")
    upstream_password = os.getenv("BRIDGE_UPSTREAM_PASSWORD", "")
    if not upstream_host or upstream_port <= 0:
        raise SystemExit("Missing BRIDGE_UPSTREAM_HOST/BRIDGE_UPSTREAM_PORT")

    semaphore = asyncio.Semaphore(max(1, int(args.max_upstream_connections)))
    server = await asyncio.start_server(
        lambda reader, writer: _handle_client(
            reader,
            writer,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
            upstream_username=upstream_username,
            upstream_password=upstream_password,
            semaphore=semaphore,
            queue_timeout_seconds=max(1.0, float(args.queue_timeout_seconds)),
            connect_timeout_seconds=max(1.0, float(args.connect_timeout_seconds)),
            idle_timeout_seconds=max(2.0, float(args.idle_timeout_seconds)),
        ),
        host="127.0.0.1",
        port=int(args.local_port),
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(_main())
"""


def _start_local_socks5_auth_bridge(account_ctx: Dict[str, Any]) -> Tuple[Any, str]:
    proxy_host = str(account_ctx.get("host") or "").strip()
    proxy_port = int(account_ctx.get("port") or 0)
    if not proxy_host or proxy_port <= 0:
        raise RuntimeError("SOCKS5 bridge requires upstream host and port.")
    max_upstream_connections = _parse_env_int(
        str(account_ctx.get("max_upstream_connections") or ""),
        default=PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS,
        minimum=1,
    )
    queue_timeout_seconds = _parse_env_int(
        str(account_ctx.get("queue_timeout_seconds") or ""),
        default=PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS,
        minimum=1,
    )
    connect_timeout_seconds = _parse_env_int(
        str(account_ctx.get("connect_timeout_seconds") or ""),
        default=PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS,
        minimum=1,
    )
    idle_timeout_seconds = _parse_env_int(
        str(account_ctx.get("idle_timeout_seconds") or ""),
        default=PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS,
        minimum=2,
    )
    local_port = _find_free_local_port()
    local_proxy = f"socks5://127.0.0.1:{local_port}"

    cmd = [
        sys.executable,
        "-u",
        "-c",
        LOCAL_SOCKS5_BRIDGE_SCRIPT,
        "--local-port",
        str(local_port),
        "--max-upstream-connections",
        str(max_upstream_connections),
        "--queue-timeout-seconds",
        str(queue_timeout_seconds),
        "--connect-timeout-seconds",
        str(connect_timeout_seconds),
        "--idle-timeout-seconds",
        str(idle_timeout_seconds),
    ]

    bridge_env = os.environ.copy()
    bridge_env["BRIDGE_UPSTREAM_HOST"] = proxy_host
    bridge_env["BRIDGE_UPSTREAM_PORT"] = str(proxy_port)
    bridge_env["BRIDGE_UPSTREAM_USERNAME"] = str(account_ctx.get("username") or "")
    bridge_env["BRIDGE_UPSTREAM_PASSWORD"] = str(account_ctx.get("password") or "")

    process = subprocess.Popen(
        cmd,
        cwd=Path(__file__).parent,
        env=bridge_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    start = datetime.now()
    while (datetime.now() - start).total_seconds() < 8:
        if process.poll() is not None:
            stderr_output = ""
            try:
                if process.stderr:
                    stderr_output = process.stderr.read().decode("utf-8", errors="ignore")[:500]
            except Exception:
                pass
            if stderr_output:
                raise RuntimeError(f"Failed to start local SOCKS5 bridge process: {stderr_output}")
            raise RuntimeError("Failed to start local SOCKS5 bridge process.")
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=0.5):
                return process, local_proxy
        except OSError:
            pass
        time.sleep(0.2)

    try:
        process.terminate()
    except Exception:
        pass
    raise RuntimeError("Timed out waiting for local SOCKS5 bridge to become ready.")


def build_playwright_proxy_for_account(account_ctx: Dict[str, Any]) -> Tuple[Optional[Dict[str, str]], Optional[Any]]:
    """
    Build Playwright proxy config for account context.
    Returns (proxy_config_or_none, bridge_process_or_none).
    """
    if not account_ctx or not account_ctx.get("proxy_id"):
        return None, None

    proxy_type = (account_ctx.get("proxy_type") or "").lower()
    host = account_ctx.get("host")
    port = account_ctx.get("port")
    if proxy_type != "socks5" or not host or not port:
        return None, None

    if FORCE_LOCAL_PROXY_BRIDGE or account_ctx.get("username") or account_ctx.get("password"):
        bridge_process, bridge_proxy = _start_local_socks5_auth_bridge(account_ctx)
        return {"server": bridge_proxy}, bridge_process

    return {"server": f"{proxy_type}://{host}:{port}"}, None


def cleanup_bridge_process(bridge_process: Any):
    if bridge_process and bridge_process.poll() is None:
        try:
            bridge_process.terminate()
            bridge_process.wait(timeout=3)
        except Exception:
            try:
                bridge_process.kill()
            except Exception:
                pass


def reserve_and_build_scraper_runtime_context() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Reserve next rotating scraper account and build runtime context.
    Returns (context, error). Context keys:
      - account_ctx
      - account_label
      - user_data_dir
      - proxy
      - bridge_process
    """
    account_ctx, account_error = reserve_next_scraper_account_for_run()
    if account_error:
        return None, account_error

    account_label = account_ctx.get("account_name") or account_ctx.get("email") or f"Account {account_ctx['account_id']}"

    profile_path = (account_ctx.get("profile_path") or "").strip()
    if profile_path:
        user_data_dir = profile_path
    else:
        user_data_dir = str(Path(__file__).parent / "browser_profiles" / f"fb_account_{account_ctx['account_id']}")

    bridge_process = None
    try:
        proxy_config, bridge_process = build_playwright_proxy_for_account(account_ctx)
    except Exception as exc:
        return None, f"Failed to initialize proxy for {account_label}: {exc}"

    if not proxy_config:
        cleanup_bridge_process(bridge_process)
        return None, (
            f"{account_label} has no usable SOCKS5 proxy. "
            "Assign an ACTIVE SOCKS5 proxy and try again."
        )

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE fb_accounts SET status = 'ACTIVE', cooldown_until = NULL, updated_at = ? WHERE id = ?",
        (datetime.now().isoformat(), account_ctx["account_id"]),
    )
    conn.commit()
    conn.close()

    return {
        "account_ctx": account_ctx,
        "account_label": account_label,
        "user_data_dir": user_data_dir,
        "proxy": proxy_config,
        "bridge_process": bridge_process,
    }, None


def load_price_list(csv_path: str = "price_list.csv") -> dict:
    """Load the price list from CSV into a dictionary."""
    import csv
    price_data = {}
    with open(Path(__file__).parent / csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            model = row["Model"].strip()
            price_data[model] = {
                "buying_price": float(row["Buying Price"]),
                "selling_price": float(row["Selling Price"]),
                "backglass_repair": float(row["Backglass repair cost"]),
                "screen_repair": float(row["Screen repair cost"]),
                "battery_repair": float(row["Battery repair cost"]),
                "camera_lens_repair": float(row["Camera lens repair cost"]),
            }
    return price_data


def _normalize_model_key(model: str) -> str:
    return re.sub(r"\s+", " ", str(model or "").strip()).lower()


def resolve_price_model_key(model: Optional[str], price_data: Dict[str, Any]) -> Optional[str]:
    """
    Resolve a detected model name to a key present in price_data.
    Matching is case-insensitive and whitespace-tolerant.
    """
    raw_model = str(model or "").strip()
    if not raw_model:
        return None

    if raw_model in price_data:
        return raw_model

    normalized_target = _normalize_model_key(raw_model)
    for key in price_data.keys():
        if _normalize_model_key(str(key)) == normalized_target:
            return str(key)
    return None


def identify_model(title: str, description: str = "") -> Optional[str]:
    """Identify the iPhone model from the listing title and description."""
    text = "{} {}".format(title, description).lower()
    
    # Order matters: check more specific models first
    models = [
        "iPhone 16 Pro Max",
        "iPhone 16 Pro",
        "iPhone 16 Plus",
        "iPhone 16",
        "iPhone 15 Pro Max",
        "iPhone 15 Pro",
        "iPhone 15 Plus",
        "iPhone 15",
        "iPhone 14 Pro Max",
        "iPhone 14 Pro",
        "iPhone 14 Plus",
        "iPhone 14",
        "iPhone 13 Pro Max",
        "iPhone 13 Pro",
        "iPhone 13 Mini",
        "iPhone 13",
        "iPhone 11 Pro Max",
        "iPhone 12 Pro Max",
        "iPhone 12 Pro",
        "iPhone 12 Mini",
        "iPhone 12",
        "iPhone 11 Pro",
        "iPhone 11",
    ]
    
    for model in models:
        if model.lower().replace(" ", "") in text.replace(" ", ""):
            return model
        # Also check with common variations
        if model.lower() in text:
            return model
    
    return None


def assess_condition(title: str, description: str = "") -> Tuple[str, List[str]]:
    """
    Assess the condition of the iPhone based on keywords.
    Returns (condition_category, list_of_issues).
    """
    text = "{} {}".format(title, description).lower()
    issues = []
    
    # Check for various damage indicators
    damage_keywords = {
        "cracked screen": ["cracked screen", "broken screen", "smashed screen", "screen crack"],
        "cracked back": ["cracked back", "back glass cracked", "back cracked", "broken back"],
        "battery issue": ["battery", "battery health", "low battery", "battery replacement"],
        "camera issue": ["camera broken", "camera crack", "camera lens", "lens cracked"],
        "water damage": ["water damage", "water damaged", "liquid damage"],
        "not working": ["not working", "doesn't work", "dead", "won't turn on", "for parts"],
        "icloud locked": ["icloud lock", "icloud locked", "activation lock"],
    }
    
    for issue, keywords in damage_keywords.items():
        for keyword in keywords:
            if keyword in text:
                issues.append(issue)
                break
    
    # Determine overall condition
    if "icloud locked" in issues or "not working" in issues:
        return "parts_only", issues
    elif len(issues) >= 2:
        return "repairable_major", issues
    elif len(issues) == 1:
        return "repairable_minor", issues
    else:
        return "good", issues


def calculate_max_offer(model: str, condition: str, issues: List[str], price_data: Dict[str, Any]) -> Tuple[float, float]:
    """
    Calculate the maximum offer price and potential profit based on model, condition, and issues.
    Returns (max_offer, potential_profit).
    """
    if model not in price_data:
        return 0.0, 0.0
    
    model_data = price_data[model]
    base_buy_price = model_data["buying_price"]
    
    # Start with base buying price
    max_offer = base_buy_price
    
    # Adjust for condition and calculate repair costs
    repair_costs = calculate_repair_costs(model_data, issues)
    
    # For parts-only phones, offer significantly less
    if condition == "parts_only":
        max_offer = base_buy_price * 0.3  # 30% of base price for parts
        potential_profit = calculate_profit_for_listing(
            model=model,
            condition=condition,
            issues=issues,
            price_data=price_data,
            listed_price=None,
            fallback_purchase_price=max_offer,
        )
    else:
        # Reduce offer by repair costs
        max_offer = base_buy_price - repair_costs
        potential_profit = calculate_profit_for_listing(
            model=model,
            condition=condition,
            issues=issues,
            price_data=price_data,
            listed_price=None,
            fallback_purchase_price=max_offer,
        )
    
    # Ensure max_offer is not negative
    max_offer = max(max_offer, 10.0)
    
    return float(round(max_offer, 2)), float(round(potential_profit, 2))


def calculate_repair_costs(model_data: Dict[str, Any], issues: List[str]) -> float:
    """Calculate expected repair costs for a set of detected issues."""
    repair_costs = 0.0
    if "cracked screen" in issues:
        repair_costs += model_data["screen_repair"]
    if "cracked back" in issues:
        repair_costs += model_data["backglass_repair"]
    if "battery issue" in issues:
        repair_costs += model_data["battery_repair"]
    if "camera issue" in issues:
        repair_costs += model_data["camera_lens_repair"]
    return float(repair_costs)


def calculate_profit_for_listing(
    model: str,
    condition: str,
    issues: List[str],
    price_data: Dict[str, Any],
    listed_price: Optional[float],
    fallback_purchase_price: Optional[float] = None,
) -> float:
    """
    Calculate expected profit using listing price when available.

    Profit formula:
        selling_price - purchase_price - estimated_repair_costs
    """
    if model not in price_data:
        return 0.0

    model_data = price_data[model]
    selling_price = model_data["selling_price"]
    repair_costs = calculate_repair_costs(model_data, issues)

    purchase_price = listed_price if listed_price is not None else fallback_purchase_price
    if purchase_price is None:
        return 0.0

    if condition == "parts_only":
        # For parts-only devices, keep a conservative floor.
        purchase_price = min(purchase_price, model_data["buying_price"] * 0.3)

    profit = selling_price - float(purchase_price) - repair_costs
    return float(round(profit, 2))


def parse_listing_price(price_raw: Any) -> Optional[float]:
    """Parse listing price from numeric, dict, or string values."""
    if price_raw is None:
        return None

    if isinstance(price_raw, (int, float)):
        return float(price_raw)

    if isinstance(price_raw, dict):
        # Common GraphQL/DOM keys where numeric price can appear.
        for key in ("amount", "value", "price"):
            value = price_raw.get(key)
            if isinstance(value, (int, float)):
                return float(value)

        for key in ("formatted_amount", "formatted", "text", "display_price"):
            nested_value = price_raw.get(key)
            nested_price = parse_listing_price(nested_value)
            if nested_price is not None:
                return nested_price
        return None

    if isinstance(price_raw, str):
        cleaned = price_raw.strip()
        if not cleaned:
            return None

        # Prefer currency-tagged values to avoid false matches against model/storage numbers.
        currency_value = extract_currency_price_from_text(cleaned)
        if currency_value is not None:
            return currency_value

        shorthand_value = extract_shorthand_k_price_from_text(cleaned)
        if shorthand_value is not None:
            return shorthand_value

        numeric_only = cleaned.replace(" ", "")
        if re.fullmatch(r"\d[\d.,]*", numeric_only):
            return _parse_numeric_price_token(numeric_only)

        # As a fallback, only parse first numeric token when string has no letters.
        if re.search(r"[A-Za-z]", cleaned):
            return None
        match = re.search(r"\d[\d.,]*", cleaned)
        if not match:
            return None
        return _parse_numeric_price_token(match.group(0))

    return None


def _parse_numeric_price_token(token: str) -> Optional[float]:
    """
    Parse numeric token that may contain thousands/decimal separators.
    Supports:
      - 1,000
      - 1.000
      - 1,000.50
      - 1.000,50
      - 300
    """
    if not token:
        return None

    value = token.strip().replace(" ", "")
    if not value:
        return None

    if "," in value and "." in value:
        # Choose decimal separator by right-most occurrence.
        if value.rfind(".") > value.rfind(","):
            normalized = value.replace(",", "")
        else:
            normalized = value.replace(".", "").replace(",", ".")
    elif "," in value:
        # Single comma with 1-2 trailing digits -> decimal, else thousands.
        if value.count(",") == 1 and len(value.split(",")[1]) in (1, 2):
            normalized = value.replace(",", ".")
        else:
            normalized = value.replace(",", "")
    elif "." in value:
        # Single dot with 1-2 trailing digits -> decimal, else thousands.
        if value.count(".") == 1 and len(value.split(".")[1]) in (1, 2):
            normalized = value
        else:
            normalized = value.replace(".", "")
    else:
        normalized = value

    try:
        return float(normalized)
    except ValueError:
        return None


def extract_currency_price_from_text(text: Any) -> Optional[float]:
    """
    Extract a price only when a currency marker is present.

    This avoids false positives like parsing "iPhone 13" as price=13.
    Supports patterns like:
      - "$300", "AU$ 300", "A$300"
      - "300 AU$", "300 A$"
      - "AUD 300"
    """
    if text is None:
        return None

    raw = str(text).strip()
    if not raw:
        return None

    normalized = raw.replace(",", "")
    patterns = (
        r"(?:AU\$|A\$|\$)\s*([0-9][0-9.,]*)(?:\s*([kK]))?",
        r"([0-9][0-9.,]*)(?:\s*([kK]))?\s*(?:AU\$|A\$|\$)",
        r"\bAUD\s*([0-9][0-9.,]*)(?:\s*([kK]))?\b",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        parsed = _parse_numeric_price_token(match.group(1))
        if parsed is not None:
            return parsed * 1000.0 if match.group(2) else parsed
    return None


def extract_shorthand_k_price_from_text(text: Any) -> Optional[float]:
    """
    Extract shorthand thousand notation like `1.4k` or `1650k`.
    Accepts optional leading currency marker but does not require one.
    """
    if text is None:
        return None
    raw = str(text).strip()
    if not raw:
        return None

    match = re.search(r"(?:AU\$|A\$|\$)?\s*([0-9][0-9.,]*)\s*([kK])\b", raw)
    if not match:
        return None
    base = _parse_numeric_price_token(match.group(1))
    if base is None:
        return None
    return base * 1000.0


def is_accessory_only_listing(
    title: str,
    description: str = "",
    listed_price: Any = None,
) -> bool:
    """Heuristic filter for accessory-only listings (case/cover/charger, not handset)."""
    filter_settings = load_accessory_filter_settings()
    accessory_keywords = tuple(filter_settings.get("keywords") or ACCESSORY_SIGNAL_KEYWORDS)
    max_accessory_price = float(filter_settings.get("max_price") or DEFAULT_ACCESSORY_MAX_PRICE)

    title_text = str(title or "")
    description_text = str(description or "")
    text = "{} {}".format(title_text, description_text).strip().lower()
    text = re.sub(r"\s+", " ", text)
    if not text or "iphone" not in text:
        return False

    has_accessory_keyword = any(keyword in text for keyword in accessory_keywords)
    has_case_or_cover_word = bool(re.search(r"\b(?:case|cases|cover|covers)\b", text))
    has_accessory_keyword = has_accessory_keyword or has_case_or_cover_word
    if not has_accessory_keyword:
        return False

    explicit_accessory_only = any(phrase in text for phrase in ACCESSORY_ONLY_PHRASES)
    if explicit_accessory_only:
        return True

    compatibility_pattern = re.search(
        r"\b(?:for|fits|fit|compatible with|to suit)\s+iphone\b",
        text,
        flags=re.IGNORECASE,
    )
    iphone_case_pattern = re.search(
        r"\biphone(?:\s*(?:11|12|13|14|15|16))?(?:\s*(?:pro|max|plus|mini)){0,2}\s+(?:case|cases|cover|covers)\b",
        text,
        flags=re.IGNORECASE,
    )

    has_storage_signal = bool(re.search(r"\b(?:64|128|256|512)\s*gb\b|\b1\s*tb\b", text))
    has_device_sale_signal = any(signal in text for signal in DEVICE_SALE_SIGNALS)
    has_device_sale_signal = has_device_sale_signal or any(
        phrase in text
        for phrase in (
            "with case",
            "case included",
            "comes with case",
            "includes case",
        )
    )

    if compatibility_pattern and not has_device_sale_signal and not has_storage_signal:
        return True
    if iphone_case_pattern and not has_device_sale_signal and not has_storage_signal:
        return True

    numeric_price = parse_listing_price(listed_price)
    if numeric_price is None:
        numeric_price = (
            extract_currency_price_from_text(title_text)
            or extract_currency_price_from_text(description_text)
            or extract_shorthand_k_price_from_text(title_text)
            or extract_shorthand_k_price_from_text(description_text)
        )

    if (
        numeric_price is not None
        and numeric_price <= max_accessory_price
        and not has_device_sale_signal
        and not has_storage_signal
    ):
        return True

    return False


def purge_accessory_only_listings(
    cursor: sqlite3.Cursor,
    statuses: Optional[set[str]] = None,
) -> int:
    """Delete accessory-only rows for statuses managed by scraper automation."""
    target_statuses = statuses or {"new", "unclassified", "needs_pricing"}
    if not target_statuses:
        return 0

    placeholders = ",".join("?" for _ in target_statuses)
    cursor.execute(
        f"""
        SELECT id, title, description, price
        FROM listings
        WHERE status IN ({placeholders})
        """,
        tuple(sorted(target_statuses)),
    )
    rows = cursor.fetchall()
    delete_rows: List[Tuple[str]] = []
    for listing_id, title, description, price in rows:
        if is_accessory_only_listing(title or "", description or "", price):
            delete_rows.append((str(listing_id),))

    if not delete_rows:
        return 0

    cursor.executemany("DELETE FROM listings WHERE id = ?", delete_rows)
    return len(delete_rows)


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _extract_text_value(value: Any) -> str:
    """Extract a readable text value from nested GraphQL objects."""
    if isinstance(value, str):
        return value.strip()

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, dict):
        text_keys = (
            "text",
            "name",
            "title",
            "display_name",
            "formatted_amount",
            "raw_text",
            "description",
            "message",
            "label",
        )
        for key in text_keys:
            candidate = _extract_text_value(value.get(key))
            if candidate:
                return candidate
        return ""

    if isinstance(value, list):
        for item in value:
            candidate = _extract_text_value(item)
            if candidate:
                return candidate
        return ""

    return ""


def _get_nested(mapping: Any, *keys: str) -> Any:
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    return current


def _normalize_marketplace_url(raw_url: Any, listing_id: str) -> str:
    normalized_id = _extract_text_value(listing_id)
    if normalized_id:
        return f"https://www.facebook.com/marketplace/item/{normalized_id}/"

    url = _extract_text_value(raw_url)
    if not url:
        return ""

    if url.startswith("/"):
        url = f"https://www.facebook.com{url}"

    if url.startswith("http://") or url.startswith("https://"):
        parsed = urlparse(url)
        if parsed.path:
            match = re.search(r"/marketplace/item/(\d+)", parsed.path)
            if match:
                return f"https://www.facebook.com/marketplace/item/{match.group(1)}/"
        return urlunparse(parsed._replace(query="", fragment=""))

    return url


def _extract_graphql_listing_candidates(payload: Any) -> List[Dict[str, Any]]:
    """Recursively traverse GraphQL payload and collect listing-shaped nodes."""
    candidates: List[Dict[str, Any]] = []

    def walk(node: Any):
        if isinstance(node, dict):
            listing_obj = node.get("listing")
            if isinstance(listing_obj, dict):
                candidates.append(listing_obj)

            if "id" in node and (
                "marketplace_listing_title" in node
                or "listing_price" in node
                or "marketplace_listing_seller" in node
                or "redacted_description" in node
            ):
                candidates.append(node)

            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return candidates


def _normalize_graphql_listing(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a GraphQL listing object into scraper listing shape."""
    listing_id_raw = candidate.get("id") or candidate.get("listing_id")
    listing_id = _extract_text_value(listing_id_raw)
    if listing_id:
        id_match = re.search(r"\d{6,}", listing_id)
        if id_match:
            listing_id = id_match.group()

    url = _normalize_marketplace_url(
        candidate.get("url")
        or candidate.get("permalink")
        or _get_nested(candidate, "story", "url")
        or _get_nested(candidate, "story", "www_lite_url"),
        listing_id,
    )
    if not listing_id:
        id_from_url = re.search(r"/item/(\d+)", url)
        if id_from_url:
            listing_id = id_from_url.group(1)

    if not listing_id:
        return None

    title = _extract_text_value(candidate.get("marketplace_listing_title") or candidate.get("title"))
    description = _extract_text_value(candidate.get("redacted_description") or candidate.get("description"))
    price = None
    price_candidates = [
        candidate.get("listing_price"),
        candidate.get("price"),
        candidate.get("marketplace_listing_price"),
        candidate.get("listing_price_text"),
        _get_nested(candidate, "formatted_price"),
        _get_nested(candidate, "listing_price", "formatted_amount"),
    ]
    for raw_price in price_candidates:
        price = parse_listing_price(raw_price)
        if price is not None:
            break

    # Some GraphQL shapes flatten card text (e.g. "300 AU$iPhone ...") into title.
    if price is None:
        price = (
            extract_currency_price_from_text(title)
            or extract_currency_price_from_text(description)
            or extract_shorthand_k_price_from_text(title)
            or extract_shorthand_k_price_from_text(description)
        )
    else:
        shorthand_price = extract_shorthand_k_price_from_text(title) or extract_shorthand_k_price_from_text(description)
        if shorthand_price is not None and price < 20 and shorthand_price >= 100:
            price = shorthand_price

    seller_data = candidate.get("marketplace_listing_seller") or candidate.get("seller")
    seller_name = _extract_text_value(_get_nested(seller_data, "name") if isinstance(seller_data, dict) else seller_data)

    location_data = candidate.get("location") or {}
    location_name = _extract_text_value(
        _get_nested(location_data, "reverse_geocode", "city_page", "display_name")
        or _get_nested(location_data, "city_page", "display_name")
        or location_data.get("name")
        or location_data.get("display_name")
    )

    return {
        "id": listing_id,
        "title": title,
        "url": url,
        "price": price,
        "location": location_name,
        "description": description,
        "seller_name": seller_name,
    }


def _listing_quality_score(listing: Dict[str, Any]) -> int:
    title = _extract_text_value(listing.get("title"))
    score = 0
    if title:
        score += 3
    if re.search(r"iphone", title, re.IGNORECASE):
        score += 3
    if parse_listing_price(listing.get("price")) is not None:
        score += 2
    if _has_value(listing.get("url")):
        score += 2
    if _has_value(listing.get("location")):
        score += 1
    if _has_value(listing.get("description")):
        score += 1
    if _has_value(listing.get("seller_name")):
        score += 1
    return score


def merge_listing_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge duplicate listing IDs, preserving richer values."""
    merged: Dict[str, Dict[str, Any]] = {}
    fields = ("title", "url", "price", "location", "description", "seller_name")

    for candidate in candidates:
        listing_id = _extract_text_value(candidate.get("id"))
        if not listing_id:
            continue

        if listing_id not in merged:
            merged[listing_id] = dict(candidate)
            continue

        existing = merged[listing_id]
        existing_score = _listing_quality_score(existing)
        incoming_score = _listing_quality_score(candidate)

        preferred = dict(candidate) if incoming_score > existing_score else dict(existing)
        backup = existing if incoming_score > existing_score else candidate

        for field in fields:
            if not _has_value(preferred.get(field)) and _has_value(backup.get(field)):
                preferred[field] = backup[field]

        merged[listing_id] = preferred

    return list(merged.values())


def extract_listings_from_graphql_payload(payload: Any) -> List[Dict[str, Any]]:
    """Extract normalized listings from a Marketplace GraphQL payload."""
    normalized: List[Dict[str, Any]] = []
    for candidate in _extract_graphql_listing_candidates(payload):
        listing = _normalize_graphql_listing(candidate)
        if listing:
            normalized.append(listing)
    return merge_listing_candidates(normalized)


def decode_json_body(body_text: str) -> Optional[Any]:
    """Decode response body text into JSON, handling Facebook anti-prefixes."""
    if not body_text:
        return None

    payload = body_text.lstrip()
    if payload.startswith("for (;;);"):
        payload = payload[len("for (;;);"):].lstrip()

    if not payload:
        return None

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def _extract_ip_from_text(value: str) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    for token in re.split(r"[\s,;]+", text):
        token_clean = token.strip().strip("[]")
        if not token_clean:
            continue
        try:
            return str(ipaddress.ip_address(token_clean))
        except ValueError:
            continue
    match = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text)
    if not match:
        return None
    try:
        return str(ipaddress.ip_address(match.group(0)))
    except ValueError:
        return None


def _extract_proxy_check_ip(payload: Any) -> Optional[str]:
    if payload is None:
        return None
    if isinstance(payload, str):
        return _extract_ip_from_text(payload)
    if isinstance(payload, list):
        for item in payload:
            found = _extract_proxy_check_ip(item)
            if found:
                return found
        return None
    if isinstance(payload, dict):
        for key in ("ip", "origin", "query", "address"):
            if key not in payload:
                continue
            found = _extract_proxy_check_ip(payload.get(key))
            if found:
                return found
        return None
    return _extract_ip_from_text(str(payload))


async def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """Add a random delay to mimic human behavior."""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)


async def human_like_scroll(page, scroll_count: int = 3):
    """Scroll the page in a human-like manner with variable speeds."""
    for i in range(scroll_count):
        # Vary the scroll distance
        scroll_distance = random.randint(300, 800)
        await page.evaluate("window.scrollBy(0, {})".format(scroll_distance))
        # Variable delay between scrolls
        await random_delay(1.5, 4.0)
        # Occasionally pause longer (simulating reading)
        if random.random() < 0.3:
            await random_delay(3.0, 6.0)


async def _collect_marketplace_visible_ids(page) -> set[str]:
    """Collect unique listing IDs currently rendered in the Marketplace feed."""
    try:
        raw_ids = await page.evaluate(
            """
            () => {
                const ids = new Set();
                const links = document.querySelectorAll('a[href*="/marketplace/item/"]');
                links.forEach((link) => {
                    const href = link.getAttribute('href') || '';
                    const match = href.match(/\\/item\\/(\\d+)/);
                    if (match && match[1]) {
                        ids.add(match[1]);
                    } else if (href) {
                        ids.add(href);
                    }
                });
                return Array.from(ids);
            }
            """
        )
    except Exception:
        return set()

    ids: set[str] = set()
    for value in raw_ids or []:
        text = str(value or "").strip()
        if text:
            ids.add(text)
    return ids


async def _scroll_marketplace_feed(page, distance: int) -> bool:
    """Scroll the most likely Marketplace feed container. Returns True if movement was detected."""
    distance = int(distance)
    try:
        moved = await page.evaluate(
            """
            (distance) => {
                const root = document.scrollingElement || document.documentElement;
                let best = root;
                let bestRange = Math.max(0, (root.scrollHeight || 0) - (root.clientHeight || 0));

                const candidates = Array.from(document.querySelectorAll("div, main, section"));
                for (const el of candidates) {
                    const style = window.getComputedStyle(el);
                    if (!style || !/(auto|scroll)/i.test(style.overflowY || "")) {
                        continue;
                    }
                    const range = Math.max(0, (el.scrollHeight || 0) - (el.clientHeight || 0));
                    if (range > bestRange + 120) {
                        best = el;
                        bestRange = range;
                    }
                }

                const before = best.scrollTop || 0;
                if (typeof best.scrollBy === "function") {
                    best.scrollBy(0, distance);
                } else {
                    best.scrollTop = before + distance;
                }
                const after = best.scrollTop || 0;
                if (after !== before) {
                    return true;
                }

                const rootBefore = window.pageYOffset || document.documentElement.scrollTop || 0;
                window.scrollBy(0, distance);
                const rootAfter = window.pageYOffset || document.documentElement.scrollTop || 0;
                return rootAfter !== rootBefore;
            }
            """,
            distance,
        )
        return bool(moved)
    except Exception:
        return False


async def progressive_marketplace_scroll(
    page,
    target_cards: int = 180,
    max_rounds: int = 14,
    min_rounds: int = 1,
    snapshot_callback: Optional[Callable[[], Awaitable[None]]] = None,
) -> Dict[str, int]:
    """
    Scroll progressively until enough cards are loaded or growth stalls.
    This avoids getting stuck around the initial ~70-card viewport batch.
    """
    target_cards = max(40, int(target_cards))
    max_rounds = max(2, int(max_rounds))
    min_rounds = max(0, int(min_rounds))

    seen_listing_ids = await _collect_marketplace_visible_ids(page)
    best_visible = len(seen_listing_ids)
    idle_rounds = 0
    rounds = 0

    if snapshot_callback:
        try:
            await snapshot_callback()
        except Exception:
            pass

    for _ in range(max_rounds):
        # Enforce at least one scroll pass, even if the initial card count
        # already meets the target threshold.
        if rounds >= min_rounds and best_visible >= target_cards:
            break

        rounds += 1
        first_scroll = random.randint(900, 1700)
        moved = await _scroll_marketplace_feed(page, first_scroll)
        if not moved:
            try:
                await page.mouse.wheel(0, first_scroll)
            except Exception:
                pass
        await random_delay(1.0, 2.4)

        if random.random() < 0.35:
            second_scroll = random.randint(350, 850)
            moved_second = await _scroll_marketplace_feed(page, second_scroll)
            if not moved_second:
                try:
                    await page.mouse.wheel(0, second_scroll)
                except Exception:
                    pass
            await random_delay(0.8, 1.8)

        try:
            clicked_more = await page.evaluate(
                """
                () => {
                    const nodes = Array.from(
                        document.querySelectorAll('div[role="button"], a[role="button"], button')
                    );
                    for (const node of nodes) {
                        const text = (node.textContent || '').trim().toLowerCase();
                        if (text.includes('see more') || text.includes('more results') || text.includes('show more')) {
                            node.click();
                            return true;
                        }
                    }
                    return false;
                }
                """
            )
            if clicked_more:
                await random_delay(0.8, 1.8)
        except Exception:
            pass

        if snapshot_callback:
            try:
                await snapshot_callback()
            except Exception:
                pass

        current_ids = await _collect_marketplace_visible_ids(page)
        newly_seen = current_ids - seen_listing_ids
        if newly_seen:
            seen_listing_ids.update(newly_seen)
            best_visible = len(seen_listing_ids)
            idle_rounds = 0
        else:
            idle_rounds += 1

        if idle_rounds >= 5:
            break

    return {
        "scroll_rounds": rounds,
        "visible_cards": best_visible,
    }


async def _extract_dom_listing_candidates(page) -> List[Dict[str, str]]:
    """Extract visible DOM listing candidates for the current viewport snapshot."""
    return await page.evaluate("""
        () => {
            // Try multiple selectors as Facebook changes them
            const selectors = [
                '[data-testid="marketplace_feed_item"]',
                'a[href*="/marketplace/item/"]',
                'div[class*="x9f619"] a[href*="/marketplace/item/"]',
            ];

            let links = [];
            for (const selector of selectors) {
                const found = document.querySelectorAll(selector);
                if (found.length > 0) {
                    links = found;
                    break;
                }
            }

            const byId = new Map();

            const cleanText = (text) => (text || "").replace(/\\s+/g, " ").trim();
            const looksLikeLocationOnly = (text) =>
                /^(perth|wa|australia|listed|sponsored)(,\\s*wa)?$/i.test(text);
            const looksLikePrice = (text) => {
                if (!text) return false;
                return (
                    /(?:A?U?\\$)\\s?\\d/i.test(text) ||
                    /\\d[\\d,.]*\\s?(?:A?U?\\$)/i.test(text) ||
                    /^\\d{2,6}(?:[.,]\\d{3})*(?:\\.\\d{1,2})?$/.test(text.trim())
                );
            };

            const pickBestText = (texts) => {
                const filtered = texts
                    .map(cleanText)
                    .filter((t) => t.length >= 4 && t.length <= 220 && !looksLikeLocationOnly(t));

                if (!filtered.length) return "";

                const iphoneText = filtered.find((t) => /iphone/i.test(t));
                if (iphoneText) return iphoneText;

                const nonPrice = filtered.find((t) => !looksLikePrice(t));
                if (nonPrice) return nonPrice;

                return filtered.sort((a, b) => b.length - a.length)[0];
            };

            const pickBestPrice = (texts) => {
                const price = texts.find((t) => looksLikePrice(t));
                return cleanText(price || "");
            };

            links.forEach((el) => {
                try {
                    let linkEl = el;
                    if (!el.href || !el.href.includes('/marketplace/item/')) {
                        linkEl = el.querySelector('a[href*="/marketplace/item/"]');
                    }
                    if (!linkEl || !linkEl.href) return;

                    const idMatch = linkEl.href.match(/\\/item\\/(\\d+)/);
                    if (!idMatch) return;
                    const listingId = idMatch[1];

                    const container =
                        linkEl.closest('[data-testid="marketplace_feed_item"]') ||
                        linkEl.closest('[role="article"]') ||
                        linkEl.closest('div[class*="x9f619"]') ||
                        linkEl.parentElement;

                    const spanTexts = container
                        ? Array.from(container.querySelectorAll("span")).map((s) => s.textContent || "")
                        : [];
                    const textCandidates = [
                        linkEl.getAttribute("aria-label") || "",
                        linkEl.textContent || "",
                        ...spanTexts,
                    ];

                    const title = pickBestText(textCandidates);
                    const price = pickBestPrice(textCandidates);

                    let score = 0;
                    if (title) score += 1;
                    if (/iphone/i.test(title)) score += 5;
                    if (price) score += 3;
                    if (title.length > 15) score += 1;

                    const candidate = {
                        id: listingId,
                        title,
                        url: linkEl.href,
                        price,
                        _score: score,
                    };

                    const existing = byId.get(listingId);
                    if (!existing || candidate._score > existing._score) {
                        byId.set(listingId, candidate);
                    }
                } catch (e) {}
            });

            return Array.from(byId.values()).map((item) => ({
                id: item.id,
                title: item.title,
                url: item.url,
                price: item.price,
            }));
        }
    """)


async def apply_stealth_scripts(page):
    """Apply stealth JavaScript to hide automation signals."""
    # Hide webdriver property
    await page.add_init_script("""
        // Override the navigator.webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });

        // Override the chrome property
        window.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {},
        };

        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // Override plugins to look more realistic
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                { name: 'Native Client', filename: 'internal-nacl-plugin' },
            ],
        });

        // Override languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-AU', 'en-US', 'en'],
        });

        // Remove automation-related properties
        delete navigator.__proto__.webdriver;

        // Override the platform
        Object.defineProperty(navigator, 'platform', {
            get: () => 'MacIntel',
        });

        // Override hardware concurrency
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 8,
        });

        // Override device memory
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => 8,
        });
    """)


def _store_listing_candidate(
    cursor: sqlite3.Cursor,
    listing: Dict[str, Any],
    price_data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Persist a single normalized listing candidate if it is new.
    Returns the processed listing payload when inserted, else None.
    """
    listing_id = _extract_text_value(listing.get("id"))
    if not listing_id:
        return None

    cursor.execute("SELECT id FROM listings WHERE id = ?", (listing_id,))
    if cursor.fetchone():
        return None

    title_text = (listing.get("title") or "").strip()
    if not title_text:
        title_text = "Untitled listing"

    description_text = (listing.get("description") or "").strip()
    location_text = (listing.get("location") or "").strip() or None
    seller_name = (listing.get("seller_name") or "").strip() or None
    listing_url = _normalize_marketplace_url(listing.get("url"), listing_id)

    price_value = parse_listing_price(listing.get("price"))
    inferred_price = (
        extract_currency_price_from_text(title_text)
        or extract_currency_price_from_text(description_text)
        or extract_shorthand_k_price_from_text(title_text)
        or extract_shorthand_k_price_from_text(description_text)
    )
    if price_value is None:
        price_value = inferred_price
    elif inferred_price is not None and price_value < 20 and inferred_price >= 100:
        price_value = inferred_price

    if is_accessory_only_listing(title_text, description_text, price_value):
        return None

    detected_model = identify_model(title_text, description_text)
    resolved_model = resolve_price_model_key(detected_model, price_data)
    if not resolved_model:
        # Scope control: only keep listings for models present in price sheet.
        return None

    model = resolved_model
    condition, issues = assess_condition(title_text, description_text)
    max_offer, _ = calculate_max_offer(model, condition, issues, price_data)
    profit = calculate_profit_for_listing(
        model=model,
        condition=condition,
        issues=issues,
        price_data=price_data,
        listed_price=price_value,
        fallback_purchase_price=max_offer,
    )
    status = "new"

    created_at = datetime.now().isoformat()
    cursor.execute(
        """
        INSERT OR IGNORE INTO listings (
            id, title, price, location, url, description, seller_name,
            model, condition, max_buy_price, potential_profit, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            listing_id,
            title_text,
            price_value,
            location_text,
            listing_url,
            description_text,
            seller_name,
            model,
            condition,
            max_offer,
            profit,
            status,
            created_at,
        ),
    )
    if cursor.rowcount <= 0:
        return None

    return {
        "id": listing_id,
        "title": title_text,
        "description": description_text,
        "price": price_value,
        "model": model,
        "condition": condition,
        "max_offer": max_offer,
        "potential_profit": profit,
        "location": location_text,
        "seller_name": seller_name,
        "url": listing_url,
        "status": status,
    }


async def scrape_marketplace(
    headless: bool = True,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    stop_event: Any = None,
    user_data_dir: Optional[str] = None,
    proxy: Optional[Dict[str, str]] = None,
    search_queries: Optional[List[str]] = None,
    scroll_target_cards_override: Optional[int] = None,
    scroll_max_rounds_override: Optional[int] = None,
    verify_proxy_ip: bool = False,
    proxy_ip_check_url: Optional[str] = None,
    proxy_ip_check_timeout_ms: int = 20000,
    proxy_ip_expected: Optional[str] = None,
    browser_context_options: Optional[Dict[str, Any]] = None,
    extra_http_headers: Optional[Dict[str, str]] = None,
    browser_max_connections_per_proxy: Optional[int] = None,
    browser_max_connections_per_host: Optional[int] = None,
    blocked_resource_types: Optional[List[str]] = None,
):
    """
    Scrape Facebook Marketplace for iPhone listings.
    
    Uses a persistent browser profile to maintain login state and appear
    more like a real user. On first run, use headless=False to log in manually.
    """
    init_db()
    price_data = load_price_list()
    runtime_settings = load_runtime_scraper_settings()
    inter_query_delay_min = runtime_settings["scrape_delay_min_seconds"]
    inter_query_delay_max = runtime_settings["scrape_delay_max_seconds"]
    scroll_target_cards = runtime_settings["scrape_scroll_target_cards"]
    scroll_max_rounds = runtime_settings["scrape_scroll_max_rounds"]
    if scroll_target_cards_override is not None:
        try:
            scroll_target_cards = max(20, int(scroll_target_cards_override))
        except (TypeError, ValueError):
            pass
    if scroll_max_rounds_override is not None:
        try:
            scroll_max_rounds = max(2, int(scroll_max_rounds_override))
        except (TypeError, ValueError):
            pass
    explicit_queries = [q.strip() for q in (search_queries or []) if str(q).strip()]
    if explicit_queries:
        active_queries = explicit_queries
    else:
        active_queries = load_active_search_queries(max_queries=runtime_settings["max_queries_per_run"])

    def emit_progress(event: str, **payload):
        if not progress_callback:
            return
        try:
            progress_callback({"event": event, **payload})
        except Exception:
            # Progress updates should never break scraping flow.
            pass
    
    # Try to import playwright-stealth for additional evasion.
    # Support both old API (`stealth_async`) and newer API (`Stealth().apply_stealth_async`).
    stealth_apply_async = None
    stealth_mode = ""
    try:
        import playwright_stealth as playwright_stealth_module

        if hasattr(playwright_stealth_module, "stealth_async"):
            stealth_apply_async = playwright_stealth_module.stealth_async
            stealth_mode = "stealth_async"
        elif hasattr(playwright_stealth_module, "Stealth"):
            stealth_apply_async = playwright_stealth_module.Stealth().apply_stealth_async
            stealth_mode = "Stealth.apply_stealth_async"
    except Exception:
        stealth_apply_async = None
        stealth_mode = ""

    has_stealth = stealth_apply_async is not None
    if has_stealth:
        print(f"  Using playwright-stealth ({stealth_mode}) for enhanced anti-detection")
    else:
        print("  playwright-stealth not installed. Using built-in stealth scripts.")
        print("  For better results, run: pip install playwright-stealth")

    # Import playwright
    from playwright.async_api import async_playwright

    safe_max_connections_per_proxy = _parse_env_int(
        str(browser_max_connections_per_proxy) if browser_max_connections_per_proxy is not None else None,
        default=BROWSER_MAX_CONNECTIONS_PER_PROXY,
        minimum=1,
    )
    safe_max_connections_per_host = _parse_env_int(
        str(browser_max_connections_per_host) if browser_max_connections_per_host is not None else None,
        default=BROWSER_MAX_CONNECTIONS_PER_HOST,
        minimum=1,
    )
    if blocked_resource_types is None:
        effective_blocked_resource_types = set(BROWSER_BLOCK_RESOURCE_TYPES)
    else:
        effective_blocked_resource_types = {
            str(token).strip().lower()
            for token in blocked_resource_types
            if str(token).strip()
        }

    async with async_playwright() as p:
        # Use a persistent context (like a real browser profile)
        # This preserves cookies, localStorage, and login state between runs
        launch_kwargs: Dict[str, Any] = {
            "user_data_dir": str(user_data_dir or USER_DATA_DIR),
            "headless": headless,
            # Use realistic browser arguments
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-infobars",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-background-networking",
                f"--max-connections-per-proxy={safe_max_connections_per_proxy}",
                f"--max-connections-per-host={safe_max_connections_per_host}",
                "--window-size=1920,1080",
            ],
            # Realistic viewport
            "viewport": {"width": 1920, "height": 1080},
            # Realistic user agent (macOS Chrome)
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            # Locale and timezone matching Perth
            "locale": "en-AU",
            "timezone_id": "Australia/Perth",
            # Geolocation for Perth
            "geolocation": {"latitude": -31.9505, "longitude": 115.8605},
            "permissions": ["geolocation"],
            # Realistic color scheme
            "color_scheme": "light",
        }
        if proxy:
            launch_kwargs["proxy"] = proxy
        if isinstance(browser_context_options, dict):
            for key in (
                "viewport",
                "screen",
                "device_scale_factor",
                "user_agent",
                "timezone_id",
                "locale",
                "color_scheme",
            ):
                value = browser_context_options.get(key)
                if value is not None:
                    launch_kwargs[key] = value
        if isinstance(extra_http_headers, dict):
            headers = {
                str(key).strip(): str(value).strip()
                for key, value in extra_http_headers.items()
                if str(key).strip() and str(value).strip()
            }
            if headers:
                launch_kwargs["extra_http_headers"] = headers

        context = await p.chromium.launch_persistent_context(**launch_kwargs)
        
        page = context.pages[0] if context.pages else await context.new_page()
        if effective_blocked_resource_types:
            async def _resource_filter(route):
                try:
                    resource_type = str(route.request.resource_type or "").strip().lower()
                except Exception:
                    resource_type = ""
                try:
                    if resource_type in effective_blocked_resource_types:
                        await route.abort()
                        return
                    await route.continue_()
                except Exception:
                    try:
                        await route.continue_()
                    except Exception:
                        pass

            await context.route("**/*", _resource_filter)
        
        # Apply stealth scripts
        if has_stealth:
            try:
                await stealth_apply_async(page)
            except Exception as exc:
                print(f"  playwright-stealth apply failed ({exc}); using built-in stealth scripts.")
                await apply_stealth_scripts(page)
        else:
            await apply_stealth_scripts(page)

        if verify_proxy_ip and proxy:
            check_url = str(proxy_ip_check_url or "https://api.ipify.org?format=json").strip() or "https://api.ipify.org?format=json"
            try:
                check_timeout_ms = max(1000, int(proxy_ip_check_timeout_ms))
            except (TypeError, ValueError):
                check_timeout_ms = 20000
            expected_ip = _extract_ip_from_text(proxy_ip_expected or "")
            observed_ip: Optional[str] = None
            status = "check_failed"
            try:
                await page.goto(check_url, wait_until="domcontentloaded", timeout=check_timeout_ms)
                body_text = (await page.text_content("body") or "").strip()
                decoded = decode_json_body(body_text)
                observed_ip = _extract_proxy_check_ip(decoded)
                if not observed_ip:
                    observed_ip = _extract_proxy_check_ip(body_text)
                status = "checked" if observed_ip else "no_ip"
            except Exception as exc:
                status = "check_failed"
                print(f"  Proxy IP verification failed ({exc}); continuing scrape cycle.")
            if expected_ip and observed_ip and expected_ip != observed_ip:
                status = "mismatch"
            emit_progress(
                "proxy_ip_check",
                url=check_url,
                expected_ip=expected_ip,
                observed_ip=observed_ip,
                status=status,
            )
            if status == "mismatch":
                raise RuntimeError(
                    f"proxy_mismatch: expected={expected_ip} observed={observed_ip} url={check_url}"
                )

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        processed_listings: List[Dict[str, Any]] = []
        seen_listing_ids: set[str] = set()
        new_listings_count = 0
        
        # If not logged in, navigate to Facebook first for manual login
        if not headless:
            print("\n" + "=" * 60)
            print("MANUAL LOGIN REQUIRED")
            print("=" * 60)
            print("A browser window will open. Please:")
            print("1. Log in to Facebook manually")
            print("2. Navigate to Marketplace to confirm access")
            print("3. Come back to this terminal and press Enter")
            print("=" * 60)
            
            await page.goto("https://www.facebook.com/login", wait_until="domcontentloaded", timeout=60000)
            input("\nPress Enter after you have logged in and can see Marketplace...")
            
            # Give it a moment after login
            await random_delay(3, 5)
        
        total_queries = len(active_queries)
        processed_queries = 0
        cancelled = False
        manual_login_error: Optional[str] = None

        for index, query in enumerate(active_queries, start=1):
            if stop_event and stop_event.is_set():
                cancelled = True
                print("  Scrape cancellation requested. Stopping after current progress...")
                emit_progress(
                    "cancelled",
                    query_index=processed_queries,
                    query_total=total_queries,
                )
                break

            emit_progress(
                "query_start",
                query=query,
                query_index=index,
                query_total=total_queries,
            )
            print("  Searching for: {}".format(query))
            
            # Construct the Marketplace search URL
            search_url = "https://www.facebook.com/marketplace/perth/search?query={}&exact=false&sortBy=creation_time_descend".format(
                query.replace(" ", "%20")
            )

            graphql_candidates: Dict[str, Dict[str, Any]] = {}
            graphql_tasks: set[asyncio.Task] = set()

            async def consume_graphql_response(response):
                """Parse marketplace listings from GraphQL responses captured during page activity."""
                try:
                    request = response.request
                    if request.method != "POST" or "/api/graphql/" not in response.url:
                        return

                    payload = decode_json_body(await response.text())
                    if payload is None:
                        return

                    listings_from_payload = extract_listings_from_graphql_payload(payload)
                    for extracted in listings_from_payload:
                        listing_id = extracted.get("id")
                        if not listing_id:
                            continue

                        existing = graphql_candidates.get(listing_id)
                        if not existing:
                            graphql_candidates[listing_id] = extracted
                        else:
                            merged = merge_listing_candidates([existing, extracted])
                            if merged:
                                graphql_candidates[listing_id] = merged[0]
                except Exception:
                    # GraphQL parsing is a best-effort enrichment path.
                    return

            def on_response(response):
                if "/api/graphql/" not in response.url:
                    return

                task = asyncio.create_task(consume_graphql_response(response))
                graphql_tasks.add(task)
                task.add_done_callback(lambda done_task: graphql_tasks.discard(done_task))

            page.on("response", on_response)

            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                # Wait for page to settle with human-like delay
                await random_delay(3, 6)
                checkpoint_reason = await _detect_manual_login_required_state(page)
                if checkpoint_reason:
                    raise RuntimeError(f"{MANUAL_LOGIN_REQUIRED_PREFIX} {checkpoint_reason}")

                dom_candidates_by_id: Dict[str, Dict[str, Any]] = {}

                def _dom_candidate_score(candidate: Dict[str, Any]) -> int:
                    title = str(candidate.get("title") or "")
                    score = 0
                    if title:
                        score += 1
                    if "iphone" in title.lower():
                        score += 5
                    if candidate.get("price"):
                        score += 3
                    if len(title) > 15:
                        score += 1
                    if candidate.get("url"):
                        score += 1
                    return score

                async def capture_dom_snapshot() -> None:
                    dom_snapshot = await _extract_dom_listing_candidates(page)
                    for listing in dom_snapshot:
                        listing_id = _extract_text_value(listing.get("id"))
                        if not listing_id:
                            continue

                        candidate: Dict[str, Any] = {
                            "id": listing_id,
                            "title": _extract_text_value(listing.get("title")),
                            "url": _normalize_marketplace_url(listing.get("url"), listing_id),
                            "price": listing.get("price"),
                            "location": "",
                            "description": "",
                            "seller_name": "",
                        }
                        candidate_score = _dom_candidate_score(candidate)

                        existing = dom_candidates_by_id.get(listing_id)
                        if not existing or candidate_score > int(existing.get("_score", 0)):
                            candidate["_score"] = candidate_score
                            dom_candidates_by_id[listing_id] = candidate

                scroll_stats = await progressive_marketplace_scroll(
                    page,
                    target_cards=scroll_target_cards,
                    max_rounds=scroll_max_rounds,
                    snapshot_callback=capture_dom_snapshot,
                )
                await random_delay(0.8, 1.8)
                checkpoint_reason = await _detect_manual_login_required_state(page)
                if checkpoint_reason:
                    raise RuntimeError(f"{MANUAL_LOGIN_REQUIRED_PREFIX} {checkpoint_reason}")
                await capture_dom_snapshot()

                if graphql_tasks:
                    await asyncio.wait(list(graphql_tasks), timeout=8)

                normalized_dom_listings: List[Dict[str, Any]] = []
                for listing in dom_candidates_by_id.values():
                    payload = dict(listing)
                    payload.pop("_score", None)
                    normalized_dom_listings.append(payload)

                query_listings = merge_listing_candidates(
                    list(graphql_candidates.values()) + normalized_dom_listings
                )
                query_new_saved = 0
                for listing in query_listings:
                    listing_id = _extract_text_value(listing.get("id"))
                    if not listing_id or listing_id in seen_listing_ids:
                        continue
                    seen_listing_ids.add(listing_id)

                    saved_listing = _store_listing_candidate(cursor, listing, price_data)
                    if not saved_listing:
                        continue

                    conn.commit()
                    new_listings_count += 1
                    query_new_saved += 1
                    processed_listings.append(saved_listing)
                    emit_progress(
                        "listing_saved",
                        query=query,
                        query_index=index,
                        query_total=total_queries,
                        new_count=new_listings_count,
                        listing=saved_listing,
                    )

                found_count = len(query_listings)
                graphql_count = len(graphql_candidates)
                dom_count = len(normalized_dom_listings)
                print(
                    "    Found {} listings (graphql: {}, dom: {}, page_cards: {}, scroll_rounds: {}, new saved: {})".format(
                        found_count,
                        graphql_count,
                        dom_count,
                        scroll_stats.get("visible_cards", 0),
                        scroll_stats.get("scroll_rounds", 0),
                        query_new_saved,
                    )
                )
                emit_progress(
                    "query_result",
                    query=query,
                    query_index=index,
                    query_total=total_queries,
                    found=found_count,
                    graphql_found=graphql_count,
                    dom_found=dom_count,
                    page_cards=scroll_stats.get("visible_cards", 0),
                    scroll_rounds=scroll_stats.get("scroll_rounds", 0),
                    new_saved=query_new_saved,
                )
                
            except Exception as e:
                error_text = str(e)
                print("  Error scraping query '{}': {}".format(query, error_text))
                emit_progress(
                    "query_error",
                    query=query,
                    query_index=index,
                    query_total=total_queries,
                    error=error_text,
                )
                if MANUAL_LOGIN_REQUIRED_PREFIX.lower() in error_text.lower():
                    manual_login_error = error_text
            finally:
                try:
                    page.remove_listener("response", on_response)
                except Exception:
                    pass
                mark_search_query_polled(query)

            processed_queries = index

            if stop_event and stop_event.is_set():
                cancelled = True
                print("  Scrape cancellation requested. Stopping after current progress...")
                emit_progress(
                    "cancelled",
                    query_index=processed_queries,
                    query_total=total_queries,
                )
                break
            
            # Human-like delay between searches (longer and more variable)
            await random_delay(inter_query_delay_min, inter_query_delay_max)

        if manual_login_error:
            conn.close()
            await context.close()
            raise RuntimeError(manual_login_error)
        
        removed_accessory_count = purge_accessory_only_listings(cursor)
        if removed_accessory_count > 0:
            conn.commit()
            print(
                "  Removed {} accessory-only listing(s) from local DB.".format(
                    removed_accessory_count
                )
            )
            emit_progress("accessory_cleanup", removed=removed_accessory_count)

        conn.close()
        await context.close()
        
        print("\n  Total new listings found: {}".format(new_listings_count))
        emit_progress(
            "completed",
            new_count=new_listings_count,
            query_index=processed_queries,
            query_total=total_queries,
            cancelled=cancelled,
        )
        
        return processed_listings


def get_pending_listings():
    """Get all new listings that haven't been contacted yet."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM listings WHERE status = 'new' AND potential_profit > 0 ORDER BY potential_profit DESC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def recalculate_listing_financials(csv_path: str = "price_list.csv") -> int:
    """
    Recalculate model, condition, max buy, and profit for all existing listings.

    Useful after editing price_list.csv so GUI/CLI values are immediately refreshed.
    Returns number of listings updated.
    """
    init_db()
    price_data = load_price_list(csv_path=csv_path)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, description, price, status FROM listings")
    rows = cursor.fetchall()

    updated = 0
    managed_statuses = {"new", "unclassified", "needs_pricing"}
    delete_rows: List[Tuple[str]] = []

    for row in rows:
        title = (row["title"] or "").strip()
        description = row["description"] or ""
        existing_status = row["status"] or "new"
        listed_price = row["price"]
        inferred_price = (
            extract_currency_price_from_text(title)
            or extract_currency_price_from_text(description)
            or extract_shorthand_k_price_from_text(title)
            or extract_shorthand_k_price_from_text(description)
        )
        derived_price = listed_price
        if derived_price is None:
            derived_price = inferred_price
        elif inferred_price is not None and derived_price < 20 and inferred_price >= 100:
            derived_price = inferred_price

        if existing_status in managed_statuses and is_accessory_only_listing(title, description, derived_price):
            delete_rows.append((str(row["id"]),))
            continue

        detected_model = identify_model(title, description)
        resolved_model = resolve_price_model_key(detected_model, price_data)

        if existing_status in managed_statuses and not resolved_model:
            # Keep local queue aligned to price-sheet scope.
            delete_rows.append((str(row["id"]),))
            continue

        if resolved_model:
            model = resolved_model
            condition, issues = assess_condition(title, description)
            max_offer, _ = calculate_max_offer(model, condition, issues, price_data)
            profit = calculate_profit_for_listing(
                model=model,
                condition=condition,
                issues=issues,
                price_data=price_data,
                listed_price=derived_price,
                fallback_purchase_price=max_offer,
            )
            computed_status = "new"
        else:
            model = "Unknown"
            condition = "unknown"
            max_offer = 0.0
            profit = 0.0
            computed_status = "unclassified"

        next_status = computed_status if existing_status in managed_statuses else existing_status
        cursor.execute(
            """
            UPDATE listings
            SET price = ?, model = ?, condition = ?, max_buy_price = ?, potential_profit = ?, status = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                float(round(derived_price, 2)) if derived_price is not None else None,
                model,
                condition,
                float(round(max_offer, 2)),
                float(round(profit, 2)),
                next_status,
                datetime.now().isoformat(),
                row["id"],
            ),
        )
        updated += 1

    if delete_rows:
        cursor.executemany("DELETE FROM listings WHERE id = ?", delete_rows)

    conn.commit()
    conn.close()
    return updated


if __name__ == "__main__":
    # Test the scraper
    print("Testing Scraper...")
    asyncio.run(scrape_marketplace(headless=True))
