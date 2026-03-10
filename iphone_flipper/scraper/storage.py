"""
Scraper storage layer: database initialisation, settings, account management,
price list loading, and listing persistence.
"""

import csv
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from scraper.config import (
    ACCESSORY_SIGNAL_KEYWORDS,
    DB_PATH,
    DEFAULT_ACCESSORY_KEYWORD_CSV,
    DEFAULT_ACCESSORY_MAX_PRICE,
    PACKAGE_ROOT,
    SEARCH_QUERIES,
    _ACCESSORY_FILTER_CACHE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Accessory-filter in-memory cache
# ---------------------------------------------------------------------------

_accessory_filter_cache: Dict[str, Any] = {
    "loaded_at": 0.0,
    "keywords": tuple(ACCESSORY_SIGNAL_KEYWORDS),
    "max_price": float(DEFAULT_ACCESSORY_MAX_PRICE),
}

_LISTING_COLUMN_DEFINITIONS: Dict[str, str] = {
    "thumbnail_url": "TEXT",
    "enrichment_status": "TEXT DEFAULT 'complete'",
    "enrichment_source_hash": "TEXT",
    "enriched_at": "TEXT",
    "enrichment_last_error": "TEXT",
}


def _ensure_listing_columns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("PRAGMA table_info(listings)")
    existing_columns = {str(row[1]) for row in cursor.fetchall()}
    for column_name, definition in _LISTING_COLUMN_DEFINITIONS.items():
        if column_name in existing_columns:
            continue
        cursor.execute(f"ALTER TABLE listings ADD COLUMN {column_name} {definition}")

# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------


def init_db() -> None:
    """Create core tables if they don't already exist."""
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
            thumbnail_url TEXT,
            model TEXT,
            condition TEXT,
            max_buy_price REAL,
            potential_profit REAL,
            status TEXT DEFAULT 'new',
            enrichment_status TEXT DEFAULT 'complete',
            enrichment_source_hash TEXT,
            enriched_at TEXT,
            enrichment_last_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    _ensure_listing_columns(cursor)
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
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Runtime scraper settings
# ---------------------------------------------------------------------------


def load_runtime_scraper_settings() -> Dict[str, int]:
    """Load operator-tunable scraper settings from the database."""
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
        "SELECT setting_key, setting_value FROM scraper_settings WHERE setting_key IN (?, ?, ?, ?, ?)",
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
    return settings


def get_scraper_setting(key: str, default: str = "") -> str:
    """Retrieve a single scraper setting from the database."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT setting_value FROM scraper_settings WHERE setting_key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return str(row[0]) if row and row[0] is not None else str(default)


def set_scraper_setting(key: str, value: str) -> None:
    """Insert or update a scraper setting in the database."""
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


# ---------------------------------------------------------------------------
# Accessory filter helpers
# ---------------------------------------------------------------------------


def _parse_accessory_keywords_csv(raw_keywords: Any) -> Tuple[str, ...]:
    """Parse a CSV string of accessory keywords into a deduplicated tuple."""
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
    """Load accessory filter config with caching."""
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
            "SELECT setting_key, setting_value FROM scraper_settings WHERE setting_key IN (?, ?)",
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
    except sqlite3.Error:
        logger.warning("Failed to load accessory filter settings from DB; using defaults", exc_info=True)

    _accessory_filter_cache["loaded_at"] = now_ts
    _accessory_filter_cache["keywords"] = keywords
    _accessory_filter_cache["max_price"] = max_price
    return {"keywords": keywords, "max_price": max_price}


# ---------------------------------------------------------------------------
# Search query management
# ---------------------------------------------------------------------------


def load_active_search_queries(max_queries: Optional[int] = None) -> List[str]:
    """Return active search query keywords from the database (or defaults)."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT keywords FROM search_queries WHERE COALESCE(is_active, 1) = 1 ORDER BY id ASC"
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


def mark_search_query_polled(keyword: str) -> None:
    """Update the last-polled timestamp for a search query keyword."""
    normalized = str(keyword or "").strip()
    if not normalized:
        return
    init_db()
    now_iso = datetime.now().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE search_queries SET last_polled = ?, updated_at = ? WHERE LOWER(TRIM(keywords)) = LOWER(TRIM(?))",
        (now_iso, now_iso, normalized),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Facebook account management
# ---------------------------------------------------------------------------


def update_fb_account_runtime_status(account_id: int, success: bool, reason: str = "") -> None:
    """Update an account's health status after a scrape attempt."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT UPPER(COALESCE(status, 'ACTIVE')), COALESCE(failure_count, 0), COALESCE(cooldown_until, '') FROM fb_accounts WHERE id = ?",
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
    now = datetime.now()
    now_iso = now.isoformat()

    if current_status == "BANNED":
        conn.close()
        return

    if success:
        cursor.execute(
            "UPDATE fb_accounts SET status = 'ACTIVE', failure_count = 0, cooldown_until = NULL, updated_at = ? WHERE id = ?",
            (now_iso, account_id),
        )
        conn.commit()
        conn.close()
        return

    next_failure = failure_count + 1
    cursor.execute(
        "UPDATE fb_accounts SET failure_count = ?, updated_at = ? WHERE id = ?",
        (next_failure, now_iso, account_id),
    )
    conn.commit()
    conn.close()


def load_notify_profitable_only_setting() -> bool:
    """Return whether notifications should be limited to positive-profit listings."""
    raw = get_scraper_setting("notify_profitable_only", "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _parse_iso_timestamp(value: Any) -> Optional[datetime]:
    """Safely parse an ISO-format timestamp string."""
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _get_account_context_by_id(account_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a single FB account context (with proxy) by ID."""
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
    """Get the currently-selected active scraper account context."""
    raw = get_scraper_setting("active_scraper_account_id", "").strip()
    if not raw:
        return None
    try:
        account_id = int(raw)
    except ValueError:
        return None
    return _get_account_context_by_id(account_id)


def list_eligible_scraper_account_contexts() -> List[Dict[str, Any]]:
    """Return eligible FB accounts with ACTIVE/expired-COOLDOWN + ACTIVE SOCKS5 proxy."""
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
    """Select the next scraper account using round-robin with reuse cooldown."""
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


def activate_account(account_id: int) -> None:
    """Mark an account as ACTIVE after successful use."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE fb_accounts SET status = 'ACTIVE', cooldown_until = NULL, updated_at = ? WHERE id = ?",
        (datetime.now().isoformat(), account_id),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Price list & listing persistence
# ---------------------------------------------------------------------------


def load_price_list(csv_path: str = "price_list.csv") -> dict:
    """Load the iPhone price / repair cost reference data from a CSV file."""
    price_data = {}
    target_path = PACKAGE_ROOT / csv_path
    if not target_path.exists():
        return {}

    with open(target_path, "r") as f:
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


def save_listing(cursor: sqlite3.Cursor, listing: Dict[str, Any]) -> bool:
    """Insert a listing into the database (ignoring duplicates)."""
    cursor.execute(
        """
        INSERT OR IGNORE INTO listings (
            id, title, price, location, url, description, seller_name,
            thumbnail_url, model, condition, max_buy_price, potential_profit, status,
            enrichment_status, enrichment_source_hash, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            listing["id"],
            listing.get("title"),
            listing.get("price"),
            listing.get("location"),
            listing.get("url"),
            listing.get("description"),
            listing.get("seller_name"),
            listing.get("thumbnail_url"),
            listing.get("model"),
            listing.get("condition"),
            listing.get("max_offer"),
            listing.get("potential_profit"),
            listing.get("status", "new"),
            listing.get("enrichment_status", "complete"),
            listing.get("enrichment_source_hash"),
            datetime.now().isoformat(),
            datetime.now().isoformat(),
        ),
    )
    return cursor.rowcount > 0


def get_pending_listings() -> list:
    """Retrieve all new listings with positive profit, sorted by profit descending."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM listings WHERE status = 'new' AND potential_profit > 0 ORDER BY potential_profit DESC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]
