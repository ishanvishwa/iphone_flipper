import csv
import json
import sqlite3
import time
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from .config import ACCESSORY_SIGNAL_KEYWORDS
from .config import DB_PATH
from .config import DEFAULT_ACCESSORY_KEYWORD_CSV
from .config import DEFAULT_ACCESSORY_MAX_PRICE
from .config import PACKAGE_ROOT
from .config import SEARCH_QUERIES
from .config import SEARCH_RADIUS_KM
from .config import _ACCESSORY_FILTER_CACHE_TTL_SECONDS
from .config import _parse_accessory_keywords_csv


_accessory_filter_cache: Dict[str, Any] = {
    "loaded_at": 0.0,
    "keywords": tuple(ACCESSORY_SIGNAL_KEYWORDS),
    "max_price": float(DEFAULT_ACCESSORY_MAX_PRICE),
}


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

    # Lightweight schema migrations
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


def _parse_iso_timestamp(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


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


def activate_account(account_id: int):
    """Mark an account as ACTIVE after successful use."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE fb_accounts SET status = 'ACTIVE', cooldown_until = NULL, updated_at = ? WHERE id = ?",
        (datetime.now().isoformat(), account_id),
    )
    conn.commit()
    conn.close()


def load_price_list(csv_path: str = "price_list.csv") -> dict:
    """Load the price list from CSV into a dictionary."""
    price_data = {}
    
    # Try multiple paths for robustness (core vs root)
    target_path = PACKAGE_ROOT / csv_path
    if not target_path.exists():
        # Fallback to local import if called from different context
        target_path = Path(__file__).parent / csv_path
    
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
    """
    Persist a fully processed listing dict to DB.
    Returns True if inserted, False if ignored (exists).
    """
    cursor.execute(
        """
        INSERT OR IGNORE INTO listings (
            id, title, price, location, url, description, seller_name,
            model, condition, max_buy_price, potential_profit, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            listing["id"],
            listing.get("title"),
            listing.get("price"),
            listing.get("location"),
            listing.get("url"),
            listing.get("description"),
            listing.get("seller_name"),
            listing.get("model"),
            listing.get("condition"),
            listing.get("max_offer"),
            listing.get("potential_profit"),
            listing.get("status", "new"),
            datetime.now().isoformat(),
        ),
    )
    return cursor.rowcount > 0


def get_pending_listings():
    """Get all new listings that haven't been contacted yet."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM listings WHERE status = 'new' AND potential_profit > 0 ORDER BY potential_profit DESC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]
