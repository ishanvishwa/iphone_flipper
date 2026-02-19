"""
scraper package — backward-compatible re-export surface.

All public names previously importable via ``from scraper import X`` remain
available through this ``__init__.py``.  New code should prefer importing from
the specific sub-module (e.g. ``from scraper.browser import apply_stealth_scripts``).
"""

# --- config ---
from scraper.config import (  # noqa: F401
    ACCESSORY_ONLY_PHRASES,
    ACCESSORY_SIGNAL_KEYWORDS,
    BROWSER_BLOCK_RESOURCE_TYPES,
    BROWSER_MAX_CONNECTIONS_PER_HOST,
    BROWSER_MAX_CONNECTIONS_PER_PROXY,
    CHECKPOINT_TEXT_MARKERS,
    CHECKPOINT_URL_MARKERS,
    DB_PATH,
    DEFAULT_ACCESSORY_KEYWORD_CSV,
    DEFAULT_ACCESSORY_MAX_PRICE,
    DEVICE_SALE_SIGNALS,
    FORCE_LOCAL_PROXY_BRIDGE,
    MANUAL_LOGIN_REQUIRED_PREFIX,
    PACKAGE_ROOT,
    PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS,
    PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS,
    PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS,
    PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS,
    SEARCH_LOCATION,
    SEARCH_QUERIES,
    SEARCH_RADIUS_KM,
    USER_DATA_DIR,
    _ACCESSORY_FILTER_CACHE_TTL_SECONDS,
    _parse_blocked_resource_types,
    _parse_env_bool,
    _parse_env_int,
)

# --- storage ---
from scraper.storage import (  # noqa: F401
    activate_account,
    get_pending_listings,
    get_scraper_setting,
    init_db,
    list_eligible_scraper_account_contexts,
    load_accessory_filter_settings,
    load_active_search_queries,
    load_notify_profitable_only_setting,
    load_price_list,
    load_runtime_scraper_settings,
    mark_search_query_polled,
    reserve_next_scraper_account_for_run,
    save_listing,
    set_scraper_setting,
    update_fb_account_runtime_status,
)

# --- parsers ---
from scraper.parsers import (  # noqa: F401
    assess_condition,
    calculate_max_offer,
    calculate_profit_for_listing,
    extract_currency_price_from_text,
    extract_shorthand_k_price_from_text,
    identify_model,
    is_accessory_only_listing,
    parse_listing_price,
    resolve_price_model_key,
)

# --- proxy ---
from scraper.proxy import (  # noqa: F401
    LOCAL_SOCKS5_BRIDGE_SCRIPT,
    build_playwright_proxy_for_account,
    cleanup_bridge_process,
    reserve_and_build_scraper_runtime_context,
    _find_free_local_port,
    _start_local_socks5_auth_bridge,
)

# --- browser ---
from scraper.browser import (  # noqa: F401
    apply_stealth_scripts,
    launch_browser_context,
    random_delay,
)

# --- core ---
from scraper.core import (  # noqa: F401
    purge_accessory_only_listings,
    recalculate_listing_financials,
    scrape_marketplace,
)
