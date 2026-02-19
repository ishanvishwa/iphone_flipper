"""
Scraper configuration: constants, environment variable parsing, and paths.
"""

import os
from pathlib import Path


# ---------------------------------------------------------------------------
# Environment parsing helpers
# ---------------------------------------------------------------------------

def _parse_env_int(raw_value: str | None, default: int, minimum: int = 1) -> int:
    """Parse an environment variable as an integer with a minimum bound."""
    try:
        parsed = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), parsed)


def _parse_env_bool(raw_value: str | None, default: bool = False) -> bool:
    """Parse an environment variable as a boolean flag."""
    value = str(raw_value or "").strip().lower()
    if not value:
        return bool(default)
    return value in {"1", "true", "yes", "on"}


def _parse_blocked_resource_types(raw_value: str | None) -> set[str]:
    """Parse a comma-separated list of Playwright resource types to block."""
    default_types: set[str] = set()
    text = str(raw_value or "").strip()
    if not text:
        return default_types
    parsed = {token.strip().lower() for token in text.split(",") if token.strip()}
    return parsed or default_types


# ---------------------------------------------------------------------------
# Search & scraping constants
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Accessory filtering constants
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Checkpoint / ban-detection markers
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Internal cache TTL
# ---------------------------------------------------------------------------

_ACCESSORY_FILTER_CACHE_TTL_SECONDS = 10.0

# ---------------------------------------------------------------------------
# Browser / Playwright connection tuning (from env vars)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Proxy bridge configuration (from env vars)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Filesystem paths
# ---------------------------------------------------------------------------

# PACKAGE_ROOT points to the *parent* of the scraper/ package directory,
# i.e. the ``iphone_flipper/`` project root — matching the original
# ``Path(__file__).parent`` when scraper.py was a single flat file.
PACKAGE_ROOT = Path(__file__).resolve().parent.parent

DB_PATH = Path(
    os.getenv("IPHONE_FLIPPER_DB_PATH", str(PACKAGE_ROOT / "listings.db"))
)

USER_DATA_DIR = PACKAGE_ROOT / "browser_profile"
