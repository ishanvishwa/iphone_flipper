"""
Scraper parsers: price extraction, model identification, condition
assessment, and accessory-only detection.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from scraper.config import (
    ACCESSORY_SIGNAL_KEYWORDS,
    DEFAULT_ACCESSORY_MAX_PRICE,
)
from scraper.storage import load_accessory_filter_settings


# ---------------------------------------------------------------------------
# Price extraction
# ---------------------------------------------------------------------------


def _parse_numeric_price_token(token: str) -> Optional[float]:
    """Normalise a numeric string (with commas / dots) and return a float."""
    if not token:
        return None
    value = token.strip().replace(" ", "")
    if not value:
        return None
    if "," in value and "." in value:
        if value.rfind(".") > value.rfind(","):
            normalized = value.replace(",", "")
        else:
            normalized = value.replace(".", "").replace(",", ".")
    elif "," in value:
        if value.count(",") == 1 and len(value.split(",")[1]) in (1, 2):
            normalized = value.replace(",", ".")
        else:
            normalized = value.replace(",", "")
    elif "." in value:
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
    """Extract an AUD / USD price from free-form text (e.g. ``$450``)."""
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
    """Extract shorthand prices like ``1.2k`` or ``$1k`` from text."""
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


def parse_listing_price(price_raw: Any) -> Optional[float]:
    """Parse a listing price from various input types (dict, str, number)."""
    if price_raw is None:
        return None
    if isinstance(price_raw, (int, float)):
        return float(price_raw)
    if isinstance(price_raw, dict):
        for key in ("amount", "value", "price"):
            value = price_raw.get(key)
            if isinstance(value, (int, float)):
                return float(value)
        return None
    if isinstance(price_raw, str):
        cleaned = price_raw.strip()
        if not cleaned:
            return None
        currency_value = extract_currency_price_from_text(cleaned)
        if currency_value is not None:
            return currency_value
        shorthand_value = extract_shorthand_k_price_from_text(cleaned)
        if shorthand_value is not None:
            return shorthand_value
        numeric_only = cleaned.replace(" ", "")
        if re.fullmatch(r"\d[\d.,]*", numeric_only):
            return _parse_numeric_price_token(numeric_only)
        if re.search(r"[A-Za-z]", cleaned):
            return None
        match = re.search(r"\d[\d.,]*", cleaned)
        if not match:
            return None
        return _parse_numeric_price_token(match.group(0))
    return None


# ---------------------------------------------------------------------------
# Accessory detection
# ---------------------------------------------------------------------------


def is_accessory_only_listing(title: str, description: str = "", listed_price: Any = None) -> bool:
    """Return True if the listing looks like an accessory rather than a device."""
    filter_settings = load_accessory_filter_settings()
    accessory_keywords = tuple(filter_settings.get("keywords") or ACCESSORY_SIGNAL_KEYWORDS)
    max_accessory_price = float(filter_settings.get("max_price") or DEFAULT_ACCESSORY_MAX_PRICE)
    title_text = str(title or "")
    description_text = str(description or "")
    text = f"{title_text} {description_text}".strip().lower()
    text = re.sub(r"\s+", " ", text)
    if not text or "iphone" not in text:
        return False
    if any(keyword in text for keyword in accessory_keywords):
        return True
    numeric_price = parse_listing_price(listed_price)
    if numeric_price is not None and numeric_price <= max_accessory_price:
        return True
    return False


# ---------------------------------------------------------------------------
# Model identification & condition assessment
# ---------------------------------------------------------------------------


def identify_model(title: str, description: str = "") -> Optional[str]:
    """Identify the iPhone model from listing title / description text."""
    text = f"{title} {description}".lower()
    models = [
        "iPhone 16 Pro Max", "iPhone 16 Pro", "iPhone 16 Plus", "iPhone 16",
        "iPhone 15 Pro Max", "iPhone 15 Pro", "iPhone 15 Plus", "iPhone 15",
        "iPhone 14 Pro Max", "iPhone 14 Pro", "iPhone 14 Plus", "iPhone 14",
        "iPhone 13 Pro Max", "iPhone 13 Pro", "iPhone 13 Mini", "iPhone 13",
        "iPhone 12 Pro Max", "iPhone 12 Pro", "iPhone 12 Mini", "iPhone 12",
        "iPhone 11 Pro Max", "iPhone 11 Pro", "iPhone 11",
    ]
    for model in models:
        if model.lower() in text:
            return model
    return None


def resolve_price_model_key(model: Optional[str], price_data: Dict[str, Any]) -> Optional[str]:
    """Return the price-data key for *model*, or ``None`` if not found."""
    raw_model = str(model or "").strip()
    if raw_model in price_data:
        return raw_model
    return None


def assess_condition(title: str, description: str = "") -> Tuple[str, List[str]]:
    """Return (condition_label, issues_list) based on listing text."""
    text = f"{title} {description}".lower()
    issues = []
    if "cracked" in text:
        issues.append("cracked screen")
    return "good" if not issues else "repairable_minor", issues


# ---------------------------------------------------------------------------
# Financial calculations
# ---------------------------------------------------------------------------


def calculate_max_offer(
    model: str,
    condition: str,
    issues: List[str],
    price_data: Dict[str, Any],
) -> Tuple[float, float]:
    """Calculate the maximum offer price and expected margin for *model*."""
    if model not in price_data:
        return 0.0, 0.0
    model_data = price_data[model]
    base = model_data["buying_price"]
    if condition != "good":
        base *= 0.8  # Simple logic for now
    return float(round(base, 2)), float(round(model_data["selling_price"] - base, 2))


def calculate_profit_for_listing(
    model: str,
    condition: str,
    issues: List[str],
    price_data: Dict[str, Any],
    listed_price: Optional[float],
    fallback_purchase_price: Optional[float] = None,
) -> float:
    """Calculate expected profit for a listing given pricing data."""
    if model not in price_data:
        return 0.0
    model_data = price_data[model]
    purchase = listed_price if listed_price is not None else fallback_purchase_price
    if purchase is None:
        return 0.0
    return float(round(model_data["selling_price"] - float(purchase), 2))
