import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from .config import ACCESSORY_ONLY_PHRASES
from .config import ACCESSORY_SIGNAL_KEYWORDS
from .config import DEFAULT_ACCESSORY_MAX_PRICE
from .config import DEVICE_SALE_SIGNALS
from .storage import load_accessory_filter_settings


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
