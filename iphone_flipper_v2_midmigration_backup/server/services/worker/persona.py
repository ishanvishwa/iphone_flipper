from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict, dataclass
from typing import Any, Mapping


VIEWPORT_POOL: tuple[tuple[int, int], ...] = (
    (1366, 768),
    (1440, 900),
    (1536, 864),
    (1600, 900),
    (1680, 1050),
    (1920, 1080),
    (1280, 720),
)

UA_POOL: tuple[str, ...] = (
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/132.0.6834.84 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/132.0.6834.84 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/132.0.6834.84 Safari/537.36"
    ),
)

GEO_LOCALE_MAP: dict[str, tuple[tuple[str, str], ...]] = {
    "US": (
        ("America/New_York", "en-US"),
        ("America/Chicago", "en-US"),
        ("America/Denver", "en-US"),
        ("America/Los_Angeles", "en-US"),
    ),
    "GB": (("Europe/London", "en-GB"),),
    "AU": (
        ("Australia/Sydney", "en-AU"),
        ("Australia/Melbourne", "en-AU"),
        ("Australia/Perth", "en-AU"),
    ),
    "CA": (
        ("America/Toronto", "en-CA"),
        ("America/Vancouver", "en-CA"),
    ),
    "DE": (("Europe/Berlin", "de-DE"),),
    "FR": (("Europe/Paris", "fr-FR"),),
    "SG": (("Asia/Singapore", "en-SG"),),
    "JP": (("Asia/Tokyo", "ja-JP"),),
}

DEVICE_SCALE_POOL: tuple[float, ...] = (1.0, 1.25, 1.5, 2.0)


def _choice(rng: Any, options: tuple[Any, ...] | list[Any]) -> Any:
    return rng.choice(list(options))


@dataclass(frozen=True)
class BrowserPersona:
    viewport_width: int
    viewport_height: int
    screen_width: int
    screen_height: int
    device_scale_factor: float
    user_agent: str
    timezone_id: str
    locale: str
    color_scheme: str

    def to_context_options(self) -> dict[str, Any]:
        return {
            "viewport": {"width": int(self.viewport_width), "height": int(self.viewport_height)},
            "screen": {"width": int(self.screen_width), "height": int(self.screen_height)},
            "device_scale_factor": float(self.device_scale_factor),
            "user_agent": str(self.user_agent),
            "timezone_id": str(self.timezone_id),
            "locale": str(self.locale),
            "color_scheme": str(self.color_scheme),
        }

    def to_telemetry_dict(self) -> dict[str, Any]:
        return asdict(self)


def generate_persona(proxy_geo: Mapping[str, Any] | None, rng: Any = random) -> BrowserPersona:
    geo = dict(proxy_geo or {})
    country_code = str(geo.get("country_code") or "US").strip().upper() or "US"
    timezone_id = str(geo.get("timezone_id") or "").strip()
    locale = str(geo.get("locale") or "").strip()

    geo_options = GEO_LOCALE_MAP.get(country_code) or GEO_LOCALE_MAP["US"]
    if not timezone_id or not locale:
        picked_timezone, picked_locale = _choice(rng, geo_options)
        if not timezone_id:
            timezone_id = picked_timezone
        if not locale:
            locale = picked_locale

    base_width, base_height = _choice(rng, VIEWPORT_POOL)
    viewport_width = max(1024, int(base_width + rng.randint(-16, 16)))
    viewport_height = max(640, int(base_height + rng.randint(-16, 16)))
    screen_width = max(viewport_width, viewport_width + int(rng.randint(0, 24)))
    screen_height = max(viewport_height, viewport_height + int(rng.randint(40, 120)))
    device_scale_factor = float(_choice(rng, DEVICE_SCALE_POOL))
    color_scheme = str(_choice(rng, ("light", "light", "dark")))

    return BrowserPersona(
        viewport_width=viewport_width,
        viewport_height=viewport_height,
        screen_width=screen_width,
        screen_height=screen_height,
        device_scale_factor=device_scale_factor,
        user_agent=str(_choice(rng, UA_POOL)),
        timezone_id=timezone_id,
        locale=locale,
        color_scheme=color_scheme,
    )


def persona_hash(persona: BrowserPersona) -> str:
    payload = json.dumps(persona.to_telemetry_dict(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()
