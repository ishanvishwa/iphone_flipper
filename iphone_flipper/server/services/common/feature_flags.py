from __future__ import annotations

import asyncio
import time
from typing import Any

FLAG_HASH_KEY = "flipper:flags"

DEFAULT_FEATURE_FLAGS: dict[str, bool] = {
    "ENABLE_REDIS_STREAM_EVENTS": False,
    "ENABLE_NOTIFICATION_CONSUMER": False,
    "ENABLE_GUI_WEBSOCKET_PUSH": False,
    "ENABLE_PRIORITY_SCHEDULER": False,
    "ENABLE_ROUTE_LANES": False,
    "ENABLE_BACKGROUND_ENRICHMENT": False,
}

_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


def parse_feature_flag_value(raw_value: Any, default: bool = False) -> bool:
    if raw_value is None:
        return bool(default)
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, (int, float)):
        return bool(raw_value)
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8", errors="ignore")
    value = str(raw_value).strip().lower()
    if not value:
        return bool(default)
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return bool(default)


class RedisFeatureFlags:
    def __init__(
        self,
        redis_client: Any | None,
        *,
        hash_key: str = FLAG_HASH_KEY,
        cache_ttl_seconds: float = 1.0,
        defaults: dict[str, bool] | None = None,
    ) -> None:
        self._redis_client = redis_client
        self._hash_key = str(hash_key or FLAG_HASH_KEY)
        self._cache_ttl_seconds = max(0.0, float(cache_ttl_seconds or 0.0))
        self._defaults = dict(DEFAULT_FEATURE_FLAGS if defaults is None else defaults)
        self._lock = asyncio.Lock()
        self._cache: dict[str, bool] = dict(self._defaults)
        self._cache_expires_at = 0.0

    async def snapshot(self) -> dict[str, bool]:
        now = time.monotonic()
        if now < self._cache_expires_at:
            return dict(self._cache)

        async with self._lock:
            now = time.monotonic()
            if now < self._cache_expires_at:
                return dict(self._cache)

            payload: dict[str, Any] = {}
            if self._redis_client is not None:
                try:
                    payload = await self._redis_client.hgetall(self._hash_key)
                except Exception:
                    payload = {}

            snapshot = {
                key: parse_feature_flag_value(payload.get(key), default=default)
                for key, default in self._defaults.items()
            }
            self._cache = snapshot
            self._cache_expires_at = time.monotonic() + self._cache_ttl_seconds
            return dict(snapshot)

    async def is_enabled(self, flag_name: str) -> bool:
        snapshot = await self.snapshot()
        default = self._defaults.get(flag_name, False)
        return bool(snapshot.get(flag_name, default))
