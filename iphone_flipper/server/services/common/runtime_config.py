from __future__ import annotations

import asyncio
import time
from typing import Any, Mapping

from server.services.common.feature_flags import parse_feature_flag_value

RUNTIME_CONFIG_HASH_KEY = "flipper:runtime_config"

DEFAULT_RUNTIME_CONFIG: dict[str, bool | float] = {
    "NOTIFICATION_CONSUMER_DRAIN": False,
    "ROUTE_LANE_HOT_SCORE_MIN": 5.5,
    "ROUTE_LANE_HOT_PROFITABLE_HIT_RATE_MIN": 0.35,
    "ROUTE_LANE_SWEEP_SCORE_MAX": 2.4,
    "ROUTE_LANE_SWEEP_PROFITABLE_HIT_RATE_MAX": 0.10,
    "ROUTE_LANE_SWEEP_AVG_RESULT_COUNT_MAX": 2.0,
    "CENTRAL_ROUTE_EXPLORATION_EVERY_N": 5.0,
    "V4_FAMILY_EXPLORATION_EVERY_N": 5.0,
}


def parse_runtime_config_value(raw_value: Any, default: bool | float) -> bool | float:
    if isinstance(default, bool):
        return parse_feature_flag_value(raw_value, default=default)

    if raw_value is None:
        return float(default)
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8", errors="ignore")
    try:
        return float(str(raw_value).strip())
    except (TypeError, ValueError):
        return float(default)


class RedisRuntimeConfig:
    def __init__(
        self,
        redis_client: Any | None,
        *,
        hash_key: str = RUNTIME_CONFIG_HASH_KEY,
        cache_ttl_seconds: float = 1.0,
        defaults: dict[str, bool | float] | None = None,
    ) -> None:
        self._redis_client = redis_client
        self._hash_key = str(hash_key or RUNTIME_CONFIG_HASH_KEY)
        self._cache_ttl_seconds = max(0.0, float(cache_ttl_seconds or 0.0))
        self._defaults = dict(DEFAULT_RUNTIME_CONFIG if defaults is None else defaults)
        self._lock = asyncio.Lock()
        self._cache: dict[str, bool | float] = dict(self._defaults)
        self._cache_expires_at = 0.0

    async def snapshot(self) -> dict[str, bool | float]:
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
                key: parse_runtime_config_value(payload.get(key), default)
                for key, default in self._defaults.items()
            }
            self._cache = snapshot
            self._cache_expires_at = time.monotonic() + self._cache_ttl_seconds
            return dict(snapshot)

    async def get_bool(self, key: str) -> bool:
        snapshot = await self.snapshot()
        return bool(snapshot.get(key, self._defaults.get(key, False)))

    async def get_float(self, key: str) -> float:
        snapshot = await self.snapshot()
        default = self._defaults.get(key, 0.0)
        try:
            return float(snapshot.get(key, default))
        except (TypeError, ValueError):
            return float(default)

    def invalidate(self) -> None:
        self._cache_expires_at = 0.0

    async def set_values(self, updates: Mapping[str, Any]) -> dict[str, bool | float]:
        if self._redis_client is None:
            raise RuntimeError("Redis client is not configured for runtime-config updates.")

        normalized_updates = {
            str(key): value
            for key, value in dict(updates or {}).items()
            if str(key) in self._defaults
        }
        if normalized_updates:
            to_set: dict[str, str] = {}
            to_delete: list[str] = []
            for key, value in normalized_updates.items():
                if value is None:
                    to_delete.append(key)
                    continue
                parsed = parse_runtime_config_value(value, self._defaults[key])
                if isinstance(self._defaults[key], bool):
                    to_set[key] = "1" if bool(parsed) else "0"
                else:
                    to_set[key] = str(float(parsed))

            if to_set:
                await self._redis_client.hset(self._hash_key, mapping=to_set)
            if to_delete:
                await self._redis_client.hdel(self._hash_key, *to_delete)

        self.invalidate()
        return await self.snapshot()
