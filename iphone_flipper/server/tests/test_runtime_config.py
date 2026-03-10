from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

from server.services.common.runtime_config import (
    DEFAULT_RUNTIME_CONFIG,
    RedisRuntimeConfig,
    parse_runtime_config_value,
)


class RuntimeConfigParsingTests(unittest.TestCase):
    def test_parse_runtime_config_value_handles_bool_and_float_defaults(self) -> None:
        self.assertTrue(parse_runtime_config_value("1", False))
        self.assertFalse(parse_runtime_config_value("off", True))
        self.assertEqual(parse_runtime_config_value("5.75", 2.0), 5.75)
        self.assertEqual(parse_runtime_config_value("bad", 2.0), 2.0)


class RedisRuntimeConfigTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_defaults_to_built_in_values(self) -> None:
        config = RedisRuntimeConfig(redis_client=None)

        self.assertEqual(await config.snapshot(), DEFAULT_RUNTIME_CONFIG)

    async def test_snapshot_uses_cache_within_ttl(self) -> None:
        redis_client = AsyncMock()
        redis_client.hgetall.return_value = {
            "NOTIFICATION_CONSUMER_DRAIN": "1",
            "ROUTE_LANE_HOT_SCORE_MIN": "6.5",
        }
        config = RedisRuntimeConfig(redis_client=redis_client, cache_ttl_seconds=60)

        first = await config.snapshot()
        second = await config.snapshot()

        self.assertTrue(first["NOTIFICATION_CONSUMER_DRAIN"])
        self.assertEqual(second["ROUTE_LANE_HOT_SCORE_MIN"], 6.5)
        self.assertEqual(redis_client.hgetall.await_count, 1)

    async def test_snapshot_falls_back_to_defaults_when_redis_errors(self) -> None:
        redis_client = AsyncMock()
        redis_client.hgetall.side_effect = RuntimeError("redis unavailable")
        config = RedisRuntimeConfig(redis_client=redis_client)

        self.assertEqual(await config.snapshot(), DEFAULT_RUNTIME_CONFIG)

    async def test_set_values_updates_and_resets_runtime_config_fields(self) -> None:
        redis_client = AsyncMock()
        redis_client.hgetall.return_value = {
            "NOTIFICATION_CONSUMER_DRAIN": "1",
            "ROUTE_LANE_HOT_SCORE_MIN": "6.75",
        }
        config = RedisRuntimeConfig(redis_client=redis_client)

        snapshot = await config.set_values(
            {
                "NOTIFICATION_CONSUMER_DRAIN": True,
                "ROUTE_LANE_HOT_SCORE_MIN": 6.75,
                "ROUTE_LANE_SWEEP_SCORE_MAX": None,
            }
        )

        redis_client.hset.assert_awaited_once()
        redis_client.hdel.assert_awaited_once()
        self.assertTrue(snapshot["NOTIFICATION_CONSUMER_DRAIN"])
        self.assertEqual(snapshot["ROUTE_LANE_HOT_SCORE_MIN"], 6.75)
        self.assertEqual(snapshot["ROUTE_LANE_SWEEP_SCORE_MAX"], DEFAULT_RUNTIME_CONFIG["ROUTE_LANE_SWEEP_SCORE_MAX"])


if __name__ == "__main__":
    unittest.main()
