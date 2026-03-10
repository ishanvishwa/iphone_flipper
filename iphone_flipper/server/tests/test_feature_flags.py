from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

from server.services.common.feature_flags import DEFAULT_FEATURE_FLAGS, RedisFeatureFlags, parse_feature_flag_value


class FeatureFlagParsingTests(unittest.TestCase):
    def test_parse_feature_flag_value_handles_truthy_and_falsey_inputs(self) -> None:
        self.assertTrue(parse_feature_flag_value("1"))
        self.assertTrue(parse_feature_flag_value("true"))
        self.assertTrue(parse_feature_flag_value(b"yes"))
        self.assertFalse(parse_feature_flag_value("0", default=True))
        self.assertFalse(parse_feature_flag_value("disabled", default=True))
        self.assertFalse(parse_feature_flag_value(None))


class RedisFeatureFlagsTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_defaults_all_flags_to_false(self) -> None:
        flags = RedisFeatureFlags(redis_client=None)
        self.assertEqual(await flags.snapshot(), DEFAULT_FEATURE_FLAGS)

    async def test_snapshot_uses_cached_values_within_ttl(self) -> None:
        redis_client = AsyncMock()
        redis_client.hgetall.return_value = {
            "ENABLE_PRIORITY_SCHEDULER": "1",
            "ENABLE_ROUTE_LANES": "true",
        }
        flags = RedisFeatureFlags(redis_client=redis_client, cache_ttl_seconds=60)

        first = await flags.snapshot()
        second = await flags.snapshot()

        self.assertTrue(first["ENABLE_PRIORITY_SCHEDULER"])
        self.assertTrue(second["ENABLE_ROUTE_LANES"])
        self.assertEqual(redis_client.hgetall.await_count, 1)

    async def test_snapshot_falls_back_to_defaults_when_redis_errors(self) -> None:
        redis_client = AsyncMock()
        redis_client.hgetall.side_effect = RuntimeError("redis unavailable")
        flags = RedisFeatureFlags(redis_client=redis_client)

        snapshot = await flags.snapshot()

        self.assertEqual(snapshot, DEFAULT_FEATURE_FLAGS)

    async def test_set_flag_values_updates_and_resets_fields(self) -> None:
        redis_client = AsyncMock()
        redis_client.hgetall.return_value = {
            "ENABLE_PRIORITY_SCHEDULER": "1",
            "ENABLE_ROUTE_LANES": "0",
        }
        flags = RedisFeatureFlags(redis_client=redis_client)

        snapshot = await flags.set_flag_values(
            {
                "ENABLE_PRIORITY_SCHEDULER": True,
                "ENABLE_ROUTE_LANES": None,
            }
        )

        redis_client.hset.assert_awaited_once()
        redis_client.hdel.assert_awaited_once()
        self.assertTrue(snapshot["ENABLE_PRIORITY_SCHEDULER"])
        self.assertFalse(snapshot["ENABLE_ROUTE_LANES"])
