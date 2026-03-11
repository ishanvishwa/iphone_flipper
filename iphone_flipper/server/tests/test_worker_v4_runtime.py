from __future__ import annotations

import asyncio
import contextlib
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

os.environ.setdefault("IPHONE_FLIPPER_DB_PATH", "/tmp/iphone_flipper_worker_v4_test.db")

try:
    from server.services.worker import worker
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    worker = None


@unittest.skipIf(worker is None, "Worker dependencies are not installed.")
class WorkerV4RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        worker._worker_shutdown_event().clear()

    async def asyncTearDown(self) -> None:
        with contextlib.suppress(Exception):
            worker._worker_shutdown_event().clear()

    async def test_close_v4_warm_session_releases_leases_even_if_browser_close_fails(self) -> None:
        warm_session = worker.V4WarmSessionState(
            profile={"profile_id": 7, "profile_lease_token": "lease-7", "user_data_dir": "/profiles/7"},
            profile_lease_token="lease-7",
            runtime_identity="/profiles/7",
            session=object(),
            started_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
            last_activity_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
        )

        with (
            patch.object(worker, "close_profile_session", AsyncMock(side_effect=RuntimeError("close failed"))),
            patch.object(worker, "_release_v4_profile", AsyncMock()) as release_profile,
        ):
            await worker._close_v4_warm_session(
                pool=object(),
                warm_session=warm_session,
                available_after_seconds=worker.PROFILE_MIN_REUSE_SECONDS,
            )

        release_profile.assert_awaited_once()

    async def test_run_v4_worker_loop_idle_closes_profile_with_available_after(self) -> None:
        feature_flags = AsyncMock()
        feature_flags.is_enabled = AsyncMock(side_effect=[True, False])
        warm_session = worker.V4WarmSessionState(
            profile={"profile_id": 3, "profile_lease_token": "lease-3", "user_data_dir": "/profiles/3"},
            profile_lease_token="lease-3",
            runtime_identity="/profiles/3",
            session=object(),
            started_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
            last_activity_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
        )
        heartbeat_task = asyncio.create_task(asyncio.sleep(0))

        with (
            patch.object(worker, "_apply_v4_rollout_family_overrides", AsyncMock()),
            patch.object(worker, "_claim_v4_profile", AsyncMock(return_value=warm_session.profile)),
            patch.object(worker, "_open_v4_warm_session", AsyncMock(return_value=warm_session)),
            patch.object(worker, "_start_v4_lease_heartbeat_task", AsyncMock(return_value=heartbeat_task)),
            patch.object(worker, "evaluate_warm_session_state", side_effect=["idle_close"]),
            patch.object(worker, "_close_v4_warm_session", AsyncMock()) as close_session,
        ):
            await worker._run_v4_worker_loop(
                pool=object(),
                redis_client=object(),
                feature_flags=feature_flags,
                runtime_config=None,
            )

        close_session.assert_awaited_once()
        self.assertEqual(
            close_session.await_args.kwargs["available_after_seconds"],
            worker.PROFILE_MIN_REUSE_SECONDS,
        )

    async def test_open_v4_warm_session_uses_claimed_profile_user_data_dir(self) -> None:
        profile = {"profile_id": 11, "profile_lease_token": "lease-11", "user_data_dir": "/profiles/11"}

        with (
            patch.object(worker, "scrub_orphaned_chromium_locks", return_value=[]),
            patch.object(worker, "open_profile_session", AsyncMock(return_value=object())) as open_session,
        ):
            warm_session = await worker._open_v4_warm_session(pool=object(), profile=profile)

        open_session.assert_awaited_once_with(user_data_dir="/profiles/11", headless=worker.WORKER_HEADLESS)
        self.assertEqual(warm_session.runtime_identity, "/profiles/11")

    async def test_claim_v4_family_passes_rollout_allowlist(self) -> None:
        with patch.object(worker, "claim_next_due_family_from_module", AsyncMock(return_value=None)) as claim_family:
            await worker._claim_v4_family(pool=object())

        self.assertEqual(
            claim_family.await_args.kwargs["family_names"],
            worker.V4_ROLLOUT_FAMILY_ALLOWLIST or None,
        )

    def test_v4_variant_urls_expand_query_placeholder(self) -> None:
        urls = worker._v4_variant_urls(
            {
                "query_text": "iPhone 15 Pro",
                "url_template": "https://example.com/search?query={query}&sort=creation_time_descend",
            }
        )

        self.assertEqual(
            urls,
            ["https://example.com/search?query=iPhone%2015%20Pro&sort=creation_time_descend"],
        )

    async def test_dom_circuit_breaker_requires_distinct_profiles(self) -> None:
        class _FakeRedis:
            def __init__(self) -> None:
                self.profile_sets: dict[str, set[str]] = {}
                self.pause_ttls: dict[str, int] = {}

            async def sadd(self, key: str, value: str) -> int:
                bucket = self.profile_sets.setdefault(key, set())
                before = len(bucket)
                bucket.add(value)
                return 1 if len(bucket) > before else 0

            async def expire(self, key: str, seconds: int) -> bool:
                self.pause_ttls.setdefault(key, seconds)
                return True

            async def scard(self, key: str) -> int:
                return len(self.profile_sets.get(key, set()))

            async def set(self, key: str, value: str, ex: int) -> bool:
                _ = value
                self.pause_ttls[key] = ex
                return True

            async def ttl(self, key: str) -> int:
                return self.pause_ttls.get(key, 0)

        redis_client = _FakeRedis()
        family = {"name": "iphone_broad"}

        with patch.object(worker, "_should_send_v4_dom_circuit_alert", return_value=False):
            first = await worker._record_v4_dom_changed_signal(
                redis_client,
                family=family,
                profile={"profile_id": 1},
            )
            second_same_profile = await worker._record_v4_dom_changed_signal(
                redis_client,
                family=family,
                profile={"profile_id": 1},
            )
            third_distinct_profile = await worker._record_v4_dom_changed_signal(
                redis_client,
                family=family,
                profile={"profile_id": 2},
            )

        self.assertEqual(first, 0)
        self.assertEqual(second_same_profile, 0)
        self.assertEqual(third_distinct_profile, 0)

        with patch.object(worker, "_should_send_v4_dom_circuit_alert", return_value=False):
            opened = await worker._record_v4_dom_changed_signal(
                redis_client,
                family=family,
                profile={"profile_id": 3},
            )

        self.assertEqual(opened, worker.V4_DOM_GLOBAL_PAUSE_SECONDS)


if __name__ == "__main__":
    unittest.main()
