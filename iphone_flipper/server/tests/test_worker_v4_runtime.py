from __future__ import annotations

import asyncio
import contextlib
import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("IPHONE_FLIPPER_DB_PATH", "/tmp/iphone_flipper_worker_v4_test.db")

try:
    from server.services.worker import worker
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    worker = None

from server.services.common.v42_family_catalog import V42_FAMILY_PRESETS


@unittest.skipIf(worker is None, "Worker dependencies are not installed.")
class WorkerV4RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        worker._worker_shutdown_event().clear()
        worker._V4_FAMILY_DISPATCH_COUNT = 0

    async def asyncTearDown(self) -> None:
        with contextlib.suppress(Exception):
            worker._worker_shutdown_event().clear()
        worker._V4_FAMILY_DISPATCH_COUNT = 0

    def _warm_session(self) -> worker.V4WarmSessionState:
        return worker.V4WarmSessionState(
            profile={
                "profile_id": 3,
                "user_data_dir": "/profiles/3",
                "dolphin_profile_id": "746386753",
                "dolphin_profile_name": "Profile 3",
            },
            profile_lease_token="lease-3",
            runtime_identity="/profiles/3",
            session=object(),
            started_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
            last_activity_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
        )

    @staticmethod
    def _query_diagnostics(
        *,
        final_url: str,
        feed_present: bool = True,
        empty_state_detected: bool = False,
    ) -> list[dict[str, object]]:
        return [
            {
                "final_url": final_url,
                "feed_present": feed_present,
                "empty_state_detected": empty_state_detected,
            }
        ]

    async def _emit_attempt(
        self,
        progress_callback,
        *,
        scraped: int,
        saved: int = 0,
        final_url: str = "https://www.facebook.com/marketplace/perth/search?query=iPhone",
        feed_present: bool = True,
        empty_state_detected: bool = False,
    ) -> SimpleNamespace:
        progress_callback({"event": "query_start"})
        progress_callback(
            {
                "event": "query_result",
                "found": scraped,
                "page_cards": scraped,
                "final_url": final_url,
                "feed_present": feed_present,
                "empty_state_detected": empty_state_detected,
            }
        )
        for index in range(saved):
            progress_callback(
                {
                    "event": "listing_saved",
                    "listing": {"id": f"listing-{index}", "estimated_profit_aud": 250},
                    "query": "iPhone",
                    "query_index": 1,
                    "query_total": 1,
                    "discovery_ts": "2026-03-11T00:00:00+00:00",
                }
            )
        return SimpleNamespace(
            query_diagnostics=self._query_diagnostics(
                final_url=final_url,
                feed_present=feed_present,
                empty_state_detected=empty_state_detected,
            )
        )

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

    async def test_open_v4_warm_session_uses_claimed_dolphin_profile_when_available(self) -> None:
        profile = {
            "profile_id": 11,
            "profile_lease_token": "lease-11",
            "user_data_dir": "/profiles/11",
            "dolphin_profile_id": "746386753",
            "dolphin_profile_name": "Profile 11",
        }

        with (
            patch.object(worker, "scrub_orphaned_chromium_locks", return_value=[]) as scrub_locks,
            patch.object(worker, "open_profile_session", AsyncMock(return_value=object())) as open_session,
        ):
            warm_session = await worker._open_v4_warm_session(pool=object(), profile=profile)

        scrub_locks.assert_not_called()
        open_session.assert_awaited_once_with(
            profile_id="746386753",
            user_data_dir="/profiles/11",
            headless=worker.WORKER_HEADLESS,
        )
        self.assertEqual(warm_session.runtime_identity, "746386753")

    async def test_open_v4_warm_session_falls_back_to_claimed_profile_user_data_dir(self) -> None:
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
            worker._v4_worker_family_allowlist(),
        )
        self.assertFalse(claim_family.await_args.kwargs["exploration"])

    async def test_claim_v4_family_passes_exploration_flag(self) -> None:
        with patch.object(worker, "claim_next_due_family_from_module", AsyncMock(return_value=None)) as claim_family:
            await worker._claim_v4_family(pool=object(), exploration=True)

        self.assertTrue(claim_family.await_args.kwargs["exploration"])

    def test_v4_family_exploration_every_n_defaults_to_five(self) -> None:
        self.assertEqual(worker._v4_family_exploration_every_n(None), 5)

    def test_v4_family_exploration_cadence_uses_every_n_for_model_workers(self) -> None:
        self.assertFalse(
            worker._should_use_v4_family_exploration(
                {"V4_FAMILY_EXPLORATION_EVERY_N": 5},
                dispatch_count=3,
                family_names=("iphone_16_pro", "iphone_14_pro"),
            )
        )
        self.assertTrue(
            worker._should_use_v4_family_exploration(
                {"V4_FAMILY_EXPLORATION_EVERY_N": 5},
                dispatch_count=4,
                family_names=("iphone_16_pro", "iphone_14_pro"),
            )
        )

    def test_v4_family_exploration_is_disabled_for_broad_only_workers(self) -> None:
        self.assertFalse(
            worker._should_use_v4_family_exploration(
                {"V4_FAMILY_EXPLORATION_EVERY_N": 5},
                dispatch_count=4,
                family_names=("iphone_broad",),
            )
        )

    def test_default_v4_family_allowlist_matches_v42_catalog(self) -> None:
        self.assertEqual(
            worker.V4_ROLLOUT_FAMILY_ALLOWLIST,
            tuple(preset.name for preset in V42_FAMILY_PRESETS),
        )

    def test_worker_specific_v4_family_allowlist_partitions_broad_and_model_families(self) -> None:
        family_names = ("iphone_broad", "iphone_15_pro", "iphone_14_pro")
        with (
            patch.object(worker, "V4_ROLLOUT_FAMILY_ALLOWLIST", family_names),
            patch.object(worker, "V4_BROAD_FAMILY_ALLOWLIST", ("iphone_broad",)),
            patch.object(worker, "V4_BROAD_WORKER_ALLOWLIST", ("worker_3",)),
        ):
            with patch.object(worker, "WORKER_NAME", "worker_3"):
                self.assertEqual(worker._v4_worker_family_allowlist(), ("iphone_broad",))
            with patch.object(worker, "WORKER_NAME", "worker"):
                self.assertEqual(
                    worker._v4_worker_family_allowlist(),
                    ("iphone_15_pro", "iphone_14_pro"),
                )

    async def test_apply_v4_rollout_family_overrides_syncs_presets_before_overrides(self) -> None:
        conn = AsyncMock()

        class _PoolAcquire:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        pool = SimpleNamespace(acquire=lambda: _PoolAcquire())

        with (
            patch.object(worker, "V4_ROLLOUT_FAMILY_ALLOWLIST", ("iphone_broad", "iphone_15_pro")),
            patch.object(worker, "sync_v42_family_presets", AsyncMock(return_value=[{"name": "iphone_broad"}])) as sync_presets,
        ):
            await worker._apply_v4_rollout_family_overrides(pool)

        sync_presets.assert_awaited_once_with(
            conn,
            family_names=("iphone_broad", "iphone_15_pro"),
        )
        self.assertGreaterEqual(conn.execute.await_count, 3)

    def test_profile_display_label_prefers_dolphin_profile_name(self) -> None:
        self.assertEqual(
            worker._profile_display_label(
                {
                    "dolphin_profile_name": "Profile 8",
                    "user_data_dir": "/app/runtime/browser_profile_8",
                }
            ),
            "Profile 8",
        )

    async def test_interruptible_worker_sleep_wakes_when_v4_canary_turns_on(self) -> None:
        feature_flags = AsyncMock()
        feature_flags.is_enabled = AsyncMock(side_effect=[False, True])

        with (
            patch.object(worker, "_worker_v4_rollout_enabled", return_value=True),
            patch.object(worker.asyncio, "sleep", AsyncMock()) as sleep_mock,
        ):
            result = await worker._interruptible_worker_sleep(
                30.0,
                feature_flags=feature_flags,
                max_chunk_seconds=5.0,
                wake_on_v4_enable=True,
            )

        self.assertEqual(result, "v4_enabled")
        sleep_mock.assert_awaited_once_with(5.0)

    async def test_interruptible_worker_sleep_ignores_v4_enable_for_non_canary_worker(self) -> None:
        feature_flags = AsyncMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)

        with (
            patch.object(worker, "_worker_v4_rollout_enabled", return_value=False),
            patch.object(worker.asyncio, "sleep", AsyncMock()) as sleep_mock,
        ):
            result = await worker._interruptible_worker_sleep(
                10.0,
                feature_flags=feature_flags,
                max_chunk_seconds=5.0,
                wake_on_v4_enable=True,
            )

        self.assertIsNone(result)
        self.assertEqual(sleep_mock.await_count, 2)

    async def test_apply_v4_profile_claim_outcome_checkpoint_quarantines_execution_profile(self) -> None:
        claim_result = worker.V4FamilyClaimResult(
            metrics={},
            outcome=worker.FamilyClaimOutcome.CHECKPOINT,
            error_text="manual_login_required: checkpoint",
            error_category=worker.ErrorCategory.AUTH_REQUIRED,
            final_url="https://www.facebook.com/checkpoint/",
        )

        with (
            patch.object(worker, "_mark_v4_profile_manual_login_required", AsyncMock()) as mark_v4_profile,
            patch.object(worker, "_mark_execution_profile_manual_login_required", AsyncMock()) as mark_execution_profile,
            patch.object(worker, "_should_send_manual_login_alert", return_value=False),
        ):
            should_abort = await worker._apply_v4_profile_claim_outcome(
                pool=object(),
                profile={"profile_id": 3, "user_data_dir": "/profiles/3"},
                family={"family_id": 70, "name": "iphone_broad"},
                claim_result=claim_result,
            )

        self.assertTrue(should_abort)
        mark_v4_profile.assert_awaited_once()
        mark_execution_profile.assert_awaited_once()

    async def test_apply_v4_profile_claim_outcome_stale_feed_counts_as_profile_success(self) -> None:
        claim_result = worker.V4FamilyClaimResult(
            metrics={"listings_scraped": 72, "listings_saved": 0},
            outcome=worker.FamilyClaimOutcome.STALE_FEED,
            error_text=None,
            error_category=worker.ErrorCategory.NONE,
            final_url="https://www.facebook.com/marketplace/perth/search?query=iphone",
            feed_present=True,
            empty_state_detected=False,
        )

        with (
            patch.object(worker, "_record_v4_profile_success", AsyncMock()) as profile_success,
            patch.object(worker, "_record_v4_profile_empty_feed", AsyncMock()) as profile_empty,
        ):
            should_abort = await worker._apply_v4_profile_claim_outcome(
                pool=object(),
                profile={"profile_id": 3, "user_data_dir": "/profiles/3"},
                family={"family_id": 70, "name": "iphone_broad"},
                claim_result=claim_result,
            )

        self.assertFalse(should_abort)
        profile_success.assert_awaited_once()
        profile_empty.assert_not_awaited()

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

    async def test_run_v4_family_claim_reports_active_variant_query_in_heartbeat(self) -> None:
        warm_session = self._warm_session()

        with (
            patch.object(
                worker,
                "execute_family_claim",
                AsyncMock(return_value=SimpleNamespace(query_diagnostics=[])),
            ),
            patch.object(worker, "_upsert_worker_heartbeat", AsyncMock()) as heartbeat,
            patch.object(worker, "_cleanup_old_scrape_events", AsyncMock()),
        ):
            await worker._run_v4_family_claim(
                pool=object(),
                redis_client=object(),
                feature_flags=None,
                warm_session=warm_session,
                family={"family_id": 70, "name": "iphone_broad"},
                variant={"variant_id": 440, "query_text": "iPhone"},
            )

        first_route = heartbeat.await_args_list[0].kwargs["route"]
        self.assertEqual(first_route["source"], "v4")
        self.assertEqual(first_route["search_queries"], "iPhone")
        self.assertEqual(first_route["dolphin_profile_name"], "Profile 3")
        self.assertFalse(first_route["exploration_dispatch"])

    async def test_run_v4_family_claim_includes_exploration_flag_in_heartbeat_route(self) -> None:
        warm_session = self._warm_session()

        with (
            patch.object(
                worker,
                "execute_family_claim",
                AsyncMock(return_value=SimpleNamespace(query_diagnostics=[])),
            ),
            patch.object(worker, "_upsert_worker_heartbeat", AsyncMock()) as heartbeat,
            patch.object(worker, "_cleanup_old_scrape_events", AsyncMock()),
        ):
            await worker._run_v4_family_claim(
                pool=object(),
                redis_client=object(),
                feature_flags=None,
                warm_session=warm_session,
                family={"family_id": 70, "name": "iphone_14_pro", "exploration_dispatch": True},
                variant={"variant_id": 440, "query_text": "iPhone 14 Pro"},
            )

        first_route = heartbeat.await_args_list[0].kwargs["route"]
        self.assertTrue(first_route["exploration_dispatch"])

    async def test_run_v4_family_claim_does_not_cache_bust_before_threshold(self) -> None:
        async def execute_once(*args, **kwargs):
            return await self._emit_attempt(kwargs["progress_callback"], scraped=72)

        with (
            patch.object(worker, "execute_family_claim", AsyncMock(side_effect=execute_once)) as execute_claim,
            patch.object(worker, "_record_scrape_events", AsyncMock()),
            patch.object(worker, "_upsert_worker_heartbeat", AsyncMock()),
            patch.object(worker, "_cleanup_old_scrape_events", AsyncMock()),
        ):
            claim_result = await worker._run_v4_family_claim(
                pool=object(),
                redis_client=object(),
                feature_flags=None,
                warm_session=self._warm_session(),
                family={"family_id": 70, "name": "iphone_broad", "consecutive_stale": 1},
                variant={"variant_id": 440, "query_text": "iPhone"},
            )

        self.assertEqual(execute_claim.await_count, 1)
        self.assertEqual(claim_result.outcome, worker.FamilyClaimOutcome.STALE_FEED)
        self.assertFalse(claim_result.cache_bust_attempted)

    async def test_run_v4_family_claim_cache_busts_on_third_stale_outcome(self) -> None:
        attempts = 0

        async def execute_threshold(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            self.assertEqual(kwargs["search_queries"], ["iPhone"])
            if attempts == 1:
                self.assertEqual(kwargs["search_urls"], [])
                return await self._emit_attempt(kwargs["progress_callback"], scraped=72)
            self.assertEqual(len(kwargs["search_urls"]), 1)
            self.assertIn("__cb=", kwargs["search_urls"][0])
            return await self._emit_attempt(kwargs["progress_callback"], scraped=72)

        with (
            patch.object(worker, "execute_family_claim", AsyncMock(side_effect=execute_threshold)) as execute_claim,
            patch.object(worker, "_record_scrape_events", AsyncMock()),
            patch.object(worker, "_upsert_worker_heartbeat", AsyncMock()),
            patch.object(worker, "_cleanup_old_scrape_events", AsyncMock()),
        ):
            claim_result = await worker._run_v4_family_claim(
                pool=object(),
                redis_client=object(),
                feature_flags=None,
                warm_session=self._warm_session(),
                family={"family_id": 70, "name": "iphone_broad", "consecutive_stale": 2},
                variant={"variant_id": 440, "query_text": "iPhone"},
            )

        self.assertEqual(execute_claim.await_count, 2)
        self.assertEqual(claim_result.outcome, worker.FamilyClaimOutcome.STALE_FEED)
        self.assertTrue(claim_result.cache_bust_attempted)

    async def test_run_v4_family_claim_skips_cache_bust_when_scraped_count_is_too_low(self) -> None:
        async def execute_once(*args, **kwargs):
            return await self._emit_attempt(kwargs["progress_callback"], scraped=12)

        with (
            patch.object(worker, "execute_family_claim", AsyncMock(side_effect=execute_once)) as execute_claim,
            patch.object(worker, "_record_scrape_events", AsyncMock()),
            patch.object(worker, "_upsert_worker_heartbeat", AsyncMock()),
            patch.object(worker, "_cleanup_old_scrape_events", AsyncMock()),
        ):
            claim_result = await worker._run_v4_family_claim(
                pool=object(),
                redis_client=object(),
                feature_flags=None,
                warm_session=self._warm_session(),
                family={"family_id": 70, "name": "iphone_broad", "consecutive_stale": 2},
                variant={"variant_id": 440, "query_text": "iPhone"},
            )

        self.assertEqual(execute_claim.await_count, 1)
        self.assertEqual(claim_result.outcome, worker.FamilyClaimOutcome.STALE_FEED)
        self.assertFalse(claim_result.cache_bust_attempted)

    async def test_run_v4_family_claim_skips_cache_bust_during_cooldown(self) -> None:
        async def execute_once(*args, **kwargs):
            return await self._emit_attempt(kwargs["progress_callback"], scraped=72)

        family = {
            "family_id": 70,
            "name": "iphone_broad",
            "consecutive_stale": 2,
            "last_cache_bust_at": datetime.now(timezone.utc) - timedelta(minutes=5),
        }

        with (
            patch.object(worker, "execute_family_claim", AsyncMock(side_effect=execute_once)) as execute_claim,
            patch.object(worker, "_record_scrape_events", AsyncMock()),
            patch.object(worker, "_upsert_worker_heartbeat", AsyncMock()),
            patch.object(worker, "_cleanup_old_scrape_events", AsyncMock()),
        ):
            claim_result = await worker._run_v4_family_claim(
                pool=object(),
                redis_client=object(),
                feature_flags=None,
                warm_session=self._warm_session(),
                family=family,
                variant={"variant_id": 440, "query_text": "iPhone"},
            )

        self.assertEqual(execute_claim.await_count, 1)
        self.assertEqual(claim_result.outcome, worker.FamilyClaimOutcome.STALE_FEED)
        self.assertFalse(claim_result.cache_bust_attempted)

    async def test_run_v4_family_claim_cache_bust_retry_can_convert_stale_to_matches(self) -> None:
        attempts = 0

        async def execute_with_retry_match(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return await self._emit_attempt(kwargs["progress_callback"], scraped=72)
            self.assertIn("__cb=", kwargs["search_urls"][0])
            return await self._emit_attempt(kwargs["progress_callback"], scraped=72, saved=1)

        with (
            patch.object(worker, "execute_family_claim", AsyncMock(side_effect=execute_with_retry_match)) as execute_claim,
            patch.object(worker, "_record_scrape_events", AsyncMock()),
            patch.object(worker, "_process_listing_event", AsyncMock()),
            patch.object(worker, "_upsert_worker_heartbeat", AsyncMock()),
            patch.object(worker, "_cleanup_old_scrape_events", AsyncMock()),
        ):
            claim_result = await worker._run_v4_family_claim(
                pool=object(),
                redis_client=object(),
                feature_flags=None,
                warm_session=self._warm_session(),
                family={"family_id": 70, "name": "iphone_broad", "consecutive_stale": 2},
                variant={"variant_id": 440, "query_text": "iPhone"},
            )

        self.assertEqual(execute_claim.await_count, 2)
        self.assertEqual(claim_result.outcome, worker.FamilyClaimOutcome.MATCHES)
        self.assertTrue(claim_result.cache_bust_attempted)
        self.assertEqual(claim_result.metrics["listings_saved"], 1)

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
