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
            browser_profile_id="browser-7",
            browser_profile_lock_id="browser-lock-7",
            session=object(),
            started_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
            last_activity_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
        )

        with (
            patch.object(worker, "close_profile_session", AsyncMock(side_effect=RuntimeError("close failed"))),
            patch.object(worker, "_release_profile_lock", AsyncMock()) as release_browser_lock,
            patch.object(worker, "_release_v4_profile", AsyncMock()) as release_profile,
        ):
            await worker._close_v4_warm_session(
                pool=object(),
                warm_session=warm_session,
                available_after_seconds=worker.PROFILE_MIN_REUSE_SECONDS,
            )

        release_browser_lock.assert_awaited_once()
        release_profile.assert_awaited_once()

    async def test_run_v4_worker_loop_idle_closes_profile_with_available_after(self) -> None:
        feature_flags = AsyncMock()
        feature_flags.is_enabled = AsyncMock(side_effect=[True, False])
        warm_session = worker.V4WarmSessionState(
            profile={"profile_id": 3, "profile_lease_token": "lease-3", "user_data_dir": "/profiles/3"},
            profile_lease_token="lease-3",
            browser_profile_id="browser-3",
            browser_profile_lock_id="browser-lock-3",
            session=object(),
            started_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
            last_activity_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
        )
        heartbeat_task = asyncio.create_task(asyncio.sleep(0))

        with (
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


if __name__ == "__main__":
    unittest.main()
