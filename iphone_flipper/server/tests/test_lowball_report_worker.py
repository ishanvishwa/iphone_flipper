from __future__ import annotations

import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

sys.modules.setdefault("asyncpg", types.SimpleNamespace(Connection=object, Pool=object))

from server.services.common.lowball_report import LowballCandidate, LowballReportConfig
from server.services.worker import lowball_report_worker


class _FakeAcquire:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _AlreadySentConn:
    async def fetchval(self, query: str, *args):
        _ = query, args
        return 1


class _AlreadySentPool:
    def __init__(self) -> None:
        self.conn = _AlreadySentConn()

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


class _RetryConn:
    def __init__(self, *, completed_at: datetime | None) -> None:
        self.completed_at = completed_at

    async def fetchval(self, query: str, *args):
        _ = query, args
        return None

    async def fetchrow(self, query: str, *args):
        _ = query, args
        if self.completed_at is None:
            return None
        return {"completed_at": self.completed_at}


class _RetryPool:
    def __init__(self, *, completed_at: datetime | None) -> None:
        self.conn = _RetryConn(completed_at=completed_at)

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


def _candidate(listing_id: str, *, verification_required: bool = False) -> LowballCandidate:
    return LowballCandidate(
        listing_id=listing_id,
        title=f"Listing {listing_id}",
        url=f"https://example.com/{listing_id}",
        model="iPhone 14 Pro 128GB",
        condition="Good",
        current_price=430,
        target_price=380,
        resale_price=460,
        zone="SWEET",
        reason_tag="NEW",
        days_since_first_seen=10,
        days_since_last_price_change=2,
        last_drop_days=2,
        price_drop_count=1,
        seller_discount=0.12,
        reachability=0.03,
        required_discount=0.11,
        acceptable_discount=0.14,
        max_acceptable_price=440,
        walkaway_price=420,
        staleness_raw=10.0,
        trajectory_raw=12.0,
        verification_required=verification_required,
    )


class LowballReportWorkerTests(unittest.IsolatedAsyncioTestCase):
    def test_next_scheduled_run_at_handles_perth_cutoff(self) -> None:
        before_cutoff = datetime(2026, 4, 18, 21, 30, tzinfo=timezone.utc)
        within_grace = datetime(2026, 4, 18, 22, 3, tzinfo=timezone.utc)
        after_grace = datetime(2026, 4, 18, 22, 30, tzinfo=timezone.utc)

        today_target = lowball_report_worker.next_scheduled_run_at(before_cutoff, report_sent_today=False)
        immediate_target = lowball_report_worker.next_scheduled_run_at(within_grace, report_sent_today=False)
        tomorrow_target = lowball_report_worker.next_scheduled_run_at(after_grace, report_sent_today=False)
        sent_tomorrow_target = lowball_report_worker.next_scheduled_run_at(after_grace, report_sent_today=True)

        self.assertEqual(today_target, datetime(2026, 4, 18, 22, 0, tzinfo=timezone.utc))
        self.assertEqual(immediate_target, within_grace)
        self.assertEqual(tomorrow_target, datetime(2026, 4, 19, 22, 0, tzinfo=timezone.utc))
        self.assertEqual(sent_tomorrow_target, datetime(2026, 4, 19, 22, 0, tzinfo=timezone.utc))

    async def test_seconds_until_next_scheduled_run_retries_failed_delivery_same_day(self) -> None:
        pool = _RetryPool(completed_at=datetime(2026, 4, 19, 22, 4, tzinfo=timezone.utc))

        wait_seconds = await lowball_report_worker.seconds_until_next_scheduled_run(
            pool,
            now=datetime(2026, 4, 19, 22, 10, tzinfo=timezone.utc),
        )

        self.assertEqual(wait_seconds, 540)

    async def test_seconds_until_next_scheduled_run_uses_immediate_retry_when_due(self) -> None:
        pool = _RetryPool(completed_at=datetime(2026, 4, 19, 22, 4, tzinfo=timezone.utc))

        wait_seconds = await lowball_report_worker.seconds_until_next_scheduled_run(
            pool,
            now=datetime(2026, 4, 19, 22, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(wait_seconds, 0)

    async def test_select_report_entries_backfills_after_removed_listing(self) -> None:
        candidates = [
            _candidate("removed-first", verification_required=True),
            _candidate("confirmed-second"),
            _candidate("confirmed-third"),
        ]

        async def _verify(candidate: LowballCandidate):
            if candidate.listing_id == "removed-first":
                return lowball_report_worker.VerificationResult(
                    availability_status="removed",
                    verification_status="confirmed",
                )
            return lowball_report_worker.VerificationResult(
                availability_status="active",
                verification_status="confirmed",
            )

        entries = await lowball_report_worker.select_report_entries(
            pool=object(),
            candidates=candidates,
            config=LowballReportConfig(report_size=2),
            verifier=_verify,
        )

        self.assertEqual([entry.listing_id for entry in entries], ["confirmed-second", "confirmed-third"])

    async def test_run_lowball_report_once_refuses_second_send_for_same_day(self) -> None:
        pool = _AlreadySentPool()

        with (
            patch.object(lowball_report_worker, "load_latest_report", AsyncMock(return_value={"listing_count": 4})) as latest_mock,
            patch.object(lowball_report_worker, "fetch_report_rows", AsyncMock()) as fetch_rows_mock,
        ):
            result = await lowball_report_worker.run_lowball_report_once(
                pool=pool,
                run_source="scheduled",
                send_telegram=True,
                dry_run=False,
                now=datetime(2026, 4, 19, 0, 10, tzinfo=timezone.utc),
            )

        latest_mock.assert_awaited_once()
        fetch_rows_mock.assert_not_awaited()
        self.assertEqual(result["status"], "already_sent")
        self.assertTrue(result["already_sent"])


if __name__ == "__main__":
    unittest.main()
