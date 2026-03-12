from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import sys
import types
import unittest

sys.modules.setdefault("asyncpg", types.SimpleNamespace(Connection=object, Pool=object))

from server.services.worker import lease_manager


class _FakeAcquire:
    def __init__(self, conn: "_LeaseConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_LeaseConn":
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _LeaseConn:
    def __init__(
        self,
        *,
        profiles: list[dict[str, object]] | None = None,
        families: list[dict[str, object]] | None = None,
        variants: list[dict[str, object]] | None = None,
    ) -> None:
        self.now = datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc)
        self.profiles = {int(row["profile_id"]): dict(row) for row in (profiles or [])}
        self.families = {int(row["family_id"]): dict(row) for row in (families or [])}
        self.variants = [dict(row) for row in (variants or [])]
        self.queries: list[str] = []
        self.last_claim_family_args: tuple[object, ...] = ()

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(str(query).split())
        self.queries.append(normalized)
        await asyncio.sleep(0)
        if normalized.startswith("UPDATE profiles SET profile_lease_token = $2"):
            return self._claim_profile(*args)
        if normalized.startswith("UPDATE profiles SET profile_lease_expires_at = NOW() + ($3::INT * INTERVAL '1 second')"):
            return self._heartbeat_profile(*args)
        if normalized.startswith("UPDATE profiles SET profile_lease_token = NULL"):
            return self._release_profile(*args)
        if normalized.startswith("UPDATE query_families SET family_lease_token = $1"):
            self.last_claim_family_args = args
            return self._claim_family(*args[:2])
        if normalized.startswith("UPDATE query_families SET family_lease_expires_at = NOW() + ($3::INT * INTERVAL '1 second')"):
            return self._heartbeat_family(*args)
        if normalized.startswith("UPDATE query_families SET family_lease_token = NULL"):
            return self._release_family(*args)
        raise AssertionError(f"Unexpected query: {normalized}")

    def _claim_profile(self, worker_name: str, lease_token: str, lease_seconds: int):
        eligible = []
        for row in self.profiles.values():
            if not bool(row.get("is_enabled", True)):
                continue
            if str(row.get("status") or "READY") not in {"READY", "DEGRADED", "THROTTLED"}:
                continue
            if bool(row.get("manual_login_required", False)):
                continue
            cooldown_until = row.get("cooldown_until")
            if cooldown_until is not None and cooldown_until > self.now:
                continue
            available_after = row.get("available_after")
            if available_after is not None and available_after > self.now:
                continue
            lease_expires = row.get("profile_lease_expires_at")
            if lease_expires is not None and lease_expires > self.now:
                continue
            eligible.append(row)

        if not eligible:
            return None

        def _sort_key(row: dict[str, object]):
            last_started_at = row.get("last_started_at")
            status = str(row.get("status") or "READY")
            status_order = {"READY": 0, "DEGRADED": 1, "THROTTLED": 2}.get(status, 9)
            return (
                0 if str(row.get("worker_name") or "").strip() == str(worker_name) else 1,
                status_order,
                last_started_at is not None,
                last_started_at or datetime.min.replace(tzinfo=timezone.utc),
                int(row["profile_id"]),
            )

        chosen = min(eligible, key=_sort_key)
        chosen["profile_lease_token"] = str(lease_token)
        chosen["profile_lease_expires_at"] = self.now + timedelta(seconds=int(lease_seconds))
        chosen["last_started_at"] = self.now
        chosen["last_heartbeat_at"] = self.now
        return dict(chosen)

    def _heartbeat_profile(self, profile_id: int, lease_token: str, lease_seconds: int):
        row = self.profiles.get(int(profile_id))
        if row is None or row.get("profile_lease_token") != str(lease_token):
            return None
        row["profile_lease_expires_at"] = self.now + timedelta(seconds=int(lease_seconds))
        row["last_heartbeat_at"] = self.now
        return dict(row)

    def _release_profile(self, profile_id: int, lease_token: str, available_after_seconds: int | None):
        row = self.profiles.get(int(profile_id))
        if row is None or row.get("profile_lease_token") != str(lease_token):
            return None
        row["profile_lease_token"] = None
        row["profile_lease_expires_at"] = None
        row["last_heartbeat_at"] = self.now
        if available_after_seconds is not None and int(available_after_seconds) > 0:
            row["available_after"] = self.now + timedelta(seconds=int(available_after_seconds))
        return dict(row)

    def _claim_family(self, lease_token: str, lease_seconds: int):
        allowlisted_family_names: set[str] | None = None
        if len(self.last_claim_family_args) >= 3:
            allowlisted_family_names = {
                str(name).strip().lower()
                for name in (self.last_claim_family_args[2] or [])
                if str(name).strip()
            }
        eligible = []
        for row in self.families.values():
            if not bool(row.get("is_enabled", True)):
                continue
            if allowlisted_family_names and str(row.get("name") or "").strip().lower() not in allowlisted_family_names:
                continue
            if row.get("next_due_at") is not None and row["next_due_at"] > self.now:
                continue
            lease_expires = row.get("family_lease_expires_at")
            if lease_expires is not None and lease_expires > self.now:
                continue
            has_enabled_variant = any(
                int(variant.get("family_id") or 0) == int(row["family_id"])
                and bool(variant.get("is_enabled", True))
                and str(variant.get("validation_state") or "pending_validation").strip().lower() == "validated"
                for variant in self.variants
            )
            if not has_enabled_variant:
                continue
            eligible.append(row)

        if not eligible:
            return None

        def _sort_key(row: dict[str, object]):
            priority_score = row.get("priority_score")
            priority = int(row.get("priority") or 100)
            next_due_at = row.get("next_due_at") or self.now
            return (
                priority_score is None,
                -(float(priority_score) if priority_score is not None else 0.0),
                -priority,
                next_due_at,
                int(row["family_id"]),
            )

        chosen = min(eligible, key=_sort_key)
        chosen["family_lease_token"] = str(lease_token)
        chosen["family_lease_expires_at"] = self.now + timedelta(seconds=int(lease_seconds))
        chosen["last_claimed_at"] = self.now
        return dict(chosen)

    def _heartbeat_family(self, family_id: int, lease_token: str, lease_seconds: int):
        row = self.families.get(int(family_id))
        if row is None or row.get("family_lease_token") != str(lease_token):
            return None
        row["family_lease_expires_at"] = self.now + timedelta(seconds=int(lease_seconds))
        return dict(row)

    def _release_family(self, family_id: int, lease_token: str):
        row = self.families.get(int(family_id))
        if row is None or row.get("family_lease_token") != str(lease_token):
            return None
        row["family_lease_token"] = None
        row["family_lease_expires_at"] = None
        return dict(row)


class _LeasePool:
    def __init__(self, conn: _LeaseConn) -> None:
        self.conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


class V4LeaseManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_profile_claim_has_single_winner(self) -> None:
        conn = _LeaseConn(
            profiles=[
                {
                    "profile_id": 1,
                    "worker_name": "worker",
                    "is_enabled": True,
                    "status": "READY",
                    "manual_login_required": False,
                    "available_after": None,
                    "cooldown_until": None,
                    "profile_lease_expires_at": None,
                    "last_started_at": None,
                }
            ]
        )
        pool = _LeasePool(conn)

        async def _claim(token: str):
            return await lease_manager.claim_next_profile(
                pool,
                worker_name="worker",
                lease_token=token,
                lease_seconds=120,
            )

        results = await asyncio.gather(_claim("lease-a"), _claim("lease-b"))
        winners = [row for row in results if row is not None]

        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0]["profile_id"], 1)
        self.assertIn("FOR UPDATE SKIP LOCKED", conn.queries[0])
        self.assertIn("RETURNING *", conn.queries[0])

    async def test_profile_claim_skips_ineligible_rows(self) -> None:
        conn = _LeaseConn(
            profiles=[
                {
                    "profile_id": 1,
                    "worker_name": "worker",
                    "is_enabled": True,
                    "status": "READY",
                    "manual_login_required": True,
                    "available_after": None,
                    "cooldown_until": None,
                    "profile_lease_expires_at": None,
                    "last_started_at": None,
                },
                {
                    "profile_id": 2,
                    "worker_name": "worker",
                    "is_enabled": True,
                    "status": "READY",
                    "manual_login_required": False,
                    "available_after": None,
                    "cooldown_until": datetime(2026, 3, 11, 0, 1, tzinfo=timezone.utc),
                    "profile_lease_expires_at": None,
                    "last_started_at": None,
                },
                {
                    "profile_id": 3,
                    "worker_name": "worker",
                    "is_enabled": True,
                    "status": "DEGRADED",
                    "manual_login_required": False,
                    "available_after": None,
                    "cooldown_until": None,
                    "profile_lease_expires_at": None,
                    "last_started_at": None,
                },
            ]
        )
        pool = _LeasePool(conn)

        claimed = await lease_manager.claim_next_profile(
            pool,
            worker_name="worker",
            lease_token="lease-1",
            lease_seconds=120,
        )

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["profile_id"], 3)

    async def test_profile_claim_falls_back_to_other_workers_profile_when_own_is_ineligible(self) -> None:
        conn = _LeaseConn(
            profiles=[
                {
                    "profile_id": 1,
                    "worker_name": "worker",
                    "is_enabled": True,
                    "status": "COOLDOWN",
                    "manual_login_required": False,
                    "available_after": None,
                    "cooldown_until": datetime(2026, 3, 11, 0, 1, tzinfo=timezone.utc),
                    "profile_lease_expires_at": None,
                    "last_started_at": None,
                },
                {
                    "profile_id": 2,
                    "worker_name": "worker_3",
                    "is_enabled": True,
                    "status": "READY",
                    "manual_login_required": False,
                    "available_after": None,
                    "cooldown_until": None,
                    "profile_lease_expires_at": None,
                    "last_started_at": None,
                },
            ]
        )
        pool = _LeasePool(conn)

        claimed = await lease_manager.claim_next_profile(
            pool,
            worker_name="worker",
            lease_token="lease-1",
            lease_seconds=120,
        )

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["profile_id"], 2)
        self.assertEqual(claimed["worker_name"], "worker_3")

    async def test_profile_claim_prefers_owned_profile_before_shared_fallback(self) -> None:
        conn = _LeaseConn(
            profiles=[
                {
                    "profile_id": 1,
                    "worker_name": "worker",
                    "is_enabled": True,
                    "status": "READY",
                    "manual_login_required": False,
                    "available_after": None,
                    "cooldown_until": None,
                    "profile_lease_expires_at": None,
                    "last_started_at": datetime(2026, 3, 11, 0, 0, 30, tzinfo=timezone.utc),
                },
                {
                    "profile_id": 2,
                    "worker_name": "worker_3",
                    "is_enabled": True,
                    "status": "READY",
                    "manual_login_required": False,
                    "available_after": None,
                    "cooldown_until": None,
                    "profile_lease_expires_at": None,
                    "last_started_at": None,
                },
            ]
        )
        pool = _LeasePool(conn)

        claimed = await lease_manager.claim_next_profile(
            pool,
            worker_name="worker",
            lease_token="lease-1",
            lease_seconds=120,
        )

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["profile_id"], 1)

    async def test_profile_heartbeat_release_and_available_after_require_matching_token(self) -> None:
        conn = _LeaseConn(
            profiles=[
                {
                    "profile_id": 1,
                    "worker_name": "worker",
                    "is_enabled": True,
                    "status": "READY",
                    "manual_login_required": False,
                    "available_after": datetime(2026, 3, 11, 0, 1, tzinfo=timezone.utc),
                    "cooldown_until": None,
                    "profile_lease_expires_at": None,
                    "last_started_at": None,
                }
            ]
        )
        pool = _LeasePool(conn)

        not_yet = await lease_manager.claim_next_profile(
            pool,
            worker_name="worker",
            lease_token="lease-1",
            lease_seconds=120,
        )
        self.assertIsNone(not_yet)

        conn.now += timedelta(seconds=61)

        claimed = await lease_manager.claim_next_profile(
            pool,
            worker_name="worker",
            lease_token="lease-1",
            lease_seconds=120,
        )
        self.assertIsNotNone(claimed)

        wrong_heartbeat = await lease_manager.heartbeat_profile_lease(
            pool,
            profile_id=1,
            lease_token="wrong-token",
            lease_seconds=120,
        )
        self.assertIsNone(wrong_heartbeat)

        wrong_release = await lease_manager.release_profile_lease(
            pool,
            profile_id=1,
            lease_token="wrong-token",
            available_after_seconds=180,
        )
        self.assertIsNone(wrong_release)

        good_heartbeat = await lease_manager.heartbeat_profile_lease(
            pool,
            profile_id=1,
            lease_token="lease-1",
            lease_seconds=120,
        )
        self.assertIsNotNone(good_heartbeat)

        released = await lease_manager.release_profile_lease(
            pool,
            profile_id=1,
            lease_token="lease-1",
            available_after_seconds=180,
        )
        self.assertIsNotNone(released)
        self.assertIsNone(released["profile_lease_token"])
        self.assertGreater(released["available_after"], conn.now)

        blocked_reclaim = await lease_manager.claim_next_profile(
            pool,
            worker_name="worker",
            lease_token="lease-2",
            lease_seconds=120,
        )
        self.assertIsNone(blocked_reclaim)

        conn.now += timedelta(seconds=181)
        reclaimed = await lease_manager.claim_next_profile(
            pool,
            worker_name="worker",
            lease_token="lease-2",
            lease_seconds=120,
        )
        self.assertIsNotNone(reclaimed)

    async def test_concurrent_family_claim_requires_enabled_variant_and_matching_release_token(self) -> None:
        conn = _LeaseConn(
            families=[
                {
                    "family_id": 1,
                    "name": "iphone_broad",
                    "is_enabled": True,
                    "priority": 100,
                    "priority_score": 9.5,
                    "next_due_at": datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc),
                    "family_lease_expires_at": None,
                },
                {
                    "family_id": 2,
                    "name": "iphone_15_pro",
                    "is_enabled": True,
                    "priority": 50,
                    "priority_score": 4.0,
                    "next_due_at": datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc),
                    "family_lease_expires_at": None,
                },
            ],
            variants=[
                {"family_id": 1, "is_enabled": True, "validation_state": "validated"},
                {"family_id": 2, "is_enabled": True, "validation_state": "pending_validation"},
            ],
        )
        pool = _LeasePool(conn)

        async def _claim(token: str):
            return await lease_manager.claim_next_due_family(
                pool,
                lease_token=token,
                lease_seconds=120,
                family_names=["iphone_broad"],
            )

        results = await asyncio.gather(_claim("family-a"), _claim("family-b"))
        winners = [row for row in results if row is not None]

        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0]["family_id"], 1)

        wrong_heartbeat = await lease_manager.heartbeat_family_lease(
            pool,
            family_id=1,
            lease_token="wrong-token",
            lease_seconds=120,
        )
        self.assertIsNone(wrong_heartbeat)

        wrong_release = await lease_manager.release_family_lease(
            pool,
            family_id=1,
            lease_token="wrong-token",
        )
        self.assertIsNone(wrong_release)

        good_heartbeat = await lease_manager.heartbeat_family_lease(
            pool,
            family_id=1,
            lease_token=winners[0]["family_lease_token"],
            lease_seconds=120,
        )
        self.assertIsNotNone(good_heartbeat)

        released = await lease_manager.release_family_lease(
            pool,
            family_id=1,
            lease_token=winners[0]["family_lease_token"],
        )
        self.assertIsNotNone(released)
        self.assertIsNone(released["family_lease_token"])
        self.assertIn("FOR UPDATE SKIP LOCKED", conn.queries[0])
        self.assertIn("RETURNING *", conn.queries[0])


if __name__ == "__main__":
    unittest.main()
