from __future__ import annotations

import unittest
from pathlib import Path
import sys
import types

sys.modules.setdefault("asyncpg", types.SimpleNamespace(Connection=object, Pool=object))

from server.services.common import schema_ensure


class _FakeAcquire:
    def __init__(self, conn: "_RecordingConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_RecordingConn":
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _RecordingConn:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def execute(self, query: str, *args) -> str:
        _ = args
        self.queries.append(" ".join(str(query).split()))
        return "OK"

    async def fetch(self, query: str, *args):
        _ = args
        self.queries.append(" ".join(str(query).split()))
        return []


class _RecordingPool:
    def __init__(self) -> None:
        self.conn = _RecordingConn()

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


class _CentralBackfillConn(_RecordingConn):
    async def fetch(self, query: str, *args):
        _ = args
        normalized = " ".join(str(query).split())
        self.queries.append(normalized)
        if "FROM central_routes" in normalized:
            return []
        if "FROM worker_routes" in normalized:
            return [
                {
                    "worker_name": "worker_3",
                    "route_name": "worker_3__env_default",
                    "is_enabled": True,
                    "proxy_server": "proxy.example:443",
                    "proxy_username": "user",
                    "proxy_password": "pass",
                    "proxy_mode": "fixed",
                    "proxy_pool": None,
                    "preferred_proxy_key": None,
                    "preferred_proxy_updated_at": None,
                    "user_data_dir": "/app/runtime/browser_profile_3",
                    "search_queries": "iPhone",
                    "priority": 100,
                    "status": "ENABLED",
                    "status_reason": None,
                    "status_since": None,
                    "next_run_at": None,
                    "route_interval_seconds": 5,
                    "avg_result_count": None,
                    "profitable_hit_rate": 0.0,
                    "recent_duplicate_ratio": 0.0,
                    "avg_page_load_ms": None,
                    "successful_cycles": 0,
                    "last_selected_at": None,
                    "last_success_at": None,
                    "consecutive_failures": 0,
                    "cooldown_until": None,
                    "lane_override": None,
                    "computed_lane": "hot",
                    "effective_lane": "hot",
                    "priority_score": 6.9,
                    "priority_score_updated_at": None,
                    "manual_login_required": True,
                    "manual_login_reason": "Manual login required. checkpoint",
                    "manual_login_required_at": None,
                    "quarantined_at": None,
                    "quarantine_reason": "checkpoint",
                    "quarantine_evidence": {"source": "test"},
                    "last_error": "MANUAL_LOGIN_REQUIRED: checkpoint",
                }
            ]
        if "FROM worker_heartbeats" in normalized:
            return []
        return []


class V4SchemaFoundationTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_worker_tables_adds_v4_schema_and_is_repeatable(self) -> None:
        pool = _RecordingPool()

        await schema_ensure.ensure_worker_tables(pool, include_triggers=True)
        await schema_ensure.ensure_worker_tables(pool, include_triggers=True)

        combined = "\n".join(pool.conn.queries)
        self.assertIn("(12, 'V4 profile and query family lease foundation')", combined)
        self.assertIn("(13, 'V4 listing dedupe and discovery timestamp fields')", combined)
        self.assertIn("(14, 'V4.4 lowball report and pricing foundations')", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS discovery_ts TIMESTAMPTZ", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS current_price NUMERIC", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_price_hash TEXT", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS persisted_at TIMESTAMPTZ", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_event_kind TEXT", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS availability_status TEXT NOT NULL DEFAULT 'active';", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_seen_in_search_at TIMESTAMPTZ;", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS report_action_taken TEXT;", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS profiles", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS query_families", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS query_variants", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS model_prices", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS listing_price_history", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS als_score_log", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS lowball_report_runs", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS lowball_report_entries", combined)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_profiles_claim", combined)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_query_families_claim", combined)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_query_variants_family_enabled", combined)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_listings_availability_status", combined)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_listings_report_action_taken", combined)
        self.assertIn("CREATE TRIGGER trg_set_updated_at_profiles", combined)
        self.assertIn("CREATE TRIGGER trg_set_updated_at_query_families", combined)
        self.assertIn("CREATE TRIGGER trg_set_updated_at_query_variants", combined)
        self.assertIn("CREATE TRIGGER trg_set_updated_at_lowball_report_runs", combined)

    def test_sql_bootstrap_file_contains_v4_tables_and_schema_version(self) -> None:
        sql_path = (
            Path(__file__).resolve().parents[1] / "services" / "api" / "sql" / "001_init.sql"
        )
        sql_text = sql_path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS schema_versions", sql_text)
        self.assertIn("(12, 'V4 profile and query family lease foundation')", sql_text)
        self.assertIn("(13, 'V4 listing dedupe and discovery timestamp fields')", sql_text)
        self.assertIn("(14, 'V4.4 lowball report and pricing foundations')", sql_text)
        self.assertIn("discovery_ts TIMESTAMPTZ", sql_text)
        self.assertIn("first_seen_at TIMESTAMPTZ", sql_text)
        self.assertIn("last_seen_at TIMESTAMPTZ", sql_text)
        self.assertIn("current_price NUMERIC", sql_text)
        self.assertIn("last_price_hash TEXT", sql_text)
        self.assertIn("persisted_at TIMESTAMPTZ", sql_text)
        self.assertIn("last_event_kind TEXT", sql_text)
        self.assertIn("availability_status TEXT NOT NULL DEFAULT 'active'", sql_text)
        self.assertIn("last_checked_at TIMESTAMPTZ", sql_text)
        self.assertIn("last_seen_in_search_at TIMESTAMPTZ", sql_text)
        self.assertIn("report_action_taken TEXT", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS profiles", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS query_families", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS query_variants", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS model_prices", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS listing_price_history", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS als_score_log", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS lowball_report_runs", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS lowball_report_entries", sql_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_profiles_claim", sql_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_query_families_claim", sql_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_query_variants_family_enabled", sql_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_listings_availability_status", sql_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_listings_report_action_taken", sql_text)

    async def test_v4_backfill_queries_are_idempotent_and_cover_v3_sources(self) -> None:
        conn = _RecordingConn()

        await schema_ensure._backfill_v4_scheduler_tables(conn)
        await schema_ensure._backfill_v4_scheduler_tables(conn)

        combined = "\n".join(conn.queries)
        self.assertIn("INSERT INTO profiles", combined)
        self.assertIn("FROM execution_profiles ep", combined)
        self.assertIn("FROM worker_heartbeats wh", combined)
        self.assertIn("ON CONFLICT (user_data_dir) DO UPDATE", combined)
        self.assertIn("INSERT INTO query_families", combined)
        self.assertIn("FROM central_routes cr", combined)
        self.assertIn("INSERT INTO query_variants", combined)
        self.assertIn("FROM route_queries rq", combined)
        self.assertIn("ON CONFLICT (name) DO UPDATE", combined)
        self.assertIn("ON CONFLICT (family_id, query_text) DO UPDATE", combined)
        self.assertIn("UPDATE query_families qf", combined)
        self.assertIn("variant_count", combined)
        self.assertIn(
            "manual_login_required = COALESCE(profiles.manual_login_required, FALSE) OR COALESCE(EXCLUDED.manual_login_required, FALSE)",
            combined,
        )

    async def test_central_backfill_preserves_manual_login_quarantine(self) -> None:
        conn = _CentralBackfillConn()

        await schema_ensure._backfill_central_scheduler_tables(conn)

        combined = "\n".join(conn.queries)
        self.assertIn("manual_login_required", combined)
        self.assertIn("manual_login_reason", combined)
        self.assertIn(
            "manual_login_required = COALESCE(execution_profiles.manual_login_required, FALSE) OR COALESCE(EXCLUDED.manual_login_required, FALSE)",
            combined,
        )
        self.assertIn(
            "manual_login_reason = COALESCE(execution_profiles.manual_login_reason, EXCLUDED.manual_login_reason)",
            combined,
        )
        self.assertIn(
            "WHEN COALESCE(execution_profiles.manual_login_required, FALSE) OR COALESCE(EXCLUDED.manual_login_required, FALSE) THEN 'NEEDS_LOGIN'",
            combined,
        )

    def test_sql_bootstrap_file_contains_v4_backfill_statements(self) -> None:
        sql_path = (
            Path(__file__).resolve().parents[1] / "services" / "api" / "sql" / "001_init.sql"
        )
        sql_text = sql_path.read_text(encoding="utf-8")

        self.assertIn("INSERT INTO profiles (", sql_text)
        self.assertIn("FROM execution_profiles ep", sql_text)
        self.assertIn("FROM worker_heartbeats wh", sql_text)
        self.assertIn("INSERT INTO query_families (", sql_text)
        self.assertIn("FROM central_routes cr", sql_text)
        self.assertIn("INSERT INTO query_variants (", sql_text)
        self.assertIn("FROM route_queries rq", sql_text)
        self.assertIn(
            "manual_login_required = COALESCE(profiles.manual_login_required, FALSE)",
            sql_text,
        )


if __name__ == "__main__":
    unittest.main()
