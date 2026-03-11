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


class V4SchemaFoundationTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_worker_tables_adds_v4_schema_and_is_repeatable(self) -> None:
        pool = _RecordingPool()

        await schema_ensure.ensure_worker_tables(pool, include_triggers=True)
        await schema_ensure.ensure_worker_tables(pool, include_triggers=True)

        combined = "\n".join(pool.conn.queries)
        self.assertIn("(12, 'V4 profile and query family lease foundation')", combined)
        self.assertIn("(13, 'V4 listing dedupe and discovery timestamp fields')", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS discovery_ts TIMESTAMPTZ", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS current_price NUMERIC", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_price_hash TEXT", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS persisted_at TIMESTAMPTZ", combined)
        self.assertIn("ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_event_kind TEXT", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS profiles", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS query_families", combined)
        self.assertIn("CREATE TABLE IF NOT EXISTS query_variants", combined)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_profiles_claim", combined)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_query_families_claim", combined)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_query_variants_family_enabled", combined)
        self.assertIn("CREATE TRIGGER trg_set_updated_at_profiles", combined)
        self.assertIn("CREATE TRIGGER trg_set_updated_at_query_families", combined)
        self.assertIn("CREATE TRIGGER trg_set_updated_at_query_variants", combined)

    def test_sql_bootstrap_file_contains_v4_tables_and_schema_version(self) -> None:
        sql_path = (
            Path(__file__).resolve().parents[1] / "services" / "api" / "sql" / "001_init.sql"
        )
        sql_text = sql_path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS schema_versions", sql_text)
        self.assertIn("(12, 'V4 profile and query family lease foundation')", sql_text)
        self.assertIn("(13, 'V4 listing dedupe and discovery timestamp fields')", sql_text)
        self.assertIn("discovery_ts TIMESTAMPTZ", sql_text)
        self.assertIn("first_seen_at TIMESTAMPTZ", sql_text)
        self.assertIn("last_seen_at TIMESTAMPTZ", sql_text)
        self.assertIn("current_price NUMERIC", sql_text)
        self.assertIn("last_price_hash TEXT", sql_text)
        self.assertIn("persisted_at TIMESTAMPTZ", sql_text)
        self.assertIn("last_event_kind TEXT", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS profiles", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS query_families", sql_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS query_variants", sql_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_profiles_claim", sql_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_query_families_claim", sql_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_query_variants_family_enabled", sql_text)

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


if __name__ == "__main__":
    unittest.main()
