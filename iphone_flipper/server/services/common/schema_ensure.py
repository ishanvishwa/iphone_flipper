from __future__ import annotations

import asyncpg


async def ensure_worker_tables(pool: asyncpg.Pool, include_triggers: bool = False) -> None:
    """Create or migrate all worker-related database tables.

    Idempotent — safe to call on every worker start-up.
    When *include_triggers* is True the ``set_updated_at_timestamp`` function
    and associated triggers are also created.
    """
    async with pool.acquire() as conn:
        # ------------------------------------------------------------------
        # Schema version tracking (idempotent)
        # ------------------------------------------------------------------
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_versions (
                version INT PRIMARY KEY,
                description TEXT,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            """
            INSERT INTO schema_versions (version, description) VALUES
                (1, 'Initial worker_routes, heartbeats, scrape_events'),
                (2, 'Proxy mode, pool, preferred proxy columns'),
                (3, 'Route status, quarantine, cooldown columns'),
                (4, 'Worker proxy leases and proxy stats tables'),
                (5, 'Schema versioning table')
            ON CONFLICT (version) DO NOTHING;
            """
        )
        if include_triggers:
            await conn.execute(
                """
                CREATE OR REPLACE FUNCTION set_updated_at_timestamp()
                RETURNS TRIGGER AS $$
                BEGIN
                  NEW.updated_at = NOW();
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
            )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_routes (
                id BIGSERIAL PRIMARY KEY,
                worker_name TEXT NOT NULL,
                route_name TEXT NOT NULL,
                is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                proxy_server TEXT NOT NULL,
                proxy_username TEXT,
                proxy_password TEXT,
                proxy_mode TEXT NOT NULL DEFAULT 'fixed',
                proxy_pool TEXT,
                preferred_proxy_key TEXT,
                preferred_proxy_updated_at TIMESTAMPTZ,
                user_data_dir TEXT,
                search_queries TEXT,
                priority INTEGER NOT NULL DEFAULT 100,
                status TEXT NOT NULL DEFAULT 'ENABLED',
                status_reason TEXT,
                status_since TIMESTAMPTZ,
                next_run_at TIMESTAMPTZ,
                route_interval_seconds INTEGER,
                avg_result_count DOUBLE PRECISION,
                avg_page_load_ms DOUBLE PRECISION,
                successful_cycles INTEGER NOT NULL DEFAULT 0,
                last_selected_at TIMESTAMPTZ,
                last_success_at TIMESTAMPTZ,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                cooldown_until TIMESTAMPTZ,
                manual_login_required BOOLEAN NOT NULL DEFAULT FALSE,
                manual_login_reason TEXT,
                manual_login_required_at TIMESTAMPTZ,
                quarantined_at TIMESTAMPTZ,
                quarantine_reason TEXT,
                quarantine_evidence JSONB,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(worker_name, route_name)
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_worker_routes_worker
            ON worker_routes (worker_name, is_enabled, priority, route_name);
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_worker_routes_schedule
            ON worker_routes (worker_name, is_enabled, next_run_at, priority, route_name);
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_worker_routes_status
            ON worker_routes (worker_name, is_enabled, status, cooldown_until, next_run_at);
            """
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS proxy_mode TEXT NOT NULL DEFAULT 'fixed';"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS proxy_pool TEXT;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS preferred_proxy_key TEXT;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS preferred_proxy_updated_at TIMESTAMPTZ;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ENABLED';"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS status_reason TEXT;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS status_since TIMESTAMPTZ;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS route_interval_seconds INTEGER;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS avg_result_count DOUBLE PRECISION;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS avg_page_load_ms DOUBLE PRECISION;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS successful_cycles INTEGER NOT NULL DEFAULT 0;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS cooldown_until TIMESTAMPTZ;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS manual_login_required BOOLEAN NOT NULL DEFAULT FALSE;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS manual_login_reason TEXT;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS manual_login_required_at TIMESTAMPTZ;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;"
        )
        await conn.execute(
            "ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS quarantine_evidence JSONB;"
        )
        await conn.execute(
            """
            UPDATE worker_routes
            SET proxy_mode = 'fixed'
            WHERE proxy_mode IS NULL OR BTRIM(proxy_mode) = '';
            """
        )
        await conn.execute(
            """
            UPDATE worker_routes
            SET status = CASE
                WHEN is_enabled = FALSE THEN 'DISABLED'
                WHEN COALESCE(manual_login_required, FALSE) = TRUE THEN 'NEEDS_LOGIN'
                WHEN cooldown_until IS NOT NULL AND cooldown_until > NOW() THEN 'COOLDOWN'
                ELSE 'ENABLED'
            END,
            status_since = COALESCE(status_since, NOW())
            WHERE status IS NULL OR BTRIM(status) = '';
            """
        )
        await conn.execute(
            """
            UPDATE worker_routes
            SET
                quarantined_at = COALESCE(quarantined_at, manual_login_required_at),
                quarantine_reason = COALESCE(NULLIF(BTRIM(quarantine_reason), ''), manual_login_reason)
            WHERE COALESCE(manual_login_required, FALSE) = TRUE
            """
        )

        if include_triggers:
            await conn.execute("DROP TRIGGER IF EXISTS trg_set_updated_at_worker_routes ON worker_routes;")
            await conn.execute(
                """
                CREATE TRIGGER trg_set_updated_at_worker_routes
                BEFORE UPDATE ON worker_routes
                FOR EACH ROW
                EXECUTE FUNCTION set_updated_at_timestamp();
                """
            )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_heartbeats (
                worker_name TEXT PRIMARY KEY,
                route_name TEXT,
                status TEXT NOT NULL DEFAULT 'idle',
                listings_saved INTEGER NOT NULL DEFAULT 0,
                query_count INTEGER NOT NULL DEFAULT 0,
                last_run_started_at TIMESTAMPTZ,
                last_run_finished_at TIMESTAMPTZ,
                last_error TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_scrape_events (
                id BIGSERIAL PRIMARY KEY,
                worker_name TEXT NOT NULL,
                route_name TEXT,
                scraped_count INTEGER NOT NULL DEFAULT 0,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_worker_scrape_events_worker_observed
            ON worker_scrape_events (worker_name, observed_at DESC);
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_proxy_leases (
                proxy_id TEXT PRIMARY KEY,
                proxy_server TEXT NOT NULL,
                proxy_username TEXT,
                proxy_password TEXT,
                leased_by_worker TEXT,
                leased_by_route TEXT,
                leased_at TIMESTAMPTZ,
                lease_until TIMESTAMPTZ,
                last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_worker_proxy_leases_until
            ON worker_proxy_leases (lease_until, last_used_at);
            """
        )

        if include_triggers:
            await conn.execute("DROP TRIGGER IF EXISTS trg_set_updated_at_worker_proxy_leases ON worker_proxy_leases;")
            await conn.execute(
                """
                CREATE TRIGGER trg_set_updated_at_worker_proxy_leases
                BEFORE UPDATE ON worker_proxy_leases
                FOR EACH ROW
                EXECUTE FUNCTION set_updated_at_timestamp();
                """
            )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS proxy_stats (
                proxy_key TEXT PRIMARY KEY,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_success_at TIMESTAMPTZ,
                banned_until TIMESTAMPTZ,
                avg_latency_ms INTEGER,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_proxy_stats_banned_until
            ON proxy_stats (banned_until, consecutive_failures, last_success_at);
            """
        )
