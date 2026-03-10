CREATE TABLE IF NOT EXISTS listings (
    seq_id BIGSERIAL PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    price NUMERIC,
    location TEXT,
    url TEXT,
    description TEXT,
    seller_name TEXT,
    thumbnail_url TEXT,
    model TEXT,
    condition TEXT,
    max_buy_price NUMERIC,
    potential_profit NUMERIC,
    status TEXT DEFAULT 'new',
    enrichment_status TEXT NOT NULL DEFAULT 'complete',
    enrichment_source_hash TEXT,
    enriched_at TIMESTAMPTZ,
    enrichment_last_error TEXT,
    source_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE listings ADD COLUMN IF NOT EXISTS thumbnail_url TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS enrichment_status TEXT NOT NULL DEFAULT 'complete';
ALTER TABLE listings ADD COLUMN IF NOT EXISTS enrichment_source_hash TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS enrichment_last_error TEXT;
UPDATE listings
SET enrichment_status = CASE
    WHEN LOWER(COALESCE(enrichment_status, '')) IN ('pending', 'complete', 'failed')
        THEN LOWER(enrichment_status)
    ELSE 'complete'
END
WHERE enrichment_status IS NULL
   OR LOWER(COALESCE(enrichment_status, '')) NOT IN ('pending', 'complete', 'failed');

CREATE INDEX IF NOT EXISTS idx_listings_created_at ON listings (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings (status);
CREATE INDEX IF NOT EXISTS idx_listings_profit ON listings (potential_profit DESC);
CREATE INDEX IF NOT EXISTS idx_listings_enrichment_status ON listings (enrichment_status, updated_at DESC);

CREATE OR REPLACE FUNCTION set_updated_at_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_updated_at ON listings;
CREATE TRIGGER trg_set_updated_at
BEFORE UPDATE ON listings
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

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
    profitable_hit_rate DOUBLE PRECISION,
    recent_duplicate_ratio DOUBLE PRECISION,
    avg_page_load_ms DOUBLE PRECISION,
    successful_cycles INTEGER NOT NULL DEFAULT 0,
    last_selected_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    cooldown_until TIMESTAMPTZ,
    lane_override TEXT,
    computed_lane TEXT NOT NULL DEFAULT 'warm',
    effective_lane TEXT NOT NULL DEFAULT 'warm',
    priority_score DOUBLE PRECISION,
    priority_score_updated_at TIMESTAMPTZ,
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

ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS proxy_mode TEXT NOT NULL DEFAULT 'fixed';
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS proxy_pool TEXT;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS preferred_proxy_key TEXT;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS preferred_proxy_updated_at TIMESTAMPTZ;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ENABLED';
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS status_reason TEXT;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS status_since TIMESTAMPTZ;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS route_interval_seconds INTEGER;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS avg_result_count DOUBLE PRECISION;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS profitable_hit_rate DOUBLE PRECISION;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS recent_duplicate_ratio DOUBLE PRECISION;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS avg_page_load_ms DOUBLE PRECISION;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS successful_cycles INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS cooldown_until TIMESTAMPTZ;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS lane_override TEXT;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS computed_lane TEXT NOT NULL DEFAULT 'warm';
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS effective_lane TEXT NOT NULL DEFAULT 'warm';
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS priority_score DOUBLE PRECISION;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS priority_score_updated_at TIMESTAMPTZ;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS manual_login_required BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS manual_login_reason TEXT;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS manual_login_required_at TIMESTAMPTZ;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;
ALTER TABLE worker_routes ADD COLUMN IF NOT EXISTS quarantine_evidence JSONB;
UPDATE worker_routes
SET proxy_mode = 'fixed'
WHERE proxy_mode IS NULL OR BTRIM(proxy_mode) = '';
UPDATE worker_routes
SET status = CASE
    WHEN is_enabled = FALSE THEN 'DISABLED'
    WHEN COALESCE(manual_login_required, FALSE) = TRUE THEN 'NEEDS_LOGIN'
    WHEN cooldown_until IS NOT NULL AND cooldown_until > NOW() THEN 'COOLDOWN'
    ELSE 'ENABLED'
END,
status_since = COALESCE(status_since, NOW())
WHERE status IS NULL OR BTRIM(status) = '';
UPDATE worker_routes
SET
    quarantined_at = COALESCE(quarantined_at, manual_login_required_at),
    quarantine_reason = COALESCE(NULLIF(BTRIM(quarantine_reason), ''), manual_login_reason)
WHERE COALESCE(manual_login_required, FALSE) = TRUE;
UPDATE worker_routes
SET
    profitable_hit_rate = COALESCE(profitable_hit_rate, 0.0),
    recent_duplicate_ratio = COALESCE(recent_duplicate_ratio, 0.0),
    computed_lane = CASE
        WHEN LOWER(COALESCE(computed_lane, '')) IN ('hot', 'warm', 'sweep')
            THEN LOWER(computed_lane)
        ELSE 'warm'
    END,
    effective_lane = CASE
        WHEN LOWER(COALESCE(effective_lane, '')) IN ('hot', 'warm', 'sweep')
            THEN LOWER(effective_lane)
        ELSE 'warm'
    END,
    lane_override = CASE
        WHEN LOWER(COALESCE(lane_override, '')) IN ('hot', 'warm', 'sweep')
            THEN LOWER(lane_override)
        ELSE NULL
    END;

CREATE INDEX IF NOT EXISTS idx_worker_routes_worker ON worker_routes (worker_name, is_enabled, priority, route_name);
CREATE INDEX IF NOT EXISTS idx_worker_routes_schedule
ON worker_routes (worker_name, is_enabled, next_run_at, priority, route_name);
CREATE INDEX IF NOT EXISTS idx_worker_routes_status
ON worker_routes (worker_name, is_enabled, status, cooldown_until, next_run_at);

DROP TRIGGER IF EXISTS trg_set_updated_at_worker_routes ON worker_routes;
CREATE TRIGGER trg_set_updated_at_worker_routes
BEFORE UPDATE ON worker_routes
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

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

CREATE TABLE IF NOT EXISTS worker_scrape_events (
    id BIGSERIAL PRIMARY KEY,
    worker_name TEXT NOT NULL,
    route_name TEXT,
    scraped_count INTEGER NOT NULL DEFAULT 0,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worker_scrape_events_worker_observed
ON worker_scrape_events (worker_name, observed_at DESC);

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

CREATE INDEX IF NOT EXISTS idx_worker_proxy_leases_until
ON worker_proxy_leases (lease_until, last_used_at);

DROP TRIGGER IF EXISTS trg_set_updated_at_worker_proxy_leases ON worker_proxy_leases;
CREATE TRIGGER trg_set_updated_at_worker_proxy_leases
BEFORE UPDATE ON worker_proxy_leases
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

CREATE TABLE IF NOT EXISTS proxy_stats (
    proxy_key TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_success_at TIMESTAMPTZ,
    banned_until TIMESTAMPTZ,
    avg_latency_ms INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_proxy_stats_banned_until
ON proxy_stats (banned_until, consecutive_failures, last_success_at);

CREATE TABLE IF NOT EXISTS notification_delivery_ledger (
    listing_id TEXT PRIMARY KEY,
    first_stream_event_id TEXT NOT NULL,
    last_stream_event_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notification_delivery_ledger_status
ON notification_delivery_ledger (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS listing_enrichment_ledger (
    listing_id TEXT PRIMARY KEY,
    first_stream_event_id TEXT NOT NULL,
    last_stream_event_id TEXT NOT NULL,
    enrichment_source_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TIMESTAMPTZ,
    enriched_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_listing_enrichment_ledger_status
ON listing_enrichment_ledger (status, updated_at DESC);
