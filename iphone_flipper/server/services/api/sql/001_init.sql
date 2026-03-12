CREATE TABLE IF NOT EXISTS schema_versions (
    version INT PRIMARY KEY,
    description TEXT,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO schema_versions (version, description) VALUES
    (1, 'Initial worker_routes, heartbeats, scrape_events'),
    (2, 'Proxy mode, pool, preferred proxy columns'),
    (3, 'Route status, quarantine, cooldown columns'),
    (4, 'Worker proxy leases and proxy stats tables'),
    (5, 'Schema versioning table'),
    (6, 'Notification delivery ledger'),
    (7, 'Priority scheduler route lanes and metrics'),
    (8, 'Background enrichment fields and ledger'),
    (9, 'Phase 6 runtime controls and publish health'),
    (10, 'Worker heartbeat route snapshot fields'),
    (11, 'Central route scheduler tables and execution profiles'),
    (12, 'V4 profile and query family lease foundation'),
    (13, 'V4 listing dedupe and discovery timestamp fields')
ON CONFLICT (version) DO NOTHING;

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
    discovery_ts TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    current_price NUMERIC,
    last_price_hash TEXT,
    persisted_at TIMESTAMPTZ,
    last_event_kind TEXT,
    source_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE listings ADD COLUMN IF NOT EXISTS thumbnail_url TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS enrichment_status TEXT NOT NULL DEFAULT 'complete';
ALTER TABLE listings ADD COLUMN IF NOT EXISTS enrichment_source_hash TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS enrichment_last_error TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS discovery_ts TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS current_price NUMERIC;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_price_hash TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS persisted_at TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_event_kind TEXT;
UPDATE listings
SET discovery_ts = COALESCE(discovery_ts, source_seen_at, created_at),
    first_seen_at = COALESCE(first_seen_at, source_seen_at, created_at),
    last_seen_at = COALESCE(last_seen_at, source_seen_at, updated_at),
    current_price = COALESCE(current_price, price),
    persisted_at = COALESCE(persisted_at, updated_at),
    last_event_kind = COALESCE(last_event_kind, 'listing_created')
WHERE discovery_ts IS NULL
   OR first_seen_at IS NULL
   OR last_seen_at IS NULL
   OR current_price IS NULL
   OR persisted_at IS NULL
   OR last_event_kind IS NULL;
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

CREATE TABLE IF NOT EXISTS central_routes (
    route_name TEXT PRIMARY KEY,
    legacy_worker_name TEXT,
    legacy_route_name TEXT,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    proxy_server TEXT NOT NULL DEFAULT '',
    proxy_username TEXT,
    proxy_password TEXT,
    proxy_mode TEXT NOT NULL DEFAULT 'fixed',
    proxy_pool TEXT,
    preferred_proxy_key TEXT,
    preferred_proxy_updated_at TIMESTAMPTZ,
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
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (legacy_worker_name, legacy_route_name)
);

CREATE INDEX IF NOT EXISTS idx_central_routes_schedule
ON central_routes (is_enabled, status, cooldown_until, next_run_at, priority, route_name);
CREATE INDEX IF NOT EXISTS idx_central_routes_status
ON central_routes (is_enabled, status, effective_lane, priority_score DESC);

DROP TRIGGER IF EXISTS trg_set_updated_at_central_routes ON central_routes;
CREATE TRIGGER trg_set_updated_at_central_routes
BEFORE UPDATE ON central_routes
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

CREATE TABLE IF NOT EXISTS route_queries (
    id BIGSERIAL PRIMARY KEY,
    route_name TEXT NOT NULL REFERENCES central_routes(route_name) ON DELETE CASCADE,
    query_text TEXT NOT NULL,
    query_order INTEGER NOT NULL DEFAULT 0,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_selected_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    avg_result_count DOUBLE PRECISION,
    profitable_hit_rate DOUBLE PRECISION,
    recent_duplicate_ratio DOUBLE PRECISION,
    selection_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (route_name, query_text)
);

CREATE INDEX IF NOT EXISTS idx_route_queries_route_enabled
ON route_queries (route_name, is_enabled, query_order);

DROP TRIGGER IF EXISTS trg_set_updated_at_route_queries ON route_queries;
CREATE TRIGGER trg_set_updated_at_route_queries
BEFORE UPDATE ON route_queries
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

CREATE TABLE IF NOT EXISTS execution_profiles (
    user_data_dir TEXT PRIMARY KEY,
    worker_name TEXT NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'READY',
    status_reason TEXT,
    status_since TIMESTAMPTZ,
    cooldown_until TIMESTAMPTZ,
    manual_login_required BOOLEAN NOT NULL DEFAULT FALSE,
    manual_login_reason TEXT,
    manual_login_required_at TIMESTAMPTZ,
    quarantined_at TIMESTAMPTZ,
    quarantine_reason TEXT,
    quarantine_evidence JSONB,
    last_selected_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execution_profiles_worker
ON execution_profiles (worker_name, is_enabled, status, cooldown_until, last_selected_at);

DROP TRIGGER IF EXISTS trg_set_updated_at_execution_profiles ON execution_profiles;
CREATE TRIGGER trg_set_updated_at_execution_profiles
BEFORE UPDATE ON execution_profiles
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

CREATE TABLE IF NOT EXISTS profiles (
    profile_id BIGSERIAL PRIMARY KEY,
    worker_name TEXT NOT NULL,
    user_data_dir TEXT NOT NULL UNIQUE,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'READY',
    status_reason TEXT,
    status_since TIMESTAMPTZ,
    available_after TIMESTAMPTZ,
    cooldown_until TIMESTAMPTZ,
    manual_login_required BOOLEAN NOT NULL DEFAULT FALSE,
    manual_login_reason TEXT,
    manual_login_required_at TIMESTAMPTZ,
    quarantined_at TIMESTAMPTZ,
    quarantine_reason TEXT,
    quarantine_evidence JSONB,
    failure_count INTEGER NOT NULL DEFAULT 0,
    consecutive_empty_claims INTEGER NOT NULL DEFAULT 0,
    last_started_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    last_error TEXT,
    profile_lease_token TEXT,
    profile_lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_profiles_claim
ON profiles (
    worker_name,
    is_enabled,
    status,
    manual_login_required,
    cooldown_until,
    available_after,
    profile_lease_expires_at,
    last_started_at
);

DROP TRIGGER IF EXISTS trg_set_updated_at_profiles ON profiles;
CREATE TRIGGER trg_set_updated_at_profiles
BEFORE UPDATE ON profiles
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

CREATE TABLE IF NOT EXISTS query_families (
    family_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    legacy_route_name TEXT,
    legacy_worker_name TEXT,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,
    priority_score DOUBLE PRECISION,
    lane TEXT NOT NULL DEFAULT 'warm',
    next_due_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    min_gap_s INTEGER NOT NULL DEFAULT 0,
    max_gap_s INTEGER,
    variant_cursor INTEGER NOT NULL DEFAULT 0,
    variant_count INTEGER NOT NULL DEFAULT 0,
    consecutive_hits INTEGER NOT NULL DEFAULT 0,
    consecutive_empty INTEGER NOT NULL DEFAULT 0,
    last_claimed_at TIMESTAMPTZ,
    last_discovery_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error TEXT,
    family_lease_token TEXT,
    family_lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_query_families_claim
ON query_families (
    is_enabled,
    next_due_at,
    family_lease_expires_at,
    priority_score DESC,
    priority DESC,
    family_id
);

DROP TRIGGER IF EXISTS trg_set_updated_at_query_families ON query_families;
CREATE TRIGGER trg_set_updated_at_query_families
BEFORE UPDATE ON query_families
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

CREATE TABLE IF NOT EXISTS query_variants (
    variant_id BIGSERIAL PRIMARY KEY,
    family_id BIGINT NOT NULL REFERENCES query_families(family_id) ON DELETE CASCADE,
    query_text TEXT NOT NULL,
    url_template TEXT,
    validation_state TEXT NOT NULL DEFAULT 'pending_validation',
    weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    variant_order INTEGER NOT NULL DEFAULT 0,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_selected_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (family_id, query_text)
);

CREATE INDEX IF NOT EXISTS idx_query_variants_family_enabled
ON query_variants (family_id, is_enabled, variant_order, query_text);

DROP TRIGGER IF EXISTS trg_set_updated_at_query_variants ON query_variants;
CREATE TRIGGER trg_set_updated_at_query_variants
BEFORE UPDATE ON query_variants
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
    last_event_publish_at TIMESTAMPTZ,
    last_event_publish_status TEXT,
    last_event_publish_error TEXT,
    last_stream_event_id TEXT,
    route_source TEXT,
    route_user_data_dir TEXT,
    route_search_queries TEXT,
    route_proxy_mode TEXT,
    route_proxy_server TEXT,
    route_proxy_username TEXT,
    route_proxy_password TEXT,
    route_proxy_pool TEXT,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS last_event_publish_at TIMESTAMPTZ;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS last_event_publish_status TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS last_event_publish_error TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS last_stream_event_id TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS route_source TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS route_user_data_dir TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS route_search_queries TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS route_proxy_mode TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS route_proxy_server TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS route_proxy_username TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS route_proxy_password TEXT;
ALTER TABLE worker_heartbeats ADD COLUMN IF NOT EXISTS route_proxy_pool TEXT;

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

INSERT INTO profiles (
    worker_name,
    user_data_dir,
    is_enabled,
    status,
    status_reason,
    status_since,
    available_after,
    cooldown_until,
    manual_login_required,
    manual_login_reason,
    manual_login_required_at,
    quarantined_at,
    quarantine_reason,
    quarantine_evidence,
    failure_count,
    last_started_at,
    last_success_at,
    last_failure_at,
    last_error
)
SELECT
    ep.worker_name,
    ep.user_data_dir,
    ep.is_enabled,
    CASE
        WHEN ep.is_enabled = FALSE THEN 'DISABLED'
        WHEN COALESCE(ep.manual_login_required, FALSE) = TRUE THEN 'NEEDS_LOGIN'
        WHEN COALESCE(ep.status, 'READY') IN ('READY', 'DEGRADED', 'THROTTLED', 'COOLDOWN', 'NEEDS_LOGIN', 'DISABLED')
            THEN COALESCE(ep.status, 'READY')
        WHEN ep.cooldown_until IS NOT NULL AND ep.cooldown_until > NOW() THEN 'COOLDOWN'
        ELSE 'READY'
    END,
    ep.status_reason,
    ep.status_since,
    NULL,
    ep.cooldown_until,
    COALESCE(ep.manual_login_required, FALSE),
    ep.manual_login_reason,
    ep.manual_login_required_at,
    ep.quarantined_at,
    ep.quarantine_reason,
    ep.quarantine_evidence,
    GREATEST(0, COALESCE(ep.consecutive_failures, 0)),
    ep.last_selected_at,
    ep.last_success_at,
    CASE
        WHEN NULLIF(BTRIM(COALESCE(ep.last_error, '')), '') IS NOT NULL
            THEN COALESCE(ep.status_since, ep.manual_login_required_at, ep.last_success_at, NOW())
        ELSE NULL
    END,
    NULLIF(BTRIM(COALESCE(ep.last_error, '')), '')
FROM execution_profiles ep
WHERE NULLIF(BTRIM(COALESCE(ep.user_data_dir, '')), '') IS NOT NULL
ON CONFLICT (user_data_dir) DO UPDATE SET
    worker_name = EXCLUDED.worker_name,
    is_enabled = EXCLUDED.is_enabled,
    status = CASE
        WHEN EXCLUDED.is_enabled = FALSE THEN 'DISABLED'
        WHEN COALESCE(profiles.manual_login_required, FALSE)
            OR COALESCE(EXCLUDED.manual_login_required, FALSE) THEN 'NEEDS_LOGIN'
        WHEN COALESCE(EXCLUDED.cooldown_until, profiles.cooldown_until) IS NOT NULL
            AND COALESCE(EXCLUDED.cooldown_until, profiles.cooldown_until) > NOW() THEN 'COOLDOWN'
        ELSE COALESCE(profiles.status, EXCLUDED.status, 'READY')
    END,
    status_reason = COALESCE(EXCLUDED.status_reason, profiles.status_reason),
    status_since = COALESCE(EXCLUDED.status_since, profiles.status_since),
    cooldown_until = COALESCE(EXCLUDED.cooldown_until, profiles.cooldown_until),
    manual_login_required = COALESCE(profiles.manual_login_required, FALSE)
        OR COALESCE(EXCLUDED.manual_login_required, FALSE),
    manual_login_reason = COALESCE(profiles.manual_login_reason, EXCLUDED.manual_login_reason),
    manual_login_required_at = COALESCE(profiles.manual_login_required_at, EXCLUDED.manual_login_required_at),
    quarantined_at = COALESCE(profiles.quarantined_at, EXCLUDED.quarantined_at),
    quarantine_reason = COALESCE(profiles.quarantine_reason, EXCLUDED.quarantine_reason),
    quarantine_evidence = COALESCE(profiles.quarantine_evidence, EXCLUDED.quarantine_evidence),
    failure_count = GREATEST(COALESCE(profiles.failure_count, 0), COALESCE(EXCLUDED.failure_count, 0)),
    last_started_at = COALESCE(EXCLUDED.last_started_at, profiles.last_started_at),
    last_success_at = COALESCE(EXCLUDED.last_success_at, profiles.last_success_at),
    last_failure_at = COALESCE(EXCLUDED.last_failure_at, profiles.last_failure_at),
    last_error = COALESCE(EXCLUDED.last_error, profiles.last_error);

INSERT INTO profiles (
    worker_name,
    user_data_dir,
    is_enabled,
    status,
    status_since,
    last_started_at,
    last_success_at,
    last_failure_at,
    last_error
)
SELECT
    wh.worker_name,
    wh.route_user_data_dir,
    TRUE,
    'READY',
    NOW(),
    wh.last_run_started_at,
    CASE
        WHEN NULLIF(BTRIM(COALESCE(wh.last_error, '')), '') IS NULL THEN wh.last_run_finished_at
        ELSE NULL
    END,
    CASE
        WHEN NULLIF(BTRIM(COALESCE(wh.last_error, '')), '') IS NOT NULL
            THEN COALESCE(wh.last_run_finished_at, wh.last_run_started_at, NOW())
        ELSE NULL
    END,
    NULLIF(BTRIM(COALESCE(wh.last_error, '')), '')
FROM worker_heartbeats wh
WHERE NULLIF(BTRIM(COALESCE(wh.worker_name, '')), '') IS NOT NULL
  AND NULLIF(BTRIM(COALESCE(wh.route_user_data_dir, '')), '') IS NOT NULL
  AND NOT EXISTS (
        SELECT 1
        FROM execution_profiles ep
        WHERE ep.user_data_dir = wh.route_user_data_dir
  )
ON CONFLICT (user_data_dir) DO UPDATE SET
    worker_name = EXCLUDED.worker_name,
    last_started_at = COALESCE(profiles.last_started_at, EXCLUDED.last_started_at),
    last_success_at = COALESCE(profiles.last_success_at, EXCLUDED.last_success_at),
    last_failure_at = COALESCE(profiles.last_failure_at, EXCLUDED.last_failure_at),
    last_error = COALESCE(profiles.last_error, EXCLUDED.last_error);

INSERT INTO query_families (
    name,
    legacy_route_name,
    legacy_worker_name,
    is_enabled,
    priority,
    priority_score,
    lane,
    next_due_at,
    min_gap_s,
    max_gap_s,
    variant_cursor,
    variant_count,
    last_success_at,
    last_error
)
SELECT
    cr.route_name,
    NULLIF(BTRIM(COALESCE(cr.legacy_route_name, '')), ''),
    NULLIF(BTRIM(COALESCE(cr.legacy_worker_name, '')), ''),
    COALESCE(cr.is_enabled, TRUE),
    COALESCE(cr.priority, 100),
    cr.priority_score,
    CASE
        WHEN LOWER(COALESCE(cr.effective_lane, cr.computed_lane, '')) IN ('hot', 'warm', 'sweep')
            THEN LOWER(COALESCE(cr.effective_lane, cr.computed_lane, 'warm'))
        ELSE 'warm'
    END,
    COALESCE(cr.next_run_at, NOW()),
    GREATEST(0, COALESCE(cr.route_interval_seconds, 0)),
    CASE
        WHEN cr.route_interval_seconds IS NULL THEN NULL
        ELSE GREATEST(0, COALESCE(cr.route_interval_seconds, 0))
    END,
    0,
    COALESCE(
        (
            SELECT COUNT(*)
            FROM route_queries rq
            WHERE rq.route_name = cr.route_name
              AND COALESCE(rq.is_enabled, TRUE) = TRUE
        ),
        0
    ),
    cr.last_success_at,
    NULLIF(BTRIM(COALESCE(cr.last_error, '')), '')
FROM central_routes cr
WHERE NULLIF(BTRIM(COALESCE(cr.route_name, '')), '') IS NOT NULL
ON CONFLICT (name) DO UPDATE SET
    legacy_route_name = COALESCE(EXCLUDED.legacy_route_name, query_families.legacy_route_name),
    legacy_worker_name = COALESCE(EXCLUDED.legacy_worker_name, query_families.legacy_worker_name),
    is_enabled = EXCLUDED.is_enabled,
    priority = EXCLUDED.priority,
    priority_score = COALESCE(EXCLUDED.priority_score, query_families.priority_score),
    lane = EXCLUDED.lane,
    next_due_at = COALESCE(EXCLUDED.next_due_at, query_families.next_due_at),
    min_gap_s = EXCLUDED.min_gap_s,
    max_gap_s = COALESCE(EXCLUDED.max_gap_s, query_families.max_gap_s),
    variant_count = GREATEST(COALESCE(query_families.variant_count, 0), COALESCE(EXCLUDED.variant_count, 0)),
    last_success_at = COALESCE(EXCLUDED.last_success_at, query_families.last_success_at),
    last_error = COALESCE(EXCLUDED.last_error, query_families.last_error);

INSERT INTO query_variants (
    family_id,
    query_text,
    url_template,
    validation_state,
    weight,
    variant_order,
    is_enabled,
    last_selected_at,
    last_success_at,
    notes
)
SELECT
    qf.family_id,
    rq.query_text,
    NULL,
    'pending_validation',
    1.0,
    COALESCE(rq.query_order, 0),
    COALESCE(rq.is_enabled, TRUE),
    rq.last_selected_at,
    rq.last_success_at,
    'Seeded from route_queries during V4 phase 1 backfill'
FROM route_queries rq
JOIN query_families qf
  ON qf.name = rq.route_name
WHERE NULLIF(BTRIM(COALESCE(rq.query_text, '')), '') IS NOT NULL
ON CONFLICT (family_id, query_text) DO UPDATE SET
    variant_order = EXCLUDED.variant_order,
    is_enabled = EXCLUDED.is_enabled,
    last_selected_at = COALESCE(EXCLUDED.last_selected_at, query_variants.last_selected_at),
    last_success_at = COALESCE(EXCLUDED.last_success_at, query_variants.last_success_at),
    notes = COALESCE(query_variants.notes, EXCLUDED.notes);

UPDATE query_families qf
SET
    variant_count = COALESCE(variant_counts.variant_count, 0),
    variant_cursor = CASE
        WHEN COALESCE(variant_counts.variant_count, 0) <= 0 THEN 0
        ELSE LEAST(COALESCE(qf.variant_cursor, 0), variant_counts.variant_count - 1)
    END
FROM (
    SELECT
        family_id,
        COUNT(*) FILTER (WHERE COALESCE(is_enabled, TRUE) = TRUE) AS variant_count
    FROM query_variants
    GROUP BY family_id
) AS variant_counts
WHERE qf.family_id = variant_counts.family_id;

UPDATE query_families
SET
    variant_count = 0,
    variant_cursor = 0
WHERE NOT EXISTS (
    SELECT 1
    FROM query_variants qv
    WHERE qv.family_id = query_families.family_id
      AND COALESCE(qv.is_enabled, TRUE) = TRUE
);
