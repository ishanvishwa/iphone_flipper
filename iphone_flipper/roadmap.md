# iPhone Flipper Roadmap

## Document Intent

This roadmap is a comprehensive implementation plan that combines:

- Baseline product goals
- Completed work already delivered in code
- Additional/manual enhancements discovered during code audit
- Remaining work needed for stability, condition accuracy, and safe automation

Audit date: **2026-03-08**
Last implementation update: **2026-03-10**

## Roadmap Structure

- Phase 0: Foundation and Stabilization
- Phase 1: Discovery and Evaluation Engine
- Phase 2: Negotiation and Conversion Intelligence
- Phase 3: GUI and Operator Experience
- Phase 4: Condition Intelligence Hardening (priority)
- Phase 5: Reliability, Safety, and Scale
- Phase 5A: 24x7 Server and Real-Time Sync Migration (approved track)
- Phase 6: Strategic Expansion

---

## Phase 0: Foundation and Stabilization

### Goals

- Ensure local-first workflow is runnable and understandable
- Establish persistent data model and auth setup flow

### Completed

- [x] Core CLI entrypoint with command suite (`main.py`)
- [x] SQLite initialization and schema migrations (`scraper.py`, `deal_tracker.py`)
- [x] Persistence schema for multi-account/proxy operations (`fb_accounts`, `proxies`, `scraper_settings`)
- [x] DB-backed search query registry (`search_queries`) seeded from baseline iPhone query set
- [x] Auth setup + provider detection (`auth_manager.py`, `setup_auth.py`)
- [x] Auth status and token-expiry visibility (`main.py auth-status`, `auth_manager.py`)
- [x] One-time auth bootstrap from `auth.json` with automatic source-file cleanup
- [x] Launch entrypoint for GUI (`run_gui.py`)

### Remaining

- [ ] Add startup diagnostics check (DB writable, price sheet valid, auth present)
- [ ] Add schema version table for explicit DB migration tracking

---

## Phase 1: Discovery and Evaluation Engine

### Goals

- Find relevant listings reliably
- Estimate model/condition/profit quickly

### Completed

- [x] Playwright persistent profile scraping with anti-detection behaviors
- [x] Query expansion for iPhone 12–15 + damage terms
- [x] Dedupe across query runs by listing ID
- [x] Hybrid marketplace listing fetch (GraphQL response capture + DOM fallback)
- [x] Rich listing persistence fields captured when available (`location`, `description`, `seller_name`)
- [x] GraphQL capture widened (no strict request-body keyword gate) for better feed hit-rate
- [x] Price normalization hardened for mixed marketplace formats (`$300`, `300 AU$`, `1.000 AU$`) with currency-aware text fallback
- [x] Marketplace listing URLs normalized to canonical item links (tracking query params removed)
- [x] Model detection and rule-based condition inference
- [x] Automatic accessory-only listing suppression (`case/cover/protector/charger`) before persistence
- [x] Accessory suppression controls made operator-configurable in Settings (`accessory_filter_keywords`, `accessory_filter_max_price`)
- [x] VPS settings save now syncs accessory suppression controls to worker runtime DBs over SSH
- [x] VPS accessory-filter sync now auto-repairs worker runtime DB permissions on readonly failures and retries write propagation
- [x] VPS accessory-filter permission repair scope constrained to target DB dir/file (non-recursive) to avoid modifying unrelated runtime volumes
- [x] Desktop server-sync batch now purges local accessory-only rows to avoid stale accessory listings in GUI
- [x] Scraper persistence scope now limited to models present in Price Sheet (`price_list.csv`) only
- [x] Profit formula updated to `selling - listed - repair`
- [x] Status model expanded: `new`, `needs_pricing`, `unclassified`
- [x] Recalculation engine for existing listings after price-sheet edits
- [x] Scraper query source moved to DB-backed active `search_queries` with fallback defaults

### Additional Implemented (Audit-Detected, previously not in original roadmap)

- [x] More inclusive persistence of non-profitable/unclassified listings instead of dropping them
- [x] Scraper progress event stream for query-level UI telemetry
- [x] Cooperative stop/cancel mechanism for long scrape runs
- [x] Query-level source telemetry split (`graphql_found` vs `dom_found`) for runtime diagnostics
- [x] Search-query polling metadata refresh (`last_polled`) recorded on each processed query
- [x] Model identifier table expanded to include iPhone 16 family recognition
- [x] Immediate per-listing persistence and `listing_saved` event emission (no run-end batch wait)
- [x] Progressive scroll-depth loading to move beyond initial card batch (configurable target cards + max rounds)
- [x] Virtualized-feed-safe DOM accumulation across scroll snapshots (not only final viewport extraction)
- [x] Marketplace feed container-aware scrolling fallback (feed scroller + mouse-wheel) for deeper page traversal
- [x] Scraper module refactored from monolithic `scraper.py` into structured package (`scraper/`) with backward-compatible re-exports:
  - [x] `scraper/config.py` — constants and environment variable parsing
  - [x] `scraper/storage.py` — database operations and persistence
  - [x] `scraper/parsers.py` — model/condition/price parsing and accessory detection
  - [x] `scraper/proxy.py` — proxy bridge and Playwright proxy configuration
  - [x] `scraper/browser.py` — stealth scripts, browser context launcher, Dolphin Anty CDP integration with cloud-first API listing and duplicate-running reuse
  - [x] `scraper/core.py` — `scrape_marketplace()`, financial recalculation, accessory purge
  - [x] `scraper/legacy_utils.py` — V1 monolithic scraper logic preserved as shared utilities (GraphQL extraction, DOM extraction, progressive scroll, stealth scripts, listing persistence)
- [x] V2.2 `core/scraper/` package built for Dolphin Anty CDP-based browser automation:
  - [x] `core/scraper/driver.py` — CDP WebSocket connection via Dolphin Anty API (`connect_over_cdp`), stealth delegated to Dolphin's native engine
  - [x] `core/scraper/pipeline.py` — `scrape_marketplace()` with GraphQL interception on CDP-connected context
  - [x] `core/scraper/parser.py`, `config.py`, `storage.py` — model/condition/price/storage mirroring `scraper/` package
  - [x] `core/scraper/config.py` — carries bridge/connection-limiting configs (`PROXY_BRIDGE_*`, `BROWSER_MAX_CONNECTIONS_*`) from prior iterations; available for future re-activation

### Remaining

- [ ] Add detail-page selective fetch for low-confidence listings
- [ ] Add pricing anomaly detector (too-low/too-high outliers)
- [ ] Add duplicate URL fingerprinting beyond listing ID (future-proofing)
- [ ] Investigate low-yield sessions where GraphQL capture stays `0` and DOM discovery stalls near 24 cards despite deep scrolling (account/session quality or feed throttling)
- [ ] Tune accessory-only suppression heuristics from sampled false-positive/false-negative audits

---

## Phase 2: Negotiation and Conversion Intelligence

### Goals

- Assist operator with high-quality replies
- Learn from actual outcomes

### Completed

- [x] Negotiation generation (initial, response, offer)
- [x] Provider abstraction (Gemini/OpenAI)
- [x] Conversation persistence and analysis flow
- [x] Purchase recording with savings/timing/message metrics
- [x] Post-purchase resale/profit reconciliation workflow (`resale` command path)
- [x] Pattern learning store and conversion scoring
- [x] Insights generation from historical outcomes
- [x] Daily summary broadcast command (`summary`) for operational reporting

### Remaining

- [ ] Add explicit negotiation state machine transitions (deterministic)
- [ ] Add guardrails to block unsafe suggestions at runtime
- [ ] Add confidence-weighted conversion score components

---

## Phase 3: GUI and Operator Experience

### Goals

- Provide complete control plane via GUI
- Remove dependency on CLI for daily operations

### Completed

- [x] Multi-tab GUI (Listings, Negotiation, Deals)
- [x] Menu architecture (File, Scraper, Analytics, Price Sheet, Help)
- [x] Settings menu with GUI manager for:
  - [x] Facebook accounts CRUD + status controls + proxy assignment
  - [x] Manual Facebook login launcher per account (SOCKS5-only) with cookie/session capture
  - [x] Local SOCKS5 auth bridge fallback for Chromium compatibility (`pproxy`)
  - [x] Account-scoped browser session launcher for manual browsing from configured scraper accounts
  - [x] Scraper rotation-start selector (`Set Rotation Start`)
  - [x] Account-only scraper execution with per-run round-robin over eligible ACTIVE+SOCKS5 accounts (`last_scraper_account_id`)
  - [x] Proxy CRUD + account assignment visibility
  - [x] Proxy API import sync (bulk fetch and upsert)
  - [x] Scraper-tab proxy API fetch action using saved API settings
  - [x] Scraper runtime settings (monitor interval, monitor jitter, account reuse window, query delay bounds, max queries/run)
- [x] Button reliability fixes for selection/focus handling
- [x] Double-click and button parity for opening listings
- [x] Fast listing triage filters (All/New/High-Profit/iPhone 14+)
- [x] Advanced listing filters in GUI:
  - [x] Model multi-select dropdown
  - [x] Profit range (`min` to `max`)
  - [x] Minimum listing price
- [x] Listings feed now hides `Unknown`/blank-model rows from operator view (records still retained in DB)
- [x] Integrated Price Sheet editor with row CRUD and CSV save
- [x] Price Sheet editor action label updated to `Apply Changes` for clearer operator intent
- [x] Price Sheet editor model upsert now de-duplicates by model name (case/spacing-insensitive) so add/edit stays clean
- [x] In-GUI listing financial recalculation trigger
- [x] Row color-marking via right-click context menu:
  - [x] `scam` (red)
  - [x] `interested` (yellow)
  - [x] `not_interested` (ash/grey)
  - [x] clear mark
- [x] Bulk selection support in listings table (drag-select range + Cmd/Ctrl multi-select) with right-click bulk mark/delete actions
- [x] Opened listing tracking (`opened_at`) with distinct row color to avoid reopening the same item
- [x] Live scraper runtime status with elapsed timer and query progress
- [x] In-app launch guide/help flow

### Additional Implemented (Audit-Detected, previously not in original roadmap)

- [x] Default notification mode constrained to profitable subset (configurable via `notify_profitable_only`)
- [x] Telegram listing notifications now support per-listing compact cards (title/model/price/profit/description/link)
- [x] Scraper monitor launch from GUI (`main.py monitor` subprocess)
- [x] CLI/monitor account-only execution with rotating ACTIVE+SOCKS5 account pool and proxy bridge support
- [x] `notify_profitable_only` runtime setting enforcement across both GUI and CLI paths
- [x] Monitor jitter control (`monitor_jitter_seconds`) enforced in CLI monitor scheduling
- [x] Account min-reuse control (`account_min_reuse_seconds`) enforced in GUI/CLI account rotation selection
- [x] Existing null-price listing backfill via recalculation flow using currency-aware title/description parsing
- [x] Runtime account health tracking (`failure_count`, `cooldown_until`) with automatic status transitions (`ACTIVE`/`COOLDOWN`/`NEEDS_LOGIN`) on scrape success/failure
- [x] Eligible account pool supports expired cooldown re-entry while excluding active cooldown windows
- [x] Proxy quick-import from pasted cURL/proxy lines (URI and `host:port[:user:pass]` forms)
- [x] Proxy supplier parser resilience for mixed payload schemas (`proxies`/`data`/`results`/`items`) and plaintext fallback
- [x] Account browse-session workflow persists latest cookies/user-agent after operator session close
- [x] Rotation reuse logic now uses dedicated `last_scrape_started_at` timestamps instead of generic `updated_at`
- [x] Settings window simplified to VPS-first single-tab workflow (`VPS Scrapers`) with route/proxy/profile management and VPS manual login action
- [x] Settings window adjusted to two-tab workflow per operator request: `Proxies` + `VPS Scrapers`
- [x] VPS route form now auto-suggests profile directories and targets active workers for rotation participation (no unique-worker-name requirement)
- [x] Added Scraper-menu `Worker Queries & Keywords` manager for per-worker query-shard edits and universal negative-keyword control
- [x] Worker Queries manager now operates at worker scope only (`worker`, `worker_2`, `worker_3`) and writes query shard updates across all routes/profiles under that worker
- [x] VPS `Manual Login (SOCKS5)` now auto-runs remote profile directory creation/bootstrap commands on click
- [x] VPS scraper panel now loads seeded existing profiles from server `worker_routes` (`worker/profile_1`, `worker_2/profile_2`)
- [x] Production API redeploy verified `GET /worker-routes` availability (resolved prior 404 surface from desktop GUI)
- [x] VPS manual login command now defaults to `google-chrome` and uses an auto-started `pproxy` auth bridge for credentialed SOCKS5 proxies (avoids Chrome proxy-auth URL limitation)
- [x] VPS manual login now auto-detects active VNC display/Xauthority (`Xtigervnc :1`) and launches directly from GUI command path (no manual placeholder command substitution)
- [x] VPS manual login output now reports proxy egress verification and profile `c_user` cookie presence before launch for login-capture diagnostics
- [x] VPS manual login preflight now does best-effort profile ownership/permission repair and treats cookie DB permission-denied checks as non-fatal
- [x] VPS manual login now auto-saves/upserts route definitions (with auto-suggested route name when blank) so new scraper profiles immediately appear in route list
- [x] VPS Scrapers tab wrapped in vertical scroll container to keep action buttons accessible on smaller screens
- [x] VPS route status rendering now distinguishes `ready` (worker healthy but on another route) from `unknown` (no worker heartbeat for that worker name)
- [x] Added `Send Telegram Test` action in VPS settings/monitor to validate Telegram delivery from VPS worker environment
- [x] VPS Activity Monitor now supports live worker log streaming (`Worker Logs (Live)`) with explicit stop action
- [x] Main Listings tab now shows live VPS worker health indicators (red/green dots) for `worker`, `worker_2`, and `worker_3`
- [x] Main Listings worker pills now include live runtime hints for each worker (`lease`, `cooldown`, `idle`) from `/worker-health`
- [x] VPS Scrapers route table/form now shows per-worker lease state and cooldown/waiting state without requiring log monitoring
- [x] Added rolling `listings_scraped_last_minute` worker metric (actual scraped `found` volume, not `new_saved`) in API + worker runtime + GUI worker status tables
- [x] Proxies tab now shows server proxy-health data (`Failures`, `Banned`, `Ban Until`) from API `proxy_stats`
- [x] Proxies tab now includes per-selected-proxy unban/reset action (`Reset Selected Proxy Ban`)
- [x] VPS route-table rendering now tolerates duplicate route-key collisions so one dirty row cannot hide the rest of the table
- [x] Fixed GUI worker health/route rendering crash on non-standard lease endpoints (`query-shard://...`) so all routes and worker pills remain visible/updating.
- [x] Enforced hard one-proxy-per-session semantics with active lease keepalive to prevent mid-session proxy lease expiry/sharing.
- [x] Added sticky proxy-profile pairing (`preferred_proxy_key`) so each route prefers its last successful proxy and rotates only on unavailable/banned/unhealthy conditions.
- [x] Strengthened global no-sharing rule: no two profiles can hold the same proxy lease simultaneously.
- [x] Rolled out sticky/lease enforcement to VPS (`api`, `worker`, `worker_2`, `worker_3`) and verified live health/routes post-restart.
- [x] Fixed API route upsert regression causing manual-login route save failures (`PUT /worker-routes/...` 500 after sticky-field schema expansion).
- [x] Improved GUI VPS route-save diagnostics to surface HTTP status and API `detail` text for operator triage.
- [x] Fixed worker cooldown-threshold enforcement to honor configured `WORKER_ROUTE_COOLDOWN_BAD_CYCLES` (no silent clamp) and seed bad-cycle tracking from DB `consecutive_failures` after restart.
- [x] Added Dolphin Anty profile management tab in GUI Settings (`Connections & Scraper > Dolphin Profiles`):
  - [x] Treeview table with profile columns (ID, Name, Status, Browser, Tags, Memory)
  - [x] `Fetch Profiles` button queries Dolphin Anty local API and populates table
  - [x] `Start Selected` / `Stop Selected` buttons for profile lifecycle control
- [x] Added Dolphin Anty API Key integration:
  - [x] `dolphin_api_key` setting with masked entry field and instant save
  - [x] `_dolphin_auth_headers()` generates `Authorization: Bearer <key>` headers
  - [x] All Dolphin API methods inject Bearer token automatically
- [x] Added configurable Dolphin Anty API URL:
  - [x] `dolphin_api_url` setting with default to `http://localhost:3001`
  - [x] Profile fetching explicitly uses Cloud API (`https://anty-api.com`) to resolve VPS 401 auth issues
  - [x] Start/Stop operations use the local `dolphin_api_url` via automatic SSH tunnel (`_ensure_dolphin_ssh_tunnel`)

### Remaining

- [ ] Add explicit “Manual Review Queue” filter based on confidence/flags
- [ ] Extend bulk actions with export/status-update flows (multi-select mark/delete now implemented)
- [ ] Add column sorting controls and saved view presets

---

## Phase 4: Condition Intelligence Hardening (Priority)

### Why this phase

Condition accuracy is a critical decision parameter and currently constrained by card-level text only.

### Objectives

- Improve condition accuracy while minimizing Facebook account risk
- Introduce automation-safe confidence gating

### Planned Tasks

- [ ] Implement `condition_confidence` field (`high`, `medium`, `low`)
- [ ] Emit evidence signals used for classification (matched keywords/source)
- [ ] Add two-stage condition pipeline:
  - [ ] Stage A: card-level condition + confidence
  - [ ] Stage B: selective detail-page scrape for low-confidence/high-value listings
- [ ] Add budget controls for detail-page visits per run
- [ ] Add GUI filter for confidence tiers and “needs review”
- [ ] Route low-confidence listings to manual review actions only
- [ ] Add recalculation command to backfill confidence for existing rows

### Acceptance Criteria

- Condition false-positive/false-negative rate materially reduced in sampled audits
- Automation never auto-acts on `low` confidence listings
- Scrape intensity remains under configured risk thresholds

---

## Phase 5: Reliability, Safety, and Scale

### Goals

- Improve system reliability and operational safety
- Reduce account-ban exposure under sustained runs

### Completed (This Update)

- [x] Added worker runtime-compensated pacing (`sleep = max(0, interval - elapsed)`) with configurable jitter (`SCRAPE_INTERVAL_JITTER_PCT`).
- [x] Added worker-level backoff scaling from eligible/enabled route ratio (capped multiplier) to protect surviving routes during low-capacity windows.
- [x] Added structured JSON telemetry per worker cycle (`cycle_id`, `duration_ms`, `outcome`, `error_category`, `retry_count`).
- [x] Added proxy reliability tracking (`proxy_stats`) with ban windows and health-weighted proxy candidate ordering.
- [x] Added explicit route state machine (`ENABLED`, `DEGRADED`, `THROTTLED`, `COOLDOWN`, `NEEDS_LOGIN`, `DISABLED`) with state-driven interval multipliers.
- [x] Added pre-checkpoint soft-signal detection with action routing (`continue`, `throttle`, `pause`, `quarantine`) and route baselines (`avg_result_count`, `avg_page_load_ms`).
- [x] Added cross-worker query-shard lock deduplication (`WAIT_QUERY_SHARD`) so overlapping shards are deferred, not failed.
- [x] Added minimum-route resilience controls: API warnings for `< WORKER_MIN_ENABLED_ROUTES_WARN` enabled routes and mandatory extended rest multiplier when only one route is enabled.
- [x] Added manual-login quarantine operations metadata (`quarantined_at`, `quarantine_reason`, `quarantine_evidence`) across worker/API schema and payloads.
- [x] Added quarantine recovery actions in API + GUI (`retest` per route, worker-level bulk clear for manual-login locks).
- [x] Added manual-login Telegram alert deduplication by route+reason window (`WORKER_MANUAL_LOGIN_ALERT_DEDUP_SECONDS`).
- [x] Added per-cycle proxy-binding validation controls (`VERIFY_PROXY_IP`, `PROXY_IP_CHECK_URL`) and non-failure `WAIT_PROXY_MISMATCH` handling.
- [x] Added per-cycle browser persona variation with coherent context options (`ENABLE_FINGERPRINT_VARIATION`) covering viewport/screen/scale factor, UA, timezone, locale, and color scheme.
- [x] Added persona telemetry context (`persona_hash`, `persona`) for cycle-level diagnostics and incident evidence payloads.
- [x] Added proxy-stats operator API (`GET /proxy-stats`, `POST /proxy-stats/reset`) for ban-state visibility and targeted reset.
- [x] Added telemetry payload-shape coverage and extracted payload builder (`server/services/worker/telemetry.py`, `server/tests/test_telemetry.py`).
- [x] Added telemetry rollup utility (`server/scripts/telemetry_rollup.py`) for route-level success/wait/fail trend summaries.
- [x] Refactored worker internals into focused modules (`server/services/worker/lease_manager.py`, `server/services/worker/route_transitions.py`, `server/services/worker/scheduler.py`, `server/services/worker/persona.py`) while preserving behavior.
- [x] Consolidated API/worker schema bootstrapping into shared ensure path (`server/services/common/schema_ensure.py`) and retained SQL bootstrap parity with `server/services/api/sql/001_init.sql`.
- [x] Added unit tests for scheduling/outcome helper behavior (`server/tests/test_worker_runtime.py`).
- [x] Added signal-detector unit tests (`server/tests/test_signal_detector.py`).
- [x] Added runtime tests for proxy-mismatch classification/wait behavior (`server/tests/test_worker_runtime.py`).
- [x] Added persona generation tests (`server/tests/test_persona.py`).
- [x] Added stealth script fingerprint randomization to strengthen anti-detection against platform bans.
- [x] Added enhanced soft-signals (`CONSECUTIVE_EMPTY_RESULTS`, `SESSION_TOO_LONG`) to the signal detector for safer proactive pausing.
- [x] Added quiet hours scheduling (`WORKER_QUIET_HOURS_UTC`, `WORKER_QUIET_HOURS_MULTIPLIER`) to slow scraping during off-peak UTC windows (midnight-wrapping supported).
- [x] Added session duration caps (`WORKER_SESSION_MAX_QUERIES`) to restart browser sessions and avoid session-length fingerprinting.
- [x] Added weighted bucket query diversification (`_get_bucket_queries`) with four categories (broad 40%, exact 30%, flipper 20%, misspelling 10%) for per-cycle query randomization.

### Remaining

- [ ] Add broader automated tests for pricing, condition rules, DB/API integration, and migrations.
- [ ] Extend pacing policies with explicit time windows and run-depth strategies.
- [ ] Add secure secret handling guidance and hardening checks.
- [ ] Add backup/restore path for DB and price sheet.

---

## Phase 5A: 24x7 Server and Real-Time Sync Migration (Approved Track)

### Goals

- Move scraping to always-on VPS/server runtime
- Deliver real-time listing writes and push-sync updates to desktop app
- Scale account concurrency safely with account/proxy isolation

### Implemented Baseline (This Update)

- [x] Split scraper runtime into long-lived worker service scaffolding deployable on VPS (`server/services/worker`)
- [x] Introduced centralized server database schema baseline (`server/services/api/sql/001_init.sql`)
- [x] Refactored ingest path from run-end batch commit to per-listing immediate persist (`scraper.py`)
- [x] Added event layer for `listing_created`/`listing_updated` fanout via Redis pub/sub (`worker.py` + `api/main.py`)
- [x] Added API service with:
  - [x] bootstrap listing snapshot endpoint (`GET /listings?since_id=...`)
  - [x] authenticated WebSocket channel (`/ws/listings`)
- [x] Added Docker Compose deployment baseline for Caddy + Postgres + Redis + API + multi-worker (`server/infra/docker-compose.yml`)
- [x] Added built-in `worker_3` fast-lane container profile (`SCRAPER_QUERIES_3=iPhone`, shorter interval, medium-depth overrides with target depth capped at 100)
- [x] Enforced newest-first query sort (`sortBy=creation_time_descend`) and minimum one scroll pass before target-depth early exit
- [x] Added profile-failure detection guard with configurable consecutive-cycle threshold before `degraded` state
- [x] Added zero-query scrape-cycle failure detection so empty/inactive route cycles are counted as bad cycles (eligible for degraded/cooldown/alerts)
- [x] Added Telegram profile-failure alert path (worker/profile/route context + cooldown) for degraded routes
- [x] Normalized worker monitor service parsing (comma/space-safe) and enforced required monitor set (`worker`, `worker_2`, `worker_3`) for GUI status/log actions
- [x] Added operator deployment/runbook and env template (`server/README.md`, `server/.env.example`)
- [x] Hardened Docker build context excludes (`.dockerignore`) to prevent runtime/data permission failures during VPS worker image builds
- [x] Removed worker-side concurrent trigger/function bootstrap DDL to eliminate startup races (`tuple concurrently updated` / duplicate trigger errors)
- [x] Added worker retry/cooldown policy: retry transient failures, cooldown repeated bad routes, and rotate automatically to next route/profile
- [x] Added VPS route proxy modes (`fixed`/`auto_rotation`) with optional SOCKS5 proxy pool payload
- [x] Added GUI TXT/CSV import for SOCKS5 proxy onboarding and VPS route proxy-pool import
- [x] Added one-command VPS deploy script (`server/scripts/deploy_vps.sh`) with rsync, compose rebuild, migration bootstrap, and health checks
- [x] Fixed VPS deploy sync coverage in `server/scripts/deploy_vps.sh` so worker runtime root files (`scraper.py`, `notifications.py`, `requirements.txt`, `.dockerignore`) are deployed alongside `/server`
- [x] Applied VPS host hardening baseline (SSH key-only auth, UFW allowlist for `22/80/443`, fail2ban `sshd` jail)
- [x] Validated profile-failure Telegram alert path through controlled `worker_3/profile_3` failure injection and recovery
- [x] Added global proxy lease control (`worker_proxy_leases`) to prevent concurrent same-proxy usage across profiles/routes
- [x] Added auto-rotation proxy reuse cooldown (`WORKER_PROXY_REUSE_COOLDOWN_SECONDS`) and lease window (`WORKER_PROXY_LEASE_SECONDS`)
- [x] Updated GUI VPS route save behavior: `proxy_mode=auto_rotation` no longer requires proxy profile selection; pool-driven random seed proxy is used
- [x] Updated GUI VPS route save behavior: worker name can be left blank and is auto-assigned to running/least-loaded worker
- [x] Worker no longer silently falls back to `env_default` when DB routes exist but are temporarily unavailable (cooldown/proxy lock)
- [x] Added checkpoint/challenge detection path in scraper runtime; worker now auto-quarantines impacted routes as `manual_login_required` and excludes them from rotation
- [x] Hardened checkpoint handling so scraper stops the cycle immediately once `MANUAL_LOGIN_REQUIRED` is detected (no full-query-loop churn under checkpoint state)
- [x] Added Telegram alert path for manual-login-required quarantine events (worker/profile/route context)
- [x] Added VPS Scrapers connection controls for default proxy rotation period and profile rotation period, with server `.env` apply + worker service restart
- [x] Added VPS Scrapers top-level runtime controls to start/stop worker services from GUI
- [x] Added per-route interval override control in GUI and surfaced route interval in the VPS route table
- [x] Extended worker route/API schema with manual-login fields (`manual_login_required`, `manual_login_reason`, `manual_login_required_at`)
- [x] Replaced single profile-rotation interval control with per-worker scrape-frequency controls in VPS Scrapers (`worker`, `worker_2`, `worker_3`)
- [x] Added worker_2-specific compose/env interval key support (`SCRAPE_INTERVAL_SECONDS_WORKER_2`) while keeping worker_1/worker_3 interval keys
- [x] Replaced in-memory round-robin route cursor with DB-backed due-time scheduling (`worker_routes.next_run_at`, optional `route_interval_seconds`) to avoid restart hotspotting and deterministic cadence.
- [x] Added first-class WAIT outcomes for capacity blockers (`WAIT_PROXY`, `WAIT_PROFILE_LOCK`) so no-proxy/profile-lock contention does not increment failure/cooldown counters.
- [x] Added first-class `WAIT_QUERY_SHARD` and `WAIT_SIGNAL_PAUSE` outcomes to avoid misclassifying shard contention or proactive risk pauses as hard failures.
- [x] Added error taxonomy + category-aware retry/backoff (transient categories retry exponentially; deterministic categories fail fast).
- [x] Updated proxy lease release semantics so `last_used_at` is stamped on release (not acquisition), keeping TTL as crash safety.
- [x] Added profile runtime lock guard using `worker_proxy_leases` (`PROFILE:` keys) and API route-save validation for unique enabled `user_data_dir`.
- [x] Added scheduler/lock/cadence env controls in template (`SCRAPE_INTERVAL_JITTER_PCT`, `WORKER_WAIT_BACKOFF_SECONDS`, `WORKER_MAX_BACKOFF_MULTIPLIER`, `WORKER_CYCLE_RETRY_BACKOFF_MAX_SECONDS`).
- [x] Added proxy health-scoring persistence (`proxy_stats`) with exponential ban windows and healthy-candidate ordering.
- [x] Added route state machine + interval multipliers (`DEGRADED=2x`, `THROTTLED=4x`) with persisted status metadata (`status`, `status_reason`, `status_since`).
- [x] Added pre-checkpoint soft-signal detector with EMA baselines (`avg_result_count`, `avg_page_load_ms`) and proactive throttle/pause/quarantine actions.
- [x] Added min-route resilience enforcement (API warnings for `<2` enabled routes and worker-side single-route rest multiplier).
- [x] Migrated worker browser launch to Dolphin Anty CDP integration:
  - [x] `core/scraper/driver.py` connects to Dolphin Anty-managed Chromium via `connect_over_cdp` WebSocket
  - [x] Dynamic auto-allocation of profiles per worker via `worker_proxy_leases` Postgres lock (bypasses static env vars)
  - [x] Cloud-first API fallback for fetching available profiles to avoid local session lockouts
  - [x] `DOLPHIN_API_URL` and `DOLPHIN_ANTY_TOKEN` env vars in compose for container→host API connectivity and authentication
  - [x] proxy management delegated to Dolphin Anty's native per-profile proxy bindings
  - [x] `DOLPHIN_WS_HOST` env var (default `host.docker.internal`) for Docker→host WebSocket connectivity
  - [x] Duplicate-running profile reuse (`E_BROWSER_RUN_DUPLICATE`) with session port query and stop+restart fallback
  - [x] Three-tier profile listing: Cloud API → Local API w/auth → Local API fallback
- [x] Updated VPS deployment target from `root@147.182.131.111` to `ubuntu@15.235.185.32` with new remote directory structure
- [x] Added Caddy reverse proxy service in Docker Compose (`caddy:2`) with auto-TLS via `SERVER_DOMAIN` env var, persistent cert volumes, and API health-check dependency
- [x] ~~Added 5-slot concurrent worker architecture~~ — **removed**: reverted to single-loop-per-container; parallelism via Docker worker containers instead
  - ~~`_run_worker_loop()` launches 5 parallel `_worker_slot_loop()` tasks per worker container~~
  - ~~Route selection serialized via `_route_selection_lock` to prevent duplicate claims~~
  - ~~Slot-0 exclusive idle heartbeat updates to avoid spam~~
  - ~~5x effective route throughput per Docker service without additional compose definitions~~
- [x] Wired proxy provider health monitor into worker slot loop:
  - [x] Cycle-level saturated check skips scraping with `wait_proxy_provider` heartbeat
  - [x] Pacing multiplier applied to `next_interval_seconds` when provider under load
- [x] Fixed worker container connectivity (`ECONNREFUSED`) to Dolphin Anty by migrating worker compose services to `network_mode: host`.
- [x] Fixed Dolphin profile collision lock leaks by explicitly tracking and releasing both the database lease (`profile_lock_id`) and the remote Dolphin API lock (`dolphin_lock_id`).
- [x] Hardened `_start_dolphin_profile` against transient `E_BROWSER_RUN_DUPLICATE` errors by issuing explicit stop commands and executing exponential backoff retries.
- [x] Added Per-Profile Health Tracking and automation safeguards:
  - [x] Distributed PostgreSQL-backed tracking (`proxy_stats` table) of consecutive failures targeted at the mapped Dolphin Profile ID (`PROFILE:{id}`).
  - [x] Automatic profile blacklisting with exponential backoff (starting at 30 minutes, max 24 hours) after 2 consecutive failures (`DOLPHIN_PROFILE_BLACKLIST_AFTER`).
  - [x] Safe bypass during profile selection; all workers query the DB to skip blacklisted profiles, preventing cross-worker retry loops.
  - [x] Sent Telegram alerts immediately upon blacklisting to notify operators of blocked or broken profiles (includes profile name and ID).

### Remaining Tasks

- [ ] Update desktop GUI sync model:
  - [x] initial snapshot load from server (`GET /listings?since_id=...`)
  - [x] reconnect + missed-event catch-up by persisted watermark cursor (`server_sync_since_id`)
  - [x] near-real-time incremental refresh via background polling cursor worker
  - [x] apply live row updates from WebSocket
- [ ] Add worker orchestration policy for parallel account execution with full query-shard coordination controls
  - [x] cooldown-aware retries and WAIT outcomes are now implemented
  - [x] query-shard lock/deduplication is now implemented (`WAIT_QUERY_SHARD`)
- [ ] Add GUI-driven server worker management:
  - [x] manage remote worker route definitions (create/update/remove)
  - [x] assign profile/proxy/query-shard per worker route
  - [ ] trigger safe deploy/restart and health/status checks from GUI
- [x] Add server-side worker route registry + heartbeat APIs (`worker_routes`, `worker_heartbeats`) and due-time route scheduling (`next_run_at`)
- [x] Add worker-side Telegram alerts for newly created listings with non-negative profit threshold (`TELEGRAM_NOTIFY_MIN_PROFIT`, default `0`) and suppress blank/`Unknown` model notifications
- [ ] Add production operations controls (public TLS reverse proxy, health dashboards, restart supervision, structured logs, metrics)
  - [x] add GUI-accessible VPS activity monitor actions (API health, worker status/log tail, listing count via SSH)
  - [x] production TLS reverse proxy via Caddy on `443` (domain `api.iphoneguy.com.au`)
  - [x] host-level SSH/UFW/fail2ban hardening baseline applied on VPS
  - [x] structured per-cycle worker telemetry logs are now emitted in JSON
- [x] Add notification worker consuming server event stream (decoupled from GUI/CLI run completion)
  - [x] standalone `notification_worker.py` subscribes to Redis `listing_events` pub/sub
  - [x] priority-tier dispatch: instant (≥ $50 profit), fast-batch (≥ $0), suppressed (< $0)
  - [x] Telegram + Firebase Cloud Messaging (FCM) push delivery
  - [x] rate limiting (`NOTIFY_MAX_PER_MINUTE`) and deduplication (`NOTIFY_DEDUP_WINDOW_SECONDS`)
  - [ ] wire into Docker Compose as production container service
- [x] Add proxy provider real-time health monitor (`proxy_monitor.py`)
  - [x] async polling of proxy gateway utilization API
  - [x] pacing multiplier computation based on utilization/error-rate thresholds
  - [x] Telegram degradation alerts with cooldown deduplication
  - [x] pacing multiplier wired into worker scrape-interval adjustment flow (per-cycle `next_interval_seconds` scaling + saturated cycle skipping)

### Operator Dependencies (User-Side Prerequisites)

- [x] Provision VPS (recommended 4 vCPU / 8 GB RAM / 80+ GB disk) and secure SSH-only access
- [x] Provision domain + TLS for API/WebSocket endpoint
- [ ] Provision server database and provide credentials/secrets securely
- [x] Ensure Dolphin Anty desktop application is running (locally or on VPS) to allow API connections
- [x] Configure headless autostart for Dolphin Anty and TigerVNC (`systemd`) on server reboots
- [x] Validate each FB account + SOCKS5 proxy pair on server-hosted browser profiles (using Dolphin Anty profiles via CDP)
- [x] Define concurrency and pacing limits (workers/account, query cap, delay floor/ceiling)

### Acceptance Criteria

- New listings are inserted without waiting for entire scrape session completion
- Desktop listing view updates within seconds of server-side ingest events
- Server scraper processes run continuously with automatic restart on failure
- Multi-account workers increase throughput while respecting cooldown/rate-limit controls

---

## Phase 6: Strategic Expansion

### Candidate Enhancements (from baseline plan + audit)

- [ ] Make.com integration for orchestrated workflows
- [ ] Price trend and market analysis dashboards
- [ ] Inventory lifecycle management after purchase
- [ ] Mobile-first alerting and action handoff
- [ ] ML-based condition/profit prediction model

---

## V3.0 Upgrade Track: Latency and Responsiveness

### Goals

- Improve end-to-end alert latency without increasing per-profile request intensity
- Add safe rollout controls and measurement before changing the current event path
- Execute the upgrade strictly in approved phases

### Explicit Non-Goals

- No separate raw HTTP scraper driver as the primary discovery path
- No token extraction/replay architecture outside the browser as the main runtime mode
- No proxy/fingerprint escalation intended to preserve large-scale account automation
- No profile role-segregation specifically for anti-spam evasion
- No notification-before-persistence flow

### Phase 0 Completed (This Update)

- [x] Added shared Redis-backed feature-flag registry (`flipper:flags`) with all V3.0 upgrade-path flags defaulting to `false`:
  - [x] `ENABLE_REDIS_STREAM_EVENTS`
  - [x] `ENABLE_NOTIFICATION_CONSUMER`
  - [x] `ENABLE_GUI_WEBSOCKET_PUSH`
  - [x] `ENABLE_PRIORITY_SCHEDULER`
  - [x] `ENABLE_ROUTE_LANES`
- [x] Added safe feature-flag cache/fallback behavior (`server/services/common/feature_flags.py`):
  - [x] async Redis reads
  - [x] ~1s in-process cache
  - [x] safe fallback to defaults on Redis failure
  - [x] startup flag snapshot logging in API, worker, and notification worker
- [x] Added shared JSON observability helper layer (`server/services/common/observability.py`) with timestamp/latency utilities and JSON-only log emission
- [x] Added worker-side listing-path observability without changing pub/sub payload contracts:
  - [x] `listing_seen_ts`
  - [x] `listing_persisted_ts`
  - [x] `listing_event_published_ts`
  - [x] inline Telegram `notification_sent_ts` / delivery status
  - [x] join keys via `listing_id` + worker/route context
- [x] Extended cycle telemetry with:
  - [x] listings parsed count
  - [x] Postgres upsert latency aggregates
  - [x] Redis publish latency aggregates
  - [x] inline notification delivery latency aggregates
  - [x] end-to-end alert latency aggregates
- [x] Added API-side observability for WebSocket broadcast latency and `gui_pushed_ts`
- [x] Added GUI poll-sync observability for per-listing `gui_rendered_ts` with source marker `poll_sync`
- [x] Added targeted tests for feature flags, telemetry, worker observability, API observability, and existing notification worker coverage
- [x] Fresh 2026-03-08 code audit found no additional undocumented features/process enhancements beyond the audit sections already captured elsewhere in this roadmap and `dev-log.md`

### Phase 1 Completed (This Update)

- [x] Added shared Redis Streams schema/codec module (`server/services/common/stream_events.py`) for `stream:listings`
- [x] Implemented durable post-persistence stream dual-write behind `ENABLE_REDIS_STREAM_EVENTS`
- [x] Kept Postgres as source of truth and preserved existing Redis pub/sub + inline Telegram behavior
- [x] Added meaningful-change suppression for unchanged updates using bounded hot-path comparison fields:
  - [x] `title`
  - [x] `price`
  - [x] `url`
  - [x] `model`
  - [x] `condition`
  - [x] `status`
  - [x] `max_buy_price`
  - [x] `potential_profit`
  - [x] `location`
- [x] Added non-fatal stream publish handling with structured failure logs and cycle telemetry for:
  - [x] stream publish latency sum/count
  - [x] stream publish failure count
  - [x] `stream_event_id` / returned Redis stream IDs in observability
- [x] Used capped retention with `XADD ... MAXLEN ~ 10000`
- [x] Kept `notification_worker.py` dormant and unchanged:
  - [x] no compose wiring in this phase
  - [x] no consumer groups / `XREADGROUP`
  - [x] no notification ledger / replay logic yet
  - [x] no pub/sub payload-shape changes for the standalone notifier
- [x] Added targeted tests for stream schema serialization, meaningful-change detection, worker gating/fallback behavior, and `XADD` call shape
- [x] Fresh 2026-03-09 code audit found no newly undocumented features/process enhancements beyond the existing audit sections; only the approved Phase 1 event-spine changes required documentation updates

### Phase 2 Completed (Previous Update)

- [x] Converted `notification_worker.py` from Redis pub/sub to a Redis Streams consumer-group service over `stream:listings`
- [x] Added consumer-group bootstrap with `XGROUP CREATE ... MKSTREAM` and `BUSYGROUP` handling
- [x] Added restart-safe unread/pending recovery with `XREADGROUP` + `XAUTOCLAIM`
- [x] Added durable PostgreSQL notification ledger (`notification_delivery_ledger`) keyed by `listing_id`
- [x] Replaced in-memory dedupe with durable ledger-backed dedupe so duplicate stream deliveries and consumer restarts do not resend already-sent listings
- [x] Moved notification delivery off the worker hot path when `ENABLE_NOTIFICATION_CONSUMER=1`
- [x] Added worker-side exact-once hardening for same-listing persistence by serializing `listing_id` upsert decisions with a PostgreSQL advisory lock
- [x] Added delegated-notification worker logs so stream-published created listings can be handed off without blocking on external Telegram I/O
- [x] Added immediate-send pacing of at most one Telegram notification per second plus transient retry/backoff handling
- [x] Added compose wiring and deploy-path support for the dedicated `notification_worker` service
- [x] Added targeted tests for:
  - [x] consumer-group bootstrap
  - [x] stream ack flow
  - [x] `XAUTOCLAIM` recovery path
  - [x] durable ledger dedupe
  - [x] retry/backoff behavior
  - [x] one-per-second pacing
  - [x] worker delegation when the consumer flag is enabled
  - [x] concurrent duplicate processing producing one stream publish in the happy path
- [x] Fresh 2026-03-09 code audit found no newly undocumented features/process enhancements beyond the approved Phase 2 work and the previously recorded audit sections

### Phase 3 Completed (This Update)

- [x] Upgraded `/ws/listings` from a heartbeat-only stub to a normalized live listing feed behind `ENABLE_GUI_WEBSOCKET_PUSH`
- [x] Replaced raw websocket client tracking with an API connection manager that serializes sends per socket and tracks per-client heartbeat state
- [x] Added API-side application heartbeat handling:
  - [x] server sends `ping`
  - [x] desktop replies with `pong`
  - [x] dead clients are timed out and removed
  - [x] flag disable events close active clients with `websocket_disabled`
- [x] Normalized live push payloads to match `/listings` item shape, including `seq_id` cursor metadata
- [x] Added desktop WebSocket-first sync engine with:
  - [x] background thread + asyncio loop
  - [x] `aiohttp` websocket/HTTP client
  - [x] websocket-first connect order
  - [x] replay-window catch-up from `max(0, since_id - 100)`
  - [x] buffered live-event drain after poll catch-up
  - [x] exponential reconnect with jitter
  - [x] immediate polling fallback on disconnect/disable/auth/transport errors
- [x] Preserved poll path as the canonical fallback when WebSocket is disabled or disconnected
- [x] Added GUI duplicate protection by cursor gating plus existing SQLite upsert idempotency
- [x] Preserved GUI local filters and selection/focus state across sync-driven refreshes
- [x] Hardened API websocket snapshots to use JSON-safe normalized listing rows so live push does not fail on Postgres `NUMERIC`/`Decimal` fields
- [x] Hardened desktop poll fallback so transient REST failures during API restarts do not kill reconnect/backfill attempts
- [x] Added targeted tests for:
  - [x] normalized websocket snapshot push
  - [x] websocket disabled control frame
  - [x] ping/pong heartbeat + timeout cleanup
  - [x] reconnect replay overlap without duplicate apply
  - [x] poll fallback when live push is unavailable
  - [x] poll fallback retry after transient poll error
  - [x] tree selection/focus preservation
- [x] Fresh 2026-03-09 code audit found no newly undocumented features/process enhancements beyond the approved Phase 3 work and the existing audit sections

### Phase 4 Completed (This Update)

- [x] Added dedicated scheduler/lane helpers:
  - [x] `server/services/worker/scheduler.py` for route scoring, lane assignment, route selection, query ranking, and lane interval multipliers
  - [x] `server/services/common/route_lanes.py` for shared lane validation/normalization used by worker + API
- [x] Added persisted Phase 4 route state on `worker_routes`:
  - [x] `lane_override`
  - [x] `computed_lane`
  - [x] `effective_lane`
  - [x] `priority_score`
  - [x] `priority_score_updated_at`
  - [x] `profitable_hit_rate`
  - [x] `recent_duplicate_ratio`
- [x] Added auto lane/score calculation from bounded existing signals:
  - [x] operator `priority`
  - [x] route due-age / revisit-age
  - [x] `avg_result_count`
  - [x] profitable-hit rate
  - [x] duplicate ratio
  - [x] failure/status penalties
- [x] Added operator pin override semantics:
  - [x] auto lanes default to computed `hot` / `warm` / `sweep`
  - [x] `lane_override` pins effective lane when set
  - [x] only `lane_override` is operator-editable; computed/effective lane and score remain server-derived
- [x] Added Redis per-query locking for priority mode:
  - [x] normalized lock key namespace `query-lock:<sha1>`
  - [x] `SET NX EX` short-TTL acquisition
  - [x] explicit release on cycle exit
  - [x] fallback to next candidate query when the top choice is already locked
  - [x] non-fatal scheduler skip when all candidate queries are locked
- [x] Preserved flags-off compatibility:
  - [x] DB routes still use the existing due-time + priority ordering when `ENABLE_PRIORITY_SCHEDULER=0`
  - [x] env-fallback routes remain on the legacy path
  - [x] existing cooldown / throttle / quiet-hours / single-route-rest / browser reuse semantics remain intact
- [x] Extended worker telemetry and structured logs with scheduler observability:
  - [x] `lane_override`, `computed_lane`, `effective_lane`
  - [x] `priority_score` + compact score components
  - [x] selected query + query lock key/status
  - [x] route due-age / revisit-age
  - [x] profitable/duplicate counts feeding route metrics
- [x] Extended API + GUI operator surfaces:
  - [x] `/worker-routes` now returns lane/score fields
  - [x] route update validation only permits `lane_override` edits
  - [x] GUI VPS Scrapers route table/editor now shows lane + score and supports `auto/hot/warm/sweep` pinning
  - [x] GUI VPS Scrapers route table now falls back to live `/worker-health` rows when no DB-backed routes exist, so env-backed workers still render as active even though Phase 4 lane/score fields are unavailable for them
- [x] Added targeted tests for:
  - [x] deterministic score/lane calculation and hot-over-sweep route choice
  - [x] lane-override precedence
  - [x] Redis query lock acquisition / fallback / scheduler skip
  - [x] route API lane validation + omitted-vs-null payload handling
  - [x] telemetry field coverage for Phase 4 scheduler data
- [x] Fresh 2026-03-10 code audit found no newly undocumented code-level features/process enhancements beyond the approved Phase 4 work and the earlier audit sections

### Phase 5 Completed (This Update)

- [x] Added shared Phase 5 enrichment contracts in `server/services/common/enrichment_events.py`
- [x] Added `ENABLE_BACKGROUND_ENRICHMENT` to the shared Redis-backed feature-flag registry
- [x] Extended authoritative listing storage with:
  - [x] nullable `thumbnail_url`
  - [x] `enrichment_status`
  - [x] `enrichment_source_hash`
  - [x] `enriched_at`
  - [x] `enrichment_last_error`
- [x] Added durable enrichment infrastructure:
  - [x] Redis Stream `stream:listing_enrichment`
  - [x] capped retention via `XADD ... MAXLEN ~ 10000`
  - [x] PostgreSQL `listing_enrichment_ledger`
  - [x] dedicated `enrichment_worker` compose service with consumer-group bootstrap, `XREADGROUP`, and `XAUTOCLAIM`
- [x] Slimmed the worker hot path when background enrichment is enabled:
  - [x] decision-ready first-alert fields stay on the initial persist path
  - [x] cold fields (`description`, `seller_name`, thumbnail backfill) are deferred behind the enrichment stream
  - [x] enrichment enqueue is suppressed for unchanged duplicate cold snapshots using `enrichment_source_hash`
- [x] Kept public listing contracts backward compatible:
  - [x] `/listings` and WebSocket snapshots now add only nullable `thumbnail_url`
  - [x] desktop sync keeps idempotent upserts and now accepts same-cursor websocket enrichment updates when `updated_at` is newer or cold fields fill in
- [x] Preserved decision-ready notifications:
  - [x] title
  - [x] price
  - [x] projected profit
  - [x] URL
  - [x] text-only Telegram formatting with no thumbnail/media dependency
- [x] Added Phase 5 observability:
  - [x] `listing_enrichment_enqueued`
  - [x] `listing_enrichment_started`
  - [x] `listing_enrichment_completed`
  - [x] `listing_enrichment_failed`
  - [x] cycle telemetry counters for enrichment enqueue latency/failure
- [x] Added targeted tests for:
  - [x] hot/cold field partitioning and enrichment event schema
  - [x] thumbnail normalization and source-hash dedupe
  - [x] worker enqueue gating and flags-off compatibility
  - [x] enrichment consumer bootstrap/ack/retry/duplicate suppression
  - [x] desktop same-cursor enrichment merge
  - [x] API payload compatibility with nullable `thumbnail_url`
- [x] Fresh 2026-03-10 code audit found no additional undocumented code-level features/process enhancements beyond the approved Phase 5 work and the earlier audit sections

### Phase 6 Completed (This Update)

- [x] Added shared Phase 6 runtime-ops/config contracts:
  - [x] Redis runtime-config hash `flipper:runtime_config`
  - [x] typed runtime-config helper with cached reads and authenticated write support
  - [x] operator-adjustable lane thresholds for Phase 4 scheduler scoring
  - [x] `NOTIFICATION_CONSUMER_DRAIN` drain control for the notification consumer
- [x] Added authenticated operator API endpoints:
  - [x] `GET /ops/realtime-health`
  - [x] `GET /ops/stream-backlog`
  - [x] `GET /ops/runtime-config`
  - [x] `PUT /ops/runtime-config`
  - [x] `GET /ops/notification-dead-letter`
  - [x] `POST /ops/replay/notifications`
- [x] Added notification dead-letter + replay tooling:
  - [x] Redis Stream `stream:notification_dead_letter`
  - [x] shared dead-letter codec carrying original listing-stream payload + failure metadata
  - [x] terminal delivery failures now write a dead-letter entry, mark the ledger `failed_terminal`, and `XACK` the original listing stream entry
  - [x] bounded replay tooling resets `failed_terminal` ledger rows and republishes only dead-lettered notification events back onto `stream:listings`
- [x] Added backlog/health visibility:
  - [x] worker publish-health fields persisted on `worker_heartbeats`
  - [x] Redis backlog inspection for `stream:listings`, `stream:listing_enrichment`, and `stream:notification_dead_letter`
  - [x] notification consumer lag/pending summary and websocket connection count exposed through `GET /ops/realtime-health`
- [x] Added operational observability:
  - [x] `runtime_config_snapshot`
  - [x] `runtime_config_updated`
  - [x] `notification_consumer_drain_state`
  - [x] `notification_dead_letter_written`
  - [x] `notification_dead_letter_failed`
  - [x] `notification_replay_requested`
  - [x] `notification_replay_completed`
  - [x] `stream_backlog_inspected`
  - [x] `realtime_health_inspected`
- [x] Deployment/runtime hardening:
  - [x] compose `stop_grace_period` added for `notification_worker` and `enrichment_worker`
  - [x] existing Redis-backed feature flags remain the authoritative rollback switches for live push, notification consumer, priority scheduler, route lanes, and background enrichment
- [x] Added targeted tests for:
  - [x] runtime-config parsing/cache/update behavior
  - [x] dead-letter codec round-trip
  - [x] notification consumer dead-letter, drain-mode, and replay-fallback behavior
  - [x] API runtime-config mutation, backlog inspection, realtime health, and replay
  - [x] scheduler lane-threshold override behavior
  - [x] worker publish-health persistence/failure recording
- [x] Fresh 2026-03-10 code audit found no newly undocumented code-level features/process enhancements beyond the approved Phase 6 work and the earlier audit sections

### Planned Next Phases

- [x] Phase 5: hot-path payload slimming and background enrichment for non-critical fields
- [x] Phase 6: reliability/replay/operator controls (health endpoints, backlog visibility, replay tooling, live push rollback switches)
- [x] Phase 4 alignment addendum: central route ownership, execution-profile-gated workers, exploration reserve, and text-only Telegram notifications

### Phase 4 Alignment Addendum Completed (This Update)

- [x] Added central scheduler ownership model:
  - [x] authoritative `central_routes`
  - [x] canonical `route_queries`
  - [x] local `execution_profiles`
  - [x] rollout gate `ENABLE_CENTRAL_ROUTE_DISPATCH`
- [x] Migrated worker-owned scheduler state into the central model:
  - [x] worker routes backfill into globally unique central routes
  - [x] CSV query shards backfill into canonical query rows
  - [x] legacy origin retained as `legacy_worker_name` / `legacy_route_name`
  - [x] execution profiles backfilled from route config and live worker heartbeat snapshots
- [x] Replaced worker-owned dispatch with profile-gated central dispatch when enabled:
  - [x] workers lease one eligible local execution profile first
  - [x] scheduler selects the best due central route globally
  - [x] route query ranking uses canonical `route_queries`
  - [x] Redis per-query locking remains the duplicate-prevention guard
- [x] Added explicit anti-starvation:
  - [x] runtime-configured exploration reserve `CENTRAL_ROUTE_EXPLORATION_EVERY_N`
  - [x] every Nth eligible dispatch prefers due non-hot work when available
  - [x] hot work still falls back normally when no due non-hot route exists
- [x] Switched operator surfaces to central-route management:
  - [x] new central route APIs: `GET/PUT/DELETE /routes`, `PUT /routes/{route_name}/queries`
  - [x] execution-profile visibility: `GET /execution-profiles`
  - [x] GUI VPS Scrapers now treats workers as health/capacity views and routes as the editable scheduler objects
  - [x] GUI query manager now edits canonical central-route query sets instead of worker-owned query shards
- [x] Preserved legacy worker-route APIs only as migration/audit helpers; they are no longer the primary write path
- [x] Kept Telegram notifications text-only:
  - [x] thumbnail line removed from notification builders
  - [x] web-page previews disabled
  - [x] `thumbnail_url` remains stored/synced for listings but does not affect alert formatting or latency
- [x] Added targeted regression coverage for:
  - [x] central route API serialization
  - [x] execution-profile API visibility
  - [x] central-route GUI query saving
  - [x] exploration reserve (`prefer_non_hot`) behavior
  - [x] text-only Telegram formatting
- [x] Local validation completed:
  - [x] full server regression suite now passes with `153 passed`
  - [x] Python compile sanity passed for the modified worker/API/GUI modules
- [ ] VPS rollout of `ENABLE_CENTRAL_ROUTE_DISPATCH`
  - pending in this pass; production remains on the pre-alignment ownership model until the new flag is deployed and enabled

### Upgrade-Track Notes

- Current baseline after the Phase 4 alignment addendum + Phase 6 rollout: Postgres remains authoritative, Redis pub/sub fanout stays active as the API trigger for normalized WebSocket fanout, Redis Streams publishing remains enabled, notification delivery remains on the dedicated notification consumer, central route dispatch is enabled, workers are execution-profile-gated executors, priority scheduling / route lanes remain enabled, cold-field enrichment remains on its own Redis Streams consumer, and operators now have authenticated API control over flags/runtime thresholds plus dead-letter/replay and backlog inspection.
- VPS rollout state on 2026-03-10:
  - `ENABLE_REDIS_STREAM_EVENTS=1`
  - `ENABLE_NOTIFICATION_CONSUMER=1`
  - `ENABLE_GUI_WEBSOCKET_PUSH=1`
  - `ENABLE_PRIORITY_SCHEDULER=1`
  - `ENABLE_ROUTE_LANES=1`
  - `ENABLE_BACKGROUND_ENRICHMENT=1`
- Rollout verification included:
  - clean startup with `ENABLE_GUI_WEBSOCKET_PUSH=0` proving the desktop remained poll-only
  - live flag enable in `flipper:flags` for `ENABLE_GUI_WEBSOCKET_PUSH=1`
  - Redis verification of `stream:listings`, consumer-group presence, and zero lag after catch-up
  - controlled unread-event smoke with `phase2-smoke-unread-1` proving exactly one alert send
  - duplicate synthetic stream event for the same listing proving `duplicate_already_sent` dedupe via the PostgreSQL ledger
  - controlled restart smoke with `phase2-smoke-unread-restart-1` proving unread events are consumed after the consumer restarts
      - controlled worker-process smoke with `phase2-worker-delegate-1` proving worker-side `notification_delivery_delegated` instead of inline Telegram delivery when the consumer flag is enabled
- controlled Phase 3 desktop smoke with synthetic listings:
  - rollout validation initially exposed two late bugs that were fixed before final acceptance:
    - websocket push failed on raw Postgres `NUMERIC`/`Decimal` values until API snapshot payloads were JSON-normalized
    - desktop fallback exited on transient `502` poll errors during API restarts until retry handling was hardened
  - `phase3-smoke-live-pass-1773067600` proved a new listing appears in the GUI via `websocket` before the next poll interval
  - `phase3-smoke-reconnect-pass-1773067812` proved disconnect/reconnect backfill does not lose listings and returns to live mode afterward
  - `phase3-smoke-pollonly-pass-1773067877` proved poll fallback still works when `ENABLE_GUI_WEBSOCKET_PUSH=0`
  - synthetic Phase 3 rows were removed after verification
- controlled Phase 4 scheduler smoke on `worker_3`:
  - isolated `worker_3` temporarily while `worker` and `worker_2` were stopped to avoid Dolphin profile contention during the scheduler comparison window
  - seeded three temporary DB routes only for acceptance:
    - `phase4_hot_w3` (`priority=300`, `lane_override=hot`, query `iPhone 15 Pro`)
    - `phase4_sweep_w3` (`priority=100`, `lane_override=sweep`, query `iPhone 13 mini`)
    - `phase4_sweep2_w3` (`priority=110`, `lane_override=sweep`, query `iPhone 12 mini`)
  - cleaned stale `worker_proxy_leases` and Redis `query-lock:*` keys between runs so the comparison used equal `next_run_at` values and fresh lease state
  - flags-off baseline (`ENABLE_ROUTE_LANES=0`, `ENABLE_PRIORITY_SCHEDULER=0`) selected routes in plain priority order:
    - `phase4_sweep_w3` at `2026-03-09 22:21:23 UTC`
    - `phase4_sweep2_w3` at `2026-03-09 22:22:04 UTC`
    - `phase4_hot_w3` at `2026-03-09 22:22:37 UTC`
  - verified intermediate `ENABLE_ROUTE_LANES=1`, `ENABLE_PRIORITY_SCHEDULER=0` behavior:
    - `/worker-routes` exposed non-null `priority_score`, `computed_lane`, and `effective_lane`
    - worker logs included lane/score components while dispatch order remained legacy
  - final flags-on verification (`ENABLE_ROUTE_LANES=1`, `ENABLE_PRIORITY_SCHEDULER=1`) used a verified equal `next_run_at` reset and produced:
    - first selection `phase4_hot_w3` at `2026-03-09 22:34:58 UTC`
    - Redis query-lock acquisition on the hot route (`query_lock_status=acquired`, `query_lock_key=query-lock:a37b7398f5d5cdd2099f30b6`)
    - second hot-route selection at `2026-03-09 22:37:28 UTC`, proving the hot-route revisit gap stayed at the current baseline cadence (`149.646s`)
    - successful sweep selection later with `query_lock_status=acquired` on `phase4_sweep2_w3`
    - persisted post-cycle interval evidence from `/worker-routes`:
      - hot route `next_run_at - last_selected_at = 149.570s`
      - sweep route `phase4_sweep2_w3 next_run_at - last_selected_at = 229.610s`
  - temporary Phase 4 routes were deleted afterward and the full worker pool was restarted; `worker_3` returned to env-backed routing and `/worker-routes?worker_name=worker_3` returned `count=0`
- controlled Phase 5 enrichment smoke:
  - verified clean startup with `ENABLE_BACKGROUND_ENRICHMENT=0`
  - enabled `ENABLE_BACKGROUND_ENRICHMENT=1` in `flipper:flags`
  - confirmed `stream:listing_enrichment` activity plus active consumer group `listing_enrichment`
  - confirmed live `listing_enrichment_enqueued` / `listing_enrichment_completed` JSON events in worker + enrichment-worker logs
  - verified later cold-field updates still reach API/WebSocket clients through pub/sub-triggered `listing_updated`
  - compared worker CPU/memory and scrape-cycle duration before/after enable; worker resource usage stayed flat while enrichment work moved onto `enrichment_worker`
  - confirmed alert payloads remained decision-ready with title, price, projected profit, and URL while remaining text-only
- Acceptance criteria now expected to hold together after Phase 1 + Phase 2:
  - newly persisted listings generate exactly one stream event in the happy path
  - consumer restart does not lose unread events
  - duplicate unchanged listings do not generate duplicate alerts
- Acceptance criteria now expected to hold together after Phase 3:
  - new listing appears in GUI without waiting for the next poll interval
  - disconnect/reconnect does not lose listings
  - poll fallback still works with WebSocket disabled
- Acceptance criteria now met after Phase 4:
  - hot routes show shorter revisit intervals than sweep routes
  - per-profile request intensity does not exceed the current baseline cadence
  - high-value route coverage time decreases measurably versus the flags-off baseline
- Acceptance criteria now met after Phase 5:
  - alert pipeline does not block on deep enrichment
  - CPU and memory usage per worker decrease or remain flat
  - notification payload remains sufficient for decision-making
- Acceptance criteria now met after Phase 6:
  - notification service can be restarted without data loss
  - backlog and consumer lag can be measured and inspected
  - the new realtime subsystems can be disabled or tuned through authenticated Redis-backed operator controls without manual Redis access
- Acceptance criteria implemented and locally verified after the Phase 4 alignment addendum:
  - route/query/worker ownership is centrally managed rather than worker-partitioned
  - workers execute the best eligible central route/query available to a safe local execution profile
  - exploration reserve prevents hot work from fully starving due non-hot work
  - Telegram notifications remain text-only and do not wait on thumbnail/enrichment readiness
- Optional later-phase audit status:
  - Optional B (`Separate enrichment workers`) audited on `2026-03-10` and confirmed already satisfied by the shipped Phase 5/6 enrichment design
  - no additional runtime, schema, or deploy changes were required beyond the existing `stream:listing_enrichment` queue and dedicated `enrichment_worker` service
  - focused validation reconfirmed enqueue-only worker behavior, separate consumer-group processing, and same-row late cold-field merge semantics

---

## Original Roadmap Drift Audit (Summary)

The following significant enhancements were found in code but not reflected as explicit roadmap items in the original baseline:

- GUI-integrated price sheet editor and full recalculation workflow
- Scraper cancellation and live query-level progress reporting
- User-applied listing color flags (`scam`, `interested`)
- Expanded listing retention model (`new`/`needs_pricing`/`unclassified`) instead of only profitable-path visibility
- Updated profit calculation based on listed price rather than max buy reference
- Hybrid marketplace fetch path combining GraphQL response extraction with DOM fallback
- Auth bootstrap hardening (`auth.json` auto-import and cleanup) plus explicit auth health command
- Resale reconciliation command and daily summary reporting path
- GUI-based multi-account/proxy configuration and runtime scraper settings manager
- API-driven proxy ingestion flow with configurable endpoint/auth headers in GUI settings
- Proxy quick-import from pasted cURL/proxy lines and mixed-schema payload normalization
- Proxied manual login workflow with browser-level anti-leak hardening flags (QUIC/WebRTC/DNS prefetch controls)
- iPhone 16 family support in model detection logic (even before query seed expansion)
- Currency-aware price normalization/backfill for mixed Marketplace price formats
- Monitor jitter controls to avoid fixed-cadence polling signatures
- DB-backed query registry with per-query poll timestamps (`search_queries.last_polled`)
- Automated runtime account health + cooldown tracking for scraper account pool

These are now integrated into this roadmap as completed and maintained items.

Note: Specifically, an external `patch_core.py` was used to dynamically patch `scraper/core.py` and extract some standalone functions to `scraper/legacy_utils.py` outside of the formal module refactor.

1. V2.2 `core/scraper/` package rewrite with Dolphin Anty CDP-based browser launch (`connect_over_cdp`) replacing direct Playwright profile management.
2. Weighted bucket query system (`_get_bucket_queries`) with four diversified query categories (broad/exact/flipper/misspelling) and per-cycle randomized selection.
3. Quiet hours scheduling (`WORKER_QUIET_HOURS_UTC`, `WORKER_QUIET_HOURS_MULTIPLIER`) for reduced scraping cadence during configurable UTC off-peak windows.
4. Session duration caps (`WORKER_SESSION_MAX_QUERIES`) to restart browser sessions after N queries, preventing session-length fingerprinting.
5. Per-worker dynamic Dolphin Anty profile binding via Postgres lease pool and `DOLPHIN_API_URL`/`DOLPHIN_ANTY_TOKEN` compose integration.
6. VPS infrastructure migration: deploy target updated from `root@147.182.131.111` to `ubuntu@15.235.185.32` with new directory layout.
7. ~~5-slot concurrent worker architecture~~ — **removed**: reverted to single-loop-per-container in dev-log §34.
8. `scraper/legacy_utils.py` (814 lines) preserving V1 monolithic scraper logic as shared utilities during package refactoring.
9. Dolphin Anty browser.py hardening: `DOLPHIN_WS_HOST` for Docker→host WS connectivity, `E_BROWSER_RUN_DUPLICATE` reuse, three-tier profile listing.
10. Caddy reverse proxy service in Docker Compose with auto-TLS provisioning.
11. Proxy provider health monitor fully wired into worker slot loop (per-cycle saturated skip + pacing multiplier).
12. V2.2 core scraper package carries bridge/connection configs (`PROXY_BRIDGE_*`) from prior iterations — available but inactive in worker runtime.

Action taken: all above are now documented in `roadmap.md` as completed scope.

Fresh audit note (2026-03-10): no additional undocumented code-level features were found beyond the items already captured in this summary and the later V3.0 upgrade-track section above.

## Current Focus Recommendation

Active priority remains the broader **Phase 5A** server-migration hardening track plus optional later-phase profiling work. V3.0 Phase 6 reliability/replay/operator controls are now implemented. Remaining high-impact items after Phase 6 acceptance: deeper `legacy_utils.py` coverage, continued server migration cleanup, and any optional post-V3 performance studies only if profiling justifies them.
