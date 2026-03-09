# iPhone Flipper Development Log

## Log Policy

This is a living development log tracking:

- completed implementation work
- architectural and product decisions
- roadmap drift (manual/additional features)
- current status and active risks

Last updated: **2026-03-10**
Author: Codex implementation/update pass

---

## 1. Current System Snapshot

### Runtime/Project Snapshot

- Application type: local Python app (CLI + Tkinter GUI)
- Primary DB: `listings.db` (SQLite)
- Deployment mode: local app + server migration baseline scaffolded (`server/`), rollout in progress
- Current listing count: **383** (`new`: 334, `needs_pricing`: 20, `unclassified`: 29)
- Purchases recorded: **0**
- Conversations recorded: **0**

### Primary Operational Capabilities

- Marketplace scraping with persistent browser profile
- Hybrid listing acquisition (captured Marketplace GraphQL responses + DOM fallback)
- Model/condition inference and pricing calculations
- AI negotiation message generation (Gemini/OpenAI)
- Notification fanout (email/Telegram/Discord)
- Purchase tracking, patterns, conversion scoring, insights
- GUI-driven workflows for scrape/analysis/price-sheet edits

---

## 2. Implementation Timeline (Consolidated)

### Milestone A: Core Automation Foundation

Status: Completed

- Implemented `main.py` command surface for scrape/monitor/list/respond/analyze/status.
- Implemented DB bootstrap and scrape ingestion pipeline.
- Added persistent schema for multi-account/proxy runtime config (`fb_accounts`, `proxies`, `scraper_settings`).
- Added DB-backed `search_queries` table seeded from default iPhone terms for runtime query registry.
- Added auth configuration handling for API key and OAuth token formats.
- Added `auth-status` visibility command with OAuth expiry awareness.
- Added one-time `auth.json` bootstrap import with source-file cleanup.

### Milestone B: Negotiation + Analytics Engine

Status: Completed

- Added AI negotiation module with provider abstraction and conversation persistence.
- Added purchase recording model and pattern-learning tables.
- Added conversion score and insights generation pipeline.

### Milestone C: GUI Control Plane

Status: Completed

- Added tabbed GUI for listings, negotiation, and deals.
- Added action buttons for open URL, negotiation flow, and purchase marking.
- Added analytics access via menu (patterns/history/insights).

### Milestone D: UX and Process Enhancements

Status: Completed

- Added `Price Sheet` menu and in-app CSV editor.
- Added full listing recalculation after price updates.
- Added scraper run telemetry in GUI (elapsed time + query progress).
- Added scraper cancellation control.
- Added listing color flags through context menu (`scam`, `interested`, `not_interested`, clear).
- Added bulk selection behavior for listings (drag-select range + Cmd/Ctrl multi-select) and right-click bulk actions for mark/clear/delete.
- Added opened-listing visual state:
  - new `opened_at` listing field for persistent tracking of listings opened in browser
  - opening a listing now auto-sets `opened_at`
  - opened rows render with distinct blue tint unless an explicit user flag color is set
- Added advanced Listings filters:
  - multi-select model dropdown (checkable menu)
  - profit range inputs (`min` / `max`)
  - minimum price input
  - all filters are combined in SQL query path and can be cleared via `Clear Filters`
- Price Sheet editor now de-duplicates models on save (case/spacing-insensitive upsert behavior) to prevent duplicate model rows.
- Renamed Price Sheet save action button from `Save To CSV` to `Apply Changes` for clearer workflow wording.
- Added feed-level suppression for non-actionable model rows:
  - listings where `model` is blank or `Unknown` are hidden from Listings tab and model dropdown
  - rows are retained in database for audit/debug use
- Added setup/launch guide panel in GUI.

### Milestone E: Financial and Visibility Corrections

Status: Completed

- Corrected profit computation to use listed price vs sell price (with repair costs).
- Adjusted ingestion/status handling so not only “good/profitable” records are shown.
- Tightened scraper scope to persist only listings whose detected model exists in current price sheet.
- Recalculation now purges managed listings whose model is not in price sheet (keeps queue aligned after model deletes).
- Limited outbound new-listing notifications to profitable subset by default.
- Added post-purchase resale reconciliation path (`main.py resale` + `update_purchase_resale`).
- Added on-demand daily operational summary broadcast (`main.py summary`).

### Milestone F: Marketplace Fetch Upgrade (Hybrid Path)

Status: Completed

- Added GraphQL response capture during Marketplace search navigation.
- Added normalized extraction of listing ID/title/price/location/description/seller from GraphQL payloads.
- Added canonical Marketplace URL normalization (`/marketplace/item/<id>/`) to strip tracking params from stored/opened links.
- Added DOM fallback merge path so scraping still works when GraphQL extraction is sparse.
- Added richer listing persistence (`location`, `description`, `seller_name`) for downstream evaluation.
- Added per-query source visibility (`graphql_found`, `dom_found`) in progress callbacks.

### Milestone G: GUI Connection Settings Manager

Status: Completed

- Added `Settings > Connections & Scraper` GUI entry.
- Added full CRUD interface for Facebook account configurations (status, profile path, user agent, proxy mapping).
- Added per-account manual login action constrained to SOCKS5 proxy assignment.
- Added local SOCKS5 auth bridge fallback (`pproxy`) to handle Chromium's SOCKS5-auth limitation.
- Added account-scoped browser session launcher so operator can manually browse from configured account identity.
- Added full CRUD interface for proxies (type/host/port/auth/country/status).
- Added API-driven proxy sync (fetch + parse + upsert) for bulk proxy ingestion.
- Added GUI-editable scraper settings persisted in DB (monitor interval, monitor jitter, inter-query delay bounds, max queries/run).
- Added proxy API configuration settings in GUI (URL, key, auth header, timeout, default type/country).
- Added scraper-tab action button to trigger proxy API import directly from saved API settings.
- Added Scraper-menu `Worker Queries & Keywords` manager for:
  - per-worker route query shard updates (`worker_routes.search_queries`)
  - universal accessory/negative keyword management with VPS runtime sync
- Wired monitor launch to use persisted interval setting.
- Wired scraper runtime to use persisted delay bounds and query cap.
- Wired manual login flow to capture and persist cookies, user agent, proxy-observed IP, and account status.
- Wired scraper execution to account-only mode (no default shared profile fallback).
- Added per-run round-robin scraper account rotation across eligible ACTIVE+SOCKS5 accounts using `last_scraper_account_id`.
- Added rotation-start selector (`active_scraper_account_id`) to anchor first run in the rotation sequence.
- Extended strict account-only rotation to CLI `scrape` and `monitor` commands with automatic SOCKS5 auth bridge handling.
- Wired `notify_profitable_only` setting into both GUI and CLI notification dispatch behavior.
- Added scrape runtime account-health tracking (`failure_count`, `cooldown_until`) with automatic status transitions on run success/failure.

### Milestone H: 24x7 Server + Real-Time Sync Migration Planning

Status: Completed

- Added target-state architecture for VPS-hosted worker pool, centralized DB, and event-driven desktop sync.
- Added operator-side migration checklist (VPS, DB, domain/TLS, secrets, account-profile validation).
- Added engineering migration sequence (immediate per-listing upsert + event fanout + WebSocket sync).
- Re-prioritized roadmap toward server migration track before further local-only optimization.

### Milestone I: 24x7 Server Baseline Implementation

Status: In Progress (Baseline Completed)

- Refactored `scraper.scrape_marketplace()` ingest path to commit listings immediately per insert (removed run-end batch wait).
- Added `listing_saved` event emission with full listing payload for downstream realtime consumers.
- Added server deployment scaffold under `server/`:
  - `server/infra/docker-compose.yml` (Caddy + Postgres + Redis + API + worker pool)
  - `server/infra/Caddyfile` (TLS reverse-proxy target for API)
  - `server/services/api/app/main.py` (`/healthz`, `/listings`, `/ws/listings`)
  - `server/services/api/sql/001_init.sql` (authoritative listings table + indexes/trigger)
  - `server/services/worker/worker.py` (continuous scraping + per-listing Postgres upsert + Redis publish)
  - `server/.env.example`, `server/README.md` (operator runbook)
- Removed hardcoded worker proxy credentials from compose templates and moved them to env-based settings (`SCRAPER_PROXY_*`, `SCRAPER_PROXY_*_2`).
- Corrected Postgres persistent volume mapping in compose template to `../runtime/postgres:/var/lib/postgresql/data`.
- Added Docker build context hardening (`.dockerignore`) to avoid shipping local browser cache/profile artifacts into image build context.
- Added GUI progress-state support for incremental `listing_saved` counts during active scrape runs.

### Milestone J: Desktop Server-Sync (Incremental Cursor Polling)

Status: In Progress (Poll Path Completed)

- Added GUI settings for server-sync controls:
  - `server_sync_enabled`
  - `server_api_base_url`
  - `server_api_token`
  - `server_sync_poll_seconds`
  - `server_sync_since_id`
- Added background GUI sync worker that continuously pulls incremental updates from server `/listings` using `since_id` cursor pagination.
- Added local SQLite upsert path for server listings with conflict-safe updates and preservation of local `purchased` status.
- Added cursor persistence in `scraper_settings` so reconnects/backfills continue from last synced watermark.
- Added GUI startup/shutdown lifecycle hooks to auto-start sync when enabled and cleanly stop thread on app exit.
- Added GUI VPS activity monitor window with operator actions for:
  - API health check
  - worker service status (`docker compose ps`)
  - worker log tail (`docker compose logs`)
  - server-side listing count (`psql count(*)`)
- Added GUI-configurable server monitor SSH settings (`server_ssh_*`) and worker service selector (`server_monitor_worker_services`).

### Milestone K: VPS Scraper Route Control + Pagination/Rotation Hardening

Status: Completed

- Added server-side worker route and heartbeat schema (`worker_routes`, `worker_heartbeats`) in `server/services/api/sql/001_init.sql`.
- Added API endpoints for VPS scraper route control and status:
  - `GET /worker-routes`
  - `PUT /worker-routes/{worker_name}/{route_name}`
  - `DELETE /worker-routes/{worker_name}/{route_name}`
  - `GET /worker-health`
- Added worker-side route rotation per cycle:
  - worker loads enabled routes for its configured `WORKER_NAME`
  - route selection is round-robin each scrape cycle
  - route markers update (`last_selected_at`, `last_success_at`, `last_error`)
  - worker heartbeat updates for `running`/`ok`/`error` states
- Added GUI **VPS Scrapers** tab for remote route CRUD and heartbeat visibility from desktop.
- Simplified settings UX to VPS-first single-tab workflow (removed account/proxy multi-tab dependency from primary path).
- Added VPS-side `Manual Login (SOCKS5)` action with proxy-profile selection to bootstrap new scraper profiles via remote SSH command flow.
- Updated settings UX per operator request to two-tab structure: `Proxies` + `VPS Scrapers`.
- Updated VPS route save/manual-login behavior to target active workers for immediate rotation participation (instead of requiring unique worker names).
- Added worker-route status rendering for rotation context: non-active routes on a healthy worker show `ready` (instead of `unknown`).
- Migrated existing non-running route `worker_3/profile_3` to active worker rotation as `worker/profile_3`; heartbeat now reports live selection on `worker`.
- Added automatic profile-dir suggestion/population and VPS-side directory creation bootstrap during manual login trigger.
- Reworked Marketplace page-depth traversal to progressive scroll loading (target cards + max rounds) and added diagnostics (`page_cards`, `scroll_rounds`).
- Rotation min-reuse filtering now uses dedicated `last_scrape_started_at` instead of generic account `updated_at`.

### Milestone L: VPS Route Availability Recovery + Runtime Hardening

Status: Completed

- Restored missing VPS runtime env file and re-established deploy path after accidental `.env` deletion on server.
- Redeployed API and validated public endpoint availability for:
  - `GET /worker-routes`
  - `GET /worker-health`
  - `GET /healthz`
- Seeded current VPS scraper profiles into route registry for immediate UI visibility:
  - `worker/profile_1`
  - `worker_2/profile_2`
- Hardened root `.dockerignore` to exclude runtime/data/log/secrets paths and prevent Docker build-context permission failures on VPS.
- Removed concurrent trigger/function bootstrap DDL from worker startup to eliminate multi-worker startup race errors.
- Improved pagination-depth behavior in scraper runtime:
  - feed-container-aware scroll targeting (not only window scroll)
  - cumulative DOM snapshot aggregation across scroll rounds for virtualized feeds
  - increased idle-stall tolerance before stopping scroll rounds
- Updated VPS manual login execution:
  - default browser preference now starts with `google-chrome`
  - credentialed SOCKS5 proxies now run through a temporary local `pproxy` bridge on VPS before launching browser (`--proxy-server=http://127.0.0.1:<port>`)
  - manual-login command now auto-detects VNC display/Xauthority (`Xtigervnc :1`, `/home/ubuntu/.Xauthority`) and launches directly from SSH-triggered GUI action
  - manual-login output now includes proxy egress-IP check and pre-launch `facebook.com` `c_user` cookie presence to confirm login-capture state
  - manual-login preflight now performs best-effort profile ownership/permission repair (`chown/chmod`) and downgrades cookie DB permission-denied probes to non-fatal status output (`unknown (permission denied)`)
  - hotfix: escaped embedded cookie-check snippet interpolation in GUI manual-login command builder (fixed `NameError: db_path is not defined` on button click)
  - manual login now auto-saves/upserts VPS route before launch so newly logged-in scraper profile is immediately visible in VPS scraper list
  - VPS Scrapers settings tab now supports vertical scrolling for smaller-screen usability (all action buttons remain reachable)
- Added automatic accessory-only suppression for scraped listings:
  - new heuristic filter drops accessory-only posts (case/cover/protector/charger without handset signals) before listing insert
  - worker ingest path now applies the same guard so VPS pipeline also rejects accessory-only events
  - scrape run and financial recalculation now auto-purge previously stored accessory-only rows from managed statuses (`new`, `unclassified`, `needs_pricing`)
- Added operator-tunable accessory suppression settings in GUI:
  - VPS Scrapers settings now expose `Accessory Keywords (CSV)` and `Accessory Max Price`
  - values persist to `scraper_settings` as `accessory_filter_keywords` / `accessory_filter_max_price`
  - save action performs best-effort SSH sync to VPS `runtime/worker_*.db` scraper settings for server-side worker parity
- Added desktop sync-side cleanup guard:
  - server cursor-sync upsert path now calls accessory-only purge on local SQLite each batch, so rows deleted/filtered upstream do not remain stale in GUI
  - accessory detector now matches plural title variants (`cases`, `covers`) in addition to singular (`case`, `cover`)

### Milestone M: Telegram Listing Card Notifications (VPS Worker Path)

Status: Completed

- Upgraded Telegram sender to support optional parse mode and per-message web preview control.
- Added Telegram listing-card formatter (`build_telegram_listing_card`) with compact fields:
  - listing title/model/condition
  - listed price and potential profit
  - truncated description snippet
  - direct listing link
- Updated local notification fanout to send one Telegram card per listing (instead of only a large text dump).
- Changed notifiable-profit threshold from `> 0` to `>= 0` for new-listing notifications.
- Added worker-side Telegram dispatch for `listing_created` events where:
  - Telegram token/chat is configured
  - listing `potential_profit >= TELEGRAM_NOTIFY_MIN_PROFIT` (default `0`)
- Added guard to suppress worker Telegram alerts when listing model is blank or `Unknown`.
- Worker path uses non-blocking thread handoff (`asyncio.to_thread`) so notification HTTP calls do not stall scrape/event loop progress.
- Added GUI `Send Telegram Test` controls in:
  - `Connections & Scraper > VPS Scrapers` connection actions
  - `VPS Scraper Activity Monitor`
  The action executes over SSH and sends a test message from running VPS worker env to verify real production token/chat settings.

### Milestone N: Worker_3 Fast-Lane + Query Routing Controls

Status: Completed

- Added built-in `worker_3` service to compose stack for fast newest-listing sweep behavior.
- Added worker runtime env overrides for scroll depth:
  - `WORKER_SCROLL_TARGET_CARDS`
  - `WORKER_SCROLL_MAX_ROUNDS`
- Wired `worker_3` defaults to broad `iPhone` query with short cycle interval and medium-depth scroll profile (target cards capped at 100).
- Updated monitor defaults to include `worker_3` service checks in GUI/DB defaults.
- Fixed VPS accessory-filter sync script indentation bug in GUI SSH helper (`IndentationError` on `targets = sorted(runtime_dir.glob("worker_*.db"))`).
- Upgraded VPS Activity Monitor worker logs to live stream mode (`docker compose logs -f`) with `Stop Logs` control and cleanup on window/app close.
- Hardened VPS accessory-filter sync to update settings via `docker compose exec` inside worker containers, avoiding host-file SQLite permission/read-only failures.
- Hardened VPS accessory-filter sync failure handling for SQLite readonly states:
  - detects readonly write failures from container-side sync calls
  - runs one-time permission repair on target worker runtime DB path (`docker compose exec -u 0 ... chown/chmod`)
  - retries sync write after repair and reports partial failures per worker service
  - safety fix: permission repair is now non-recursive (repairs DB dir/file only) to avoid touching unrelated runtime subtrees such as PostgreSQL data
- Normalized GUI monitor worker-service parsing to accept comma/space-separated values and always enforce required coverage (`worker`, `worker_2`, `worker_3`) so `worker_3` appears in status/log checks even when older saved settings were missing it.

### Milestone O: Feed Recency + Worker Profile Failure Safeguards

Status: Completed

- Enforced newest-first Marketplace search ordering (`sortBy=creation_time_descend`) for all query URLs.
- Updated progressive feed scroll logic to enforce at least one scroll pass before target-card early exit, reducing false "already loaded enough" starts.
- Added worker-side profile failure detection using `query_result` telemetry:
  - routes are marked `degraded` when all query results return `page_cards=0`
  - degraded state is persisted into `worker_heartbeats.last_error/status` for operator visibility
- Added Telegram profile failure notifications with worker/profile/route context and cooldown control (`WORKER_PROFILE_FAILURE_ALERT_COOLDOWN_SECONDS`).
- Added GUI main-page VPS worker health strip with three red/green indicators for `worker`, `worker_2`, and `worker_3`, polled from `/worker-health`.

### Milestone P: VPS Deploy Automation + Host Security Hardening

Status: Completed

- Added one-command deployment script: `server/scripts/deploy_vps.sh`.
  - syncs `server/` to VPS via rsync (excluding `.env`/runtime cache artifacts)
  - runs `docker compose --env-file ../.env up -d --build` for API + worker services
  - applies SQL bootstrap migration (`001_init.sql`) idempotently
  - validates local API health and worker heartbeat endpoint after rollout
- Updated server runbook with script usage and environment override examples (`server/README.md`).
- Applied VPS host hardening baseline:
  - enforced SSH key-only access via `/etc/ssh/sshd_config.d/99-codex-hardening.conf`
  - disabled password/KbdInteractive auth and set conservative SSH auth limits
  - validated active UFW allowlist (`22`, `80`, `443`)
  - installed/enabled fail2ban with `sshd` jail
- Executed controlled profile-failure alert validation:
  - intentionally injected bad `proxy_server` on `worker_3/profile_3`
  - observed degraded/cooldown state transitions and retry behavior
  - verified worker log line `Telegram message sent` for profile-failure alert path
  - restored original route proxy and cleared cooldown to return `worker_3` to normal route selection

### Milestone Q: Proxy Rotation Concurrency Guard + GUI Auto Assignment

Status: Completed

- Added server-side global proxy lease table (`worker_proxy_leases`) with migration wiring in:
  - `server/services/api/sql/001_init.sql`
  - `server/services/api/app/main.py`
  - `server/services/worker/worker.py`
- Worker proxy selection now acquires DB-backed lease before use:
  - blocks concurrent use of the same proxy across routes/profiles
  - enforces auto-rotation proxy reuse cooldown (`WORKER_PROXY_REUSE_COOLDOWN_SECONDS`)
  - applies lease TTL (`WORKER_PROXY_LEASE_SECONDS`) and explicit release after cycle
- Updated worker fallback behavior:
  - if DB routes exist but are unavailable (cooldown/proxy lock), worker waits in cooldown state
  - avoids silent `env_default` fallback loops that masked route-level failures
- Updated VPS Scrapers GUI route form:
  - `Worker Name` can be left blank and is auto-assigned on save
  - when `proxy_mode=auto_rotation`, proxy profile selection is optional
  - auto mode now seeds proxy selection randomly from parsed pool/proxy inventory
- Updated manual-login flow:
  - blank worker name now auto-assigns
  - auto-rotation mode can seed login proxy from proxy pool when `proxy_server` is blank

### Milestone R: Worker Lease/Cooldown Visibility in GUI

Status: Completed

- Expanded API worker-health payload (`GET /worker-health`) to include:
  - active lease metadata (`leased_proxy_server`, `leased_proxy_route`, `leased_proxy_until`, `lease_remaining_seconds`)
  - active route cooldown metadata (`route_cooldown_until`, `cooldown_remaining_seconds`)
  - convenience booleans (`has_active_lease`, `is_route_in_cooldown`)
- Updated main Listings worker status strip to include runtime hints per worker:
  - `lease mm:ss` when proxy is actively leased
  - `cd mm:ss` when route is waiting in cooldown
  - `idle` otherwise
- Updated `Settings > Connections & Scraper > VPS Scrapers` monitoring surfaces:
  - route table now includes `Lease` and `Cooldown` columns
  - route details form now includes read-only `Lease Status` and `Cooldown Status`
  - displays are sourced from `/worker-health` + route cooldown timestamps, so operators can monitor worker activity without tailing logs

### Milestone S: Worker-Scoped Query Manager + Zero-Query Failure Guard

Status: Completed

- Updated `Worker Queries & Negative Keywords` UI to worker scope only:
  - table now shows one row per worker (`worker`, `worker_2`, `worker_3`) instead of worker+route combinations
  - query shard edits now apply across all routes/profiles under the selected worker
  - route/profile naming is removed from query-management workflow to match worker-level query ownership
- Hardened worker profile-failure detection:
  - scrape cycles with zero executed queries now count as bad cycles
  - these cycles now participate in degraded/cooldown/alert pipeline instead of silently passing as healthy

### Milestone T: Last-Minute Scrape Throughput Metric

Status: Completed

- Added `worker_scrape_events` persistence in worker/API schema (`worker_name`, `route_name`, `scraped_count`, `observed_at`) with worker+time index.
- Worker runtime now records query-level `found` counts as scrape events (actual scraped volume), separate from `listing_saved`/`new_saved`.
- Scrape events are persisted at query-result time (not only end-of-cycle) so `listings_scraped_last_minute` remains live during long worker cycles.
- API `GET /worker-health` now returns `listings_scraped_last_minute` as rolling 60-second sum per worker.
- GUI monitoring updates:
  - `Worker Queries & Negative Keywords` worker status table includes `Listings Scraped last minute`.
  - `Settings > Connections & Scraper > VPS Scrapers` route table includes `Scraped (1m)` and route form includes read-only `Listings Scraped last minute`.

### Milestone U: Worker Scheduling + Capacity-Wait Hardening

Status: Completed

- Added worker runtime helper module (`server/services/worker/runtime.py`) for:
  - `CycleOutcome` / `ErrorCategory` types
  - runtime-compensated sleep + jitter computation
  - due-route (`next_run_at`) selection helpers
  - retry/backoff and bad-cycle counting helpers
- Replaced in-memory route round-robin with DB-backed due-time scheduling:
  - `worker_routes.next_run_at` and optional `route_interval_seconds`
  - route selection now picks earliest due route
  - worker sleeps until due-time/cooldown windows instead of fixed loops
- Added first-class non-failure outcomes:
  - `WAIT_PROXY` when no proxy can be leased
  - `WAIT_PROFILE_LOCK` when profile runtime lock is unavailable
  - both outcomes do not increment bad-cycle/degrade/cooldown counters
- Added profile runtime lock guard:
  - lock acquisition/release via `worker_proxy_leases` with `PROFILE:` key prefix
  - prevents concurrent Playwright launches on the same `user_data_dir`
- Hardened lease lifecycle:
  - explicit release in cycle `finally` paths retained
  - `last_used_at` now stamped on release (not on lease acquisition)
- Added API-side protection against profile contention:
  - `PUT /worker-routes/{worker}/{route}` now rejects enabled routes reusing another enabled route's `user_data_dir`
- Added structured worker telemetry logs per cycle:
  - `cycle_id`, `route_name`, `duration_ms`, `outcome`, `error_category`, `retry_count`
- Added tests and docs for the new scheduler/outcome behavior:
  - `server/tests/test_worker_runtime.py`
  - `server/tests/test_api_profile_validation.py`
  - `AGENTS.md`, `TESTING.md`

### Milestone V: Route State Machine + Proxy Health + Signal Safeguards

Status: Completed

- Added proxy health persistence and scoring:
  - new `proxy_stats` table with `consecutive_failures`, `last_success_at`, `banned_until`, `avg_latency_ms`
  - worker now updates proxy health on scrape success/failure and applies exponential temporary bans after repeated failures
  - proxy candidate selection now prioritizes healthy candidates and excludes currently banned proxies
- Added explicit route status state machine persistence on `worker_routes`:
  - `status`, `status_reason`, `status_since`
  - supported statuses: `ENABLED`, `DEGRADED`, `THROTTLED`, `COOLDOWN`, `NEEDS_LOGIN`, `DISABLED`
  - degraded/throttled states now drive interval multipliers (`2x`, `4x`) and are updated in worker runtime transitions
  - expired cooldown windows are auto-released back to `ENABLED` before route selection
- Added pre-checkpoint soft-signal detection path:
  - new pure module `server/services/worker/signal_detector.py`
  - signal analyzer computes risk + recommended action (`continue`, `throttle`, `pause`, `quarantine`)
  - route EMA baselines persisted via `avg_result_count`, `avg_page_load_ms`, `successful_cycles`
  - worker now maps high signal risk to proactive throttle/pause/quarantine behavior (`WAIT_SIGNAL_PAUSE` or `NEEDS_LOGIN`)
- Added cross-worker query-shard deduplication:
  - worker acquires/releases shard locks using `worker_proxy_leases` (`QUERY_SHARD:*`)
  - lock contention returns non-failure `WAIT_QUERY_SHARD` outcome (no bad-cycle increment)
- Added minimum-route resilience enforcement:
  - API route upsert/delete now returns warnings when a worker has fewer than configured minimum enabled routes (`WORKER_MIN_ENABLED_ROUTES_WARN`)
  - worker applies mandatory extended rest multiplier (`WORKER_SINGLE_ROUTE_REST_MULTIPLIER`) when running with a single enabled route
- Expanded tests and telemetry:
  - extended runtime tests for new classifications/state helpers (`server/tests/test_worker_runtime.py`)
  - added signal-detector tests (`server/tests/test_signal_detector.py`)
  - cycle telemetry now includes dynamic `proxy_key`, `query_shard_key`, soft-signal fields where available

### Milestone W: Quarantine Operations + Proxy Binding Validation + Telemetry Rollup

Status: Completed

- Added quarantine incident metadata to route state:
  - new `worker_routes` fields: `quarantined_at`, `quarantine_reason`, `quarantine_evidence`
  - metadata now populated when worker marks a route `NEEDS_LOGIN`
  - API/GUI route payloads now surface quarantine fields for operator diagnostics
- Added manual-login operational recovery controls:
  - API endpoint `POST /worker-routes/{worker}/{route}/retest` clears manual-login lock and schedules immediate retest cycle
  - API endpoint `POST /worker-routes/{worker}/bulk-clear-manual-login` clears manual-login lock across a worker in one action
  - GUI VPS Scrapers now exposes `Retest Route` and `Bulk Clear Login Locks` actions
- Added Telegram deduplication for manual-login quarantine alerts:
  - dedupe key uses route + normalized reason
  - duplicate alerts are suppressed within `WORKER_MANUAL_LOGIN_ALERT_DEDUP_SECONDS` (default 300s)
- Added proxy binding validation guard in worker cycle path:
  - scraper now performs optional browser-level IP check (`VERIFY_PROXY_IP`, `PROXY_IP_CHECK_URL`, `PROXY_IP_CHECK_TIMEOUT_MS`)
  - mismatches are classified as `PROXY_MISMATCH` and mapped to non-failure wait outcome `WAIT_PROXY_MISMATCH`
  - proxy mismatch waits do not increment route bad-cycle counters
- Completed telemetry hardening deliverables:
  - extracted structured telemetry payload builder to `server/services/worker/telemetry.py`
  - added payload-shape tests (`server/tests/test_telemetry.py`)
  - added telemetry rollup utility (`server/scripts/telemetry_rollup.py`) for per-route success/wait/fail trend reporting

### Milestone X: Persona Variation + Worker Refactor + Shared Schema Ensure

Status: Completed

- Implemented controlled per-cycle browser persona variation using Playwright-native context options:
  - new module `server/services/worker/persona.py` generates coherent personas (`viewport`, `screen`, `device_scale_factor`, `user_agent`, `timezone_id`, `locale`, `color_scheme`)
  - worker runtime now enables persona variation by default via `ENABLE_FINGERPRINT_VARIATION=1`
  - worker passes persona-derived context + `Accept-Language` headers into `scraper.scrape_marketplace()` context creation
- Extended telemetry/evidence coverage for persona debugging:
  - cycle telemetry now includes `persona_hash` and sanitized `persona` fields
  - quarantine evidence now captures persona metadata when present
  - added persona-focused telemetry assertions in `server/tests/test_telemetry.py`
- Completed optional worker modularization track:
  - extracted route mutation writes to `server/services/worker/route_transitions.py`
  - extracted due-time helpers to `server/services/worker/scheduler.py`
  - worker now delegates lease functions to `server/services/worker/lease_manager.py`
- Completed schema ensure consolidation track:
  - added `server/services/common/schema_ensure.py`
  - API (`server/services/api/app/main.py`) and worker (`server/services/worker/worker.py`) now share one schema-ensure path
  - API enables trigger creation (`include_triggers=True`), worker keeps trigger bootstrap disabled (`include_triggers=False`) to avoid multi-worker DDL races
- Added persona unit tests (`server/tests/test_persona.py`).

### Milestone Y: Scraper Module Refactoring Into Package

Status: Completed

- Refactored monolithic `scraper.py` (~3000+ lines) into a structured Python package at `scraper/`:
  - `scraper/__init__.py` — backward-compatible re-export surface preserving all existing imports
  - `scraper/config.py` — constants, environment variable parsing, configuration definitions
  - `scraper/storage.py` — database operations (init, save, load, account/query/setting persistence)
  - `scraper/parsers.py` — model identification, condition assessment, price parsing, profit calculation, accessory detection
  - `scraper/proxy.py` — proxy bridge management, Playwright proxy config builder, runtime context assembly
  - `scraper/browser.py` — stealth scripts, browser context launcher, `random_delay`
  - `scraper/core.py` — main `scrape_marketplace()` entry point, financial recalculation, accessory purge
- Original `scraper.py` retained as thin compatibility shim
- Sub-module boundaries follow logical domain separation (config/storage/parsing/proxy/browser/core)
- Zero downstream import breakage across `gui.py`, `main.py`, `worker.py`, and tests

### Milestone Z: Event-Driven Notification Worker + Proxy Provider Health Monitor

Status: Completed

- Added standalone event-driven notification worker (`server/services/worker/notification_worker.py`, 519 lines):
  - subscribes to Redis `listing_events` pub/sub for sub-second delivery latency
  - priority-tier dispatch (instant ≥ $50 profit, fast-batch ≥ $0, suppressed < $0)
  - Telegram delivery with MarkdownV2 listing card formatting
  - Firebase Cloud Messaging (FCM) push for iOS/Android devices via topic-based delivery (`FCM_TOPIC`)
  - sliding-window rate limiter (`NOTIFY_MAX_PER_MINUTE`) and deduplication (`NOTIFY_DEDUP_WINDOW_SECONDS`)
  - data model: `ListingEvent` dataclass with full listing context fields
  - operational statistics tracking (`NotificationStats`) for monitoring
- Added proxy provider real-time health monitor (`server/services/worker/proxy_monitor.py`, 329 lines):
  - async background polling of proxy gateway utilization API
  - health snapshot (`ProxyProviderHealth`) with threads/utilization/error-rate/bandwidth tracking
  - pacing multiplier computation based on provider saturation thresholds (1.0x / 1.5x / 2.5x)
  - stale-data detection (3x poll interval)
  - Telegram degradation alerts with 5-minute cooldown deduplication
  - integration-ready: `get_proxy_health()` for worker pacing decisions
- Added proxy provider health tests (`server/tests/test_proxy_provider_runtime.py`)

### Milestone AA: Dolphin Anty Profile Management + API Key Integration

Status: Completed

- Added Dolphin Anty profile management tab in GUI Settings (`Connections & Scraper > Dolphin Profiles`):
  - Treeview table with profile columns (ID, Name, Status, Browser, Tags, Memory)
  - `Fetch Profiles` button: queries Dolphin Anty Cloud API (`GET https://anty-api.com/browser_profiles`) to bypass local VPS auth constraints
  - `Start Selected` button: starts selected profiles using local API URL
  - `Stop Selected` button: stops selected profiles using local API URL
- Added Dolphin Anty API Key & URL integration:
  - `dolphin_api_key` and `dolphin_api_url` settings added to defaults and save logic
  - Masked entry field (`show="*"`) for API Key, and text entry for API URL (defaults to `http://localhost:3001`)
- `_dolphin_auth_headers()` helper generates `Authorization: Bearer <API_KEY>` headers
  - API methods automatically enforce SSH tunneling to the VPS for local API commands (Start/Stop)

### Milestone AB: V2.2 GUI Fixes and Dolphin Anty Refinement

Status: Completed

- Fixed "Start Selected" button in `Dolphin Profiles` tab to catch connection errors and display explicit error alerts (`messagebox.showerror`) instead of swallowing exceptions silently.
- Dropped legacy local scraping UI actions ("Run Scraper", "Stop Scraper") to enforce 24x7 server execution model.
- Fixed VPS "Worker Logs (Live)" stream not updating in real-time by adding `-tt` to SSH command to force pseudo-terminal allocation and prevent pipe buffering.
- Audited and cleared old local sqlite routes (`fb_account_...`) from `listings.db` to prevent synchronization of legacy routes to the VPS database.
- Confirmed VPS `worker_routes` utilizes PostgreSQL, and successfully cleaned up legacy configurations to mitigate worker quarantine logs.

### Milestone AC: Worker Container Stability & Per-Profile Health Tracking

Status: Completed

- Resolved `ECONNREFUSED` errors by setting `network_mode: "host"` for worker containers, allowing direct access to host-bound Chrome DevTools ports.
- Fixed Dolphin Anty profile footprint lock leaks by explicitly separating and releasing both the local Postgres `profile_lock_id` and the remote `dolphin_lock_id` in the worker cycle's `finally` block.
- Strengthened Dolphin Anty browser startup (`_start_dolphin_profile`) with exponential backoff and retries against `E_BROWSER_RUN_DUPLICATE` to accommodate delayed browser shutdown processing.
- Implemented Per-Profile Health Tracking:
  - Tracks consecutive failures per Dolphin profile ID, independently of the database route.
  - Automatically blacklists Dolphin profiles with consecutive failures for an explicit 30-minute cooldown window.
  - Worker route selection safely excludes blacklisted profiles to prevent continuous scrape failure loops.
  - Emits detailed Telegram alert upon profile blacklisting containing profile ID/name, failure count, and reason.
  - Profile failure counters reset upon the first successful scrape cycle on that profile.
- Removed the experimental 5-slot concurrent worker loop architecture to eliminate race conditions and excessive host memory pressure; concurrency reverts safely to scaling via discrete Docker container replicas (e.g., `worker_1`, `worker_2`, `worker_3`).

---

## 3. Decision Log

### Decision: Profit Formula Shift to Market-Realistic Basis

- Decision: calculate profit from listing asking price where available.
- Reason: comparing sell price to max-buy alone overstated opportunity quality.
- Impact: more realistic opportunity ranking and clearer downside risk.

### Decision: Preserve Unclassified/Needs-Pricing Listings

- Decision: keep discovered listings even when model/pricing is incomplete.
- Reason: prevents silent data loss and enables manual remediation.
- Impact: improved transparency and reviewability.

### Decision: Add User Flags at DB Level

- Decision: store operator marks (`user_flag`) in `listings`.
- Reason: manual trust/risk intent should survive refreshes.
- Impact: supports workflow triage without external notes.

### Decision: Keep Condition Logic Rule-Based for Now

- Decision: use keyword rules on card text as baseline.
- Reason: safer and lighter than full detail-page scraping at current ban-risk posture.
- Impact: lower scrape risk, but accuracy ceiling remains.

### Decision: Auto-Suppress Accessory-Only Listings

- Decision: reject listings that appear to sell accessories only (e.g., cases) rather than a handset.
- Reason: accessory noise was polluting listing queue and reducing operator efficiency.
- Impact: cleaner queue quality, with heuristic tuning still needed over time as listing phrasing evolves.

### Decision: Introduce Hybrid Marketplace Fetch

- Decision: capture and parse Marketplace GraphQL responses, with DOM extraction as fallback.
- Reason: GraphQL returns cleaner structured listing fields while fallback preserves resilience when payload formats shift.
- Impact: improved listing data quality without removing existing browser-safe behavior.

### Decision: Shift to Server-First 24x7 Scraping + Push Sync

- Decision: migrate scraping runtime from desktop-triggered batch loops to VPS-hosted continuous workers with central persistence and live event streaming.
- Reason: current local flow cannot meet always-on operation, second-level ingest visibility, or multi-worker throughput goals.
- Impact: desktop app becomes operator client; ingestion and state authority move to server stack.

### Decision: Treat Capacity Contention as WAIT, Not Failure

- Decision: classify no-proxy and profile-lock contention as capacity-wait outcomes (`WAIT_PROXY`, `WAIT_PROFILE_LOCK`) instead of route failures.
- Reason: proxy/profile contention is an infrastructure-capacity condition, not a profile health signal, and should not burn bad-cycle counters.
- Impact: reduced cooldown cascades during temporary capacity drops; healthier route stability under concurrent worker load.

### Decision: Move Route Scheduling from Round-Robin to Due-Time (`next_run_at`)

- Decision: replace in-memory round-robin route selection with persisted due-time scheduling using `worker_routes.next_run_at` and optional `route_interval_seconds`.
- Reason: round-robin produced deterministic cadence, restart hotspotting, and inaccurate interval semantics under variable cycle durations.
- Impact: stable per-route cadence, reduced deterministic traffic patterns, and better control when route availability changes dynamically.

### Decision: Formalize Route Health as an Explicit State Machine

- Decision: persist route statuses (`ENABLED`, `DEGRADED`, `THROTTLED`, `COOLDOWN`, `NEEDS_LOGIN`, `DISABLED`) with reason/timestamp metadata and state-based interval multipliers.
- Reason: ad-hoc boolean/counter interpretation made recovery windows ambiguous and made operator diagnosis difficult.
- Impact: clearer operator visibility, wider recovery window before cooldown, and deterministic transition behavior in worker runtime.

### Decision: Use Proactive Soft-Signal Detection Before Hard Checkpoints

- Decision: run pure, post-cycle signal analysis and map elevated risk to proactive throttle/pause/quarantine actions.
- Reason: waiting for hard checkpoint/login challenges is reactive and burns profile/proxy reputation before mitigation starts.
- Impact: route cadence can back off earlier, reducing checkpoint incidence risk while preserving non-bypass safety posture.

### Decision: Treat Query-Shard Contention as Coordination WAIT

- Decision: lock query shards across workers and return `WAIT_QUERY_SHARD` when another worker currently owns the shard.
- Reason: overlapping query shards from multiple workers increase correlated bot-like traffic patterns and inflate false failures.
- Impact: reduced duplicate shard traffic and cleaner failure accounting (contention no longer increments bad-cycle counters).

### Decision: Enforce Minimum-Route Resilience with Warning + Runtime Guard

- Decision: warn via API when enabled routes per worker are below threshold and apply mandatory extended rest in single-route mode.
- Reason: strict blocking would be disruptive during incidents, but single-route operation needs explicit pacing protection.
- Impact: operators retain flexibility while runtime still avoids aggressive no-rotation scrape cadence.

### Decision: Add Quarantine Metadata + Operational Recovery Actions

- Decision: persist quarantine evidence (`quarantined_at`, `quarantine_reason`, `quarantine_evidence`) and expose retest/bulk-clear actions in API + GUI.
- Reason: boolean-only manual-login locks were operationally sticky and hard to audit/recover during multi-route incidents.
- Impact: faster operator recovery for shared failures and clearer incident context per route.

### Decision: Use Controlled Persona Variation with Native Playwright Options

- Decision: enable per-cycle persona variation using Playwright context options only (`viewport`, `screen`, `device_scale_factor`, `user_agent`, `timezone_id`, `locale`, `color_scheme`), without external stealth/evasion libraries.
- Reason: static context fingerprints across rotating proxies increase correlation risk and reduce route resilience during sustained operation.
- Impact: route cycles now carry coherent persona context and telemetry evidence (`persona_hash`) while keeping implementation bounded to native browser configuration APIs.

---

## 4. Roadmap Drift Audit (Manual/Additional Features)

The following were detected in implementation and added to roadmap because they were not explicitly captured in original baseline planning:

1. In-app price sheet editor with CSV write-back and recalculation trigger.
2. GUI scraper cancellation with progress callback plumbing.
3. Listing row color coding and persistence via `user_flag`.
4. Expanded listing state handling (`new`, `needs_pricing`, `unclassified`).
5. Profit recalculation logic tied to current listing price.
6. “Help > Setup & Launch Guide” in GUI for operational onboarding.
7. Hybrid GraphQL + DOM listing acquisition pipeline in scraper runtime.
8. Auth bootstrap hardening (`auth.json` auto-import + cleanup) and explicit auth health command.
9. Resale reconciliation and daily summary commands for operational lifecycle completeness.
10. GUI-managed multi-account and proxy configuration with persisted runtime settings.
11. Proxy API integration for on-demand high-volume proxy import.
12. SOCKS5-only manual account login workflow from GUI with persisted login artifacts.
13. Scraper account pool round-robin rotation with strict ACTIVE+SOCKS5 eligibility enforcement.
14. CLI monitor/scrape parity with GUI account rotation and proxy routing controls.
15. DB-backed search query registry (`search_queries`) with per-query `last_polled` tracking.
16. Runtime account health counters/cooldown tracking with auto exclusion of active cooldown windows.
17. Proxy quick-import workflow from pasted cURL/proxy strings in settings UI.
18. Proxy supplier payload normalization across mixed response shapes and plain-text endpoint lines.
19. Model identification coverage extended to iPhone 16 family names.
20. Account browse-session closure now persists refreshed cookies and user-agent metadata.
21. VPS worker route management + heartbeat APIs with GUI CRUD/status view.
22. Progressive page-depth scrolling and telemetry to avoid first-batch-only listing capture.
23. Rotation reuse gating shifted to dedicated `last_scrape_started_at` tracking.
24. VPS API route recovery/redeploy flow documented and validated after production `404 /worker-routes` failure.
25. VPS build-context hardening to avoid runtime/data permission errors during worker image rebuilds.
26. Virtualized-feed-safe DOM snapshot aggregation plus feed-container-aware scrolling for deeper listing capture.

27. Scraper module refactored from monolithic `scraper.py` into structured package (`scraper/`) with config/storage/parsers/proxy/browser/core sub-modules and backward-compatible `__init__.py` re-exports.
28. Standalone event-driven notification worker (`notification_worker.py`) with Redis pub/sub, priority-tier dispatch, FCM push, rate limiting, and deduplication.
29. Proxy provider real-time health monitor (`proxy_monitor.py`) with async polling, utilization tracking, pacing multiplier computation, and Telegram degradation alerts.
30. Dolphin Anty profile management GUI tab with Fetch/Start/Stop controls and API key integration (`dolphin_api_key` setting + Bearer auth headers on all requests).

Action taken: all above are now documented in `roadmap.md` as completed scope.

---

## 5. Process Enhancements Across the System

### Scraping Process

- Added query-level progress events for operator observability.
- Added cooperative stop behavior to avoid hard-kill instability.
- Improved listing card extraction and candidate selection scoring.
- Added GraphQL feed-response parsing with automatic fallback to DOM card extraction.
- Relaxed GraphQL request gating so feed capture is not dropped by strict request-body keyword checks.
- Added source-level count telemetry for GraphQL vs DOM extraction coverage.
- Replaced shallow fixed scroll behavior with progressive page-depth scrolling (target cards + max rounds).
- Added page-depth telemetry (`page_cards`, `scroll_rounds`) to query diagnostics and runtime logging.
- Added DB-backed active query loading from `search_queries` with fallback defaults.
- Added per-query `last_polled` updates on each processed query.
- Hardened price parsing for mixed marketplace formats (`$300`, `300 AU$`, `1.000 AU$`) to reduce null-price ingestion.
- Added currency-aware fallback extraction from title/description when structured price field is missing in feed payloads.
- Added shorthand-thousands (`k`) price parsing support (`$1.4k`, `A$1.65k`) for structured and text-derived price values.
- Tightened string price parsing to prefer currency-tagged tokens and avoid model-number false positives.
- Expanded model matcher to include iPhone 16 family labels for forward compatibility.

### Evaluation Process

- Recalculation path allows backfilling all listings when price sheet changes.
- Status normalization avoids hiding unresolved listings.
- Added richer listing context persistence (`location`, `description`, `seller_name`) for better downstream reasoning.

### Operator Workflow Process

- Added menu-level access to price-sheet and analytics workflows.
- Added visual triage markers in listings table.
- Hid `Unknown`/blank model rows from default listings feed to keep operator queue focused.
- Improved button behaviors by normalizing selected/focused listing retrieval.
- Added quick triage filters in GUI (`All`, `New`, `High Profit`, `iPhone 14+`).
- Added a centralized settings manager for account/proxy setup without manual DB edits.
- Added browser-session persistence flow so account cookies/user-agent can be refreshed via operator browsing.

### Auth and Credential Process

- Added normalized auth provider detection (Gemini/OpenAI) and token-expiry checks.
- Added one-time raw credential import flow with cleanup of bootstrap auth file.

### Notification Process

- Reduced noise by notifying only listings with non-negative potential profit (default threshold `>= 0`).
- Added Telegram per-listing summary card format including link + price + description snippet for faster triage.
- Added explicit daily summary trigger for operator-level reporting.

### Connection and Runtime Control Process

- Added structured account lifecycle states (`ACTIVE`, `COOLDOWN`, `NEEDS_LOGIN`, `BANNED`) in GUI.
- Added explicit proxy inventory with assignment visibility to accounts.
- Added persisted runtime controls for scraper pacing and query volume from GUI.
- Added monitor jitter runtime control (`monitor_jitter_seconds`) and wired CLI monitor sleeps to randomized intervals.
- Added minimum account reuse control (`account_min_reuse_seconds`) in account rotation selection for GUI and CLI runs.
- Rotation reuse timing now keys off `last_scrape_started_at` to avoid false reuse filtering from unrelated account updates.
- Added runtime account failure counters with automatic transitions:
  - `NEEDS_LOGIN` on auth-like failures
  - `COOLDOWN` on rate-limit-like failures or repeated generic failures
  - reset back to `ACTIVE` on successful scrape runs
- Added proxy supplier API ingestion path to keep proxy pool refreshed without manual entry.
- Added proxy ingestion parser support for mixed provider payload schemas and plaintext endpoint responses.
- Added operator quick-import for pasted cURL/proxy lines to accelerate bulk onboarding.
- Added operator file import path for SOCKS5 proxy lists from TXT/CSV in the Proxies settings tab.
- Added manual proxied login process with timeout-based cookie detection (`c_user`) and automated account status updates.
- Added automatic SOCKS5-auth bridge bootstrap/teardown around manual login for account isolation.
- Added strict scraper-account reservation and round-robin account rotation so each run uses a configured account proxy/profile pair.
- Added GUI-managed VPS scraper route CRUD/status using server API (`worker_routes`, `worker_heartbeats`) to reduce manual server edits.
- Added VPS route-level proxy mode controls (`fixed` / `auto_rotation`) with optional proxy-pool import from TXT/CSV.
- Added shared runtime context builder in scraper layer to enforce proxy/account routing consistently in CLI monitor and one-shot scrape.
- Added selector logic to include expired cooldown accounts while excluding active cooldown windows.
- Added recalculation-time price backfill for legacy rows with missing prices using currency-aware text inference.
- Added server worker retry policy for transient scrape failures plus configurable degraded gating by consecutive bad cycles.
- Added server worker auto-cooldown for repeatedly failing routes/profiles and automatic fallback to the next available route in rotation.
- Added checkpoint/login-challenge detection in scraper runtime and surfaced it as a manual-login-required error signal.
- Added worker quarantine behavior for checkpointed profiles: mark route `manual_login_required`, remove it from rotation eligibility, and persist reason/timestamp.
- Added Telegram alert for manual-login-required quarantine with worker/profile/route context so operator can intervene quickly.
- Added VPS Scrapers route form fields to view/override `manual_login_required` state and reason from GUI.
- Added VPS Scrapers connection controls for default proxy rotation and profile rotation periods, with automated VPS `.env` apply + worker restart action.
- Separated scrape frequency controls by worker in VPS Scrapers connection settings:
  - `Worker 1 Scrape Frequency (seconds)` -> `SCRAPE_INTERVAL_SECONDS`
  - `Worker 2 Scrape Frequency (seconds)` -> `SCRAPE_INTERVAL_SECONDS_WORKER_2`
  - `Worker 3 Scrape Frequency (seconds)` -> `SCRAPE_INTERVAL_SECONDS_WORKER_3`
- Added compose/env support for worker_2 interval override key (`SCRAPE_INTERVAL_SECONDS_WORKER_2`) and kept compatibility fallback behavior for existing deployments.
- Added worker runtime cadence controls:
  - `SCRAPE_INTERVAL_JITTER_PCT`
  - `WORKER_WAIT_BACKOFF_SECONDS`
  - `WORKER_MAX_BACKOFF_MULTIPLIER`
  - `WORKER_CYCLE_RETRY_BACKOFF_MAX_SECONDS`
- Worker scheduling now persists next-run timestamps (`next_run_at`) and honors route-level interval overrides (`route_interval_seconds`).
- Worker cycle error handling now uses category-aware retries with exponential backoff for transient classes only.
- Added API validation to block duplicate enabled route `user_data_dir` assignments and prevent runtime profile collisions.
- Added query-shard deduplication locks with non-failure `WAIT_QUERY_SHARD` outcomes for cross-worker shard overlap.
- Added pre-checkpoint soft-signal detector with risk-scored actions (`throttle`, `pause`, `quarantine`) and route baseline tracking (`avg_result_count`, `avg_page_load_ms`).
- Added route status-state persistence and transitions (`ENABLED`/`DEGRADED`/`THROTTLED`/`COOLDOWN`/`NEEDS_LOGIN`/`DISABLED`) plus interval multipliers for degraded states.
- Added proxy health memory table (`proxy_stats`) and candidate scoring/ban logic to avoid repeated use of recently failing proxies.
- Added API-level `<2 enabled routes` warnings and worker-side single-route extended-rest enforcement to reduce no-rotation ban risk.
- Added Dolphin Anty profile management in GUI (`Dolphin Profiles` tab) with Fetch/Start/Stop profile controls and authenticated Bearer-token API requests.

### Code Quality and Maintainability Process

- Refactored monolithic scraper module into Python package with clean domain separation.
- Backward-compatible re-export surface ensures zero downstream breakage.

### Notification Process (Enhanced)

- Added standalone event-driven notification worker consuming Redis events for sub-second push delivery.
- Added Firebase Cloud Messaging (FCM) push path for native iOS/Android notifications.
- Added priority-tier notification dispatch (instant/batch/suppressed) to reduce operator notification fatigue.
- Added sliding-window rate limiting and deduplication in notification delivery pipeline.

### Proxy Provider Monitoring Process

- Added real-time proxy gateway utilization/error-rate polling with pacing multiplier integration.
- Added automated Telegram alerts for proxy provider degradation events.

### Dolphin Anty Integration Process

- Added GUI-managed Dolphin profile lifecycle controls (fetch/start/stop) with tabular profile visibility.
- Added secure API key management with masked entry, SQLite persistence, and automatic Bearer-token injection.

---

## 6. Titan Spec Audit Delta (2026-02-09)

Audit reference: `/Users/ishanrathnayaka/Downloads/Titan_Scraper_Ultimate_Spec.md`

Implemented immediately from audit:

- Added anti-pattern fix for price extraction where feed/title strings include `amount + currency` format (`300 AU$...`) instead of structured price fields.
- Added support for thousand-separator dot format (`1.000 AU$`) in parser normalization.
- Added support for shorthand-thousands suffix parsing (`1.4k`, `1.65k`) during ingestion.
- Added recomputation/backfill path so legacy rows with null prices can be repaired through financial recalculation.
- Added randomized monitor jitter control to avoid perfectly periodic polling cadence.
- Added minimum account-reuse delay control to reduce rapid identity reuse in rotation.
- Added DB-backed `search_queries` registry and per-query `last_polled` state updates during scrape runs.
- Added runtime account health counters (`failure_count`, `cooldown_until`) with automatic cooldown/login state transitions.

Still out of current local-app scope (documented, not removed):

- Distributed service split (Manager/Worker/Harvester/Notifier as separate deployable services).
- Redis queue + pub/sub transport layer.
- PostgreSQL multi-tenant schema with users/search_queries ownership model.
- Direct GraphQL POST worker via `curl_cffi` with captured `doc_id/fb_dtsg/lsd` payload orchestration.

---

## 7. Current Risks and Gaps

1. Condition inference is still keyword/rule-based and can miss nuance even with better listing metadata.
2. No condition confidence signal yet to gate automation decisions.
3. Automated coverage is still limited (runtime scheduling helpers covered, but broader scraper/API integration coverage and CI are still missing).
4. Browser profile and auth artifacts require stronger operational security guidance.
5. Structured cycle telemetry rollup script exists, but no always-on dashboard or automated report delivery pipeline is wired yet.
6. Desktop GUI server-sync is currently cursor-poll based; direct WebSocket consumer path is still pending.
7. Notification worker (`notification_worker.py`) is implemented but requires compose service entry and deployment wiring to run as production container.
8. Query-shard lock deduplication is implemented, but higher-level shard planning/validation (conflict prevention at config time, load-balancing heuristics) still needs hardening.
9. Persona variation currently covers native context-level traits only; deeper browser-surface controls (e.g., strict geo-IP datasets and broader persona QA) still need hardening and monitoring.
10. ~~Dolphin Anty installation on VPS is pending~~ — resolved: Dolphin Anty installed on VPS with TigerVNC + systemd autostart; GUI profile management operational.
11. ~~Proxy provider monitor pacing multiplier not wired~~ — resolved: worker slot loop now applies `proxy_health.pacing_multiplier` to `next_interval_seconds` and skips cycles entirely when provider is saturated (`wait_proxy_provider` heartbeat state).
12. V2.2 core scraper package (`core/scraper/config.py`) and V1 scraper package (`scraper/config.py`, `scraper/__init__.py`) still carry bridge/connection-limiting configs (`FORCE_LOCAL_PROXY_BRIDGE`, `PROXY_BRIDGE_*`) that were rolled back from the worker runtime path in dev-log §25. Config symbols are importable but have no active runtime consumer. Cleanup or re-activation decision is pending.
13. `scraper/legacy_utils.py` preserves V1 monolithic scraper logic (GraphQL extraction, DOM extraction, progressive scroll, stealth scripts) as shared utilities. No automated tests cover these utilities directly; coverage relies on integration-level scrape runs.
14. ~~Worker slot concurrency (`WORKER_CONCURRENCY = 5` hardcoded in `_run_worker_loop`) is not operator-configurable via env var.~~ **Resolved** – concurrency removed entirely (§34). Workers now run single-loop-per-container; parallelism via Docker compose.
15. Current worker pub/sub event payload shape and `notification_worker.py` parse expectations do not fully align. This is now explicitly documented and deferred to the later Redis event-spine / notification-consumer phases, not fixed in V3.0 Phase 0.

---

## 8. Active Work Queue (Aligned to Roadmap)

### Priority 1

- Review, test, and approve V3.0 Phase 0 (Redis flags + baseline observability) before starting any V3.0 behavior-change phase.
- Complete Phase 5A desktop sync integration by adding direct WebSocket apply on top of the shipped cursor-poll baseline.
- Implement `condition_confidence` tiers and manual-review routing.
- Add selective detail-page enrichment for low-confidence/high-value listings.
- Extend GUI-managed remote worker control from route assignment/status to full deploy/restart lifecycle actions.

### Priority 2

- Add automated tests for pricing/condition/scoring logic.
- Add automated telemetry rollup scheduling/report delivery (dashboard or periodic artifacts) on top of current CLI script.
- Add worker health metrics and reconnect-safe desktop catch-up cursor.

### Priority 3

- Add advanced GUI filters (confidence/manual-review/user-flag presets).
- Add backup/export flows for DB and operational snapshots.

---

## 9. Operator-Side Migration Checklist (Approved)

1. Provision VPS host and secure SSH-only administration.
2. Provision PostgreSQL and generate least-privilege app credentials.
3. Provision domain + TLS endpoint for API/WebSocket traffic.
4. Prepare server secret storage for proxy credentials, notification tokens, and AI keys.
5. Validate each account/profile + SOCKS5 proxy pair on server environment.
6. Confirm desired parallelism/pacing policy before enabling continuous workers.
7. Keep desktop environment ready to maintain outbound HTTPS connectivity to server (`443`), with WebSocket support reserved for next sync stage.

---

## 10. Next Update Protocol

For each future update, append:

- date/time
- change summary
- files touched
- decision/rationale (if behavior changed)
- validation performed
- unresolved follow-ups

This keeps `dev-log.md` actionable for both engineering and operations.

---

## 11. 2026-02-16 Hotfix: Worker Scrape Invocation + Deploy Sync Gap

- Date/time: 2026-02-16
- Change summary:
  - fixed worker compatibility so `server/services/worker/worker.py` only passes scraper kwargs supported by the current `scrape_marketplace` signature (prevents `unexpected keyword argument 'verify_proxy_ip'` crash loops)
  - fixed deployment drift in `server/scripts/deploy_vps.sh` by syncing root runtime files (`scraper.py`, `notifications.py`, `requirements.txt`, `.dockerignore`) in addition to `/server`
  - added GUI operator controls to start/stop worker services and exposed VPS timing controls (`Proxy Reuse Cooldown`, `Proxy Lease TTL`, per-route `Route Interval`)
- Files touched:
  - `server/services/worker/worker.py`
  - `server/scripts/deploy_vps.sh`
  - `gui.py`
- Decision/rationale:
  - remote workers were importing stale `/app/scraper.py` after deploy (script synced only `/server`), causing repeated fail cycles that incremented `proxy_stats.consecutive_failures` and banned all auto-rotation proxies
  - compatibility layer and deploy sync close both immediate failure and recurrence vectors
- Validation performed:
  - local `py_compile` for modified modules
  - VPS redeploy completed successfully
  - verified running worker imports now show updated `scrape_marketplace` signature including proxy verification args
  - cleared proxy bans (`proxy_stats`) and observed workers resume real query execution in logs
- Unresolved follow-ups:
  - add explicit GUI/API action to reset proxy health bans without direct SQL

---

## 12. 2026-02-16 Hotfix: Proxy Ban Controls + Route List Resilience

- Date/time: 2026-02-16
- Change summary:
  - added API proxy-health operator endpoints:
    - `GET /proxy-stats` (ban/failure visibility)
    - `POST /proxy-stats/reset` (clear selected proxy ban, optionally reset failures)
  - updated Proxies tab to show server-side proxy health columns (`Failures`, `Banned`, `Ban Until`)
  - added Proxies tab operator actions:
    - `Refresh Proxy Stats`
    - `Reset Selected Proxy Ban`
  - hardened VPS route-table rendering to avoid one duplicate/dirty row key breaking visibility of other routes
  - switched route-action identity resolution (`delete`, `retest`, `bulk clear`) to row payload identity rather than fragile tree key parsing
- Files touched:
  - `server/services/api/app/main.py`
  - `server/tests/test_api_proxy_stats.py`
  - `gui.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Decision/rationale:
  - operators needed direct in-UI proxy-ban control after no-proxy-available waves
  - ban state belongs in the Proxies control surface (proxy inventory/operations), not mixed into VPS route editing
  - route visibility must be resilient even when upstream data includes duplicate/dirty route keys
- Validation performed:
  - local compile check: `python3 -m compileall gui.py server/services/api/app/main.py`
  - unit test check (environment without FastAPI deps): `python3 -m unittest server/tests/test_api_proxy_stats.py` (tests skipped as designed)
- Unresolved follow-ups:
  - deploy latest API container to VPS so desktop can consume `/proxy-stats` and `/proxy-stats/reset` immediately

---

## 13. 2026-02-16 Hotfix: Worker Status Offline + VPS Route List Truncation

- Date/time: 2026-02-16
- Change summary:
  - fixed `_compact_proxy_host()` to handle non-standard lease endpoints (`query-shard://...`) without raising `ValueError` on non-numeric pseudo-port segments
  - added per-route defensive handling in VPS route-table refresh loop so one malformed row cannot abort rendering of all remaining routes
- Root cause:
  - `/worker-health` lease rows can include query-shard lock identifiers (not real proxy host:port), and prior host-compaction logic assumed numeric ports
  - resulting exception interrupted both:
    - main worker-pill refresh loop (left strip showing offline)
    - VPS route-table refresh loop (stopped after early rows, showing only ~2 routes)
- Files touched:
  - `gui.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Validation performed:
  - verified live API currently returns `worker-routes.count=10` and `worker-health.count=3` (workers `worker`, `worker_2`, `worker_3`)
  - local compile check: `python3 -m compileall gui.py`

---

## 14. 2026-02-16 Enforcement Update: Sticky Proxy Pairing + Hard Session Lease Rules

- Date/time: 2026-02-16
- Change summary:
  - added route-level sticky proxy memory fields:
    - `worker_routes.preferred_proxy_key`
    - `worker_routes.preferred_proxy_updated_at`
  - worker now persists preferred proxy key on successful cycles and uses preferred-first selection on next cycle
  - preferred proxy is skipped only when unavailable, banned, or unhealthy (near-ban failure threshold)
  - added active lease keepalive for:
    - proxy lease
    - profile lock lease
    - query-shard lock lease
  - scrape cycle now aborts into wait-proxy path if active lease is lost during session, preventing silent overlap
  - API route payloads now include sticky preference metadata for operator visibility (`GET /worker-routes`, upsert/retest responses)
- Files touched:
  - `server/services/worker/worker.py`
  - `server/services/worker/lease_manager.py`
  - `server/services/common/schema_ensure.py`
  - `server/services/api/sql/001_init.sql`
  - `server/services/api/app/main.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Decision/rationale:
  - enforce non-negotiable rule: one proxy per active session/profile with no concurrent sharing
  - keep profile behavior realistic and stable via sticky route-to-proxy affinity while preserving graceful fallback on degraded proxies
- Validation performed:
  - local compile checks:
    - `python3 -m compileall server/services/worker/worker.py`
    - `python3 -m compileall server/services/worker/lease_manager.py`
    - `python3 -m compileall server/services/api/app/main.py`
    - `python3 -m compileall server/services/common/schema_ensure.py`

---

## 15. 2026-02-16 VPS Rollout: Sticky/Lease Enforcement Activated

- Date/time: 2026-02-16
- Change summary:
  - deployed latest API + worker images to VPS using `server/scripts/deploy_vps.sh`
  - applied SQL bootstrap/migrations on VPS Postgres during deploy
  - restarted runtime services (`api`, `worker`, `worker_2`, `worker_3`) under compose
- Validation performed:
  - API health check passed: `GET /healthz` => `{"ok":true,"db":true,"redis":true}`
  - worker health check passed: `GET /worker-health` returned 3 live workers
  - route inventory check passed: `GET /worker-routes` returned `count=9` (`worker`, `worker_2`, `worker_3`)
  - compose status healthy for `api`, `postgres`, `redis`, `worker`, `worker_2`, `worker_3`
  - recent service logs contain no `ImportError`, `ModuleNotFoundError`, or traceback startup failures
- Follow-up resolution:
  - closes prior unresolved follow-up from section 12 ("deploy latest API container to VPS")

---

## 16. 2026-02-16 Hotfix: Manual Login Route Save 500 (worker-routes upsert)

- Date/time: 2026-02-16
- Incident:
  - manual login flow failed when saving route (`PUT /worker-routes/worker/profile_1`) with `500 Internal Server Error`
  - API traceback: `asyncpg.exceptions.PostgresSyntaxError: INSERT has more target columns than expressions`
- Root cause:
  - `worker_routes` upsert SQL in API had target/value mismatch after sticky fields (`preferred_proxy_key`, `preferred_proxy_updated_at`) were added to insert columns
- Fix:
  - corrected insert `VALUES` list to explicitly set sticky fields as server-managed (`NULL, NULL`) while preserving existing parameter mapping for operator payload fields
  - this prevents column-count mismatch and avoids accidental client override of sticky preference fields
- Files touched:
  - `server/services/api/app/main.py`
  - `dev-log.md`
  - `roadmap.md`
  - `architecture.md`
- Validation performed:
  - local compile check: `python3 -m compileall server/services/api/app/main.py`
  - test suite check: `python3 -m unittest discover -s server/tests -p 'test_*.py'`
  - VPS deploy: `DEPLOY_SERVICES=api ./server/scripts/deploy_vps.sh`
  - live endpoint retest: `PUT /worker-routes/worker/profile_1` now returns `200` with updated route payload

---

## 17. 2026-02-16 UX Hardening: VPS Route Save Error Diagnostics

- Date/time: 2026-02-16
- Change summary:
  - improved VPS scraper route save error handling in GUI to show:
    - HTTP status code
    - API `detail` field when present
    - response body snippet fallback
- Why:
  - operators were seeing generic save-failure popups without enough context to distinguish `401`, `409`, and server-side `500` regressions
- Files touched:
  - `gui.py`
  - `dev-log.md`
  - `roadmap.md`
  - `architecture.md`
- Validation performed:
  - local compile check: `python3 -m compileall gui.py`
  - live API verification: public `PUT /worker-routes/{worker}/{route}` returns `200` for all current routes (`failed=0`)

---

## 18. 2026-02-16 Hotfix: Repeated Profile-Failure Alerts Not Entering Cooldown Fast Enough

- Date/time: 2026-02-16
- Incident:
  - repeated Telegram profile-failure alerts were observed for:
    - `worker_3/profile_11`
    - `worker_2/profile_5`
  - routes remained in `DEGRADED`/`THROTTLED` without entering cooldown as expected by configured policy.
- Root cause:
  - worker startup logic forced:
    - `WORKER_ROUTE_COOLDOWN_BAD_CYCLES >= WORKER_THROTTLED_CONSECUTIVE_CYCLES + 1`
  - this silently overrode operator-set cooldown thresholds (for example configured `4` effectively became `6` when throttled threshold was `5`).
  - in-memory bad-cycle tracker also started from `0` after worker restart, ignoring persisted DB `consecutive_failures`.
- Fix:
  - made cooldown threshold independently operator-configurable:
    - `WORKER_ROUTE_COOLDOWN_BAD_CYCLES` now honors env value directly (minimum `1`), defaulting to `throttled_after + 1` only when unset.
  - added DB-seeded bad-cycle initialization:
    - if in-memory counter is missing, worker seeds from route `consecutive_failures` so restart does not erase failure progression.
  - applied immediate operational mitigation by setting cooldown on affected routes:
    - `worker_2/profile_5`
    - `worker_3/profile_11`
- Files touched:
  - `server/services/worker/worker.py`
  - `dev-log.md`
  - `roadmap.md`
  - `architecture.md`
- Validation performed:
  - local compile check: `python3 -m compileall server/services/worker/worker.py`
  - test suite check: `python3 -m unittest discover -s server/tests -p 'test_*.py'`
  - VPS worker rollout: `DEPLOY_SERVICES=\"worker worker_2 worker_3\" ./server/scripts/deploy_vps.sh`
  - runtime startup logs now show configured threshold being honored:
    - `cooldown_after=4` on `worker`, `worker_2`, and `worker_3`

---

## 19. 2026-02-17 Feature: Global Proxy Concurrency Cap (Provider Limit Protection)

- Date/time: 2026-02-17
- Change summary:
  - added global proxy session cap setting:
    - env/runtime key: `MAX_CONCURRENT_PROXY_CONNECTIONS` (default `5`)
  - enforced cap inside lease acquisition path (`try_acquire_proxy_lease`) with transaction-scoped advisory lock to prevent race oversubscription across workers
  - cap counts only active real proxy leases and excludes synthetic profile/query-shard locks:
    - excludes `PROFILE:*`
    - excludes `QUERY_SHARD:*`
  - added VPS Scrapers UI field:
    - `Max Concurrent Proxy Connections`
  - Save VPS Connection now persists and applies this value to server `.env`, then restarts worker services.
- Behavior:
  - when active proxy leases reach cap, new lease acquisition returns `None`
  - route naturally falls into existing `WAIT_PROXY` path (no failure counting, no cooldown penalty)
- Files touched:
  - `scraper.py`
  - `server/services/worker/lease_manager.py`
  - `server/services/worker/worker.py`
  - `server/infra/docker-compose.yml`
  - `server/.env.example`
  - `gui.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Validation performed:
  - local compile checks:
    - `python3 -m compileall server/services/worker/lease_manager.py`
    - `python3 -m compileall server/services/worker/worker.py`
    - `python3 -m compileall gui.py`
  - test suite:
    - `python3 -m unittest discover -s server/tests -p 'test_*.py'`
  - VPS rollout:
    - `DEPLOY_SERVICES=\"worker worker_2 worker_3\" ./server/scripts/deploy_vps.sh`
- runtime verification:
  - worker startup log now includes `max_proxy_connections=5` for `worker`, `worker_2`, and `worker_3`

---

## 20. 2026-02-17 Hotfix: Provider Limit Pressure Recheck (Socket Fan-Out + Manual-Login Loop)

- Date/time: 2026-02-17
- Incident:
  - operator still observed large provider-side "Limit Reached" counts after enabling `MAX_CONCURRENT_PROXY_CONNECTIONS=5`.
- Findings:
  - live worker logs showed no direct `limit reached`, `too many connections`, `429`, or `rate limit` strings in recent runtime output.
  - live socket sampling inside worker containers showed one active profile session opening many concurrent proxy sockets:
    - `worker`: `165.49.210.38:12324` had `12` established sockets
    - `worker_2`: `92.71.71.248:6442` had `13` established sockets
  - scraper query loop was continuing across all remaining queries even after `MANUAL_LOGIN_REQUIRED` was detected on the first failed query, inflating unnecessary proxy traffic.
  - worker heuristic `_is_profile_failed` treated `query_count > 0` + `query_result_count = 0` as non-failure, allowing bad cycles to be misclassified as `ok`.
- Fix:
  - scraper now terminates the cycle immediately when a `MANUAL_LOGIN_REQUIRED` query error appears and raises the error to worker runtime.
  - added browser-level connection fan-out guardrails in scraper launch:
    - `BROWSER_MAX_CONNECTIONS_PER_PROXY` (default `1`)
    - `BROWSER_MAX_CONNECTIONS_PER_HOST` (default `1`)
    - `BROWSER_BLOCK_RESOURCE_TYPES` (default `image,media,font,websocket,manifest`)
    - transport guardrails: `--disable-quic`, `--disable-http2`
  - updated worker profile-failure heuristic to fail cycles when queries started but zero query results were produced.
- Files touched:
  - `scraper.py`
  - `server/services/worker/worker.py`
  - `server/.env.example`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Validation performed:
  - local compile checks:
    - `python3 -m compileall scraper.py`
    - `python3 -m compileall server/services/worker/worker.py`
    - `python3 -m compileall gui.py`
  - test suite:
    - `python3 -m unittest discover -s server/tests -p 'test_*.py'`
  - live runtime diagnostics:
    - confirmed active real proxy lease set and lease cap config on VPS
    - confirmed high per-session socket fan-out in running worker containers before this patch
- Remaining operational constraint:
  - provider-side "5 concurrent connections" is below observed browser-session fan-out in some cycles (single active browser can still spike above `5` concurrent upstream sockets), so residual provider limit events may still occur until provider connection allowance is increased or scraping transport is moved off full-browser mode.

---

## 21. 2026-02-17 Feature: Strict Local Proxy Bridge Limiter (Queue + Hard Cap)

- Date/time: 2026-02-17
- Change summary:
  - replaced ad-hoc auth-only SOCKS bridge path with a dedicated local SOCKS5 bridge implementation that enforces:
    - hard upstream socket cap per active browser session
    - queued waits with timeout when cap is reached
  - worker proxy path now forces SOCKS5 routes through local bridge by default (`WORKER_FORCE_LOCAL_PROXY_BRIDGE=1`)
  - added worker bridge controls:
    - `WORKER_PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS` (default `1`)
    - `WORKER_PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS` (default `30`)
    - `WORKER_PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS` (default `15`)
  - added VPS Scrapers connection fields to persist/apply bridge controls to server `.env` from GUI.
- Rule impact:
  - existing hard rules remain intact and unchanged:
    - one proxy per session/profile
    - sticky proxy-profile preference
    - no simultaneous proxy sharing
  - limiter is additive: it constrains per-session upstream proxy socket fan-out, it does not alter lease/sticky routing behavior.
- Files touched:
  - `scraper.py`
  - `server/services/worker/worker.py`
  - `server/infra/docker-compose.yml`
  - `server/.env.example`
  - `gui.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Validation performed:
  - local compile checks:
    - `python3 -m compileall scraper.py`
    - `python3 -m compileall server/services/worker/worker.py`
    - `python3 -m compileall gui.py`
  - test suite:
    - `python3 -m unittest discover -s server/tests -p 'test_*.py'`
- VPS rollout:
  - `DEPLOY_SERVICES=\"worker worker_2 worker_3\" ./server/scripts/deploy_vps.sh`

---

## 22. 2026-02-17 Hotfix: Proxy-Pressure Stabilization + False Manual-Login Reduction

- Date/time: 2026-02-17
- Incident report:
  - provider realtime usage endpoint showed high cumulative thread-cap failures (`threadLimitReachedErrors`), with low relative successful connection volume.
  - operator observed zero-scrape cycles and multiple manual-login-required quarantines after aggressive proxy-limiter settings rollout.
- Root cause analysis:
  - global cap (`MAX_CONCURRENT_PROXY_CONNECTIONS`) was enforced as *lease/session count* only, not estimated upstream proxy-thread budget.
  - strict transport defaults (`bridge cap=1`, browser per-proxy/per-host cap `1`) over-constrained Marketplace page loading and increased transport failure pressure.
  - manual-login classification accepted broad generic phrases, which could over-classify non-auth errors during high transport contention.
- Fixes implemented:
  - lease manager now applies weighted capacity checks:
    - new lease check uses `active_leases * estimated_connections_per_lease`
    - acquisition waits (`WAIT_PROXY`) when projected total would exceed `MAX_CONCURRENT_PROXY_CONNECTIONS`
    - same worker+route owner can now reacquire its own active proxy lease after restart (no forced wait for old TTL expiry)
  - worker now passes bridge-aware estimate (`WORKER_PROXY_CONNECTIONS_PER_LEASE_ESTIMATE`) into lease acquisition.
  - tightened manual-login markers/classification to favor explicit checkpoint/manual-login signatures instead of broad generic wording.
  - VPS settings save path now enforces safer bridge minimum when local bridge is enabled:
    - `WORKER_PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS >= 2`
  - local SOCKS5 bridge now enforces idle tunnel recycling (`WORKER_PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS`, default `8`) to prevent slot starvation from long-lived keepalive sockets.
  - profile/query-shard lock acquisition now allows same worker+route owner reacquisition while lock is still active, preventing post-restart lock-stall loops.
  - runtime template defaults updated for safer Marketplace operation:
    - `WORKER_PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS=2`
    - `WORKER_PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS=8`
    - `BROWSER_MAX_CONNECTIONS_PER_PROXY=2`
    - `BROWSER_MAX_CONNECTIONS_PER_HOST=2`
- Files touched:
  - `server/services/worker/lease_manager.py`
  - `server/services/worker/worker.py`
  - `server/services/worker/runtime.py`
  - `gui.py`
  - `server/infra/docker-compose.yml`
  - `server/.env.example`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Validation performed:
  - compile checks:
    - `python3 -m py_compile server/services/worker/lease_manager.py server/services/worker/worker.py server/services/worker/runtime.py gui.py`
  - test suite:
    - `python3 -m unittest discover -s server/tests -p 'test_*.py'`

---

## 23. 2026-02-17 Throughput Profile: Keep worker_3 Active Under 5-Thread Provider Cap

- Date/time: 2026-02-17
- Goal:
  - maximize throughput under strict provider concurrent-thread limit (`5`) while keeping `worker_3` fast-lane continuously serviceable.
- Change summary:
  - added worker_3-specific bridge-cap override:
    - `WORKER_3_PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS` (default `1`)
  - added worker_3 reservation control:
    - `WORKER_3_RESERVED_PROXY_CONNECTIONS` (default `1`)
  - lease-manager capacity logic now supports priority reservation:
    - non-priority workers (`worker`, `worker_2`) cannot consume reserved capacity needed by `worker_3`
    - reservation is adaptive to current `worker_3` usage and remains bounded by global max
  - wired these controls to VPS Scrapers save/apply flow and server `.env` updates.
- Operational target configuration:
  - `MAX_CONCURRENT_PROXY_CONNECTIONS=5`
  - `WORKER_PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS=2` (worker/worker_2)
  - `WORKER_3_PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS=1` (worker_3)
  - `WORKER_3_RESERVED_PROXY_CONNECTIONS=1`
- Files touched:
  - `server/services/worker/lease_manager.py`
  - `server/services/worker/worker.py`
  - `server/infra/docker-compose.yml`
  - `server/.env.example`
  - `gui.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Validation performed:
  - compile checks:
    - `python3 -m py_compile server/services/worker/lease_manager.py server/services/worker/worker.py gui.py scraper.py`
  - test suite:
    - `python3 -m unittest discover -s server/tests -p 'test_*.py'`

---

## 24. 2026-02-17 Throughput Hardening: Fast Query Cycles + Effective Shard Deduping

- Date/time: 2026-02-17
- Incident context:
  - provider realtime usage sampled repeatedly at runtime showed low active threads (`threadsConnected` mostly `0-1`) while workers were still scraping.
  - workers were spending long time inside single route cycles (`~19 queries` per cycle in logs), increasing freshness latency.
- Root causes identified:
  - long per-route query loops monopolized each worker before route rotation.
  - query-shard dedupe key was derived from route `search_queries` only; routes with blank route-level queries could bypass shard dedupe despite using fallback query sets at runtime.
- Fixes implemented:
  - worker now resolves effective query list per cycle in this order:
    - route `search_queries`
    - worker `SCRAPER_QUERIES`
    - worker runtime DB active queries (`load_active_search_queries`)
  - added bounded per-cycle query batching with round-robin route coverage:
    - `WORKER_MAX_QUERIES_PER_CYCLE` (default `4`)
    - each cycle processes only a slice of effective queries, then rotates to next slice next cycle.
  - query-shard lock key now applies to the effective cycle query set, closing dedupe bypass for blank route-level query rows.
  - added VPS Scrapers UI controls (save + apply to server `.env`) for:
    - `WORKER_MAX_QUERIES_PER_CYCLE`
    - `WORKER_SCROLL_TARGET_CARDS`
    - `WORKER_SCROLL_MAX_ROUNDS`
  - docker compose defaults now pass these values to worker containers for consistent runtime behavior.
- Files touched:
  - `server/services/worker/worker.py`
  - `server/infra/docker-compose.yml`
  - `server/.env.example`
  - `gui.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`

---

## 25. 2026-02-17 Rollback: Revert Post-Proxy-Cap Iterations (Back To Pre-19 Baseline)

- Date/time: 2026-02-17
- Operator request:
  - rollback to state before the `MAX_CONCURRENT_PROXY_CONNECTIONS` feature request.
- Rollback scope applied:
  - reverted lease-manager global proxy-cap/weighted-capacity/reservation logic.
  - removed worker runtime controls tied to post-19 changes:
    - global cap / bridge limiter / worker_3 reservation / query-batch caps.
  - reverted worker proxy path to pre-bridge-limiter behavior (auth bridge only when credentials are present).
  - reverted VPS Scrapers connection form and save/apply flow by removing post-19 controls.
  - reverted compose/env templates by removing post-19 proxy-cap/bridge/query-batch variables.
  - reverted post-19 architecture/roadmap statements so docs match active code.
  - restored non-restrictive browser transport defaults and removed immediate manual-login early-break loop behavior.
- Files touched:
  - `server/services/worker/lease_manager.py`
  - `server/services/worker/worker.py`
  - `scraper.py`
  - `gui.py`
  - `server/infra/docker-compose.yml`
  - `server/.env.example`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`

---

## 26. 2026-02-17 Feature: Scraper Module Refactoring Into Package

- Date/time: 2026-02-17
- Change summary:
  - refactored monolithic `scraper.py` (~3000+ lines) into a well-organized Python package (`scraper/`):
    - `scraper/__init__.py` — backward-compatible re-export surface so existing imports (`from scraper import X`) continue to work
    - `scraper/config.py` — all constants, environment variable parsing, default values, and configuration definitions
    - `scraper/storage.py` — database operations: `init_db`, `save_listing`, `load_price_list`, account/query/setting persistence, `reserve_next_scraper_account_for_run`
    - `scraper/parsers.py` — model identification, condition assessment, price parsing, profit calculation, accessory detection
    - `scraper/proxy.py` — proxy bridge management, auth-bridge spawning, Playwright proxy configuration builder, runtime context assembly
    - `scraper/browser.py` — stealth scripts, browser context launcher, `random_delay` utility
    - `scraper/core.py` — main `scrape_marketplace()` entry point, `recalculate_listing_financials`, `purge_accessory_only_listings`
  - original `scraper.py` retained as a thin compatibility layer pointing to the package
- Design rationale:
  - monolithic file exceeded maintainability thresholds
  - sub-module boundaries follow logical domain separation (config/storage/parsing/proxy/browser/core)
  - backward compatibility ensures zero downstream import breakage in `gui.py`, `main.py`, `worker.py`, and test files
- Files touched:
  - `scraper/__init__.py` [NEW]
  - `scraper/config.py` [NEW]
  - `scraper/storage.py` [NEW]
  - `scraper/parsers.py` [NEW]
  - `scraper/proxy.py` [NEW]
  - `scraper/browser.py` [NEW]
  - `scraper/core.py` [NEW]
- Validation performed:
  - syntax check: `python3 -c "import ast; ast.parse(open('scraper/__init__.py').read())"`
  - import compatibility verified: existing `from scraper import scrape_marketplace` continues to resolve

---

## 27. 2026-02-20 Feature: Event-Driven Notification Worker (FCM + Priority Tiers)

- Date/time: 2026-02-20
- Change summary:
  - added standalone notification worker service (`server/services/worker/notification_worker.py`, 519 lines):
    - subscribes to Redis `listing_events` pub/sub channel for sub-second notification delivery
    - implements priority-tier dispatch:
      - **Instant tier**: listings with `potential_profit >= $50` are sent immediately via Telegram + FCM
      - **Fast-batch tier**: listings with `potential_profit >= $0` are queued and flushed every 10 seconds
      - **Suppressed tier**: negative-profit listings are silently dropped
    - Telegram delivery via Bot API with MarkdownV2 card formatting
    - Firebase Cloud Messaging (FCM) push notification delivery:
      - topic-based push to subscribed iOS/Android devices (`FCM_TOPIC`, default `new_iphones`)
      - data payloads include model, price, profit, URL for deep-link handling
      - Firebase Admin SDK initialization from service account credentials or Application Default Credentials
    - rate limiter with sliding 60-second window (`NOTIFY_MAX_PER_MINUTE`, default `30`)
    - in-memory deduplication with configurable TTL window (`NOTIFY_DEDUP_WINDOW_SECONDS`, default `60`)
    - operational statistics tracking (`NotificationStats` dataclass) for received/sent/deduped/rate-limited/errored counts
    - health endpoint and stats API can be integrated into the main API service
  - data model: `ListingEvent` dataclass parsed from Redis JSON payloads
    - fields: `event_type`, `listing_id`, `model`, `price`, `profit`, `url`, `condition`, `description`, `source`, `worker_name`, `route_name`, `timestamp`
  - configurable environment variables:
    - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
    - `FCM_CREDENTIALS_PATH`, `FCM_TOPIC`
    - `NOTIFY_INSTANT_PROFIT_THRESHOLD`, `NOTIFY_BATCH_INTERVAL_SECONDS`
    - `NOTIFY_MAX_PER_MINUTE`, `NOTIFY_DEDUP_WINDOW_SECONDS`
- Decision/rationale:
  - decouples notification delivery from worker scraping runtime
  - event-driven architecture enables sub-second push latency for high-value listings
  - FCM integration enables native iOS push notifications for the companion mobile app
- Files touched:
  - `server/services/worker/notification_worker.py` [NEW]

---

## 28. 2026-02-20 Feature: Proxy Provider Real-Time Health Monitor

- Date/time: 2026-02-20
- Change summary:
  - added proxy provider health monitoring module (`server/services/worker/proxy_monitor.py`, 329 lines):
    - background async polling loop (`start_monitor` / `stop_monitor`) with configurable interval (`PROXY_PROVIDER_POLL_INTERVAL_SECONDS`, default `15s`)
    - polls proxy gateway realtime-usage API (`https://api.proxyrotator.com/proxy-gateways/realtime-usage`)
    - health snapshot dataclass (`ProxyProviderHealth`) tracking:
      - `threads_connected` / `threads_total` / `utilization_ratio`
      - `success_count` / `thread_limit_errors` / `error_rate`
      - `bandwidth_used_mb`
      - computed fields: `is_saturated`, `should_back_off`, `pacing_multiplier`
    - pacing multiplier calculation based on utilization and error-rate thresholds:
      - `< 60%` utilization + `< 50%` error rate → `1.0x` (normal)
      - `60-80%` utilization or `50-80%` error rate → `1.5x` (warning)
      - `> 80%` utilization or `> 80%` error rate → `2.5x` (high/saturated)
    - stale-data detection (health is stale if older than `3x` poll interval)
    - Telegram degradation alerts with cooldown deduplication (at most once per 5 minutes)
    - integration-ready: worker can call `get_proxy_health()` to apply pacing multipliers to scrape intervals
  - added tests: `server/tests/test_proxy_provider_runtime.py`
- Decision/rationale:
  - provider-side thread-limit failures were invisible until manual API checks
  - real-time health visibility enables proactive worker pacing before proxy saturation causes upstream failures
- Files touched:
  - `server/services/worker/proxy_monitor.py` [NEW]
  - `server/tests/test_proxy_provider_runtime.py` [NEW]

---

## 29. 2026-02-20 Feature: Dolphin Anty Profile Management + API Key Integration

- Date/time: 2026-02-20
- Change summary:
  - added Dolphin Anty profile management tab in `Settings > Connections & Scraper`:
    - new `Dolphin Profiles` tab in settings notebook
    - Treeview table with columns: ID, Name, Status, Browser, Tags, Memory
    - `Fetch Profiles` button: calls Dolphin Anty API (`GET {dolphin_api_url}/v1.0/browser_profiles`) and populates the profile table
    - `Start Selected` button: starts selected profiles via `GET {dolphin_api_url}/v1.0/browser_profiles/{id}/start?automation=1` and displays running port in Status column
    - `Stop Selected` button: stops selected profiles via `GET {dolphin_api_url}/v1.0/browser_profiles/{id}/stop`
  - added Dolphin Anty API Key and URL integration:
    - `dolphin_api_key` and `dolphin_api_url` added to `scraper_settings` defaults and save logic
    - dedicated API configuration parameters in the Dolphin Profiles tab
    - one-click `Save Key` button that persists the API key to SQLite via `_set_scraper_setting()`
    - new helper method `_dolphin_auth_headers()` generates `Authorization: Bearer <API_KEY>` headers
    - all three Dolphin API methods (`_refresh_dolphin_profiles`, `_start_selected_dolphin_profiles`, `_stop_selected_dolphin_profiles`) now inject the Bearer auth header via `headers=self._dolphin_auth_headers()`
  - **Headless VPS Autostart**: Configured TigerVNC (`vncserver@.service`) and Dolphin Anty (`dolphin-anty.service`) as `systemd` autostart services on the VPS to ensure the Dolphin API (port 3001) is available immediately after server restarts without requiring manual GUI login.
  - security: API key masked in GUI, persisted in SQLite, and transmitted only in Authorization headers
  - **Troubleshooting Connection Refused**: If a `Failed to contact Dolphin Anty API` or `Connection refused` error occurs, ensure that the Dolphin Anty desktop application is actively running on the target machine (local or VPS). The API server is only exposed when the application is open.
- Files touched:
  - `gui.py`
- Validation performed:
  - syntax check: `python3 -c "import ast; ast.parse(open('gui.py').read())"` → `Syntax OK`

---

## 30. 2026-02-20 Code Audit Features: Stealth Scripts & Enhanced Soft-Signals

- Date/time: 2026-02-20
- Change summary:
  - discovered previously undocumented stealth script fingerprint randomization:
    - randomizes `hardwareConcurrency`, `deviceMemory`, `platform`, and `webgl_renderer` via JS injection
    - adds missing standard properties like `window.chrome` and specific plugins
    - strengthens the crawler's anti-detection posture against platform fingerprinting
  - discovered new enhanced soft-signals in the signal detector (`server/services/worker/signal_detector.py`):
    - `CONSECUTIVE_EMPTY_RESULTS`: proactive pause signal triggered when multiple consecutive empty responses are encountered (threshold >= 3)
    - `SESSION_TOO_LONG`: signal emitted when session duration exceeds maximum allowed time, triggering safe cycle resets
- Decision/rationale:
  - these features were added to further harden the system against silent bot bans and excessive cycle durations
  - officially documenting these features ensures visibility for ongoing risk/pacing calibrations
- Validation performed:
  - reviewed tests for stealth scripts (`server/tests/test_stealth_scripts.py`)
  - reviewed tests for enhanced signals (`server/tests/test_signal_detector_new.py`)

---

## 31. 2026-02-21 Milestone AC: V2.2 Dolphin Anty CDP Migration + Worker Hardening

- Date/time: 2026-02-21
- Change summary:
  - **V2.2 Core Scraper Rewrite (`core/scraper/` package)**:
    - added entirely new `core/scraper/` package (parallel to existing `scraper/`) purpose-built for Dolphin Anty CDP-based browser automation:
      - `core/scraper/config.py` — configuration constants including blocked resource types and connection limits
      - `core/scraper/driver.py` — Dolphin Anty CDP integration: `_start_dolphin_profile()` starts profile via Dolphin local API and returns WebSocket endpoint, `launch_browser_context()` connects over CDP (`connect_over_cdp`), `stop_dolphin_profile()` for lifecycle cleanup
      - `core/scraper/parser.py` — model identification, condition assessment, price parsing, accessory detection (mirrors `scraper/parsers.py`)
      - `core/scraper/pipeline.py` — `scrape_marketplace()` entry point with GraphQL interception and DOM extraction
      - `core/scraper/storage.py` — database operations, listing persistence, account/query/setting management
    - browser launch now connects to Dolphin Anty-managed Chromium via CDP WebSocket instead of launching Playwright directly, delegating stealth/fingerprinting to Dolphin Anty's native anti-detect engine
    - resource filtering (`BROWSER_BLOCK_RESOURCE_TYPES`) applied via Playwright route interception on the CDP-connected context
  - **Dolphin Anty Profile Binding (Worker V2.2)**:
    - added `DOLPHIN_PROFILE_ID` env var per worker container in Docker Compose (e.g. `746373986`, `746380035`, `746386753`) for direct Dolphin Anty profile-to-worker mapping
    - added `DOLPHIN_ANTY_TOKEN` env var in compose for API authentication from worker containers
    - added `DOLPHIN_API_URL` env var pointing workers to `http://host.docker.internal:3001/v1.0` for container→host Dolphin API connectivity
    - worker `_run_scrape_cycle()` now reads `DOLPHIN_PROFILE_ID` and passes it to `scrape_marketplace(profile_id=...)` for CDP-based session launch
    - proxy management delegated to Dolphin Anty's native per-profile proxy bindings (`_build_playwright_proxy()` now returns all-None values)
  - **Weighted Bucket Query System**:
    - added `_get_bucket_queries()` in `worker.py` with weighted random selection across four query buckets:
      - `BUCKET_BROAD` (40% weight): `["iPhone"]` — catch-all broad sweep
      - `BUCKET_EXACT` (30% weight): specific model terms (`iPhone 15 Pro Max`, `iPhone 14 Pro`, etc.)
      - `BUCKET_FLIPPER` (20% weight): deal-hunter terms (`need gone iphone`, `broken iphone`, `cracked iphone`, etc.)
      - `BUCKET_MISSPELLING` (10% weight): common misspelling variants (`i phone 13`, `ipon 12`, etc.)
    - when route `search_queries` is empty or set to `BUCKETS`, worker generates 3 random weighted queries per cycle instead of using a static query list
    - ensures `iPhone` (broad catch-all) is always placed first when selected
  - **Quiet Hours Scheduling**:
    - added `WORKER_QUIET_HOURS_UTC` env var (format `HH:MM-HH:MM`) to configure a UTC time window where scraping slows down
    - added `WORKER_QUIET_HOURS_MULTIPLIER` (default `4.0x`) to multiply the effective scrape interval during quiet hours
    - supports midnight-wrapping ranges (e.g. `22:00-06:00`)
    - purpose: appear more human during off-peak hours to reduce ban risk
  - **Session Duration Caps**:
    - added `WORKER_SESSION_MAX_QUERIES` env var to limit total queries per browser session per route
    - when exceeded, worker restarts the browser session to avoid session-length fingerprinting
  - **VPS Infrastructure Migration**:
    - VPS SSH target updated from `root@147.182.131.111` to `ubuntu@15.235.185.32` in `deploy_vps.sh`
    - deploy remote directory updated from `/root/iphone_flipper/server` to `/home/ubuntu/iphone-flipper-server/server`
  - **Hotfix: `datetime` Import in `core/scraper/pipeline.py`**:
    - fixed missing `from datetime import datetime` import that caused `NameError` at runtime when pipeline tried to reference `datetime` objects
  - **Worker Failure Diagnostics (Investigation)**:
    - investigated `worker` failure: `manual_login_required` — Facebook checkpoint/login challenge triggered on Dolphin profile `746373986`; requires manual VPS intervention via TigerVNC
    - investigated `worker_2` failure: `Page.goto: Target page, context or browser has been closed` — Dolphin profile `746380035` crashes or closes immediately after browser launch; likely proxy/profile configuration issue
    - investigated `worker_3` failure: `No queries were executed in this scrape cycle` — confirmed query parsing works correctly (`SCRAPER_QUERIES="iPhone"` → `["iPhone"]`), but marketplace page returns zero cards for all queries; likely soft-ban or geo-blocking on profile `746386753`
    - all three failures require manual VPS intervention (TigerVNC-based Dolphin profile inspection and Facebook re-login)
- Files touched:
  - `core/scraper/config.py` [NEW]
  - `core/scraper/driver.py` [NEW]
  - `core/scraper/parser.py` [NEW]
  - `core/scraper/pipeline.py` [NEW] (+ datetime hotfix)
  - `core/scraper/storage.py` [NEW]
  - `server/services/worker/worker.py`
  - `server/infra/docker-compose.yml`
  - `server/scripts/deploy_vps.sh`
  - `dev-log.md`
  - `roadmap.md`
  - `architecture.md`
- Decision/rationale:
  - Dolphin Anty CDP integration replaces direct Playwright profile management, delegating fingerprinting/stealth to Dolphin's native anti-detect engine for better ban resistance
  - weighted bucket queries diversify search patterns across cycles to reduce repetitive query fingerprinting
  - quiet hours and session caps are proactive anti-detection measures for sustained 24x7 operation
- Validation performed:
  - local compile check: `core/scraper/pipeline.py` datetime import fix verified
  - query parsing verified: `_split_query_csv("iPhone")` → `["iPhone"]` correct
  - VPS SSH route verified: `deploy_vps.sh` targets `ubuntu@15.235.185.32`
- Unresolved follow-ups:
  - update Dolphin Anty API token on VPS server `.env`
  - manually inspect all three Dolphin profiles via TigerVNC on VPS to resolve Facebook login/checkpoint blocks
  - verify Dolphin Anty is running and accessible on port 3001 from worker containers

---

## 32. 2026-02-21 Milestone AD: Dynamic Dolphin Profile Allocation & Auto-Assignment

- Date/time: 2026-02-21
- Change summary:
  - **Dynamic Profile Allocation (Worker `worker.py`)**:
    - Removed hardcoded `DOLPHIN_PROFILE_ID` mapping from `docker-compose.yml` for all workers.
    - Workers now fetch the live list of available Dolphin profiles directly from the API on each cycle start.
    - Added `_try_acquire_profile_lock` which iteratively attempts to exclusively lock an available profile ID using a distributed PostgreSQL lease in the `worker_proxy_leases` table.
    - Avoids overlapping sessions and automatically handles profile additions/deletions without manual `.env` updates.
  - **Cloud-First API Fetching (`browser.py`)**:
    - Discovered that the local Dolphin Anty application (`port 3001`) actively blocks profile listing (`/browser_profiles`) if the GUI session is expired, returning `invalid session token`.
    - Modified `list_dolphin_profiles()` to prioritize fetching the profile list from the Dolphin Cloud API (`https://anty-api.com/browser_profiles`) using the `DOLPHIN_ANTY_TOKEN`.
    - This bypasses local session locks and successfully retrieves the synced profile IDs, while still allowing the worker to execute actual browser launch commands (`/start`) against the local `3001` port API (which does not validate the session).
  - **Error Handling**:
    - Worker now correctly aborts and surfaces a critical visibility error if 0 profiles are returned from the API (meaning token is invalid or missing), instead of blindly falling back to legacy profile indexes (like "1" or "3") which crash the local launcher.
  - **VPS Configuration**:
    - Extracted the local `dolphin_api_key` originally entered in the GUI from `listings.db` via SQL and injected it directly into the VPS `/home/ubuntu/iphone-flipper-server/.env` file.
- Files touched:
  - `server/infra/docker-compose.yml`
  - `server/services/worker/worker.py`
  - `scraper/browser.py`
  - `architecture.md`
  - `roadmap.md`
- Decision/rationale:
  - Hardcoded profile IDs break constantly if a profile is deleted or re-created. Dynamic leasing guarantees 1-to-1 worker-to-profile mapping safely over Postgres.
  - Using the Cloud API for discovery while using the Local API for execution provides the most resilient fallback strategy against Dolphin's aggressive underlying session expirations.
- Validation performed:
  - Deployed to VPS (`ubuntu@15.235.185.32`).
  - Container logs (`docker logs iphone-flipper-worker_3-1`) verified bypassing the local auth lock and successfully logging: `Dynamically claimed Dolphin Anty profile 746208087 (Profile 1)`.

---

## 33. 2026-02-22 Code Audit: Undocumented Features & Process Enhancements

- Date/time: 2026-02-22
- Change summary (documentation-only — no code modifications):
  - **5-Slot Concurrent Worker Architecture**:
    - `_run_worker_loop()` in `worker.py` now launches `WORKER_CONCURRENCY = 5` concurrent async slots per worker container via `_worker_slot_loop()`.
    - Each slot independently selects routes, acquires leases, and executes scrape cycles in parallel.
    - Route selection is serialized via `_route_selection_lock` (asyncio.Lock) to prevent duplicate route claims.
    - Slot-0 has exclusive responsibility for idle/waiting heartbeat updates to avoid heartbeat spam from other slots.
    - This effectively gives each worker container 5x route throughput without additional Docker service definitions.
  - **Legacy Utilities Module (`scraper/legacy_utils.py`)**:
    - 814-line module created during the scraper package refactoring (dev-log §26) to preserve V1 monolithic scraper logic as reusable shared utilities.
    - Contains: GraphQL payload extraction/normalization (`_extract_graphql_listing_candidates`, `_normalize_graphql_listing`, `merge_listing_candidates`), DOM listing extraction (`_extract_dom_listing_candidates`), progressive marketplace scroll (`progressive_marketplace_scroll` with target cards, max rounds, snapshot callbacks), manual login detection (`_detect_manual_login_required_state`), stealth script injection (`apply_stealth_scripts`), listing candidate persistence (`_store_listing_candidate`), and human-like scroll utilities.
    - Backward-compatible: both V1 `scraper/core.py` and V2.2 `core/scraper/pipeline.py` can import these utilities.
  - **Dolphin Anty Browser Hardening (`scraper/browser.py`)**:
    - Added `DOLPHIN_WS_HOST` env var (default `host.docker.internal`) for Docker container → VPS host WebSocket connectivity.
    - Added duplicate-running profile reuse: `_start_dolphin_profile()` handles `E_BROWSER_RUN_DUPLICATE` by querying the active session's automation port via `/browser_profiles/{id}/active` instead of crashing. Falls back to stop+restart if active query fails.
    - Added full WebSocket endpoint construction: `ws://{DOLPHIN_WS_HOST}:{port}{wsEndpoint}` using `wsEndpoint` path from Dolphin response.
    - Three-tier profile listing in `list_dolphin_profiles()`: (1) Cloud API (`https://anty-api.com/browser_profiles`) with Bearer auth → (2) Local API with headers → (3) Local API without headers fallback.
  - **V2.2 Core Scraper Package Config State**:
    - `core/scraper/config.py` carries bridge and connection-limiting configs (`FORCE_LOCAL_PROXY_BRIDGE`, `PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS`, `PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS`, `PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS`, `PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS`, `BROWSER_MAX_CONNECTIONS_PER_PROXY=32`, `BROWSER_MAX_CONNECTIONS_PER_HOST=6`).
    - `scraper/__init__.py` re-exports all bridge symbols for backward compatibility.
    - These configs are parsed at import time but have no active runtime consumer in the worker path following the §25 rollback. They remain available for future re-activation.
  - **Proxy Provider Health Gating (Worker Integration)**:
    - Each worker slot checks `proxy_health.is_saturated` before executing a scrape cycle. If saturated and health data is not stale, the cycle is skipped with `wait_proxy_provider` heartbeat status.
    - After each successful cycle, `proxy_health.pacing_multiplier` is applied to `next_interval_seconds` when multiplier > 1.0, dynamically slowing workers when provider is under load.
    - This closes the previously documented gap (dev-log §7 item 11) where pacing multiplier was computed but not wired.
  - **Caddy TLS Reverse Proxy**:
    - `server/infra/docker-compose.yml` includes a `caddy:2` service with auto-TLS provisioning via `SERVER_DOMAIN` env var.
    - Caddy reverse-proxies HTTPS/443 → API service (port 8080), with health-check dependency on the API container.
    - Persistent volumes: `caddy_data` (TLS certificates), `caddy_config`.
  - **`DOLPHIN_ANTY_TOKEN` in Compose Environment**:
    - All three worker containers (`worker`, `worker_2`, `worker_3`) receive `DOLPHIN_ANTY_TOKEN` from `.env` for Cloud API authentication.
    - This completes the static → dynamic profile migration: workers no longer receive `DOLPHIN_PROFILE_ID` but do receive the API token needed for cloud-first profile discovery.
- Files touched (documentation-only audit, no code changes):
  - `dev-log.md`
  - `roadmap.md`
  - `architecture.md`
- Decision/rationale:
  - All features listed above were implemented across prior milestones but were not formally documented in the three canonical project documents.
  - This audit entry ensures documentation accurately reflects the current runtime behavior and infrastructure topology.
- Validation performed:
  - Code audit verified each feature against source files: `server/services/worker/worker.py`, `scraper/browser.py`, `scraper/__init__.py`, `scraper/legacy_utils.py`, `core/scraper/config.py`, `server/infra/docker-compose.yml`, `server/.env.example`.

---

### §34 – Remove 5-Slot Concurrent Worker Limit (2026-02-22)

- Summary: Removed the `WORKER_CONCURRENCY = 5` concurrent slot architecture from the worker runtime. Each worker container now runs a single sequential scrape loop (`_run_worker_loop()`) instead of 5 parallel `_worker_slot_loop()` tasks.
- Motivation: The 5-slot concurrency was added as a throughput multiplier but is no longer needed. Parallelism is achieved via multiple Docker worker containers (`worker`, `worker_2`, `worker_3`) in `docker-compose.yml`.
- Changes:
  - Removed `_route_selection_lock` (asyncio.Lock used to serialize route selection across slots)
  - Removed `_worker_slot_loop()` function and `slot_id` parameter
  - Removed `WORKER_CONCURRENCY = 5` constant and multi-task spawning
  - Removed slot-0 heartbeat exclusivity guard (heartbeats now always update since single loop)
  - Replaced `_run_worker_loop()` from task-spawner to direct single-loop implementation
  - All `[%s/slot-%d]` log format strings updated to `[%s]`
- Files touched:
  - `server/services/worker/worker.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Resolved risks:
  - Dev-log §7 item 14 (`WORKER_CONCURRENCY` not operator-configurable via env var) is now moot — concurrency is removed entirely.
- Validation performed:
  - Python AST syntax check: OK
  - Grep verification: no remaining references to `slot_id`, `WORKER_CONCURRENCY`, `_route_selection_lock`, `_worker_slot_loop`

---

### §35 – Distributed Profile Health Tracking & Bug Fixes (2026-02-22)

- Summary: Migrated the Per-Profile Health Tracker from a local container in-memory map to a distributed PostgreSQL-backed implementation utilizing the `proxy_stats` table.
- Motivation: In-memory tracking meant blacklisted Dolphin profiles on one worker could still be picked up and failed by other workers. The distributed tracking ensures global visibility of blacklisted profiles across the worker fleet.
- Changes:
  - Removed `ProfileHealthTracker` class and in-memory dicts from `worker.py`.
  - `_fetch_dolphin_profile_blacklist()` now queries `proxy_stats` for keys prefixing `PROFILE:<dolphin_id>` to filter out banned instances.
  - `_record_dolphin_profile_outcome()` upserts success/failure counts directly to PostgreSQL with an exponential backoff penalty up to 24 hours on consecutive failures.
  - Fixed an incomplete success path by ensuring Upsert (`INSERT ... ON CONFLICT DO UPDATE`) logic creates baseline success records for fresh profiles.
  - Handled a `NameError` crash relating to uninitialized `available_profiles` when the explicit `DOLPHIN_PROFILE_ID` env variable was strictly set.
  - Fixed alert spam with the `RETURNING` clause: profile failure alerts exactly notify when the threshold is first crossed.
  - Supplemented Telegram alerts with Profile Name resolution (e.g., `Profile 6`) rather than just the generic ID.
- Files touched:
  - `server/services/worker/worker.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Validation performed:
  - Deployed to worker containers on VPS.
  - Monitored container logs to confirm clean startup without previous NameErrors alongside successful allocation of Dolphin profiles spanning workers.

---

### §36 – Documentation Audit & .env.example Gap Fix (2026-02-22)

- Summary: Comprehensive audit of `architecture.md`, `roadmap.md`, and `dev-log.md` against the latest codebase to verify all implemented features, process enhancements, and configuration details are accurately documented.
- Findings:
  - `DOLPHIN_ANTY_TOKEN` environment variable was required by all three worker containers in `docker-compose.yml` but was missing from `server/.env.example` template. Added along with commented `DOLPHIN_API_URL` for operator awareness.
  - Profile blacklist max escalation (30 min → 24 hrs via exponential backoff) and RETURNING-clause Telegram alert deduplication in `_record_dolphin_profile_outcome()` are correctly documented in architecture.md and dev-log §35.
  - Weighted bucket query system, quiet hours scheduling, session caps, and dynamic Dolphin profile allocation are all correctly reflected across all three documents.
  - No undocumented code-level features were found beyond the `.env.example` template gap.
- Files touched:
  - `server/.env.example` (added `DOLPHIN_ANTY_TOKEN` + `DOLPHIN_API_URL`)
  - `dev-log.md` (this entry)
  - `architecture.md` (revision note date confirmation)
  - `roadmap.md` (implementation date confirmation)
- Validation performed:
  - Cross-referenced all worker env vars in `docker-compose.yml` against `.env.example` template
  - Verified all dev-log entries §1–§35 accurately reflect current source code in `worker.py`, `lease_manager.py`, `gui.py`, `docker-compose.yml`

---

### §37 – V3.0 Phase 0: Redis Flags + Baseline Observability (2026-03-08)

- Summary: Implemented Phase 0 of the approved V3.0 latency/responsiveness upgrade track. This phase adds Redis-backed runtime feature flags plus JSON observability across worker/API/GUI poll-sync flows without changing current event payload contracts or moving into Phase 1 behavior.
- Motivation:
  - establish rollout controls and latency measurement before modifying the current event spine
  - keep the upgrade within the approved non-goals and strict phase order
  - document the current server baseline, including known deferred mismatches, before deeper transport/scheduler changes
- Changes:
  - added shared Redis feature-flag helper (`server/services/common/feature_flags.py`):
    - Redis hash key: `flipper:flags`
    - default-disabled flags: `ENABLE_REDIS_STREAM_EVENTS`, `ENABLE_NOTIFICATION_CONSUMER`, `ENABLE_GUI_WEBSOCKET_PUSH`, `ENABLE_PRIORITY_SCHEDULER`, `ENABLE_ROUTE_LANES`
    - ~1s in-process cache, async reads, safe fallback to defaults on Redis failure
  - added shared JSON observability helper (`server/services/common/observability.py`) for timestamp normalization, latency calculation, and JSON-only log emission
  - added startup feature-flag snapshot logs in:
    - `server/services/api/app/main.py`
    - `server/services/worker/worker.py`
    - `server/services/worker/notification_worker.py`
  - added worker-side listing-path observability:
    - `listing_seen_ts`
    - `listing_persisted_ts`
    - `listing_event_published_ts`
    - inline Telegram `notification_sent_ts` / delivery status
    - aggregate cycle metrics for Postgres upsert latency, Redis publish latency, inline notification delivery latency, and end-to-end alert latency
  - extended worker cycle telemetry payload builder (`server/services/worker/telemetry.py`) with listings-parsed count and new latency aggregate fields
  - added API-side observability for listing WebSocket push attempts:
    - `gui_pushed_ts`
    - `websocket_broadcast_latency_ms`
    - `websocket_client_count`
  - added GUI poll-sync observability for per-listing local render/apply events:
    - `gui_rendered_ts`
    - source marker `poll_sync`
  - updated `architecture.md`, `roadmap.md`, and `dev-log.md` to record:
    - V3.0 Phase 0 scope and explicit non-goals
    - current baseline vs later approved phases
    - deferred notification-worker payload-shape mismatch
  - fresh audit result:
    - no additional undocumented features or process enhancements were found beyond the existing roadmap-drift audit sections already captured in project docs
- Files touched:
  - `server/services/common/feature_flags.py`
  - `server/services/common/observability.py`
  - `server/services/worker/worker.py`
  - `server/services/worker/telemetry.py`
  - `server/services/worker/notification_worker.py`
  - `server/services/api/app/main.py`
  - `gui.py`
  - `server/tests/test_feature_flags.py`
  - `server/tests/test_api_observability.py`
  - `server/tests/test_worker_observability.py`
  - `server/tests/test_telemetry.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Decision/rationale:
  - Phase 0 is intentionally measurement-only. Existing Redis pub/sub fanout, inline worker Telegram notifications, standalone notification-worker implementation, and API WebSocket endpoint remain behaviorally unchanged.
  - The pre-existing payload mismatch between current worker pub/sub events and `notification_worker.py` parsing is documented but intentionally deferred until the later Redis event-spine / notification-consumer phases.
  - Untracked workspace SSH keys (`iphone_flipper/l`, `iphone_flipper/l.pub`) were explicitly left untouched and are not part of this implementation.
- Validation performed:
  - installed missing server test dependencies into the existing repo venv so API/worker modules could be imported for targeted Phase 0 validation
  - `./.venv/bin/python -m pytest server/tests/test_feature_flags.py server/tests/test_telemetry.py server/tests/test_api_observability.py server/tests/test_worker_observability.py server/tests/test_notification_worker.py -q`
  - result: `27 passed`
  - Python AST parse checks passed for modified API/worker/GUI/test modules before running pytest
- Unresolved follow-ups:
  - Phase 1 is blocked pending operator review, test, and approval of this Phase 0 patch
  - notification worker still is not compose-wired as a production service
- worker pub/sub payload alignment with `notification_worker.py` remains deferred to later approved phases
- GUI still applies server changes via cursor polling; WebSocket-first apply path remains deferred to later approved phases

---

### §38 – V3.0 Phase 1: Durable Redis Streams Event Spine (2026-03-09)

- Summary: Implemented Phase 1 of the approved V3.0 latency/responsiveness upgrade track. This phase adds a durable Redis Streams event spine after persistence, behind `ENABLE_REDIS_STREAM_EVENTS`, while keeping the current Redis pub/sub fanout, inline worker Telegram notifications, API behavior, and dormant notification worker unchanged.
- Motivation:
  - add a durable event log after persistence without regressing the current live fanout path
  - prepare the later notification-consumer and replay phases while preserving strict phase ordering
  - keep rollout measurable and reversible through the Phase 0 flag layer and structured telemetry
- Changes:
  - added shared Redis Streams event module (`server/services/common/stream_events.py`):
    - stream name: `stream:listings`
    - retention policy: `XADD ... MAXLEN ~ 10000`
    - flat event schema: `schema_version`, `event_name`, `listing_id`, `worker_name`, `route_name`, `query`, `query_index`, `query_total`, `query_shard_key`, `persisted_at`, `price`, `potential_profit`, `title`, `url`, `source`
    - helper paths for event serialization/deserialization and meaningful-change comparison state extraction
  - extended worker post-upsert flow (`server/services/worker/worker.py`) to dual-write safely:
    - Postgres remains the source of truth
    - Redis pub/sub publish path remains active and unchanged
    - Redis Streams publish occurs only when `ENABLE_REDIS_STREAM_EVENTS` is enabled
    - stream emission occurs only for newly created listings or meaningful updates over the bounded hot-path fields:
      - `title`
      - `price`
      - `url`
      - `model`
      - `condition`
      - `status`
      - `max_buy_price`
      - `potential_profit`
      - `location`
    - unchanged updates still upsert to Postgres so `source_seen_at` / `updated_at` semantics remain intact, but stream emission is suppressed for those cases
  - made stream publish failure non-fatal:
    - stream publish exceptions emit `listing_stream_publish_failed`
    - worker continues current pub/sub + inline notification flow
    - returned Redis stream IDs are captured as `stream_event_id` / `event_id` when available
  - extended worker observability and cycle telemetry:
    - `listing_pipeline_observed` now includes stream name, stream publish status, stream event ID, stream publish timestamp, and stream publish latency
    - cycle telemetry now tracks stream publish latency sum/count and stream publish failure count
  - updated canonical documentation (`architecture.md`, `roadmap.md`, `dev-log.md`) to record:
    - optional Phase 1 dual-write architecture
    - retention policy and compact schema decision
    - rollout flag state and verification approach
    - explicit note that notification consumers remain disabled in this phase
  - fresh audit result:
    - no additional undocumented code-level features or process enhancements were found beyond the previously captured roadmap-drift audit sections and the newly approved Phase 1 changes above
- Files touched:
  - `server/services/common/stream_events.py`
  - `server/services/worker/worker.py`
  - `server/services/worker/telemetry.py`
  - `server/tests/test_stream_events.py`
  - `server/tests/test_worker_observability.py`
  - `server/tests/test_telemetry.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Decision/rationale:
  - Phase 1 is intentionally limited to the event spine. `notification_worker.py` remains compose-unwired, consumer groups are not introduced yet, and the current pub/sub payload is not reshaped in this phase.
  - The stream schema stays compact and uses existing runtime metadata (`query`, `query_index`, `query_total`, `query_shard_key`, `route_name`) instead of inventing new persisted route/query identifiers that do not exist in the current runtime.
  - The rollout left `ENABLE_NOTIFICATION_CONSUMER=0`, `ENABLE_GUI_WEBSOCKET_PUSH=0`, `ENABLE_PRIORITY_SCHEDULER=0`, and `ENABLE_ROUTE_LANES=0` so later phases remain opt-in and independently reviewable.
- Validation performed:
  - local import/compile sanity:
    - `./.venv/bin/python -m py_compile server/services/common/stream_events.py server/services/worker/worker.py server/services/worker/telemetry.py server/tests/test_stream_events.py server/tests/test_worker_observability.py server/tests/test_telemetry.py`
  - targeted Phase 1 pytest suite:
    - `./.venv/bin/python -m pytest server/tests/test_stream_events.py server/tests/test_worker_observability.py server/tests/test_telemetry.py -q`
    - result: `13 passed`
  - full server pytest regression:
    - `./.venv/bin/python -m pytest server/tests -q`
    - result: `89 passed`
  - VPS rollout and verification:
    - deployed Phase 1 to `ubuntu@15.235.185.32` via `server/scripts/deploy_vps.sh`
    - verified clean startup with `ENABLE_REDIS_STREAM_EVENTS=0` and `stream:listings` absent
    - enabled flags in Redis hash `flipper:flags`:
      - `ENABLE_REDIS_STREAM_EVENTS=1`
      - `ENABLE_NOTIFICATION_CONSUMER=0`
      - `ENABLE_GUI_WEBSOCKET_PUSH=0`
      - `ENABLE_PRIORITY_SCHEDULER=0`
      - `ENABLE_ROUTE_LANES=0`
    - verified Redis Streams on VPS with:
      - `XLEN stream:listings`
      - `XREVRANGE stream:listings + - COUNT 2`
      - `XINFO STREAM stream:listings`
    - verified unchanged pub/sub-driven API fanout with a safe Redis pub/sub smoke event and observed API-side `listing_gui_push`
    - because natural worker listing traffic during rollout was sparse and intermittently blocked by existing Dolphin/browser failures, stream runtime verification used a safe one-off worker-container smoke publish that exercised the deployed stream codec and `XADD` path without touching persistence or sending Telegram alerts
- Unresolved follow-ups:
  - Phase 2 is still required to wire notification consumers through Redis Streams consumer groups and compose deployment
- pre-existing worker pub/sub payload mismatch vs `notification_worker.py` remains intentionally deferred
- natural production traffic should continue to be observed now that Phase 1 is live, so stream publish rates can be measured under real listing flow once Dolphin/browser stability improves

---

### §39 – V3.0 Phase 2: Redis Streams Notification Consumer + Acceptance Hardening (2026-03-09)

- Summary: Implemented Phase 2 of the approved V3.0 latency/responsiveness upgrade track and added the minimum Phase 1 hardening needed to satisfy the combined event-spine + notification acceptance criteria. Notification delivery now runs through a dedicated Redis Streams consumer service with durable PostgreSQL dedupe, while worker-side exact-once behavior for same-listing creation is hardened with a transaction-level advisory lock.
- Motivation:
  - move notification I/O off the scrape worker hot path
  - make notification delivery restart-safe and deduplicated across duplicate stream delivery
  - close the remaining Phase 1 acceptance gaps around same-listing races and duplicate alerts
- Changes:
  - hardened worker persistence / stream publication (`server/services/worker/worker.py`):
    - added PostgreSQL transaction-level advisory lock on `listing_id` before the read/upsert decision
    - preserved Postgres as source of truth and kept Redis pub/sub fanout unchanged
    - retained Phase 1 stream publish rules (`listing_created` or meaningful hot-path change only)
    - when `ENABLE_NOTIFICATION_CONSUMER=1` and stream publish succeeds, worker now emits `notification_delivery_delegated` instead of blocking on inline Telegram send
  - replaced the old pub/sub notification worker with a Redis Streams consumer-group service (`server/services/worker/notification_worker.py`):
    - consumer group: `listing_notifications`
    - startup bootstrap: `XGROUP CREATE ... MKSTREAM` with `BUSYGROUP` ignored
    - reads new entries via `XREADGROUP`
    - reclaims pending/restart-surviving work via `XAUTOCLAIM`
    - terminal ack outcomes: `sent`, `suppressed`, `duplicate_already_sent`
    - Telegram send is authoritative; FCM remains best-effort only
    - outbound pacing changed to immediate send with minimum 1 second spacing
    - transient failures retry with backoff and remain pending when retries are exhausted
  - added durable notification ledger (`notification_delivery_ledger`) to shared schema/bootstrap:
    - key: `listing_id`
    - stores `first_stream_event_id`, `last_stream_event_id`, `status`, `attempt_count`, `last_error`, `last_attempt_at`, `sent_at`, `updated_at`
    - suppresses re-sends after restart or duplicate stream delivery
  - wired the dedicated `notification_worker` into Docker Compose and the standard VPS deploy path
  - updated canonical docs (`architecture.md`, `roadmap.md`, `dev-log.md`) to reflect:
    - consumer-group notification architecture
    - exact-once producer hardening
    - rollout state and acceptance verification
  - fresh audit result:
    - no additional undocumented code-level features or process enhancements were found beyond the approved Phase 2 work and the existing roadmap-drift audit sections
- Files touched:
  - `server/services/worker/worker.py`
  - `server/services/worker/notification_worker.py`
  - `server/services/common/schema_ensure.py`
  - `server/services/api/sql/001_init.sql`
  - `server/infra/docker-compose.yml`
  - `server/scripts/deploy_vps.sh`
  - `server/tests/test_worker_observability.py`
  - `server/tests/test_notification_worker.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Decision/rationale:
  - durable dedupe is keyed by `listing_id` in Postgres rather than Redis memory so duplicate stream delivery and consumer restarts do not resend already-sent alerts
  - Redis pub/sub remains active for API fanout in this phase; notifications move to the stream consumer path without changing public API/WebSocket contracts
  - the worker keeps inline notification as a fallback only when the dedicated consumer is disabled or stream delegation is unavailable, avoiding silent alert loss during misconfiguration or publish failure
- Validation performed:
  - local import/compile sanity:
    - `./.venv/bin/python -m py_compile server/services/worker/worker.py server/services/worker/notification_worker.py server/services/common/schema_ensure.py server/tests/test_worker_observability.py server/tests/test_notification_worker.py`
  - targeted Phase 2 pytest suite:
    - `./.venv/bin/python -m pytest server/tests/test_worker_observability.py server/tests/test_notification_worker.py -q`
    - result: `18 passed`
  - full server pytest regression:
    - `./.venv/bin/python -m pytest server/tests -q`
    - result: `85 passed`
  - VPS rollout and acceptance verification:
    - deployed Phase 2 to `ubuntu@15.235.185.32` via `server/scripts/deploy_vps.sh`
    - verified clean startup with:
      - `ENABLE_REDIS_STREAM_EVENTS=1`
      - `ENABLE_NOTIFICATION_CONSUMER=0`
      - `ENABLE_GUI_WEBSOCKET_PUSH=0`
      - `ENABLE_PRIORITY_SCHEDULER=0`
      - `ENABLE_ROUTE_LANES=0`
    - verified `notification_worker` startup and consumer-group bootstrap via `XINFO GROUPS stream:listings`
    - enabled `ENABLE_NOTIFICATION_CONSUMER=1` live in `flipper:flags`
    - observed the consumer drain existing backlog and reach zero lag
    - controlled unread-event smoke:
      - stopped `notification_worker`
      - appended synthetic stream event `phase2-smoke-unread-1`
      - restarted `notification_worker`
      - verified exactly one `notification_delivery_result` with `notification_status=sent`
    - controlled duplicate smoke:
      - appended a second synthetic `listing_created` event for the same `listing_id`
      - verified `notification_status=duplicate_already_sent` and no second send
    - controlled restart smoke:
      - stopped `notification_worker`
      - appended synthetic stream event `phase2-smoke-unread-restart-1`
      - restarted `notification_worker`
      - verified the unread event was delivered successfully after restart
    - controlled worker-delegation smoke:
      - invoked `_process_listing_event` with synthetic listing `phase2-worker-delegate-1` inside the running worker container
      - observed `notification_delivery_delegated` from worker code
      - observed corresponding consumer-side `notification_delivery_result` with `notification_status=sent`
    - inspected `notification_delivery_ledger` rows for all three synthetic listing IDs and confirmed `status=sent`, `attempt_count=1`, and expected `last_stream_event_id`
    - cleaned the synthetic persisted listing row `phase2-worker-delegate-1` from the `listings` table after verification
- Acceptance criteria status:
  - `Newly persisted listings generate exactly one stream event in the happy path.`  
    Met via advisory-lock serialization plus unchanged-update suppression, with targeted concurrent worker test coverage.
  - `Consumer restart does not lose unread events.`  
    Met via Redis Streams consumer groups plus unread/pending recovery behavior, verified in controlled restart smoke.
  - `Duplicate unchanged listings do not generate duplicate alerts.`  
    Met via producer unchanged-event suppression, worker delegation when the consumer is enabled, and PostgreSQL ledger dedupe by `listing_id`.
- Unresolved follow-ups:
  - Phase 3 is still required for WebSocket-first desktop sync and reconnect/backfill behavior
  - dead-letter handling and operator replay tooling remain Phase 6 scope
  - natural production measurements should continue to confirm latency improvement under real worker discoveries once Dolphin/browser stability improves

---

### §40 – V3.0 Phase 3: Desktop WebSocket Live Sync + Poll Fallback (2026-03-09)

- Summary: Implemented Phase 3 of the approved V3.0 latency/responsiveness upgrade track. The API now emits normalized live listing snapshots over `/ws/listings`, and the desktop GUI now consumes them through a dedicated `aiohttp` sync coordinator that prefers WebSocket, falls back to polling on disconnect/disable, and replays missed listings from the persisted `server_sync_since_id` cursor with a bounded overlap window. Initial rollout surfaced one API payload bug and one fallback retry bug; both were fixed before final acceptance.
- Motivation:
  - remove the remaining desktop lag caused by waiting for the next polling interval
  - keep the existing cursor-poll path as a safe fallback instead of replacing it
  - make reconnects deterministic by backfilling missed rows from the canonical `/listings` cursor API
- Changes:
  - upgraded API WebSocket live sync (`server/services/api/app/main.py`):
    - replaced raw `set[WebSocket]` tracking with a connection manager carrying per-client send lock, connection metadata, and last-pong state
    - gated live desktop push behind `ENABLE_GUI_WEBSOCKET_PUSH`
    - when the flag is disabled, `/ws/listings` now accepts, sends `websocket_disabled`, and closes so the desktop falls back immediately
    - stopped forwarding raw worker pub/sub payloads to desktop clients; API now resolves the canonical Postgres listing row by `listing_id` and sends a normalized `listing_snapshot` frame shaped like `/listings`
    - normalized snapshot rows through JSON-safe encoding so websocket push survives Postgres `NUMERIC`/`Decimal` fields in `price`, `max_buy_price`, and `potential_profit`
    - added application heartbeat control frames:
      - server sends `ping`
      - client replies `pong`
      - stale clients are removed on timeout
      - active clients are closed when the live-push flag flips off
  - added desktop sync helper module (`desktop_sync.py`):
    - derives `wss://.../ws/listings?token=...` from the configured API base URL
    - runs in a background thread with its own asyncio loop and `aiohttp` session
    - opens WebSocket first, buffers live snapshots, then replays `/listings` from `max(0, since_id - 100)` before draining buffered live items
    - on disconnect, auth failure, transport error, heartbeat timeout, or `websocket_disabled`, switches to poll-only mode and retries the WebSocket connection with exponential backoff and jitter
    - explicitly answers websocket control `PING` frames in addition to JSON heartbeat `ping` messages
    - keeps retrying through transient REST failures during fallback polling (for example brief `502` windows during API restarts) instead of terminating the sync thread
    - applies cursor gating (`seq_id > server_sync_since_id`) before local upsert so replay overlap and later polling cannot duplicate rows
  - updated GUI integration (`gui.py`):
    - server sync startup now uses the new threaded coordinator instead of the legacy polling-only loop
    - main Tk thread receives status/apply events through a queue and stays free of network I/O
    - sync-driven refreshes now preserve selected listing IDs and focused row when those rows still exist after refresh
  - updated dependencies and tests:
    - promoted `aiohttp>=3.9.0` into root `requirements.txt`
    - added API tests for normalized snapshots, disabled control frames, heartbeat ping/pong, and timeout cleanup
    - added desktop sync tests for replay overlap, buffered live drain, poll fallback, retry-after-error fallback, websocket control ping handling, and tree selection/focus preservation
  - updated canonical docs (`architecture.md`, `roadmap.md`, `dev-log.md`) to reflect:
    - WebSocket-first desktop sync as the current baseline
    - heartbeat protocol and control frames
    - replay-window reconnect semantics
    - final rollout state with `ENABLE_GUI_WEBSOCKET_PUSH=1`
  - fresh audit result:
    - no additional undocumented code-level features or process enhancements were found beyond the approved Phase 3 work and the existing roadmap-drift audit sections
- Files touched:
  - `server/services/api/app/main.py`
  - `desktop_sync.py`
  - `gui.py`
  - `requirements.txt`
  - `server/tests/test_api_observability.py`
  - `server/tests/test_desktop_sync.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Decision/rationale:
  - WebSocket payloads now mirror `/listings` item shape so polling and live sync share one canonical listing schema and one cursor model
  - Redis pub/sub remains the API fanout trigger in Phase 3; this keeps the current worker/API event spine intact while improving only the desktop consumption path
  - replay overlap is implemented by rewinding 100 sequence IDs on reconnect and relying on cursor gating plus local SQLite upsert semantics to suppress duplicates
  - GUI refresh remains table-rebuild based, but selection/focus state is preserved to avoid regressions in operator workflow
- Validation performed:
  - local import/compile sanity:
    - `./.venv/bin/python -m py_compile server/services/api/app/main.py gui.py desktop_sync.py server/tests/test_api_observability.py server/tests/test_desktop_sync.py`
  - targeted Phase 3 pytest suite:
    - `./.venv/bin/python -m pytest server/tests/test_api_observability.py server/tests/test_desktop_sync.py -q`
    - result after final hardening: `14 passed`
  - full server pytest regression:
    - `./.venv/bin/python -m pytest server/tests -q`
    - result after final hardening: `97 passed`
  - VPS rollout and acceptance verification:
    - deployed Phase 3 to `ubuntu@15.235.185.32` via `server/scripts/deploy_vps.sh`
    - verified clean startup with `ENABLE_GUI_WEBSOCKET_PUSH=0` and confirmed the desktop remained poll-only
    - enabled `ENABLE_GUI_WEBSOCKET_PUSH=1` live in `flipper:flags`
    - initial websocket smoke exposed two rollout bugs:
      - API websocket push failed when normalized snapshot payloads still contained raw Postgres `NUMERIC`/`Decimal` objects
      - desktop fallback exited on transient `502` poll failures during API restart/reconnect testing
    - patched and redeployed the API for JSON-safe websocket snapshots
    - patched the desktop sync coordinator so fallback polling retries instead of terminating
    - final controlled live-listing smoke:
      - inserted synthetic listing row `phase3-smoke-live-pass-1773067600`
      - published matching `listing_created` pub/sub event
      - verified GUI import through `source=websocket` before the next poll interval
    - final controlled reconnect smoke:
      - stopped the API service to force live disconnect
      - inserted synthetic listing row `phase3-smoke-reconnect-pass-1773067812` while the API was unavailable
      - restarted the API service
      - verified fallback polling imported the listing exactly once and the client returned to live mode
    - final controlled poll-only smoke:
      - set `ENABLE_GUI_WEBSOCKET_PUSH=0`
      - inserted synthetic listing row `phase3-smoke-pollonly-pass-1773067877`
      - verified the desktop imported it through `source=poll_sync` without websocket assistance
      - restored `ENABLE_GUI_WEBSOCKET_PUSH=1`
    - cleaned all synthetic Phase 3 rows from Postgres with `DELETE FROM listings WHERE id LIKE 'phase3-%'`
- Acceptance criteria status:
  - `New listing appears in GUI without waiting for the next poll interval.`  
    Met via controlled live-listing smoke with `phase3-smoke-live-pass-1773067600` (`source=websocket`).
  - `Disconnect/reconnect does not lose listings.`  
    Met via controlled reconnect smoke with `phase3-smoke-reconnect-pass-1773067812`; the listing was recovered during fallback/backfill and the client returned to live mode.
  - `Poll fallback still works with WebSocket disabled.`  
    Met via controlled poll-only smoke with `phase3-smoke-pollonly-pass-1773067877`.
- Unresolved follow-ups:
  - Phase 4 is still required for priority scheduling and route/query lanes
  - Phase 6 still owns operator replay tooling, backlog visibility, and live-push rollback controls beyond the current flag
  - broader GUI test coverage is still light outside the new sync helper path because most existing GUI behavior remains Tk-driven and integration-heavy

---

### §41 – V3.0 Phase 4: Priority Scheduler + Route/Query Lanes (2026-03-10)

- Summary: Implemented Phase 4 of the approved V3.0 latency/responsiveness upgrade track. DB-backed worker routes now support persisted route lanes (`hot`, `warm`, `sweep`), score-based dispatch, lane-aware query ranking, and Redis per-query locks behind `ENABLE_ROUTE_LANES` and `ENABLE_PRIORITY_SCHEDULER`. Final rollout required a controlled VPS acceptance window on `worker_3` to prove that hot-route coverage improves without increasing hot-route cadence, then the temporary smoke routes were removed so production returned to the normal env-backed route set with both Phase 4 flags left enabled.
- Motivation:
  - prioritize high-value routes without increasing per-profile request intensity
  - keep current cooldown/throttle/quiet-hours/session pacing intact while changing selection order only
  - prevent workers from racing the same query when DB-backed routes use multiple overlapping search terms
- Changes:
  - added dedicated Phase 4 scheduler/lane modules:
    - `server/services/worker/scheduler.py` now owns route score calculation, computed/effective lanes, lane interval multipliers, due-route selection, and lane-aware query ranking
    - `server/services/common/route_lanes.py` now owns shared lane validation/normalization for worker + API surfaces
  - extended worker route persistence and runtime state:
    - added `lane_override`, `computed_lane`, `effective_lane`, `priority_score`, and `priority_score_updated_at` to `worker_routes`
    - promoted `profitable_hit_rate` and `recent_duplicate_ratio` into persisted route metrics so scheduling can use bounded historical signals without a second aggregation system
    - worker now recomputes/persists lane + score snapshots whenever `ENABLE_ROUTE_LANES=1`
  - added priority scheduler and query locking behavior in `server/services/worker/worker.py`:
    - DB routes still use the legacy due-time path when Phase 4 flags are off
    - when both Phase 4 flags are on, the worker selects the next due route by `effective_lane`, then `priority_score`, then legacy tie-breakers
    - candidate queries are ranked by lane intent and duplicate pressure
    - Redis query locks use `query-lock:<sha1>` keys with `SET NX EX` semantics and explicit release on cycle exit
    - if the preferred query is locked, the worker falls through to the next candidate instead of blocking
    - if all candidate queries are locked, the cycle returns a scheduler WAIT outcome rather than a false scrape failure
    - env-backed fallback routes intentionally remain on the legacy scheduling path
  - extended telemetry/observability:
    - cycle JSON now carries `lane_override`, `computed_lane`, `effective_lane`, `priority_score`, compact score components, `selected_query`, `query_lock_key`, `query_lock_status`, `route_due_age_seconds`, `route_revisit_age_seconds`, and `lane_interval_multiplier`
    - cycle counters now include profitable/duplicate listing counts and query-lock acquired/skipped totals
  - extended API + GUI operator surfaces:
    - `/worker-routes` now serializes all lane/score fields
    - route upsert validation permits only `lane_override` edits (`hot`, `warm`, `sweep`, or clear)
    - GUI VPS route table/editor now shows lane + score state and exposes `auto/hot/warm/sweep` pinning without making computed/effective fields editable
  - rollout fixes discovered during implementation:
    - moved route-lane normalization into shared `server/services/common/route_lanes.py` because importing it from the worker package broke the slimmer API image
    - fixed a worker loop bug where DB-backed routes could hit `UnboundLocalError: sleep_seconds referenced before assignment` after a cycle finished
  - fresh audit result:
    - no additional undocumented code-level features or process enhancements were found beyond the approved Phase 4 work and the earlier audit sections
- Files touched:
  - `server/services/common/route_lanes.py`
  - `server/services/common/schema_ensure.py`
  - `server/services/api/sql/001_init.sql`
  - `server/services/api/app/main.py`
  - `server/services/worker/scheduler.py`
  - `server/services/worker/lease_manager.py`
  - `server/services/worker/worker.py`
  - `server/services/worker/telemetry.py`
  - `gui.py`
  - `server/tests/test_priority_scheduler.py`
  - `server/tests/test_api_route_lanes.py`
  - `server/tests/test_worker_observability.py`
  - `server/tests/test_telemetry.py`
  - `architecture.md`
  - `roadmap.md`
  - `dev-log.md`
- Decision/rationale:
  - route priority uses only already-available signals or small derived aggregates, keeping Phase 4 bounded to scheduling rather than introducing a separate analytics subsystem
  - operator lane pinning is intentionally narrow: only `lane_override` is editable; computed/effective lane and score stay server-derived to avoid control-plane drift
  - route cadence remains bounded by the configured route interval; Phase 4 differentiates revisit timing by stretching lower-value lanes, not by shortening hot-route cadence
  - the production acceptance run used temporary DB routes on `worker_3` because the real VPS currently has no persisted `worker_routes`; after the smoke test these routes were deleted so the deployed server returned to the normal env-backed behavior while leaving the new flags enabled
- Validation performed:
  - local import/compile sanity:
    - `python3 -m py_compile server/services/common/route_lanes.py server/services/worker/scheduler.py server/services/worker/lease_manager.py server/services/worker/worker.py server/services/worker/telemetry.py server/services/api/app/main.py gui.py`
  - full server pytest regression:
    - `PYTHONPATH=iphone_flipper /tmp/iphone_flipper_phase4_venv/bin/python -m pytest iphone_flipper/server/tests -q`
    - result: `107 passed`
  - VPS rollout and acceptance verification:
    - deployed the Phase 4 code to `ubuntu@15.235.185.32`
    - verified baseline behavior with `ENABLE_ROUTE_LANES=0` and `ENABLE_PRIORITY_SCHEDULER=0`
    - seeded three temporary DB routes only for acceptance on `worker_3`:
      - `phase4_hot_w3` (`priority=300`, `lane_override=hot`, query `iPhone 15 Pro`)
      - `phase4_sweep_w3` (`priority=100`, `lane_override=sweep`, query `iPhone 13 mini`)
      - `phase4_sweep2_w3` (`priority=110`, `lane_override=sweep`, query `iPhone 12 mini`)
    - temporarily isolated `worker_3` while `worker` and `worker_2` were stopped to avoid unrelated Dolphin profile contention during the comparison window
    - cleaned stale `worker_proxy_leases` rows and Redis `query-lock:*` keys between smoke runs so acceptance used fresh lease state
    - flags-off baseline (`ENABLE_ROUTE_LANES=0`, `ENABLE_PRIORITY_SCHEDULER=0`):
      - `phase4_sweep_w3` selected first at `2026-03-09 22:21:23 UTC`
      - `phase4_sweep2_w3` selected second at `2026-03-09 22:22:04 UTC`
      - `phase4_hot_w3` selected third at `2026-03-09 22:22:37 UTC`
      - hot-route first-coverage delay from baseline start (`2026-03-09 22:20:40 UTC`) was `117.355s`
    - route-lanes-only verification (`ENABLE_ROUTE_LANES=1`, `ENABLE_PRIORITY_SCHEDULER=0`):
      - `/worker-routes` returned non-null `priority_score`, `computed_lane`, and `effective_lane`
      - worker cycle logs carried score components and `lane_interval_multiplier` while dispatch order remained legacy
    - final priority-scheduler verification (`ENABLE_ROUTE_LANES=1`, `ENABLE_PRIORITY_SCHEDULER=1`):
      - verified all three temporary routes shared the same persisted `next_run_at` before starting the worker
      - `phase4_hot_w3` was selected first at `2026-03-09 22:34:58 UTC`
      - hot-route first-coverage delay from Phase 4 start (`2026-03-09 22:34:57 UTC`) dropped to `1.866s`
      - first hot-route cycle emitted `query_lock_status=acquired` with `query_lock_key=query-lock:a37b7398f5d5cdd2099f30b6`
      - hot route was selected again at `2026-03-09 22:37:28 UTC`, yielding a hot-route revisit gap of `149.646s`
      - `phase4_sweep2_w3` later completed with `query_lock_status=acquired` and persisted `next_run_at - last_selected_at = 229.610s`
      - post-cycle `/worker-routes` showed hot-route `next_run_at - last_selected_at = 149.570s`
    - cleanup after acceptance:
      - deleted all temporary Phase 4 routes from Postgres
      - truncated `worker_proxy_leases`
      - cleared Redis `query-lock:*`
      - restarted `worker`, `worker_2`, and `worker_3`
      - verified final rollout state:
        - `ENABLE_REDIS_STREAM_EVENTS=1`
        - `ENABLE_NOTIFICATION_CONSUMER=1`
        - `ENABLE_GUI_WEBSOCKET_PUSH=1`
        - `ENABLE_PRIORITY_SCHEDULER=1`
        - `ENABLE_ROUTE_LANES=1`
      - verified `/worker-routes?worker_name=worker_3` returned `count=0` after cleanup, confirming the temporary acceptance routes were removed
- Acceptance criteria status:
  - `Hot routes show shorter revisit intervals than sweep routes.`  
    Met. In the final flags-on run, `phase4_hot_w3` persisted a revisit interval of `149.570s`, while `phase4_sweep2_w3` persisted `229.610s`.
  - `Per-profile request intensity does not exceed the current baseline cadence.`  
    Met. The hot route’s actual repeat-selection gap in the final run was `149.646s`, which stayed aligned with the existing `150s` route interval rather than going faster.
  - `High-value route coverage time decreases measurably versus the pre-enable baseline.`  
    Met. Hot-route first coverage improved from `117.355s` in the flags-off baseline to `1.866s` in the final flags-on run, a reduction of `115.489s`.
- Unresolved follow-ups:
  - Phase 5 still owns hot-path slimming/background enrichment so lower-value field fetches can move off the scrape hot path
  - Phase 6 still owns operator replay/backlog controls and explicit rollback tooling for live scheduler behavior
  - production currently has no persisted `worker_routes` after the acceptance cleanup, so Phase 4 remains enabled but inert until operators create DB-backed routes again through the API/GUI

---
