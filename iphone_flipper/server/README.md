# Server Runtime (VPS)

This folder contains a production baseline for:
- always-on scraper worker
- immediate per-listing upsert to PostgreSQL
- real-time event fanout via Redis + WebSocket API
- API-managed VPS scraper routes with round-robin rotation per worker

## 1) Copy this folder to VPS

From your Mac (run locally):

```bash
cd "/Users/ishanrathnayaka/Documents/Developments/iPhone Flipper/iphone_flipper"
rsync -avz server/ ubuntu@15.235.185.32:/home/ubuntu/iphone-flipper-server/
```

This creates:
- `/home/ubuntu/iphone-flipper-server/server/infra`
- `/home/ubuntu/iphone-flipper-server/server/services`

### One-command deploy (recommended after first setup)

From your Mac:

```bash
cd "/Users/ishanrathnayaka/Documents/Developments/iPhone Flipper/iphone_flipper"
./server/scripts/deploy_vps.sh
```

Optional overrides:

```bash
DEPLOY_SSH_TARGET=ubuntu@15.235.185.32 \
DEPLOY_REMOTE_DIR=/home/ubuntu/iphone-flipper-server/server \
DEPLOY_SERVICES="api worker worker_2 worker_3" \
DEPLOY_RUN_MIGRATIONS=1 \
./server/scripts/deploy_vps.sh
```

## 2) Create `.env`

On VPS:

```bash
cd /home/ubuntu/iphone-flipper-server/server
cp .env.example .env
nano .env
```

Set strong values for:
- `POSTGRES_PASSWORD`
- `REDIS_PASSWORD`
- `APP_API_TOKEN`

Set deployment values for:
- `SERVER_DOMAIN` (example: `api.iphoneguy.com.au`)
- `SCRAPER_PROXY_SERVER`, `SCRAPER_PROXY_USERNAME`, `SCRAPER_PROXY_PASSWORD` (worker_1)
- `SCRAPER_PROXY_SERVER_2`, `SCRAPER_PROXY_USERNAME_2`, `SCRAPER_PROXY_PASSWORD_2` (worker_2)
- `SCRAPER_PROXY_SERVER_3`, `SCRAPER_PROXY_USERNAME_3`, `SCRAPER_PROXY_PASSWORD_3` (worker_3 fast lane)
- `SCRAPER_QUERIES_3` (default `iPhone`)
- `SCRAPE_INTERVAL_SECONDS` (worker_1), `SCRAPE_INTERVAL_SECONDS_WORKER_2` (worker_2), `SCRAPE_INTERVAL_SECONDS_WORKER_3` (worker_3)
- `WORKER_SCROLL_TARGET_CARDS_WORKER_3`, `WORKER_SCROLL_MAX_ROUNDS_WORKER_3` for worker_3 newest-listing sweep tuning
- `WORKER_CYCLE_RETRY_ATTEMPTS`, `WORKER_CYCLE_RETRY_BACKOFF_SECONDS` (retry behavior for transient failures)
- `WORKER_DEGRADED_CONSECUTIVE_CYCLES` (require N bad cycles before degraded alert)
- `WORKER_ROUTE_COOLDOWN_BAD_CYCLES`, `WORKER_ROUTE_COOLDOWN_SECONDS` (auto-cooldown + profile/route rotation)
- `WORKER_PROXY_REUSE_COOLDOWN_SECONDS` (prevents immediate proxy reuse in `auto_rotation`)
- `WORKER_PROXY_LEASE_SECONDS` (proxy lease lock duration to avoid same-proxy concurrent sessions)
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` (optional worker-side listing alerts)
- `TELEGRAM_NOTIFY_MIN_PROFIT` (default `0`; send alerts for `potential_profit >= this value`)

## 3) Prepare runtime profile data

Worker uses persistent browser data under `server/runtime/`.

```bash
mkdir -p /home/ubuntu/iphone-flipper-server/server/runtime
```

If you already have a logged-in profile, copy it into:
- `server/runtime/browser_profile_1`

## 4) Start stack

```bash
cd /home/ubuntu/iphone-flipper-server/server/infra
docker compose --env-file ../.env up -d --build
```

## 5) Initialize PostgreSQL schema

```bash
docker compose --env-file ../.env exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < ../services/api/sql/001_init.sql
```

## 6) Verify health

```bash
docker compose --env-file ../.env ps
curl -sS http://127.0.0.1:8080/healthz
```

## 6.1) Validate Caddy/TLS

```bash
docker compose --env-file ../.env exec -T caddy caddy validate --config /etc/caddy/Caddyfile
curl -sS https://$SERVER_DOMAIN/healthz
```

## 7) Read live logs

```bash
docker compose --env-file ../.env logs -f worker
docker compose --env-file ../.env logs -f api
```

Worker logs show Telegram send results when listing alerts are enabled.

## 8) Manage VPS scraper routes from GUI/API (recommended)

The desktop app now exposes a VPS-first single settings surface: **Settings > Connections & Scraper > VPS Scrapers**.
Routes are saved to server DB (`worker_routes`) through API and are rotated per worker cycle.

Each route includes:
- `worker_name` (for example: `worker`/`worker_2`/`worker_3`)
- `route_name` (unique per worker)
- `proxy_server` (+ optional username/password)
- `proxy_mode` (`fixed` or `auto_rotation`)
- `proxy_pool` (optional SOCKS5 pool list for `auto_rotation`)
- `user_data_dir` (for example: `/app/runtime/browser_profile_3`)
- optional `search_queries` shard (CSV)

Behavior notes:
- In GUI, `Worker Name` can be left blank; app auto-assigns to a running/least-loaded worker.
- In GUI, when `proxy_mode=auto_rotation`, `Proxy Profile` is optional; proxy pool drives selection.
- Worker runtime now uses global proxy leases (`worker_proxy_leases`) so the same proxy is not used by two profiles at the same time.

Worker health/status is exposed by:
- `GET /worker-health`
- `GET /worker-routes`

Important:
- Login sessions for VPS routes must be prepared on VPS profiles (`/app/runtime/...`) to avoid local IP leakage.
- Local GUI manual login is for local account mode only, not VPS route profiles.
- Use the VPS Scrapers **Manual Login (SOCKS5)** action to launch remote login bootstrap command with selected proxy/profile.
- Use **Import TXT/CSV (SOCKS5)** in Proxies and **Import Pool TXT/CSV** in VPS Scrapers for bulk proxy onboarding + rotation pools.
- Use **Scraper > Worker Queries & Keywords** in GUI to edit each worker route query CSV and maintain universal accessory-negative keywords.

## 9) Scale to more accounts/workers (throughput scaling)

`worker`, `worker_2`, and `worker_3` are included in compose.
`worker_3` is configured as a fast lane (`iPhone` query + medium-depth scroll + short interval) to catch newest listings quickly.
First add more routes to existing workers (rotation) before adding more worker containers.
For `worker_4+` container services, duplicate an existing worker service block in `server/infra/docker-compose.yml`.
For each worker set:
- unique `WORKER_NAME`
- unique `WORKER_USER_DATA_DIR` (e.g. `/app/runtime/browser_profile_2`)
- unique `IPHONE_FLIPPER_DB_PATH` (e.g. `/app/runtime/worker_2.db`)
- account-specific proxy values
- optional `SCRAPER_QUERIES` shard

Then:

```bash
docker compose --env-file ../.env up -d --build
```

## 9.1) V4.1 Cutover And Rollback

Use the V4.1 cutover tool to seed the initial `iphone_broad` family, snapshot the current flag state, and keep central dispatch available for fast rollback.

From the VPS server project root:

```bash
cd /home/ubuntu/iphone-flipper-server/server
docker compose --env-file ../.env exec -T worker python server/scripts/v41_cutover.py status
docker compose --env-file ../.env exec -T worker python server/scripts/v41_cutover.py snapshot
docker compose --env-file ../.env exec -T worker python server/scripts/v41_cutover.py prepare
docker compose --env-file ../.env exec -T worker python server/scripts/v41_cutover.py activate
```

Behavior notes:
- `prepare` creates or refreshes the `iphone_broad` family with one validated broad `iPhone` variant and `min_gap_s=5`, but does not enable V4.
- `activate` writes a snapshot file under `server/runtime/`, seeds the broad family, and enables `ENABLE_V4_WARM_RUNTIME=1`. Central routes stay in place for rollback.
- `rollback` disables the V4 flags and restores central-dispatch-friendly defaults. If you provide the snapshot file created during activation, the prior flag values are restored.

Examples:

```bash
docker compose --env-file ../.env exec -T worker python server/scripts/v41_cutover.py activate --source-route worker_3__env_default
docker compose --env-file ../.env exec -T worker python server/scripts/v41_cutover.py rollback --snapshot /app/runtime/v41-cutover-snapshot-20260312T030000Z.json
```

## 9.2) Hardening Baseline (Do this before long runs)

1. Keep worker browser/runtime data persistent:
- mount `../runtime:/app/runtime` for every worker service.

2. Keep price sheet mounted read-only for every worker:
- mount `../../price_list.csv:/app/price_list.csv:ro`.

3. Keep Playwright runtime aligned:
- use `mcr.microsoft.com/playwright/python:v1.58.0-jammy`
- build step runs `python -m playwright install chromium`

4. Validate after deploy:

```bash
docker compose --env-file ../.env ps
docker compose --env-file ../.env logs --tail=120 worker worker_2 worker_3
docker compose --env-file ../.env exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select count(*) from listings;"'
```

## 10) Desktop sync API usage

- Snapshot endpoint: `GET /listings?since_id=0&limit=500`
- Live stream: `ws://<host>:8080/ws/listings?token=<APP_API_TOKEN>`
- Auth header for REST: `x-api-token: <APP_API_TOKEN>`

Recommended production endpoint via Caddy:
- `https://api.iphoneguy.com.au/listings?since_id=0&limit=500`
- `wss://api.iphoneguy.com.au/ws/listings?token=<APP_API_TOKEN>`

GUI server-sync settings (Connections & Scraper tab):
- `server_sync_enabled`: `1`
- `server_api_base_url`: `https://api.iphoneguy.com.au`
- `server_api_token`: `<APP_API_TOKEN>`
- `server_sync_poll_seconds`: `1` to `3` (recommended `2`)
- `server_sync_since_id`: `0` for first bootstrap, then auto-managed

GUI VPS monitor settings (Connections & Scraper tab):
- `server_ssh_enabled`: `1`
- `server_ssh_user`: `ubuntu`
- `server_ssh_host`: your VPS IP (example `15.235.185.32`)
- `server_ssh_project_dir`: `/home/ubuntu/iphone-flipper-server/server`
- `server_monitor_worker_services`: `worker worker_2 worker_3`
