#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SERVER_DIR="${PROJECT_ROOT}/server"

DEPLOY_SSH_TARGET="${DEPLOY_SSH_TARGET:-ubuntu@15.235.185.32}"
DEPLOY_REMOTE_DIR="${DEPLOY_REMOTE_DIR:-/home/ubuntu/iphone-flipper-server/server}"
DEPLOY_REMOTE_ROOT="${DEPLOY_REMOTE_ROOT:-$(dirname "${DEPLOY_REMOTE_DIR}")}"
DEPLOY_SERVICES="${DEPLOY_SERVICES:-api notification_worker enrichment_worker worker worker_2 worker_3 lowball_report_worker}"
DEPLOY_RUN_MIGRATIONS="${DEPLOY_RUN_MIGRATIONS:-1}"

echo "[deploy] syncing server directory to ${DEPLOY_SSH_TARGET}:${DEPLOY_REMOTE_DIR}"
rsync -az \
  --exclude '.env' \
  --exclude 'runtime/' \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${SERVER_DIR}/" "${DEPLOY_SSH_TARGET}:${DEPLOY_REMOTE_DIR}/"

echo "[deploy] syncing root runtime files to ${DEPLOY_SSH_TARGET}:${DEPLOY_REMOTE_ROOT}"
for item in scraper/ notifications.py requirements.txt .dockerignore; do
  if [[ -e "${PROJECT_ROOT}/${item}" ]]; then
    rsync -az "${PROJECT_ROOT}/${item}" "${DEPLOY_SSH_TARGET}:${DEPLOY_REMOTE_ROOT}/${item}"
  fi
done

echo "[deploy] running remote compose update"
ssh -o BatchMode=yes "${DEPLOY_SSH_TARGET}" \
  "DEPLOY_REMOTE_DIR='${DEPLOY_REMOTE_DIR}' DEPLOY_REMOTE_ROOT='${DEPLOY_REMOTE_ROOT}' DEPLOY_SERVICES='${DEPLOY_SERVICES}' DEPLOY_RUN_MIGRATIONS='${DEPLOY_RUN_MIGRATIONS}' bash -s" <<'REMOTE'
set -euo pipefail

cd "${DEPLOY_REMOTE_DIR}/infra"

# Remove legacy monolithic file if it exists to avoid module resolution conflicts
rm -f "${DEPLOY_REMOTE_ROOT}/scraper.py"

docker compose --env-file ../.env up -d postgres redis

if [[ "${DEPLOY_RUN_MIGRATIONS}" == "1" ]]; then
  echo "[deploy] applying sql migration bootstrap"
  docker compose --env-file ../.env exec -T postgres sh -lc \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
    < ../services/api/sql/001_init.sql
fi

docker compose --env-file ../.env up -d --build ${DEPLOY_SERVICES}

echo "[deploy] waiting for local api health endpoint"
for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS http://127.0.0.1:8080/healthz
echo

APP_API_TOKEN="$(awk -F= '/^APP_API_TOKEN=/{sub(/^APP_API_TOKEN=/,""); print; exit}' ../.env)"
if [[ -n "${APP_API_TOKEN}" ]]; then
  echo "[deploy] worker health"
  curl -fsS -H "x-api-token: ${APP_API_TOKEN}" http://127.0.0.1:8080/worker-health
  echo
fi

echo "[deploy] compose status"
docker compose --env-file ../.env ps
REMOTE

echo "[deploy] done"
