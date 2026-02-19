#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Error: python3 is not installed or not in PATH."
  exit 1
fi

if [ ! -d ".venv" ]; then
  echo "[Setup] Creating virtual environment..."
  "$PYTHON_BIN" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

missing_modules=0
for module in tkinter playwright openai requests; do
  if ! python -c "import ${module}" >/dev/null 2>&1; then
    missing_modules=1
    break
  fi
done

if [ "$missing_modules" -eq 1 ]; then
  echo "[Setup] Installing/updating Python dependencies..."
  pip install -r requirements.txt
fi

if [ "${1:-}" = "--check" ]; then
  echo "Launcher check passed. Environment is ready."
  exit 0
fi

echo "[Run] Starting iPhone Flipper GUI..."
exec python run_gui.py
