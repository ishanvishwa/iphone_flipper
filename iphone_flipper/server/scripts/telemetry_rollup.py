#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _parse_iso_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iter_lines(path: Path | None):
    if path is None:
        for line in sys.stdin:
            yield line
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            yield line


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return (float(numerator) / float(denominator)) * 100.0


def main() -> int:
    parser = argparse.ArgumentParser(description="Roll up worker scrape-cycle telemetry JSON logs.")
    parser.add_argument("--file", help="Path to telemetry JSON log file. Defaults to stdin.")
    parser.add_argument("--minutes", type=int, default=60, help="Lookback window in minutes (default: 60).")
    args = parser.parse_args()

    log_path = Path(args.file).expanduser() if args.file else None
    lookback_minutes = max(1, int(args.minutes or 60))
    now_dt = datetime.now(timezone.utc)
    cutoff_dt = now_dt - timedelta(minutes=lookback_minutes)

    overall = {
        "total": 0,
        "ok": 0,
        "fail": 0,
        "wait": 0,
        "needs_login": 0,
        "proxy_mismatch": 0,
    }
    per_route: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "total": 0,
            "ok": 0,
            "fail": 0,
            "wait": 0,
            "needs_login": 0,
            "proxy_mismatch": 0,
            "retries": 0,
            "last_success_at": None,
        }
    )

    for raw_line in _iter_lines(log_path):
        line = str(raw_line or "").strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("event") or "").strip() != "scrape_cycle":
            continue
        started_at = _parse_iso_timestamp(str(payload.get("start_ts") or ""))
        if started_at is None or started_at < cutoff_dt:
            continue

        route_name = str(payload.get("route_name") or "unknown").strip() or "unknown"
        outcome = str(payload.get("outcome") or "").strip().lower()
        error_category = str(payload.get("error_category") or "").strip().lower()
        retry_count = int(payload.get("retry_count") or 0)

        route_stats = per_route[route_name]
        route_stats["total"] += 1
        route_stats["retries"] += max(0, retry_count)
        overall["total"] += 1

        if outcome == "ok":
            route_stats["ok"] += 1
            route_stats["last_success_at"] = started_at
            overall["ok"] += 1
        elif outcome == "needs_login":
            route_stats["needs_login"] += 1
            overall["needs_login"] += 1
        elif outcome.startswith("wait_"):
            route_stats["wait"] += 1
            overall["wait"] += 1
        else:
            route_stats["fail"] += 1
            overall["fail"] += 1

        if error_category == "proxy_mismatch":
            route_stats["proxy_mismatch"] += 1
            overall["proxy_mismatch"] += 1

    print(f"Telemetry window: last {lookback_minutes} minute(s), since {cutoff_dt.isoformat()}")
    print(f"Total cycles: {overall['total']}")
    if overall["total"] <= 0:
        return 0
    print(
        "Overall: "
        f"ok={overall['ok']} ({_pct(overall['ok'], overall['total']):.1f}%), "
        f"wait={overall['wait']}, fail={overall['fail']}, "
        f"needs_login={overall['needs_login']}, proxy_mismatch={overall['proxy_mismatch']}"
    )
    print("")
    print(
        f"{'route':<20} {'cycles':>6} {'ok%':>6} {'wait':>6} {'fail':>6} {'login':>6} {'p_mis':>6} {'retries':>8} {'last_success':>20}"
    )
    print("-" * 96)
    for route_name in sorted(per_route.keys()):
        stats = per_route[route_name]
        last_success = stats["last_success_at"].isoformat() if stats["last_success_at"] else "-"
        print(
            f"{route_name:<20} "
            f"{stats['total']:>6} "
            f"{_pct(stats['ok'], stats['total']):>5.1f}% "
            f"{stats['wait']:>6} "
            f"{stats['fail']:>6} "
            f"{stats['needs_login']:>6} "
            f"{stats['proxy_mismatch']:>6} "
            f"{stats['retries']:>8} "
            f"{last_success:>20}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
