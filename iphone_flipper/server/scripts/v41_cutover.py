#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from server.services.common.v41_cutover import (  # noqa: E402
    V41_BROAD_QUERY,
    V41_FAMILY_NAME,
    V41_MIN_GAP_SECONDS,
    activate_v41,
    build_flag_restore_mapping,
    collect_v41_status,
    rollback_v41,
    seed_v41_broad_family,
    write_snapshot,
)

PGHOST = os.getenv("PGHOST", "127.0.0.1")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "iphone_flipper")
PGUSER = os.getenv("PGUSER", "flipper_app")
PGPASSWORD = os.getenv("PGPASSWORD", "")

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")


def _default_snapshot_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return PROJECT_ROOT / "server" / "runtime" / f"v41-cutover-snapshot-{timestamp}.json"


async def _open_db_pool() -> Any:
    import asyncpg

    return await asyncpg.create_pool(
        host=PGHOST,
        port=PGPORT,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        min_size=1,
        max_size=4,
    )


def _open_redis() -> Any:
    from redis.asyncio import Redis

    return Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD or None,
        decode_responses=True,
    )


def _load_snapshot(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


async def _run_async(args: argparse.Namespace) -> int:
    pool = await _open_db_pool()
    redis_client = _open_redis()
    try:
        if args.command == "status":
            payload = await collect_v41_status(pool, redis_client, family_name=args.family_name)
            print(json.dumps(payload, indent=2, sort_keys=True, default=str))
            return 0

        if args.command == "snapshot":
            payload = await collect_v41_status(pool, redis_client, family_name=args.family_name)
            snapshot_path = write_snapshot(args.output, payload)
            print(json.dumps({"ok": True, "snapshot_path": str(snapshot_path), "status": payload}, indent=2, sort_keys=True))
            return 0

        if args.command == "prepare":
            payload = await seed_v41_broad_family(
                pool,
                family_name=args.family_name,
                broad_query=args.broad_query,
                preferred_route_name=args.source_route,
                min_gap_seconds=args.min_gap_seconds,
            )
            status = await collect_v41_status(pool, redis_client, family_name=args.family_name)
            print(json.dumps({"ok": True, "prepare": payload, "status": status}, indent=2, sort_keys=True, default=str))
            return 0

        if args.command == "activate":
            snapshot_path = args.snapshot or _default_snapshot_path()
            snapshot_payload = await collect_v41_status(pool, redis_client, family_name=args.family_name)
            write_snapshot(snapshot_path, snapshot_payload)
            payload = await activate_v41(
                pool,
                redis_client,
                family_name=args.family_name,
                broad_query=args.broad_query,
                preferred_route_name=args.source_route,
                min_gap_seconds=args.min_gap_seconds,
                enable_first_seen_dedupe=not args.no_first_seen_dedupe,
            )
            print(
                json.dumps(
                    {
                        "ok": True,
                        "snapshot_path": str(snapshot_path),
                        "activation": payload,
                    },
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            )
            return 0

        if args.command == "rollback":
            snapshot_payload = None
            if args.snapshot is not None and args.snapshot.exists():
                snapshot_payload = _load_snapshot(args.snapshot)
            flags = await rollback_v41(redis_client, snapshot=snapshot_payload)
            status = await collect_v41_status(pool, redis_client, family_name=args.family_name)
            print(
                json.dumps(
                    {
                        "ok": True,
                        "feature_flags": flags,
                        "recommended_restore_flags": build_flag_restore_mapping(snapshot_payload),
                        "status": status,
                    },
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            )
            return 0

        raise RuntimeError(f"Unsupported command: {args.command}")
    finally:
        await pool.close()
        await redis_client.aclose()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare, activate, and roll back the V4.1 broad-family cutover.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--family-name", default=V41_FAMILY_NAME, help=f"Target V4 family name (default: {V41_FAMILY_NAME}).")

    snapshot_parser = subparsers.add_parser("snapshot", parents=[shared], help="Capture current V4.1-related DB and flag state.")
    snapshot_parser.add_argument("--output", type=Path, default=_default_snapshot_path(), help="Snapshot file path.")

    status_parser = subparsers.add_parser("status", parents=[shared], help="Print current V4.1-related status.")
    _ = status_parser

    prepare_parser = subparsers.add_parser("prepare", parents=[shared], help="Seed the broad V4 family without turning V4 on.")
    prepare_parser.add_argument("--broad-query", default=V41_BROAD_QUERY, help=f"Broad query text to validate (default: {V41_BROAD_QUERY}).")
    prepare_parser.add_argument("--source-route", default=None, help="Optional central route name to use as the seed source.")
    prepare_parser.add_argument("--min-gap-seconds", type=int, default=V41_MIN_GAP_SECONDS, help=f"Target min_gap_s value (default: {V41_MIN_GAP_SECONDS}).")

    activate_parser = subparsers.add_parser("activate", parents=[shared], help="Snapshot state, seed the broad family, and enable V4.")
    activate_parser.add_argument("--broad-query", default=V41_BROAD_QUERY, help=f"Broad query text to validate (default: {V41_BROAD_QUERY}).")
    activate_parser.add_argument("--source-route", default=None, help="Optional central route name to use as the seed source.")
    activate_parser.add_argument("--min-gap-seconds", type=int, default=V41_MIN_GAP_SECONDS, help=f"Target min_gap_s value (default: {V41_MIN_GAP_SECONDS}).")
    activate_parser.add_argument("--snapshot", type=Path, default=None, help="Optional explicit snapshot path.")
    activate_parser.add_argument("--no-first-seen-dedupe", action="store_true", help="Leave ENABLE_V4_FIRST_SEEN_DEDUPE disabled during activation.")

    rollback_parser = subparsers.add_parser("rollback", parents=[shared], help="Disable V4 flags and restore central dispatch-friendly defaults.")
    rollback_parser.add_argument("--snapshot", type=Path, default=None, help="Optional snapshot file whose flags should be restored.")

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    return asyncio.run(_run_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
