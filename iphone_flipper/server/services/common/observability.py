from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso(value: datetime | None = None) -> str:
    moment = value or utc_now()
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat()


def normalize_timestamp(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def monotonic_duration_ms(started_at: float, finished_at: float | None = None) -> int:
    end_value = time.monotonic() if finished_at is None else float(finished_at)
    return max(0, int((end_value - float(started_at)) * 1000))


def timestamp_delta_ms(
    started_at: datetime | str | None,
    finished_at: datetime | str | None,
) -> int | None:
    start_dt = normalize_timestamp(started_at)
    end_dt = normalize_timestamp(finished_at)
    if start_dt is None or end_dt is None:
        return None
    return max(0, int((end_dt - start_dt).total_seconds() * 1000))


def emit_json_payload(payload: dict[str, Any]) -> dict[str, Any]:
    serializable = {key: value for key, value in payload.items() if value is not None}
    line = json.dumps(serializable, default=_json_default, separators=(",", ":"), sort_keys=True)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    return serializable


def emit_json_log(event: str, **fields: Any) -> dict[str, Any]:
    payload = {"event": str(event or "unknown")}
    payload.update(fields)
    return emit_json_payload(payload)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return utc_now_iso(value)
    return str(value)
