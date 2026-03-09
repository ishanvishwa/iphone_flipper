from __future__ import annotations

from typing import Any

VALID_ROUTE_LANES = ("hot", "warm", "sweep")


def normalize_route_lane(raw: Any, *, allow_none: bool = True) -> str | None:
    value = str(raw or "").strip().lower()
    if value in VALID_ROUTE_LANES:
        return value
    if value in {"", "auto", "default", "computed"}:
        return None if allow_none else "warm"
    return None if allow_none else "warm"
