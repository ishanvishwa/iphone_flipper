from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import asyncpg

PERTH_TZ = ZoneInfo("Australia/Perth")
LOWBALL_REPORT_TIME = time(hour=6, minute=0, tzinfo=PERTH_TZ)
LOWBALL_REPORT_RUN_SOURCE_SCHEDULED = "scheduled"
LOWBALL_REPORT_RUN_SOURCE_MANUAL = "manual"

ZONE_EXCLUDE_SCAM = "EXCLUDE_SCAM"
ZONE_RED = "RED"
ZONE_SWEET = "SWEET"
ZONE_PREMIUM = "PREMIUM"
ZONE_EXCLUDE_OVERPRICED = "EXCLUDE_OVERPRICED"
VERIFICATION_CONFIRMED = "confirmed"
VERIFICATION_NEEDED = "needs_verification"
VERIFICATION_UNVERIFIED = "unverified"


@dataclass(frozen=True)
class LowballReportConfig:
    min_profit_retention_pct: float = 0.50
    scam_floor_pct: float = 0.70
    capture_ceiling_pct: float = 1.35
    staleness_ramp_days: int = 30
    staleness_clamp_days: int = 30
    staleness_milestones: tuple[int, ...] = (7, 14, 30)
    sweet_min_days: int = 2
    premium_min_days: int = 7
    weights_sweet: Mapping[str, float] = None  # type: ignore[assignment]
    weights_premium: Mapping[str, float] = None  # type: ignore[assignment]
    report_size: int = 10
    resurface_after_days: int = 7
    big_drop_threshold: float = 0.15
    search_sweep_liveness_window_hours: int = 48
    hard_expire_after_days: int = 5
    max_consecutive_check_failures: int = 3

    def __post_init__(self) -> None:
        object.__setattr__(self, "weights_sweet", dict(self.weights_sweet or {"staleness": 0.3, "reachability": 0.4, "trajectory": 0.3}))
        object.__setattr__(self, "weights_premium", dict(self.weights_premium or {"staleness": 0.5, "reachability": 0.25, "trajectory": 0.25}))


@dataclass
class PriceHistoryPoint:
    observed_price: float
    observed_at: datetime


@dataclass
class LowballCandidate:
    listing_id: str
    title: str
    url: str
    model: str
    condition: str
    current_price: float
    target_price: float
    resale_price: float
    zone: str
    reason_tag: str
    days_since_first_seen: int
    days_since_last_price_change: int
    last_drop_days: int
    price_drop_count: int
    seller_discount: float
    reachability: float
    required_discount: float
    acceptable_discount: float
    max_acceptable_price: float
    walkaway_price: float
    staleness_raw: float
    trajectory_raw: float
    staleness_score: float = 0.0
    price_reachability_score: float = 0.0
    trajectory_score: float = 0.0
    als: float = 0.0
    last_seen_in_search_at: datetime | None = None
    last_shown_in_report_at: datetime | None = None
    current_bracket: int = 0
    availability_status: str = "active"
    verification_status: str = VERIFICATION_CONFIRMED
    verification_required: bool = False
    verification_warning: str | None = None

    def as_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("last_seen_in_search_at", "last_shown_in_report_at"):
            value = payload.get(key)
            if hasattr(value, "isoformat"):
                payload[key] = value.isoformat()
        return payload


def current_perth_datetime(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(PERTH_TZ)


def current_perth_date(now: datetime | None = None) -> date:
    return current_perth_datetime(now).date()


def report_scheduled_at(report_date: date) -> datetime:
    return datetime.combine(report_date, LOWBALL_REPORT_TIME)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return float(default)
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _days_since(value: datetime | None, now: datetime) -> int:
    if value is None:
        return 0
    return max(0, int((now - value).total_seconds() // 86400))


def current_staleness_bracket(days_since_first_seen: int, milestones: Iterable[int]) -> int:
    bracket = 0
    for milestone in sorted(int(item) for item in milestones):
        if days_since_first_seen >= milestone:
            bracket = milestone
    return bracket


def compute_walkaway_price(target_price: float, resale_price: float, *, min_profit_retention_pct: float) -> float:
    expected_profit = max(0.0, resale_price - target_price)
    min_profit_dollars = expected_profit * float(min_profit_retention_pct or 0.0)
    return resale_price - min_profit_dollars


def classify_price_zone(
    price: float,
    target_price: float,
    resale_price: float,
    *,
    scam_floor_pct: float,
    capture_ceiling_pct: float,
) -> str:
    if price < target_price * float(scam_floor_pct or 0.0):
        return ZONE_EXCLUDE_SCAM
    if price < target_price:
        return ZONE_RED
    if price <= resale_price:
        return ZONE_SWEET
    if price <= resale_price * float(capture_ceiling_pct or 0.0):
        return ZONE_PREMIUM
    return ZONE_EXCLUDE_OVERPRICED


def _percent_rank(values: list[float], value: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return 100.0
    less_than = sum(1 for item in values if item < value)
    equal_to = sum(1 for item in values if item == value)
    position = less_than + max(0.0, (equal_to - 1) / 2.0)
    return round((position / float(len(values) - 1)) * 100.0, 2)


def _pricing_history_features(
    history: list[PriceHistoryPoint],
    *,
    current_price: float,
    fallback_start_at: datetime | None,
    now: datetime,
) -> tuple[float, int, datetime | None, datetime | None]:
    if not history:
        return current_price, 0, fallback_start_at, fallback_start_at
    sorted_history = sorted(history, key=lambda item: item.observed_at)
    original_price = sorted_history[0].observed_price
    drop_count = 0
    last_price_change_at = sorted_history[0].observed_at
    last_drop_at: datetime | None = None
    previous_price = sorted_history[0].observed_price
    for point in sorted_history[1:]:
        if point.observed_price != previous_price:
            last_price_change_at = point.observed_at
        if point.observed_price < previous_price:
            drop_count += 1
            last_drop_at = point.observed_at
        previous_price = point.observed_price
    if last_price_change_at is None:
        last_price_change_at = fallback_start_at
    return original_price, drop_count, last_price_change_at, last_drop_at


def determine_reason_tag(
    *,
    current_price: float,
    price_when_last_shown: float | None,
    report_action_taken: str | None,
    last_shown_in_report_at: datetime | None,
    zone: str,
    last_zone_shown: str | None,
    current_bracket: int,
    last_staleness_bracket_shown: int | None,
    now: datetime,
    config: LowballReportConfig,
) -> str | None:
    last_price = price_when_last_shown if price_when_last_shown and price_when_last_shown > 0 else None
    action = _normalize_text(report_action_taken).lower() or None
    if last_shown_in_report_at is None:
        return "NEW"
    if last_zone_shown == ZONE_PREMIUM and zone == ZONE_SWEET:
        return "ENTERED SWEET SPOT"
    if (
        action in {"contacted", "dismissed"}
        and last_price is not None
        and current_price <= last_price * (1.0 - config.big_drop_threshold)
    ):
        return "BIG DROP"
    if last_price is not None and current_price < last_price:
        return f"DROPPED -${int(round(last_price - current_price))}"
    if current_bracket > int(last_staleness_bracket_shown or 0) and current_bracket > 0:
        return f"{current_bracket}D MARK"
    if _days_since(last_shown_in_report_at, now) >= config.resurface_after_days:
        return f"RESURFACE {config.resurface_after_days}D"
    return None


async def fetch_report_rows(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            l.id,
            l.title,
            l.url,
            l.model,
            l.condition,
            l.price,
            l.current_price,
            l.max_buy_price,
            l.availability_status,
            l.last_checked_at,
            l.last_seen_available_at,
            l.last_seen_in_search_at,
            l.consecutive_check_failures,
            l.first_seen_at,
            l.last_seen_at,
            l.created_at,
            l.updated_at,
            l.last_shown_in_report_at,
            l.price_when_last_shown,
            l.last_staleness_bracket_shown,
            l.last_zone_shown,
            l.report_action_taken,
            l.report_action_taken_at,
            mp.buying_price,
            mp.selling_price
        FROM listings l
        JOIN model_prices mp
          ON LOWER(BTRIM(l.model)) = LOWER(BTRIM(mp.model))
        WHERE TRIM(COALESCE(l.model, '')) <> ''
        ORDER BY l.updated_at DESC, l.id ASC
        """
    )
    return [dict(row) for row in rows]


async def fetch_price_history(
    conn: asyncpg.Connection,
    listing_ids: list[str],
) -> dict[str, list[PriceHistoryPoint]]:
    if not listing_ids:
        return {}
    rows = await conn.fetch(
        """
        SELECT
            listing_id,
            observed_price,
            observed_at
        FROM listing_price_history
        WHERE listing_id = ANY($1::TEXT[])
        ORDER BY listing_id ASC, observed_at ASC
        """,
        listing_ids,
    )
    history_by_listing: dict[str, list[PriceHistoryPoint]] = {}
    for row in rows:
        listing_id = _normalize_text(row["listing_id"])
        if not listing_id:
            continue
        history_by_listing.setdefault(listing_id, []).append(
            PriceHistoryPoint(
                observed_price=_safe_float(row["observed_price"]),
                observed_at=_coerce_timestamp(row["observed_at"]) or datetime.now(timezone.utc),
            )
        )
    return history_by_listing


def build_scored_candidates(
    rows: list[Mapping[str, Any]],
    history_by_listing: Mapping[str, list[PriceHistoryPoint]],
    *,
    now: datetime | None = None,
    config: LowballReportConfig | None = None,
) -> list[LowballCandidate]:
    effective_config = config or LowballReportConfig()
    current_time = now or datetime.now(timezone.utc)
    perth_today = current_perth_date(current_time)
    candidates: list[LowballCandidate] = []

    for row in rows:
        listing_id = _normalize_text(row.get("id"))
        if not listing_id:
            continue
        current_price = _safe_float(row.get("current_price") if row.get("current_price") is not None else row.get("price"))
        target_price = _safe_float(row.get("max_buy_price") if row.get("max_buy_price") is not None else row.get("buying_price"))
        resale_price = _safe_float(row.get("selling_price"))
        if current_price <= 0 or target_price <= 0 or resale_price <= 0:
            continue

        first_seen_at = _coerce_timestamp(row.get("first_seen_at")) or _coerce_timestamp(row.get("created_at"))
        last_seen_in_search_at = _coerce_timestamp(row.get("last_seen_in_search_at")) or _coerce_timestamp(row.get("last_seen_at"))
        last_shown_in_report_at = _coerce_timestamp(row.get("last_shown_in_report_at"))
        if last_shown_in_report_at is not None and current_perth_date(last_shown_in_report_at) == perth_today:
            continue

        availability_status = _normalize_text(row.get("availability_status") or "active").lower() or "active"
        if availability_status != "active":
            continue
        if int(row.get("consecutive_check_failures") or 0) > effective_config.max_consecutive_check_failures:
            continue

        zone = classify_price_zone(
            current_price,
            target_price,
            resale_price,
            scam_floor_pct=effective_config.scam_floor_pct,
            capture_ceiling_pct=effective_config.capture_ceiling_pct,
        )
        if zone not in {ZONE_SWEET, ZONE_PREMIUM}:
            continue

        days_since_first_seen = _days_since(first_seen_at, current_time)
        if zone == ZONE_SWEET and days_since_first_seen < effective_config.sweet_min_days:
            continue
        if zone == ZONE_PREMIUM and days_since_first_seen < effective_config.premium_min_days:
            continue

        original_price, price_drop_count, last_price_change_at, last_drop_at = _pricing_history_features(
            list(history_by_listing.get(listing_id) or []),
            current_price=current_price,
            fallback_start_at=first_seen_at,
            now=current_time,
        )
        seller_discount = 0.0
        if original_price > 0:
            seller_discount = max(0.0, (original_price - current_price) / original_price)
        days_since_last_price_change = _days_since(last_price_change_at, current_time)
        last_drop_days = _days_since(last_drop_at, current_time) if last_drop_at is not None else days_since_last_price_change
        if last_seen_in_search_at is not None and current_time - last_seen_in_search_at >= timedelta(days=effective_config.hard_expire_after_days):
            continue

        walkaway_price = compute_walkaway_price(
            target_price,
            resale_price,
            min_profit_retention_pct=effective_config.min_profit_retention_pct,
        )
        staleness_progress = min(1.0, days_since_first_seen / float(max(1, effective_config.staleness_ramp_days)))
        max_acceptable_price = target_price + (walkaway_price - target_price) * staleness_progress
        acceptable_discount = max(0.0, ((walkaway_price - target_price) / target_price) * staleness_progress)
        required_discount = max(0.0, (current_price - target_price) / current_price)
        reachability = acceptable_discount - required_discount
        if reachability < 0:
            continue

        action = _normalize_text(row.get("report_action_taken")).lower() or None
        price_when_last_shown = _safe_float(row.get("price_when_last_shown"), default=0.0) or None
        if action == "purchased":
            continue
        if (
            action in {"contacted", "dismissed"}
            and not (
                price_when_last_shown is not None
                and current_price <= price_when_last_shown * (1.0 - effective_config.big_drop_threshold)
            )
        ):
            continue

        current_bracket = current_staleness_bracket(days_since_first_seen, effective_config.staleness_milestones)
        reason_tag = determine_reason_tag(
            current_price=current_price,
            price_when_last_shown=price_when_last_shown,
            report_action_taken=action,
            last_shown_in_report_at=last_shown_in_report_at,
            zone=zone,
            last_zone_shown=_normalize_text(row.get("last_zone_shown")) or None,
            current_bracket=current_bracket,
            last_staleness_bracket_shown=int(row.get("last_staleness_bracket_shown") or 0),
            now=current_time,
            config=effective_config,
        )
        if not reason_tag:
            continue

        verification_required = True
        verification_status = VERIFICATION_NEEDED
        if last_seen_in_search_at is not None and current_time - last_seen_in_search_at <= timedelta(hours=effective_config.search_sweep_liveness_window_hours):
            verification_required = False
            verification_status = VERIFICATION_CONFIRMED

        candidates.append(
            LowballCandidate(
                listing_id=listing_id,
                title=_normalize_text(row.get("title")) or "Untitled listing",
                url=_normalize_text(row.get("url")),
                model=_normalize_text(row.get("model")),
                condition=_normalize_text(row.get("condition")) or "unknown",
                current_price=current_price,
                target_price=target_price,
                resale_price=resale_price,
                zone=zone,
                reason_tag=reason_tag,
                days_since_first_seen=days_since_first_seen,
                days_since_last_price_change=days_since_last_price_change,
                last_drop_days=last_drop_days,
                price_drop_count=price_drop_count,
                seller_discount=seller_discount,
                reachability=reachability,
                required_discount=required_discount,
                acceptable_discount=acceptable_discount,
                max_acceptable_price=max_acceptable_price,
                walkaway_price=walkaway_price,
                staleness_raw=min(float(days_since_first_seen), float(effective_config.staleness_clamp_days)) + (price_drop_count * 0.25),
                trajectory_raw=(price_drop_count * 10.0) + (seller_discount * 100.0) + max(0.0, effective_config.staleness_ramp_days - last_drop_days),
                last_seen_in_search_at=last_seen_in_search_at,
                last_shown_in_report_at=last_shown_in_report_at,
                current_bracket=current_bracket,
                availability_status=availability_status,
                verification_status=verification_status,
                verification_required=verification_required,
            )
        )

    by_zone: dict[str, list[LowballCandidate]] = {}
    for candidate in candidates:
        by_zone.setdefault(candidate.zone, []).append(candidate)

    for zone, zone_candidates in by_zone.items():
        staleness_values = [candidate.staleness_raw for candidate in zone_candidates]
        reachability_values = [candidate.reachability for candidate in zone_candidates]
        trajectory_values = [candidate.trajectory_raw for candidate in zone_candidates]
        weights = effective_config.weights_sweet if zone == ZONE_SWEET else effective_config.weights_premium
        for candidate in zone_candidates:
            candidate.staleness_score = _percent_rank(staleness_values, candidate.staleness_raw)
            candidate.price_reachability_score = _percent_rank(reachability_values, candidate.reachability)
            candidate.trajectory_score = _percent_rank(trajectory_values, candidate.trajectory_raw)
            candidate.als = round(
                (
                    (weights.get("staleness", 0.0) * candidate.staleness_score)
                    + (weights.get("reachability", 0.0) * candidate.price_reachability_score)
                    + (weights.get("trajectory", 0.0) * candidate.trajectory_score)
                ),
                2,
            )

    candidates.sort(
        key=lambda candidate: (
            -candidate.als,
            candidate.verification_required,
            -candidate.reachability,
            -candidate.seller_discount,
            candidate.listing_id,
        )
    )
    return candidates


def build_report_message(
    report_date: date,
    entries: list[LowballCandidate],
) -> str:
    lines = [
        f"Lowball Report - {report_date.isoformat()}",
        f"{len(entries)} opportunities",
        "",
    ]
    for index, entry in enumerate(entries, start=1):
        lines.append(f"#{index}  ALS {int(round(entry.als))}   [{entry.zone}]   {entry.reason_tag}")
        lines.append(f"{entry.model} - {entry.condition}")
        lines.append(
            f"${int(round(entry.current_price))}  (target ${int(round(entry.target_price))}, resale ${int(round(entry.resale_price))}, reach {entry.reachability:+.1%})"
        )
        lines.append(
            f"Listed {entry.days_since_first_seen}d - {entry.price_drop_count} drops - last drop {entry.last_drop_days}d ago"
        )
        lines.append(f"Seller discount: {entry.seller_discount:.0%}")
        if entry.verification_status == VERIFICATION_UNVERIFIED:
            lines.append("\u26a0 unverified - check before messaging")
        if entry.url:
            lines.append(entry.url)
        lines.append("")
    return "\n".join(lines).strip()


async def persist_report_run(
    conn: asyncpg.Connection,
    *,
    report_date: date,
    run_source: str,
    entries: list[LowballCandidate],
    report_text: str,
    sent_at: datetime | None,
    status: str,
    last_error: str | None = None,
) -> dict[str, Any]:
    run_row = await conn.fetchrow(
        """
        INSERT INTO lowball_report_runs (
            report_date,
            run_source,
            status,
            started_at,
            completed_at,
            sent_at,
            listing_count,
            report_text,
            last_error
        ) VALUES ($1, $2, $3, NOW(), NOW(), $4, $5, $6, $7)
        ON CONFLICT (report_date, run_source) DO UPDATE SET
            status = EXCLUDED.status,
            completed_at = EXCLUDED.completed_at,
            sent_at = EXCLUDED.sent_at,
            listing_count = EXCLUDED.listing_count,
            report_text = EXCLUDED.report_text,
            last_error = EXCLUDED.last_error
        RETURNING id, report_date, run_source, status, started_at, completed_at, sent_at, listing_count, report_text, last_error
        """,
        report_date,
        run_source,
        status,
        sent_at,
        len(entries),
        report_text,
        last_error,
    )
    run_id = int(run_row["id"])
    await conn.execute("DELETE FROM lowball_report_entries WHERE run_id = $1", run_id)
    if entries:
        await conn.executemany(
            """
            INSERT INTO lowball_report_entries (
                run_id,
                listing_id,
                rank,
                zone,
                reason_tag,
                als,
                reachability,
                verification_status,
                payload
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::JSONB)
            """,
            [
                (
                    run_id,
                    entry.listing_id,
                    index,
                    entry.zone,
                    entry.reason_tag,
                    entry.als,
                    entry.reachability,
                    entry.verification_status,
                    json.dumps(entry.as_payload()),
                )
                for index, entry in enumerate(entries, start=1)
            ],
        )
        await conn.executemany(
            """
            INSERT INTO als_score_log (
                listing_id,
                snapshot_date,
                zone,
                als,
                staleness_score,
                price_reachability_score,
                trajectory_score,
                reachability,
                required_discount,
                acceptable_discount,
                max_acceptable_price,
                walkaway_price,
                days_since_first_seen,
                price_drop_count,
                seller_discount,
                reason_tag
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
            )
            ON CONFLICT (listing_id, snapshot_date) DO UPDATE SET
                zone = EXCLUDED.zone,
                als = EXCLUDED.als,
                staleness_score = EXCLUDED.staleness_score,
                price_reachability_score = EXCLUDED.price_reachability_score,
                trajectory_score = EXCLUDED.trajectory_score,
                reachability = EXCLUDED.reachability,
                required_discount = EXCLUDED.required_discount,
                acceptable_discount = EXCLUDED.acceptable_discount,
                max_acceptable_price = EXCLUDED.max_acceptable_price,
                walkaway_price = EXCLUDED.walkaway_price,
                days_since_first_seen = EXCLUDED.days_since_first_seen,
                price_drop_count = EXCLUDED.price_drop_count,
                seller_discount = EXCLUDED.seller_discount,
                reason_tag = EXCLUDED.reason_tag
            """,
            [
                (
                    entry.listing_id,
                    report_date,
                    entry.zone,
                    entry.als,
                    entry.staleness_score,
                    entry.price_reachability_score,
                    entry.trajectory_score,
                    entry.reachability,
                    entry.required_discount,
                    entry.acceptable_discount,
                    entry.max_acceptable_price,
                    entry.walkaway_price,
                    entry.days_since_first_seen,
                    entry.price_drop_count,
                    entry.seller_discount,
                    entry.reason_tag,
                )
                for entry in entries
            ],
        )
        if sent_at is not None:
            await conn.executemany(
                """
                UPDATE listings
                SET
                    last_shown_in_report_at = $2,
                    price_when_last_shown = $3,
                    last_staleness_bracket_shown = $4,
                    last_zone_shown = $5
                WHERE id = $1
                """,
                [
                    (
                        entry.listing_id,
                        sent_at,
                        entry.current_price,
                        entry.current_bracket,
                        entry.zone,
                    )
                    for entry in entries
                ],
            )
    return dict(run_row)


async def load_latest_report(conn: asyncpg.Connection) -> dict[str, Any] | None:
    run_row = await conn.fetchrow(
        """
        SELECT
            id,
            report_date,
            run_source,
            status,
            started_at,
            completed_at,
            sent_at,
            listing_count,
            report_text,
            last_error
        FROM lowball_report_runs
        ORDER BY report_date DESC, id DESC
        LIMIT 1
        """
    )
    if run_row is None:
        return None
    entries = await conn.fetch(
        """
        SELECT
            rank,
            listing_id,
            zone,
            reason_tag,
            als,
            reachability,
            verification_status,
            payload
        FROM lowball_report_entries
        WHERE run_id = $1
        ORDER BY rank ASC
        """,
        int(run_row["id"]),
    )
    payload = dict(run_row)
    for key in ("started_at", "completed_at", "sent_at"):
        value = payload.get(key)
        if hasattr(value, "isoformat"):
            payload[key] = value.isoformat()
    payload["report_date"] = str(payload["report_date"])
    payload["entries"] = [dict(entry) for entry in entries]
    return payload


async def mark_listing_report_action(
    conn: asyncpg.Connection,
    *,
    listing_id: str,
    action: str,
    action_taken_at: datetime | None = None,
) -> dict[str, Any] | None:
    normalized_action = _normalize_text(action).lower()
    if normalized_action not in {"contacted", "dismissed", "purchased"}:
        raise ValueError("action must be one of contacted, dismissed, or purchased")
    row = await conn.fetchrow(
        """
        UPDATE listings
        SET
            report_action_taken = $2,
            report_action_taken_at = COALESCE($3, NOW())
        WHERE id = $1
        RETURNING id, report_action_taken, report_action_taken_at
        """,
        listing_id,
        normalized_action,
        action_taken_at,
    )
    return dict(row) if row is not None else None
