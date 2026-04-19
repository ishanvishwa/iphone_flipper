from __future__ import annotations

import asyncio
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import asyncpg

from server.services.common.lowball_report import (
    LOWBALL_REPORT_RUN_SOURCE_SCHEDULED,
    LowballCandidate,
    LowballReportConfig,
    VERIFICATION_CONFIRMED,
    VERIFICATION_UNVERIFIED,
    build_report_message,
    build_scored_candidates,
    current_perth_date,
    current_perth_datetime,
    fetch_price_history,
    fetch_report_rows,
    load_latest_report,
    persist_report_run,
    report_scheduled_at,
)
from server.services.common.model_prices import seed_model_prices_from_csv_if_empty
from server.services.common.observability import emit_json_log
from server.services.common.schema_ensure import ensure_worker_tables as ensure_common_worker_tables

logger = logging.getLogger(__name__)

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "iphone_flipper")
PGUSER = os.getenv("PGUSER", "flipper_app")
PGPASSWORD = os.getenv("PGPASSWORD", "")

MODEL_PRICES_CSV_PATH = os.getenv("MODEL_PRICES_CSV_PATH", "/app/price_list.csv").strip() or "/app/price_list.csv"
LOWBALL_REPORT_WORKER_NAME = (
    os.getenv("LOWBALL_REPORT_WORKER_NAME", "").strip()
    or os.getenv("WORKER_NAME", "").strip()
    or "lowball_report_worker"
)
LOWBALL_REPORT_PROFILE_LEASE_SECONDS = max(
    60,
    int(os.getenv("LOWBALL_REPORT_PROFILE_LEASE_SECONDS", "900") or 900),
)
LOWBALL_REPORT_VERIFICATION_TIMEOUT_MS = max(
    1000,
    int(os.getenv("LOWBALL_REPORT_VERIFICATION_TIMEOUT_MS", "15000") or 15000),
)
LOWBALL_REPORT_IDLE_POLL_SECONDS = max(
    15,
    int(os.getenv("LOWBALL_REPORT_IDLE_POLL_SECONDS", "60") or 60),
)
LOWBALL_REPORT_START_GRACE_SECONDS = max(
    0,
    int(os.getenv("LOWBALL_REPORT_START_GRACE_SECONDS", "300") or 300),
)
LOWBALL_REPORT_DELIVERY_RETRY_SECONDS = max(
    60,
    int(os.getenv("LOWBALL_REPORT_DELIVERY_RETRY_SECONDS", "900") or 900),
)
LOWBALL_REPORT_TELEGRAM_TIMEOUT_SECONDS = max(
    5,
    int(os.getenv("LOWBALL_REPORT_TELEGRAM_TIMEOUT_SECONDS", "20") or 20),
)
LOWBALL_REPORT_TELEGRAM_MAX_ATTEMPTS = max(
    1,
    int(os.getenv("LOWBALL_REPORT_TELEGRAM_MAX_ATTEMPTS", "4") or 4),
)
LOWBALL_REPORT_TELEGRAM_RETRY_DELAY_SECONDS = max(
    1,
    int(os.getenv("LOWBALL_REPORT_TELEGRAM_RETRY_DELAY_SECONDS", "5") or 5),
)
LOWBALL_REPORT_HEADLESS = str(os.getenv("WORKER_HEADLESS", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

REMOVED_TEXT_MARKERS: tuple[str, ...] = (
    "this listing isn't available anymore",
    "this listing is no longer available",
    "this item isn't available anymore",
    "item no longer available",
    "listing is no longer available",
    "seller has removed this listing",
)
SOLD_TEXT_MARKERS: tuple[str, ...] = (
    "this listing is pending",
    "this listing has sold",
    "listing marked as sold",
    "marked as sold",
)
LOGIN_TEXT_MARKERS: tuple[str, ...] = (
    "you must log in to continue",
    "log in to continue",
    "please log in",
)
CHECKPOINT_URL_MARKERS: tuple[str, ...] = (
    "/checkpoint",
    "facebook.com/checkpoint",
    "facebook.com/login",
)


@dataclass(slots=True)
class VerificationResult:
    availability_status: str
    verification_status: str
    warning: str | None = None
    detail: str | None = None
    final_url: str | None = None


async def _report_already_sent(conn: asyncpg.Connection, report_date: date) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT 1
            FROM lowball_report_runs
            WHERE report_date = $1
              AND sent_at IS NOT NULL
            LIMIT 1
            """,
            report_date,
        )
    )


async def _delivery_retry_due_at(
    conn: asyncpg.Connection,
    report_date: date,
) -> datetime | None:
    row = await conn.fetchrow(
        """
        SELECT completed_at
        FROM lowball_report_runs
        WHERE report_date = $1
          AND run_source = $2
          AND sent_at IS NULL
          AND status = 'send_failed'
        LIMIT 1
        """,
        report_date,
        LOWBALL_REPORT_RUN_SOURCE_SCHEDULED,
    )
    if row is None:
        return None
    completed_at = row.get("completed_at")
    if completed_at is None:
        return datetime.now(timezone.utc)
    if completed_at.tzinfo is None:
        completed_at = completed_at.replace(tzinfo=timezone.utc)
    return completed_at.astimezone(timezone.utc) + timedelta(seconds=LOWBALL_REPORT_DELIVERY_RETRY_SECONDS)


async def _write_verification_state(
    conn: asyncpg.Connection,
    *,
    listing_id: str,
    availability_status: str,
    checked_at: datetime,
) -> None:
    normalized_status = str(availability_status or "").strip().lower() or "unknown"
    if normalized_status not in {"active", "sold", "removed", "unknown"}:
        normalized_status = "unknown"
    await conn.execute(
        """
        UPDATE listings
        SET
            availability_status = $2,
            last_checked_at = $3,
            last_seen_available_at = CASE
                WHEN $2 = 'active' THEN $3
                ELSE listings.last_seen_available_at
            END,
            consecutive_check_failures = CASE
                WHEN $2 = 'unknown' THEN GREATEST(0, COALESCE(consecutive_check_failures, 0)) + 1
                ELSE 0
            END
        WHERE id = $1
        """,
        listing_id,
        normalized_status,
        checked_at,
    )


def _format_exception_message(exc: Exception) -> str:
    detail = str(exc).strip()
    if detail:
        return f"{exc.__class__.__name__}: {detail}"[:250]
    return exc.__class__.__name__[:250]


async def _send_telegram_message(message: str) -> bool:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        logger.warning("Lowball report Telegram delivery skipped because TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID is not configured")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }

    try:
        import aiohttp

        async def _send_once() -> bool:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=LOWBALL_REPORT_TELEGRAM_TIMEOUT_SECONDS)
            ) as session:
                async with session.post(url, json=payload) as response:
                    body = await response.text()
                    if response.status != 200:
                        logger.warning(
                            "Lowball report Telegram send failed with status %s: %s",
                            response.status,
                            body[:250],
                        )
                        return False
                    return True

        for attempt in range(1, LOWBALL_REPORT_TELEGRAM_MAX_ATTEMPTS + 1):
            try:
                if await _send_once():
                    return True
            except Exception as exc:
                logger.warning(
                    "Lowball report Telegram send attempt %s/%s failed: %s",
                    attempt,
                    LOWBALL_REPORT_TELEGRAM_MAX_ATTEMPTS,
                    _format_exception_message(exc),
                )
            if attempt < LOWBALL_REPORT_TELEGRAM_MAX_ATTEMPTS:
                await asyncio.sleep(float(LOWBALL_REPORT_TELEGRAM_RETRY_DELAY_SECONDS))
        return False
    except ImportError:
        try:
            import requests
        except ImportError:
            logger.warning("Lowball report Telegram send skipped because neither aiohttp nor requests is installed")
            return False

        def _send_sync() -> bool:
            response = requests.post(url, json=payload, timeout=LOWBALL_REPORT_TELEGRAM_TIMEOUT_SECONDS)
            response.raise_for_status()
            body = response.json() if response.content else {}
            return bool(body.get("ok", True))

        for attempt in range(1, LOWBALL_REPORT_TELEGRAM_MAX_ATTEMPTS + 1):
            try:
                return bool(await asyncio.to_thread(_send_sync))
            except Exception as exc:
                logger.warning(
                    "Lowball report Telegram send attempt %s/%s failed: %s",
                    attempt,
                    LOWBALL_REPORT_TELEGRAM_MAX_ATTEMPTS,
                    _format_exception_message(exc),
                )
            if attempt < LOWBALL_REPORT_TELEGRAM_MAX_ATTEMPTS:
                await asyncio.sleep(float(LOWBALL_REPORT_TELEGRAM_RETRY_DELAY_SECONDS))
        return False


def _now_utc(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def next_scheduled_run_at(now: datetime, *, report_sent_today: bool) -> datetime:
    current = current_perth_datetime(now)
    today_target = report_scheduled_at(current.date())
    if current < today_target:
        return today_target.astimezone(timezone.utc)
    if report_sent_today:
        return report_scheduled_at(current.date() + timedelta(days=1)).astimezone(timezone.utc)
    delay_seconds = max(0.0, (current - today_target).total_seconds())
    if delay_seconds <= float(LOWBALL_REPORT_START_GRACE_SECONDS):
        return current.astimezone(timezone.utc)
    return report_scheduled_at(current.date() + timedelta(days=1)).astimezone(timezone.utc)


async def seconds_until_next_scheduled_run(
    pool: asyncpg.Pool,
    *,
    now: datetime | None = None,
) -> int:
    current = _now_utc(now)
    report_date = current_perth_date(current)
    async with pool.acquire() as conn:
        sent_today = await _report_already_sent(conn, report_date)
        retry_due_at = None if sent_today else await _delivery_retry_due_at(conn, report_date)
    if retry_due_at is not None:
        return max(0, int((retry_due_at - current).total_seconds()))
    next_run = next_scheduled_run_at(current, report_sent_today=sent_today)
    return max(0, int((next_run - current).total_seconds()))


class AvailabilityVerifier:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._profile: dict[str, Any] | None = None
        self._session: Any | None = None

    async def __aenter__(self) -> "AvailabilityVerifier":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        session = self._session
        profile = self._profile
        self._session = None
        self._profile = None

        if session is not None:
            try:
                from scraper import close_profile_session

                await close_profile_session(session)
            except Exception as exc:  # pragma: no cover - defensive cleanup
                logger.warning("Lowball report verifier session cleanup failed: %s", str(exc)[:250])

        if profile is not None:
            try:
                from server.services.worker.lease_manager import release_profile_lease

                await release_profile_lease(
                    self._pool,
                    profile_id=int(profile.get("profile_id") or 0) or None,
                    lease_token=str(profile.get("profile_lease_token") or ""),
                )
            except Exception as exc:  # pragma: no cover - defensive cleanup
                logger.warning("Lowball report verifier lease cleanup failed: %s", str(exc)[:250])

    async def _ensure_session(self) -> Any | None:
        if self._session is not None:
            return self._session

        from server.services.worker.lease_manager import claim_next_profile, release_profile_lease

        profile = await claim_next_profile(
            self._pool,
            worker_name=LOWBALL_REPORT_WORKER_NAME,
            lease_token=uuid.uuid4().hex,
            lease_seconds=LOWBALL_REPORT_PROFILE_LEASE_SECONDS,
        )
        if not profile:
            logger.warning("Lowball report verification skipped because no warm profile lease was available")
            return None

        profile_id = str(profile.get("dolphin_profile_id") or "").strip() or None
        user_data_dir = str(profile.get("user_data_dir") or "").strip() or None
        if not profile_id and not user_data_dir:
            await release_profile_lease(
                self._pool,
                profile_id=int(profile.get("profile_id") or 0) or None,
                lease_token=str(profile.get("profile_lease_token") or ""),
            )
            logger.warning("Lowball report verification skipped because the claimed profile has no launch identity")
            return None

        try:
            from scraper import open_profile_session

            self._session = await open_profile_session(
                profile_id=profile_id,
                user_data_dir=user_data_dir,
                headless=LOWBALL_REPORT_HEADLESS,
            )
            self._profile = profile
            return self._session
        except Exception as exc:
            logger.warning("Lowball report verifier failed to open a warm session: %s", str(exc)[:250])
            await release_profile_lease(
                self._pool,
                profile_id=int(profile.get("profile_id") or 0) or None,
                lease_token=str(profile.get("profile_lease_token") or ""),
            )
            return None

    async def _inspect_url(self, url: str) -> VerificationResult:
        session = await self._ensure_session()
        if session is None:
            return VerificationResult(
                availability_status="unknown",
                verification_status=VERIFICATION_UNVERIFIED,
                warning="\u26a0 unverified - no warm profile available",
                detail="no_profile_available",
            )

        page = session.page
        try:
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=LOWBALL_REPORT_VERIFICATION_TIMEOUT_MS,
            )
            try:
                await page.wait_for_load_state("networkidle", timeout=3000)
            except Exception:
                pass
            try:
                body_text = (await page.text_content("body") or "").strip().lower()
            except Exception:
                body_text = ""
            final_url = str(getattr(page, "url", "") or "").strip()
        except Exception as exc:
            return VerificationResult(
                availability_status="unknown",
                verification_status=VERIFICATION_UNVERIFIED,
                warning="\u26a0 unverified - check before messaging",
                detail=str(exc)[:250] or exc.__class__.__name__,
            )

        final_url_lower = final_url.lower()
        if any(marker in final_url_lower for marker in CHECKPOINT_URL_MARKERS):
            return VerificationResult(
                availability_status="unknown",
                verification_status=VERIFICATION_UNVERIFIED,
                warning="\u26a0 unverified - check before messaging",
                detail="checkpoint_or_login",
                final_url=final_url,
            )
        if any(marker in body_text for marker in LOGIN_TEXT_MARKERS):
            return VerificationResult(
                availability_status="unknown",
                verification_status=VERIFICATION_UNVERIFIED,
                warning="\u26a0 unverified - check before messaging",
                detail="login_required",
                final_url=final_url,
            )
        if any(marker in body_text for marker in REMOVED_TEXT_MARKERS):
            return VerificationResult(
                availability_status="removed",
                verification_status=VERIFICATION_CONFIRMED,
                detail="removed_marker",
                final_url=final_url,
            )
        if any(marker in body_text for marker in SOLD_TEXT_MARKERS):
            return VerificationResult(
                availability_status="sold",
                verification_status=VERIFICATION_CONFIRMED,
                detail="sold_marker",
                final_url=final_url,
            )
        return VerificationResult(
            availability_status="active",
            verification_status=VERIFICATION_CONFIRMED,
            final_url=final_url,
        )

    async def verify_candidate(
        self,
        candidate: LowballCandidate,
        *,
        now: datetime | None = None,
    ) -> VerificationResult:
        checked_at = _now_utc(now)
        url = str(candidate.url or "").strip()
        result = await self._inspect_url(url)
        async with self._pool.acquire() as conn:
            await _write_verification_state(
                conn,
                listing_id=candidate.listing_id,
                availability_status=result.availability_status,
                checked_at=checked_at,
            )
        return result


async def select_report_entries(
    pool: asyncpg.Pool,
    candidates: list[LowballCandidate],
    *,
    config: LowballReportConfig | None = None,
    verifier: Callable[[LowballCandidate], Awaitable[VerificationResult]] | None = None,
    verify_candidates: bool = True,
    now: datetime | None = None,
) -> list[LowballCandidate]:
    effective_config = config or LowballReportConfig()
    final_entries: list[LowballCandidate] = []

    if not verify_candidates:
        for candidate in candidates[: effective_config.report_size]:
            if candidate.verification_required:
                candidate.verification_required = False
                candidate.verification_status = VERIFICATION_UNVERIFIED
                candidate.verification_warning = "\u26a0 unverified - check before messaging"
            else:
                candidate.verification_status = VERIFICATION_CONFIRMED
                candidate.verification_warning = None
            final_entries.append(candidate)
        return final_entries

    async def _finalize(
        verify_candidate: Callable[[LowballCandidate], Awaitable[VerificationResult]],
    ) -> list[LowballCandidate]:
        for candidate in candidates:
            if len(final_entries) >= effective_config.report_size:
                break
            if not candidate.verification_required:
                candidate.verification_status = VERIFICATION_CONFIRMED
                candidate.verification_warning = None
                final_entries.append(candidate)
                continue

            verification = await verify_candidate(candidate)
            availability_status = str(verification.availability_status or "").strip().lower() or "unknown"
            if availability_status in {"sold", "removed"}:
                continue
            candidate.availability_status = availability_status
            candidate.verification_required = False
            candidate.verification_status = verification.verification_status
            candidate.verification_warning = verification.warning
            final_entries.append(candidate)
        return final_entries

    if verifier is not None:
        return await _finalize(verifier)

    async with AvailabilityVerifier(pool) as managed_verifier:
        return await _finalize(
            lambda candidate: managed_verifier.verify_candidate(candidate, now=now)
        )


async def run_lowball_report_once(
    *,
    pool: asyncpg.Pool,
    run_source: str,
    send_telegram: bool,
    dry_run: bool,
    verify_candidates: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    started_at = _now_utc(now)
    report_date = current_perth_date(started_at)
    effective_config = LowballReportConfig()

    async with pool.acquire() as conn:
        if send_telegram and not dry_run and await _report_already_sent(conn, report_date):
            latest = await load_latest_report(conn)
            return {
                "ok": True,
                "status": "already_sent",
                "report_date": report_date.isoformat(),
                "listing_count": int((latest or {}).get("listing_count") or 0),
                "sent": False,
                "already_sent": True,
                "item": latest,
            }

        rows = await fetch_report_rows(conn)
        history_by_listing = await fetch_price_history(
            conn,
            [str(row.get("id") or "").strip() for row in rows if str(row.get("id") or "").strip()],
        )

    candidates = build_scored_candidates(
        rows,
        history_by_listing,
        now=started_at,
        config=effective_config,
    )
    selected_entries = await select_report_entries(
        pool,
        candidates,
        config=effective_config,
        verify_candidates=verify_candidates,
        now=started_at,
    )
    report_text = build_report_message(report_date, selected_entries)

    status = "dry_run" if dry_run else "ready"
    sent_at: datetime | None = None
    last_error: str | None = None

    if send_telegram and not dry_run:
        if await _send_telegram_message(report_text):
            status = "sent"
            sent_at = datetime.now(timezone.utc)
        else:
            status = "send_failed"
            last_error = "Telegram send failed"

    async with pool.acquire() as conn:
        async with conn.transaction():
            run_row = await persist_report_run(
                conn,
                report_date=report_date,
                run_source=run_source,
                entries=selected_entries,
                report_text=report_text,
                sent_at=sent_at,
                status=status,
                last_error=last_error,
            )

    emit_json_log(
        "lowball_report_completed",
        service="lowball_report_worker",
        run_source=run_source,
        report_date=report_date.isoformat(),
        status=status,
        listing_count=len(selected_entries),
        candidate_count=len(candidates),
        sent_at=sent_at.isoformat() if sent_at is not None else None,
        dry_run=bool(dry_run),
    )
    return {
        "ok": status != "send_failed",
        "status": status,
        "report_date": report_date.isoformat(),
        "listing_count": len(selected_entries),
        "candidate_count": len(candidates),
        "sent": sent_at is not None,
        "sent_at": sent_at.isoformat() if sent_at is not None else None,
        "report_text": report_text,
        "entries": [entry.as_payload() for entry in selected_entries],
        "run": run_row,
    }


async def run_lowball_report_scheduler() -> None:
    logger.info(
        "Lowball report worker starting (worker_name=%s, verification_timeout_ms=%s, headless=%s)",
        LOWBALL_REPORT_WORKER_NAME,
        LOWBALL_REPORT_VERIFICATION_TIMEOUT_MS,
        LOWBALL_REPORT_HEADLESS,
    )

    pool = await asyncpg.create_pool(
        host=PGHOST,
        port=PGPORT,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        min_size=1,
        max_size=5,
    )
    await ensure_common_worker_tables(pool=pool, include_triggers=False)
    seeded_prices = await seed_model_prices_from_csv_if_empty(pool, csv_path=MODEL_PRICES_CSV_PATH)
    if seeded_prices:
        emit_json_log(
            "model_prices_seeded_from_csv",
            service="lowball_report_worker",
            item_count=seeded_prices,
            csv_path=MODEL_PRICES_CSV_PATH,
        )

    try:
        while True:
            wait_seconds = await seconds_until_next_scheduled_run(pool)
            if wait_seconds > 0:
                await asyncio.sleep(min(wait_seconds, LOWBALL_REPORT_IDLE_POLL_SECONDS))
                continue
            try:
                await run_lowball_report_once(
                    pool=pool,
                    run_source=LOWBALL_REPORT_RUN_SOURCE_SCHEDULED,
                    send_telegram=True,
                    dry_run=False,
                )
            except Exception:
                logger.exception("Lowball report scheduled run failed")
                emit_json_log(
                    "lowball_report_failed",
                    service="lowball_report_worker",
                    run_source=LOWBALL_REPORT_RUN_SOURCE_SCHEDULED,
                )
                await asyncio.sleep(LOWBALL_REPORT_IDLE_POLL_SECONDS)
                continue
            await asyncio.sleep(LOWBALL_REPORT_IDLE_POLL_SECONDS)
    finally:
        await pool.close()
        logger.info("Lowball report worker shut down")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_lowball_report_scheduler())


if __name__ == "__main__":
    main()
