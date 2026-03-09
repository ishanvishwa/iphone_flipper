from __future__ import annotations

import asyncio
import json
import random
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

try:
    import aiohttp
except ImportError:  # pragma: no cover - optional in some local setups
    aiohttp = None  # type: ignore[assignment]

from scraper import purge_accessory_only_listings
from server.services.common.observability import emit_json_log, utc_now_iso


@dataclass(slots=True)
class SyncApplyResult:
    inserted: int = 0
    updated: int = 0
    max_seq_id: int = 0


@dataclass(slots=True)
class TreeviewSelectionState:
    selected_listing_ids: tuple[str, ...]
    focused_listing_id: str | None


@dataclass(slots=True)
class ServerSyncConfig:
    base_url: str
    token: str
    poll_seconds: int
    since_id: int
    replay_window: int = 100
    websocket_receive_timeout_seconds: float = 35.0
    websocket_retry_initial_seconds: float = 1.0
    websocket_retry_max_seconds: float = 30.0
    websocket_retry_jitter_seconds: float = 0.75
    websocket_idle_sleep_seconds: float = 0.2


class WebSocketDisabledError(RuntimeError):
    pass


def build_server_headers(token: str) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    token_clean = str(token or "").strip()
    if token_clean:
        headers["x-api-token"] = token_clean
    return headers


def derive_websocket_url(base_url: str, token: str) -> str:
    parsed = urlparse(str(base_url or "").strip())
    scheme = "wss" if parsed.scheme == "https" else "ws"
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if str(token or "").strip():
        query["token"] = str(token).strip()
    return urlunparse(
        (
            scheme,
            parsed.netloc,
            "/ws/listings",
            "",
            urlencode(query),
            "",
        )
    )


def persist_sync_cursor(db_path: Path, since_id: int) -> None:
    now_iso = datetime.now().isoformat()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO scraper_settings (setting_key, setting_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
        """,
        ("server_sync_since_id", str(max(0, int(since_id))), now_iso),
    )
    conn.commit()
    conn.close()


def apply_server_listing_batch(
    db_path: Path,
    items: list[dict[str, Any]],
    *,
    source: str,
    min_seq_id_exclusive: int,
) -> SyncApplyResult:
    eligible_items: list[dict[str, Any]] = []
    for item in items:
        seq_id = _safe_int(item.get("seq_id"), 0)
        listing_id = str(item.get("id") or "").strip()
        if not listing_id or seq_id <= int(min_seq_id_exclusive):
            continue
        eligible_items.append(item)

    if not eligible_items:
        return SyncApplyResult()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    listing_ids = [str(item.get("id") or "").strip() for item in eligible_items]

    existing_ids: set[str] = set()
    placeholders = ",".join(["?"] * len(listing_ids))
    cursor.execute(f"SELECT id FROM listings WHERE id IN ({placeholders})", listing_ids)
    existing_ids = {str(row[0]) for row in cursor.fetchall()}

    inserted = 0
    updated = 0
    max_seq_id = int(min_seq_id_exclusive)
    now_iso = datetime.now().isoformat()

    for item in eligible_items:
        listing_id = str(item.get("id") or "").strip()
        seq_id = _safe_int(item.get("seq_id"), 0)
        max_seq_id = max(max_seq_id, seq_id)
        title = str(item.get("title") or "")
        location = str(item.get("location") or "")
        url = str(item.get("url") or "")
        description = str(item.get("description") or "")
        seller_name = str(item.get("seller_name") or "")
        model = str(item.get("model") or "")
        condition = str(item.get("condition") or "")
        status = str(item.get("status") or "new")
        created_at = str(item.get("created_at") or now_iso)
        updated_at = str(item.get("updated_at") or now_iso)

        cursor.execute(
            """
            INSERT INTO listings (
                id, title, price, location, url, description, seller_name, model,
                condition, max_buy_price, potential_profit, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                price = excluded.price,
                location = excluded.location,
                url = excluded.url,
                description = excluded.description,
                seller_name = excluded.seller_name,
                model = excluded.model,
                condition = excluded.condition,
                max_buy_price = excluded.max_buy_price,
                potential_profit = excluded.potential_profit,
                status = CASE
                    WHEN listings.status = 'purchased' THEN listings.status
                    ELSE excluded.status
                END,
                updated_at = excluded.updated_at
            """,
            (
                listing_id,
                title,
                _safe_float(item.get("price")),
                location,
                url,
                description,
                seller_name,
                model,
                condition,
                _safe_float(item.get("max_buy_price")),
                _safe_float(item.get("potential_profit")),
                status,
                created_at,
                updated_at,
            ),
        )

        if listing_id in existing_ids:
            updated += 1
            sync_action = "updated"
        else:
            inserted += 1
            sync_action = "inserted"

        emit_json_log(
            "listing_gui_rendered",
            listing_id=listing_id,
            source=source,
            sync_action=sync_action,
            cursor=seq_id,
            gui_rendered_ts=utc_now_iso(),
            server_created_at=created_at,
            server_updated_at=updated_at,
        )

    purge_accessory_only_listings(cursor)
    conn.commit()
    conn.close()
    return SyncApplyResult(inserted=inserted, updated=updated, max_seq_id=max_seq_id)


def capture_treeview_listing_state(treeview: Any) -> TreeviewSelectionState:
    selected_listing_ids: list[str] = []
    for item_id in treeview.selection():
        values = treeview.item(item_id).get("values", [])
        if values:
            selected_listing_ids.append(str(values[0]))

    focused_listing_id: str | None = None
    focused_item_id = treeview.focus()
    if focused_item_id:
        values = treeview.item(focused_item_id).get("values", [])
        if values:
            focused_listing_id = str(values[0])

    return TreeviewSelectionState(
        selected_listing_ids=tuple(selected_listing_ids),
        focused_listing_id=focused_listing_id,
    )


def restore_treeview_listing_state(treeview: Any, state: TreeviewSelectionState) -> None:
    row_by_listing_id: dict[str, str] = {}
    for item_id in treeview.get_children():
        values = treeview.item(item_id).get("values", [])
        if values:
            row_by_listing_id[str(values[0])] = item_id

    selected_item_ids = [
        row_by_listing_id[listing_id]
        for listing_id in state.selected_listing_ids
        if listing_id in row_by_listing_id
    ]
    if selected_item_ids:
        treeview.selection_set(selected_item_ids)

    focused_item_id = None
    if state.focused_listing_id and state.focused_listing_id in row_by_listing_id:
        focused_item_id = row_by_listing_id[state.focused_listing_id]
    elif selected_item_ids:
        focused_item_id = selected_item_ids[0]

    if focused_item_id:
        treeview.focus(focused_item_id)
        treeview.see(focused_item_id)


class ServerSyncEngine:
    def __init__(
        self,
        *,
        config: ServerSyncConfig,
        db_path: Path,
        event_sink: Callable[[dict[str, Any]], None],
        upsert_batch_func: Callable[..., SyncApplyResult] = apply_server_listing_batch,
        persist_cursor_func: Callable[[Path, int], None] = persist_sync_cursor,
        session_factory: Callable[..., Any] | None = None,
        aiohttp_module: Any | None = aiohttp,
        random_func: Callable[[], float] = random.random,
    ) -> None:
        self.config = config
        self.db_path = Path(db_path)
        self._event_sink = event_sink
        self._upsert_batch_func = upsert_batch_func
        self._persist_cursor_func = persist_cursor_func
        self._aiohttp = aiohttp_module
        self._session_factory = session_factory
        self._random = random_func
        self._cursor = max(0, int(config.since_id))

    async def run(self, stop_event: threading.Event) -> None:
        if self._aiohttp is None:
            self._emit_status("Server sync unavailable: 'aiohttp' dependency missing")
            return

        retry_delay = max(0.5, float(self.config.websocket_retry_initial_seconds))
        async with self._build_http_session() as session:
            while not stop_event.is_set():
                try:
                    await self._run_websocket_primary(session=session, stop_event=stop_event)
                    retry_delay = max(0.5, float(self.config.websocket_retry_initial_seconds))
                except WebSocketDisabledError:
                    retry_delay = await self._run_poll_fallback(
                        session=session,
                        stop_event=stop_event,
                        retry_delay=retry_delay,
                        status_text="Server sync polling fallback | live push disabled",
                    )
                except Exception as exc:
                    retry_delay = await self._run_poll_fallback(
                        session=session,
                        stop_event=stop_event,
                        retry_delay=retry_delay,
                        status_text=f"Server sync polling fallback | live disconnected ({exc})",
                    )

    def _build_http_session(self) -> Any:
        if self._session_factory is not None:
            return self._session_factory(headers=build_server_headers(self.config.token))
        timeout = self._aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
        return self._aiohttp.ClientSession(timeout=timeout, headers=build_server_headers(self.config.token))

    def _build_websocket_session(self) -> Any:
        if self._session_factory is not None:
            return self._session_factory(headers=build_server_headers(self.config.token))
        timeout = self._aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
        return self._aiohttp.ClientSession(timeout=timeout, headers=build_server_headers(self.config.token))

    async def _run_websocket_primary(self, *, session: Any, stop_event: threading.Event) -> None:
        websocket_url = derive_websocket_url(self.config.base_url, self.config.token)
        self._emit_status("Server sync connecting live...")
        async with self._build_websocket_session() as websocket_session:
            async with websocket_session.ws_connect(
                websocket_url,
                autoping=False,
                heartbeat=None,
            ) as websocket:
                buffered_messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
                receiver_task = asyncio.create_task(
                    self._websocket_receive_loop(
                        websocket=websocket,
                        buffered_messages=buffered_messages,
                        stop_event=stop_event,
                    )
                )
                try:
                    replay_cursor = max(0, self._cursor - int(self.config.replay_window))
                    await self._poll_until_caught_up(
                        session=session,
                        start_cursor=replay_cursor,
                        min_seq_id_exclusive=self._cursor,
                        source="poll_sync",
                    )
                    if receiver_task.done():
                        await receiver_task
                    await self._drain_buffered_messages(buffered_messages)
                    self._emit_status(f"Server sync live | cursor={self._cursor}")
                    while not stop_event.is_set():
                        if receiver_task.done():
                            await receiver_task
                        await self._drain_buffered_messages(buffered_messages)
                        await asyncio.sleep(self.config.websocket_idle_sleep_seconds)
                finally:
                    receiver_task.cancel()
                    try:
                        await receiver_task
                    except (asyncio.CancelledError, WebSocketDisabledError, RuntimeError):
                        pass

    async def _run_poll_fallback(
        self,
        *,
        session: Any,
        stop_event: threading.Event,
        retry_delay: float,
        status_text: str,
    ) -> float:
        self._emit_status(status_text)
        delay_seconds = min(
            float(self.config.websocket_retry_max_seconds),
            max(0.5, float(retry_delay)) + (self._random() * float(self.config.websocket_retry_jitter_seconds)),
        )
        deadline = time.monotonic() + delay_seconds
        while not stop_event.is_set():
            try:
                await self._poll_until_caught_up(
                    session=session,
                    start_cursor=self._cursor,
                    min_seq_id_exclusive=self._cursor,
                    source="poll_sync",
                )
            except Exception as exc:
                self._emit_status(f"{status_text} | poll retry pending ({exc})")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(float(self.config.poll_seconds), max(0.1, remaining)))
        return min(float(self.config.websocket_retry_max_seconds), max(1.0, retry_delay * 2.0))

    async def _websocket_receive_loop(
        self,
        *,
        websocket: Any,
        buffered_messages: asyncio.Queue[dict[str, Any]],
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.is_set():
            try:
                message = await asyncio.wait_for(
                    websocket.receive(),
                    timeout=float(self.config.websocket_receive_timeout_seconds),
                )
            except asyncio.TimeoutError as exc:
                raise RuntimeError("heartbeat_timeout") from exc

            message_type = getattr(message, "type", None)
            if self._aiohttp is not None and message_type == self._aiohttp.WSMsgType.TEXT:
                payload = json.loads(str(getattr(message, "data", "") or "{}"))
                event_name = str(payload.get("event") or "").strip().lower()
                if event_name == "ping":
                    await websocket.send_json({"event": "pong", "at": utc_now_iso()})
                    continue
                if event_name == "websocket_disabled":
                    raise WebSocketDisabledError(str(payload.get("reason") or "feature_flag_off"))
                if event_name == "listing_snapshot":
                    await buffered_messages.put(payload)
                continue

            if self._aiohttp is not None and message_type == self._aiohttp.WSMsgType.PING:
                await websocket.pong(getattr(message, "data", b""))
                continue
            if self._aiohttp is not None and message_type == self._aiohttp.WSMsgType.PONG:
                continue

            if self._aiohttp is not None and message_type in {
                self._aiohttp.WSMsgType.CLOSE,
                self._aiohttp.WSMsgType.CLOSED,
                self._aiohttp.WSMsgType.CLOSING,
            }:
                raise RuntimeError("websocket_closed")
            if self._aiohttp is not None and message_type == self._aiohttp.WSMsgType.ERROR:
                raise RuntimeError("websocket_error")

    async def _poll_until_caught_up(
        self,
        *,
        session: Any,
        start_cursor: int,
        min_seq_id_exclusive: int,
        source: str,
    ) -> None:
        cursor = max(0, int(start_cursor))
        total_inserted = 0
        total_updated = 0
        initial_cursor = self._cursor
        while True:
            payload = await self._fetch_listings_page(session=session, since_id=cursor)
            items = payload.get("items") or []
            next_cursor = max(cursor, _safe_int(payload.get("next_since_id"), cursor))
            result = self._upsert_batch_func(
                self.db_path,
                list(items),
                source=source,
                min_seq_id_exclusive=min_seq_id_exclusive,
            )
            total_inserted += int(result.inserted)
            total_updated += int(result.updated)
            if next_cursor > self._cursor or result.max_seq_id > self._cursor:
                self._cursor = max(self._cursor, next_cursor, int(result.max_seq_id))
                self._persist_cursor_func(self.db_path, self._cursor)
                min_seq_id_exclusive = self._cursor
            if len(items) < 500:
                break
            cursor = next_cursor

        if total_inserted or total_updated:
            self._event_sink(
                {
                    "type": "sync_applied",
                    "inserted": total_inserted,
                    "updated": total_updated,
                    "since_id": self._cursor,
                    "source": source,
                    "status_text": self._format_applied_status(source, total_inserted, total_updated, self._cursor),
                }
            )
        elif self._cursor != initial_cursor and source == "poll_sync":
            self._emit_status(f"Server sync catch-up complete | cursor={self._cursor}")

    async def _drain_buffered_messages(self, buffered_messages: asyncio.Queue[dict[str, Any]]) -> None:
        messages: list[dict[str, Any]] = []
        while True:
            try:
                messages.append(buffered_messages.get_nowait())
            except asyncio.QueueEmpty:
                break

        if not messages:
            return

        messages.sort(key=lambda item: _safe_int(item.get("cursor"), 0))
        snapshot_items = [
            dict(message.get("item") or {}, seq_id=_safe_int(message.get("cursor"), 0))
            for message in messages
            if _safe_int(message.get("cursor"), 0) > self._cursor
        ]
        if not snapshot_items:
            return

        result = self._upsert_batch_func(
            self.db_path,
            snapshot_items,
            source="websocket",
            min_seq_id_exclusive=self._cursor,
        )
        if result.max_seq_id > self._cursor:
            self._cursor = int(result.max_seq_id)
            self._persist_cursor_func(self.db_path, self._cursor)

        if result.inserted or result.updated:
            self._event_sink(
                {
                    "type": "sync_applied",
                    "inserted": int(result.inserted),
                    "updated": int(result.updated),
                    "since_id": self._cursor,
                    "source": "websocket",
                    "status_text": self._format_applied_status(
                        "websocket",
                        int(result.inserted),
                        int(result.updated),
                        self._cursor,
                    ),
                }
            )

    async def _fetch_listings_page(self, *, session: Any, since_id: int) -> dict[str, Any]:
        async with session.get(
            f"{self.config.base_url}/listings",
            params={"since_id": int(since_id), "limit": 500},
            headers=build_server_headers(self.config.token),
        ) as response:
            status = int(getattr(response, "status", 200))
            if status == 401:
                raise RuntimeError("Unauthorized (APP_API_TOKEN mismatch).")
            raise_for_status = getattr(response, "raise_for_status", None)
            if callable(raise_for_status):
                raise_for_status()
            payload = await response.json() if getattr(response, "content_type", "application/json") else {}
        return dict(payload or {})

    def _emit_status(self, text: str) -> None:
        self._event_sink({"type": "status", "text": str(text)})

    @staticmethod
    def _format_applied_status(source: str, inserted: int, updated: int, since_id: int) -> str:
        label = "live" if source == "websocket" else "poll"
        return f"Server sync {label} | +{inserted} new, {updated} updated | cursor={since_id}"


class ThreadedServerSyncCoordinator:
    def __init__(
        self,
        *,
        config: ServerSyncConfig,
        db_path: Path,
        event_queue: Any,
        upsert_batch_func: Callable[..., SyncApplyResult] = apply_server_listing_batch,
        persist_cursor_func: Callable[[Path, int], None] = persist_sync_cursor,
        session_factory: Callable[..., Any] | None = None,
        aiohttp_module: Any | None = aiohttp,
        random_func: Callable[[], float] = random.random,
    ) -> None:
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._engine = ServerSyncEngine(
            config=config,
            db_path=db_path,
            event_sink=event_queue.put_nowait,
            upsert_batch_func=upsert_batch_func,
            persist_cursor_func=persist_cursor_func,
            session_factory=session_factory,
            aiohttp_module=aiohttp_module,
            random_func=random_func,
        )

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="server-sync-worker",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self._thread = None

    def _run(self) -> None:
        asyncio.run(self._engine.run(self._stop_event))


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError, AttributeError):
        return default


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
