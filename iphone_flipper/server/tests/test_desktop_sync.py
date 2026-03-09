from __future__ import annotations

import asyncio
import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import desktop_sync


class _FakeResponse:
    def __init__(self, payload: dict[str, object], status: int = 200) -> None:
        self._payload = dict(payload)
        self.status = status
        self.content_type = "application/json"

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def json(self) -> dict[str, object]:
        return dict(self._payload)

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, responses: list[dict[str, object]] | None = None) -> None:
        self._responses = list(responses or [])

    def get(self, *_args, **_kwargs) -> _FakeResponse:
        if not self._responses:
            raise AssertionError("No fake poll response configured")
        return _FakeResponse(self._responses.pop(0))


class _FakeTreeview:
    def __init__(self) -> None:
        self.rows: dict[str, tuple[str, ...]] = {}
        self.selected: tuple[str, ...] = ()
        self.focused: str = ""
        self.seen: str | None = None

    def selection(self) -> tuple[str, ...]:
        return self.selected

    def focus(self, item_id: str | None = None) -> str:
        if item_id is None:
            return self.focused
        self.focused = item_id
        return self.focused

    def item(self, item_id: str) -> dict[str, object]:
        return {"values": self.rows[item_id]}

    def get_children(self) -> tuple[str, ...]:
        return tuple(self.rows)

    def selection_set(self, item_ids) -> None:
        if isinstance(item_ids, str):
            self.selected = (item_ids,)
            return
        self.selected = tuple(item_ids)

    def see(self, item_id: str) -> None:
        self.seen = item_id


@unittest.skipIf(desktop_sync.aiohttp is None, "aiohttp is not installed.")
class DesktopSyncEngineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "listings.db"
        self.events: list[dict[str, object]] = []
        self.persisted_cursors: list[int] = []
        self.upsert_calls: list[dict[str, object]] = []

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _build_engine(self) -> desktop_sync.ServerSyncEngine:
        config = desktop_sync.ServerSyncConfig(
            base_url="https://api.iphoneguy.com.au",
            token="test-token",
            poll_seconds=2,
            since_id=10,
            replay_window=100,
            websocket_retry_jitter_seconds=0.0,
        )

        def _upsert(_db_path: Path, items: list[dict[str, object]], *, source: str, min_seq_id_exclusive: int):
            self.upsert_calls.append(
                {
                    "source": source,
                    "min_seq_id_exclusive": min_seq_id_exclusive,
                    "seq_ids": [int(item["seq_id"]) for item in items],
                }
            )
            if not items:
                return desktop_sync.SyncApplyResult()
            return desktop_sync.SyncApplyResult(
                inserted=len(items),
                updated=0,
                max_seq_id=max(int(item["seq_id"]) for item in items),
            )

        def _persist(_db_path: Path, since_id: int) -> None:
            self.persisted_cursors.append(int(since_id))

        return desktop_sync.ServerSyncEngine(
            config=config,
            db_path=self.db_path,
            event_sink=self.events.append,
            upsert_batch_func=_upsert,
            persist_cursor_func=_persist,
            session_factory=lambda **_kwargs: _FakeSession(),
            aiohttp_module=desktop_sync.aiohttp,
            random_func=lambda: 0.0,
        )

    async def test_poll_catchup_and_buffered_websocket_merge_do_not_duplicate_cursor(self) -> None:
        engine = self._build_engine()
        session = _FakeSession(
            responses=[
                {
                    "items": [
                        {"seq_id": 11, "id": "listing-11"},
                        {"seq_id": 12, "id": "listing-12"},
                    ],
                    "next_since_id": 12,
                }
            ]
        )

        await engine._poll_until_caught_up(
            session=session,
            start_cursor=0,
            min_seq_id_exclusive=10,
            source="poll_sync",
        )

        buffered_messages: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        await buffered_messages.put({"event": "listing_snapshot", "cursor": 12, "item": {"id": "listing-12", "seq_id": 12}})
        await buffered_messages.put({"event": "listing_snapshot", "cursor": 13, "item": {"id": "listing-13", "seq_id": 13}})
        await engine._drain_buffered_messages(buffered_messages)

        self.assertEqual(len(self.upsert_calls), 2)
        self.assertEqual(self.upsert_calls[0]["source"], "poll_sync")
        self.assertEqual(self.upsert_calls[0]["seq_ids"], [11, 12])
        self.assertEqual(self.upsert_calls[1]["source"], "websocket")
        self.assertEqual(self.upsert_calls[1]["seq_ids"], [13])
        self.assertEqual(self.persisted_cursors, [12, 13])
        self.assertEqual(self.events[-1]["since_id"], 13)

    async def test_websocket_receive_loop_sends_pong_and_buffers_snapshots(self) -> None:
        engine = self._build_engine()
        ping_message = SimpleNamespace(
            type=desktop_sync.aiohttp.WSMsgType.TEXT,
            data=json.dumps({"event": "ping"}),
        )
        protocol_ping_message = SimpleNamespace(
            type=desktop_sync.aiohttp.WSMsgType.PING,
            data=b"proxy-heartbeat",
        )
        snapshot_message = SimpleNamespace(
            type=desktop_sync.aiohttp.WSMsgType.TEXT,
            data=json.dumps(
                {
                    "event": "listing_snapshot",
                    "cursor": 33,
                    "item": {"id": "listing-33", "seq_id": 33},
                }
            ),
        )
        closed_message = SimpleNamespace(type=desktop_sync.aiohttp.WSMsgType.CLOSED, data="")
        websocket = SimpleNamespace(
            receive=AsyncMock(side_effect=[ping_message, protocol_ping_message, snapshot_message, closed_message]),
            send_json=AsyncMock(),
            pong=AsyncMock(),
        )
        buffered_messages: asyncio.Queue[dict[str, object]] = asyncio.Queue()

        with self.assertRaisesRegex(RuntimeError, "websocket_closed"):
            await engine._websocket_receive_loop(
                websocket=websocket,
                buffered_messages=buffered_messages,
                stop_event=threading.Event(),
            )

        websocket.send_json.assert_awaited_once()
        self.assertEqual(websocket.send_json.await_args.args[0]["event"], "pong")
        websocket.pong.assert_awaited_once_with(b"proxy-heartbeat")
        buffered = await buffered_messages.get()
        self.assertEqual(buffered["cursor"], 33)

    async def test_poll_fallback_runs_when_live_sync_is_disabled(self) -> None:
        engine = self._build_engine()
        session = _FakeSession(
            responses=[
                {
                    "items": [{"seq_id": 21, "id": "listing-21"}],
                    "next_since_id": 21,
                }
            ]
        )

        with patch.object(desktop_sync.time, "monotonic", side_effect=[0.0, 1.0]):
            next_retry_delay = await engine._run_poll_fallback(
                session=session,
                stop_event=threading.Event(),
                retry_delay=1.0,
                status_text="Server sync polling fallback | live push disabled",
            )

        self.assertEqual(self.upsert_calls[0]["seq_ids"], [21])
        self.assertEqual(self.events[0]["type"], "status")
        self.assertGreaterEqual(next_retry_delay, 2.0)

    async def test_poll_fallback_survives_poll_errors_and_retries(self) -> None:
        engine = self._build_engine()
        stop_event = threading.Event()

        async def _poll_until_caught_up(**_kwargs) -> None:
            if not hasattr(_poll_until_caught_up, "calls"):
                _poll_until_caught_up.calls = 0  # type: ignore[attr-defined]
            _poll_until_caught_up.calls += 1  # type: ignore[attr-defined]
            if _poll_until_caught_up.calls == 1:  # type: ignore[attr-defined]
                raise RuntimeError("boom")
            stop_event.set()

        engine._poll_until_caught_up = AsyncMock(side_effect=_poll_until_caught_up)  # type: ignore[method-assign]

        next_retry_delay = await engine._run_poll_fallback(
            session=_FakeSession(),
            stop_event=stop_event,
            retry_delay=1.0,
            status_text="Server sync polling fallback | live disconnected (websocket_closed)",
        )

        self.assertEqual(engine._poll_until_caught_up.await_count, 2)
        self.assertEqual(self.events[0]["text"], "Server sync polling fallback | live disconnected (websocket_closed)")
        self.assertIn("poll retry pending (boom)", self.events[1]["text"])
        self.assertGreaterEqual(next_retry_delay, 2.0)


class DesktopSyncHelpersTests(unittest.TestCase):
    def test_derive_websocket_url_uses_secure_scheme_and_token(self) -> None:
        url = desktop_sync.derive_websocket_url("https://api.iphoneguy.com.au", "abc123")

        self.assertEqual(url, "wss://api.iphoneguy.com.au/ws/listings?token=abc123")

    def test_treeview_selection_state_round_trip_preserves_selection_and_focus(self) -> None:
        tree = _FakeTreeview()
        tree.rows = {
            "row-1": ("listing-1", "iPhone 15"),
            "row-2": ("listing-2", "iPhone 15 Pro"),
            "row-3": ("listing-3", "iPhone 16"),
        }
        tree.selected = ("row-2",)
        tree.focused = "row-2"

        state = desktop_sync.capture_treeview_listing_state(tree)

        tree.rows = {
            "row-9": ("listing-2", "iPhone 15 Pro"),
            "row-10": ("listing-4", "iPhone 16 Pro"),
        }
        tree.selected = ()
        tree.focused = ""
        desktop_sync.restore_treeview_listing_state(tree, state)

        self.assertEqual(tree.selected, ("row-9",))
        self.assertEqual(tree.focused, "row-9")
        self.assertEqual(tree.seen, "row-9")
