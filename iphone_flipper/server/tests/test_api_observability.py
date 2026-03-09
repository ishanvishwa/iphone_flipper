from __future__ import annotations

import asyncio
import contextlib
import json
import time
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

try:
    from server.services.api.app import main as api_main
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    api_main = None


class _DummyWebSocket:
    def __init__(self, receive_messages: list[dict[str, object]] | None = None) -> None:
        self.accept = AsyncMock()
        self.send_json = AsyncMock()
        self.close = AsyncMock()
        self.client = ("127.0.0.1", 9000)
        self._receive_messages = list(receive_messages or [])

    async def receive(self) -> dict[str, object]:
        if self._receive_messages:
            return dict(self._receive_messages.pop(0))
        return {"type": "websocket.disconnect"}


@unittest.skipIf(api_main is None, "API dependencies are not installed.")
class ApiObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._original_ws_manager = getattr(api_main.app.state, "ws_manager", None)
        api_main.app.state.ws_manager = api_main.WebSocketConnectionManager()

    async def asyncTearDown(self) -> None:
        if self._original_ws_manager is None:
            with contextlib.suppress(Exception):
                delattr(api_main.app.state, "ws_manager")
        else:
            api_main.app.state.ws_manager = self._original_ws_manager

    async def test_broadcast_logs_listing_push_without_clients(self) -> None:
        payload = {
            "event": "listing_created",
            "worker": "worker",
            "route_name": "profile_1",
            "listing": {"id": "listing-1"},
        }
        listing_item = {
            "seq_id": 101,
            "id": "listing-1",
            "title": "iPhone 15",
            "price": 900,
            "location": "Perth",
            "url": "https://example.com/listing-1",
            "description": "",
            "seller_name": "",
            "model": "iPhone 15",
            "condition": "used",
            "max_buy_price": 950,
            "potential_profit": 100,
            "status": "new",
            "source_seen_at": "2026-03-09T00:00:00+00:00",
            "created_at": "2026-03-09T00:00:00+00:00",
            "updated_at": "2026-03-09T00:00:00+00:00",
        }

        with (
            patch.object(api_main, "_is_gui_websocket_push_enabled", AsyncMock(return_value=True)),
            patch.object(api_main, "_fetch_listing_item", AsyncMock(return_value=listing_item)),
            patch.object(api_main, "emit_json_log") as emit_mock,
        ):
            await api_main._broadcast(payload)

        emit_mock.assert_called_once()
        _, kwargs = emit_mock.call_args
        self.assertEqual(kwargs["listing_id"], "listing-1")
        self.assertEqual(kwargs["cursor"], 101)
        self.assertEqual(kwargs["websocket_client_count"], 0)
        self.assertEqual(kwargs["event_name"], "listing_created")

    async def test_broadcast_pushes_normalized_snapshot_to_connected_clients(self) -> None:
        websocket = _DummyWebSocket()
        client = api_main.ManagedWebSocketClient(websocket=websocket)
        api_main.app.state.ws_manager._clients[client.client_id] = client
        payload = {
            "event": "listing_updated",
            "worker": "worker_2",
            "route_name": "profile_2",
            "listing": {"id": "listing-2"},
            "at": "2026-03-09T01:02:03+00:00",
        }
        listing_item = {
            "seq_id": 205,
            "id": "listing-2",
            "title": "iPhone 15 Pro",
            "price": 1200,
            "location": "Perth",
            "url": "https://example.com/listing-2",
            "description": "",
            "seller_name": "",
            "model": "iPhone 15 Pro",
            "condition": "used",
            "max_buy_price": 1260,
            "potential_profit": 140,
            "status": "new",
            "source_seen_at": "2026-03-09T01:00:00+00:00",
            "created_at": "2026-03-09T01:00:00+00:00",
            "updated_at": "2026-03-09T01:01:00+00:00",
        }

        with (
            patch.object(api_main, "_is_gui_websocket_push_enabled", AsyncMock(return_value=True)),
            patch.object(api_main, "_fetch_listing_item", AsyncMock(return_value=listing_item)),
            patch.object(api_main, "emit_json_log") as emit_mock,
        ):
            await api_main._broadcast(payload)

        websocket.send_json.assert_awaited_once_with(
            {
                "event": "listing_snapshot",
                "at": "2026-03-09T01:02:03+00:00",
                "cursor": 205,
                "item": listing_item,
                "worker_name": "worker_2",
                "route_name": "profile_2",
                "source": "websocket",
            }
        )
        emit_mock.assert_called_once()
        self.assertEqual(emit_mock.call_args.kwargs["websocket_client_count"], 1)

    async def test_listing_row_to_item_is_websocket_json_safe(self) -> None:
        item = api_main._listing_row_to_item(
            {
                "seq_id": 205,
                "id": "listing-2",
                "title": "iPhone 15 Pro",
                "price": Decimal("1200.50"),
                "location": "Perth",
                "url": "https://example.com/listing-2",
                "description": "",
                "seller_name": "",
                "model": "iPhone 15 Pro",
                "condition": "used",
                "max_buy_price": Decimal("1260.00"),
                "potential_profit": Decimal("140.25"),
                "status": "new",
                "source_seen_at": "2026-03-09T01:00:00+00:00",
                "created_at": "2026-03-09T01:00:00+00:00",
                "updated_at": "2026-03-09T01:01:00+00:00",
            }
        )

        self.assertEqual(item["price"], 1200.5)
        self.assertEqual(item["max_buy_price"], 1260.0)
        self.assertEqual(item["potential_profit"], 140.25)
        json.dumps(item)

    async def test_broadcast_skips_when_listing_row_is_missing(self) -> None:
        payload = {
            "event": "listing_created",
            "worker": "worker",
            "route_name": "profile_1",
            "listing": {"id": "listing-3"},
        }

        with (
            patch.object(api_main, "_is_gui_websocket_push_enabled", AsyncMock(return_value=True)),
            patch.object(api_main, "_fetch_listing_item", AsyncMock(return_value=None)),
            patch.object(api_main, "emit_json_log") as emit_mock,
        ):
            await api_main._broadcast(payload)

        emit_mock.assert_called_once()
        self.assertEqual(emit_mock.call_args.args[0], "listing_gui_push_skipped")
        self.assertEqual(emit_mock.call_args.kwargs["reason"], "listing_not_found")

    async def test_ws_listings_sends_disabled_frame_when_flag_is_off(self) -> None:
        websocket = _DummyWebSocket()

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(api_main, "_is_gui_websocket_push_enabled", AsyncMock(return_value=False)),
            patch.object(api_main, "emit_json_log"),
        ):
            await api_main.ws_listings(websocket, token="test-token")

        websocket.accept.assert_awaited_once()
        websocket.send_json.assert_awaited_once_with(
            {
                "event": "websocket_disabled",
                "at": unittest.mock.ANY,
                "reason": "feature_flag_off",
            }
        )
        websocket.close.assert_awaited()

    async def test_ws_listings_records_pong_and_unregisters_client(self) -> None:
        websocket = _DummyWebSocket(
            receive_messages=[
                {"type": "websocket.receive", "text": json.dumps({"event": "pong"})},
                {"type": "websocket.disconnect"},
            ]
        )
        manager = api_main.WebSocketConnectionManager()
        captured: dict[str, api_main.ManagedWebSocketClient] = {}
        real_register = manager.register

        async def _register(ws) -> api_main.ManagedWebSocketClient:
            client = await real_register(ws)
            client.last_pong_at = 0.0
            captured["client"] = client
            return client

        manager.register = AsyncMock(side_effect=_register)  # type: ignore[method-assign]
        api_main.app.state.ws_manager = manager

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(api_main, "_is_gui_websocket_push_enabled", AsyncMock(return_value=True)),
            patch.object(api_main, "_ws_heartbeat_loop", AsyncMock(return_value=None)),
        ):
            await api_main.ws_listings(websocket, token="test-token")

        self.assertIn("client", captured)
        self.assertGreater(captured["client"].last_pong_at, 0.0)
        self.assertEqual(await api_main.app.state.ws_manager.active_count(), 0)

    async def test_ws_heartbeat_loop_sends_ping(self) -> None:
        websocket = _DummyWebSocket()
        client = api_main.ManagedWebSocketClient(websocket=websocket)

        async def _sleep_once(_seconds: float) -> None:
            if websocket.send_json.await_count:
                raise asyncio.CancelledError()
            return None

        with (
            patch.object(api_main, "_is_gui_websocket_push_enabled", AsyncMock(return_value=True)),
            patch.object(api_main.asyncio, "sleep", AsyncMock(side_effect=_sleep_once)),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await api_main._ws_heartbeat_loop(client)

        websocket.send_json.assert_awaited_once()
        sent_payload = websocket.send_json.await_args.args[0]
        self.assertEqual(sent_payload["event"], "ping")

    async def test_ws_heartbeat_loop_times_out_dead_client(self) -> None:
        websocket = _DummyWebSocket()
        client = api_main.ManagedWebSocketClient(websocket=websocket)
        client.last_pong_at = time.monotonic() - 120.0

        with (
            patch.object(api_main, "_is_gui_websocket_push_enabled", AsyncMock(return_value=True)),
            patch.object(api_main.asyncio, "sleep", AsyncMock(return_value=None)),
            patch.object(api_main, "_close_ws_client", AsyncMock()) as close_mock,
            patch.object(api_main, "emit_json_log"),
        ):
            await api_main._ws_heartbeat_loop(client)

        close_mock.assert_awaited_once()
        self.assertEqual(close_mock.await_args.kwargs["reason"], "pong_timeout")
