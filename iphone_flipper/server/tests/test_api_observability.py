from __future__ import annotations

import contextlib
import unittest
from unittest.mock import AsyncMock, patch

try:
    from server.services.api.app import main as api_main
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    api_main = None


class _DummyWebSocket:
    def __init__(self) -> None:
        self.send_json = AsyncMock()
        self.close = AsyncMock()


@unittest.skipIf(api_main is None, "API dependencies are not installed.")
class ApiObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._original_ws_clients = getattr(api_main.app.state, "ws_clients", None)

    async def asyncTearDown(self) -> None:
        if self._original_ws_clients is None:
            with contextlib.suppress(Exception):
                delattr(api_main.app.state, "ws_clients")
        else:
            api_main.app.state.ws_clients = self._original_ws_clients

    async def test_broadcast_logs_listing_push_without_clients(self) -> None:
        api_main.app.state.ws_clients = set()
        payload = {
            "event": "listing_created",
            "worker": "worker",
            "route_name": "profile_1",
            "listing": {"id": "listing-1"},
        }

        with patch.object(api_main, "emit_json_log") as emit_mock:
            await api_main._broadcast(payload)

        emit_mock.assert_called_once()
        _, kwargs = emit_mock.call_args
        self.assertEqual(kwargs["listing_id"], "listing-1")
        self.assertEqual(kwargs["websocket_client_count"], 0)
        self.assertEqual(kwargs["event_name"], "listing_created")

    async def test_broadcast_logs_listing_push_with_connected_clients(self) -> None:
        websocket = _DummyWebSocket()
        api_main.app.state.ws_clients = {websocket}
        payload = {
            "event": "listing_updated",
            "worker": "worker_2",
            "route_name": "profile_2",
            "listing": {"id": "listing-2"},
        }

        with patch.object(api_main, "emit_json_log") as emit_mock:
            await api_main._broadcast(payload)

        websocket.send_json.assert_awaited_once_with(payload)
        emit_mock.assert_called_once()
        _, kwargs = emit_mock.call_args
        self.assertEqual(kwargs["listing_id"], "listing-2")
        self.assertEqual(kwargs["websocket_client_count"], 1)
        self.assertGreaterEqual(kwargs["websocket_broadcast_latency_ms"], 0)
