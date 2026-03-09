from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

try:
    from fastapi import HTTPException
    from server.services.api.app import main as api_main
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    HTTPException = Exception  # type: ignore[assignment]
    api_main = None


@unittest.skipIf(api_main is None, "FastAPI dependencies are not installed.")
class ApiRouteLaneTests(unittest.IsolatedAsyncioTestCase):
    async def test_upsert_worker_route_rejects_invalid_lane_override(self) -> None:
        payload = api_main.WorkerRouteUpsert(
            proxy_server="socks5://127.0.0.1:1080",
            lane_override="invalid-lane",
        )

        with patch.object(api_main, "_auth_rest", AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as ctx:
                await api_main.upsert_worker_route("worker", "route_a", payload, x_api_token=None)

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("lane_override", ctx.exception.detail)

    def test_payload_field_is_set_distinguishes_omitted_and_explicit_lane_override(self) -> None:
        omitted = api_main.WorkerRouteUpsert(proxy_server="socks5://127.0.0.1:1080")
        explicit = api_main.WorkerRouteUpsert(
            proxy_server="socks5://127.0.0.1:1080",
            lane_override=None,
        )

        self.assertFalse(api_main._payload_field_is_set(omitted, "lane_override"))
        self.assertTrue(api_main._payload_field_is_set(explicit, "lane_override"))


if __name__ == "__main__":
    unittest.main()
