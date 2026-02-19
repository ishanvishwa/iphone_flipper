from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

try:
    from fastapi import HTTPException
    from server.services.api.app.main import _assert_unique_profile_dir
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    HTTPException = Exception  # type: ignore[assignment]
    _assert_unique_profile_dir = None


@unittest.skipIf(_assert_unique_profile_dir is None, "FastAPI dependencies are not installed.")
class ApiProfileValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_skips_validation_for_disabled_route(self) -> None:
        conn = AsyncMock()
        await _assert_unique_profile_dir(
            conn=conn,
            worker_name="worker_1",
            route_name="route_a",
            profile_dir="/profiles/a",
            is_enabled=False,
        )
        conn.fetchrow.assert_not_awaited()

    async def test_raises_when_profile_dir_conflicts(self) -> None:
        conn = AsyncMock()
        conn.fetchrow.return_value = {"worker_name": "worker_2", "route_name": "route_b"}
        with self.assertRaises(HTTPException) as ctx:
            await _assert_unique_profile_dir(
                conn=conn,
                worker_name="worker_1",
                route_name="route_a",
                profile_dir="/profiles/shared",
                is_enabled=True,
            )
        self.assertEqual(ctx.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
