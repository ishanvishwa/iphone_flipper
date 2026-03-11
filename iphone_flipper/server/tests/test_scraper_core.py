from __future__ import annotations

import sqlite3
import types
import unittest
from unittest.mock import AsyncMock, patch

try:
    from scraper import core
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    core = None


def _fake_session(profile_id: str = "123") -> object:
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    return core.MarketplaceSession(
        profile_id=profile_id,
        playwright=types.SimpleNamespace(stop=AsyncMock()),
        browser=types.SimpleNamespace(close=AsyncMock()),
        context=types.SimpleNamespace(close=AsyncMock()),
        page=None,
        conn=conn,
        cursor=cursor,
        price_data={},
        runtime_settings={},
        close_browser_explicitly=True,
        stop_profile_on_close=True,
        launch_mode="fresh_start",
    )


@unittest.skipIf(core is None, "Scraper core dependencies are not installed.")
class ScraperCoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_profile_session_ignores_dead_browser_errors(self) -> None:
        session = _fake_session()
        session.context.close = AsyncMock(side_effect=RuntimeError("Target page, context or browser has been closed"))
        session.browser.close = AsyncMock(side_effect=RuntimeError("Browser has been closed"))
        session.playwright.stop = AsyncMock(side_effect=RuntimeError("Browser disconnected"))

        with patch.object(core, "stop_dolphin_profile", AsyncMock()) as stop_profile:
            await core.close_profile_session(session)

        stop_profile.assert_awaited_once_with("123")

    async def test_scrape_marketplace_reraises_browser_session_error_when_enabled(self) -> None:
        session = _fake_session(profile_id="756486761")
        browser_error = core.BrowserSessionLostError(
            "Browser session lost while executing query 'iPhone': Target page, context or browser has been closed",
            failure_stage="in_query",
            launch_mode="reused",
            details={"dolphin_profile_id": "756486761"},
        )

        with (
            patch.object(core, "open_profile_session", AsyncMock(return_value=session)),
            patch.object(core, "execute_session_queries", AsyncMock(side_effect=browser_error)),
            patch.object(core, "close_profile_session", AsyncMock()) as close_session,
        ):
            with self.assertRaises(core.BrowserSessionLostError):
                await core.scrape_marketplace(profile_id="756486761", raise_browser_errors=True)

        close_session.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
