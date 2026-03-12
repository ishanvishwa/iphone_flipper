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


class _FakePage:
    def __init__(self) -> None:
        self.wait_for_load_state = AsyncMock()
        self.wait_for_function = AsyncMock()


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

    async def test_wait_for_marketplace_results_surface_waits_for_feed_after_networkidle(self) -> None:
        page = _FakePage()

        with (
            patch.object(
                core,
                "_inspect_marketplace_results_surface",
                AsyncMock(
                    return_value={
                        "final_url": "https://www.facebook.com/marketplace/perth/search?query=iPhone",
                        "marketplace_shell_detected": True,
                        "feed_present": True,
                        "empty_state_detected": False,
                    }
                ),
            ),
            patch(
                "scraper.legacy_utils._detect_manual_login_required_state",
                AsyncMock(return_value=None),
            ),
        ):
            result = await core._wait_for_marketplace_results_surface(page, timeout_ms=12000)

        page.wait_for_load_state.assert_awaited_once_with("networkidle", timeout=12000)
        page.wait_for_function.assert_awaited_once()
        self.assertTrue(result["networkidle_reached"])
        self.assertIsNone(result["manual_login"])
        self.assertTrue(result["feed_present"])

    async def test_wait_for_marketplace_results_surface_reports_networkidle_timeout(self) -> None:
        page = _FakePage()
        page.wait_for_load_state = AsyncMock(side_effect=TimeoutError("networkidle timeout"))

        with (
            patch.object(
                core,
                "_inspect_marketplace_results_surface",
                AsyncMock(
                    return_value={
                        "final_url": "https://www.facebook.com/marketplace/perth/search?query=iPhone",
                        "marketplace_shell_detected": True,
                        "feed_present": True,
                        "empty_state_detected": False,
                    }
                ),
            ),
            patch(
                "scraper.legacy_utils._detect_manual_login_required_state",
                AsyncMock(return_value=None),
            ),
        ):
            result = await core._wait_for_marketplace_results_surface(page, timeout_ms=8000)

        self.assertFalse(result["networkidle_reached"])
        self.assertIn("networkidle timeout", result["wait_error"])
        page.wait_for_function.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
