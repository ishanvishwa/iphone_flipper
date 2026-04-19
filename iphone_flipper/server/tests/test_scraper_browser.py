from __future__ import annotations

import types
import unittest
from unittest.mock import AsyncMock, patch

try:
    from scraper import browser
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    browser = None


class _FakePage:
    def __init__(self, *, closed: bool = False, evaluate_result: object = "complete") -> None:
        self._closed = closed
        self._evaluate_result = evaluate_result

    def is_closed(self) -> bool:
        return self._closed

    async def evaluate(self, _script: str) -> object:
        if isinstance(self._evaluate_result, Exception):
            raise self._evaluate_result
        return self._evaluate_result


class _FakeContext:
    def __init__(self, *, pages: list[_FakePage] | None = None, new_page_result: _FakePage | None = None) -> None:
        self.pages = pages or []
        self._new_page_result = new_page_result or _FakePage()
        self.route = AsyncMock()
        self.close = AsyncMock()

    async def new_page(self) -> _FakePage:
        return self._new_page_result


class _FakeBrowser:
    def __init__(self, *, contexts: list[_FakeContext] | None = None, new_context_result: _FakeContext | None = None) -> None:
        self.contexts = contexts or []
        self._new_context_result = new_context_result or _FakeContext()
        self.close = AsyncMock()

    async def new_context(self) -> _FakeContext:
        return self._new_context_result


class _FakeAsyncPlaywrightFactory:
    def __init__(self, playwright: object) -> None:
        self._playwright = playwright

    async def start(self) -> object:
        return self._playwright


class _FakeClientSession:
    async def __aenter__(self) -> "_FakeClientSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


@unittest.skipIf(browser is None, "Scraper browser dependencies are not installed.")
class ScraperBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_dolphin_browser_retries_before_success(self) -> None:
        fake_browser = object()
        chromium = types.SimpleNamespace(
            connect_over_cdp=AsyncMock(side_effect=[RuntimeError("ECONNRESET"), fake_browser])
        )
        playwright = types.SimpleNamespace(chromium=chromium)

        with patch.object(browser.asyncio, "sleep", AsyncMock()):
            result = await browser._connect_dolphin_browser(
                playwright,
                ws_endpoint="ws://127.0.0.1:9222/devtools/browser/abc",
                profile_id="123",
                launch_mode="fresh_start",
            )

        self.assertIs(result, fake_browser)
        self.assertEqual(chromium.connect_over_cdp.await_count, 2)

    async def test_probe_browser_ready_rejects_closed_page(self) -> None:
        closed_page = _FakePage(closed=True)
        context = _FakeContext(pages=[closed_page], new_page_result=closed_page)
        fake_browser = _FakeBrowser(contexts=[context], new_context_result=context)

        with self.assertRaises(browser.BrowserLaunchError) as exc_info:
            await browser._probe_browser_ready(
                fake_browser,
                profile_id="123",
                launch_mode="reused",
            )

        self.assertEqual(exc_info.exception.failure_stage, "ready_probe")
        self.assertEqual(exc_info.exception.launch_mode, "reused")

    async def test_launch_browser_context_recycles_same_profile_once_after_failed_reuse(self) -> None:
        ready_page = _FakePage()
        ready_context = _FakeContext(pages=[ready_page], new_page_result=ready_page)
        ready_browser = _FakeBrowser(contexts=[ready_context], new_context_result=ready_context)
        fake_playwright = types.SimpleNamespace(chromium=types.SimpleNamespace(), stop=AsyncMock())
        client_session = _FakeClientSession()

        with (
            patch.object(browser, "async_playwright", return_value=_FakeAsyncPlaywrightFactory(fake_playwright)),
            patch.object(browser.aiohttp, "ClientSession", return_value=client_session),
            patch.object(browser, "_start_or_reuse_dolphin_profile", AsyncMock(return_value=("ws://active", "reused"))),
            patch.object(browser, "_hard_recycle_dolphin_profile", AsyncMock(return_value="ws://recycled")) as recycle_mock,
            patch.object(
                browser,
                "_connect_dolphin_browser",
                AsyncMock(
                    side_effect=[
                        browser.BrowserLaunchError(
                            "stale active session",
                            failure_stage="connect",
                            launch_mode="reused",
                            details={"dolphin_profile_id": "123"},
                        ),
                        ready_browser,
                    ]
                ),
            ) as connect_mock,
            patch.object(browser, "_probe_browser_ready", AsyncMock(return_value=(ready_context, ready_page))),
            patch.object(browser, "_safe_close_browser_targets", AsyncMock()) as cleanup_mock,
        ):
            result = await browser.launch_browser_context(headless=False, profile_id="123")

        self.assertEqual(result.launch_mode, "recycled_start")
        recycle_mock.assert_awaited_once()
        self.assertEqual(connect_mock.await_count, 2)
        cleanup_mock.assert_awaited()

    async def test_launch_browser_context_recycles_when_active_profile_has_no_automation_endpoint(self) -> None:
        ready_page = _FakePage()
        ready_context = _FakeContext(pages=[ready_page], new_page_result=ready_page)
        ready_browser = _FakeBrowser(contexts=[ready_context], new_context_result=ready_context)
        fake_playwright = types.SimpleNamespace(chromium=types.SimpleNamespace(), stop=AsyncMock())
        client_session = _FakeClientSession()

        with (
            patch.object(browser, "async_playwright", return_value=_FakeAsyncPlaywrightFactory(fake_playwright)),
            patch.object(browser.aiohttp, "ClientSession", return_value=client_session),
            patch.object(
                browser,
                "_start_or_reuse_dolphin_profile",
                AsyncMock(
                    side_effect=browser.BrowserLaunchError(
                        "active session missing automation endpoint",
                        failure_stage="start",
                        launch_mode="reused",
                        details={"dolphin_profile_id": "123"},
                    )
                ),
            ),
            patch.object(browser, "_hard_recycle_dolphin_profile", AsyncMock(return_value="ws://recycled")) as recycle_mock,
            patch.object(browser, "_connect_dolphin_browser", AsyncMock(return_value=ready_browser)) as connect_mock,
            patch.object(browser, "_probe_browser_ready", AsyncMock(return_value=(ready_context, ready_page))),
            patch.object(browser, "_safe_close_browser_targets", AsyncMock()) as cleanup_mock,
        ):
            result = await browser.launch_browser_context(headless=False, profile_id="123")

        self.assertEqual(result.launch_mode, "recycled_start")
        recycle_mock.assert_awaited_once()
        connect_mock.assert_awaited_once()
        cleanup_mock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
