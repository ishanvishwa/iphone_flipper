"""
Scraper browser management: stealth script injection, Playwright persistent
context launching, and human-like random delays.
"""

import asyncio
from dataclasses import dataclass
import logging
import os
import random
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright

from scraper.config import (
    BROWSER_BLOCK_RESOURCE_TYPES,
    BROWSER_MAX_CONNECTIONS_PER_HOST,
    BROWSER_MAX_CONNECTIONS_PER_PROXY,
    USER_DATA_DIR,
    _parse_env_int,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stealth scripts
# ---------------------------------------------------------------------------


async def apply_stealth_scripts(page) -> None:
    """Apply stealth JavaScript to hide automation signals.

    Randomizes hardware fingerprint values on each invocation to reduce
    cross-session fingerprint correlation.
    """
    # Randomized fingerprint values
    hw_concurrency = random.choice([4, 6, 8, 10, 12, 16])
    device_memory = random.choice([4, 8, 16])
    platform = random.choice(["MacIntel", "Win32"])
    webgl_renderer = random.choice([
        "ANGLE (Apple, Apple M1 Pro, OpenGL 4.1)",
        "ANGLE (Intel, Intel(R) UHD Graphics 630, OpenGL 4.1)",
        "ANGLE (NVIDIA, NVIDIA GeForce GTX 1650, OpenGL 4.5)",
        "ANGLE (AMD, AMD Radeon Pro 5500M, OpenGL 4.1)",
        "ANGLE (Intel, Intel(R) Iris(TM) Plus Graphics 650, OpenGL 4.1)",
    ])

    await page.add_init_script(f"""
        // Override the navigator.webdriver property
        Object.defineProperty(navigator, 'webdriver', {{
            get: () => undefined,
        }});

        // Override the chrome property
        window.chrome = {{
            runtime: {{}},
            loadTimes: function() {{}},
            csi: function() {{}},
            app: {{}},
        }};

        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({{ state: Notification.permission }}) :
                originalQuery(parameters)
        );

        // Override plugins to look more realistic
        Object.defineProperty(navigator, 'plugins', {{
            get: () => [
                {{ name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' }},
                {{ name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' }},
                {{ name: 'Native Client', filename: 'internal-nacl-plugin' }},
            ],
        }});

        // Override languages
        Object.defineProperty(navigator, 'languages', {{
            get: () => ['en-AU', 'en-US', 'en'],
        }});

        // Remove automation-related properties
        delete navigator.__proto__.webdriver;

        // Randomized platform
        Object.defineProperty(navigator, 'platform', {{
            get: () => '{platform}',
        }});

        // Randomized hardware concurrency
        Object.defineProperty(navigator, 'hardwareConcurrency', {{
            get: () => {hw_concurrency},
        }});

        // Randomized device memory
        Object.defineProperty(navigator, 'deviceMemory', {{
            get: () => {device_memory},
        }});

        // WebGL renderer spoofing
        (function() {{
            const getParameterOrig = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(param) {{
                // UNMASKED_RENDERER_WEBGL
                if (param === 0x9246) return '{webgl_renderer}';
                // UNMASKED_VENDOR_WEBGL
                if (param === 0x9245) return 'Google Inc.';
                return getParameterOrig.call(this, param);
            }};
            if (typeof WebGL2RenderingContext !== 'undefined') {{
                const getParameter2Orig = WebGL2RenderingContext.prototype.getParameter;
                WebGL2RenderingContext.prototype.getParameter = function(param) {{
                    if (param === 0x9246) return '{webgl_renderer}';
                    if (param === 0x9245) return 'Google Inc.';
                    return getParameter2Orig.call(this, param);
                }};
            }}
        }})();
    """)


# ---------------------------------------------------------------------------
# Random delay
# ---------------------------------------------------------------------------


async def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """Add a random delay to mimic human behavior."""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# Browser context launch
# ---------------------------------------------------------------------------


import aiohttp

DOLPHIN_API_URL = os.getenv("DOLPHIN_API_URL", "http://localhost:3001/v1.0")
DOLPHIN_ANTY_TOKEN = os.getenv("DOLPHIN_ANTY_TOKEN", "")
# The Dolphin browser process runs on the VPS host, not inside Docker.
# Docker containers must connect to the host's IP, not 127.0.0.1.
DOLPHIN_WS_HOST = os.getenv("DOLPHIN_WS_HOST", "host.docker.internal")
DOLPHIN_CDP_CONNECT_RETRIES = 5
DOLPHIN_CDP_CONNECT_INITIAL_BACKOFF_SECONDS = 1.0
DOLPHIN_STOP_WAIT_TIMEOUT_SECONDS = 15.0
DOLPHIN_STOP_WAIT_POLL_SECONDS = 1.0
BROWSER_LAUNCH_ERROR_PREFIX = "BROWSER_LAUNCH_FAILED:"
BROWSER_SESSION_LOST_PREFIX = "BROWSER_SESSION_LOST:"
_BROWSER_SESSION_LOST_MARKERS = (
    "target page, context or browser has been closed",
    "browser has been closed",
    "browser disconnected",
    "econnreset",
    "econnrefused",
    "websocket error",
    "connect_over_cdp",
)


@dataclass
class BrowserLaunchResult:
    playwright: Any
    browser: Any
    context: Any
    page: Any
    launch_mode: str = "unknown"

    def __iter__(self):
        yield self.playwright
        yield self.browser
        yield self.context
        yield self.page


class BrowserSessionError(RuntimeError):
    prefix = "BROWSER_SESSION_ERROR:"

    def __init__(
        self,
        message: str,
        *,
        failure_stage: str,
        launch_mode: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        self.failure_stage = str(failure_stage or "unknown").strip() or "unknown"
        self.launch_mode = str(launch_mode or "unknown").strip() or "unknown"
        payload = dict(details or {})
        payload.setdefault("browser_failure_stage", self.failure_stage)
        payload.setdefault("browser_launch_mode", self.launch_mode)
        self.details = payload
        super().__init__(f"{self.prefix} {message}")


class BrowserLaunchError(BrowserSessionError):
    prefix = BROWSER_LAUNCH_ERROR_PREFIX


class BrowserSessionLostError(BrowserSessionError):
    prefix = BROWSER_SESSION_LOST_PREFIX


def is_browser_session_error(error: object) -> bool:
    if isinstance(error, BrowserSessionError):
        return True
    text = str(error or "").strip().lower()
    if not text:
        return False
    return (
        BROWSER_LAUNCH_ERROR_PREFIX.lower() in text
        or BROWSER_SESSION_LOST_PREFIX.lower() in text
        or any(marker in text for marker in _BROWSER_SESSION_LOST_MARKERS)
    )


def is_benign_browser_shutdown_error(error: object) -> bool:
    return is_browser_session_error(error)


def _dolphin_auth_headers() -> dict[str, str]:
    """Return Authorization headers for the Dolphin Anty API."""
    headers: dict[str, str] = {}
    if DOLPHIN_ANTY_TOKEN:
        headers["Authorization"] = f"Bearer {DOLPHIN_ANTY_TOKEN}"
    return headers


async def list_dolphin_profiles() -> list[dict]:
    """List all Dolphin Anty profiles, preferring Cloud API to bypass local session errors."""
    headers = _dolphin_auth_headers()
    async with aiohttp.ClientSession() as session:
        # 1. Try Cloud API first (most reliable for listings if token exists)
        if "Authorization" in headers:
            try:
                async with session.get("https://anty-api.com/browser_profiles", headers=headers) as resp:
                    data = await resp.json()
                    if data.get("success", True) or data.get("data"):
                        return data.get("data", [])
            except Exception as exc:
                logger.warning("Failed to list Dolphin profiles from cloud API: %s", exc)

        # 2. Try Local API with headers
        try:
            async with session.get(f"{DOLPHIN_API_URL}/browser_profiles", headers=headers) as resp:
                data = await resp.json()
                if data.get("success") or data.get("data"):
                    return data.get("data", [])
        except Exception as exc:
            pass

        # 3. Fallback Local API without headers
        try:
            async with session.get(f"{DOLPHIN_API_URL}/browser_profiles") as resp:
                data = await resp.json()
                if not data.get("success") and not data.get("data"):
                    logger.warning("Failed to list Dolphin profiles locally (no auth fallback): %s", data)
                    return []
                return data.get("data", [])
        except Exception as exc:
            logger.error("Dolphin /browser_profiles completely failed: %s", exc)
            return []


def _build_dolphin_ws_endpoint(automation: Dict[str, Any], profile_id: str, *, stage: str, launch_mode: str) -> str:
    try:
        port = int(automation.get("port") or 0)
    except (TypeError, ValueError):
        port = 0
    ws_path = str(automation.get("wsEndpoint") or "").strip()
    if port <= 0:
        raise BrowserLaunchError(
            f"Dolphin profile {profile_id} did not return a valid automation port.",
            failure_stage=stage,
            launch_mode=launch_mode,
            details={"dolphin_profile_id": str(profile_id)},
        )
    return f"ws://{DOLPHIN_WS_HOST}:{port}{ws_path}"


async def _fetch_active_dolphin_ws_endpoint(session: aiohttp.ClientSession, profile_id: str) -> str | None:
    try:
        async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/active") as resp:
            active_data = await resp.json()
    except Exception as exc:
        logger.warning("Failed to query active Dolphin profile %s: %s", profile_id, exc)
        return None

    if not active_data.get("success") or not active_data.get("automation"):
        return None

    try:
        return _build_dolphin_ws_endpoint(
            active_data.get("automation") or {},
            profile_id,
            stage="start",
            launch_mode="reused",
        )
    except BrowserLaunchError as exc:
        logger.warning("Active Dolphin profile %s returned unusable automation data: %s", profile_id, exc)
        return None


async def _start_or_reuse_dolphin_profile(session: aiohttp.ClientSession, profile_id: str) -> tuple[str, str]:
    async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/start?automation=1") as resp:
        data = await resp.json()

    if data.get("success") and data.get("automation"):
        return _build_dolphin_ws_endpoint(
            data["automation"],
            profile_id,
            stage="start",
            launch_mode="fresh_start",
        ), "fresh_start"

    error_obj = data.get("errorObject") or {}
    if error_obj.get("code") == "E_BROWSER_RUN_DUPLICATE":
        logger.info("Dolphin profile %s already running, attempting to reuse active session...", profile_id)
        active_ws_endpoint = await _fetch_active_dolphin_ws_endpoint(session, profile_id)
        if active_ws_endpoint:
            return active_ws_endpoint, "reused"
        raise BrowserLaunchError(
            f"Dolphin profile {profile_id} is already running but no active automation endpoint is available.",
            failure_stage="start",
            launch_mode="reused",
            details={"dolphin_profile_id": str(profile_id)},
        )

    raise BrowserLaunchError(
        f"Failed to start Dolphin profile {profile_id}: {data}",
        failure_stage="start",
        launch_mode="fresh_start",
        details={"dolphin_profile_id": str(profile_id)},
    )


async def _stop_dolphin_profile_with_session(session: aiohttp.ClientSession, profile_id: str) -> None:
    try:
        async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/stop") as resp:
            data = await resp.json()
    except Exception as exc:
        logger.warning("Warning: Failed to stop Dolphin profile %s: %s", profile_id, exc)
        return
    if not data.get("success"):
        logger.warning("Warning: Failed to stop Dolphin profile %s: %s", profile_id, data)


async def _wait_for_dolphin_profile_inactive(session: aiohttp.ClientSession, profile_id: str) -> None:
    remaining = max(DOLPHIN_STOP_WAIT_POLL_SECONDS, float(DOLPHIN_STOP_WAIT_TIMEOUT_SECONDS))
    while remaining > 0:
        active_ws_endpoint = await _fetch_active_dolphin_ws_endpoint(session, profile_id)
        if not active_ws_endpoint:
            return
        await asyncio.sleep(DOLPHIN_STOP_WAIT_POLL_SECONDS)
        remaining -= DOLPHIN_STOP_WAIT_POLL_SECONDS
    raise BrowserLaunchError(
        f"Dolphin profile {profile_id} did not stop cleanly before recycle.",
        failure_stage="start",
        launch_mode="recycled_start",
        details={"dolphin_profile_id": str(profile_id)},
    )


async def _hard_recycle_dolphin_profile(session: aiohttp.ClientSession, profile_id: str) -> str:
    logger.info("Dolphin profile %s performing hard recycle before reconnect...", profile_id)
    await _stop_dolphin_profile_with_session(session, profile_id)
    await _wait_for_dolphin_profile_inactive(session, profile_id)
    async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/start?automation=1") as resp:
        data = await resp.json()
    if not data.get("success") or not data.get("automation"):
        raise BrowserLaunchError(
            f"Failed to restart Dolphin profile {profile_id}: {data}",
            failure_stage="start",
            launch_mode="recycled_start",
            details={"dolphin_profile_id": str(profile_id)},
        )
    return _build_dolphin_ws_endpoint(
        data["automation"],
        profile_id,
        stage="start",
        launch_mode="recycled_start",
    )


async def _connect_dolphin_browser(
    playwright: Any,
    *,
    ws_endpoint: str,
    profile_id: str,
    launch_mode: str,
) -> Any:
    await asyncio.sleep(DOLPHIN_CDP_CONNECT_INITIAL_BACKOFF_SECONDS)
    for attempt in range(1, DOLPHIN_CDP_CONNECT_RETRIES + 1):
        try:
            return await playwright.chromium.connect_over_cdp(ws_endpoint)
        except Exception as exc:
            if attempt >= DOLPHIN_CDP_CONNECT_RETRIES:
                raise BrowserLaunchError(
                    f"CDP connect failed for profile {profile_id}: {exc}",
                    failure_stage="connect",
                    launch_mode=launch_mode,
                    details={"dolphin_profile_id": str(profile_id)},
                ) from exc
            wait_seconds = DOLPHIN_CDP_CONNECT_INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "CDP connect attempt %d/%d failed for profile %s (retrying in %.1fs): %s",
                attempt,
                DOLPHIN_CDP_CONNECT_RETRIES,
                profile_id,
                wait_seconds,
                exc,
            )
            await asyncio.sleep(wait_seconds)


async def _probe_browser_ready(
    browser: Any,
    *,
    profile_id: str,
    launch_mode: str,
) -> tuple[Any, Any]:
    try:
        context = browser.contexts[0] if browser and getattr(browser, "contexts", None) else await browser.new_context()
        existing_pages = []
        try:
            existing_pages = [page for page in (context.pages or []) if not page.is_closed()]
        except Exception:
            existing_pages = []
        page = existing_pages[0] if existing_pages else await context.new_page()
        if hasattr(page, "is_closed") and page.is_closed():
            raise RuntimeError("Browser returned a closed page during readiness probe.")
        await page.evaluate("() => document.readyState || 'unknown'")
        if hasattr(page, "is_closed") and page.is_closed():
            raise RuntimeError("Browser page closed immediately after readiness probe.")
        return context, page
    except BrowserLaunchError:
        raise
    except Exception as exc:
        raise BrowserLaunchError(
            f"Browser ready probe failed for profile {profile_id}: {exc}",
            failure_stage="ready_probe",
            launch_mode=launch_mode,
            details={"dolphin_profile_id": str(profile_id)},
        ) from exc


async def _safe_close_browser_targets(context: Any, browser: Any) -> None:
    try:
        if context is not None:
            await context.close()
    except Exception:
        pass
    try:
        if browser is not None:
            await browser.close()
    except Exception:
        pass


async def stop_dolphin_profile(profile_id: str) -> None:
    """Stop a Dolphin Anty profile."""
    headers = _dolphin_auth_headers()
    async with aiohttp.ClientSession(headers=headers) as session:
        await _stop_dolphin_profile_with_session(session, profile_id)

async def launch_browser_context(
    headless: bool,
    profile_id: Optional[str] = None,
    user_data_dir: Optional[str] = None,
    blocked_resource_types: Optional[List[str]] = None,
    **kwargs
):
    """Launch a browser context from either a Dolphin profile or a local user-data-dir."""
    profile_id_value = str(profile_id or "").strip() or None
    user_data_dir_value = str(user_data_dir or "").strip() or None
    if not profile_id_value and not user_data_dir_value:
        raise ValueError("A Dolphin Anty profile_id or local user_data_dir is required.")

    if blocked_resource_types is None:
        effective_blocked_resource_types = set(BROWSER_BLOCK_RESOURCE_TYPES)
    else:
        effective_blocked_resource_types = {
            str(token).strip().lower()
            for token in blocked_resource_types
            if str(token).strip()
        }

    p = None
    browser = None
    context = None
    page = None
    launch_mode = "unknown"
    try:
        try:
            p = await async_playwright().start()
        except Exception as exc:
            raise BrowserLaunchError(
                f"Failed to start Playwright: {exc}",
                failure_stage="start",
                launch_mode="fresh_start",
                details={
                    "dolphin_profile_id": profile_id_value,
                    "user_data_dir": user_data_dir_value,
                },
            ) from exc
        if profile_id_value:
            headers = _dolphin_auth_headers()
            async with aiohttp.ClientSession(headers=headers) as session:
                last_error: BrowserLaunchError | None = None
                for recycle_attempt in range(2):
                    if recycle_attempt == 0:
                        ws_endpoint, launch_mode = await _start_or_reuse_dolphin_profile(session, profile_id_value)
                    else:
                        ws_endpoint = await _hard_recycle_dolphin_profile(session, profile_id_value)
                        launch_mode = "recycled_start"

                    try:
                        browser = await _connect_dolphin_browser(
                            p,
                            ws_endpoint=ws_endpoint,
                            profile_id=profile_id_value,
                            launch_mode=launch_mode,
                        )
                        context, page = await _probe_browser_ready(
                            browser,
                            profile_id=profile_id_value,
                            launch_mode=launch_mode,
                        )
                        break
                    except BrowserLaunchError as exc:
                        last_error = exc
                        logger.warning(
                            "Dolphin profile %s failed during %s (%s): %s",
                            profile_id_value,
                            exc.failure_stage,
                            launch_mode,
                            exc,
                        )
                        await _safe_close_browser_targets(context, browser)
                        browser = None
                        context = None
                        page = None
                        if recycle_attempt >= 1:
                            raise
                if context is None or page is None:
                    raise last_error or BrowserLaunchError(
                        f"Dolphin profile {profile_id_value} failed to produce a usable browser session.",
                        failure_stage="ready_probe",
                        launch_mode=launch_mode,
                        details={"dolphin_profile_id": str(profile_id_value)},
                    )
        else:
            launch_mode = "fresh_start"
            launch_dir = user_data_dir_value or str(USER_DATA_DIR)
            os.makedirs(launch_dir, exist_ok=True)
            try:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=launch_dir,
                    headless=headless,
                    **kwargs,
                )
            except Exception as exc:
                raise BrowserLaunchError(
                    f"Failed to launch persistent browser context for {launch_dir}: {exc}",
                    failure_stage="start",
                    launch_mode=launch_mode,
                    details={"user_data_dir": launch_dir},
                ) from exc
            browser = None
            try:
                existing_pages = [existing for existing in (context.pages or []) if not existing.is_closed()]
            except Exception:
                existing_pages = []
            page = existing_pages[0] if existing_pages else await context.new_page()
            if hasattr(page, "is_closed") and page.is_closed():
                raise BrowserLaunchError(
                    f"Persistent browser context for {launch_dir} returned a closed page.",
                    failure_stage="ready_probe",
                    launch_mode=launch_mode,
                    details={"user_data_dir": launch_dir},
                )
            try:
                await page.evaluate("() => document.readyState || 'unknown'")
            except Exception as exc:
                raise BrowserLaunchError(
                    f"Persistent browser ready probe failed for {launch_dir}: {exc}",
                    failure_stage="ready_probe",
                    launch_mode=launch_mode,
                    details={"user_data_dir": launch_dir},
                ) from exc

        if effective_blocked_resource_types:
            async def _resource_filter(route):
                try:
                    resource_type = str(route.request.resource_type or "").strip().lower()
                except Exception:
                    resource_type = ""
                try:
                    if resource_type in effective_blocked_resource_types:
                        await route.abort()
                        return
                    await route.continue_()
                except Exception:
                    try:
                        await route.continue_()
                    except Exception:
                        pass

            await context.route("**/*", _resource_filter)

        # Dolphin natively handles stealth (WebGL, fonts, Canvas, etc.)
        # so we do NOT need to apply our old manual stealth scripts here.
        return BrowserLaunchResult(
            playwright=p,
            browser=browser,
            context=context,
            page=page,
            launch_mode=launch_mode,
        )
    except Exception:
        try:
            await _safe_close_browser_targets(context, browser)
        except Exception:
            pass
        if p is not None:
            await p.stop()
        raise
