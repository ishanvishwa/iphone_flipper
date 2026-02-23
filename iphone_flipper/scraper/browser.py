"""
Scraper browser management: stealth script injection, Playwright persistent
context launching, and human-like random delays.
"""

import asyncio
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


async def _start_dolphin_profile(profile_id: str) -> str:
    """Start a Dolphin Anty profile and return the CDP WebSocket endpoint."""
    headers = _dolphin_auth_headers()
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/start?automation=1") as resp:
            data = await resp.json()

        # Handle "already running" — reuse active session instead of killing it
        if not data.get("success"):
            error_obj = data.get("errorObject") or {}
            if error_obj.get("code") == "E_BROWSER_RUN_DUPLICATE":
                logger.info("Dolphin profile %s already running, reusing active session...", profile_id)
                # Query the active profile's automation port
                try:
                    async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/active") as active_resp:
                        active_data = await active_resp.json()
                    if active_data.get("success") and active_data.get("automation"):
                        port = active_data["automation"]["port"]
                        ws_path = active_data["automation"].get("wsEndpoint", "")
                        return f"ws://{DOLPHIN_WS_HOST}:{port}{ws_path}"
                except Exception as exc:
                    logger.warning("Failed to query active Dolphin profile %s: %s", profile_id, exc)
                # Fallback: stop and restart if we can't get the active session.
                # The browser process may take several seconds to fully terminate
                # after a /stop call, so we retry with increasing waits.
                logger.info("Dolphin profile %s falling back to stop+restart...", profile_id)
                async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/stop") as stop_resp:
                    await stop_resp.json()
                for restart_attempt in range(3):
                    wait_secs = 5 + (restart_attempt * 3)  # 5s, 8s, 11s
                    await asyncio.sleep(wait_secs)
                    async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/start?automation=1") as resp2:
                        data = await resp2.json()
                    if data.get("success"):
                        break
                    err_obj = data.get("errorObject") or {}
                    if err_obj.get("code") == "E_BROWSER_RUN_DUPLICATE" and restart_attempt < 2:
                        logger.warning(
                            "Dolphin profile %s still running after stop (attempt %d/3), retrying...",
                            profile_id, restart_attempt + 1,
                        )
                        # Re-issue stop in case the first one didn't take effect
                        try:
                            async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/stop") as stop_resp2:
                                await stop_resp2.json()
                        except Exception:
                            pass
                        continue
                    break  # Non-duplicate error or last attempt — fall through to error check

        if not data.get("success"):
            raise RuntimeError(f"Failed to start Dolphin profile {profile_id}: {data}")

        port = data["automation"]["port"]
        ws_path = data["automation"].get("wsEndpoint", "")
        return f"ws://{DOLPHIN_WS_HOST}:{port}{ws_path}"


async def stop_dolphin_profile(profile_id: str) -> None:
    """Stop a Dolphin Anty profile."""
    headers = _dolphin_auth_headers()
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/stop") as resp:
            data = await resp.json()
            if not data.get("success"):
                logger.warning(f"Warning: Failed to stop Dolphin profile {profile_id}: {data}")

async def launch_browser_context(
    headless: bool,
    profile_id: Optional[str] = None,
    blocked_resource_types: Optional[List[str]] = None,
    **kwargs
):
    """Connect to a Dolphin Anty profile over CDP."""
    if not profile_id:
        raise ValueError("A Dolphin Anty profile_id is required for V2.2")

    if blocked_resource_types is None:
        effective_blocked_resource_types = set(BROWSER_BLOCK_RESOURCE_TYPES)
    else:
        effective_blocked_resource_types = {
            str(token).strip().lower()
            for token in blocked_resource_types
            if str(token).strip()
        }

    ws_endpoint = await _start_dolphin_profile(profile_id)

    # Give the browser process a moment to bind to the debug port.
    # Dolphin's /start API can return before Chromium is fully listening.
    await asyncio.sleep(2)

    p = await async_playwright().start()

    # Retry CDP connection with exponential backoff — the Dolphin browser
    # process may not be listening on its debug port immediately after the
    # API reports it as started.
    max_cdp_retries = 5
    cdp_backoff = 2.0  # seconds, doubles each retry
    browser = None
    for attempt in range(max_cdp_retries):
        try:
            browser = await p.chromium.connect_over_cdp(ws_endpoint)
            break
        except Exception as exc:
            if attempt < max_cdp_retries - 1:
                wait = cdp_backoff * (2 ** attempt)
                logger.warning(
                    "CDP connect attempt %d/%d failed for profile %s (retrying in %.1fs): %s",
                    attempt + 1, max_cdp_retries, profile_id, wait, exc,
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    "CDP connect failed after %d attempts for profile %s: %s",
                    max_cdp_retries, profile_id, exc,
                )
                await p.stop()
                raise

    context = browser.contexts[0] if browser.contexts else await browser.new_context()
    page = context.pages[0] if context.pages else await context.new_page()

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

    return p, browser, context, page
