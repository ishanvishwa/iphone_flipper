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

async def _start_dolphin_profile(profile_id: str) -> str:
    """Start a Dolphin Anty profile and return the CDP WebSocket endpoint."""
    async with aiohttp.ClientSession() as session:
        # Start the profile
        async with session.get(f"{DOLPHIN_API_URL}/browser_profiles/{profile_id}/start?automation=1") as resp:
            data = await resp.json()
            if not data.get("success"):
                raise RuntimeError(f"Failed to start Dolphin profile {profile_id}: {data}")
            return f"ws://127.0.0.1:{data['automation']['port']}"
    raise RuntimeError(f"Failed to complete Dolphin profile start request for {profile_id}")

async def stop_dolphin_profile(profile_id: str) -> None:
    """Stop a Dolphin Anty profile."""
    async with aiohttp.ClientSession() as session:
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
    p = await async_playwright().start()

    # Connect to the Dolphin browser over CDP
    browser = await p.chromium.connect_over_cdp(ws_endpoint)
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
