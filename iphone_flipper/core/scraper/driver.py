import asyncio
import random
import re
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from playwright.async_api import async_playwright

from .config import BROWSER_BLOCK_RESOURCE_TYPES
from .config import BROWSER_MAX_CONNECTIONS_PER_HOST
from .config import BROWSER_MAX_CONNECTIONS_PER_PROXY

# --- Dolphin Anty Integration ---
import aiohttp

DOLPHIN_API_URL = "http://localhost:3001/v1.0"

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
                pass  # Optional: log warning if desired


async def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """Add a random delay to mimic human behavior."""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)


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
