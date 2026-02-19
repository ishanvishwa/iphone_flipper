"""
Scraper browser management: stealth script injection, Playwright persistent
context launching, and human-like random delays.
"""

import asyncio
import logging
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


async def launch_browser_context(
    headless: bool,
    user_data_dir: Optional[str] = None,
    proxy: Optional[Dict[str, str]] = None,
    browser_context_options: Optional[Dict[str, Any]] = None,
    extra_http_headers: Optional[Dict[str, str]] = None,
    browser_max_connections_per_proxy: Optional[int] = None,
    browser_max_connections_per_host: Optional[int] = None,
    blocked_resource_types: Optional[List[str]] = None,
):
    """Launch Playwright persistent context with stealth and configuration."""

    # Try to import playwright-stealth
    stealth_apply_async = None
    try:
        import playwright_stealth as playwright_stealth_module
        if hasattr(playwright_stealth_module, "stealth_async"):
            stealth_apply_async = playwright_stealth_module.stealth_async
        elif hasattr(playwright_stealth_module, "Stealth"):
            stealth_apply_async = playwright_stealth_module.Stealth().apply_stealth_async
    except ImportError:
        logger.debug("playwright-stealth not installed; using built-in stealth scripts")
    except Exception:
        logger.debug("Failed to load playwright-stealth; falling back to built-in", exc_info=True)

    has_stealth = stealth_apply_async is not None

    safe_max_connections_per_proxy = _parse_env_int(
        str(browser_max_connections_per_proxy) if browser_max_connections_per_proxy is not None else None,
        default=BROWSER_MAX_CONNECTIONS_PER_PROXY,
        minimum=1,
    )
    safe_max_connections_per_host = _parse_env_int(
        str(browser_max_connections_per_host) if browser_max_connections_per_host is not None else None,
        default=BROWSER_MAX_CONNECTIONS_PER_HOST,
        minimum=1,
    )

    if blocked_resource_types is None:
        effective_blocked_resource_types = set(BROWSER_BLOCK_RESOURCE_TYPES)
    else:
        effective_blocked_resource_types = {
            str(token).strip().lower()
            for token in blocked_resource_types
            if str(token).strip()
        }

    p = await async_playwright().start()

    launch_kwargs: Dict[str, Any] = {
        "user_data_dir": str(user_data_dir or USER_DATA_DIR),
        "headless": headless,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-infobars",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-background-networking",
            f"--max-connections-per-proxy={safe_max_connections_per_proxy}",
            f"--max-connections-per-host={safe_max_connections_per_host}",
            "--window-size=1920,1080",
        ],
        "viewport": {"width": 1920, "height": 1080},
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "locale": "en-AU",
        "timezone_id": "Australia/Perth",
        "geolocation": {"latitude": -31.9505, "longitude": 115.8605},
        "permissions": ["geolocation"],
        "color_scheme": "light",
    }
    if proxy:
        launch_kwargs["proxy"] = proxy
    if isinstance(browser_context_options, dict):
        for key in (
            "viewport",
            "screen",
            "device_scale_factor",
            "user_agent",
            "timezone_id",
            "locale",
            "color_scheme",
        ):
            value = browser_context_options.get(key)
            if value is not None:
                launch_kwargs[key] = value
    if isinstance(extra_http_headers, dict):
        headers = {
            str(key).strip(): str(value).strip()
            for key, value in extra_http_headers.items()
            if str(key).strip() and str(value).strip()
        }
        if headers:
            launch_kwargs["extra_http_headers"] = headers

    context = await p.chromium.launch_persistent_context(**launch_kwargs)

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

    # Apply stealth
    if has_stealth:
        try:
            await stealth_apply_async(page)
        except Exception:
            await apply_stealth_scripts(page)
    else:
        await apply_stealth_scripts(page)

    return p, context, page
