import asyncio
import contextlib
import os
import random
import re
import socket
import struct
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

from playwright.async_api import async_playwright

from .config import BROWSER_BLOCK_RESOURCE_TYPES
from .config import BROWSER_MAX_CONNECTIONS_PER_HOST
from .config import BROWSER_MAX_CONNECTIONS_PER_PROXY
from .config import FORCE_LOCAL_PROXY_BRIDGE
from .config import MANUAL_LOGIN_REQUIRED_PREFIX
from .config import PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS
from .config import PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS
from .config import PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS
from .config import PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS
from .config import USER_DATA_DIR
from .config import _parse_env_int
from .storage import _get_active_scraper_account_context
from .storage import activate_account
from .storage import reserve_next_scraper_account_for_run

# --- Local SOCKS5 Bridge Logic ---

LOCAL_SOCKS5_BRIDGE_SCRIPT = r"""
import argparse
import asyncio
import contextlib
import os
import socket
import struct
from typing import Optional


async def _read_exact(reader: asyncio.StreamReader, size: int) -> bytes:
    data = await reader.readexactly(size)
    if len(data) != size:
        raise asyncio.IncompleteReadError(data, size)
    return data


def _encode_socks5_addr_port(host: str, port: int) -> bytes:
    try:
        packed = socket.inet_aton(host)
        return b"\x01" + packed + struct.pack("!H", int(port))
    except OSError:
        pass
    host_bytes = host.encode("utf-8", errors="ignore")
    host_bytes = host_bytes[:255]
    return b"\x03" + bytes([len(host_bytes)]) + host_bytes + struct.pack("!H", int(port))


async def _drain_or_raise(writer: asyncio.StreamWriter) -> None:
    await writer.drain()


async def _socks5_connect_upstream(
    upstream_host: str,
    upstream_port: int,
    upstream_username: str,
    upstream_password: str,
    connect_payload: bytes,
    connect_timeout_seconds: float,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, bytes]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(upstream_host, upstream_port),
        timeout=connect_timeout_seconds,
    )
    methods = [0x00]
    if upstream_username or upstream_password:
        methods.append(0x02)
    writer.write(bytes([0x05, len(methods), *methods]))
    await _drain_or_raise(writer)

    server_choice = await _read_exact(reader, 2)
    if server_choice[0] != 0x05:
        raise RuntimeError("Upstream proxy returned invalid SOCKS version.")
    selected_method = int(server_choice[1])
    if selected_method == 0xFF:
        raise RuntimeError("Upstream proxy rejected auth methods.")

    if selected_method == 0x02:
        username_bytes = upstream_username.encode("utf-8", errors="ignore")[:255]
        password_bytes = upstream_password.encode("utf-8", errors="ignore")[:255]
        writer.write(
            bytes([0x01, len(username_bytes)])
            + username_bytes
            + bytes([len(password_bytes)])
            + password_bytes
        )
        await _drain_or_raise(writer)
        auth_reply = await _read_exact(reader, 2)
        if auth_reply[1] != 0x00:
            raise RuntimeError("Upstream proxy authentication failed.")
    elif selected_method != 0x00:
        raise RuntimeError("Upstream proxy selected unsupported auth method.")

    writer.write(connect_payload)
    await _drain_or_raise(writer)

    reply_head = await _read_exact(reader, 4)
    if reply_head[0] != 0x05:
        raise RuntimeError("Upstream connect reply version mismatch.")
    reply_code = int(reply_head[1])
    atyp = int(reply_head[3])
    if atyp == 0x01:
        addr_tail = await _read_exact(reader, 4 + 2)
    elif atyp == 0x03:
        name_len = await _read_exact(reader, 1)
        addr_tail = name_len + await _read_exact(reader, int(name_len[0]) + 2)
    elif atyp == 0x04:
        addr_tail = await _read_exact(reader, 16 + 2)
    else:
        raise RuntimeError("Upstream connect reply had unsupported address type.")
    full_reply = reply_head + addr_tail
    if reply_code != 0x00:
        raise RuntimeError(f"Upstream connect failed with SOCKS code={reply_code}")
    return reader, writer, full_reply


async def _pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    idle_timeout_seconds: float,
) -> None:
    while True:
        try:
            chunk = await asyncio.wait_for(reader.read(65536), timeout=idle_timeout_seconds)
        except asyncio.TimeoutError:
            # Release bridge slot if connection is idle for too long.
            break
        if not chunk:
            break
        writer.write(chunk)
        await writer.drain()


async def _handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    upstream_host: str,
    upstream_port: int,
    upstream_username: str,
    upstream_password: str,
    semaphore: asyncio.Semaphore,
    queue_timeout_seconds: float,
    connect_timeout_seconds: float,
    idle_timeout_seconds: float,
) -> None:
    upstream_reader: Optional[asyncio.StreamReader] = None
    upstream_writer: Optional[asyncio.StreamWriter] = None
    acquired = False
    try:
        hello = await _read_exact(client_reader, 2)
        if hello[0] != 0x05:
            return
        method_count = int(hello[1])
        client_methods = await _read_exact(client_reader, method_count)
        if 0x00 not in client_methods:
            client_writer.write(b"\x05\xFF")
            await client_writer.drain()
            return
        client_writer.write(b"\x05\x00")
        await client_writer.drain()

        request_head = await _read_exact(client_reader, 4)
        if request_head[0] != 0x05:
            return
        cmd = int(request_head[1])
        atyp = int(request_head[3])
        if cmd != 0x01:
            client_writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_writer.drain()
            return
        if atyp == 0x01:
            addr_port_payload = await _read_exact(client_reader, 4 + 2)
        elif atyp == 0x03:
            name_len = await _read_exact(client_reader, 1)
            addr_port_payload = name_len + await _read_exact(client_reader, int(name_len[0]) + 2)
        elif atyp == 0x04:
            addr_port_payload = await _read_exact(client_reader, 16 + 2)
        else:
            client_writer.write(b"\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_writer.drain()
            return
        connect_payload = request_head + addr_port_payload

        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=queue_timeout_seconds)
            acquired = True
        except asyncio.TimeoutError:
            client_writer.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_writer.drain()
            return

        upstream_reader, upstream_writer, upstream_reply = await _socks5_connect_upstream(
            upstream_host=upstream_host,
            upstream_port=upstream_port,
            upstream_username=upstream_username,
            upstream_password=upstream_password,
            connect_payload=connect_payload,
            connect_timeout_seconds=connect_timeout_seconds,
        )

        client_writer.write(upstream_reply)
        await client_writer.drain()

        to_upstream = asyncio.create_task(
            _pipe(client_reader, upstream_writer, idle_timeout_seconds=idle_timeout_seconds)
        )
        to_client = asyncio.create_task(
            _pipe(upstream_reader, client_writer, idle_timeout_seconds=idle_timeout_seconds)
        )
        done, pending = await asyncio.wait(
            {to_upstream, to_client},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in done:
            with contextlib.suppress(Exception):
                task.result()
    except asyncio.IncompleteReadError:
        pass
    except Exception:
        with contextlib.suppress(Exception):
            client_writer.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_writer.drain()
    finally:
        if acquired:
            semaphore.release()
        if upstream_writer is not None:
            upstream_writer.close()
            with contextlib.suppress(Exception):
                await upstream_writer.wait_closed()
        client_writer.close()
        with contextlib.suppress(Exception):
            await client_writer.wait_closed()


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-port", type=int, required=True)
    parser.add_argument("--max-upstream-connections", type=int, default=1)
    parser.add_argument("--queue-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--idle-timeout-seconds", type=float, default=8.0)
    args = parser.parse_args()

    upstream_host = os.getenv("BRIDGE_UPSTREAM_HOST", "").strip()
    upstream_port = int(os.getenv("BRIDGE_UPSTREAM_PORT", "0") or 0)
    upstream_username = os.getenv("BRIDGE_UPSTREAM_USERNAME", "")
    upstream_password = os.getenv("BRIDGE_UPSTREAM_PASSWORD", "")
    if not upstream_host or upstream_port <= 0:
        raise SystemExit("Missing BRIDGE_UPSTREAM_HOST/BRIDGE_UPSTREAM_PORT")

    semaphore = asyncio.Semaphore(max(1, int(args.max_upstream_connections)))
    server = await asyncio.start_server(
        lambda reader, writer: _handle_client(
            reader,
            writer,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
            upstream_username=upstream_username,
            upstream_password=upstream_password,
            semaphore=semaphore,
            queue_timeout_seconds=max(1.0, float(args.queue_timeout_seconds)),
            connect_timeout_seconds=max(1.0, float(args.connect_timeout_seconds)),
            idle_timeout_seconds=max(2.0, float(args.idle_timeout_seconds)),
        ),
        host="127.0.0.1",
        port=int(args.local_port),
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(_main())
"""


def _find_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_local_socks5_auth_bridge(account_ctx: Dict[str, Any]) -> Tuple[Any, str]:
    proxy_host = str(account_ctx.get("host") or "").strip()
    proxy_port = int(account_ctx.get("port") or 0)
    if not proxy_host or proxy_port <= 0:
        raise RuntimeError("SOCKS5 bridge requires upstream host and port.")
    max_upstream_connections = _parse_env_int(
        str(account_ctx.get("max_upstream_connections") or ""),
        default=PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS,
        minimum=1,
    )
    queue_timeout_seconds = _parse_env_int(
        str(account_ctx.get("queue_timeout_seconds") or ""),
        default=PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS,
        minimum=1,
    )
    connect_timeout_seconds = _parse_env_int(
        str(account_ctx.get("connect_timeout_seconds") or ""),
        default=PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS,
        minimum=1,
    )
    idle_timeout_seconds = _parse_env_int(
        str(account_ctx.get("idle_timeout_seconds") or ""),
        default=PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS,
        minimum=2,
    )
    local_port = _find_free_local_port()
    local_proxy = f"socks5://127.0.0.1:{local_port}"

    cmd = [
        sys.executable,
        "-u",
        "-c",
        LOCAL_SOCKS5_BRIDGE_SCRIPT,
        "--local-port",
        str(local_port),
        "--max-upstream-connections",
        str(max_upstream_connections),
        "--queue-timeout-seconds",
        str(queue_timeout_seconds),
        "--connect-timeout-seconds",
        str(connect_timeout_seconds),
        "--idle-timeout-seconds",
        str(idle_timeout_seconds),
    ]

    bridge_env = os.environ.copy()
    bridge_env["BRIDGE_UPSTREAM_HOST"] = proxy_host
    bridge_env["BRIDGE_UPSTREAM_PORT"] = str(proxy_port)
    bridge_env["BRIDGE_UPSTREAM_USERNAME"] = str(account_ctx.get("username") or "")
    bridge_env["BRIDGE_UPSTREAM_PASSWORD"] = str(account_ctx.get("password") or "")

    process = subprocess.Popen(
        cmd,
        cwd=Path(__file__).parent,
        env=bridge_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    start = datetime.now()
    while (datetime.now() - start).total_seconds() < 8:
        if process.poll() is not None:
            stderr_output = ""
            try:
                if process.stderr:
                    stderr_output = process.stderr.read().decode("utf-8", errors="ignore")[:500]
            except Exception:
                pass
            if stderr_output:
                raise RuntimeError(f"Failed to start local SOCKS5 bridge process: {stderr_output}")
            raise RuntimeError("Failed to start local SOCKS5 bridge process.")
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=0.5):
                return process, local_proxy
        except OSError:
            pass
        time.sleep(0.2)

    try:
        process.terminate()
    except Exception:
        pass
    raise RuntimeError("Timed out waiting for local SOCKS5 bridge to become ready.")


def build_playwright_proxy_for_account(account_ctx: Dict[str, Any]) -> Tuple[Optional[Dict[str, str]], Optional[Any]]:
    """
    Build Playwright proxy config for account context.
    Returns (proxy_config_or_none, bridge_process_or_none).
    """
    if not account_ctx or not account_ctx.get("proxy_id"):
        return None, None

    proxy_type = (account_ctx.get("proxy_type") or "").lower()
    host = account_ctx.get("host")
    port = account_ctx.get("port")
    if proxy_type != "socks5" or not host or not port:
        return None, None

    if FORCE_LOCAL_PROXY_BRIDGE or account_ctx.get("username") or account_ctx.get("password"):
        bridge_process, bridge_proxy = _start_local_socks5_auth_bridge(account_ctx)
        return {"server": bridge_proxy}, bridge_process

    return {"server": f"{proxy_type}://{host}:{port}"}, None


def cleanup_bridge_process(bridge_process: Any):
    if bridge_process and bridge_process.poll() is None:
        try:
            bridge_process.terminate()
            bridge_process.wait(timeout=3)
        except Exception:
            try:
                bridge_process.kill()
            except Exception:
                pass


def reserve_and_build_scraper_runtime_context() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Reserve next rotating scraper account and build runtime context.
    Returns (context, error). Context keys:
      - account_ctx
      - account_label
      - user_data_dir
      - proxy
      - bridge_process
    """
    account_ctx, account_error = reserve_next_scraper_account_for_run()
    if account_error:
        return None, account_error

    account_label = account_ctx.get("account_name") or account_ctx.get("email") or f"Account {account_ctx['account_id']}"

    profile_path = (account_ctx.get("profile_path") or "").strip()
    if profile_path:
        user_data_dir = profile_path
    else:
        # Resolve path relative to core/scraper's parent package root if needed, 
        # but config.USER_DATA_DIR handles main profile. 
        # For account-specific, we used to use scraper.py parent. 
        # Now we preserve similar logic using Path(__file__).parent.parent.parent
        # i.e. iphone_flipper/browser_profiles
        user_data_dir = str(Path(__file__).parent.parent.parent / "browser_profiles" / f"fb_account_{account_ctx['account_id']}")

    bridge_process = None
    try:
        proxy_config, bridge_process = build_playwright_proxy_for_account(account_ctx)
    except Exception as exc:
        return None, f"Failed to initialize proxy for {account_label}: {exc}"

    if not proxy_config:
        cleanup_bridge_process(bridge_process)
        return None, (
            f"{account_label} has no usable SOCKS5 proxy. "
            "Assign an ACTIVE SOCKS5 proxy and try again."
        )

    activate_account(account_ctx["account_id"])

    return {
        "account_ctx": account_ctx,
        "account_label": account_label,
        "user_data_dir": user_data_dir,
        "proxy": proxy_config,
        "bridge_process": bridge_process,
    }, None


async def apply_stealth_scripts(page):
    """Apply stealth JavaScript to hide automation signals."""
    # Hide webdriver property
    await page.add_init_script("""
        // Override the navigator.webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });

        // Override the chrome property
        window.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {},
        };

        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // Override plugins to look more realistic
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                { name: 'Native Client', filename: 'internal-nacl-plugin' },
            ],
        });

        // Override languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-AU', 'en-US', 'en'],
        });

        // Remove automation-related properties
        delete navigator.__proto__.webdriver;

        // Override the platform
        Object.defineProperty(navigator, 'platform', {
            get: () => 'MacIntel',
        });

        // Override hardware concurrency
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 8,
        });

        // Override device memory
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => 8,
        });
    """)


async def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """Add a random delay to mimic human behavior."""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)


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
    except Exception:
        stealth_apply_async = None

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
