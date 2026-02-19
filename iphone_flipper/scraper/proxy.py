"""
Scraper proxy management: SOCKS5 auth bridge, Playwright proxy building,
and bridge process cleanup.
"""

import os
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from scraper.config import (
    FORCE_LOCAL_PROXY_BRIDGE,
    PACKAGE_ROOT,
    PROXY_BRIDGE_CONNECT_TIMEOUT_SECONDS,
    PROXY_BRIDGE_IDLE_TIMEOUT_SECONDS,
    PROXY_BRIDGE_MAX_UPSTREAM_CONNECTIONS,
    PROXY_BRIDGE_QUEUE_TIMEOUT_SECONDS,
    _parse_env_int,
)
from scraper.storage import (
    activate_account,
    reserve_next_scraper_account_for_run,
    set_scraper_setting,
)


# ---------------------------------------------------------------------------
# Embedded SOCKS5 auth-bridge script
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Bridge process helpers
# ---------------------------------------------------------------------------


def _find_free_local_port() -> int:
    """Bind to port 0 and return the OS-assigned ephemeral port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_local_socks5_auth_bridge(account_ctx: Dict[str, Any]) -> Tuple[Any, str]:
    """Spawn a local SOCKS5 auth bridge subprocess for *account_ctx*."""
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
        cwd=PACKAGE_ROOT,
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


def build_playwright_proxy_for_account(
    account_ctx: Dict[str, Any],
) -> Tuple[Optional[Dict[str, str]], Optional[Any]]:
    """
    Build Playwright proxy config for account context.

    Returns ``(proxy_config_or_none, bridge_process_or_none)``.
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


def cleanup_bridge_process(bridge_process: Any) -> None:
    """Terminate and clean up a bridge subprocess."""
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

    Returns ``(context, error)``. Context keys:
      - account_ctx
      - account_label
      - user_data_dir
      - proxy
      - bridge_process
    """
    account_ctx, account_error = reserve_next_scraper_account_for_run()
    if account_error:
        return None, account_error

    account_label = (
        account_ctx.get("account_name")
        or account_ctx.get("email")
        or f"Account {account_ctx['account_id']}"
    )

    profile_path = (account_ctx.get("profile_path") or "").strip()
    if profile_path:
        user_data_dir = profile_path
    else:
        user_data_dir = str(
            PACKAGE_ROOT / "browser_profiles" / f"fb_account_{account_ctx['account_id']}"
        )

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
