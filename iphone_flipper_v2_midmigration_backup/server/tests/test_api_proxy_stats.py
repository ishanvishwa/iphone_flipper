from __future__ import annotations

import unittest

try:
    from fastapi import HTTPException
    from server.services.api.app.main import _canonical_proxy_key, _resolve_proxy_key
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    HTTPException = Exception  # type: ignore[assignment]
    _canonical_proxy_key = None
    _resolve_proxy_key = None


@unittest.skipIf(_canonical_proxy_key is None or _resolve_proxy_key is None, "FastAPI dependencies are not installed.")
class ApiProxyStatsTests(unittest.TestCase):
    def test_canonical_proxy_key_uses_scheme_host_port_and_username(self) -> None:
        key = _canonical_proxy_key("socks5://user:pass@1.2.3.4:1080", None)
        self.assertEqual(key, "socks5:1.2.3.4:1080:user")

    def test_resolve_proxy_key_prefers_explicit_key(self) -> None:
        key = _resolve_proxy_key(
            proxy_key="socks5:9.9.9.9:1080:alice",
            proxy_server="socks5://1.1.1.1:1080",
            proxy_username="bob",
        )
        self.assertEqual(key, "socks5:9.9.9.9:1080:alice")

    def test_resolve_proxy_key_builds_from_proxy_server_when_missing_key(self) -> None:
        key = _resolve_proxy_key(
            proxy_key="",
            proxy_server="socks5://5.5.5.5:1080",
            proxy_username="tester",
        )
        self.assertEqual(key, "socks5:5.5.5.5:1080:tester")

    def test_resolve_proxy_key_raises_when_no_usable_input(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            _resolve_proxy_key(proxy_key="", proxy_server="", proxy_username="")
        self.assertEqual(ctx.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
