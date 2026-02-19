"""Tests for apply_stealth_scripts fingerprint randomisation."""

from __future__ import annotations

import asyncio
import re
import sys
import os
from unittest.mock import AsyncMock, MagicMock

# Ensure project root is on sys.path so ``scraper`` can be imported.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from scraper import apply_stealth_scripts


def _extract_init_script(calls: list) -> str:
    """Join all add_init_script call args into a single string."""
    parts: list[str] = []
    for call in calls:
        args, kwargs = call
        if args:
            parts.append(str(args[0]))
        elif "script" in kwargs:
            parts.append(str(kwargs["script"]))
    return "\n".join(parts)


class TestApplyStealthScripts:
    """Verify that stealth scripts contain randomized and required values."""

    def _run(self):
        page = MagicMock()
        page.add_init_script = AsyncMock()
        asyncio.run(apply_stealth_scripts(page))
        return page

    def test_webdriver_override_present(self):
        page = self._run()
        script = _extract_init_script(page.add_init_script.call_args_list)
        assert "navigator" in script
        assert "webdriver" in script

    def test_hardware_concurrency_randomized(self):
        """Repeated invocations should produce at least 2 distinct values."""
        values = set()
        for _ in range(30):
            page = self._run()
            script = _extract_init_script(page.add_init_script.call_args_list)
            match = re.search(r"hardwareConcurrency.*?get:\s*\(\)\s*=>\s*(\d+)", script, re.DOTALL)
            assert match, "hardwareConcurrency override not found"
            values.add(int(match.group(1)))
        assert len(values) >= 2, f"Expected randomized values, got: {values}"

    def test_device_memory_randomized(self):
        values = set()
        for _ in range(30):
            page = self._run()
            script = _extract_init_script(page.add_init_script.call_args_list)
            match = re.search(r"deviceMemory.*?get:\s*\(\)\s*=>\s*(\d+)", script, re.DOTALL)
            assert match, "deviceMemory override not found"
            values.add(int(match.group(1)))
        assert len(values) >= 2, f"Expected randomized values, got: {values}"

    def test_platform_randomized(self):
        values = set()
        for _ in range(30):
            page = self._run()
            script = _extract_init_script(page.add_init_script.call_args_list)
            match = re.search(r"platform.*?get:\s*\(\)\s*=>\s*'([^']+)'", script, re.DOTALL)
            assert match, "platform override not found"
            values.add(match.group(1))
        assert len(values) >= 2, f"Expected randomized platforms, got: {values}"

    def test_webgl_renderer_present(self):
        page = self._run()
        script = _extract_init_script(page.add_init_script.call_args_list)
        assert "UNMASKED_RENDERER_WEBGL" in script or "0x9246" in script

    def test_chrome_property_present(self):
        page = self._run()
        script = _extract_init_script(page.add_init_script.call_args_list)
        assert "window.chrome" in script

    def test_plugins_override_present(self):
        page = self._run()
        script = _extract_init_script(page.add_init_script.call_args_list)
        assert "Chrome PDF Plugin" in script
