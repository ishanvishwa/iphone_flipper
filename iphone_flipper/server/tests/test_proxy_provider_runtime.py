"""Tests for runtime.py — proxy provider outcome and error category additions."""
from __future__ import annotations

import unittest

from server.services.worker.runtime import (
    CycleOutcome,
    ErrorCategory,
    counts_toward_bad_cycles,
    next_bad_cycle_count,
)


class ProxyProviderRuntimeTests(unittest.TestCase):
    """Verify that WAIT_PROXY_PROVIDER / PROXY_PROVIDER_SATURATED
    behave correctly in the bad-cycle tracking logic."""

    def test_wait_proxy_provider_does_not_increment_bad_cycles(self) -> None:
        bad_cycles = 0
        for _ in range(10):
            bad_cycles = next_bad_cycle_count(
                current_bad_cycles=bad_cycles,
                outcome=CycleOutcome.WAIT_PROXY_PROVIDER,
                category=ErrorCategory.PROXY_PROVIDER_SATURATED,
            )
        self.assertEqual(bad_cycles, 0)

    def test_proxy_provider_saturated_does_not_count_toward_bad_cycles(self) -> None:
        self.assertFalse(
            counts_toward_bad_cycles(
                outcome=CycleOutcome.FAIL,
                category=ErrorCategory.PROXY_PROVIDER_SATURATED,
            )
        )

    def test_fail_with_unknown_category_still_counts(self) -> None:
        self.assertTrue(
            counts_toward_bad_cycles(
                outcome=CycleOutcome.FAIL,
                category=ErrorCategory.UNKNOWN,
            )
        )


if __name__ == "__main__":
    unittest.main()
