"""Tests for signal_detector with new consecutive-empty and session-duration signals."""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from server.services.worker.signal_detector import analyze_cycle_signals


class TestConsecutiveEmptyResults:
    """CONSECUTIVE_EMPTY_RESULTS signal fires at threshold ≥ 3."""

    _BASE = dict(
        result_count=5,
        expected_result_count=10,
        redirect_count=0,
        page_load_ms=200,
        avg_page_load_ms=200,
        page_text_sample="",
        dom_has_captcha=False,
    )

    def test_below_threshold_no_signal(self):
        report = analyze_cycle_signals(**self._BASE, consecutive_empty_results=2)
        types = {s.signal_type for s in report.signals}
        assert "CONSECUTIVE_EMPTY_RESULTS" not in types

    def test_at_threshold_emits_signal(self):
        report = analyze_cycle_signals(**self._BASE, consecutive_empty_results=3)
        types = {s.signal_type for s in report.signals}
        assert "CONSECUTIVE_EMPTY_RESULTS" in types

    def test_above_threshold_emits_signal(self):
        report = analyze_cycle_signals(**self._BASE, consecutive_empty_results=7)
        signal = next(s for s in report.signals if s.signal_type == "CONSECUTIVE_EMPTY_RESULTS")
        assert signal.value == 7.0
        assert signal.severity == "high"

    def test_none_is_ignored(self):
        report = analyze_cycle_signals(**self._BASE, consecutive_empty_results=None)
        types = {s.signal_type for s in report.signals}
        assert "CONSECUTIVE_EMPTY_RESULTS" not in types


class TestSessionTooLong:
    """SESSION_TOO_LONG signal fires when duration exceeds max."""

    _BASE = dict(
        result_count=5,
        expected_result_count=10,
        redirect_count=0,
        page_load_ms=200,
        avg_page_load_ms=200,
        page_text_sample="",
        dom_has_captcha=False,
    )

    def test_within_limit_no_signal(self):
        report = analyze_cycle_signals(
            **self._BASE, session_duration_seconds=1200, session_max_seconds=1800
        )
        types = {s.signal_type for s in report.signals}
        assert "SESSION_TOO_LONG" not in types

    def test_exceeds_limit_emits_signal(self):
        report = analyze_cycle_signals(
            **self._BASE, session_duration_seconds=2000, session_max_seconds=1800
        )
        signal = next(s for s in report.signals if s.signal_type == "SESSION_TOO_LONG")
        assert signal.value == 2000
        assert signal.threshold == 1800
        assert signal.severity == "medium"

    def test_none_duration_is_ignored(self):
        report = analyze_cycle_signals(
            **self._BASE, session_duration_seconds=None, session_max_seconds=1800
        )
        types = {s.signal_type for s in report.signals}
        assert "SESSION_TOO_LONG" not in types

    def test_zero_max_disables_signal(self):
        report = analyze_cycle_signals(
            **self._BASE, session_duration_seconds=9999, session_max_seconds=0
        )
        types = {s.signal_type for s in report.signals}
        assert "SESSION_TOO_LONG" not in types
