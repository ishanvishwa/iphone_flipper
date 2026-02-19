from __future__ import annotations

import unittest

from server.services.worker.signal_detector import analyze_cycle_signals


class SignalDetectorTests(unittest.TestCase):
    def test_continue_when_no_signals(self) -> None:
        report = analyze_cycle_signals(
            result_count=10,
            expected_result_count=10.0,
            redirect_count=0,
            page_load_ms=1000,
            avg_page_load_ms=1000.0,
            page_text_sample="normal marketplace page",
            dom_has_captcha=False,
        )
        self.assertEqual(report.recommended_action, "continue")
        self.assertEqual(report.risk_score, 0.0)
        self.assertEqual(len(report.signals), 0)

    def test_throttle_for_low_results(self) -> None:
        report = analyze_cycle_signals(
            result_count=4,
            expected_result_count=20.0,
            redirect_count=0,
            page_load_ms=1200,
            avg_page_load_ms=1000.0,
            page_text_sample="normal page",
            dom_has_captcha=False,
        )
        self.assertEqual(report.recommended_action, "throttle")
        self.assertGreaterEqual(report.risk_score, 0.25)

    def test_pause_for_combined_high_signals(self) -> None:
        report = analyze_cycle_signals(
            result_count=0,
            expected_result_count=18.0,
            redirect_count=0,
            page_load_ms=1000,
            avg_page_load_ms=1000.0,
            page_text_sample="normal page",
            dom_has_captcha=False,
        )
        self.assertEqual(report.recommended_action, "pause")
        self.assertGreaterEqual(report.risk_score, 0.5)

    def test_quarantine_on_captcha(self) -> None:
        report = analyze_cycle_signals(
            result_count=5,
            expected_result_count=10.0,
            redirect_count=0,
            page_load_ms=900,
            avg_page_load_ms=1000.0,
            page_text_sample="normal page",
            dom_has_captcha=True,
        )
        self.assertEqual(report.recommended_action, "quarantine")
        self.assertGreaterEqual(report.risk_score, 0.8)

    def test_rate_limit_phrase_detected_case_insensitive(self) -> None:
        report = analyze_cycle_signals(
            result_count=8,
            expected_result_count=10.0,
            redirect_count=0,
            page_load_ms=1000,
            avg_page_load_ms=1000.0,
            page_text_sample="Please TRY AGAIN LATER due to activity.",
            dom_has_captcha=False,
        )
        signal_types = {signal.signal_type for signal in report.signals}
        self.assertIn("RATE_LIMIT_TEXT", signal_types)


if __name__ == "__main__":
    unittest.main()
