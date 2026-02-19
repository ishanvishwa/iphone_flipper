from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SoftSignal:
    signal_type: str
    severity: str
    detail: str
    value: Optional[float] = None
    threshold: Optional[float] = None


@dataclass(frozen=True)
class SignalReport:
    signals: list[SoftSignal]
    risk_score: float
    recommended_action: str


def _compute_risk_score(signals: list[SoftSignal]) -> float:
    weights = {
        "low": 0.10,
        "medium": 0.25,
        "high": 0.40,
        "critical": 0.80,
    }
    score = sum(weights.get(signal.severity, 0.0) for signal in signals)
    return min(1.0, max(0.0, score))


def _score_to_action(risk_score: float) -> str:
    score = max(0.0, min(1.0, float(risk_score or 0.0)))
    if score >= 0.80:
        return "quarantine"
    if score >= 0.50:
        return "pause"
    if score >= 0.25:
        return "throttle"
    return "continue"


def analyze_cycle_signals(
    result_count: int,
    expected_result_count: float,
    redirect_count: int,
    page_load_ms: int,
    avg_page_load_ms: float,
    page_text_sample: str,
    dom_has_captcha: bool,
    result_ratio_threshold: float = 0.5,
    load_time_multiplier: float = 2.0,
) -> SignalReport:
    signals: list[SoftSignal] = []

    current_result = max(0, int(result_count or 0))
    expected_result = max(0.0, float(expected_result_count or 0.0))
    redirects = max(0, int(redirect_count or 0))
    current_load_ms = max(0, int(page_load_ms or 0))
    average_load_ms = max(0.0, float(avg_page_load_ms or 0.0))
    ratio_threshold = max(0.0, min(1.0, float(result_ratio_threshold or 0.5)))
    load_multiplier = max(1.0, float(load_time_multiplier or 2.0))
    text_sample = str(page_text_sample or "").lower()

    if expected_result > 0 and current_result < expected_result * ratio_threshold:
        signals.append(
            SoftSignal(
                signal_type="LOW_RESULTS",
                severity="medium",
                detail="Current result count is below the expected ratio.",
                value=float(current_result),
                threshold=expected_result * ratio_threshold,
            )
        )

    if current_result == 0 and expected_result >= 5:
        signals.append(
            SoftSignal(
                signal_type="EMPTY_RESULTS",
                severity="high",
                detail="Current cycle returned zero results despite non-trivial historical baseline.",
                value=0.0,
                threshold=1.0,
            )
        )

    if redirects > 2:
        signals.append(
            SoftSignal(
                signal_type="REDIRECT_CHAIN",
                severity="high",
                detail="Navigation redirect count exceeded the healthy threshold.",
                value=float(redirects),
                threshold=2.0,
            )
        )

    if average_load_ms > 0 and current_load_ms > average_load_ms * load_multiplier:
        signals.append(
            SoftSignal(
                signal_type="SLOW_LOAD",
                severity="low",
                detail="Cycle load time exceeded expected multiplier.",
                value=float(current_load_ms),
                threshold=average_load_ms * load_multiplier,
            )
        )

    rate_limit_phrases = (
        "try again later",
        "you're going too fast",
        "temporarily blocked",
        "rate limit",
    )
    if any(phrase in text_sample for phrase in rate_limit_phrases):
        signals.append(
            SoftSignal(
                signal_type="RATE_LIMIT_TEXT",
                severity="high",
                detail="Rate-limit phrase detected in response text sample.",
            )
        )

    if dom_has_captcha:
        signals.append(
            SoftSignal(
                signal_type="CAPTCHA_DETECTED",
                severity="critical",
                detail="CAPTCHA/challenge element detected.",
            )
        )

    risk_score = _compute_risk_score(signals)
    return SignalReport(
        signals=signals,
        risk_score=risk_score,
        recommended_action=_score_to_action(risk_score),
    )
