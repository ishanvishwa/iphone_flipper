from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from server.services.worker.v4_runtime import (
    FamilyClaimOutcome,
    classify_family_claim,
    evaluate_warm_session_state,
    family_claim_succeeded,
    next_pause_seconds,
    next_family_due_seconds,
    next_variant_cursor,
    normalize_warm_session_config,
    should_abort_warm_session,
    scrub_orphaned_chromium_locks,
    worker_rollout_enabled,
)


class _FixedRng:
    def __init__(self, value: float) -> None:
        self._value = value

    def uniform(self, low: float, high: float) -> float:
        _ = low
        _ = high
        return self._value


class V4RuntimeTests(unittest.TestCase):
    def test_next_pause_seconds_uses_rng_between_bounds(self) -> None:
        self.assertEqual(
            next_pause_seconds(min_pause_seconds=2.0, max_pause_seconds=5.0, rng=_FixedRng(3.25)),
            3.25,
        )

    def test_evaluate_warm_session_state_detects_budget_age_and_idle(self) -> None:
        config = normalize_warm_session_config(
            max_queries=60,
            max_age_seconds=1800,
            idle_close_seconds=120,
            min_pause_seconds=2.0,
            max_pause_seconds=5.0,
        )
        started_at = datetime(2026, 3, 11, tzinfo=timezone.utc)
        last_activity_at = started_at

        self.assertEqual(
            evaluate_warm_session_state(
                started_at=started_at,
                last_activity_at=last_activity_at,
                executed_claims=60,
                config=config,
                now=started_at + timedelta(seconds=10),
            ),
            "query_budget",
        )
        self.assertEqual(
            evaluate_warm_session_state(
                started_at=started_at,
                last_activity_at=last_activity_at,
                executed_claims=10,
                config=config,
                now=started_at + timedelta(seconds=1800),
            ),
            "session_age",
        )
        self.assertEqual(
            evaluate_warm_session_state(
                started_at=started_at,
                last_activity_at=last_activity_at,
                executed_claims=10,
                config=config,
                now=started_at + timedelta(seconds=121),
            ),
            "idle_close",
        )

    def test_scrub_orphaned_chromium_locks_removes_known_lock_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            expected = []
            for name in ("SingletonLock", "SingletonCookie", "SingletonSocket", "DevToolsActivePort"):
                file_path = root / name
                file_path.write_text("x", encoding="utf-8")
                expected.append(str(file_path))
            (root / "keep.me").write_text("ok", encoding="utf-8")

            removed = scrub_orphaned_chromium_locks(str(root))

            self.assertEqual(sorted(removed), sorted(expected))
            self.assertTrue((root / "keep.me").exists())
            for path in expected:
                self.assertFalse(Path(path).exists())

    def test_classify_family_claim_distinguishes_matches_empty_dom_checkpoint_and_infra(self) -> None:
        self.assertEqual(
            classify_family_claim(
                listings_saved=2,
                listings_scraped=4,
                error_text=None,
                final_url="https://www.facebook.com/marketplace/perth/search?query=iphone",
                feed_present=True,
                empty_state_detected=False,
            ),
            FamilyClaimOutcome.MATCHES,
        )
        self.assertEqual(
            classify_family_claim(
                listings_saved=0,
                listings_scraped=0,
                error_text=None,
                final_url="https://www.facebook.com/marketplace/perth/search?query=iphone",
                feed_present=True,
                empty_state_detected=True,
            ),
            FamilyClaimOutcome.EMPTY_FEED,
        )
        self.assertEqual(
            classify_family_claim(
                listings_saved=0,
                listings_scraped=0,
                error_text=None,
                final_url="https://www.facebook.com/marketplace/perth/search?query=iphone",
                feed_present=False,
                empty_state_detected=False,
            ),
            FamilyClaimOutcome.DOM_CHANGED,
        )
        self.assertEqual(
            classify_family_claim(
                listings_saved=0,
                listings_scraped=0,
                error_text="manual_login_required: checkpoint",
                final_url="https://www.facebook.com/checkpoint/",
                feed_present=False,
                empty_state_detected=False,
            ),
            FamilyClaimOutcome.CHECKPOINT,
        )
        self.assertEqual(
            classify_family_claim(
                listings_saved=0,
                listings_scraped=0,
                error_text="navigation timeout while loading search",
                final_url="https://www.facebook.com/marketplace/perth/search?query=iphone",
                feed_present=False,
                empty_state_detected=False,
            ),
            FamilyClaimOutcome.INFRASTRUCTURE_ERROR,
        )

    def test_family_reschedule_and_variant_rotation_follow_claim_outcome(self) -> None:
        family = {
            "min_gap_s": 5,
            "max_gap_s": 120,
            "variant_count": 3,
            "variant_cursor": 1,
        }
        self.assertEqual(
            next_family_due_seconds(
                family=family,
                outcome=FamilyClaimOutcome.MATCHES,
                listings_saved=3,
                consecutive_hits=4,
                consecutive_empty=0,
            ),
            5,
        )
        self.assertEqual(
            next_family_due_seconds(
                family=family,
                outcome=FamilyClaimOutcome.EMPTY_FEED,
                listings_saved=0,
                consecutive_hits=0,
                consecutive_empty=3,
            ),
            20,
        )
        self.assertEqual(
            next_family_due_seconds(
                family=family,
                outcome=FamilyClaimOutcome.DOM_CHANGED,
                listings_saved=0,
                consecutive_hits=0,
                consecutive_empty=0,
                dom_backoff_seconds=300,
            ),
            120,
        )
        self.assertEqual(
            next_variant_cursor(
                variant_count=3,
                current_cursor=1,
                outcome=FamilyClaimOutcome.MATCHES,
            ),
            2,
        )
        self.assertEqual(
            next_variant_cursor(
                variant_count=3,
                current_cursor=1,
                outcome=FamilyClaimOutcome.DOM_CHANGED,
            ),
            1,
        )

    def test_family_success_and_abort_flags_match_blueprint_intent(self) -> None:
        self.assertTrue(family_claim_succeeded(FamilyClaimOutcome.MATCHES))
        self.assertTrue(family_claim_succeeded(FamilyClaimOutcome.EMPTY_FEED))
        self.assertFalse(family_claim_succeeded(FamilyClaimOutcome.DOM_CHANGED))
        self.assertTrue(should_abort_warm_session(FamilyClaimOutcome.DOM_CHANGED))
        self.assertTrue(should_abort_warm_session(FamilyClaimOutcome.CHECKPOINT))
        self.assertFalse(should_abort_warm_session(FamilyClaimOutcome.EMPTY_FEED))

    def test_worker_rollout_enabled_defaults_to_all_workers_when_allowlist_empty(self) -> None:
        self.assertTrue(worker_rollout_enabled("worker", ()))
        self.assertTrue(worker_rollout_enabled("worker_3", []))

    def test_worker_rollout_enabled_respects_allowlist(self) -> None:
        self.assertTrue(worker_rollout_enabled("worker_3", ("worker_3",)))
        self.assertFalse(worker_rollout_enabled("worker", ("worker_3",)))

    def test_worker_rollout_enabled_accepts_all_v42_workers(self) -> None:
        allowlist = ("worker", "worker_2", "worker_3")
        self.assertTrue(worker_rollout_enabled("worker", allowlist))
        self.assertTrue(worker_rollout_enabled("worker_2", allowlist))
        self.assertTrue(worker_rollout_enabled("worker_3", allowlist))


if __name__ == "__main__":
    unittest.main()
