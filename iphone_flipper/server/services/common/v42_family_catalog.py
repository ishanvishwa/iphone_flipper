from __future__ import annotations

from dataclasses import dataclass


V42_FAMILY_CATALOG_VERSION = "2026-03-12"


@dataclass(frozen=True)
class QueryVariantPreset:
    query_text: str
    validation_state: str = "validated"
    weight: float = 1.0
    is_enabled: bool = True
    notes: str | None = None


@dataclass(frozen=True)
class QueryFamilyPreset:
    name: str
    lane: str
    priority: int
    min_gap_s: int
    max_gap_s: int | None
    variants: tuple[QueryVariantPreset, ...]
    is_enabled: bool = True
    legacy_route_name: str | None = None
    legacy_worker_name: str | None = None


V42_FAMILY_PRESETS: tuple[QueryFamilyPreset, ...] = (
    QueryFamilyPreset(
        name="iphone_broad",
        lane="hot",
        priority=300,
        min_gap_s=5,
        max_gap_s=5,
        variants=(
            QueryVariantPreset(
                query_text="iPhone",
                validation_state="validated",
                notes="Initial broad family variant kept hot for fast discovery.",
            ),
        ),
        legacy_route_name="worker_3__env_default",
    ),
    QueryFamilyPreset(
        name="iphone_16_pro_max",
        lane="warm",
        priority=240,
        min_gap_s=18,
        max_gap_s=60,
        variants=(
            QueryVariantPreset(
                query_text="iPhone 16 Pro Max",
                notes="Current-generation high-value model preset.",
            ),
        ),
    ),
    QueryFamilyPreset(
        name="iphone_16_pro",
        lane="warm",
        priority=235,
        min_gap_s=18,
        max_gap_s=60,
        variants=(
            QueryVariantPreset(
                query_text="iPhone 16 Pro",
                notes="Current-generation high-value model preset.",
            ),
        ),
    ),
    QueryFamilyPreset(
        name="iphone_15_pro_max",
        lane="warm",
        priority=220,
        min_gap_s=20,
        max_gap_s=75,
        variants=(
            QueryVariantPreset(
                query_text="iPhone 15 Pro Max",
                notes="Recent-generation high-value model preset.",
            ),
        ),
    ),
    QueryFamilyPreset(
        name="iphone_15_pro",
        lane="warm",
        priority=215,
        min_gap_s=20,
        max_gap_s=75,
        variants=(
            QueryVariantPreset(
                query_text="iPhone 15 Pro",
                notes="Recent-generation high-value model preset.",
            ),
        ),
    ),
    QueryFamilyPreset(
        name="iphone_14_pro_max",
        lane="warm",
        priority=205,
        min_gap_s=24,
        max_gap_s=90,
        variants=(
            QueryVariantPreset(
                query_text="iPhone 14 Pro Max",
                notes="Still-active premium resale preset.",
            ),
        ),
    ),
    QueryFamilyPreset(
        name="iphone_14_pro",
        lane="warm",
        priority=200,
        min_gap_s=24,
        max_gap_s=90,
        variants=(
            QueryVariantPreset(
                query_text="iPhone 14 Pro",
                notes="Still-active premium resale preset.",
            ),
        ),
    ),
    QueryFamilyPreset(
        name="iphone_13_pro_max",
        lane="sweep",
        priority=180,
        min_gap_s=30,
        max_gap_s=120,
        variants=(
            QueryVariantPreset(
                query_text="iPhone 13 Pro Max",
                notes="Lower-cadence premium legacy preset.",
            ),
        ),
    ),
)
