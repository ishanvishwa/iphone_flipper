from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


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

V42_FAMILY_PRESET_NAMES: tuple[str, ...] = tuple(
    preset.name for preset in V42_FAMILY_PRESETS if str(preset.name or "").strip()
)
V42_BROAD_FAMILY_NAMES: tuple[str, ...] = ("iphone_broad",)


def normalize_v42_family_names(family_names: Iterable[str] | None) -> tuple[str, ...]:
    if family_names is None:
        return V42_FAMILY_PRESET_NAMES

    normalized: list[str] = []
    seen: set[str] = set()
    preset_names_by_lower = {name.lower(): name for name in V42_FAMILY_PRESET_NAMES}
    for value in family_names:
        token = str(value or "").strip()
        if not token:
            continue
        canonical = preset_names_by_lower.get(token.lower(), token)
        identity = canonical.lower()
        if identity in seen:
            continue
        seen.add(identity)
        normalized.append(canonical)
    return tuple(normalized)


def _normalize_variant_state(value: str | None) -> str:
    state = str(value or "").strip().lower()
    if state in {"validated", "pending_validation", "rejected"}:
        return state
    return "pending_validation"


async def sync_v42_family_presets(
    conn: Any,
    *,
    family_names: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    target_names = {name.lower() for name in normalize_v42_family_names(family_names)}
    results: list[dict[str, Any]] = []
    for preset in V42_FAMILY_PRESETS:
        if target_names and preset.name.lower() not in target_names:
            continue

        family_row = await conn.fetchrow(
            """
            INSERT INTO query_families (
                name,
                legacy_route_name,
                legacy_worker_name,
                is_enabled,
                priority,
                lane,
                next_due_at,
                min_gap_s,
                max_gap_s,
                variant_cursor,
                variant_count,
                last_error
            ) VALUES (
                $1, $2, $3, $4, $5, $6, NOW(), $7, $8, 0, 0, NULL
            )
            ON CONFLICT (name) DO UPDATE SET
                legacy_route_name = COALESCE(EXCLUDED.legacy_route_name, query_families.legacy_route_name),
                legacy_worker_name = COALESCE(EXCLUDED.legacy_worker_name, query_families.legacy_worker_name),
                is_enabled = EXCLUDED.is_enabled,
                priority = EXCLUDED.priority,
                lane = EXCLUDED.lane,
                min_gap_s = EXCLUDED.min_gap_s,
                max_gap_s = EXCLUDED.max_gap_s,
                next_due_at = LEAST(COALESCE(query_families.next_due_at, NOW()), NOW()),
                last_error = NULL
            RETURNING family_id, name
            """,
            preset.name,
            preset.legacy_route_name,
            preset.legacy_worker_name,
            preset.is_enabled,
            preset.priority,
            preset.lane,
            preset.min_gap_s,
            preset.max_gap_s,
        )
        if family_row is None:
            raise RuntimeError(f"Failed to bootstrap family {preset.name}.")

        family_id = int(family_row["family_id"])
        for variant_order, variant in enumerate(preset.variants):
            await conn.execute(
                """
                INSERT INTO query_variants (
                    family_id,
                    query_text,
                    url_template,
                    validation_state,
                    weight,
                    variant_order,
                    is_enabled,
                    notes
                ) VALUES (
                    $1, $2, NULL, $3, $4, $5, $6, $7
                )
                ON CONFLICT (family_id, query_text) DO UPDATE SET
                    validation_state = EXCLUDED.validation_state,
                    weight = EXCLUDED.weight,
                    variant_order = EXCLUDED.variant_order,
                    is_enabled = EXCLUDED.is_enabled,
                    notes = EXCLUDED.notes
                """,
                family_id,
                variant.query_text,
                _normalize_variant_state(variant.validation_state),
                float(variant.weight),
                variant_order,
                bool(variant.is_enabled),
                variant.notes,
            )

        await conn.execute(
            """
            UPDATE query_families
            SET
                variant_count = COALESCE((
                    SELECT COUNT(*)::INT
                    FROM query_variants
                    WHERE family_id = $1
                      AND COALESCE(is_enabled, TRUE) = TRUE
                      AND LOWER(COALESCE(validation_state, 'pending_validation')) = 'validated'
                ), 0),
                variant_cursor = 0
            WHERE family_id = $1
            """,
            family_id,
        )
        results.append({"family_id": family_id, "name": str(family_row["name"] or "").strip()})
    return results
