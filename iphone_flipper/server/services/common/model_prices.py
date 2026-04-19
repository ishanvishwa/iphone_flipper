from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Mapping

import asyncpg

MODEL_PRICE_FIELDS: tuple[str, ...] = (
    "model",
    "buying_price",
    "selling_price",
    "backglass_repair",
    "screen_repair",
    "battery_repair",
    "camera_lens_repair",
)


def normalize_model_name(model: Any) -> str:
    return " ".join(str(model or "").strip().split()).lower()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def normalize_model_price_item(item: Mapping[str, Any]) -> dict[str, Any]:
    model = " ".join(str(item.get("model") or "").strip().split())
    if not model:
        raise ValueError("model is required")
    return {
        "model": model,
        "buying_price": _safe_float(item.get("buying_price")),
        "selling_price": _safe_float(item.get("selling_price")),
        "backglass_repair": _safe_float(item.get("backglass_repair")),
        "screen_repair": _safe_float(item.get("screen_repair")),
        "battery_repair": _safe_float(item.get("battery_repair")),
        "camera_lens_repair": _safe_float(item.get("camera_lens_repair")),
    }


def read_model_prices_csv(csv_path: str | Path) -> list[dict[str, Any]]:
    path = Path(csv_path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, Any]] = []
        for raw_row in reader:
            rows.append(
                normalize_model_price_item(
                    {
                        "model": raw_row.get("Model"),
                        "buying_price": raw_row.get("Buying Price"),
                        "selling_price": raw_row.get("Selling Price"),
                        "backglass_repair": raw_row.get("Backglass repair cost"),
                        "screen_repair": raw_row.get("Screen repair cost"),
                        "battery_repair": raw_row.get("Battery repair cost"),
                        "camera_lens_repair": raw_row.get("Camera lens repair cost"),
                    }
                )
            )
    return rows


def rows_to_price_lookup(rows: list[Mapping[str, Any]]) -> dict[str, dict[str, float]]:
    price_lookup: dict[str, dict[str, float]] = {}
    for row in rows:
        model = " ".join(str(row.get("model") or "").strip().split())
        if not model:
            continue
        price_lookup[model] = {
            "buying_price": _safe_float(row.get("buying_price")),
            "selling_price": _safe_float(row.get("selling_price")),
            "backglass_repair": _safe_float(row.get("backglass_repair")),
            "screen_repair": _safe_float(row.get("screen_repair")),
            "battery_repair": _safe_float(row.get("battery_repair")),
            "camera_lens_repair": _safe_float(row.get("camera_lens_repair")),
        }
    return price_lookup


async def fetch_model_price_rows(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            model,
            buying_price,
            selling_price,
            backglass_repair,
            screen_repair,
            battery_repair,
            camera_lens_repair,
            updated_at
        FROM model_prices
        ORDER BY model ASC
        """
    )
    return [dict(row) for row in rows]


async def load_model_price_lookup(pool: asyncpg.Pool) -> dict[str, dict[str, float]]:
    async with pool.acquire() as conn:
        rows = await fetch_model_price_rows(conn)
    return rows_to_price_lookup(rows)


async def replace_model_prices(
    conn: asyncpg.Connection,
    items: list[Mapping[str, Any]],
    *,
    replace_missing: bool = True,
) -> list[dict[str, Any]]:
    normalized_items = [normalize_model_price_item(item) for item in items]
    seen_models: set[str] = set()
    deduped_items: list[dict[str, Any]] = []
    for item in normalized_items:
        identity = normalize_model_name(item["model"])
        if identity in seen_models:
            continue
        seen_models.add(identity)
        deduped_items.append(item)

    if deduped_items:
        await conn.executemany(
            """
            INSERT INTO model_prices (
                model,
                buying_price,
                selling_price,
                backglass_repair,
                screen_repair,
                battery_repair,
                camera_lens_repair,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (model) DO UPDATE SET
                buying_price = EXCLUDED.buying_price,
                selling_price = EXCLUDED.selling_price,
                backglass_repair = EXCLUDED.backglass_repair,
                screen_repair = EXCLUDED.screen_repair,
                battery_repair = EXCLUDED.battery_repair,
                camera_lens_repair = EXCLUDED.camera_lens_repair,
                updated_at = NOW()
            """,
            [
                (
                    item["model"],
                    item["buying_price"],
                    item["selling_price"],
                    item["backglass_repair"],
                    item["screen_repair"],
                    item["battery_repair"],
                    item["camera_lens_repair"],
                )
                for item in deduped_items
            ],
        )

    if replace_missing:
        if deduped_items:
            await conn.execute(
                "DELETE FROM model_prices WHERE model <> ALL($1::TEXT[])",
                [str(item["model"]) for item in deduped_items],
            )
        else:
            await conn.execute("DELETE FROM model_prices")

    return deduped_items


async def seed_model_prices_from_csv_if_empty(
    pool: asyncpg.Pool,
    *,
    csv_path: str | Path,
) -> int:
    async with pool.acquire() as conn:
        existing_count = int(
            await conn.fetchval("SELECT COUNT(*) FROM model_prices")
            or 0
        )
        if existing_count > 0:
            return 0
        csv_rows = read_model_prices_csv(csv_path)
        if not csv_rows:
            return 0
        async with conn.transaction():
            await replace_model_prices(conn, csv_rows, replace_missing=True)
        return len(csv_rows)
