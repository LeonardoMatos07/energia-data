"""When did each reservoir-day first appear in the ONS file? Gives the real publication delay.

The silver tables are rebuilt every run, so the first sighting is kept apart in the lake. Days
first seen long after they happened come from a backfill and are marked, so the delay statistics
only use days that were seen close to real time.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl

LAKE_KEY = "silver/hydro_first_seen/latest/data.parquet"
BACKFILL_AFTER_DAYS = 14

SCHEMA: dict[str, Any] = {
    "reservoir_id": pl.String,
    "day": pl.Date,
    "first_seen_at": pl.Datetime("us"),
    "backfill": pl.Boolean,
}


def _naive(moment: datetime) -> datetime:
    return moment if moment.tzinfo is None else moment.astimezone(UTC).replace(tzinfo=None)


def update_first_seen(
    existing: pl.DataFrame | None, operations: pl.DataFrame, seen_at: datetime
) -> pl.DataFrame:
    """Adds the days with a natural inflow that were not recorded yet."""
    keys = ["reservoir_id", "day"]
    present = operations.filter(pl.col("natural_inflow_m3s").is_not_null()).select(keys).unique()
    if existing is not None:
        present = present.join(existing.select(keys), how="anti", on=keys)
    new = present.with_columns(
        pl.lit(_naive(seen_at)).alias("first_seen_at"),
        ((pl.lit(seen_at.date()) - pl.col("day")).dt.total_days() > BACKFILL_AFTER_DAYS).alias(
            "backfill"
        ),
    ).select(list(SCHEMA))
    if existing is None:
        return new.sort(keys)
    return pl.concat([existing, new]).sort(keys)


def latency_summary(df: pl.DataFrame) -> dict[str, float]:
    """Delay in days between the day and its first sighting, without backfilled days."""
    live = df.filter(~pl.col("backfill")).with_columns(
        (pl.col("first_seen_at").dt.date() - pl.col("day")).dt.total_days().alias("delay")
    )
    if live.is_empty():
        return {"days": 0.0}
    delay = live["delay"]
    return {
        "days": float(live.height),
        "mean_delay_days": float(delay.mean() or 0.0),  # type: ignore[arg-type]
        "p95_delay_days": float(delay.quantile(0.95) or 0.0),
        "max_delay_days": float(delay.max() or 0.0),  # type: ignore[arg-type]
    }
