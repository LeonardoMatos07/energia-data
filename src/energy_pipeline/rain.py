"""Daily rain (IMERG) around each reservoir, read from the NASA POWER open bucket.

Source: NASA POWER analysis-ready Zarr store, ``imerg/temporal/power_imerg_daily_temporal_utc.zarr``
in the anonymous ``nasa-power`` S3 bucket (HTTPS, no account or key). The REST API of POWER is not
used: its robots.txt forbids ``/api/``.

Licence: Creative Commons Attribution 4.0 (``LICENSE.txt`` in the bucket). Attribution below.
Observations are satellite estimates, not gauge measurements.

IMERG in POWER mixes two runs: the Final run (about 3.5 months late) and, for the latest months,
the Late run (14 hours). A day is overwritten when the Final value replaces the Late one, so every
row records which run it most likely is and the first value that was seen, to measure the gap.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import numcodecs
import numpy as np
import polars as pl

log = logging.getLogger(__name__)

STORE_URL = "https://nasa-power.s3.amazonaws.com/imerg/temporal/power_imerg_daily_temporal_utc.zarr"
VARIABLE = "IMERG_PRECTOT"
LICENSE = "CC-BY-4.0"
ATTRIBUTION = (
    "NASA Prediction of Worldwide Energy Resources (POWER) IMERG daily precipitation, accessed on "
    "{date} from https://registry.opendata.aws/nasa-power ; GPM IMERG: Huffman et al., "
    "doi:10.5067/GPM/IMERGDF/DAY/07"
)
LAKE_KEY = "silver/rain_imerg_daily/latest/data.parquet"

ORIGIN = date(1998, 1, 1)
LAT_FIRST = -89.95
LON_FIRST = -179.95
STEP = 0.1
CHUNK_T, CHUNK_Y, CHUNK_X = 1461, 50, 100
# POWER documents the Final run with 3.5 months of latency; later days are the Late run.
FINAL_LATENCY_DAYS = 120

RAIN_SCHEMA: dict[str, Any] = {
    "reservoir_id": pl.String,
    "day": pl.Date,
    "precip_mm": pl.Float64,
    "run": pl.String,
    "observed_at": pl.Datetime("us"),
    "first_seen_at": pl.Datetime("us"),
    "first_precip_mm": pl.Float64,
}

ChunkFetcher = Callable[[str], bytes | None]


def _naive(moment: datetime) -> datetime:
    """UTC wall time without tzinfo (the columns are naive UTC; no tz database is needed)."""
    return moment if moment.tzinfo is None else moment.astimezone(UTC).replace(tzinfo=None)


def grid_index(latitude: float, longitude: float) -> tuple[int, int]:
    """Index of the 0.1 degree pixel that holds the point."""
    return round((latitude - LAT_FIRST) / STEP), round((longitude - LON_FIRST) / STEP)


def run_of(day: date, observed_at: datetime) -> str:
    return "final" if (observed_at.date() - day).days >= FINAL_LATENCY_DAYS else "late"


def decode_chunk(raw: bytes, zarray: dict[str, Any]) -> np.ndarray:
    """Zarr v2 chunk: blosc decompression, then the filters in reverse order."""
    data: Any = numcodecs.get_codec(dict(zarray["compressor"])).decode(raw)
    for spec in reversed(zarray.get("filters") or []):
        data = numcodecs.get_codec(dict(spec)).decode(data)
    shape = tuple(zarray["chunks"])
    return np.asarray(data).view(zarray["dtype"]).reshape(shape)


@dataclass
class RainStore:
    """Reads pixel windows from the Zarr store; ``fetch`` is injected so tests need no network."""

    fetch: ChunkFetcher
    zarray: dict[str, Any]

    def _chunk(self, ti: int, yi: int, xi: int) -> np.ndarray | None:
        raw = self.fetch(f"{VARIABLE}/{ti}.{yi}.{xi}")
        return None if raw is None else decode_chunk(raw, self.zarray)

    def window_series(
        self, latitude: float, longitude: float, first: date, last: date, radius: int = 1
    ) -> dict[date, float]:
        """Mean of the (2r+1)^2 pixels around the point, per day; days without data are left out."""
        iy, ix = grid_index(latitude, longitude)
        ys = range(max(iy - radius, 0), min(iy + radius, 1799) + 1)
        xs = range(max(ix - radius, 0), min(ix + radius, 3599) + 1)
        t0 = (first - ORIGIN).days
        t1 = (last - ORIGIN).days
        out: dict[date, float] = {}
        for ti in range(t0 // CHUNK_T, t1 // CHUNK_T + 1):
            stack = []
            for y in ys:
                for x in xs:
                    chunk = self._chunk(ti, y // CHUNK_Y, x // CHUNK_X)
                    if chunk is not None:
                        stack.append(chunk[:, y % CHUNK_Y, x % CHUNK_X].astype(np.float64))
            if not stack:
                continue
            with np.errstate(all="ignore"):
                mean = np.nanmean(np.vstack(stack), axis=0)
            for offset, value in enumerate(mean):
                t = ti * CHUNK_T + offset
                if t < t0 or t > t1 or math.isnan(value):
                    continue
                out[ORIGIN + timedelta(days=t)] = round(float(value), 3)
        return out


def build_rain_frame(
    points: pl.DataFrame,
    store: RainStore,
    first: date,
    last: date,
    observed_at: datetime,
    radius: int = 1,
) -> pl.DataFrame:
    """One row per reservoir and day. ``points`` has reservoir_id, latitude and longitude."""
    rows = []
    for rid, lat, lon in points.select("reservoir_id", "latitude", "longitude").iter_rows():
        if lat is None or lon is None:
            log.warning("Reservoir %s has no coordinates; no rain series", rid)
            continue
        for day, mm in store.window_series(lat, lon, first, last, radius).items():
            rows.append(
                {
                    "reservoir_id": rid,
                    "day": day,
                    "precip_mm": mm,
                    "run": run_of(day, observed_at),
                    "observed_at": _naive(observed_at),
                    "first_seen_at": _naive(observed_at),
                    "first_precip_mm": mm,
                }
            )
    return pl.DataFrame(rows, schema=RAIN_SCHEMA).sort("reservoir_id", "day")


def merge_rain(existing: pl.DataFrame | None, fresh: pl.DataFrame) -> pl.DataFrame:
    """New values replace old ones; ``first_seen_at`` and ``first_precip_mm`` keep the first
    sighting of the day (usually the Late run), so Late against Final can be measured."""
    if existing is None or existing.is_empty():
        return fresh
    keys = ["reservoir_id", "day"]
    first = existing.select(*keys, "first_seen_at", "first_precip_mm")
    updated = fresh.drop("first_seen_at", "first_precip_mm").join(first, on=keys, how="left")
    updated = updated.with_columns(
        pl.col("first_seen_at").fill_null(pl.col("observed_at")),
        pl.col("first_precip_mm").fill_null(pl.col("precip_mm")),
    ).select(list(RAIN_SCHEMA))
    kept = existing.join(fresh.select(keys), on=keys, how="anti")
    return pl.concat([kept, updated]).sort(keys)


def late_vs_final(df: pl.DataFrame) -> dict[str, float]:
    """Mean absolute gap between the first (Late) value and the Final one, for days that changed."""
    final = df.filter(
        (pl.col("run") == "final") & (pl.col("first_seen_at") < pl.col("observed_at"))
    )
    if final.is_empty():
        return {"days": 0.0, "mean_abs_gap_mm": float("nan")}
    gap = (final["precip_mm"] - final["first_precip_mm"]).abs()
    return {"days": float(final.height), "mean_abs_gap_mm": float(gap.mean() or 0.0)}  # type: ignore[arg-type]


def http_fetcher(timeout: int = 60) -> ChunkFetcher:
    import requests

    session = requests.Session()
    session.headers["User-Agent"] = "spillway-data rain (anonymous open-bucket read)"

    def fetch(key: str) -> bytes | None:
        response = session.get(f"{STORE_URL}/{key}", timeout=timeout)
        if response.status_code in (403, 404):
            return None
        response.raise_for_status()
        return response.content

    return fetch


def open_store(fetch: ChunkFetcher | None = None) -> RainStore:
    fetch = fetch or http_fetcher()
    raw = fetch(f"{VARIABLE}/.zarray")
    if raw is None:
        raise RuntimeError("IMERG .zarray not found in the NASA POWER bucket")
    return RainStore(fetch, json.loads(raw))


def main(argv: list[str] | None = None) -> None:
    import argparse

    from energy_pipeline.config import Settings
    from energy_pipeline.steps import RESERVOIRS_LATEST_KEY
    from energy_pipeline.storage import Lake

    parser = argparse.ArgumentParser(prog="energy_pipeline.rain")
    parser.add_argument("--from", dest="first", type=date.fromisoformat, default=date(2001, 1, 1))
    parser.add_argument("--to", dest="last", type=date.fromisoformat, default=None)
    parser.add_argument("--basin", action="append", help="only reservoirs of these basins")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    settings = Settings.from_env()
    lake = Lake(settings.lake_bucket, settings.s3_endpoint_url)
    observed_at = datetime.now(UTC)
    last = args.last or (observed_at.date() - timedelta(days=2))
    reservoirs = lake.read_parquet(RESERVOIRS_LATEST_KEY)
    if args.basin:
        reservoirs = reservoirs.filter(pl.col("basin").is_in(args.basin))
    points = reservoirs.select("reservoir_id", "latitude", "longitude")
    fresh = build_rain_frame(points, open_store(), args.first, last, observed_at)
    existing = lake.read_parquet(LAKE_KEY) if lake.metadata(LAKE_KEY) else None
    merged = merge_rain(existing, fresh)
    lake.write_parquet(merged, LAKE_KEY)
    print(ATTRIBUTION.format(date=observed_at.date().isoformat()))
    print(f"{fresh.height} rows fetched, {merged.height} stored; {late_vs_final(merged)}")


if __name__ == "__main__":
    main()
