from datetime import date, datetime

import numcodecs
import numpy as np
import polars as pl

from energy_pipeline import first_seen, rain

ZARRAY = {
    "chunks": [rain.CHUNK_T, rain.CHUNK_Y, rain.CHUNK_X],
    "compressor": {"id": "blosc", "cname": "zstd", "clevel": 3, "shuffle": 2, "blocksize": 0},
    "dtype": "<f4",
    "filters": [
        {"id": "fixedscaleoffset", "offset": 1000, "scale": 100, "dtype": "<f4", "astype": "<f4"}
    ],
}


def encode(chunk: np.ndarray) -> bytes:
    data = chunk.astype("<f4")
    for spec in ZARRAY["filters"]:
        data = numcodecs.get_codec(spec).encode(data)
    return bytes(numcodecs.get_codec(ZARRAY["compressor"]).encode(data))


def fake_store(value_at) -> rain.RainStore:
    """Every chunk is filled from ``value_at(time_index)`` so the expected series is known."""

    def fetch(key: str) -> bytes | None:
        ti = int(key.split("/")[1].split(".")[0])
        t = ti * rain.CHUNK_T + np.arange(rain.CHUNK_T, dtype="<f4")
        chunk = np.broadcast_to(value_at(t)[:, None, None], (rain.CHUNK_T, 50, 100)).copy()
        return encode(chunk)

    return rain.RainStore(fetch, ZARRAY)


def test_grid_index_matches_the_zarr_axes():
    assert rain.grid_index(-89.95, -179.95) == (0, 0)
    assert rain.grid_index(-20.67, -46.31) == (693, 1336)


def test_decode_roundtrip_with_scale_offset():
    chunk = np.full((rain.CHUNK_T, 50, 100), 12.34, dtype="<f4")
    out = rain.decode_chunk(encode(chunk), ZARRAY)
    assert out.shape == chunk.shape
    assert abs(float(out[0, 0, 0]) - 12.34) < 0.01


def test_window_series_reads_the_requested_days_across_chunks():
    store = fake_store(lambda t: (t % 10) + 0.5)
    first, last = date(2001, 12, 30), date(2002, 1, 2)  # crosses the first time chunk edge
    series = store.window_series(-20.67, -46.31, first, last)
    assert sorted(series) == [
        date(2001, 12, 30) + (date(2001, 12, 31) - date(2001, 12, 30)) * i for i in range(4)
    ]
    t = (first - rain.ORIGIN).days
    assert abs(series[first] - ((t % 10) + 0.5)) < 0.01


def test_nan_days_are_dropped():
    store = fake_store(lambda t: np.where(t > 5000, np.nan, 1.0))
    series = store.window_series(-20.67, -46.31, date(2011, 8, 1), date(2011, 9, 1))
    assert series  # days before the cut-off exist
    far = store.window_series(-20.67, -46.31, date(2030, 1, 1), date(2030, 1, 3))
    assert far == {}


def test_run_is_final_only_after_the_latency():
    now = datetime(2026, 10, 30, tzinfo=None)
    assert rain.run_of(date(2026, 10, 28), now) == "late"
    assert rain.run_of(date(2026, 7, 15), now) == "late"
    assert rain.run_of(date(2026, 6, 1), now) == "final"


def frame(day, mm, observed, first_seen=None, first_mm=None, run="late"):
    return pl.DataFrame(
        [
            {
                "reservoir_id": "R1",
                "day": day,
                "precip_mm": mm,
                "run": run,
                "observed_at": observed,
                "first_seen_at": first_seen or observed,
                "first_precip_mm": first_mm if first_mm is not None else mm,
            }
        ],
        schema=rain.RAIN_SCHEMA,
    )


def test_merge_keeps_the_first_sighting_and_measures_late_vs_final():
    t1 = datetime(2026, 7, 1, tzinfo=None)
    t2 = datetime(2026, 11, 20, tzinfo=None)
    old = frame(date(2026, 6, 25), 10.0, t1)
    new = frame(date(2026, 6, 25), 14.0, t2, run="final")
    merged = rain.merge_rain(old, new)
    row = merged.row(0, named=True)
    assert row["precip_mm"] == 14.0
    assert row["first_precip_mm"] == 10.0
    assert row["first_seen_at"] == t1
    assert rain.late_vs_final(merged) == {"days": 1.0, "mean_abs_gap_mm": 4.0}


def test_build_frame_uses_one_row_per_reservoir_and_day():
    store = fake_store(lambda t: np.full_like(t, 3.0))
    points = pl.DataFrame(
        {"reservoir_id": ["A", "B"], "latitude": [-20.67, None], "longitude": [-46.31, None]}
    )
    out = rain.build_rain_frame(
        points, store, date(2020, 1, 1), date(2020, 1, 3), datetime(2026, 10, 30, tzinfo=None)
    )
    assert out["reservoir_id"].unique().to_list() == ["A"]
    assert out.height == 3
    assert set(out["run"]) == {"final"}


def operations(*days):
    return pl.DataFrame(
        {
            "reservoir_id": ["R1"] * len(days),
            "day": list(days),
            "natural_inflow_m3s": [1.0 if d else None for d in days],
        }
    )


def test_first_seen_marks_backfill_and_never_moves_a_day():
    t1 = datetime(2026, 10, 30, 12, tzinfo=None)
    first = first_seen.update_first_seen(None, operations(date(2026, 10, 29), date(2026, 1, 1)), t1)
    assert dict(zip(first["day"], first["backfill"], strict=True)) == {
        date(2026, 1, 1): True,
        date(2026, 10, 29): False,
    }
    t2 = datetime(2026, 10, 31, 12, tzinfo=None)
    again = first_seen.update_first_seen(
        first, operations(date(2026, 10, 29), date(2026, 10, 30)), t2
    )
    by_day = dict(zip(again["day"], again["first_seen_at"], strict=True))
    assert by_day[date(2026, 10, 29)] == t1
    assert by_day[date(2026, 10, 30)] == t2
    summary = first_seen.latency_summary(again)
    assert summary["days"] == 2.0
    assert summary["max_delay_days"] == 1.0


def test_first_seen_skips_days_without_a_natural_inflow():
    ops = pl.DataFrame(
        {"reservoir_id": ["R1"], "day": [date(2026, 10, 29)], "natural_inflow_m3s": [None]},
        schema={"reservoir_id": pl.String, "day": pl.Date, "natural_inflow_m3s": pl.Float64},
    )
    out = first_seen.update_first_seen(None, ops, datetime(2026, 10, 30, tzinfo=None))
    assert out.is_empty()
