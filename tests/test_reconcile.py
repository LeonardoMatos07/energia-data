from __future__ import annotations

from pathlib import Path

import polars as pl

from energy_pipeline.period import Period
from energy_pipeline.reconcile import Deviation, official_curtailment_mwh, render
from energy_pipeline.sources import WIND_CLUSTER_CURTAILMENT


class FakeLake:
    def __init__(self, frame: pl.DataFrame) -> None:
        self.frame = frame
        self.prefix = ""

    def latest_key(self, prefix: str, file_name: str) -> str | None:
        self.prefix = prefix
        return f"{prefix}x/{file_name}"

    def download_file(self, key: str, path: Path) -> None:
        self.frame.write_parquet(path)


def test_deviation_ratio_handles_zero_official() -> None:
    period = Period(2026, 9)
    assert Deviation(period, "wind", "plant", 100.0, 99.0).ratio == 0.01
    assert Deviation(period, "wind", "plant", 0.0, 0.0).ratio == 0.0
    assert Deviation(period, "wind", "plant", 0.0, 5.0).ratio == float("inf")


def test_render_flags_deviations_above_the_tolerance() -> None:
    period = Period(2026, 9)
    table = render(
        [
            Deviation(period, "wind", "state", 1000.0, 1000.0),
            Deviation(period, "wind", "plant", 1000.0, 2000.0),
        ],
        tolerance=0.01,
    )
    assert table.splitlines()[1].endswith("ok")
    assert table.splitlines()[2].endswith("FAIL")


def test_official_curtailment_sums_gnra_in_mwh() -> None:
    frame = pl.DataFrame(
        {
            "val_geracaonaorealizadaapurada": [10.0, 20.0, None, -5.0],
            "din_instante": ["2026-09-01 00:00:00"] * 4,
        }
    )
    lake = FakeLake(frame)
    total = official_curtailment_mwh(WIND_CLUSTER_CURTAILMENT, Period(2026, 9), lake)  # type: ignore[arg-type]
    assert total == 15.0
    assert lake.prefix == "bronze/ons/wind_cluster_curtailment/"
