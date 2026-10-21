from datetime import datetime, timedelta

import polars as pl
import pytest

from energy_pipeline import quality
from energy_pipeline.period import Period
from energy_pipeline.quality import DataQualityError, Severity

FEBRUARY_2024 = Period(2024, 2)  # leap year: 29 days = 696 hours


def complete_load_curve(period: Period) -> pl.DataFrame:
    hours = [period.start + timedelta(hours=h) for h in range(period.hours)]
    return pl.DataFrame(
        {
            "subsystem_id": [s for s in sorted(quality.SUBSYSTEMS) for _ in hours],
            "measured_at": hours * len(quality.SUBSYSTEMS),
            "load_mw_avg": [10_000.0] * (period.hours * len(quality.SUBSYSTEMS)),
        }
    )


def test_complete_load_curve_passes():
    results = quality.load_curve_checks(complete_load_curve(FEBRUARY_2024), FEBRUARY_2024, 0, 0.01)

    assert all(result.passed for result in results), [str(r) for r in results]
    quality.enforce(results)


def test_incomplete_load_curve_fails():
    df = complete_load_curve(FEBRUARY_2024).head(1000)

    with pytest.raises(DataQualityError, match="completeness"):
        quality.enforce(quality.load_curve_checks(df, FEBRUARY_2024, 0, 0.01))


def test_unknown_subsystem_fails():
    df = complete_load_curve(FEBRUARY_2024).with_columns(
        pl.when(pl.col("measured_at") == FEBRUARY_2024.start)
        .then(pl.lit("XX"))
        .otherwise(pl.col("subsystem_id"))
        .alias("subsystem_id")
    )
    result = quality.accepted_values(df, "subsystem_id", quality.SUBSYSTEMS)

    assert not result.passed
    assert "XX" in result.details


def test_capacity_accepts_paraguayan_itaipu_units():
    df = pl.DataFrame(
        {
            "unit_id": ["PRIT50-0UG1", "ALUXG-0UG5"],
            "subsystem_id": ["PY", "NE"],
            "source_type": ["HYDRO", "HYDRO"],
            "capacity_mw": [700.0, 527.0],
            "operation_start_date": [datetime(1984, 5, 5).date(), datetime(1995, 1, 31).date()],
        }
    )

    quality.enforce(quality.generation_capacity_checks(df, 0, 0.01))


def test_within_period():
    df = pl.DataFrame({"measured_at": [datetime(2024, 2, 1), datetime(2024, 3, 1)]})

    assert not quality.within_period(df, "measured_at", FEBRUARY_2024).passed


def test_ratio_threshold():
    assert quality.ratio_at_most("rejected", 1, 100, 0.01).passed
    assert not quality.ratio_at_most("rejected", 2, 100, 0.01).passed
    assert quality.ratio_at_most("rejected", 0, 0, 0.01).passed


def test_warnings_do_not_stop_the_pipeline():
    warning = quality.CheckResult("negative values", False, "3 rows", Severity.WARN)

    quality.enforce([warning])
