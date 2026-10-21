from datetime import date, datetime

import pytest

from energy_pipeline.period import Period, periods_between, trailing_periods
from energy_pipeline.sources import (
    DEFAULT_ONS_BASE_URL,
    GENERATION_CAPACITY,
    LOAD_CURVE,
    PIPELINES,
    PLANT_GENERATION,
    SOLAR_PLANT_CURTAILMENT,
)


def test_period_boundaries():
    period = Period.parse("2024-12")

    assert period.start == datetime(2024, 12, 1)
    assert period.end == datetime(2025, 1, 1)
    assert period.hours == 31 * 24
    assert str(period.previous()) == "2024-11"
    assert str(period) == "2024-12"


@pytest.mark.parametrize("value", ["2024-13", "2024-1", "24-01", "abc"])
def test_period_rejects_invalid_values(value):
    with pytest.raises(ValueError):
        Period.parse(value)


def test_source_urls():
    period = Period(2024, 3)

    assert LOAD_CURVE.url(DEFAULT_ONS_BASE_URL, period).endswith(
        "/curva-carga-ho/CURVA_CARGA_2024.csv"
    )
    assert PLANT_GENERATION.url(DEFAULT_ONS_BASE_URL, period).endswith(
        "/geracao_usina_2_ho/GERACAO_USINA-2_2024_03.csv"
    )


def test_source_partitions():
    period, snapshot = Period(2024, 3), date(2024, 4, 1)

    assert LOAD_CURVE.partition(period, snapshot) == "year=2024"
    assert PLANT_GENERATION.partition(period, snapshot) == "year=2024/month=03"
    assert GENERATION_CAPACITY.partition(period, snapshot) == "snapshot_date=2024-04-01"


def test_yearly_files_are_processed_per_month():
    period, snapshot = Period(2024, 3), date(2024, 4, 1)

    assert LOAD_CURVE.processed_partition(period, snapshot) == "year=2024/month=03"
    assert GENERATION_CAPACITY.processed_partition(period, snapshot) == "snapshot_date=2024-04-01"


def test_periods_to_process_respects_availability_and_snapshots():
    periods = periods_between(Period(2024, 2), Period(2024, 5))

    assert SOLAR_PLANT_CURTAILMENT.periods_to_process(periods) == [Period(2024, 4), Period(2024, 5)]
    assert GENERATION_CAPACITY.periods_to_process(periods) == [Period(2024, 5)]
    assert LOAD_CURVE.periods_to_process(periods) == periods


def test_reprocessing_window_crosses_years():
    assert [str(p) for p in trailing_periods(Period(2026, 2), 3)] == [
        "2025-12",
        "2026-01",
        "2026-02",
    ]


def test_dependencies_are_declared_before_dependents():
    for pipeline in PIPELINES.values():
        names = list(pipeline)
        for position, source in enumerate(pipeline.values()):
            assert all(names.index(dep) < position for dep in source.depends_on), source.name
