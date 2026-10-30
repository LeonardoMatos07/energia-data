from datetime import datetime, timedelta

import polars as pl

from energy_pipeline import quality, schema, steps, system, transform
from energy_pipeline.period import Period
from energy_pipeline.sources import ENERGY_BALANCE, PIPELINES, SOURCES
from tests.conftest import raw_frame

FEBRUARY = Period(2024, 2)
BALANCE_HEADER = (
    "id_subsistema;nom_subsistema;din_instante;val_gerhidraulica;val_gertermica;val_gereolica;"
    "val_gersolar;val_carga;val_intercambio"
)


def raw_balance() -> pl.DataFrame:
    return raw_frame(
        BALANCE_HEADER,
        "NE;NORDESTE;2024-02-01 00:00:00;1000,5;200;300;0;1500;-10",
        "NE;NORDESTE;2024-02-01 00:00:00;1;1;1;1;1;1",
        "SE;SUDESTE;2024-02-01 00:00:00;5000;800;100;50;7000;25",
        "SE;SUDESTE;not a date;1;1;1;1;1;1",
        "S;SUL;2024-03-01 00:00:00;1;1;1;1;1;1",
        " ;SUL;2024-02-01 01:00:00;1;1;1;1;1;1",
    )


def balance_silver(hours: int = 4) -> pl.DataFrame:
    moments = [FEBRUARY.start + timedelta(hours=h) for h in range(hours)]
    rows = []
    for subsystem, base in (("NE", 100.0), ("SE", 1000.0)):
        for moment in moments:
            rows.append(
                {
                    "subsystem_id": subsystem,
                    "measured_at": moment,
                    "measured_at_utc": moment + timedelta(hours=3),
                    "hydro_mw_avg": base,
                    "thermal_mw_avg": base / 2,
                    "wind_mw_avg": base / 10,
                    "solar_mw_avg": 0.0,
                    "load_mw_avg": base * 2,
                    "net_interchange_mw_avg": -5.0,
                }
            )
    return pl.DataFrame(rows)


def test_energy_balance_is_registered_in_the_system_pipeline():
    assert "energy_balance" in PIPELINES["system"]
    assert {"plant_generation", "load_curve", "generation_capacity"} <= set(PIPELINES["system"])
    assert SOURCES["energy_balance"] is ENERGY_BALANCE
    assert "energy_balance" not in PIPELINES["curtailment"]


def test_transform_energy_balance_types_dedupes_and_rejects():
    result = transform.transform_energy_balance(raw_balance(), FEBRUARY)

    assert result.valid.columns[:4] == [
        "subsystem_id",
        "subsystem_name",
        "measured_at",
        "hydro_mw_avg",
    ]
    assert result.valid.height == 2  # NE (deduplicated, last wins) and SE
    assert result.duplicates_removed == 1
    assert result.rejected.height == 2  # unparsed date and empty subsystem
    northeast = result.valid.filter(pl.col("subsystem_id") == "NE").row(0, named=True)
    assert northeast["hydro_mw_avg"] == 1.0  # the later duplicate wins
    southeast = result.valid.filter(pl.col("subsystem_id") == "SE").row(0, named=True)
    assert southeast["net_interchange_mw_avg"] == 25.0
    assert result.valid["subsystem_name"].to_list() == ["NORDESTE", "SUDESTE"]


def test_energy_balance_layout_is_checked():
    clean = raw_balance().head(3)
    schema.check_schema("energy_balance", clean)
    broken = clean.drop("val_gersolar")
    try:
        schema.check_schema("energy_balance", broken)
    except schema.SchemaChangedError as error:
        assert "val_gersolar" in str(error)
    else:
        raise AssertionError("a missing column must fail")


def complete_balance(period: Period) -> pl.DataFrame:
    hours = [period.start + timedelta(hours=h) for h in range(period.hours)]
    count = len(quality.SUBSYSTEMS)
    return pl.DataFrame(
        {
            "subsystem_id": [s for s in sorted(quality.SUBSYSTEMS) for _ in hours],
            "measured_at": hours * count,
            "hydro_mw_avg": [1000.0] * (period.hours * count),
            "thermal_mw_avg": [100.0] * (period.hours * count),
            "wind_mw_avg": [50.0] * (period.hours * count),
            "solar_mw_avg": [0.0] * (period.hours * count),
            "load_mw_avg": [2000.0] * (period.hours * count),
        }
    )


def test_energy_balance_checks_pass_on_a_complete_month():
    results = quality.energy_balance_checks(complete_balance(FEBRUARY), FEBRUARY, 0, 0.01)

    assert all(result.passed for result in results), [str(r) for r in results if not r.passed]


def test_energy_balance_checks_tolerate_tiny_negative_readings():
    noisy = complete_balance(FEBRUARY).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(-0.16)
        .otherwise(pl.col("solar_mw_avg"))
        .alias("solar_mw_avg")
    )

    assert all(r.passed for r in quality.energy_balance_checks(noisy, FEBRUARY, 0, 0.01))


def test_energy_balance_checks_block_negative_generation():
    broken = complete_balance(FEBRUARY).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(-50.0)
        .otherwise(pl.col("hydro_mw_avg"))
        .alias("hydro_mw_avg")
    )
    results = quality.energy_balance_checks(broken, FEBRUARY, 0, 0.01)

    failed = [result for result in results if not result.passed]
    assert any("hydro_mw_avg" in result.name for result in failed)


def test_monthly_generation_sums_sources_and_the_national_total():
    table = system.build_monthly_generation(Period(2024, 2), balance_silver(hours=4))

    hydro = table.filter((pl.col("subsystem") == "SIN") & (pl.col("source") == "hydro")).row(
        0, named=True
    )
    assert hydro["energy_mwh"] == (100.0 + 1000.0) * 4
    assert hydro["peak_mw"] == 1100.0
    assert hydro["avg_mw"] == round(hydro["energy_mwh"] / FEBRUARY.hours, 3)
    assert set(table["subsystem"]) == {"NE", "SE", "SIN"}
    assert set(table["source"]) == set(system.GENERATION_SOURCES)
    shares = table.filter(pl.col("subsystem") == "NE")["share_pct"].sum()
    assert abs(shares - 100.0) < 0.05


def test_the_ons_national_total_is_not_double_counted():
    national = balance_silver(hours=2).with_columns(pl.lit("SIN").alias("subsystem_id"))
    with_total = pl.concat([balance_silver(hours=2), national])

    monthly = system.build_monthly_generation(FEBRUARY, with_total)
    plain = system.build_monthly_generation(FEBRUARY, balance_silver(hours=2))

    assert monthly.equals(plain)
    assert set(system.build_hourly_balance(with_total)["subsystem"]) == {"NE", "SE"}
    results = quality.energy_balance_checks(complete_balance(FEBRUARY), FEBRUARY, 0, 0.01)
    assert all(result.passed for result in results)


def test_hourly_balance_uses_local_and_utc_hours():
    table = system.build_hourly_balance(balance_silver(hours=2))

    assert table.columns == [
        "hour",
        "subsystem",
        "hour_utc",
        "hydro_mw_avg",
        "thermal_mw_avg",
        "wind_mw_avg",
        "solar_mw_avg",
        "load_mw_avg",
        "net_interchange_mw_avg",
    ]
    first = table.row(0, named=True)
    assert first["hour_utc"] - first["hour"] == timedelta(hours=3)


def plant_rows(plant_id: str, source_type: str, mwh: float, subsystem: str = "S") -> pl.DataFrame:
    moments = [datetime(2024, 2, 1) + timedelta(hours=h) for h in range(4)]  # noqa: DTZ001
    return pl.DataFrame(
        {
            "measured_at": moments,
            "plant_id": [plant_id] * 4,
            "plant_name": ["Itaipu"] * 4,
            "ceg": ["UHE.PH.PR.1"] * 4,
            "source_type": [source_type] * 4,
            "fuel_type": ["AGUA"] * 4,
            "subsystem_id": [subsystem] * 4,
            "state_id": ["PR"] * 4,
            "generation_mwh": [mwh] * 4,
        }
    )


def capacity_frame(capacity_mw: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ceg": ["UHE.PH.PR.1", "UHE.PH.PR.1"],
            "capacity_mw": [capacity_mw / 2, capacity_mw / 2],
            "decommission_date": [None, None],
        },
        schema_overrides={"decommission_date": pl.Date},
    )


def test_power_plants_get_slug_state_and_summed_capacity():
    plants = system.build_power_plants(plant_rows("ITA1", "HYDRO", 10_000.0), capacity_frame(14000))

    row = plants.row(0, named=True)
    assert row["slug"] == "itaipu-ita1"
    assert row["source_type"] == "hydro"
    assert row["state"] == "PR"
    assert row["capacity_mw"] == 14000.0


def test_plant_monthly_capacity_factor_and_implausible_values():
    period = Period(2024, 2)
    plants = plant_rows("ITA1", "HYDRO", 10_000.0)
    power_plant = system.build_power_plants(plants, capacity_frame(14000))
    monthly, implausible = system.build_plant_monthly(period, plants, power_plant)

    row = monthly.row(0, named=True)
    assert row["energy_mwh"] == 40_000.0
    assert row["peak_mw"] == 10_000.0
    assert row["capacity_factor"] == round(40_000.0 / (14000 * period.hours), 4)
    assert implausible == 0

    tiny = system.build_power_plants(plants, capacity_frame(10))
    monthly, implausible = system.build_plant_monthly(period, plants, tiny)
    assert implausible == 1
    assert monthly["capacity_factor"].to_list() == [None]  # never stored above the cap


def test_plant_without_capacity_has_no_capacity_factor():
    period = Period(2024, 2)
    plants = plant_rows("ITA1", "HYDRO", 100.0)
    power_plant = system.build_power_plants(plants, None)
    monthly, _ = system.build_plant_monthly(period, plants, power_plant)

    assert monthly["capacity_factor"].to_list() == [None]


def test_cross_check_reports_the_deviation_without_failing():
    period = Period(2024, 2)
    balance = balance_silver(hours=4)
    monthly = system.build_monthly_generation(period, balance)
    plants = plant_rows("X", "HYDRO", 1_000.0, subsystem="NE")  # balance says 400 MWh for NE

    messages = system.cross_check(monthly, plants)

    assert messages and "NE/hydro" in messages[0]
    assert system.cross_check(monthly, plant_rows("X", "HYDRO", 100.0, subsystem="NE")) == []


def test_build_system_tables_handles_missing_inputs():
    period = Period(2024, 2)
    only_balance = system.build_system_tables(period, balance_silver(), None, None)
    assert only_balance.hourly_balance is not None and only_balance.power_plant is None

    only_plants = system.build_system_tables(
        period, None, plant_rows("ITA1", "HYDRO", 10.0), capacity_frame(100)
    )
    assert only_plants.hourly_balance is None
    assert only_plants.plant_monthly is not None


def test_run_publish_dispatches_the_system_pipeline(monkeypatch):
    seen = []
    monkeypatch.setattr(steps, "run_publish_system", lambda period, settings: seen.append(period))

    steps.run_publish(Period(2024, 2), settings=None, pipeline="system")  # type: ignore[arg-type]

    assert seen == [Period(2024, 2)]
