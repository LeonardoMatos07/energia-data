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


def test_plants_sharing_a_ceg_get_no_capacity():
    both = pl.concat([plant_rows("ITA60", "HYDRO", 10.0), plant_rows("ITA50", "HYDRO", 10.0)])

    plants = system.build_power_plants(both, capacity_frame(14000))

    assert plants["capacity_mw"].to_list() == [None, None]


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


def raw_national() -> pl.DataFrame:
    return raw_frame(
        "din_instante;id_subsistema_origem;nom_subsistema_origem;id_subsistema_destino;"
        "nom_subsistema_destino;val_intercambiomwmed;val_intercambioprogmwmed",
        "2024-02-01 00:00:00;NE; NORDESTE;SE; SUDESTE;1500,5;1400",
        "2024-02-01 00:00:00;NE; NORDESTE;SE; SUDESTE;1;1",
        "2024-02-01 01:00:00;SE; SUDESTE;NE; NORDESTE;20;",
        "2024-02-01 01:00:00;;;SE; SUDESTE;5;5",
        "2024-03-01 00:00:00;N; NORTE;NE; NORDESTE;5;5",
    )


def test_national_interchange_keeps_direction_and_signs():
    result = transform.transform_national_interchange(raw_national(), FEBRUARY)

    assert result.valid.height == 2
    assert result.duplicates_removed == 1
    assert result.rejected.height == 1
    # SE -> NE of 20 MW is stored as the fixed link NE -> SE with a negative sign
    reverse = result.valid.filter(pl.col("measured_at").dt.hour() == 1).row(0, named=True)
    assert (reverse["origin"], reverse["destination"]) == ("NE", "SE")
    assert reverse["flow_mw_avg"] == -20.0
    assert reverse["scheduled_mw_avg"] is None
    forward = result.valid.filter(pl.col("measured_at").dt.hour() == 0).row(0, named=True)
    assert forward["flow_mw_avg"] == 1.0  # the later duplicate wins


def test_international_interchange_names_the_country():
    raw = raw_frame(
        "din_instante;nom_paisdestino;val_intercambiomwmed;val_intercambioprogmwmed",
        "2024-02-01 00:00:00;Argentina                     ;-15,5;0",
        "2024-02-01 01:00:00;Uruguai                       ;40;40",
    )

    result = transform.transform_international_interchange(raw, FEBRUARY)

    assert result.valid["destination"].to_list() == ["ARGENTINA", "URUGUAI"]
    assert set(result.valid["origin"]) == {"BR"}


def interchange_silver(scope_origin: str, destination: str, flows: list[float]) -> pl.DataFrame:
    moments = [FEBRUARY.start + timedelta(hours=h) for h in range(len(flows))]
    return pl.DataFrame(
        {
            "measured_at": moments,
            "measured_at_utc": [m + timedelta(hours=3) for m in moments],
            "origin": [scope_origin] * len(flows),
            "destination": [destination] * len(flows),
            "flow_mw_avg": flows,
            "scheduled_mw_avg": [1.0] * len(flows),
        }
    )


def test_interchange_gold_has_hourly_and_monthly_rows_for_both_scopes():
    national = interchange_silver("NE", "SE", [100.0, -20.0, 60.0])
    international = interchange_silver("BR", "ARGENTINA", [-10.0, -10.0])

    hourly, monthly = system.build_interchange(FEBRUARY, national, international)  # type: ignore[misc]

    assert hourly.height == 5
    assert set(hourly["scope"]) == {"national", "international"}
    row = monthly.filter(pl.col("destination") == "SE").row(0, named=True)
    assert row["energy_mwh"] == 140.0
    assert row["peak_mw"] == 100.0 and row["min_mw"] == -20.0
    assert row["avg_mw"] == round(140.0 / FEBRUARY.hours, 3)
    assert system.build_interchange(FEBRUARY, None, None) is None


def test_interchange_checks():
    results = quality.interchange_checks(
        interchange_silver("NE", "SE", [1.0] * FEBRUARY.hours), FEBRUARY, 0, 0.01
    )
    assert all(result.passed for result in results)

    huge = interchange_silver("NE", "SE", [1.0] * FEBRUARY.hours).with_columns(
        pl.lit(90_000.0).alias("flow_mw_avg")
    )
    assert any(not r.passed for r in quality.interchange_checks(huge, FEBRUARY, 0, 0.01))


def raw_thermal() -> pl.DataFrame:
    columns = ["din_instante", "id_subsistema", "nom_usina", "cod_usinaplanejamento", "ceg"]
    columns += ["nom_combustivel"]
    reason_columns = [c for pair in schema.THERMAL_REASON_COLUMNS.values() for c in pair]
    header = ";".join(columns + reason_columns)

    def line(hour: int, code: str, total: str, merit: str) -> str:
        values = [f"2024-02-01 {hour:02d}:00:00", "N", "Aparecida", code, "UTE.GN.AM.1", "Gás"]
        for _programmed, verified in schema.THERMAL_REASON_COLUMNS.values():
            if verified == "val_verifgeracao":
                values += [total, total]
            elif verified == "val_verifordemmerito":
                values += [merit, merit]
            else:
                values += ["0", "0"]
        return ";".join(values)

    return raw_frame(
        header,
        line(0, "201", "146", "100"),
        line(1, "201", "150", "120"),
        line(1, "201", "151", "121"),
        line(2, "", "1", "1"),
    )


def test_thermal_dispatch_keeps_programmed_and_verified_per_reason():
    result = transform.transform_thermal_dispatch(raw_thermal(), FEBRUARY)

    assert result.valid.height == 3 and result.rejected.height == 0
    assert result.duplicates_removed == 1
    assert "programmed_merit_order_mwh" in result.valid.columns
    assert "verified_electrical_reason_mwh" in result.valid.columns
    assert set(result.valid["fuel_type"]) == {"GAS"}
    assert "CEG:UTE.GN.AM.1" in set(result.valid["plant_code"])


def test_thermal_plant_without_planning_code_falls_back_to_the_ceg():
    raw = raw_thermal().with_columns(pl.lit("").alias("cod_usinaplanejamento"))

    result = transform.transform_thermal_dispatch(raw, FEBRUARY)

    assert set(result.valid["plant_code"]) == {"CEG:UTE.GN.AM.1"}
    assert result.rejected.height == 0


def test_thermal_dispatch_gold_is_long_and_drops_empty_reasons():
    silver = transform.transform_thermal_dispatch(raw_thermal(), FEBRUARY).valid
    table = system.build_thermal_monthly(FEBRUARY, silver)

    assert set(table["reason"]) == {"total", "merit_order"}  # every other reason is all zero
    total = table.filter(pl.col("reason") == "total").row(0, named=True)
    assert total["verified_mwh"] == 146.0 + 151.0
    assert total["plant_code"] == "201" and total["subsystem"] == "N"
    assert table.columns == [
        "month",
        "plant_code",
        "reason",
        "plant_name",
        "ceg",
        "fuel_type",
        "subsystem",
        "verified_mwh",
        "programmed_mwh",
    ]


def test_phase_two_sources_are_in_the_system_pipeline():
    assert {"national_interchange", "international_interchange", "thermal_dispatch"} <= set(
        PIPELINES["system"]
    )
    assert SOURCES["thermal_dispatch"].file_name(Period(2022, 3)).endswith("2022_03.parquet")
    assert (
        SOURCES["thermal_dispatch"].file_name(Period(2021, 3)).endswith("DESPACHO-2_2021.parquet")
    )
