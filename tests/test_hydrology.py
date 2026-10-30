from datetime import date, datetime, timedelta

import polars as pl

from energy_pipeline import hydrology, quality, schema, steps, transform
from energy_pipeline.period import Period
from energy_pipeline.sources import HYDROLOGY_SOURCES, PIPELINES, SOURCES
from tests.conftest import raw_frame

SEPTEMBER = Period(2026, 9)


def test_hydrology_sources_and_file_patterns():
    assert set(PIPELINES["hydrology"]) == set(HYDROLOGY_SOURCES)
    assert SOURCES["reservoirs"].file_name(SEPTEMBER) == "RESERVATORIOS.parquet"
    assert SOURCES["subsystem_storage"].file_name(SEPTEMBER) == "EAR_DIARIO_SUBSISTEMA_2026.parquet"
    spilled = SOURCES["spilled_turbinable_energy"]
    assert spilled.file_name(Period(2023, 5)) == "ENERGIA_VERTIDA_TURBINAVEL_2023.parquet"
    assert spilled.file_name(Period(2024, 5)) == "ENERGIA_VERTIDA_TURBINAVEL_2024_05.parquet"
    assert SOURCES["hydro_operations"].available_from == Period(2015, 1)


def raw_storage() -> pl.DataFrame:
    return raw_frame(
        "id_subsistema;nom_subsistema;ear_data;ear_max_subsistema;ear_verif_subsistema_mwmes;"
        "ear_verif_subsistema_percentual",
        "NE;NORDESTE;2026-09-01;51616.053;23936.509;46.3742",
        "SE;SUDESTE;2026-09-01;204000;100000;49.0",
        "SE;SUDESTE;2026-09-01;204000;110000;53.9",
        "SE;SUDESTE;2026-10-01;204000;110000;53.9",
        ";SUDESTE;2026-09-02;1;1;1",
    )


def test_subsystem_storage_transform():
    result = transform.transform_subsystem_storage(raw_storage(), SEPTEMBER)

    assert result.valid.height == 2 and result.duplicates_removed == 1
    assert result.rejected.height == 1
    assert result.valid.schema["day"] == pl.Date
    southeast = result.valid.filter(pl.col("subsystem_id") == "SE").row(0, named=True)
    assert southeast["storage_mw_month"] == 110000.0  # the later duplicate wins
    assert southeast["day"] == date(2026, 9, 1)


def storage_frame(days: int = 3, subsystems=("N", "NE", "S", "SE"), pct=50.0) -> pl.DataFrame:
    size = days * len(subsystems)
    return pl.DataFrame(
        {
            "subsystem_id": [s for s in subsystems for _ in range(days)],
            "day": [date(2026, 9, 1 + d) for _ in subsystems for d in range(days)],
            "storage_max_mw_month": [1000.0] * size,
            "storage_mw_month": [500.0] * size,
            "storage_pct": [pct] * size,
        }
    )


def test_subsystem_storage_gates():
    ok = quality.subsystem_storage_checks(storage_frame(), SEPTEMBER, 0, 0.01)
    assert all(r.passed for r in ok)

    over = quality.subsystem_storage_checks(storage_frame(pct=101.0), SEPTEMBER, 0, 0.01)
    assert any(not r.passed and "storage_pct" in r.name for r in over)

    missing = quality.subsystem_storage_checks(
        storage_frame(subsystems=("N", "NE", "S")), SEPTEMBER, 0, 0.01
    )
    assert any(not r.passed and "every_day_has_all" in r.name for r in missing)


def test_reservoir_storage_may_exceed_100_while_spilling():
    df = pl.DataFrame(
        {
            "reservoir_name": ["JORDAO", "JORDAO"],
            "day": [date(2026, 9, 1), date(2026, 9, 2)],
            "storage_mw_month": [22.9, 23.0],
            "storage_pct": [112.7, 166.1],
        }
    )

    assert all(r.passed for r in quality.reservoir_storage_checks(df, SEPTEMBER, 0, 0.01))


def test_reservoir_storage_without_planning_code_is_kept():
    raw = raw_frame(
        "cod_resplanejamento;nom_reservatorio;id_subsistema;ear_data;ear_total_mwmes;"
        "ear_maxima_total_mwmes;ear_reservatorio_percentual",
        "18;A. VERMELHA;SE;2026-09-01;1206.8;4375.2;27.58",
        ";BLANG;S;2026-09-01;10;9;111",
    )

    result = transform.transform_reservoir_storage(raw, SEPTEMBER)

    assert result.valid.height == 2 and result.rejected.height == 0
    blang = result.valid.filter(pl.col("reservoir_name") == "BLANG")
    assert blang["planning_code"].to_list() == [None]


def register() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "reservoir_id": ["GRAVER", "JEUITP", "BLANGX"],
            "name": ["A. VERMELHA", "ITAPEBI", "BLANG"],
            "planning_code": [18, 154, None],
            "subsystem_id": ["SE", "NE", "S"],
        },
        schema_overrides={"planning_code": pl.Int32},
    )


def test_reservoir_daily_joins_ear_ena_and_hydrological_data_through_the_register():
    day = date(2026, 9, 1)
    storage = pl.DataFrame(
        {
            "planning_code": [18, None],
            "reservoir_name": ["A. VERMELHA", "Blang"],  # the second has no code: joined by name
            "day": [day, day],
            "storage_mw_month": [1200.0, 10.0],
            "storage_max_mw_month": [4000.0, 9.0],
            "storage_pct": [30.0, 111.0],
        },
        schema_overrides={"planning_code": pl.Int32},
    )
    inflow = pl.DataFrame(
        {
            "planning_code": [18],
            "reservoir_name": ["A. VERMELHA"],
            "day": [day],
            "ena_gross_mw_avg": [100.0],
            "ena_gross_pct_mlt": [80.0],
            "ena_storable_mw_avg": [90.0],
            "ena_storable_pct_mlt": [70.0],
        },
        schema_overrides={"planning_code": pl.Int32},
    )
    operations = pl.DataFrame(
        {
            "reservoir_id": ["GRAVER", "UNKNOWN"],
            "day": [day, day],
            "useful_volume_pct": [30.0, 1.0],
            "inflow_m3s": [700.0, 1.0],
            "natural_inflow_m3s": [650.0, 1.0],
            "outflow_m3s": [600.0, 1.0],
            "turbined_m3s": [500.0, 1.0],
            "spilled_m3s": [0.0, 1.0],
            "upstream_level_m": [385.2, 1.0],
            "downstream_level_m": [None, None],
        },
        schema_overrides={"downstream_level_m": pl.Float64},
    )

    daily, unmapped = hydrology.build_reservoir_daily(register(), storage, inflow, operations)

    assert daily is not None and unmapped == 1  # the reservoir that is not in the register
    assert daily.columns == hydrology.RESERVOIR_DAILY_COLUMNS
    row = daily.filter(pl.col("reservoir_id") == "GRAVER").row(0, named=True)
    assert row["storage_pct"] == 30.0 and row["ena_storable_pct_mlt"] == 70.0
    assert row["inflow_m3s"] == 700.0
    assert daily.filter(pl.col("reservoir_id") == "BLANGX")["storage_pct"].to_list() == [111.0]
    assert daily.height == 2  # "UNKNOWN" is left out


def test_subsystem_storage_gold_joins_ear_and_ena():
    inflow = pl.DataFrame(
        {
            "subsystem_id": ["SE"],
            "day": [date(2026, 9, 1)],
            "ena_gross_mw_avg": [30000.0],
            "ena_gross_pct_mlt": [75.0],
            "ena_storable_mw_avg": [29000.0],
            "ena_storable_pct_mlt": [74.0],
        }
    )

    table = hydrology.build_subsystem_storage(storage_frame(days=1), inflow)

    assert table is not None and table.height == 4
    southeast = table.filter(pl.col("subsystem") == "SE").row(0, named=True)
    assert southeast["storage_pct"] == 50.0 and southeast["ena_gross_mw_avg"] == 30000.0
    assert hydrology.build_subsystem_storage(None, None) is None


def test_reservoir_register_gets_slugs():
    nothing = {
        name: pl.lit(None, dtype=pl.Float64)
        for name in (
            "max_level_m",
            "min_level_m",
            "max_volume_hm3",
            "min_volume_hm3",
            "useful_volume_hm3",
            "latitude",
            "longitude",
        )
    }
    silver = register().with_columns(
        pl.lit("FIO").alias("reservoir_type"),
        pl.lit(None, dtype=pl.Int32).alias("station_code"),
        pl.lit("USINA").alias("plant_name"),
        pl.lit(None, dtype=pl.String).alias("ceg"),
        pl.lit(None, dtype=pl.String).alias("ree"),
        pl.lit("BACIA").alias("basin"),
        pl.lit("RIO").alias("river"),
        pl.lit(None, dtype=pl.Date).alias("commissioned_on"),
        *[expression.alias(name) for name, expression in nothing.items()],
    )

    table = hydrology.build_reservoirs(silver)

    assert table.filter(pl.col("id") == "GRAVER")["slug"].to_list() == ["a-vermelha-graver"]


def test_spilled_energy_monthly_sums_hours():
    moments = [datetime(2026, 9, 1) + timedelta(hours=h) for h in range(3)]  # noqa: DTZ001
    silver = pl.DataFrame(
        {
            "measured_at": moments,
            "plant_code": ["99"] * 3,
            "plant_name": ["14 DE JULHO"] * 3,
            "subsystem_id": ["S"] * 3,
            "basin": ["ANTAS"] * 3,
            "river": ["TAQUARI-ANTAS"] * 3,
            "agent": ["CERAN"] * 3,
            "spilled_energy_mwh": [100.0, 200.0, 300.0],
            "spilled_turbinable_energy_mwh": [10.0, 20.0, 30.0],
        }
    )

    row = hydrology.build_spilled_monthly(SEPTEMBER, silver).row(0, named=True)

    assert row["spilled_energy_mwh"] == 600.0 and row["spilled_turbinable_energy_mwh"] == 60.0
    assert row["month"] == date(2026, 9, 1)


def test_the_hydro_operations_transform_reads_the_real_layout():
    result = transform.transform_hydro_operations(
        raw_frame(
            "id_subsistema;id_reservatorio;din_instante;val_volumeutilcon;val_vazaoafluente;"
            "val_vazaonatural;val_vazaodefluente;val_vazaoturbinada;val_vazaovertida;"
            "val_nivelmontante;val_niveljusante",
            "NE;JEUITP;2026-09-01 00:00:00;35;71.81;48.85;56;18;38;109.35;",
            "NE;JEUITP;2026-10-01 00:00:00;35;71.81;48.85;56;18;38;109.35;",
        ),
        SEPTEMBER,
    )

    assert result.valid.height == 1
    row = result.valid.row(0, named=True)
    assert row["day"] == date(2026, 9, 1) and row["spilled_m3s"] == 38.0
    assert row["downstream_level_m"] is None
    assert "reservoir_inflow" in schema.SCHEMAS and callable(steps.run_publish_hydrology)
