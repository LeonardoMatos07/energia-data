from datetime import date

import polars as pl

from energy_pipeline import grid_assets, quality, schema, transform
from energy_pipeline.period import Period
from energy_pipeline.sources import GRID_ASSET_SOURCES, PIPELINES, SOURCES
from tests.conftest import raw_frame

SNAPSHOT = date(2026, 10, 1)


def test_grid_asset_sources_are_snapshots_in_their_own_pipeline():
    assert set(PIPELINES["grid_assets"]) == set(GRID_ASSET_SOURCES)
    for source in GRID_ASSET_SOURCES.values():
        assert source.partitioning.value == "snapshot"
        assert source.file_name(Period(2026, 10)).endswith(".parquet")
    assert SOURCES["transmission_lines"].file_name(Period(2026, 10)) == "LINHA_TRANSMISSAO.parquet"


def raw_lines() -> pl.DataFrame:
    columns = list(schema.SCHEMAS["transmission_lines"])
    header = ";".join(columns)

    def line(code: str, kv: str, **values: str) -> str:
        cells = {column: "" for column in columns}
        cells |= {
            "cod_equipamento": code,
            "nom_linhadetransmissao": f"LT {kv} kV X / Y",
            "val_niveltensao_kv": kv,
            "id_subsistema_terminalde": "NE",
            "id_subsistema_terminalpara": "SE",
            "id_estado_terminalde": "BA",
            "id_estado_terminalpara": "MG",
            "val_comprimento": "280,5",
            "dat_entradaoperacao": "2024-12-16",
            "num_barra_de": "6990",
        } | values
        return ";".join(cells[c] for c in columns)

    return raw_frame(
        header,
        line("ACCS--2FJ--1        ", "230.0"),
        line("ACCS--2FJ--1", "230.0"),  # duplicate once the code is trimmed
        line(
            "MGBA--5XX--1", "500.0", id_subsistema_terminalpara="AR", dat_desativacao="2025-01-01"
        ),
        line("", "230"),
    )


def test_transform_transmission_lines():
    result = transform.transform_transmission_lines(raw_lines(), SNAPSHOT)

    assert result.valid.height == 2 and result.duplicates_removed == 1
    assert result.rejected.height == 1
    first = result.valid.row(0, named=True)
    assert first["equipment_code"] == "ACCS--2FJ--1"
    assert first["length_km"] == 280.5 and first["from_bus"] == 6990
    assert first["commissioned_on"] == date(2024, 12, 16)
    assert result.valid.filter(pl.col("equipment_code") == "MGBA--5XX--1")[
        "to_subsystem"
    ].to_list() == ["AR"]


def test_line_gates():
    silver = transform.transform_transmission_lines(raw_lines(), SNAPSHOT).valid

    assert all(r.passed for r in quality.transmission_lines_checks(silver, 0, 0.01))
    absurd = silver.with_columns(pl.lit(9_000.0).alias("voltage_kv"))
    assert any(not r.passed for r in quality.transmission_lines_checks(absurd, 0, 0.01))


def test_substations_transform_and_coordinate_warning():
    raw = raw_frame(
        "id_subsistema;nom_subsistema;id_estado;nom_estado;nom_agente_principal;id_subestacao;"
        "nom_subestacao;val_niveltensao;id_estacao;num_barra;val_latitude;val_longitude",
        "N;Norte;AM;Amazonas;AZULAO;AMAZL;AZULAO;500.0;AMAZL-5A;44932;-2.7147;-58.1944",
        "SE;Sudeste;SP;Sao Paulo;AGENT;SPSC;SANTA CRUZ;230.0;SPSC-2A;1;;",
        "SE;Sudeste;SP;Sao Paulo;AGENT;SPXX;FAR AWAY;230.0;SPXX-2A;2;45.0;-58.0",
    )

    silver = transform.transform_substations(raw, SNAPSHOT).valid

    assert silver.height == 3
    results = quality.substations_checks(silver, 0, 0.01)
    assert all(r.passed or r.severity is quality.Severity.WARN for r in results)
    assert any(not r.passed and "latitude" in r.name for r in results)  # the 45 degrees one
    tables = grid_assets.build_grid_asset_tables(
        SNAPSHOT, lines=None, substations=silver, transformers=None
    )
    assert tables.warnings == ["1 substations without coordinates in the ONS file"]


def test_transformers_transform():
    raw = raw_frame(
        "id_subsistema;id_estado;nom_tipotransformador;nom_agenteproprietario;nom_subestacao;"
        "nom_transformador;cod_equipamento;dat_entradaoperacao;dat_desativacao;val_tensaoprimario_kv;"
        "val_tensaosecundario_kv;val_tensaoterciario_kv;val_potencianominal_mva;nom_tipoderede;"
        "num_barra_primario;num_barra_secundario",
        "N;AM;TRANSFORMADOR DE 3 TERMINAIS;ENERGISA;JORG.TEIXEIRA;TR 230 / 138;AMJT--2TR1;"
        "2014-05-27;;230.0;138.0;13.8;150.0;BASICA;8511;8512",
    )

    silver = transform.transform_transformation_capacity(raw, SNAPSHOT).valid

    row = silver.row(0, named=True)
    assert row["rated_mva"] == 150.0 and row["tertiary_kv"] == 13.8
    assert row["decommissioned_on"] is None
    assert all(r.passed for r in quality.transformation_capacity_checks(silver, 0, 0.01))


def test_the_row_hash_changes_exactly_with_the_attributes():
    silver = transform.transform_transmission_lines(raw_lines(), SNAPSHOT).valid

    first = grid_assets.build_lines(silver, SNAPSHOT)
    again = grid_assets.build_lines(silver, date(2026, 11, 1))
    assert first["row_hash"].to_list() == again["row_hash"].to_list()  # same data, same hash
    assert set(again["valid_from"]) == {date(2026, 11, 1)}

    changed = silver.with_columns(
        pl.when(pl.col("equipment_code") == "ACCS--2FJ--1")
        .then(300.0)
        .otherwise(pl.col("length_km"))
        .alias("length_km")
    )
    after = grid_assets.build_lines(changed, SNAPSHOT)
    pairs = dict(
        zip(
            first["equipment_code"],
            zip(first["row_hash"], after["row_hash"], strict=True),
            strict=True,
        )
    )
    assert pairs["ACCS--2FJ--1"][0] != pairs["ACCS--2FJ--1"][1]
    assert pairs["MGBA--5XX--1"][0] == pairs["MGBA--5XX--1"][1]
    assert first.columns[:3] == ["equipment_code", "valid_from", "row_hash"]
