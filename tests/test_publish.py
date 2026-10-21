from datetime import date, datetime

import polars as pl
import pytest

from energy_pipeline import curtailment
from energy_pipeline.period import Period
from energy_pipeline.publish import build_monthly_tables, make_slug, slugify
from tests.test_curtailment import cluster_row, clusters_frame

SEPTEMBER = Period(2026, 9)
T0 = datetime(2026, 9, 1, 12, 0)


def prices() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "subsystem_id": ["NE", "S"],
            "measured_at": [T0, T0],
            "price_brl_mwh": [100.0, 50.0],
            "price_basis": ["CMO", "CMO"],
        }
    )


def cluster(**values) -> dict:
    defaults = {
        "cluster_name": "Conj. A",
        "state_id": "RN",
        "source_type": "WIND",
        "connection_point_name": "SE Ceará-Mirim 230 kV",
        "operator_agent": "AGENTE",
    }
    return cluster_row(**(defaults | values))


def plant(plant_id: str, cluster_id: str, estimated: float, verified: float, **values) -> dict:
    row = {
        "plant_id": plant_id,
        "plant_name": f"Usina {plant_id}",
        "ceg": f"EOL.{plant_id}",
        "cluster_id": cluster_id,
        "measured_at": T0,
        "estimated_mw_avg": estimated,
        "verified_mw_avg": verified,
        "is_restricted": True,
        "source_type": "WIND",
        "subsystem_id": "NE",
        "state_id": "RN",
    }
    return row | values


@pytest.fixture
def tables():
    clusters = curtailment.enrich_clusters(
        clusters_frame(
            # A real cluster with two plants: official cut 30 MWh, verified 50 MWh.
            cluster(cluster_id="CJU_A"),
            # A plant dispatched on its own: its cluster id is its own id.
            cluster(
                cluster_id="SOLO",
                cluster_name="Solo",
                gnra_mw_avg=0.0,
                minutes_restricted=0,
                minutes_ene=0,
                reason_code=None,
                generation_mw_avg=40.0,
            ),
            # Another state and source.
            cluster(
                cluster_id="CJU_B",
                cluster_name="Conj. B",
                state_id="RS",
                subsystem_id="S",
                source_type="SOLAR",
                gnra_mw_avg=20.0,
                generation_mw_avg=20.0,
            ),
        ),
        prices(),
    )
    plants = curtailment.allocate_to_plants(
        pl.DataFrame(
            [
                plant("P1", "CJU_A", 90.0, 30.0),
                plant("P2", "CJU_A", 50.0, 30.0),
                plant("SOLO", "SOLO", 40.0, 40.0, is_restricted=False),
                plant(
                    "P3", "CJU_B", 30.0, 10.0, source_type="SOLAR", subsystem_id="S", state_id="RS"
                ),
            ]
        ),
        clusters,
    )
    capacity = pl.DataFrame(
        {
            "ceg": ["EOL.P1", "EOL.P1", "EOL.P2", "EOL.P2"],
            "capacity_mw": [50.0, 50.0, 30.0, 99.0],
            "decommission_date": [None, None, None, date(2020, 1, 1)],
        }
    )
    return build_monthly_tables(SEPTEMBER, clusters, plants, capacity)


def test_slugs_are_ascii_and_stable():
    assert slugify("São Fernando 4") == "sao-fernando-4"
    assert make_slug("Conj. Monte do Ronca", "CJU_RNMNR") == "conj-monte-do-ronca-cju-rnmnr"


def test_usina_has_plants_and_real_clusters_only(tables):
    usina = {row["id"]: row for row in tables.usina.iter_rows(named=True)}

    assert set(usina) == {"CJU_A", "CJU_B", "P1", "P2", "P3", "SOLO"}
    assert usina["CJU_A"]["tipo"] == "conjunto"
    assert usina["P1"]["conjunto_id"] == "CJU_A"
    assert usina["SOLO"]["tipo"] == "usina"
    assert usina["SOLO"]["conjunto_id"] is None
    assert usina["P3"]["fonte"] == "solar"
    assert usina["CJU_A"]["ponto_conexao"] == "SE Ceará-Mirim 230 kV"
    # Decommissioned units do not count; a cluster sums its plants.
    assert usina["P1"]["potencia_mw"] == 100.0
    assert usina["P2"]["potencia_mw"] == 30.0
    assert usina["CJU_A"]["potencia_mw"] == 130.0


def test_cluster_equals_the_sum_of_its_plants(tables):
    monthly = {row["usina_id"]: row for row in tables.mensal_usina.iter_rows(named=True)}

    assert monthly["CJU_A"]["energia_cortada_mwh"] == pytest.approx(30.0)
    assert monthly["P1"]["energia_cortada_mwh"] + monthly["P2"]["energia_cortada_mwh"] == (
        pytest.approx(30.0)
    )
    assert monthly["CJU_A"]["percentual_corte"] == pytest.approx(37.5)  # 30 / (50 + 30)
    assert monthly["CJU_A"]["perda_estimada_brl"] == pytest.approx(3000.0)


def test_ranking_and_state_average_for_plants(tables):
    monthly = {row["usina_id"]: row for row in tables.mensal_usina.iter_rows(named=True)}

    assert monthly["P1"]["posicao_nacional"] == 1
    assert monthly["P2"]["posicao_nacional"] == 2
    assert monthly["P3"]["posicao_nacional"] == 1  # ranked among solar plants
    assert monthly["CJU_A"]["posicao_nacional"] is None
    rn_wind = tables.mensal_uf.filter((pl.col("uf") == "RN") & (pl.col("fonte") == "eolica"))
    assert monthly["P1"]["percentual_corte_medio_uf"] == rn_wind["percentual_corte"].item()


def test_regional_totals_include_total_source_and_national_row(tables):
    sin = {(r["subsistema"], r["fonte"]): r for r in tables.mensal_sin.iter_rows(named=True)}
    uf = {(r["uf"], r["fonte"]): r for r in tables.mensal_uf.iter_rows(named=True)}

    assert sin[("SIN", "total")]["energia_cortada_mwh"] == pytest.approx(30.0 + 10.0)
    assert sin[("NE", "eolica")]["usinas_afetadas"] == 2
    assert uf[("RN", "total")]["geracao_verificada_mwh"] == pytest.approx(50.0 + 20.0)
    assert uf[("RS", "solar")]["usinas_afetadas"] == 1
    assert set(tables.mensal_uf["mes"].unique().to_list()) == {date(2026, 9, 1)}


def test_columns_follow_the_api_contract(tables):
    assert tables.mensal_usina.columns == [
        "usina_id",
        "mes",
        "geracao_verificada_mwh",
        "energia_cortada_mwh",
        "percentual_corte",
        "corte_rel_mwh",
        "corte_cnf_mwh",
        "corte_ene_mwh",
        "corte_outros_mwh",
        "perda_estimada_brl",
        "base_preco",
        "horas_restritas",
        "percentual_corte_medio_uf",
        "posicao_nacional",
        "posicao_uf",
    ]
    assert tables.mensal_sin.columns[:3] == ["subsistema", "fonte", "mes"]
    assert tables.usina.columns == [
        "id",
        "tipo",
        "nome",
        "slug",
        "fonte",
        "uf",
        "subsistema",
        "conjunto_id",
        "ceg",
        "potencia_mw",
        "ponto_conexao",
        "agente_operador",
    ]
