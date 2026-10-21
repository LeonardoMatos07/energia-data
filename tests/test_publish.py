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
        "cluster_name": "Cluster A",
        "state_id": "RN",
        "source_type": "WIND",
        "connection_point_name": "SE Ceará-Mirim 230 kV",
        "operator_agent": "AGENT",
    }
    return cluster_row(**(defaults | values))


def plant(plant_id: str, cluster_id: str, estimated: float, verified: float, **values) -> dict:
    row = {
        "plant_id": plant_id,
        "plant_name": f"Plant {plant_id}",
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
            cluster(cluster_id="CJU_A"),
            cluster(
                cluster_id="SOLO",
                cluster_name="Solo",
                gnra_mw_avg=0.0,
                minutes_restricted=0,
                minutes_ene=0,
                reason_code=None,
                generation_mw_avg=40.0,
            ),
            cluster(
                cluster_id="CJU_B",
                cluster_name="Cluster B",
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


def test_plant_table_has_plants_and_real_clusters_only(tables):
    plants = {row["id"]: row for row in tables.plant.iter_rows(named=True)}

    assert set(plants) == {"CJU_A", "CJU_B", "P1", "P2", "P3", "SOLO"}
    assert plants["CJU_A"]["kind"] == "cluster"
    assert plants["P1"]["cluster_id"] == "CJU_A"
    assert plants["SOLO"]["kind"] == "plant"
    assert plants["SOLO"]["cluster_id"] is None
    assert plants["P3"]["source"] == "solar"
    assert plants["CJU_A"]["connection_point"] == "SE Ceará-Mirim 230 kV"
    assert plants["P1"]["capacity_mw"] == 100.0
    assert plants["P2"]["capacity_mw"] == 30.0
    assert plants["CJU_A"]["capacity_mw"] == 130.0


def test_cluster_equals_the_sum_of_its_plants(tables):
    monthly = {row["plant_id"]: row for row in tables.plant_monthly.iter_rows(named=True)}

    assert monthly["CJU_A"]["curtailed_mwh"] == pytest.approx(30.0)
    assert monthly["P1"]["curtailed_mwh"] + monthly["P2"]["curtailed_mwh"] == pytest.approx(30.0)
    assert monthly["CJU_A"]["curtailment_pct"] == pytest.approx(37.5)
    assert monthly["CJU_A"]["estimated_loss_brl"] == pytest.approx(3000.0)


def test_ranking_and_state_average_for_plants(tables):
    monthly = {row["plant_id"]: row for row in tables.plant_monthly.iter_rows(named=True)}

    assert monthly["P1"]["national_rank"] == 1
    assert monthly["P2"]["national_rank"] == 2
    assert monthly["P3"]["national_rank"] == 1
    assert monthly["CJU_A"]["national_rank"] is None
    rn_wind = tables.state_monthly.filter((pl.col("state") == "RN") & (pl.col("source") == "wind"))
    assert monthly["P1"]["state_avg_curtailment_pct"] == rn_wind["curtailment_pct"].item()


def test_regional_totals_include_total_source_and_national_row(tables):
    subsystems = {
        (r["subsystem"], r["source"]): r for r in tables.subsystem_monthly.iter_rows(named=True)
    }
    states = {(r["state"], r["source"]): r for r in tables.state_monthly.iter_rows(named=True)}

    assert subsystems[("SIN", "total")]["curtailed_mwh"] == pytest.approx(30.0 + 10.0)
    assert subsystems[("NE", "wind")]["affected_plants"] == 2
    assert states[("RN", "total")]["verified_generation_mwh"] == pytest.approx(50.0 + 20.0)
    assert states[("RS", "solar")]["affected_plants"] == 1
    assert set(tables.state_monthly["month"].unique().to_list()) == {date(2026, 9, 1)}


def test_columns_follow_the_api_contract(tables):
    assert tables.plant_monthly.columns == [
        "plant_id",
        "month",
        "verified_generation_mwh",
        "curtailed_mwh",
        "curtailment_pct",
        "curtailed_rel_mwh",
        "curtailed_cnf_mwh",
        "curtailed_ene_mwh",
        "curtailed_other_mwh",
        "estimated_loss_brl",
        "price_basis",
        "restricted_hours",
        "state_avg_curtailment_pct",
        "national_rank",
        "state_rank",
    ]
    assert tables.subsystem_monthly.columns[:3] == ["subsystem", "source", "month"]
    assert tables.plant.columns == [
        "id",
        "kind",
        "name",
        "slug",
        "source",
        "state",
        "subsystem",
        "cluster_id",
        "ceg",
        "capacity_mw",
        "connection_point",
        "operator",
    ]
