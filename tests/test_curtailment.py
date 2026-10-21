from datetime import datetime, timedelta

import polars as pl
import pytest

from energy_pipeline import curtailment
from energy_pipeline.period import Period
from energy_pipeline.transform import (
    read_raw_parquet,
    transform_cluster_curtailment,
    transform_plant_curtailment,
)
from tests.conftest import raw_frame

SEPTEMBER = Period(2026, 9)
T0 = datetime(2026, 9, 1, 12, 0)
T1 = T0 + timedelta(minutes=30)
NO_LIMITS = pl.DataFrame(
    schema={"year": pl.Int32, "pld_min": pl.Float64, "pld_max_hourly": pl.Float64}
)


def cluster_row(**values) -> dict:
    row = {
        "cluster_id": "CJU_A",
        "measured_at": T0,
        "subsystem_id": "NE",
        "generation_mw_avg": 100.0,
        "gnra_mw_avg": 60.0,
        "reason_code": "ENE",
        "minutes_rel": 0,
        "minutes_cnf": 0,
        "minutes_ene": 30,
        "minutes_restricted": 30,
    }
    return row | values


def clusters_frame(*rows: dict) -> pl.DataFrame:
    """Typed like the silver table, even when a column only holds nulls."""
    return pl.DataFrame(
        list(rows),
        schema_overrides={
            "reason_code": pl.String,
            "gnra_mw_avg": pl.Float64,
            **{f"minutes_{name}": pl.Int32 for name in ("rel", "cnf", "ene", "restricted")},
        },
    )


def prices(value: float = 100.0) -> pl.DataFrame:
    return pl.DataFrame(
        {"subsystem_id": ["NE", "NE"], "measured_at": [T0, T1], "price_brl_mwh": [value] * 2}
    ).with_columns(pl.lit("CMO").alias("price_basis"))


class TestPriceSeries:
    def cmo(self, days: list[int], value: float = 50.0) -> pl.DataFrame:
        times = [
            SEPTEMBER.start + timedelta(days=day, minutes=30 * slot)
            for day in days
            for slot in range(48)
        ]
        return pl.DataFrame(
            {"subsystem_id": ["SE"] * len(times), "measured_at": times, "cmo_brl_mwh": value}
        )

    def test_fills_missing_days_from_the_previous_day(self):
        days = [day for day in range(30) if day != 4]  # 2026-09-05 missing, like ONS
        series = curtailment.price_series(self.cmo(days), NO_LIMITS, SEPTEMBER)

        assert series.height == SEPTEMBER.half_hours
        assert series["price_brl_mwh"].null_count() == 0
        filled = series.filter(pl.col("price_basis") == "CMO_FILLED")
        assert filled.height == 48
        assert filled["measured_at"].dt.day().unique().to_list() == [5]

    def test_bounds_cmo_with_pld_limits(self):
        limits = pl.DataFrame(
            {"year": [2026], "pld_min": [60.0], "pld_max_hourly": [1500.0]},
            schema_overrides={"year": pl.Int32},
        )
        series = curtailment.price_series(self.cmo(list(range(30)), 0.0), limits, SEPTEMBER)

        assert series["price_brl_mwh"].unique().to_list() == [60.0]
        assert series["price_basis"].unique().to_list() == ["PLD"]


class TestEnrichClusters:
    def test_converts_to_energy_and_prices_the_cut(self):
        df = curtailment.enrich_clusters(clusters_frame(cluster_row()), prices(100.0))
        row = df.row(0, named=True)

        assert row["generation_mwh"] == 50.0  # 100 MW over half an hour
        assert row["curtailed_mwh"] == 30.0
        assert row["curtailed_mwh_ene"] == 30.0
        assert row["loss_brl"] == 3000.0

    def test_splits_the_cut_by_restricted_minutes(self):
        row = cluster_row(minutes_rel=10, minutes_cnf=5, minutes_ene=10, minutes_restricted=30)
        result = curtailment.enrich_clusters(clusters_frame(row), prices()).row(0, named=True)

        assert result["curtailed_mwh_rel"] == pytest.approx(10.0)
        assert result["curtailed_mwh_cnf"] == pytest.approx(5.0)
        assert result["curtailed_mwh_ene"] == pytest.approx(10.0)
        assert result["curtailed_mwh_other"] == pytest.approx(5.0)  # 5 unclassified minutes

    def test_falls_back_to_reason_code_without_minutes(self):
        row = cluster_row(
            reason_code="REL",
            minutes_rel=None,
            minutes_cnf=None,
            minutes_ene=None,
            minutes_restricted=None,
        )
        result = curtailment.enrich_clusters(clusters_frame(row), prices()).row(0, named=True)

        assert result["curtailed_mwh_rel"] == 30.0
        assert result["curtailed_mwh_ene"] == 0.0

    def test_no_restriction_means_no_cut(self):
        row = cluster_row(gnra_mw_avg=None, reason_code=None, minutes_restricted=None)
        result = curtailment.enrich_clusters(clusters_frame(row), prices()).row(0, named=True)

        assert result["curtailed_mwh"] == 0.0
        assert all(result[f"curtailed_mwh_{r}"] == 0.0 for r in curtailment.REASONS)


class TestAllocation:
    def clusters(self) -> pl.DataFrame:
        rows = [cluster_row(measured_at=T0), cluster_row(measured_at=T1)]
        return curtailment.enrich_clusters(clusters_frame(*rows), prices())

    def plants(self, rows: list[tuple]) -> pl.DataFrame:
        columns = [
            "plant_id",
            "measured_at",
            "estimated_mw_avg",
            "verified_mw_avg",
            "is_restricted",
        ]
        return pl.DataFrame(rows, schema=columns, orient="row").with_columns(
            pl.lit("CJU_A").alias("cluster_id"), pl.lit("WIND").alias("source_type")
        )

    def test_allocates_in_proportion_to_each_plant_loss(self):
        plants = self.plants([("P1", T0, 90.0, 30.0, True), ("P2", T0, 50.0, 30.0, True)])
        allocated = curtailment.allocate_to_plants(plants, self.clusters())
        by_plant = dict(allocated.select("plant_id", "curtailed_mwh").iter_rows())

        # Losses of 60 and 20 MW share the official 30 MWh as 3:1.
        assert by_plant == {"P1": pytest.approx(22.5), "P2": pytest.approx(7.5)}

    def test_falls_back_to_estimated_generation_then_equal_split(self):
        plants = self.plants(
            [
                ("P1", T0, 30.0, 30.0, False),  # no own loss: weight by estimate
                ("P2", T0, 10.0, 10.0, False),
                ("P1", T1, 0.0, 0.0, False),  # nothing at all: equal split
                ("P2", T1, 0.0, 0.0, False),
            ]
        )
        allocated = curtailment.allocate_to_plants(plants, self.clusters())
        rows = allocated.select("plant_id", "measured_at", "curtailed_mwh").rows()
        at = {(plant, time): value for plant, time, value in rows}

        assert at[("P1", T0)] == pytest.approx(22.5)
        assert at[("P2", T0)] == pytest.approx(7.5)
        assert at[("P1", T1)] == at[("P2", T1)] == pytest.approx(15.0)

    def test_plant_totals_reconcile_with_official_figures(self):
        plants = self.plants(
            [("P1", T0, 90.0, 30.0, True), ("P2", T0, 50.0, 49.0, True), ("P1", T1, 5.0, 1.0, True)]
        )
        clusters = self.clusters()
        allocated = curtailment.allocate_to_plants(plants, clusters)
        result = curtailment.reconcile(allocated, clusters)

        assert result["allocated_mwh"] == pytest.approx(result["official_mwh"])
        assert result["unallocated_mwh"] == pytest.approx(0.0)
        assert allocated["loss_brl"].sum() == pytest.approx(result["official_mwh"] * 100.0)

    def test_daily_aggregation(self):
        plants = self.plants([("P1", T0, 90.0, 30.0, True), ("P1", T1, 90.0, 30.0, True)])
        daily = curtailment.plant_daily(curtailment.allocate_to_plants(plants, self.clusters()))

        assert daily.height == 1
        row = daily.row(0, named=True)
        assert row["curtailed_mwh"] == pytest.approx(60.0)
        assert row["restricted_half_hours"] == 2


class TestCurtailmentTransforms:
    CLUSTER_HEADER = (
        "id_subsistema;id_estado;nom_usina;id_ons;din_instante;val_geracao;"
        "val_geracaolimitada;val_disponibilidade;val_geracaoreferencia;"
        "val_geracaoreferenciafinal;cod_razaorestricao;cod_origemrestricao;dsc_restricao;"
        "id_pontoconexao;nom_pontoconexao;nom_agenteoperador;val_geracaonaorealizadaapurada;"
        "num_minutos_rel;num_minutos_cnf;num_minutos_ene;num_minutos_restricao"
    )
    PLANT_HEADER = (
        "id_subsistema;id_estado;nom_modalidadeoperacao;nom_conjuntousina;nom_usina;id_ons;ceg;"
        "din_instante;val_ventoverificado;flg_dadoventoinvalido;val_geracaoestimada;"
        "val_geracaoverificada;id_ons_conjuntousina;nom_origemvento;"
        "flg_dadoventosupervisaoinvalido;nom_origemgeracaoreferencia;"
        "val_geracaoreferenciafinal;flg_geracaorestrita"
    )

    def test_cluster_transform(self):
        raw = raw_frame(
            self.CLUSTER_HEADER,
            "NE;BA;Conj. BJL;CJU_BABJL;2026-09-01 12:00:00;10.5;0.0;80.0;70.0;;ENE;SIS;"
            "Controle de frequência do SIN.;BABJS-69--A;B.JESUS LAPA - 69 kV (A);VISUS;59.5;"
            "0;0;24;24",
            "NE;BA;Conj. BJL;CJU_BABJL;2026-09-01 12:30:00;0.0;;0.0;0.0;;;;;BABJS-69--A;"
            "B.JESUS LAPA - 69 kV (A);VISUS;;;;;",
        )
        result = transform_cluster_curtailment(raw, SEPTEMBER, "SOLAR")
        restricted = result.valid.row(0, named=True)

        assert result.valid.height == 2
        assert restricted["gnra_mw_avg"] == 59.5
        assert restricted["minutes_ene"] == 24
        assert restricted["connection_point_id"] == "BABJS-69--A"
        assert restricted["source_type"] == "SOLAR"
        assert result.valid.row(1, named=True)["gnra_mw_avg"] is None

    def test_plant_transform_handles_plants_without_cluster_and_duplicates(self):
        raw = raw_frame(
            self.PLANT_HEADER,
            # Individually dispatched plant: it is its own cluster.
            "NE;CE;Tipo I;;Icaraizinho;CEUCZ;EOL.CV.CE.028699-0.01;2026-09-01 12:00:00;8.1;0;"
            "20.0;15.0;;Supervisão;0;Curva;;1",
            # Same plant and half-hour twice: without and with its new cluster.
            "NE;RN;Tipo II-C;;São Fernando 4;RNESF4;EOL.CV.RN.037077-0.01;2026-09-01 12:00:00;"
            "9.0;0;11.31;8.948;;Supervisão;0;Curva;;0",
            "NE;RN;Tipo II-C;Conj. Monte do Ronca;São Fernando 4;RNESF4;EOL.CV.RN.037077-0.01;"
            "2026-09-01 12:00:00;9.0;0;11.31;8.948;CJU_RNMNR;Supervisão;0;Curva;;0",
        )
        result = transform_plant_curtailment(raw, SEPTEMBER, "WIND")
        by_plant = {row["plant_id"]: row for row in result.valid.iter_rows(named=True)}

        assert result.duplicates_removed == 1
        assert by_plant["CEUCZ"]["cluster_id"] == "CEUCZ"
        assert by_plant["CEUCZ"]["is_restricted"] is True
        assert by_plant["CEUCZ"]["resource_value"] == pytest.approx(8.1)
        assert by_plant["RNESF4"]["cluster_id"] == "CJU_RNMNR"


def test_parquet_is_read_as_text_like_csv(tmp_path):
    path = tmp_path / "sample.parquet"
    pl.DataFrame(
        {"din_instante": [datetime(2026, 9, 1, 0, 30)], "val_geracao": [21.998], "flg": [1]}
    ).write_parquet(path)

    df = read_raw_parquet(path)

    assert df.schema == {"din_instante": pl.String, "val_geracao": pl.String, "flg": pl.String}
    assert df.row(0) == ("2026-09-01 00:30:00", "21.998", "1")
