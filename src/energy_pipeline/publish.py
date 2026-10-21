"""Monthly aggregates read by the portal (gold layer).

Builds, for one month, the tables defined by ``energia-api`` (the schema owner):
``usina``, ``curtailment_mensal_usina``, ``curtailment_mensal_uf`` and
``curtailment_mensal_sin``. Column names follow the API contract (Portuguese).

Clusters use the official ONS figures (GNRa); plants use the GNRa allocated to
them, so a cluster always equals the sum of its plants.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import polars as pl

from energy_pipeline.curtailment import REASONS
from energy_pipeline.period import Period

FONTE = {"WIND": "eolica", "SOLAR": "solar"}
REASON_COLUMNS = {
    f"curtailed_mwh_{reason}": f"corte_{'outros' if reason == 'other' else reason}_mwh"
    for reason in REASONS
}
METRICS = [
    "geracao_verificada_mwh",
    "energia_cortada_mwh",
    *REASON_COLUMNS.values(),
    "perda_estimada_brl",
]


@dataclass(frozen=True)
class MonthlyTables:
    usina: pl.DataFrame
    mensal_usina: pl.DataFrame
    mensal_uf: pl.DataFrame
    mensal_sin: pl.DataFrame


def slugify(text: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")


def make_slug(name: str, identifier: str) -> str:
    """Stable slug: the ONS id keeps it unique even when names repeat."""
    return f"{slugify(name)}-{slugify(identifier)}"


def _percent(
    cut: str = "energia_cortada_mwh", generated: str = "geracao_verificada_mwh"
) -> pl.Expr:
    potential = pl.col(cut) + pl.col(generated)
    return (
        pl.when(potential > 0).then(pl.col(cut) / potential * 100).otherwise(None).round(2)
    ).alias("percentual_corte")


def _metric_sums(generation_column: str) -> list[pl.Expr]:
    return [
        pl.col(generation_column).sum().alias("geracao_verificada_mwh"),
        pl.col("curtailed_mwh").sum().alias("energia_cortada_mwh"),
        *[pl.col(source).sum().alias(target) for source, target in REASON_COLUMNS.items()],
        pl.col("loss_brl").sum().alias("perda_estimada_brl"),
    ]


def _price_basis() -> pl.Expr:
    """Most frequent price basis among curtailed half-hours."""
    return (
        pl.col("price_basis").filter(pl.col("curtailed_mwh") > 0).mode().first().alias("base_preco")
    )


def build_usina(
    clusters: pl.DataFrame, plants: pl.DataFrame, capacity: pl.DataFrame | None
) -> pl.DataFrame:
    """Plants and clusters seen in the month, in the shape of the ``usina`` table."""
    cluster_info = clusters.unique(subset=["cluster_id"], keep="last").select(
        "cluster_id",
        "cluster_name",
        "source_type",
        "subsystem_id",
        "state_id",
        pl.col("connection_point_name").alias("ponto_conexao"),
        pl.col("operator_agent").alias("agente_operador"),
    )
    plant_ids = plants["plant_id"].unique()
    real_clusters = cluster_info.filter(~pl.col("cluster_id").is_in(plant_ids))

    plant_rows = (
        plants.unique(subset=["plant_id"], keep="last")
        .join(
            cluster_info.select("cluster_id", "ponto_conexao", "agente_operador"),
            on="cluster_id",
            how="left",
        )
        .select(
            pl.col("plant_id").alias("id"),
            pl.lit("usina").alias("tipo"),
            pl.col("plant_name").alias("nome"),
            pl.col("source_type"),
            pl.col("state_id").alias("uf"),
            pl.col("subsystem_id").alias("subsistema"),
            # Only real clusters are pages of their own (and valid foreign keys).
            pl.when(pl.col("cluster_id").is_in(real_clusters["cluster_id"]))
            .then(pl.col("cluster_id"))
            .otherwise(None)
            .alias("conjunto_id"),
            "ceg",
            "ponto_conexao",
            "agente_operador",
        )
    )
    if capacity is not None and capacity.height:
        installed = (
            capacity.filter(pl.col("decommission_date").is_null() & pl.col("ceg").is_not_null())
            .group_by("ceg")
            .agg(pl.col("capacity_mw").sum().round(3).alias("potencia_mw"))
        )
        plant_rows = plant_rows.join(installed, on="ceg", how="left")
    else:
        plant_rows = plant_rows.with_columns(pl.lit(None, dtype=pl.Float64).alias("potencia_mw"))

    cluster_capacity = (
        plant_rows.filter(pl.col("conjunto_id").is_not_null())
        .group_by("conjunto_id")
        .agg(
            # Null when none of the plants has a known capacity.
            pl.when(pl.col("potencia_mw").count() > 0)
            .then(pl.col("potencia_mw").sum())
            .otherwise(None)
            .alias("potencia_mw")
        )
    )
    cluster_rows = real_clusters.join(
        cluster_capacity, left_on="cluster_id", right_on="conjunto_id", how="left"
    ).select(
        pl.col("cluster_id").alias("id"),
        pl.lit("conjunto").alias("tipo"),
        pl.col("cluster_name").alias("nome"),
        "source_type",
        pl.col("state_id").alias("uf"),
        pl.col("subsystem_id").alias("subsistema"),
        pl.lit(None, dtype=pl.String).alias("conjunto_id"),
        pl.lit(None, dtype=pl.String).alias("ceg"),
        "ponto_conexao",
        "agente_operador",
        "potencia_mw",
    )

    usina = pl.concat([cluster_rows, plant_rows.select(cluster_rows.columns)], how="vertical")
    return usina.with_columns(
        pl.col("source_type").replace_strict(FONTE).alias("fonte"),
        pl.struct("nome", "id")
        .map_elements(lambda row: make_slug(row["nome"], row["id"]), return_dtype=pl.String)
        .alias("slug"),
    ).select(
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
    )


def _with_total_source(
    df: pl.DataFrame, keys: list[str], aggregations: list[pl.Expr]
) -> pl.DataFrame:
    """Aggregate per ``keys`` and source, plus one row per ``keys`` with ``fonte = 'total'``."""
    by_source = df.group_by(*keys, "fonte").agg(aggregations)
    total = df.group_by(*keys).agg(aggregations).with_columns(pl.lit("total").alias("fonte"))
    return pl.concat([by_source, total.select(by_source.columns)])


def _regional(clusters: pl.DataFrame, plants: pl.DataFrame, region: str) -> pl.DataFrame:
    """Official totals per region (``uf`` or ``subsistema``) and source."""
    totals = _with_total_source(clusters, [region], _metric_sums("generation_mwh"))
    affected = _with_total_source(
        plants.filter(pl.col("energia_cortada_mwh") > 0),
        [region],
        [pl.len().cast(pl.Int32).alias("usinas_afetadas")],
    )
    return totals.join(affected, on=[region, "fonte"], how="left").with_columns(
        pl.col("usinas_afetadas").fill_null(0), _percent()
    )


def build_monthly_tables(
    period: Period,
    clusters: pl.DataFrame,
    plants: pl.DataFrame,
    capacity: pl.DataFrame | None,
) -> MonthlyTables:
    """``clusters`` and ``plants`` hold both technologies (column ``source_type``)."""
    mes = period.start.date()
    clusters = clusters.with_columns(
        pl.col("source_type").replace_strict(FONTE).alias("fonte"),
        pl.col("state_id").alias("uf"),
        pl.col("subsystem_id").alias("subsistema"),
    )
    plants = plants.with_columns(pl.col("source_type").replace_strict(FONTE).alias("fonte"))
    usina = build_usina(clusters, plants, capacity)

    plant_monthly = plants.group_by(pl.col("plant_id").alias("usina_id")).agg(
        *_metric_sums("verified_mwh"),
        _price_basis(),
        (pl.col("is_restricted").sum() / 2).alias("horas_restritas"),
    )
    real_clusters = usina.filter(pl.col("tipo") == "conjunto")["id"]
    cluster_monthly = (
        clusters.filter(pl.col("cluster_id").is_in(real_clusters))
        .group_by(pl.col("cluster_id").alias("usina_id"))
        .agg(
            *_metric_sums("generation_mwh"),
            _price_basis(),
            (pl.col("minutes_restricted").fill_null(0).sum() / 60).alias("horas_restritas"),
        )
    )
    location = usina.select(pl.col("id").alias("usina_id"), "tipo", "uf", "subsistema", "fonte")
    mensal_usina = (
        pl.concat([cluster_monthly, plant_monthly.select(cluster_monthly.columns)])
        .join(location, on="usina_id")
        .with_columns(_percent(), pl.col("horas_restritas").round(1))
    )

    plant_totals = mensal_usina.filter(pl.col("tipo") == "usina")
    mensal_uf = _regional(clusters, plant_totals, "uf")
    national = pl.lit("SIN").alias("subsistema")
    mensal_sin = pl.concat(
        [
            _regional(clusters, plant_totals, "subsistema"),
            _regional(
                clusters.with_columns(national), plant_totals.with_columns(national), "subsistema"
            ),
        ]
    )

    # Ranking and state average, for plants only.
    state_average = mensal_uf.select(
        "uf", "fonte", pl.col("percentual_corte").alias("percentual_corte_medio_uf")
    )
    is_plant = pl.col("tipo") == "usina"
    rank = pl.col("energia_cortada_mwh").rank("min", descending=True)
    mensal_usina = mensal_usina.join(state_average, on=["uf", "fonte"], how="left").with_columns(
        pl.when(is_plant).then(rank.over("tipo", "fonte")).cast(pl.Int32).alias("posicao_nacional"),
        pl.when(is_plant).then(rank.over("tipo", "uf", "fonte")).cast(pl.Int32).alias("posicao_uf"),
    )

    def finish(df: pl.DataFrame, keys: list[str], extra: list[str]) -> pl.DataFrame:
        return df.with_columns(pl.lit(mes).alias("mes")).select(
            *keys, "mes", *METRICS[:2], "percentual_corte", *METRICS[2:], *extra
        )

    plant_extra = [
        "base_preco",
        "horas_restritas",
        "percentual_corte_medio_uf",
        "posicao_nacional",
        "posicao_uf",
    ]
    return MonthlyTables(
        usina=usina.sort("tipo", "id"),
        mensal_usina=finish(mensal_usina, ["usina_id"], plant_extra).sort("usina_id"),
        mensal_uf=finish(mensal_uf, ["uf", "fonte"], ["usinas_afetadas"]).sort("uf", "fonte"),
        mensal_sin=finish(mensal_sin, ["subsistema", "fonte"], ["usinas_afetadas"]).sort(
            "subsistema", "fonte"
        ),
    )
