from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import polars as pl

from energy_pipeline.curtailment import REASONS
from energy_pipeline.period import Period

SOURCE = {"WIND": "wind", "SOLAR": "solar"}
REASON_COLUMNS = {f"curtailed_mwh_{reason}": f"curtailed_{reason}_mwh" for reason in REASONS}
METRICS = [
    "verified_generation_mwh",
    "curtailed_mwh",
    *REASON_COLUMNS.values(),
    "estimated_loss_brl",
]


@dataclass(frozen=True)
class MonthlyTables:
    plant: pl.DataFrame
    plant_monthly: pl.DataFrame
    state_monthly: pl.DataFrame
    subsystem_monthly: pl.DataFrame


def slugify(text: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")


def make_slug(name: str, identifier: str) -> str:
    return f"{slugify(name)}-{slugify(identifier)}"


def _percent(
    curtailed: str = "curtailed_mwh", generated: str = "verified_generation_mwh"
) -> pl.Expr:
    potential = pl.col(curtailed) + pl.col(generated)
    return (
        pl.when(potential > 0).then(pl.col(curtailed) / potential * 100).otherwise(None).round(2)
    ).alias("curtailment_pct")


def _metric_sums(generation_column: str) -> list[pl.Expr]:
    return [
        pl.col(generation_column).sum().alias("verified_generation_mwh"),
        pl.col("curtailed_mwh").sum().alias("curtailed_mwh"),
        *[pl.col(source).sum().alias(target) for source, target in REASON_COLUMNS.items()],
        pl.col("loss_brl").sum().alias("estimated_loss_brl"),
    ]


def _price_basis() -> pl.Expr:
    return (
        pl.col("price_basis")
        .filter(pl.col("curtailed_mwh") > 0)
        .mode()
        .first()
        .alias("price_basis")
    )


def build_plants(
    clusters: pl.DataFrame, plants: pl.DataFrame, capacity: pl.DataFrame | None
) -> pl.DataFrame:
    cluster_info = clusters.unique(subset=["cluster_id"], keep="last").select(
        "cluster_id",
        "cluster_name",
        "source_type",
        "subsystem_id",
        "state_id",
        pl.col("connection_point_name").alias("connection_point"),
        pl.col("operator_agent").alias("operator"),
    )
    plant_ids = plants["plant_id"].unique()
    real_clusters = cluster_info.filter(~pl.col("cluster_id").is_in(plant_ids))

    plant_rows = (
        plants.unique(subset=["plant_id"], keep="last")
        .join(
            cluster_info.select("cluster_id", "connection_point", "operator"),
            on="cluster_id",
            how="left",
        )
        .select(
            pl.col("plant_id").alias("id"),
            pl.lit("plant").alias("kind"),
            pl.col("plant_name").alias("name"),
            pl.col("source_type"),
            pl.col("state_id").alias("state"),
            pl.col("subsystem_id").alias("subsystem"),
            pl.when(pl.col("cluster_id").is_in(real_clusters["cluster_id"]))
            .then(pl.col("cluster_id"))
            .otherwise(None)
            .alias("cluster_id"),
            "ceg",
            "connection_point",
            "operator",
        )
    )
    if capacity is not None and capacity.height:
        installed = (
            capacity.filter(pl.col("decommission_date").is_null() & pl.col("ceg").is_not_null())
            .group_by("ceg")
            .agg(pl.col("capacity_mw").sum().round(3).alias("capacity_mw"))
        )
        plant_rows = plant_rows.join(installed, on="ceg", how="left")
    else:
        plant_rows = plant_rows.with_columns(pl.lit(None, dtype=pl.Float64).alias("capacity_mw"))

    cluster_capacity = (
        plant_rows.filter(pl.col("cluster_id").is_not_null())
        .group_by("cluster_id")
        .agg(
            pl.when(pl.col("capacity_mw").count() > 0)
            .then(pl.col("capacity_mw").sum())
            .otherwise(None)
            .alias("capacity_mw")
        )
    )
    cluster_rows = real_clusters.join(cluster_capacity, on="cluster_id", how="left").select(
        pl.col("cluster_id").alias("id"),
        pl.lit("cluster").alias("kind"),
        pl.col("cluster_name").alias("name"),
        "source_type",
        pl.col("state_id").alias("state"),
        pl.col("subsystem_id").alias("subsystem"),
        pl.lit(None, dtype=pl.String).alias("cluster_id"),
        pl.lit(None, dtype=pl.String).alias("ceg"),
        "connection_point",
        "operator",
        "capacity_mw",
    )

    rows = pl.concat([cluster_rows, plant_rows.select(cluster_rows.columns)], how="vertical")
    return rows.with_columns(
        pl.col("source_type").replace_strict(SOURCE).alias("source"),
        pl.struct("name", "id")
        .map_elements(lambda row: make_slug(row["name"], row["id"]), return_dtype=pl.String)
        .alias("slug"),
    ).select(
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
    )


def _with_total_source(
    df: pl.DataFrame, keys: list[str], aggregations: list[pl.Expr]
) -> pl.DataFrame:
    by_source = df.group_by(*keys, "source").agg(aggregations)
    total = df.group_by(*keys).agg(aggregations).with_columns(pl.lit("total").alias("source"))
    return pl.concat([by_source, total.select(by_source.columns)])


def _regional(clusters: pl.DataFrame, plants: pl.DataFrame, region: str) -> pl.DataFrame:
    totals = _with_total_source(clusters, [region], _metric_sums("generation_mwh"))
    affected = _with_total_source(
        plants.filter(pl.col("curtailed_mwh") > 0),
        [region],
        [pl.len().cast(pl.Int32).alias("affected_plants")],
    )
    return totals.join(affected, on=[region, "source"], how="left").with_columns(
        pl.col("affected_plants").fill_null(0), _percent()
    )


def build_monthly_tables(
    period: Period,
    clusters: pl.DataFrame,
    plants: pl.DataFrame,
    capacity: pl.DataFrame | None,
) -> MonthlyTables:
    month = period.start.date()
    clusters = clusters.with_columns(
        pl.col("source_type").replace_strict(SOURCE).alias("source"),
        pl.col("state_id").alias("state"),
        pl.col("subsystem_id").alias("subsystem"),
    )
    plants = plants.with_columns(pl.col("source_type").replace_strict(SOURCE).alias("source"))
    plant_table = build_plants(clusters, plants, capacity)

    plant_monthly = plants.group_by(pl.col("plant_id")).agg(
        *_metric_sums("verified_mwh"),
        _price_basis(),
        (pl.col("is_restricted").sum() / 2).alias("restricted_hours"),
    )
    real_clusters = plant_table.filter(pl.col("kind") == "cluster")["id"]
    cluster_monthly = (
        clusters.filter(pl.col("cluster_id").is_in(real_clusters))
        .group_by(pl.col("cluster_id").alias("plant_id"))
        .agg(
            *_metric_sums("generation_mwh"),
            _price_basis(),
            (pl.col("minutes_restricted").fill_null(0).sum() / 60).alias("restricted_hours"),
        )
    )
    location = plant_table.select(
        pl.col("id").alias("plant_id"), "kind", "state", "subsystem", "source"
    )
    all_monthly = (
        pl.concat([cluster_monthly, plant_monthly.select(cluster_monthly.columns)])
        .join(location, on="plant_id")
        .with_columns(_percent(), pl.col("restricted_hours").round(1))
    )

    plant_totals = all_monthly.filter(pl.col("kind") == "plant")
    state_monthly = _regional(clusters, plant_totals, "state")
    national = pl.lit("SIN").alias("subsystem")
    subsystem_monthly = pl.concat(
        [
            _regional(clusters, plant_totals, "subsystem"),
            _regional(
                clusters.with_columns(national), plant_totals.with_columns(national), "subsystem"
            ),
        ]
    )

    state_average = state_monthly.select(
        "state", "source", pl.col("curtailment_pct").alias("state_avg_curtailment_pct")
    )
    is_plant = pl.col("kind") == "plant"
    rank = pl.col("curtailed_mwh").rank("min", descending=True)
    all_monthly = all_monthly.join(state_average, on=["state", "source"], how="left").with_columns(
        pl.when(is_plant).then(rank.over("kind", "source")).cast(pl.Int32).alias("national_rank"),
        pl.when(is_plant)
        .then(rank.over("kind", "state", "source"))
        .cast(pl.Int32)
        .alias("state_rank"),
    )

    def finish(df: pl.DataFrame, keys: list[str], extra: list[str]) -> pl.DataFrame:
        return df.with_columns(pl.lit(month).alias("month")).select(
            *keys, "month", *METRICS[:2], "curtailment_pct", *METRICS[2:], *extra
        )

    plant_extra = [
        "price_basis",
        "restricted_hours",
        "state_avg_curtailment_pct",
        "national_rank",
        "state_rank",
    ]
    return MonthlyTables(
        plant=plant_table.sort("kind", "id"),
        plant_monthly=finish(all_monthly, ["plant_id"], plant_extra).sort("plant_id"),
        state_monthly=finish(state_monthly, ["state", "source"], ["affected_plants"]).sort(
            "state", "source"
        ),
        subsystem_monthly=finish(
            subsystem_monthly, ["subsystem", "source"], ["affected_plants"]
        ).sort("subsystem", "source"),
    )
