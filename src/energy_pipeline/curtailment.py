from __future__ import annotations

from pathlib import Path

import polars as pl
import polars.selectors as cs

from energy_pipeline.period import Period

HALF_HOUR = 0.5
REASONS = ("rel", "cnf", "ene", "other")
PLD_LIMITS_FILE = Path(__file__).parent / "seeds" / "pld_limits.csv"

_KEYS = ["cluster_id", "measured_at"]


def load_pld_limits(path: Path = PLD_LIMITS_FILE) -> pl.DataFrame:
    return pl.read_csv(
        path,
        schema={
            "year": pl.Int32,
            "pld_min": pl.Float64,
            "pld_max_hourly": pl.Float64,
            "reference": pl.String,
        },
    )


def price_series(marginal_cost: pl.DataFrame, limits: pl.DataFrame, period: Period) -> pl.DataFrame:
    half_hours = pl.datetime_range(
        period.start, period.end, interval="30m", closed="left", eager=True, time_unit="us"
    ).alias("measured_at")
    subsystems = marginal_cost.select("subsystem_id").unique()
    df = (
        subsystems.join(half_hours.to_frame(), how="cross")
        .join(marginal_cost, on=["subsystem_id", "measured_at"], how="left")
        .sort("subsystem_id", "measured_at")
        .with_columns(
            pl.col("cmo_brl_mwh").is_null().alias("filled"),
            pl.col("measured_at").dt.time().alias("_time_of_day"),
        )
        .with_columns(
            pl.col("cmo_brl_mwh")
            .forward_fill()
            .backward_fill()
            .over("subsystem_id", "_time_of_day")
        )
    )

    df = df.with_columns(pl.col("measured_at").dt.year().cast(pl.Int32).alias("year"))
    df = df.join(limits.select("year", "pld_min", "pld_max_hourly"), on="year", how="left")
    bounded = pl.max_horizontal(
        pl.col("pld_min"), pl.min_horizontal(pl.col("cmo_brl_mwh"), pl.col("pld_max_hourly"))
    )
    has_limits = pl.col("pld_min").is_not_null() & pl.col("pld_max_hourly").is_not_null()
    basis = pl.when(has_limits).then(pl.lit("PLD")).otherwise(pl.lit("CMO"))
    return df.select(
        "subsystem_id",
        "measured_at",
        pl.when(has_limits).then(bounded).otherwise(pl.col("cmo_brl_mwh")).alias("price_brl_mwh"),
        pl.when(pl.col("filled")).then(basis + "_FILLED").otherwise(basis).alias("price_basis"),
    )


def enrich_clusters(clusters: pl.DataFrame, prices: pl.DataFrame) -> pl.DataFrame:
    minutes = {reason: pl.col(f"minutes_{reason}").fill_null(0) for reason in ("rel", "cnf", "ene")}
    minutes["other"] = (pl.col("minutes_restricted").fill_null(0) - sum(minutes.values())).clip(
        lower_bound=0
    )
    total_minutes = sum(minutes.values())
    fallback_reason = (
        pl.when(pl.col("reason_code").is_in(["REL", "CNF", "ENE"]))
        .then(pl.col("reason_code").str.to_lowercase())
        .otherwise(pl.lit("other"))
    )
    curtailed = pl.col("gnra_mw_avg").fill_null(0).clip(lower_bound=0) * HALF_HOUR

    df = clusters.with_columns(
        (pl.col("generation_mw_avg").fill_null(0) * HALF_HOUR).alias("generation_mwh"),
        curtailed.alias("curtailed_mwh"),
        *[
            (
                curtailed
                * pl.when(total_minutes > 0)
                .then(minutes[reason] / total_minutes)
                .otherwise((fallback_reason == reason).cast(pl.Float64))
            ).alias(f"curtailed_mwh_{reason}")
            for reason in REASONS
        ],
    )
    df = df.join(prices, on=["subsystem_id", "measured_at"], how="left")
    return df.with_columns((pl.col("curtailed_mwh") * pl.col("price_brl_mwh")).alias("loss_brl"))


def allocate_to_plants(plants: pl.DataFrame, clusters: pl.DataFrame) -> pl.DataFrame:
    estimated = pl.col("estimated_mw_avg").fill_null(0).clip(lower_bound=0)
    gap = (estimated - pl.col("verified_mw_avg").fill_null(0)).clip(lower_bound=0)
    df = plants.with_columns(
        pl.when(pl.col("is_restricted")).then(gap).otherwise(0.0).alias("_loss_weight"),
        estimated.alias("_estimated_weight"),
    ).with_columns(
        pl.col("_loss_weight").sum().over(_KEYS).alias("_loss_total"),
        pl.col("_estimated_weight").sum().over(_KEYS).alias("_estimated_total"),
        pl.len().over(_KEYS).alias("_plants"),
    )
    share = (
        pl.when(pl.col("_loss_total") > 0)
        .then(pl.col("_loss_weight") / pl.col("_loss_total"))
        .when(pl.col("_estimated_total") > 0)
        .then(pl.col("_estimated_weight") / pl.col("_estimated_total"))
        .otherwise(1.0 / pl.col("_plants"))
    )

    cluster_columns = ["curtailed_mwh", *(f"curtailed_mwh_{r}" for r in REASONS)]
    df = df.join(
        clusters.select(*_KEYS, *cluster_columns, "price_brl_mwh", "price_basis"),
        on=_KEYS,
        how="left",
    ).join(_standalone_plant_cuts(plants, clusters), on=["plant_id", "measured_at"], how="left")
    return (
        df.with_columns(
            (estimated * HALF_HOUR).alias("estimated_mwh"),
            (pl.col("verified_mw_avg").fill_null(0) * HALF_HOUR).alias("verified_mwh"),
            (pl.col("_loss_weight") * HALF_HOUR).alias("naive_curtailed_mwh"),
            *[
                (pl.col(c).fill_null(0) * share + pl.col(f"_own_{c}").fill_null(0)).alias(c)
                for c in cluster_columns
            ],
            pl.coalesce("price_brl_mwh", "_own_price_brl_mwh").alias("price_brl_mwh"),
            pl.coalesce("price_basis", "_own_price_basis").alias("price_basis"),
        )
        .with_columns(
            (pl.col("curtailed_mwh") * pl.col("price_brl_mwh")).alias("loss_brl"),
        )
        .drop(
            "_loss_weight",
            "_estimated_weight",
            "_loss_total",
            "_estimated_total",
            "_plants",
            cs.starts_with("_own_"),
        )
    )


def _standalone_plant_cuts(plants: pl.DataFrame, clusters: pl.DataFrame) -> pl.DataFrame:
    columns = ["curtailed_mwh", *(f"curtailed_mwh_{r}" for r in REASONS)]
    return clusters.filter(
        ~pl.col("cluster_id").is_in(plants["cluster_id"].unique())
        & pl.col("cluster_id").is_in(plants["plant_id"].unique())
    ).select(
        pl.col("cluster_id").alias("plant_id"),
        "measured_at",
        *[pl.col(c).alias(f"_own_{c}") for c in [*columns, "price_brl_mwh", "price_basis"]],
    )


def plant_daily(allocated: pl.DataFrame) -> pl.DataFrame:
    energy = [
        "estimated_mwh",
        "verified_mwh",
        "curtailed_mwh",
        *(f"curtailed_mwh_{reason}" for reason in REASONS),
    ]
    return (
        allocated.group_by("plant_id", "source_type", pl.col("measured_at").dt.date().alias("day"))
        .agg(
            *[pl.col(column).sum() for column in energy],
            pl.col("loss_brl").sum(),
            pl.col("is_restricted").sum().cast(pl.Int32).alias("restricted_half_hours"),
        )
        .sort("plant_id", "day")
    )


def reconcile(allocated: pl.DataFrame, clusters: pl.DataFrame) -> dict[str, float]:
    official = clusters["curtailed_mwh"].sum()
    allocated_keys = pl.concat(
        [
            allocated.select(_KEYS),
            allocated.select(pl.col("plant_id").alias("cluster_id"), "measured_at"),
        ]
    ).unique()
    covered = clusters.join(allocated_keys, on=_KEYS, how="semi")
    return {
        "official_mwh": official,
        "allocated_mwh": allocated["curtailed_mwh"].sum(),
        "unallocated_mwh": official - covered["curtailed_mwh"].sum(),
        "naive_mwh": allocated["naive_curtailed_mwh"].sum(),
    }
