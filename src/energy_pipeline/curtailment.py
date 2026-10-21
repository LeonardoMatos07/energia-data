"""Curtailment model: official cut per cluster, split by reason, priced and allocated to plants.

ONS publishes curtailment at two levels, both semi-hourly and in average MW:

* **Cluster** ("conjunto de usinas", or a single plant dispatched on its own):
  the official *Geracao Nao Realizada Apurada* (GNRa) and how many minutes of
  each half-hour were restricted for each reason.
* **Plant**: estimated (potential) and verified generation, plus a flag for
  restricted half-hours, but no official cut and no reason.

The official GNRa is the source of truth. It is split by reason in proportion
to the restricted minutes, then allocated to the plants of the cluster in
proportion to each plant's own estimated loss. Plant totals therefore add up to
the official ONS figures by construction.

Reasons (cod_razaorestricao):
    REL  external unavailability (transmission); eligible for compensation (ESS)
    CNF  reliability requirements of the grid
    ENE  energy reasons: supply exceeds what the grid can absorb
    other  any other reason or unclassified minutes (e.g. PAR, access opinion)
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from energy_pipeline.period import Period

HALF_HOUR = 0.5  # ONS values are average MW over 30 minutes; x 0.5 h = MWh
REASONS = ("rel", "cnf", "ene", "other")
PLD_LIMITS_FILE = Path(__file__).parent / "seeds" / "pld_limits.csv"

_KEYS = ["cluster_id", "measured_at"]


def load_pld_limits(path: Path = PLD_LIMITS_FILE) -> pl.DataFrame:
    return pl.read_csv(
        path,
        comment_prefix="#",
        schema={
            "year": pl.Int32,
            "pld_min": pl.Float64,
            "pld_max_hourly": pl.Float64,
            "reference": pl.String,
        },
    )


def price_series(marginal_cost: pl.DataFrame, limits: pl.DataFrame, period: Period) -> pl.DataFrame:
    """Reference price per subsystem and half-hour of ``period``.

    The hourly PLD is the marginal operating cost (CMO) bounded by the floor and
    ceiling set yearly by ANEEL. When the limits of a year are not configured,
    the unbounded CMO is used. ONS occasionally skips whole days of CMO; those
    half-hours take the value of the same half-hour on the nearest previous day.
    ``price_basis`` records both choices, e.g. ``PLD`` or ``CMO_FILLED``.
    """
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
    """Add energy (MWh), the official cut split by reason, its price and the loss in R$."""
    minutes = {reason: pl.col(f"minutes_{reason}").fill_null(0) for reason in ("rel", "cnf", "ene")}
    minutes["other"] = (pl.col("minutes_restricted").fill_null(0) - sum(minutes.values())).clip(
        lower_bound=0
    )
    total_minutes = sum(minutes.values())
    # Without minutes, the whole half-hour goes to the reported reason code.
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
    """Allocate each cluster's official cut to its plants.

    Weights, in order of preference within a cluster and half-hour:
    1. the plant's own estimated loss (estimated - verified, on restricted half-hours);
    2. the plant's estimated generation;
    3. an equal split.
    """
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
    )
    return (
        df.with_columns(
            (estimated * HALF_HOUR).alias("estimated_mwh"),
            (pl.col("verified_mw_avg").fill_null(0) * HALF_HOUR).alias("verified_mwh"),
            (pl.col("_loss_weight") * HALF_HOUR).alias("naive_curtailed_mwh"),
            *[(pl.col(c).fill_null(0) * share).alias(c) for c in cluster_columns],
        )
        .with_columns(
            (pl.col("curtailed_mwh") * pl.col("price_brl_mwh")).alias("loss_brl"),
        )
        .drop("_loss_weight", "_estimated_weight", "_loss_total", "_estimated_total", "_plants")
    )


def plant_daily(allocated: pl.DataFrame) -> pl.DataFrame:
    """Daily totals per plant, the grain stored in the warehouse."""
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
    """Compare the plant allocation with the official cluster totals."""
    official = clusters["curtailed_mwh"].sum()
    covered = clusters.join(allocated.select(_KEYS).unique(), on=_KEYS, how="semi")
    return {
        "official_mwh": official,
        "allocated_mwh": allocated["curtailed_mwh"].sum(),
        "unallocated_mwh": official - covered["curtailed_mwh"].sum(),
        "naive_mwh": allocated["naive_curtailed_mwh"].sum(),
    }
