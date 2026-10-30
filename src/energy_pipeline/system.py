"""Gold tables of the power system: hourly balance, generation by source and per plant."""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from energy_pipeline.period import Period
from energy_pipeline.publish import make_slug

GENERATION_SOURCES = ("hydro", "thermal", "wind", "solar")
MAX_CAPACITY_FACTOR = 1.05
CROSS_CHECK_TOLERANCE = 0.05  # reported only, never blocks the load
PLANT_SOURCE_OF_BALANCE = {"hydro": "HYDRO", "wind": "WIND", "solar": "SOLAR"}


@dataclass(frozen=True)
class SystemTables:
    hourly_balance: pl.DataFrame | None = None
    monthly_generation: pl.DataFrame | None = None
    power_plant: pl.DataFrame | None = None
    plant_monthly: pl.DataFrame | None = None
    warnings: list[str] = field(default_factory=list)


def _subsystems_only(balance: pl.DataFrame) -> pl.DataFrame:
    """The ONS balance has a "SIN" total; the gold layer derives it from the four subsystems."""
    return balance.filter(pl.col("subsystem_id") != "SIN")


def build_hourly_balance(balance: pl.DataFrame) -> pl.DataFrame:
    return (
        _subsystems_only(balance)
        .select(
            pl.col("measured_at").alias("hour"),
            pl.col("subsystem_id").alias("subsystem"),
            pl.col("measured_at_utc").alias("hour_utc"),
            *[f"{source}_mw_avg" for source in GENERATION_SOURCES],
            "load_mw_avg",
            "net_interchange_mw_avg",
        )
        .sort("subsystem", "hour")
    )


def build_monthly_generation(period: Period, balance: pl.DataFrame) -> pl.DataFrame:
    """Energy (MWh), average and peak power (MW) and share per subsystem and source.

    Every value is an hourly average power, so MWh equals the sum of the hourly values. The
    average is over all hours of the month; ``SIN`` sums the four subsystems hour by hour.
    """
    columns = [f"{source}_mw_avg" for source in GENERATION_SOURCES]
    balance = _subsystems_only(balance)
    national = (
        balance.group_by("measured_at")
        .agg([pl.col(column).sum() for column in columns])
        .with_columns(pl.lit("SIN").alias("subsystem_id"))
        .select("subsystem_id", "measured_at", *columns)
    )
    stacked = pl.concat([balance.select("subsystem_id", "measured_at", *columns), national])
    long = stacked.unpivot(
        columns, index=["subsystem_id", "measured_at"], variable_name="source", value_name="mw"
    ).with_columns(pl.col("source").str.replace("_mw_avg", ""))
    totals = (
        long.group_by("subsystem_id", "source")
        .agg(
            pl.col("mw").sum().alias("energy_mwh"),
            pl.col("mw").max().alias("peak_mw"),
        )
        .with_columns((pl.col("energy_mwh") / period.hours).alias("avg_mw"))
    )
    return (
        totals.with_columns(
            (
                pl.when(pl.col("energy_mwh").sum().over("subsystem_id") > 0)
                .then(pl.col("energy_mwh") / pl.col("energy_mwh").sum().over("subsystem_id") * 100)
                .otherwise(None)
                .round(2)
            ).alias("share_pct"),
            pl.lit(period.start.date()).alias("month"),
        )
        .with_columns(pl.col("energy_mwh", "avg_mw", "peak_mw").round(3))
        .select(
            "month",
            pl.col("subsystem_id").alias("subsystem"),
            "source",
            "energy_mwh",
            "avg_mw",
            "peak_mw",
            "share_pct",
        )
        .sort("subsystem", "source")
    )


def _installed_capacity(capacity: pl.DataFrame | None) -> pl.DataFrame:
    if capacity is None or capacity.height == 0:
        return pl.DataFrame(schema={"ceg": pl.String, "capacity_mw": pl.Float64})
    return (
        capacity.filter(pl.col("decommission_date").is_null() & pl.col("ceg").is_not_null())
        .group_by("ceg")
        .agg(pl.col("capacity_mw").sum().round(3).alias("capacity_mw"))
    )


def build_power_plants(plants: pl.DataFrame, capacity: pl.DataFrame | None) -> pl.DataFrame:
    return (
        plants.sort("measured_at")
        .unique(subset=["plant_id"], keep="last")
        .join(_installed_capacity(capacity), on="ceg", how="left")
        .select(
            pl.col("plant_id").alias("id"),
            pl.col("plant_name").alias("name"),
            pl.struct("plant_name", "plant_id")
            .map_elements(
                lambda row: make_slug(row["plant_name"], row["plant_id"]), return_dtype=pl.String
            )
            .alias("slug"),
            pl.col("source_type").str.to_lowercase().alias("source_type"),
            "fuel_type",
            pl.when(pl.col("state_id").str.len_chars() == 2)
            .then(pl.col("state_id").str.to_uppercase())
            .otherwise(None)
            .alias("state"),
            pl.col("subsystem_id").alias("subsystem"),
            "ceg",
            "capacity_mw",
        )
        .sort("id")
    )


def build_plant_monthly(
    period: Period, plants: pl.DataFrame, power_plant: pl.DataFrame
) -> tuple[pl.DataFrame, int]:
    """Monthly generation per plant and the number of capacity factors above the plausible cap."""
    monthly = (
        plants.group_by("plant_id")
        .agg(
            pl.col("generation_mwh").sum().alias("energy_mwh"),
            pl.col("generation_mwh").max().alias("peak_mw"),
        )
        .join(power_plant.select(pl.col("id").alias("plant_id"), "capacity_mw"), on="plant_id")
        .with_columns(
            (pl.col("energy_mwh") / period.hours).alias("avg_mw"),
            pl.when(pl.col("capacity_mw") > 0)
            .then(pl.col("energy_mwh") / (pl.col("capacity_mw") * period.hours))
            .otherwise(None)
            .alias("capacity_factor"),
        )
    )
    implausible = monthly.filter(pl.col("capacity_factor") > MAX_CAPACITY_FACTOR).height
    monthly = monthly.with_columns(
        pl.when(pl.col("capacity_factor").is_between(0, MAX_CAPACITY_FACTOR))
        .then(pl.col("capacity_factor").round(4))
        .otherwise(None)
        .alias("capacity_factor")
    )
    return (
        monthly.with_columns(pl.col("energy_mwh", "avg_mw", "peak_mw").round(3))
        .select(
            pl.lit(period.start.date()).alias("month"),
            "plant_id",
            "energy_mwh",
            "avg_mw",
            "peak_mw",
            "capacity_factor",
        )
        .sort("plant_id"),
        implausible,
    )


def cross_check(balance_monthly: pl.DataFrame, plants: pl.DataFrame) -> list[str]:
    """Compares the balance with the sum of the plants, per subsystem and source (hydro, wind,
    solar: the balance has no nuclear column, so thermal is left out)."""
    sums = (
        plants.filter(pl.col("source_type").is_in(list(PLANT_SOURCE_OF_BALANCE.values())))
        .group_by("subsystem_id", "source_type")
        .agg(pl.col("generation_mwh").sum().alias("plants_mwh"))
        .with_columns(
            pl.col("source_type")
            .replace_strict({v: k for k, v in PLANT_SOURCE_OF_BALANCE.items()})
            .alias("source")
        )
    )
    joined = balance_monthly.filter(pl.col("subsystem") != "SIN").join(
        sums,
        left_on=["subsystem", "source"],
        right_on=["subsystem_id", "source"],
        how="inner",
    )
    messages = []
    for row in joined.iter_rows(named=True):
        reference = row["energy_mwh"]
        if not reference:
            continue
        deviation = abs(row["plants_mwh"] - reference) / reference
        if deviation > CROSS_CHECK_TOLERANCE:
            messages.append(
                f"balance vs plants {row['subsystem']}/{row['source']}: {deviation:.1%} apart"
            )
    return messages


def build_system_tables(
    period: Period,
    balance: pl.DataFrame | None,
    plants: pl.DataFrame | None,
    capacity: pl.DataFrame | None,
) -> SystemTables:
    warnings: list[str] = []
    hourly = monthly = power_plant = plant_monthly = None
    if balance is not None:
        hourly = build_hourly_balance(balance)
        monthly = build_monthly_generation(period, balance)
    if plants is not None:
        power_plant = build_power_plants(plants, capacity)
        plant_monthly, implausible = build_plant_monthly(period, plants, power_plant)
        if implausible:
            warnings.append(f"{implausible} plants above capacity factor {MAX_CAPACITY_FACTOR}")
        if monthly is not None:
            warnings.extend(cross_check(monthly, plants))
    return SystemTables(hourly, monthly, power_plant, plant_monthly, warnings)
