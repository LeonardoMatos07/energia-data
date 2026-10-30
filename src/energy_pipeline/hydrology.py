"""Gold tables of the water side: reservoirs, daily storage and inflows, spilled energy."""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from energy_pipeline.period import Period
from energy_pipeline.publish import make_slug
from energy_pipeline.transform import normalize_label

STORAGE_COLUMNS = ["storage_max_mw_month", "storage_mw_month", "storage_pct"]
ENA_COLUMNS = [
    "ena_gross_mw_avg",
    "ena_gross_pct_mlt",
    "ena_storable_mw_avg",
    "ena_storable_pct_mlt",
]
OPERATION_COLUMNS = [
    "useful_volume_pct",
    "inflow_m3s",
    "natural_inflow_m3s",
    "outflow_m3s",
    "turbined_m3s",
    "spilled_m3s",
    "upstream_level_m",
    "downstream_level_m",
]
RESERVOIR_DAILY_COLUMNS = [
    "day",
    "reservoir_id",
    "storage_mw_month",
    "storage_max_mw_month",
    "storage_pct",
    "useful_volume_pct",
    *ENA_COLUMNS,
    "inflow_m3s",
    "natural_inflow_m3s",
    "outflow_m3s",
    "turbined_m3s",
    "spilled_m3s",
    "upstream_level_m",
    "downstream_level_m",
]


@dataclass(frozen=True)
class HydrologyTables:
    reservoir: pl.DataFrame | None = None
    subsystem_storage: pl.DataFrame | None = None
    reservoir_daily: pl.DataFrame | None = None
    spilled_monthly: pl.DataFrame | None = None
    warnings: list[str] = field(default_factory=list)


def build_reservoirs(reservoirs: pl.DataFrame) -> pl.DataFrame:
    return (
        reservoirs.with_columns(
            pl.struct("name", "reservoir_id")
            .map_elements(lambda row: make_slug(row["name"], row["reservoir_id"]), pl.String)
            .alias("slug")
        )
        .select(
            pl.col("reservoir_id").alias("id"),
            "name",
            "slug",
            "reservoir_type",
            "planning_code",
            "station_code",
            "plant_name",
            "ceg",
            pl.col("subsystem_id").alias("subsystem"),
            "ree",
            "basin",
            "river",
            "commissioned_on",
            "max_level_m",
            "min_level_m",
            "max_volume_hm3",
            "min_volume_hm3",
            "useful_volume_hm3",
            "latitude",
            "longitude",
        )
        .sort("id")
    )


def build_subsystem_storage(
    storage: pl.DataFrame | None, inflow: pl.DataFrame | None
) -> pl.DataFrame | None:
    """Stored energy (EAR) and natural inflow energy (ENA) per subsystem and day."""
    if storage is None and inflow is None:
        return None
    keys = ["day", "subsystem_id"]
    frames = [df for df in (storage, inflow) if df is not None]
    joined = frames[0]
    for other in frames[1:]:
        joined = joined.join(other, on=keys, how="full", coalesce=True)
    for column in [*STORAGE_COLUMNS, *ENA_COLUMNS]:
        if column not in joined.columns:
            joined = joined.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
    return joined.select(
        "day",
        pl.col("subsystem_id").alias("subsystem"),
        *STORAGE_COLUMNS,
        *ENA_COLUMNS,
    ).sort("day", "subsystem")


def _with_reservoir_id(daily: pl.DataFrame, reservoirs: pl.DataFrame) -> pl.DataFrame:
    """Adds ``reservoir_id``: by planning code, and by normalised name for the few reservoirs the
    ONS keeps out of the planning models (they have no code)."""
    by_code = reservoirs.select("reservoir_id", "planning_code").filter(
        pl.col("planning_code").is_not_null()
    )
    by_name = reservoirs.select(
        pl.col("reservoir_id").alias("reservoir_id_by_name"),
        pl.col("name").map_elements(normalize_label, pl.String).alias("name_key"),
    ).unique("name_key")
    return (
        daily.join(by_code, on="planning_code", how="left")
        .with_columns(
            pl.col("reservoir_name").map_elements(normalize_label, pl.String).alias("name_key")
        )
        .join(by_name, on="name_key", how="left")
        .with_columns(pl.coalesce("reservoir_id", "reservoir_id_by_name").alias("reservoir_id"))
        .drop("name_key", "reservoir_id_by_name")
    )


def build_reservoir_daily(
    reservoirs: pl.DataFrame,
    storage: pl.DataFrame | None,
    inflow: pl.DataFrame | None,
    operations: pl.DataFrame | None,
) -> tuple[pl.DataFrame | None, int]:
    """One row per reservoir and day. EAR and ENA come with the planning code and the hydrological
    data with ``res_id``; the register maps one to the other. Reservoirs that are not in the
    register cannot be published (the table points at it) and are counted."""
    parts: list[pl.DataFrame] = []
    if storage is not None:
        parts.append(
            _with_reservoir_id(storage, reservoirs).select("reservoir_id", "day", *STORAGE_COLUMNS)
        )
    if inflow is not None:
        parts.append(
            _with_reservoir_id(inflow, reservoirs).select("reservoir_id", "day", *ENA_COLUMNS)
        )
    if operations is not None:
        parts.append(operations.select("reservoir_id", "day", *OPERATION_COLUMNS))
    if not parts:
        return None, 0
    known = set(reservoirs["reservoir_id"])
    in_register = pl.col("reservoir_id").is_in(known)
    unmapped = sum(
        part.filter(~in_register | pl.col("reservoir_id").is_null()).height for part in parts
    )
    parts = [part.filter(in_register) for part in parts]
    joined = parts[0]
    for other in parts[1:]:
        joined = joined.join(other, on=["reservoir_id", "day"], how="full", coalesce=True)
    for column in RESERVOIR_DAILY_COLUMNS:
        if column not in joined.columns:
            joined = joined.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
    return joined.select(RESERVOIR_DAILY_COLUMNS).sort("reservoir_id", "day"), unmapped


def build_spilled_monthly(period: Period, spilled: pl.DataFrame) -> pl.DataFrame:
    """Monthly spilled energy per plant, in MWh (the file is hourly MWmed)."""
    return (
        spilled.sort("measured_at")
        .group_by("plant_code")
        .agg(
            pl.col("plant_name").last(),
            pl.col("subsystem_id").last().alias("subsystem"),
            pl.col("basin").last(),
            pl.col("river").last(),
            pl.col("agent").last(),
            pl.col("spilled_energy_mwh").sum().round(3),
            pl.col("spilled_turbinable_energy_mwh").sum().round(3),
        )
        .select(
            pl.lit(period.start.date()).alias("month"),
            "plant_code",
            "plant_name",
            "subsystem",
            "basin",
            "river",
            "agent",
            "spilled_energy_mwh",
            "spilled_turbinable_energy_mwh",
        )
        .sort("plant_code")
    )


def build_hydrology_tables(
    period: Period,
    *,
    reservoirs: pl.DataFrame | None,
    subsystem_storage: pl.DataFrame | None,
    subsystem_inflow: pl.DataFrame | None,
    reservoir_storage: pl.DataFrame | None,
    reservoir_inflow: pl.DataFrame | None,
    operations: pl.DataFrame | None,
    spilled: pl.DataFrame | None,
) -> HydrologyTables:
    warnings: list[str] = []
    register = build_reservoirs(reservoirs) if reservoirs is not None else None
    daily = None
    if reservoirs is not None:
        daily, unmapped = build_reservoir_daily(
            reservoirs, reservoir_storage, reservoir_inflow, operations
        )
        if unmapped:
            warnings.append(f"{unmapped} reservoir-days without a reservoir in the register")
    return HydrologyTables(
        reservoir=register,
        subsystem_storage=build_subsystem_storage(subsystem_storage, subsystem_inflow),
        reservoir_daily=daily,
        spilled_monthly=build_spilled_monthly(period, spilled) if spilled is not None else None,
        warnings=warnings,
    )
