"""Gold tables of the grid register: lines, substations and transformers, versioned by snapshot."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date

import polars as pl

HASH_COLUMN = "row_hash"
LINE_COLUMNS = [
    "name",
    "voltage_kv",
    "line_type",
    "network_type",
    "owner_agent",
    "from_substation",
    "to_substation",
    "from_subsystem",
    "to_subsystem",
    "from_state",
    "to_state",
    "length_km",
    "positive_seq_resistance",
    "positive_seq_reactance",
    "positive_seq_shunt",
    "capacity_long_no_limit_a",
    "capacity_long_with_limit_a",
    "capacity_short_no_limit_a",
    "capacity_short_with_limit_a",
    "commissioned_on",
    "decommissioned_on",
    "planned_on",
    "from_bus",
    "to_bus",
]
SUBSTATION_COLUMNS = [
    "substation_id",
    "name",
    "voltage_kv",
    "subsystem",
    "state",
    "main_agent",
    "bus_number",
    "latitude",
    "longitude",
]
TRANSFORMER_COLUMNS = [
    "name",
    "transformer_type",
    "substation",
    "owner_agent",
    "subsystem",
    "state",
    "primary_kv",
    "secondary_kv",
    "tertiary_kv",
    "rated_mva",
    "network_type",
    "commissioned_on",
    "decommissioned_on",
    "primary_bus",
    "secondary_bus",
]


@dataclass(frozen=True)
class GridAssetTables:
    transmission_line: pl.DataFrame | None = None
    substation: pl.DataFrame | None = None
    transformer_capacity: pl.DataFrame | None = None
    warnings: list[str] = field(default_factory=list)


def _row_hash(values: dict[str, object]) -> str:
    text = json.dumps(values, sort_keys=True, default=str)
    return hashlib.md5(text.encode(), usedforsecurity=False).hexdigest()  # noqa: S324


def with_hash(df: pl.DataFrame, attributes: list[str]) -> pl.DataFrame:
    """Adds ``row_hash``: it changes exactly when one of the attributes changes."""
    hashes = [_row_hash(row) for row in df.select(attributes).iter_rows(named=True)]
    return df.with_columns(pl.Series(HASH_COLUMN, hashes, dtype=pl.String))


def _versioned(
    silver: pl.DataFrame, key_from: str, key: str, attributes: list[str], snapshot_date: date
) -> pl.DataFrame:
    renamed = silver.rename({key_from: key}) if key_from != key else silver
    table = with_hash(renamed.select(key, *attributes), attributes)
    return table.select(
        key, pl.lit(snapshot_date).alias("valid_from"), HASH_COLUMN, *attributes
    ).sort(key)


def build_lines(silver: pl.DataFrame, snapshot_date: date) -> pl.DataFrame:
    return _versioned(silver, "equipment_code", "equipment_code", LINE_COLUMNS, snapshot_date)


def build_substations(silver: pl.DataFrame, snapshot_date: date) -> pl.DataFrame:
    return _versioned(silver, "station_id", "station_id", SUBSTATION_COLUMNS, snapshot_date)


def build_transformers(silver: pl.DataFrame, snapshot_date: date) -> pl.DataFrame:
    return _versioned(
        silver, "equipment_code", "equipment_code", TRANSFORMER_COLUMNS, snapshot_date
    )


def build_grid_asset_tables(
    snapshot_date: date,
    *,
    lines: pl.DataFrame | None,
    substations: pl.DataFrame | None,
    transformers: pl.DataFrame | None,
) -> GridAssetTables:
    warnings: list[str] = []
    if substations is not None:
        without = substations.filter(
            pl.col("latitude").is_null() | pl.col("longitude").is_null()
        ).height
        if without:
            warnings.append(f"{without} substations without coordinates in the ONS file")
    return GridAssetTables(
        transmission_line=build_lines(lines, snapshot_date) if lines is not None else None,
        substation=build_substations(substations, snapshot_date)
        if substations is not None
        else None,
        transformer_capacity=build_transformers(transformers, snapshot_date)
        if transformers is not None
        else None,
        warnings=warnings,
    )
