"""CCEE electricity price (PLD): hourly official, hourly "shadow", weekly average and aggregates.

Source: CCEE open data portal, license CC BY 4.0. Only market-wide price series are loaded; the
per-agent datasets (leverage factor, penalties) are deliberately not.
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from energy_pipeline import opendata
from energy_pipeline.config import Settings
from energy_pipeline.curtailment import load_pld_limits
from energy_pipeline.quality import (
    CheckResult,
    DataQualityError,
    Severity,
    accepted_values,
    between,
    no_nulls,
    not_empty,
    unique,
)
from energy_pipeline.storage import Lake
from energy_pipeline.warehouse import Warehouse

log = logging.getLogger(__name__)

PORTAL = "https://dadosabertos.ccee.org.br/dataset"
SUBSYSTEM_OF = {"NORTE": "N", "NORDESTE": "NE", "SUL": "S", "SUDESTE": "SE"}
MAX_PLAUSIBLE_PLD_BRL_MWH = 20_000.0
LOCAL_TIMEZONE = "America/Sao_Paulo"
FILE_ENCODINGS = ("utf8", "iso8859-2")
BASE_PRICE_COLUMNS = ["MES_REFERENCIA", "SUBMERCADO"]


@dataclass(frozen=True)
class Dataset:
    slug: str
    file_prefix: str
    kind: str  # hourly | shadow | weekly | daily
    first_year: int


DATASETS = (
    Dataset("pld_horario", "pld_horario_", "hourly", 2021),
    Dataset("pld_sombra", "pld_sombra_", "shadow", 2022),
    Dataset("pld_media_semanal", "pld_media_semanal_", "weekly", 2021),
)
WEEKLY_HISTORY = "pld_media_semanal_2001_2020"


def read_csv(path: Path) -> pl.DataFrame:
    """The CCEE files switch between UTF-8 and ISO-8859-2 and between quoted and bare fields."""
    last_error: Exception | None = None
    for encoding in FILE_ENCODINGS:
        try:
            return pl.read_csv(path, separator=";", infer_schema=False, encoding=encoding)
        except pl.exceptions.ComputeError as error:
            last_error = error
    raise ValueError(f"{path.name}: unreadable in {FILE_ENCODINGS}") from last_error


def _price(column: str) -> pl.Expr:
    return pl.col(column).str.strip_chars().str.replace(",", ".").cast(pl.Float64, strict=False)


def _subsystem() -> pl.Expr:
    return (
        pl.col("SUBMERCADO")
        .str.strip_chars()
        .str.to_uppercase()
        .replace_strict(SUBSYSTEM_OF, default=None)
    )


def transform_hourly(raw: pl.DataFrame, variant: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Official ("official") or shadow ("shadow") hourly PLD. Returns (valid, rejected)."""
    value_column = "PLD_SOMBRA" if variant == "shadow" else "PLD_HORA"
    method = pl.col("TIPO_SOMBRA").str.strip_chars() if variant == "shadow" else pl.lit("official")
    parsed = raw.select(
        pl.col("MES_REFERENCIA").str.strip_chars().alias("month_ref"),
        pl.col("DIA").str.strip_chars().cast(pl.Int32, strict=False).alias("day"),
        pl.col("HORA").str.strip_chars().cast(pl.Int32, strict=False).alias("hour_of_day"),
        _subsystem().alias("subsystem"),
        method.alias("method"),
        _price(value_column).alias("pld_brl_per_mwh"),
    ).with_columns(
        pl.col("month_ref").str.slice(0, 4).cast(pl.Int32, strict=False).alias("year"),
        pl.col("month_ref").str.slice(4, 2).cast(pl.Int32, strict=False).alias("month"),
    )
    parsed = parsed.with_columns(
        pl.datetime(pl.col("year"), pl.col("month"), pl.col("day"), pl.col("hour_of_day")).alias(
            "hour"
        )
    )
    keys = ["hour", "subsystem", "pld_brl_per_mwh"]
    valid_mask = pl.all_horizontal([pl.col(key).is_not_null() for key in keys])
    valid, rejected = parsed.filter(valid_mask), parsed.filter(~valid_mask)
    valid = (
        valid.unique(subset=["hour", "subsystem", "method"], keep="last", maintain_order=True)
        .with_columns(
            pl.col("hour")
            .dt.replace_time_zone(LOCAL_TIMEZONE, ambiguous="earliest", non_existent="null")
            .dt.convert_time_zone("UTC")
            .dt.replace_time_zone(None)
            .alias("hour_utc"),
            pl.lit(variant).alias("variant"),
        )
        .select("hour", "hour_utc", "subsystem", "variant", "method", "pld_brl_per_mwh")
        .sort("hour_utc", "subsystem", "method")
    )
    return valid, rejected


def transform_weekly(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    parsed = raw.select(
        pl.col("SEMANA")
        .str.strip_chars()
        .str.to_date("%d/%m/%Y", strict=False)
        .alias("week_start"),
        _subsystem().alias("subsystem"),
        _price("PLD_MEDIA_SEMANA").alias("pld_avg_brl_per_mwh"),
    )
    keys = ["week_start", "subsystem", "pld_avg_brl_per_mwh"]
    valid_mask = pl.all_horizontal([pl.col(key).is_not_null() for key in keys])
    valid = parsed.filter(valid_mask).unique(
        subset=["week_start", "subsystem"], keep="last", maintain_order=True
    )
    return valid.sort("week_start", "subsystem"), parsed.filter(~valid_mask)


def hourly_checks(valid: pl.DataFrame, rejected: pl.DataFrame, variant: str) -> list[CheckResult]:
    """Blocking gates plus the warnings against the regulatory floor and cap of each year."""
    checks = [
        not_empty(valid),
        no_nulls(valid, ["hour", "hour_utc", "subsystem", "pld_brl_per_mwh"]),
        unique(valid, ["hour_utc", "subsystem", "method"]),
        accepted_values(valid, "subsystem", set(SUBSYSTEM_OF.values())),
        between(valid, "pld_brl_per_mwh", 0.0, MAX_PLAUSIBLE_PLD_BRL_MWH),
        CheckResult(
            "rejected_rows",
            rejected.height == 0,
            f"{rejected.height} rows rejected",
            Severity.WARN,
        ),
    ]
    if variant == "official" and valid.height:
        per_hour = valid.group_by("hour_utc").agg(pl.col("subsystem").n_unique().alias("n"))
        incomplete = per_hour.filter(pl.col("n") < len(SUBSYSTEM_OF)).height
        checks.append(
            CheckResult(
                "four_subsystems_per_hour",
                incomplete == 0,
                f"{incomplete} hours with fewer than 4 submarkets",
            )
        )
        limits = load_pld_limits()
        outside = (
            valid.with_columns(pl.col("hour").dt.year().alias("year"))
            .join(limits.select("year", "pld_min", "pld_max_hourly"), on="year", how="inner")
            .filter(
                (pl.col("pld_brl_per_mwh") < pl.col("pld_min") - 0.01)
                | (pl.col("pld_brl_per_mwh") > pl.col("pld_max_hourly") + 0.01)
            )
            .height
        )
        checks.append(
            CheckResult(
                "within_regulatory_floor_and_cap",
                outside == 0,
                f"{outside} hours outside the ANEEL floor and cap of their year",
                Severity.WARN,
            )
        )
    return checks


def weekly_checks(valid: pl.DataFrame) -> list[CheckResult]:
    return [
        not_empty(valid),
        no_nulls(valid, ["week_start", "subsystem", "pld_avg_brl_per_mwh"]),
        unique(valid, ["week_start", "subsystem"]),
        accepted_values(valid, "subsystem", set(SUBSYSTEM_OF.values())),
        between(valid, "pld_avg_brl_per_mwh", 0.0, MAX_PLAUSIBLE_PLD_BRL_MWH),
    ]


def raise_on_errors(checks: list[CheckResult]) -> list[str]:
    failures = [check for check in checks if not check.passed]
    errors = [check for check in failures if check.severity is Severity.ERROR]
    if errors:
        raise DataQualityError(errors)
    return [str(check) for check in failures]


def aggregate(hourly: pl.DataFrame, period: str) -> pl.DataFrame:
    """Daily or monthly statistics of the official hourly PLD per submarket."""
    limits = load_pld_limits().select("year", "pld_min", "pld_max_hourly")
    unit = "day" if period == "day" else "month"
    frame = (
        hourly.filter(pl.col("variant") == "official")
        .with_columns(pl.col("hour").dt.year().alias("year"))
        .join(limits, on="year", how="left")
    )
    frame = frame.with_columns(
        pl.col("hour").dt.date().alias("day"),
        pl.col("hour").dt.truncate("1mo").dt.date().alias("month"),
        (pl.col("pld_brl_per_mwh") <= pl.col("pld_min") + 0.01).fill_null(False).alias("at_floor"),
        (pl.col("pld_brl_per_mwh") >= pl.col("pld_max_hourly") - 0.01)
        .fill_null(False)
        .alias("at_cap"),
    )
    return (
        frame.group_by(unit, "subsystem")
        .agg(
            pl.col("pld_brl_per_mwh").mean().round(2).alias("pld_avg_brl_per_mwh"),
            pl.col("pld_brl_per_mwh").min().alias("pld_min_brl_per_mwh"),
            pl.col("pld_brl_per_mwh").max().alias("pld_max_brl_per_mwh"),
            pl.col("pld_brl_per_mwh").std().round(2).alias("pld_std_brl_per_mwh"),
            pl.col("at_floor").sum().cast(pl.Int32).alias("hours_at_floor"),
            pl.col("at_cap").sum().cast(pl.Int32).alias("hours_at_cap"),
            pl.len().cast(pl.Int32).alias("hours"),
        )
        .sort(unit, "subsystem")
    )


def _resources(slug: str) -> dict[str, opendata.Resource]:
    page = opendata.fetch_text(f"{PORTAL}/{slug}")
    return {resource.name: resource for resource in opendata.ckan_resources(page)}


def _fetch_to_bronze(
    resource: opendata.Resource, dataset: str, lake: Lake, directory: Path
) -> tuple[Path, str]:
    path = directory / f"{resource.name}.csv"
    checksum, served_name = opendata.download(resource.url, path)
    if served_name and not served_name.startswith(resource.name):
        raise ValueError(f"{resource.url} served {served_name!r}, expected {resource.name!r}")
    collected = datetime.now(UTC).date().isoformat()
    lake.upload_file(
        path,
        f"bronze/ccee/{dataset}/collected={collected}/{resource.name}.csv",
        metadata={"source-url": resource.url, "sha256": checksum},
    )
    return path, checksum


def _hourly_year(name: str, prefix: str) -> int | None:
    suffix = name.removeprefix(prefix)
    return int(suffix) if suffix.isdigit() else None


def years_to_load(first_year: int, last_year: int, backfill: bool, now: datetime) -> list[int]:
    start = first_year if backfill else max(first_year, now.year - 1)
    return list(range(start, min(last_year, now.year) + 1))


def run_hourly(
    dataset: Dataset, settings: Settings, backfill: bool = False, now: datetime | None = None
) -> dict[str, object]:
    now = now or datetime.now(UTC)
    variant = "shadow" if dataset.kind == "shadow" else "official"
    lake, warehouse = (
        Lake(settings.lake_bucket, settings.s3_endpoint_url),
        Warehouse(settings.warehouse_dsn),
    )
    resources = _resources(dataset.slug)
    loaded: dict[int, int] = {}
    warnings: list[str] = []
    for resource in resources.values():
        year = _hourly_year(resource.name, dataset.file_prefix)
        if year is None or year not in years_to_load(dataset.first_year, now.year, backfill, now):
            continue
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = _fetch_to_bronze(resource, dataset.slug, lake, Path(tmp))
            raw = read_csv(path)
        valid, rejected = transform_hourly(raw, variant)
        warnings += raise_on_errors(hourly_checks(valid, rejected, variant))
        with warehouse.session() as session:
            session.lock(f"pld_hourly:{variant}")
            session.execute(
                "DELETE FROM pld_hourly WHERE variant = %s AND hour >= %s AND hour < %s",
                (variant, datetime(year, 1, 1), datetime(year + 1, 1, 1)),
            )
            session.upsert(
                "pld_hourly",
                valid,
                keys=["hour_utc", "subsystem", "variant", "method"],
                touch_column=None,
            )
        loaded[year] = valid.height
    if variant == "official" and loaded:
        refresh_aggregates(warehouse, min(loaded))
    return {"dataset": dataset.slug, "rows_by_year": loaded, "warnings": warnings}


def refresh_aggregates(warehouse: Warehouse, from_year: int) -> None:
    """Rebuild the daily and monthly statistics from the hourly table, from ``from_year`` on."""
    with warehouse.session() as session:
        rows = session.fetchall(
            "SELECT hour, hour_utc, subsystem, variant, method, pld_brl_per_mwh FROM pld_hourly "
            "WHERE variant = 'official' AND hour >= %s",
            (datetime(from_year, 1, 1),),
        )
        hourly = pl.DataFrame(
            rows,
            schema=["hour", "hour_utc", "subsystem", "variant", "method", "pld_brl_per_mwh"],
            orient="row",
        ).with_columns(pl.col("pld_brl_per_mwh").cast(pl.Float64))
        if hourly.is_empty():
            return
        daily, monthly = aggregate(hourly, "day"), aggregate(hourly, "month")
        session.execute("DELETE FROM pld_daily WHERE day >= %s", (date(from_year, 1, 1),))
        session.upsert("pld_daily", daily, keys=["day", "subsystem"], touch_column=None)
        session.execute("DELETE FROM pld_monthly WHERE month >= %s", (date(from_year, 1, 1),))
        session.upsert("pld_monthly", monthly, keys=["month", "subsystem"], touch_column=None)


def run_weekly(settings: Settings, backfill: bool = False) -> dict[str, object]:
    now = datetime.now(UTC)
    lake, warehouse = (
        Lake(settings.lake_bucket, settings.s3_endpoint_url),
        Warehouse(settings.warehouse_dsn),
    )
    resources = _resources("pld_media_semanal")
    wanted = {name for name in resources if name == WEEKLY_HISTORY and backfill}
    wanted |= {
        name
        for name in resources
        if (year := _hourly_year(name, "pld_media_semanal_")) is not None
        and year >= (2021 if backfill else now.year - 1)
    }
    loaded = 0
    for name in sorted(wanted):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = _fetch_to_bronze(resources[name], "pld_media_semanal", lake, Path(tmp))
            raw = read_csv(path)
        valid, _ = transform_weekly(raw)
        raise_on_errors(weekly_checks(valid))
        with warehouse.session() as session:
            session.lock("pld_weekly")
            session.upsert("pld_weekly", valid, keys=["week_start", "subsystem"], touch_column=None)
        loaded += valid.height
    return {"dataset": "pld_media_semanal", "rows": loaded}


def run_all(settings: Settings, backfill: bool = False) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for dataset in DATASETS:
        if dataset.kind == "weekly":
            results.append(run_weekly(settings, backfill))
        else:
            results.append(run_hourly(dataset, settings, backfill))
    return results
