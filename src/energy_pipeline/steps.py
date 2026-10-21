from __future__ import annotations

import logging
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from energy_pipeline import curtailment, extract, notify, publish, quality, schema, transform
from energy_pipeline.config import Settings
from energy_pipeline.period import Period
from energy_pipeline.sources import (
    GENERATION_CAPACITY,
    LOAD_CURVE,
    MARGINAL_COST,
    PLANT_GENERATION,
    SOLAR_CLUSTER_CURTAILMENT,
    SOLAR_PLANT_CURTAILMENT,
    SOURCES,
    WIND_CLUSTER_CURTAILMENT,
    WIND_PLANT_CURTAILMENT,
    Source,
)
from energy_pipeline.storage import Lake
from energy_pipeline.warehouse import Warehouse

log = logging.getLogger(__name__)

Manifest = dict[str, Any]

CLUSTER_SOURCES = {WIND_CLUSTER_CURTAILMENT.name, SOLAR_CLUSTER_CURTAILMENT.name}
CLUSTER_OF_PLANT_SOURCE = {
    WIND_PLANT_CURTAILMENT.name: WIND_CLUSTER_CURTAILMENT.name,
    SOLAR_PLANT_CURTAILMENT.name: SOLAR_CLUSTER_CURTAILMENT.name,
}
CAPACITY_LATEST_KEY = f"silver/{GENERATION_CAPACITY.name}/latest/data.parquet"
LOCAL_TIMEZONE = "America/Sao_Paulo"


def _lake(settings: Settings) -> Lake:
    return Lake(settings.lake_bucket, settings.s3_endpoint_url)


def _layer_key(layer: str, name: str, partition: str) -> str:
    return f"{layer}/{name}/{partition}/data.parquet"


def _monthly_key(layer: str, name: str, period: Period) -> str:
    return _layer_key(layer, name, f"year={period.year:04d}/month={period.month:02d}")


def _now() -> datetime:
    return datetime.now(UTC)


def prepare_infrastructure(settings: Settings) -> None:
    _lake(settings).ensure_bucket()
    Warehouse(settings.warehouse_dsn).check_schema()


def run_extract(
    source_name: str, period: Period, settings: Settings, snapshot_date: date | None = None
) -> Manifest:
    started_at = _now()
    source = SOURCES[source_name]
    snapshot_date = snapshot_date or started_at.date()
    lake = _lake(settings)
    url = source.url(settings.ons_base_url, period)
    bronze_key = (
        f"bronze/ons/{source.name}/{source.partition(period, snapshot_date)}/"
        f"{source.file_name(period)}"
    )

    etag = extract.remote_etag(url, settings.http_timeout_seconds)
    stored = lake.metadata(bronze_key)
    changed = not (etag and stored and stored.get("source-etag") == etag)
    if changed:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / source.file_name(period)
            checksum = extract.download(url, path, settings.http_timeout_seconds)
            metadata = {"source-url": url, "sha256": checksum, "source-etag": etag or ""}
            lake.upload_file(path, bronze_key, metadata=metadata)
    else:
        checksum = (stored or {}).get("sha256", "")
        log.info("%s is unchanged since the last run; reusing %s", url, bronze_key)

    return {
        "source": source.name,
        "period": str(period),
        "snapshot_date": snapshot_date.isoformat(),
        "partition": source.processed_partition(period, snapshot_date),
        "source_url": url,
        "source_changed": changed,
        "sha256": checksum,
        "bronze_key": bronze_key,
        "started_at": started_at.isoformat(),
    }


def _standardize(
    source: Source, raw: pl.DataFrame, period: Period, snapshot_date: date
) -> transform.TransformResult:
    source_type = source.source_type or ""
    match source.name:
        case LOAD_CURVE.name:
            return transform.transform_load_curve(raw, period)
        case PLANT_GENERATION.name:
            return transform.transform_plant_generation(raw, period)
        case GENERATION_CAPACITY.name:
            return transform.transform_generation_capacity(raw, snapshot_date)
        case MARGINAL_COST.name:
            return transform.transform_marginal_cost(raw, period)
        case name if name in CLUSTER_SOURCES:
            return transform.transform_cluster_curtailment(raw, period, source_type)
        case name if name in CLUSTER_OF_PLANT_SOURCE:
            return transform.transform_plant_curtailment(raw, period, source_type)
    raise ValueError(f"Unknown source: {source.name}")


def _with_utc(df: pl.DataFrame) -> pl.DataFrame:
    if "measured_at" not in df.columns:
        return df
    return df.with_columns(
        pl.col("measured_at")
        .dt.replace_time_zone(LOCAL_TIMEZONE, ambiguous="earliest")
        .dt.convert_time_zone("UTC")
        .dt.replace_time_zone(None)
        .alias("measured_at_utc")
    )


def _model(source: Source, standardized: pl.DataFrame, period: Period, lake: Lake) -> pl.DataFrame:
    if source.name in CLUSTER_SOURCES:
        marginal_cost = lake.read_parquet(_monthly_key("silver", MARGINAL_COST.name, period))
        prices = curtailment.price_series(marginal_cost, curtailment.load_pld_limits(), period)
        return curtailment.enrich_clusters(standardized, prices)
    if source.name in CLUSTER_OF_PLANT_SOURCE:
        clusters = lake.read_parquet(
            _monthly_key("silver", CLUSTER_OF_PLANT_SOURCE[source.name], period)
        )
        return curtailment.allocate_to_plants(standardized, clusters)
    return standardized


def run_transform(manifest: Manifest, settings: Settings) -> Manifest:
    lake = _lake(settings)
    source = SOURCES[manifest["source"]]
    period = Period.parse(manifest["period"])
    snapshot_date = date.fromisoformat(manifest["snapshot_date"])

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"raw.{source.file_format}"
        lake.download_file(manifest["bronze_key"], path)
        raw = transform.read_raw(path, source.file_format)

    schema.check_schema(source.name, raw)
    result = _standardize(source, raw, period, snapshot_date)
    silver = _model(source, _with_utc(result.valid), period, lake)
    silver_key = _layer_key("silver", source.name, manifest["partition"])
    lake.write_parquet(silver, silver_key)
    if source.name == GENERATION_CAPACITY.name:
        lake.write_parquet(silver, CAPACITY_LATEST_KEY)

    quarantine_key = None
    if result.rejected.height:
        quarantine_key = f"quarantine/{source.name}/{manifest['partition']}/rejected.parquet"
        lake.write_parquet(result.rejected, quarantine_key)
        log.warning("%d rows quarantined at %s", result.rejected.height, quarantine_key)

    return {
        **manifest,
        "raw_rows": raw.height,
        "silver_key": silver_key,
        "silver_rows": silver.height,
        "rejected_rows": result.rejected.height,
        "quarantine_key": quarantine_key,
        "duplicates_removed": result.duplicates_removed,
        "missing_measurements": result.missing_measurements,
    }


def run_validate(manifest: Manifest, settings: Settings) -> Manifest:
    lake = _lake(settings)
    source_name = manifest["source"]
    period = Period.parse(manifest["period"])
    rejected = manifest["rejected_rows"]
    max_rejected = settings.max_rejected_ratio
    df = lake.read_parquet(manifest["silver_key"])
    extra: Manifest = {}

    if source_name == LOAD_CURVE.name:
        results = quality.load_curve_checks(df, period, rejected, max_rejected)
    elif source_name == PLANT_GENERATION.name:
        results = quality.plant_generation_checks(
            df, period, rejected, manifest["missing_measurements"], max_rejected
        )
    elif source_name == GENERATION_CAPACITY.name:
        results = quality.generation_capacity_checks(df, rejected, max_rejected)
    elif source_name == MARGINAL_COST.name:
        results = quality.marginal_cost_checks(df, period, rejected, max_rejected)
    elif source_name in CLUSTER_SOURCES:
        results = quality.cluster_curtailment_checks(df, period, rejected, max_rejected)
    elif source_name in CLUSTER_OF_PLANT_SOURCE:
        clusters = lake.read_parquet(
            _monthly_key("silver", CLUSTER_OF_PLANT_SOURCE[source_name], period)
        )
        extra["reconciliation"] = curtailment.reconcile(df, clusters)
        results = quality.plant_curtailment_checks(
            df,
            period,
            rejected,
            max_rejected,
            extra["reconciliation"],
            settings.reconciliation_tolerance,
        )
    else:
        raise ValueError(f"Unknown source: {source_name}")

    quality.enforce(results)
    warnings = [str(result) for result in results if not result.passed]
    return {**manifest, **extra, "quality_warnings": warnings}


def run_record(manifest: Manifest, settings: Settings) -> Manifest:
    Warehouse(settings.warehouse_dsn).record_load(
        dataset=manifest["source"],
        status="success",
        started_at=datetime.fromisoformat(manifest["started_at"]),
        finished_at=_now(),
        period=Period.parse(manifest["period"]).start.date(),
        source_file=manifest["source_url"],
        checksum=manifest["sha256"],
        rows=manifest["silver_rows"],
        detail="\n".join(manifest["quality_warnings"]) or None,
    )
    return manifest


def record_failure(settings: Settings, dataset: str, detail: str) -> None:
    Warehouse(settings.warehouse_dsn).record_load(
        dataset=dataset, status="failed", started_at=_now(), finished_at=_now(), detail=detail
    )


def _read_curtailment(lake: Lake, period: Period, names: list[str]) -> pl.DataFrame | None:
    frames = [
        lake.read_parquet(_monthly_key("silver", name, period))
        for name in names
        if period in SOURCES[name].periods_to_process([period])
    ]
    return pl.concat(frames, how="diagonal_relaxed") if frames else None


def run_publish(period: Period, settings: Settings) -> Manifest:
    started_at = _now()
    lake = _lake(settings)
    clusters = _read_curtailment(lake, period, sorted(CLUSTER_SOURCES))
    plants = _read_curtailment(lake, period, sorted(CLUSTER_OF_PLANT_SOURCE))
    if clusters is None or plants is None:
        log.info("No curtailment data for %s; nothing to publish", period)
        return {"period": str(period), "published": False}
    has_capacity = lake.metadata(CAPACITY_LATEST_KEY) is not None
    capacity = lake.read_parquet(CAPACITY_LATEST_KEY) if has_capacity else None

    tables = publish.build_monthly_tables(period, clusters, plants, capacity)
    outputs = {
        "plant": tables.plant,
        "plant_monthly_curtailment": tables.plant_monthly,
        "state_monthly_curtailment": tables.state_monthly,
        "subsystem_monthly_curtailment": tables.subsystem_monthly,
    }
    for name, df in outputs.items():
        lake.write_parquet(df, _monthly_key("gold", name, period))

    with Warehouse(settings.warehouse_dsn).session() as session:
        for kind in ("cluster", "plant"):
            session.upsert("plant", tables.plant.filter(pl.col("kind") == kind), keys=["id"])
        for name in (
            "plant_monthly_curtailment",
            "state_monthly_curtailment",
            "subsystem_monthly_curtailment",
        ):
            session.replace_range(
                name,
                outputs[name],
                column="month",
                start=period.start.date(),
                end=period.end.date(),
            )

    rows = {name: df.height for name, df in outputs.items()}
    Warehouse(settings.warehouse_dsn).record_load(
        dataset="aggregates",
        status="success",
        started_at=started_at,
        finished_at=_now(),
        period=period.start.date(),
        rows=sum(rows.values()),
        detail=", ".join(f"{name}: {count}" for name, count in rows.items()),
    )
    return {"period": str(period), "published": True, "rows": rows}


def notify_portal(settings: Settings) -> Manifest:
    return notify.request_revalidation(settings)
