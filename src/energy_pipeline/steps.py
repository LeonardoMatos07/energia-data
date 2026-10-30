from __future__ import annotations

import logging
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from energy_pipeline import (
    curtailment,
    extract,
    first_seen,
    grid_assets,
    hydrology,
    notify,
    publish,
    quality,
    schema,
    system,
    transform,
)
from energy_pipeline.config import Settings
from energy_pipeline.period import Period
from energy_pipeline.sources import (
    ENERGY_BALANCE,
    GENERATION_CAPACITY,
    GRID_ASSET_SOURCES,
    HYDRO_OPERATIONS,
    INTERNATIONAL_INTERCHANGE,
    LOAD_CURVE,
    MARGINAL_COST,
    NATIONAL_INTERCHANGE,
    PLANT_GENERATION,
    RESERVOIR_INFLOW,
    RESERVOIR_STORAGE,
    RESERVOIRS,
    SOLAR_CLUSTER_CURTAILMENT,
    SOLAR_PLANT_CURTAILMENT,
    SOURCES,
    SPILLED_TURBINABLE_ENERGY,
    SUBSTATIONS,
    SUBSYSTEM_INFLOW,
    SUBSYSTEM_STORAGE,
    THERMAL_DISPATCH,
    TRANSFORMATION_CAPACITY,
    TRANSMISSION_LINES,
    WIND_CLUSTER_CURTAILMENT,
    WIND_PLANT_CURTAILMENT,
    FileFormat,
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
RESERVOIRS_LATEST_KEY = "silver/reservoirs/latest/data.parquet"


def latest_key(name: str) -> str:
    return f"silver/{name}/latest/data.parquet"


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


def _locate_remote_file(
    source: Source, period: Period, settings: Settings
) -> tuple[FileFormat, str, str | None]:
    """Find the published file: the configured format first, the other one if it is missing."""
    missing: extract.SourceNotPublishedError | None = None
    for file_format in source.formats():
        url = source.url(settings.ons_base_url, period, file_format)
        try:
            etag = extract.remote_etag(url, settings.http_timeout_seconds)
        except extract.SourceNotPublishedError as error:
            missing = missing or error
            continue
        if missing is not None:
            log.warning("%s is not published; using the %s file instead", missing, file_format)
        return file_format, url, etag
    assert missing is not None
    raise missing


def run_extract(
    source_name: str, period: Period, settings: Settings, snapshot_date: date | None = None
) -> Manifest:
    started_at = _now()
    source = SOURCES[source_name]
    snapshot_date = snapshot_date or started_at.date()
    lake = _lake(settings)
    file_format, url, etag = _locate_remote_file(source, period, settings)
    file_name = source.file_name(period, file_format)
    bronze_key = f"bronze/ons/{source.name}/{source.partition(period, snapshot_date)}/{file_name}"

    stored = lake.metadata(bronze_key)
    changed = not (etag and stored and stored.get("source-etag") == etag)
    if changed:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / file_name
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
        "file_format": str(file_format),
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
        case ENERGY_BALANCE.name:
            return transform.transform_energy_balance(raw, period)
        case RESERVOIRS.name:
            return transform.transform_reservoirs(raw, snapshot_date)
        case SUBSYSTEM_STORAGE.name:
            return transform.transform_subsystem_storage(raw, period)
        case SUBSYSTEM_INFLOW.name:
            return transform.transform_subsystem_inflow(raw, period)
        case RESERVOIR_STORAGE.name:
            return transform.transform_reservoir_storage(raw, period)
        case RESERVOIR_INFLOW.name:
            return transform.transform_reservoir_inflow(raw, period)
        case HYDRO_OPERATIONS.name:
            return transform.transform_hydro_operations(raw, period)
        case SPILLED_TURBINABLE_ENERGY.name:
            return transform.transform_spilled_turbinable_energy(raw, period)
        case TRANSMISSION_LINES.name:
            return transform.transform_transmission_lines(raw, snapshot_date)
        case SUBSTATIONS.name:
            return transform.transform_substations(raw, snapshot_date)
        case TRANSFORMATION_CAPACITY.name:
            return transform.transform_transformation_capacity(raw, snapshot_date)
        case NATIONAL_INTERCHANGE.name:
            return transform.transform_national_interchange(raw, period)
        case INTERNATIONAL_INTERCHANGE.name:
            return transform.transform_international_interchange(raw, period)
        case THERMAL_DISPATCH.name:
            return transform.transform_thermal_dispatch(raw, period)
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
        file_format = manifest.get("file_format", source.file_format)
        path = Path(tmp) / f"raw.{file_format}"
        lake.download_file(manifest["bronze_key"], path)
        raw = transform.read_raw(path, file_format)

    schema.check_schema(source.name, raw)
    result = _standardize(source, raw, period, snapshot_date)
    silver = _model(source, _with_utc(result.valid), period, lake)
    silver_key = _layer_key("silver", source.name, manifest["partition"])
    lake.write_parquet(silver, silver_key)
    if source.name == GENERATION_CAPACITY.name:
        lake.write_parquet(silver, CAPACITY_LATEST_KEY)
    if source.name == RESERVOIRS.name:
        lake.write_parquet(silver, RESERVOIRS_LATEST_KEY)
    if source.name in GRID_ASSET_SOURCES:
        lake.write_parquet(silver, latest_key(source.name))

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
    elif source_name in (NATIONAL_INTERCHANGE.name, INTERNATIONAL_INTERCHANGE.name):
        results = quality.interchange_checks(df, period, rejected, max_rejected)
    elif source_name == RESERVOIRS.name:
        results = quality.reservoirs_checks(df, rejected, max_rejected)
    elif source_name == SUBSYSTEM_STORAGE.name:
        results = quality.subsystem_storage_checks(df, period, rejected, max_rejected)
    elif source_name == SUBSYSTEM_INFLOW.name:
        results = quality.subsystem_inflow_checks(df, period, rejected, max_rejected)
    elif source_name == RESERVOIR_STORAGE.name:
        results = quality.reservoir_storage_checks(df, period, rejected, max_rejected)
    elif source_name == RESERVOIR_INFLOW.name:
        results = quality.reservoir_inflow_checks(df, period, rejected, max_rejected)
    elif source_name == HYDRO_OPERATIONS.name:
        results = quality.hydro_operations_checks(df, period, rejected, max_rejected)
    elif source_name == SPILLED_TURBINABLE_ENERGY.name:
        results = quality.spilled_energy_checks(df, period, rejected, max_rejected)
    elif source_name == TRANSMISSION_LINES.name:
        results = quality.transmission_lines_checks(df, rejected, max_rejected)
    elif source_name == SUBSTATIONS.name:
        results = quality.substations_checks(df, rejected, max_rejected)
    elif source_name == TRANSFORMATION_CAPACITY.name:
        results = quality.transformation_capacity_checks(df, rejected, max_rejected)
    elif source_name == THERMAL_DISPATCH.name:
        results = quality.thermal_dispatch_checks(df, period, rejected, max_rejected)
    elif source_name == ENERGY_BALANCE.name:
        results = quality.energy_balance_checks(df, period, rejected, max_rejected)
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


def _read_if_applicable(lake: Lake, name: str, period: Period) -> pl.DataFrame | None:
    if period not in SOURCES[name].periods_to_process([period]):
        return None
    key = _monthly_key("silver", name, period)
    return lake.read_parquet(key) if lake.metadata(key) is not None else None


def run_publish_system(period: Period, settings: Settings) -> Manifest:
    started_at = _now()
    lake = _lake(settings)
    balance = _read_if_applicable(lake, ENERGY_BALANCE.name, period)
    plants = _read_if_applicable(lake, PLANT_GENERATION.name, period)
    national = _read_if_applicable(lake, NATIONAL_INTERCHANGE.name, period)
    international = _read_if_applicable(lake, INTERNATIONAL_INTERCHANGE.name, period)
    thermal = _read_if_applicable(lake, THERMAL_DISPATCH.name, period)
    if all(df is None for df in (balance, plants, national, international, thermal)):
        log.info("No system data for %s; nothing to publish", period)
        return {"period": str(period), "published": False}
    has_capacity = lake.metadata(CAPACITY_LATEST_KEY) is not None
    capacity = lake.read_parquet(CAPACITY_LATEST_KEY) if has_capacity else None

    tables = system.build_system_tables(
        period,
        balance,
        plants,
        capacity,
        national=national,
        international=international,
        thermal=thermal,
    )
    outputs = {
        "subsystem_hourly_balance": tables.hourly_balance,
        "subsystem_monthly_generation": tables.monthly_generation,
        "power_plant": tables.power_plant,
        "plant_monthly_generation": tables.plant_monthly,
        "interchange_hourly": tables.interchange_hourly,
        "interchange_monthly": tables.interchange_monthly,
        "thermal_dispatch_monthly": tables.thermal_monthly,
    }
    built = {name: df for name, df in outputs.items() if df is not None}
    for name, df in built.items():
        lake.write_parquet(df, _monthly_key("gold", name, period))

    start, end = period.start, period.end
    with Warehouse(settings.warehouse_dsn).session() as session:
        session.lock("publish_system")
        if "power_plant" in built:
            session.upsert("power_plant", built["power_plant"], keys=["id"], keep=["slug"])
        for name in ("subsystem_hourly_balance", "interchange_hourly"):
            if name in built:
                session.replace_range(name, built[name], column="hour", start=start, end=end)
        for name in (
            "subsystem_monthly_generation",
            "plant_monthly_generation",
            "interchange_monthly",
            "thermal_dispatch_monthly",
        ):
            if name in built:
                session.replace_range(
                    name, built[name], column="month", start=start.date(), end=end.date()
                )

    rows = {name: df.height for name, df in built.items()}
    detail = ", ".join(f"{name}: {count}" for name, count in rows.items())
    if tables.warnings:
        log.warning("System checks for %s: %s", period, "; ".join(tables.warnings))
        detail += "\n" + "\n".join(tables.warnings)
    Warehouse(settings.warehouse_dsn).record_load(
        dataset="system_aggregates",
        status="success",
        started_at=started_at,
        finished_at=_now(),
        period=period.start.date(),
        rows=sum(rows.values()),
        detail=detail,
    )
    return {"period": str(period), "published": True, "rows": rows, "warnings": tables.warnings}


def run_publish_hydrology(period: Period, settings: Settings) -> Manifest:
    started_at = _now()
    lake = _lake(settings)
    read = lambda name: _read_if_applicable(lake, name, period)  # noqa: E731
    has_register = lake.metadata(RESERVOIRS_LATEST_KEY) is not None
    reservoirs = lake.read_parquet(RESERVOIRS_LATEST_KEY) if has_register else None
    inputs = {
        "subsystem_storage": read(SUBSYSTEM_STORAGE.name),
        "subsystem_inflow": read(SUBSYSTEM_INFLOW.name),
        "reservoir_storage": read(RESERVOIR_STORAGE.name),
        "reservoir_inflow": read(RESERVOIR_INFLOW.name),
        "operations": read(HYDRO_OPERATIONS.name),
        "spilled": read(SPILLED_TURBINABLE_ENERGY.name),
    }
    if reservoirs is None and all(df is None for df in inputs.values()):
        log.info("No hydrology data for %s; nothing to publish", period)
        return {"period": str(period), "published": False}

    if inputs["operations"] is not None:
        key = first_seen.LAKE_KEY
        previous = lake.read_parquet(key) if lake.metadata(key) else None
        lake.write_parquet(
            first_seen.update_first_seen(previous, inputs["operations"], _now()), key
        )

    tables = hydrology.build_hydrology_tables(period, reservoirs=reservoirs, **inputs)
    outputs = {
        "reservoir": tables.reservoir,
        "subsystem_storage_daily": tables.subsystem_storage,
        "reservoir_daily": tables.reservoir_daily,
        "plant_monthly_spilled_energy": tables.spilled_monthly,
    }
    built = {name: df for name, df in outputs.items() if df is not None}
    for name, df in built.items():
        lake.write_parquet(df, _monthly_key("gold", name, period))

    start, end = period.start, period.end
    with Warehouse(settings.warehouse_dsn).session() as session:
        session.lock("publish_hydrology")
        if "reservoir" in built:
            session.upsert("reservoir", built["reservoir"], keys=["id"], keep=["slug"])
        for name in ("subsystem_storage_daily", "reservoir_daily"):
            if name in built:
                session.replace_range(
                    name, built[name], column="day", start=start.date(), end=end.date()
                )
        if "plant_monthly_spilled_energy" in built:
            session.replace_range(
                "plant_monthly_spilled_energy",
                built["plant_monthly_spilled_energy"],
                column="month",
                start=start.date(),
                end=end.date(),
            )

    rows = {name: df.height for name, df in built.items()}
    detail = ", ".join(f"{name}: {count}" for name, count in rows.items())
    if tables.warnings:
        log.warning("Hydrology checks for %s: %s", period, "; ".join(tables.warnings))
        detail += "\n" + "\n".join(tables.warnings)
    Warehouse(settings.warehouse_dsn).record_load(
        dataset="hydrology_aggregates",
        status="success",
        started_at=started_at,
        finished_at=_now(),
        period=period.start.date(),
        rows=sum(rows.values()),
        detail=detail,
    )
    return {"period": str(period), "published": True, "rows": rows, "warnings": tables.warnings}


def run_publish_grid_assets(period: Period, settings: Settings) -> Manifest:
    """Publishes the latest register snapshot as versioned history (idempotent per date)."""
    started_at = _now()
    lake = _lake(settings)

    def latest(source: Source) -> pl.DataFrame | None:
        key = latest_key(source.name)
        return lake.read_parquet(key) if lake.metadata(key) is not None else None

    inputs = {
        "lines": latest(TRANSMISSION_LINES),
        "substations": latest(SUBSTATIONS),
        "transformers": latest(TRANSFORMATION_CAPACITY),
    }
    snapshots = {df["snapshot_date"][0] for df in inputs.values() if df is not None and df.height}
    if not snapshots:
        log.info("No grid register snapshot; nothing to publish")
        return {"period": str(period), "published": False}
    snapshot_date = max(snapshots)

    tables = grid_assets.build_grid_asset_tables(snapshot_date, **inputs)
    outputs = {
        "transmission_line": (tables.transmission_line, "equipment_code"),
        "substation": (tables.substation, "station_id"),
        "transformer_capacity": (tables.transformer_capacity, "equipment_code"),
    }
    changes: dict[str, tuple[int, int]] = {}
    with Warehouse(settings.warehouse_dsn).session() as session:
        session.lock("publish_grid_assets")
        for name, (df, key) in outputs.items():
            if df is not None:
                changes[name] = session.apply_snapshot(
                    name, df, key=key, snapshot_date=snapshot_date
                )
    for name, (df, _) in outputs.items():
        if df is not None:
            lake.write_parquet(
                df, f"gold/{name}/snapshot_date={snapshot_date.isoformat()}/data.parquet"
            )

    detail = ", ".join(
        f"{name}: {len(df)} rows, {changes[name][0]} closed, {changes[name][1]} opened"
        for name, (df, _) in outputs.items()
        if df is not None
    )
    if tables.warnings:
        detail += "\n" + "\n".join(tables.warnings)
    Warehouse(settings.warehouse_dsn).record_load(
        dataset="grid_assets_aggregates",
        status="success",
        started_at=started_at,
        finished_at=_now(),
        period=period.start.date(),
        rows=sum(len(df) for df, _ in outputs.values() if df is not None),
        detail=detail,
    )
    return {"period": str(period), "published": True, "changes": changes}


def run_publish(period: Period, settings: Settings, pipeline: str = "curtailment") -> Manifest:
    if pipeline == "grid_assets":
        return run_publish_grid_assets(period, settings)
    if pipeline == "hydrology":
        return run_publish_hydrology(period, settings)
    if pipeline == "system":
        return run_publish_system(period, settings)
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
        "hourly_curtailment": tables.hourly,
    }
    for name, df in outputs.items():
        lake.write_parquet(df, _monthly_key("gold", name, period))

    with Warehouse(settings.warehouse_dsn).session() as session:
        session.lock("publish")
        for kind in ("cluster", "plant"):
            session.upsert(
                "plant", tables.plant.filter(pl.col("kind") == kind), keys=["id"], keep=["slug"]
            )
        for name in (
            "plant_monthly_curtailment",
            "state_monthly_curtailment",
            "subsystem_monthly_curtailment",
            "hourly_curtailment",
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


def publish_reports(periods: list[str], settings: Settings) -> Manifest:
    """Writes and publishes the report of every month that was processed (a month without data
    is skipped), then asks the portal to refresh its report pages."""
    from energy_pipeline import report

    saved: list[str] = []
    for value in periods:
        try:
            built = report.build_report(report.fetch_report_data(settings, Period.parse(value)))
        except LookupError as error:
            log.info("No report for %s: %s", value, error)
            continue
        report.submit(settings, built, publish=True)
        saved.append(built.slug)
    if saved:
        notify.request_revalidation(settings, ["reports"])
    return {"published": saved}


def month_to_announce(periods: list[str], run_date: datetime, backfill: bool) -> str | None:
    """The month whose report is e-mailed after a load: the previous month of a regular monthly
    run. A backfill or a re-run of old months never mails anyone."""
    if backfill or not periods:
        return None
    newest = max(Period.parse(value) for value in periods)
    return str(newest) if newest == Period.from_datetime(run_date).previous() else None


def announce_report(
    periods: list[str], run_date: datetime, backfill: bool, settings: Settings
) -> Manifest:
    month = month_to_announce(periods, run_date, backfill)
    if month is None:
        return {"skipped": "not the monthly run for the latest month"}
    if not settings.dispatch_report_email:
        return {"skipped": "DISPATCH_REPORT_EMAIL is off"}
    return notify.request_report_dispatch(settings, month)
