"""ANEEL collective continuity indicators (DEC and FEC) per distributor and consumer-unit set.

Source: ANEEL open data portal, license ODbL. The records describe regulated distribution
companies and electrical sets; there is no personal data. Values are monthly (the dataset's
``NumPeriodoIndice`` is the month of the year) and the regulatory limits are annual.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from energy_pipeline import opendata
from energy_pipeline.ccee import raise_on_errors
from energy_pipeline.config import Settings
from energy_pipeline.quality import (
    CheckResult,
    Severity,
    between,
    no_nulls,
    not_empty,
    unique,
)
from energy_pipeline.storage import Lake
from energy_pipeline.warehouse import Warehouse

log = logging.getLogger(__name__)

DATASET_URL = (
    "https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade-dec-e-fec"
)
INDICATORS = {"DEC": "dec_hours", "FEC": "fec_count", "NumCon": "consumers"}
MAX_PLAUSIBLE_DEC_HOURS_MONTH = 744.0
MAX_PLAUSIBLE_FEC_MONTH = 500.0
FIRST_YEAR = 2020
VALUE_FILE = "indicadores-continuidade-coletivos-2020-2029.parquet"
LIMIT_FILE = "indicadores-continuidade-coletivos-limite.csv"


def _decimal(column: str) -> pl.Expr:
    """ANEEL writes decimals with a comma, sometimes without the leading zero (",08")."""
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(",", ".")
        .str.replace(r"^\.", "0.")
        .cast(pl.Float64, strict=False)
    )


def transform_values(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Long indicator rows to one row per (distributor, set, month). Returns (valid, rejected)."""
    rows = raw.filter(pl.col("SigIndicador").str.strip_chars().is_in(list(INDICATORS))).select(
        pl.col("NumCNPJ").cast(pl.Utf8).str.strip_chars().alias("distributor_cnpj"),
        pl.col("SigAgente").cast(pl.Utf8).str.strip_chars().alias("distributor_acronym"),
        pl.col("IdeConjUndConsumidoras").cast(pl.Utf8).str.strip_chars().alias("set_id"),
        pl.col("DscConjUndConsumidoras").cast(pl.Utf8).str.strip_chars().alias("set_name"),
        pl.col("SigIndicador").str.strip_chars().alias("indicator"),
        pl.col("AnoIndice")
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Int32, strict=False)
        .alias("year"),
        pl.col("NumPeriodoIndice")
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Int32, strict=False)
        .alias("month_number"),
        _decimal("VlrIndiceEnviado").alias("value"),
        pl.col("DatGeracaoConjuntoDados").cast(pl.Utf8).alias("generated_on"),
    )
    keys = ["distributor_cnpj", "set_id", "year", "month_number", "value"]
    valid_mask = pl.all_horizontal([pl.col(key).is_not_null() for key in keys]) & pl.col(
        "month_number"
    ).is_between(1, 12)
    valid, rejected = rows.filter(valid_mask), rows.filter(~valid_mask)
    valid = valid.sort("generated_on").unique(
        subset=["distributor_cnpj", "set_id", "year", "month_number", "indicator"],
        keep="last",
        maintain_order=True,
    )
    names = valid.unique(subset=["distributor_cnpj", "set_id"], keep="last").select(
        "distributor_cnpj", "distributor_acronym", "set_id", "set_name"
    )
    wide = (
        valid.with_columns(pl.col("indicator").replace_strict(INDICATORS))
        .pivot(
            on="indicator",
            index=["distributor_cnpj", "set_id", "year", "month_number"],
            values="value",
            aggregate_function="last",
        )
        .with_columns(pl.date(pl.col("year"), pl.col("month_number"), 1).alias("month"))
        .join(names, on=["distributor_cnpj", "set_id"], how="left")
    )
    for column in INDICATORS.values():
        if column not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
    wide = wide.with_columns(pl.col("consumers").round(0).cast(pl.Int64, strict=False))
    return wide.sort("month", "distributor_cnpj", "set_id"), rejected


def transform_limits(raw: pl.DataFrame) -> pl.DataFrame:
    """Annual DEC and FEC limit per set: one row per (set, year) with both limits as columns."""
    limits = (
        raw.select(
            pl.col("NumCNPJ").cast(pl.Utf8).str.strip_chars().alias("distributor_cnpj"),
            pl.col("IdeConjUndConsumidoras").cast(pl.Utf8).str.strip_chars().alias("set_id"),
            pl.col("SigIndicador").cast(pl.Utf8).str.strip_chars().alias("indicator"),
            pl.col("AnoLimiteQualidade").cast(pl.Utf8).cast(pl.Int32, strict=False).alias("year"),
            _decimal("VlrLimite").alias("limit"),
        )
        .filter(pl.col("indicator").is_in(["DEC", "FEC"]) & pl.col("limit").is_not_null())
        .unique(subset=["distributor_cnpj", "set_id", "year", "indicator"], keep="last")
    )
    return limits.pivot(
        on="indicator", index=["distributor_cnpj", "set_id", "year"], values="limit"
    ).rename({"DEC": "dec_limit_hours", "FEC": "fec_limit_count"})


def attach_limits(values: pl.DataFrame, limits: pl.DataFrame) -> pl.DataFrame:
    """The annual limit of the month's year; sets without a limit keep nulls."""
    for column in ("dec_limit_hours", "fec_limit_count"):
        if column not in limits.columns:
            limits = limits.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
    return values.join(limits, on=["distributor_cnpj", "set_id", "year"], how="left")


def reliability_checks(
    valid: pl.DataFrame, rejected: pl.DataFrame, now: datetime
) -> list[CheckResult]:
    future = valid.filter(pl.col("month") > now.date()).height
    return [
        not_empty(valid),
        no_nulls(valid, ["distributor_cnpj", "set_id", "month"]),
        unique(valid, ["distributor_cnpj", "set_id", "month"]),
        between(valid, "dec_hours", 0.0, MAX_PLAUSIBLE_DEC_HOURS_MONTH),
        between(valid, "fec_count", 0.0, MAX_PLAUSIBLE_FEC_MONTH),
        between(valid, "consumers", 0.0, None),
        CheckResult("no_future_months", future == 0, f"{future} rows in the future"),
        CheckResult(
            "rejected_rows", rejected.height == 0, f"{rejected.height} rows rejected", Severity.WARN
        ),
    ]


def _download(resource: opendata.Resource, directory: Path, lake: Lake) -> Path:
    path = directory / resource.name
    checksum, _ = opendata.download(resource.url, path)
    collected = datetime.now(UTC).date().isoformat()
    lake.upload_file(
        path,
        f"bronze/aneel/reliability/collected={collected}/{resource.name}",
        metadata={"source-url": resource.url, "sha256": checksum},
    )
    return path


def run(settings: Settings, first_year: int = FIRST_YEAR) -> dict[str, object]:
    now = datetime.now(UTC)
    lake = Lake(settings.lake_bucket, settings.s3_endpoint_url)
    warehouse = Warehouse(settings.warehouse_dsn)
    resources = {
        resource.name: resource
        for resource in opendata.ckan_resources(opendata.fetch_text(DATASET_URL))
    }
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        values_path = _download(resources[VALUE_FILE], directory, lake)
        limits_path = _download(resources[LIMIT_FILE], directory, lake)
        raw = pl.read_parquet(values_path)
        limits_raw = pl.read_csv(
            limits_path, separator=";", infer_schema=False, encoding="utf8-lossy"
        )
    valid, rejected = transform_values(raw)
    valid = attach_limits(valid.filter(pl.col("year") >= first_year), transform_limits(limits_raw))
    warnings = raise_on_errors(reliability_checks(valid, rejected, now))

    distributors = valid.select(
        pl.col("distributor_cnpj").alias("cnpj"), pl.col("distributor_acronym").alias("acronym")
    ).unique(subset=["cnpj"], keep="last")
    sets = valid.select("distributor_cnpj", "set_id", "set_name").unique(
        subset=["distributor_cnpj", "set_id"], keep="last"
    )
    facts = valid.select(
        "month",
        "distributor_cnpj",
        "set_id",
        "consumers",
        "dec_hours",
        "fec_count",
        "dec_limit_hours",
        "fec_limit_count",
    )
    with warehouse.session() as session:
        session.lock("reliability_monthly")
        session.upsert("distributor", distributors, keys=["cnpj"], touch_column=None)
        session.upsert(
            "consumer_unit_set", sets, keys=["distributor_cnpj", "set_id"], touch_column=None
        )
        session.execute(
            "DELETE FROM reliability_monthly WHERE month >= %s", (facts["month"].min(),)
        )
        session.upsert(
            "reliability_monthly",
            facts,
            keys=["month", "distributor_cnpj", "set_id"],
            touch_column=None,
        )
    return {
        "rows": facts.height,
        "distributors": distributors.height,
        "sets": sets.height,
        "warnings": warnings,
    }
