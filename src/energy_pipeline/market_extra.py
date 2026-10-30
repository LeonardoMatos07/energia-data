"""Smaller market datasets: CCEE consumption per market and agents per class, ANEEL tariff flags.

CCEE (CC BY 4.0): market-wide aggregates only; the per-agent datasets are not loaded.
ANEEL (ODbL): the monthly tariff flag and its additional charge.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from energy_pipeline import opendata
from energy_pipeline.ccee import _fetch_to_bronze, _resources, raise_on_errors, read_csv
from energy_pipeline.config import Settings
from energy_pipeline.quality import (
    CheckResult,
    between,
    no_nulls,
    not_empty,
    unique,
)
from energy_pipeline.storage import Lake
from energy_pipeline.warehouse import Warehouse

log = logging.getLogger(__name__)

FLAGS_URL = "https://dadosabertos.aneel.gov.br/dataset/bandeiras-tarifarias"
FLAGS_FILE = "bandeira-tarifaria-acionamento.csv"
ACCEPTED_FLAGS = {"Verde", "Amarela", "Vermelha P1", "Vermelha P2", "Escassez Hídrica"}
MAX_PLAUSIBLE_FLAG_BRL_MWH = 500.0
MAX_PLAUSIBLE_CONSUMPTION_MW = 200_000.0


def _decimal(column: str) -> pl.Expr:
    """Both portals write decimals with a point or a comma, sometimes without the leading zero."""
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(",", ".")
        .str.replace(r"^\.", "0.")
        .cast(pl.Float64, strict=False)
    )


def _month(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.slice(0, 6)
        .str.to_date("%Y%m", strict=False)
    )


def transform_flags(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    parsed = raw.select(
        pl.col("DatCompetencia")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.to_date("%Y-%m-%d", strict=False)
        .alias("month"),
        pl.col("NomBandeiraAcionada").cast(pl.Utf8).str.strip_chars().alias("flag"),
        _decimal("VlrAdicionalBandeira").alias("additional_brl_per_mwh"),
    )
    keys = ["month", "flag", "additional_brl_per_mwh"]
    mask = pl.all_horizontal([pl.col(key).is_not_null() for key in keys])
    valid = parsed.filter(mask).unique(subset=["month"], keep="last", maintain_order=True)
    return valid.sort("month"), parsed.filter(~mask)


def flag_checks(valid: pl.DataFrame, rejected: pl.DataFrame) -> list[CheckResult]:
    unexpected = sorted(set(valid["flag"].to_list()) - ACCEPTED_FLAGS)
    return [
        not_empty(valid),
        no_nulls(valid, ["month", "flag", "additional_brl_per_mwh"]),
        unique(valid, ["month"]),
        between(valid, "additional_brl_per_mwh", 0.0, MAX_PLAUSIBLE_FLAG_BRL_MWH),
        CheckResult("known_flags", not unexpected, f"unexpected: {unexpected}"),
        CheckResult("rejected_rows", rejected.height == 0, f"{rejected.height} rows rejected"),
    ]


def transform_consumption(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    parsed = raw.select(
        _month("MES_REFERENCIA").alias("month"),
        _decimal("CONSUMO_TOTAL_ACR").alias("regulated_mw_avg"),
        _decimal("CONSUMO_TOTAL_ACL").alias("free_mw_avg"),
    )
    mask = pl.all_horizontal([pl.col(c).is_not_null() for c in parsed.columns])
    valid = parsed.filter(mask).unique(subset=["month"], keep="last", maintain_order=True)
    return valid.sort("month"), parsed.filter(~mask)


def transform_agents(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    parsed = raw.select(
        _month("MES_REFERENCIA").alias("month"),
        pl.col("CLASSE").cast(pl.Utf8).str.strip_chars().alias("agent_class"),
        pl.col("QUANTIDADE_AGENTE_CONTABILIZACAO")
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Int32, strict=False)
        .alias("agents"),
    )
    mask = pl.all_horizontal([pl.col(c).is_not_null() for c in parsed.columns])
    valid = parsed.filter(mask).unique(subset=["month", "agent_class"], keep="last")
    return valid.sort("month", "agent_class"), parsed.filter(~mask)


def consumption_checks(valid: pl.DataFrame, rejected: pl.DataFrame) -> list[CheckResult]:
    return [
        not_empty(valid),
        no_nulls(valid, ["month", "regulated_mw_avg", "free_mw_avg"]),
        unique(valid, ["month"]),
        between(valid, "regulated_mw_avg", 0.0, MAX_PLAUSIBLE_CONSUMPTION_MW),
        between(valid, "free_mw_avg", 0.0, MAX_PLAUSIBLE_CONSUMPTION_MW),
        CheckResult("rejected_rows", rejected.height == 0, f"{rejected.height} rows rejected"),
    ]


def agents_checks(valid: pl.DataFrame, rejected: pl.DataFrame) -> list[CheckResult]:
    return [
        not_empty(valid),
        no_nulls(valid, ["month", "agent_class", "agents"]),
        unique(valid, ["month", "agent_class"]),
        between(valid, "agents", 0.0, 1_000_000.0),
        CheckResult("rejected_rows", rejected.height == 0, f"{rejected.height} rows rejected"),
    ]


def run_flags(settings: Settings) -> dict[str, object]:
    lake, warehouse = (
        Lake(settings.lake_bucket, settings.s3_endpoint_url),
        Warehouse(settings.warehouse_dsn),
    )
    resources = {r.name: r for r in opendata.ckan_resources(opendata.fetch_text(FLAGS_URL))}
    resource = resources[FLAGS_FILE]
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / FLAGS_FILE
        checksum, _ = opendata.download(resource.url, path)
        collected = datetime.now(UTC).date().isoformat()
        lake.upload_file(
            path,
            f"bronze/aneel/tariff_flags/collected={collected}/{FLAGS_FILE}",
            metadata={"source-url": resource.url, "sha256": checksum},
        )
        raw = pl.read_csv(path, separator=";", infer_schema=False, encoding="utf8-lossy")
    valid, rejected = transform_flags(raw)
    warnings = raise_on_errors(flag_checks(valid, rejected))
    with warehouse.session() as session:
        session.lock("tariff_flag_monthly")
        session.upsert("tariff_flag_monthly", valid, keys=["month"], touch_column=None)
    return {"rows": valid.height, "warnings": warnings}


def _load_years(slug: str, prefix: str, lake: Lake) -> pl.DataFrame:
    frames = []
    for name, resource in _resources(slug).items():
        if not name.startswith(prefix):
            continue
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = _fetch_to_bronze(resource, slug, lake, Path(tmp))
            frames.append(read_csv(path))
    return pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()


def run_free_market(settings: Settings) -> dict[str, object]:
    lake, warehouse = (
        Lake(settings.lake_bucket, settings.s3_endpoint_url),
        Warehouse(settings.warehouse_dsn),
    )
    consumption_raw = _load_years(
        "consumo_mensal_ambiente_comercializacao", "consumo_mensal_ambiente_comercializacao_", lake
    )
    agents_raw = _load_years("agente_qtd_contabilizacao", "agente_qtd_contabilizacao_", lake)
    consumption, consumption_rejected = transform_consumption(consumption_raw)
    agents, agents_rejected = transform_agents(agents_raw)
    warnings = raise_on_errors(consumption_checks(consumption, consumption_rejected))
    warnings += raise_on_errors(agents_checks(agents, agents_rejected))
    with warehouse.session() as session:
        session.lock("market_free")
        session.upsert("market_consumption_monthly", consumption, keys=["month"], touch_column=None)
        session.upsert(
            "market_agents_monthly", agents, keys=["month", "agent_class"], touch_column=None
        )
    return {
        "consumption_rows": consumption.height,
        "agents_rows": agents.height,
        "warnings": warnings,
    }
