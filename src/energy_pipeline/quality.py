"""Lightweight data quality checks that gate the load into the warehouse.

ERROR checks stop the pipeline; WARN checks are only logged.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

import polars as pl

from energy_pipeline.period import Period

log = logging.getLogger(__name__)

SUBSYSTEMS = {"N", "NE", "S", "SE"}
# The capacity registry also lists the Paraguayan half of Itaipu (50 Hz units) as "PY".
CAPACITY_SUBSYSTEMS = SUBSYSTEMS | {"PY"}
KNOWN_SOURCE_TYPES = {"HYDRO", "WIND", "SOLAR", "THERMAL", "NUCLEAR"}
# Itaipu, the largest plant in the grid, peaks around 7 GW per 60 Hz/50 Hz sector.
MAX_PLANT_GENERATION_MWH = 15_000
# Angra 2 (~1.35 GW) is the largest single generating unit.
MAX_UNIT_CAPACITY_MW = 2_000
MAX_SUBSYSTEM_LOAD_MW = 150_000
MAX_CMO_BRL_MWH = 10_000
RESTRICTION_REASONS = {"REL", "CNF", "ENE", "PAR"}


class Severity(StrEnum):
    ERROR = "error"
    WARN = "warn"


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: str
    severity: Severity = Severity.ERROR

    def __str__(self) -> str:
        status = "PASS" if self.passed else f"FAIL[{self.severity}]"
        return f"{status} {self.name}: {self.details}"


class DataQualityError(Exception):
    def __init__(self, failures: list[CheckResult]) -> None:
        super().__init__("Data quality checks failed:\n" + "\n".join(map(str, failures)))
        self.failures = failures


# --- Checks -----------------------------------------------------------------


def not_empty(df: pl.DataFrame) -> CheckResult:
    return CheckResult("not_empty", df.height > 0, f"{df.height} rows")


def no_nulls(
    df: pl.DataFrame, columns: list[str], severity: Severity = Severity.ERROR
) -> CheckResult:
    counts = {column: df[column].null_count() for column in columns}
    offenders = {column: count for column, count in counts.items() if count}
    return CheckResult(
        f"no_nulls{columns}", not offenders, f"null counts: {offenders or 0}", severity
    )


def unique(df: pl.DataFrame, columns: list[str]) -> CheckResult:
    duplicates = df.height - df.select(columns).unique().height
    return CheckResult(f"unique{columns}", duplicates == 0, f"{duplicates} duplicated keys")


def between(
    df: pl.DataFrame,
    column: str,
    minimum: float | None = None,
    maximum: float | None = None,
    severity: Severity = Severity.ERROR,
) -> CheckResult:
    condition = pl.lit(False)
    if minimum is not None:
        condition = condition | (pl.col(column) < minimum)
    if maximum is not None:
        condition = condition | (pl.col(column) > maximum)
    outside = df.filter(condition).height
    return CheckResult(
        f"between[{column}, {minimum}, {maximum}]",
        outside == 0,
        f"{outside} rows outside the range",
        severity,
    )


def within_period(df: pl.DataFrame, column: str, period: Period) -> CheckResult:
    outside = df.filter((pl.col(column) < period.start) | (pl.col(column) >= period.end)).height
    return CheckResult(f"within_period[{column}, {period}]", outside == 0, f"{outside} rows")


def accepted_values(
    df: pl.DataFrame, column: str, accepted: Iterable[str], severity: Severity = Severity.ERROR
) -> CheckResult:
    unexpected = sorted(set(df[column].drop_nulls().unique().to_list()) - set(accepted))
    return CheckResult(
        f"accepted_values[{column}]", not unexpected, f"unexpected: {unexpected}", severity
    )


def ratio_at_most(
    name: str, part: int, total: int, threshold: float, severity: Severity = Severity.ERROR
) -> CheckResult:
    ratio = part / total if total else 0.0
    return CheckResult(
        name, ratio <= threshold, f"{part}/{total} = {ratio:.2%} (max {threshold:.2%})", severity
    )


def completeness(df: pl.DataFrame, expected_rows: int, minimum_ratio: float) -> CheckResult:
    ratio = df.height / expected_rows if expected_rows else 0.0
    return CheckResult(
        "completeness",
        ratio >= minimum_ratio,
        f"{df.height}/{expected_rows} expected rows = {ratio:.2%} (min {minimum_ratio:.0%})",
    )


# --- Suites per dataset -------------------------------------------------------


def load_curve_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["subsystem_id", "measured_at", "load_mw_avg"]),
        unique(df, ["subsystem_id", "measured_at"]),
        within_period(df, "measured_at", period),
        accepted_values(df, "subsystem_id", SUBSYSTEMS),
        between(df, "load_mw_avg", 0, MAX_SUBSYSTEM_LOAD_MW),
        completeness(df, len(SUBSYSTEMS) * period.hours, minimum_ratio=0.98),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def plant_generation_checks(
    df: pl.DataFrame,
    period: Period,
    rejected_rows: int,
    missing_measurements: int,
    max_rejected_ratio: float,
) -> list[CheckResult]:
    total = df.height + rejected_rows + missing_measurements
    return [
        not_empty(df),
        no_nulls(df, ["plant_id", "measured_at", "generation_mwh", "source_type"]),
        unique(df, ["plant_id", "measured_at"]),
        within_period(df, "measured_at", period),
        accepted_values(df, "subsystem_id", SUBSYSTEMS),
        between(df, "generation_mwh", maximum=MAX_PLANT_GENERATION_MWH),
        # Thermal plants may report small negative values (auxiliary consumption).
        between(df, "generation_mwh", minimum=0, severity=Severity.WARN),
        accepted_values(df, "source_type", KNOWN_SOURCE_TYPES, Severity.WARN),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
        # Around 10-15% of plant-hours come without a measurement in the source.
        ratio_at_most("missing_measurements", missing_measurements, total, 0.25, Severity.WARN),
    ]


def generation_capacity_checks(
    df: pl.DataFrame, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        unique(df, ["unit_id"]),
        accepted_values(df, "subsystem_id", CAPACITY_SUBSYSTEMS),
        accepted_values(df, "source_type", KNOWN_SOURCE_TYPES, Severity.WARN),
        between(df, "capacity_mw", 0, MAX_UNIT_CAPACITY_MW, Severity.WARN),
        no_nulls(df, ["operation_start_date"], Severity.WARN),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def marginal_cost_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    present = set(df["measured_at"].dt.date().unique().to_list())
    all_days = {(period.start + timedelta(days=n)).date() for n in range(period.hours // 24)}
    missing_days = sorted(str(day) for day in all_days - present)
    return [
        not_empty(df),
        no_nulls(df, ["subsystem_id", "measured_at", "cmo_brl_mwh"]),
        unique(df, ["subsystem_id", "measured_at"]),
        within_period(df, "measured_at", period),
        accepted_values(df, "subsystem_id", SUBSYSTEMS),
        between(df, "cmo_brl_mwh", 0, MAX_CMO_BRL_MWH, Severity.WARN),
        # ONS skips whole days of CMO in about a quarter of the months (Jan/2023
        # has only 21 of 31 days). Missing days are filled downstream and flagged.
        completeness(df, len(SUBSYSTEMS) * period.half_hours, minimum_ratio=0.6),
        CheckResult(
            "missing_days",
            not missing_days,
            f"days without CMO: {missing_days or 'none'} (filled from the previous day)",
            Severity.WARN,
        ),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def cluster_curtailment_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    """Checks on the enriched (gold) cluster table."""
    total = df.height + rejected_rows
    clusters = df["cluster_id"].n_unique()
    curtailed = df["curtailed_mwh"].sum()
    unpriced = df.filter(pl.col("price_brl_mwh").is_null())["curtailed_mwh"].sum()
    return [
        not_empty(df),
        no_nulls(df, ["cluster_id", "measured_at", "subsystem_id"]),
        unique(df, ["cluster_id", "measured_at"]),
        within_period(df, "measured_at", period),
        accepted_values(df, "subsystem_id", SUBSYSTEMS),
        # ONS occasionally reports more than 30 minutes in a half-hour (up to 56 in
        # Sep/2023). Minutes are only used as proportions between reasons, so the
        # official cut is unaffected.
        between(df, "minutes_restricted", 0, 30, Severity.WARN),
        between(df, "gnra_mw_avg", minimum=0, severity=Severity.WARN),
        accepted_values(df, "reason_code", RESTRICTION_REASONS, Severity.WARN),
        CheckResult(
            "completeness",
            df.height >= 0.95 * clusters * period.half_hours,
            f"{df.height} rows for {clusters} clusters x {period.half_hours} half-hours",
            Severity.WARN,
        ),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
        # The loss in R$ needs a price for every curtailed half-hour.
        ratio_at_most(
            "unpriced_curtailment", round(unpriced), round(curtailed), 0.01, Severity.WARN
        ),
    ]


def plant_curtailment_checks(
    df: pl.DataFrame,
    period: Period,
    rejected_rows: int,
    max_rejected_ratio: float,
    reconciliation: dict[str, float],
    tolerance: float,
) -> list[CheckResult]:
    """Checks on the allocated (gold) plant table and its reconciliation with ONS."""
    total = df.height + rejected_rows
    official = reconciliation["official_mwh"]
    allocated = reconciliation["allocated_mwh"]
    deviation = abs(allocated - official) / official if official else 0.0
    naive_ratio = reconciliation["naive_mwh"] / official if official else 0.0
    return [
        not_empty(df),
        no_nulls(df, ["plant_id", "cluster_id", "measured_at", "subsystem_id"]),
        unique(df, ["plant_id", "measured_at"]),
        within_period(df, "measured_at", period),
        accepted_values(df, "subsystem_id", SUBSYSTEMS),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
        CheckResult(
            "reconciliation_with_ons",
            deviation <= tolerance,
            f"allocated {allocated:,.0f} MWh vs official GNRa {official:,.0f} MWh "
            f"= {deviation:.2%} deviation (max {tolerance:.2%}); "
            f"unallocated {reconciliation['unallocated_mwh']:,.0f} MWh; "
            f"plant-level naive estimate = {naive_ratio:.2f}x official",
        ),
    ]


def enforce(results: list[CheckResult]) -> None:
    """Log every result and raise if any ERROR check failed."""
    for result in results:
        log.log(logging.INFO if result.passed else logging.WARNING, "%s", result)
    failures = [r for r in results if not r.passed and r.severity is Severity.ERROR]
    if failures:
        raise DataQualityError(failures)
