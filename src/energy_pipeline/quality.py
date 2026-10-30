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
CAPACITY_SUBSYSTEMS = SUBSYSTEMS | {"PY"}
KNOWN_SOURCE_TYPES = {"HYDRO", "WIND", "SOLAR", "THERMAL", "NUCLEAR"}
MAX_PLANT_GENERATION_MWH = 15_000
MAX_UNIT_CAPACITY_MW = 2_000
MAX_SUBSYSTEM_LOAD_MW = 150_000
MAX_CMO_BRL_MWH = 10_000
MAX_BRAZIL_LOAD_MW = 250_000
GENERATION_NOISE_MW = 1.0
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


ENERGY_BALANCE_SOURCES = ["hydro_mw_avg", "thermal_mw_avg", "wind_mw_avg", "solar_mw_avg"]


def energy_balance_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["subsystem_id", "measured_at"]),
        unique(df, ["subsystem_id", "measured_at"]),
        within_period(df, "measured_at", period),
        # the ONS file also carries the national total ("SIN"), which is derived in the gold layer
        accepted_values(df, "subsystem_id", SUBSYSTEMS | {"SIN"}),
        # night-time solar readings drift slightly below zero (seen: -0.16 MW); that is noise
        *[
            between(df, column, -GENERATION_NOISE_MW, MAX_BRAZIL_LOAD_MW)
            for column in ENERGY_BALANCE_SOURCES
        ],
        between(df, "load_mw_avg", 0, MAX_BRAZIL_LOAD_MW),
        completeness(
            df.filter(pl.col("subsystem_id") != "SIN"),
            len(SUBSYSTEMS) * period.hours,
            minimum_ratio=0.98,
        ),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def within_days(df: pl.DataFrame, column: str, period: Period) -> CheckResult:
    outside = df.filter(
        (pl.col(column) < period.start.date()) | (pl.col(column) >= period.end.date())
    ).height
    return CheckResult(f"within_period[{column}, {period}]", outside == 0, f"{outside} rows")


def every_day_has_all(df: pl.DataFrame, key: str, expected: set[str]) -> CheckResult:
    """Each day that appears in the file must have every subsystem."""
    counts = df.group_by("day").agg(pl.col(key).n_unique().alias("n"))
    incomplete = counts.filter(pl.col("n") < len(expected)).height
    return CheckResult(
        f"every_day_has_all[{key}]", incomplete == 0, f"{incomplete} days incomplete"
    )


MAX_STORAGE_PCT = 100.0
MAX_RESERVOIR_STORAGE_PCT = 200.0
MAX_ENERGY_MW = 500_000
ENA_NOISE_MW = 50.0
MAX_FLOW_M3S = 100_000
MAX_SPILLED_ENERGY_MWH = 100_000


def subsystem_storage_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["subsystem_id", "day"]),
        unique(df, ["subsystem_id", "day"]),
        within_days(df, "day", period),
        accepted_values(df, "subsystem_id", SUBSYSTEMS),
        every_day_has_all(df, "subsystem_id", SUBSYSTEMS),
        between(df, "storage_pct", 0, MAX_STORAGE_PCT),
        between(df, "storage_mw_month", 0, MAX_ENERGY_MW),
        between(df, "storage_max_mw_month", 0, MAX_ENERGY_MW),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def subsystem_inflow_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["subsystem_id", "day"]),
        unique(df, ["subsystem_id", "day"]),
        within_days(df, "day", period),
        accepted_values(df, "subsystem_id", SUBSYSTEMS),
        every_day_has_all(df, "subsystem_id", SUBSYSTEMS),
        between(df, "ena_gross_mw_avg", 0, MAX_ENERGY_MW),
        between(df, "ena_storable_mw_avg", 0, MAX_ENERGY_MW),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def reservoir_storage_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["reservoir_name", "day"]),
        unique(df, ["reservoir_name", "day"]),
        within_days(df, "day", period),
        # Below the minimum operating volume a reservoir's EAR is negative (2015: -83%) and while it
        # spills it is above 100% (up to 379%): only absurd values (a unit change) are blocked.
        between(df, "storage_pct", -MAX_RESERVOIR_STORAGE_PCT, 1000),
        between(df, "storage_pct", 0, 200, Severity.WARN),
        between(df, "storage_mw_month", -MAX_ENERGY_MW, MAX_ENERGY_MW),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def reservoir_inflow_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["reservoir_name", "day"]),
        unique(df, ["reservoir_name", "day"]),
        within_days(df, "day", period),
        # derived from natural flows, it drifts slightly below zero (seen: -6.8 MW in 2024)
        between(df, "ena_gross_mw_avg", -ENA_NOISE_MW, MAX_ENERGY_MW),
        between(df, "ena_storable_mw_avg", 0, MAX_ENERGY_MW),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def hydro_operations_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["reservoir_id", "day"]),
        unique(df, ["reservoir_id", "day"]),
        within_days(df, "day", period),
        *[
            between(df, column, 0, MAX_FLOW_M3S)
            for column in ("inflow_m3s", "outflow_m3s", "turbined_m3s", "spilled_m3s")
        ],
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def reservoirs_checks(
    df: pl.DataFrame, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["reservoir_id", "name"]),
        unique(df, ["reservoir_id"]),
        accepted_values(df, "subsystem_id", SUBSYSTEMS, Severity.WARN),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def spilled_energy_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["plant_code", "measured_at"]),
        unique(df, ["plant_code", "measured_at"]),
        within_period(df, "measured_at", period),
        between(df, "spilled_energy_mwh", -GENERATION_NOISE_MW, MAX_SPILLED_ENERGY_MWH),
        between(df, "spilled_turbinable_energy_mwh", -GENERATION_NOISE_MW, MAX_SPILLED_ENERGY_MWH),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


MAX_INTERCHANGE_MW = 40_000
MAX_THERMAL_PLANT_MW = 15_000


def interchange_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    """Gates for the national and the international interchange (origin, destination, hour)."""
    total = df.height + rejected_rows
    pairs = df.select("origin", "destination").unique().height
    return [
        not_empty(df),
        no_nulls(df, ["origin", "destination", "measured_at"]),
        unique(df, ["origin", "destination", "measured_at"]),
        within_period(df, "measured_at", period),
        between(df, "flow_mw_avg", -MAX_INTERCHANGE_MW, MAX_INTERCHANGE_MW),
        between(df, "scheduled_mw_avg", -MAX_INTERCHANGE_MW, MAX_INTERCHANGE_MW),
        completeness(df, pairs * period.hours, minimum_ratio=0.9),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def thermal_dispatch_checks(
    df: pl.DataFrame, period: Period, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["plant_code", "measured_at", "subsystem_id", "verified_total_mwh"]),
        unique(df, ["plant_code", "measured_at"]),
        within_period(df, "measured_at", period),
        accepted_values(df, "subsystem_id", SUBSYSTEMS),
        between(df, "verified_total_mwh", -GENERATION_NOISE_MW, MAX_THERMAL_PLANT_MW),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


GRID_SUBSYSTEMS = SUBSYSTEMS | {"PY", "AR", "UY", "VE"}
MAX_LINE_KV = 1_200
MAX_LINE_KM = 3_000
MAX_TRANSFORMER_MVA = 3_000
BRAZIL_LATITUDE = (-34.0, 6.0)
BRAZIL_LONGITUDE = (-75.0, -28.0)


def transmission_lines_checks(
    df: pl.DataFrame, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["equipment_code"]),
        unique(df, ["equipment_code"]),
        between(df, "voltage_kv", 0, MAX_LINE_KV),
        between(df, "length_km", 0, MAX_LINE_KM),
        between(df, "capacity_long_no_limit_a", 0, 20_000),
        accepted_values(df, "from_subsystem", GRID_SUBSYSTEMS, Severity.WARN),
        accepted_values(df, "to_subsystem", GRID_SUBSYSTEMS, Severity.WARN),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def substations_checks(
    df: pl.DataFrame, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["station_id"]),
        unique(df, ["station_id"]),
        between(df, "voltage_kv", 0, MAX_LINE_KV),
        accepted_values(df, "subsystem", SUBSYSTEMS),
        between(df, "latitude", *BRAZIL_LATITUDE, Severity.WARN),
        between(df, "longitude", *BRAZIL_LONGITUDE, Severity.WARN),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
    ]


def transformation_capacity_checks(
    df: pl.DataFrame, rejected_rows: int, max_rejected_ratio: float
) -> list[CheckResult]:
    total = df.height + rejected_rows
    return [
        not_empty(df),
        no_nulls(df, ["equipment_code"]),
        unique(df, ["equipment_code"]),
        between(df, "rated_mva", 0, MAX_TRANSFORMER_MVA),
        between(df, "primary_kv", 0, MAX_LINE_KV),
        accepted_values(df, "subsystem", SUBSYSTEMS),
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
        between(df, "generation_mwh", minimum=0, severity=Severity.WARN),
        accepted_values(df, "source_type", KNOWN_SOURCE_TYPES, Severity.WARN),
        ratio_at_most("rejected_rows", rejected_rows, total, max_rejected_ratio),
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
    for result in results:
        log.log(logging.INFO if result.passed else logging.WARNING, "%s", result)
    failures = [r for r in results if not r.passed and r.severity is Severity.ERROR]
    if failures:
        raise DataQualityError(failures)
