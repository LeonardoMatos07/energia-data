from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from energy_pipeline import transform
from energy_pipeline.config import Settings
from energy_pipeline.curtailment import HALF_HOUR
from energy_pipeline.period import Period, periods_between
from energy_pipeline.sources import SOLAR_CLUSTER_CURTAILMENT, WIND_CLUSTER_CURTAILMENT, Source
from energy_pipeline.storage import Lake
from energy_pipeline.warehouse import Warehouse

CLUSTER_SOURCES = {"wind": WIND_CLUSTER_CURTAILMENT, "solar": SOLAR_CLUSTER_CURTAILMENT}

PUBLISHED_QUERIES = {
    "subsystem": (
        "select source, month, sum(curtailed_mwh) from subsystem_monthly_curtailment "
        "where subsystem <> 'SIN' and source <> 'total' group by 1, 2"
    ),
    "state": (
        "select source, month, sum(curtailed_mwh) from state_monthly_curtailment "
        "where source <> 'total' group by 1, 2"
    ),
    "plant": (
        "select p.source, m.month, sum(m.curtailed_mwh) from plant_monthly_curtailment m "
        "join plant p on p.id = m.plant_id where p.kind = 'plant' group by 1, 2"
    ),
}


@dataclass(frozen=True)
class Deviation:
    period: Period
    source: str
    level: str
    official_mwh: float
    published_mwh: float

    @property
    def ratio(self) -> float:
        if self.official_mwh == 0:
            return 0.0 if self.published_mwh == 0 else float("inf")
        return abs(self.published_mwh - self.official_mwh) / self.official_mwh


def official_curtailment_mwh(source: Source, period: Period, lake: Lake) -> float | None:
    prefix = f"bronze/ons/{source.name}/"
    key = lake.latest_key(prefix, source.file_name(period))
    if key is None:
        return None
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / source.file_name(period)
        lake.download_file(key, path)
        raw = transform.read_raw(path, source.file_format)
    gnra = raw.select(transform.parse_float("val_geracaonaorealizadaapurada").alias("gnra"))
    return float(gnra["gnra"].fill_null(0).clip(lower_bound=0).sum()) * HALF_HOUR


def published_curtailment_mwh(
    settings: Settings, first: Period, last: Period
) -> dict[tuple[str, str, Period], float]:
    published: dict[tuple[str, str, Period], float] = {}
    with Warehouse(settings.warehouse_dsn).session() as session:
        for level, query in PUBLISHED_QUERIES.items():
            for source, month, total in session.fetchall(query):
                period = Period(month.year, month.month)
                if first <= period <= last:
                    published[(level, source, period)] = float(total)
    return published


def reconcile_periods(settings: Settings, first: Period, last: Period) -> list[Deviation]:
    lake = Lake(settings.lake_bucket, settings.s3_endpoint_url)
    published = published_curtailment_mwh(settings, first, last)
    deviations = []
    for period in periods_between(first, last):
        for name, source in CLUSTER_SOURCES.items():
            if period not in source.periods_to_process([period]):
                continue
            official = official_curtailment_mwh(source, period, lake)
            if official is None:
                continue
            for level in PUBLISHED_QUERIES:
                deviations.append(
                    Deviation(
                        period, name, level, official, published.get((level, name, period), 0.0)
                    )
                )
    return deviations


def render(deviations: list[Deviation], tolerance: float) -> str:
    lines = [
        f"{'month':<9}{'source':<7}{'level':<10}{'official GWh':>14}{'published GWh':>15}"
        f"{'deviation':>11}  status"
    ]
    for d in deviations:
        status = "ok" if d.ratio <= tolerance else "FAIL"
        lines.append(
            f"{d.period!s:<9}{d.source:<7}{d.level:<10}{d.official_mwh / 1e3:>14,.1f}"
            f"{d.published_mwh / 1e3:>15,.1f}{d.ratio:>11.3%}  {status}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="energy_pipeline.reconcile")
    parser.add_argument("--from", dest="first", type=Period.parse, required=True)
    parser.add_argument("--to", dest="last", type=Period.parse, required=True)
    parser.add_argument("--tolerance", type=float)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING)
    settings = Settings.from_env()
    tolerance = args.tolerance if args.tolerance is not None else settings.reconciliation_tolerance
    deviations = reconcile_periods(settings, args.first, args.last)
    print(render(deviations, tolerance))
    failures = [d for d in deviations if d.ratio > tolerance]
    print(f"\n{len(deviations)} comparisons, {len(failures)} above {tolerance:.2%}")
    return 1 if failures or not deviations else 0


if __name__ == "__main__":
    sys.exit(main())
