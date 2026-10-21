"""Run a pipeline without Airflow, for local development, backfills and debugging.

Executes the same steps as the DAGs, sequentially::

    python -m energy_pipeline curtailment                  # reprocessing window
    python -m energy_pipeline curtailment --month 2026-09
    python -m energy_pipeline curtailment --from 2023-01 --to 2026-09
    python -m energy_pipeline grid --month 2026-09
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import UTC, datetime

from energy_pipeline import steps
from energy_pipeline.config import Settings
from energy_pipeline.period import Period, periods_between, trailing_periods
from energy_pipeline.sources import PIPELINES


def run_source(source_name: str, period: Period, settings: Settings) -> steps.Manifest:
    manifest = steps.run_extract(source_name, period, settings)
    manifest = steps.run_transform(manifest, settings)
    manifest = steps.run_validate(manifest, settings)
    return steps.run_record(manifest, settings)


def print_summary(manifests: list[steps.Manifest]) -> None:
    header = (
        f"{'source':<27}{'period':<9}{'changed':>8}{'raw':>10}{'prata':>10}"
        f"{'rejected':>10}{'missing':>9}"
    )
    print("\n" + header + "\n" + "-" * len(header))
    for m in manifests:
        print(
            f"{m['source']:<27}{m['period']:<9}{'yes' if m['source_changed'] else 'no':>8}"
            f"{m['raw_rows']:>10}{m['prata_rows']:>10}{m['rejected_rows']:>10}"
            f"{m['missing_measurements']:>9}"
        )
        if "reconciliation" in m:
            r = m["reconciliation"]
            print(
                f"    official GNRa {r['official_mwh'] / 1e3:,.1f} GWh, "
                f"allocated to plants {r['allocated_mwh'] / 1e3:,.1f} GWh"
            )
        for warning in m["quality_warnings"]:
            print(f"    warning: {warning}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="energy_pipeline", description=__doc__.splitlines()[0])
    parser.add_argument("pipeline", choices=sorted(PIPELINES))
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--month", type=Period.parse, help="single month, YYYY-MM")
    group.add_argument("--from", dest="first", type=Period.parse, help="first month, YYYY-MM")
    parser.add_argument("--to", dest="last", type=Period.parse, help="last month, YYYY-MM")
    parser.add_argument("--source", action="append", help="restrict to these sources")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    sources = PIPELINES[args.pipeline]
    if args.source:
        unknown = set(args.source) - set(sources)
        if unknown:
            parser.error(f"unknown sources for {args.pipeline}: {sorted(unknown)}")
        sources = {name: sources[name] for name in sources if name in args.source}

    last_complete = Period.from_datetime(datetime.now(UTC)).previous()
    if args.first:
        periods = periods_between(args.first, args.last or last_complete)
    elif args.month:
        periods = [args.month]
    elif args.pipeline == "curtailment":
        periods = trailing_periods(last_complete, settings.reprocess_months)
    else:
        periods = [last_complete]

    started = time.perf_counter()
    steps.prepare_infrastructure(settings)
    plan = {name: set(source.periods_to_process(periods)) for name, source in sources.items()}

    manifests = []
    for period in periods:
        # Sources are declared with dependencies first.
        for name in sources:
            if period in plan[name]:
                manifests.append(run_source(name, period, settings))

    if args.pipeline == "curtailment":
        for period in periods:
            published = steps.run_publish(period, settings)
            print(f"publish {period}: {published.get('rows', 'nothing to publish')}")
        try:
            steps.notify_portal(settings)
        except Exception as error:  # the data is loaded; the portal can be refreshed later
            logging.warning("Portal revalidation failed: %s", error)
    print_summary(manifests)
    print(f"\nDone in {time.perf_counter() - started:.0f}s")


if __name__ == "__main__":
    main()
