"""Run the CCEE PLD or the ANEEL reliability loads outside Airflow.

python -m energy_pipeline.market_cli ccee [--backfill]
python -m energy_pipeline.market_cli reliability
"""

from __future__ import annotations

import argparse
import json
import logging

from energy_pipeline import aneel_reliability, ccee, market_extra
from energy_pipeline.config import Settings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=["ccee", "reliability", "free-market", "flags"])
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    if args.source == "ccee":
        result: object = ccee.run_all(settings, args.backfill)
    elif args.source == "free-market":
        result = market_extra.run_free_market(settings)
    elif args.source == "flags":
        result = market_extra.run_flags(settings)
    else:
        result = aneel_reliability.run(settings)
    print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()
