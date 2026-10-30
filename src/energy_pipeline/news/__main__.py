from __future__ import annotations

import argparse
import logging

from energy_pipeline.config import Settings
from energy_pipeline.news.run import run_news


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="energy_pipeline.news", description="Collect sector news."
    )
    parser.add_argument("--source", action="append", help="restrict to these sources (slug)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    results = run_news(Settings.from_env(), only=args.source)
    print(f"{'source':<8}{'fetched':>8}{'stored':>8}{'off-topic':>10}{'rejected':>10}  note")
    for r in results:
        note = r.error or "; ".join(r.warnings)
        print(
            f"{r.slug:<8}{r.fetched:>8}{r.stored:>8}{r.skipped_off_topic:>10}"
            f"{len(r.rejected):>10}  {note}"
        )
    if any(r.error for r in results) and not any(r.stored for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
