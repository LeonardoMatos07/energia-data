"""Hourly collection of public sector news (ANEEL, MME, ANP, EPE) into `news_item`.

Light by design: a few dozen items a day. The raw listing goes to the bronze layer for audit, the
items go straight to Postgres after the gates, and the portal is told to revalidate the `news`
cache tag. Polite collection: robots.txt, identified User-Agent, 1 request per second per domain.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task


@dag(
    dag_id="news_pipeline",
    description="Hourly collection of public sector news",
    schedule="@hourly",
    start_date=datetime(2026, 10, 30, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["news"],
    doc_md=__doc__,
)
def news_pipeline() -> Any:
    @task
    def collect() -> list[dict[str, Any]]:
        from energy_pipeline.config import Settings
        from energy_pipeline.news.run import run_news

        results = run_news(Settings.from_env())
        if results and all(r.error for r in results):
            raise RuntimeError(
                "every news source failed: " + "; ".join(r.error or "" for r in results)
            )
        return [
            {"source": r.slug, "fetched": r.fetched, "stored": r.stored, "warnings": r.warnings}
            for r in results
        ]

    collect()


news_pipeline()
