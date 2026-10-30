"""Daily load of ANEEL and MME acts published in the DOU into `regulatory_act`.

Inactive until a legitimate channel exists: the DOU portal forbids scraping in its robots.txt and
the official XML channel (INLABS) needs a registration made by a person. Without DOU_ENABLED and
a configured source the run only logs why it did nothing.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task


@dag(
    dag_id="dou_acts_pipeline",
    description="Daily load of official acts (ANEEL, MME) from the DOU",
    schedule="30 9 * * *",
    start_date=datetime(2026, 10, 30, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["news"],
    doc_md=__doc__,
)
def dou_acts_pipeline() -> Any:
    @task
    def load() -> dict[str, Any]:
        from energy_pipeline.config import Settings
        from energy_pipeline.news.dou import run_dou

        result = run_dou(Settings.from_env(), date.today() - timedelta(days=1))
        return {"day": str(result.day), "stored": result.stored, "skipped": result.skipped}

    load()


dou_acts_pipeline()
