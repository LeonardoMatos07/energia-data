"""Monthly load of the ANEEL DEC and FEC continuity indicators (ODbL)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task


@dag(
    dag_id="aneel_reliability_pipeline",
    description="Load the ANEEL DEC and FEC indicators per distributor and electrical set",
    schedule="0 9 6 * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["etl", "aneel", "quality"],
    doc_md=__doc__,
)
def aneel_reliability_pipeline() -> None:
    @task
    def load() -> dict[str, Any]:
        from energy_pipeline import aneel_reliability
        from energy_pipeline.config import Settings

        return aneel_reliability.run(Settings.from_env())

    @task
    def notify_portal() -> dict[str, Any]:
        from energy_pipeline import notify
        from energy_pipeline.config import Settings

        return notify.request_revalidation(Settings.from_env(), ["reliability"])

    load() >> notify_portal()


aneel_reliability_pipeline()
