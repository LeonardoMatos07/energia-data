"""Monthly load of the ANEEL tariff flags (ODbL)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task


@dag(
    dag_id="aneel_tariff_flags_pipeline",
    description="Load the ANEEL tariff flag of each month",
    schedule="30 9 6 * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["etl", "aneel", "tariffs"],
    doc_md=__doc__,
)
def aneel_tariff_flags_pipeline() -> None:
    @task
    def load() -> dict[str, Any]:
        from energy_pipeline import market_extra
        from energy_pipeline.config import Settings

        return market_extra.run_flags(Settings.from_env())

    @task
    def notify_portal() -> dict[str, Any]:
        from energy_pipeline import notify
        from energy_pipeline.config import Settings

        return notify.request_revalidation(Settings.from_env(), ["market"])

    load() >> notify_portal()


aneel_tariff_flags_pipeline()
