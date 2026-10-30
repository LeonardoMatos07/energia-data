"""Daily load of the CCEE PLD (official hourly, shadow, weekly) and its daily/monthly aggregates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import Param, dag, task


@dag(
    dag_id="ccee_pld_pipeline",
    description="Load the CCEE PLD series (CC BY 4.0) into the warehouse",
    schedule="30 8 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=10)},
    params={"backfill": Param(False, type="boolean", description="Load every year, not 2.")},
    tags=["etl", "ccee", "market"],
    doc_md=__doc__,
)
def ccee_pld_pipeline() -> None:
    @task
    def load(**context: Any) -> list[dict[str, Any]]:
        from energy_pipeline import ccee
        from energy_pipeline.config import Settings

        return ccee.run_all(Settings.from_env(), bool(context["params"].get("backfill")))

    @task
    def load_free_market() -> dict[str, Any]:
        from energy_pipeline import market_extra
        from energy_pipeline.config import Settings

        return market_extra.run_free_market(Settings.from_env())

    @task
    def notify_portal() -> dict[str, Any]:
        from energy_pipeline import notify
        from energy_pipeline.config import Settings

        return notify.request_revalidation(Settings.from_env(), ["market"])

    [load(), load_free_market()] >> notify_portal()


ccee_pld_pipeline()
