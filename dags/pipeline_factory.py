"""Builds the pipeline DAGs.

Each source becomes a task group, mapped over the months to process::

    extract -> transform -> validate -> record

A source's transform waits for the validation of the sources it depends on
(e.g. plant curtailment needs the cluster totals and the prices). Pipelines that
feed the portal then ``publish`` each month (gold aggregates in the lake and in
Postgres) and ask the portal to regenerate its pages.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import Asset, Param, TaskGroup, dag, task

from energy_pipeline.period import Period, periods_between, trailing_periods
from energy_pipeline.sources import PIPELINES, SOURCES

MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"


def _settings() -> Any:
    # Imported lazily: DAG files are parsed often and must stay cheap to import.
    from energy_pipeline.config import Settings

    return Settings.from_env()


def _record_failure(context: dict[str, Any]) -> None:
    """Failed tasks also show up in carga_execucao (admin area)."""
    from energy_pipeline import steps

    task_instance = context["task_instance"]
    detail = f"{task_instance.task_id}[{task_instance.map_index}]: {context.get('exception')}"
    steps.record_failure(_settings(), task_instance.task_id.split(".")[0], detail[:2000])


def _month_param(description: str) -> Param:
    return Param(None, type=["null", "string"], pattern=MONTH_PATTERN, description=description)


def build_pipeline_dag(
    *,
    dag_id: str,
    pipeline: str,
    description: str,
    schedule: str,
    start_date: datetime,
    catchup: bool,
    window_months: int,
    publishes: bool,
    doc_md: str,
) -> Any:
    sources = PIPELINES[pipeline]
    lake_asset = Asset(f"s3://energy-lake/prata/{pipeline}")
    portal_asset = Asset("postgres://warehouse/curtailment_mensal")

    @dag(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        max_active_runs=1,
        # Transforms hold a month of half-hourly data in memory: keep parallelism modest.
        max_active_tasks=4,
        default_args={
            "owner": "data-engineering",
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": _record_failure,
        },
        params={
            "month": _month_param("Last month to process (YYYY-MM). Default: previous month."),
            "first_month": _month_param("Backfill: first month (YYYY-MM)."),
            "last_month": _month_param("Backfill: last month (YYYY-MM). Default: previous month."),
        },
        tags=["etl", "ons", pipeline],
        doc_md=doc_md,
    )
    def pipeline_dag() -> None:
        @task
        def resolve_periods(**context: Any) -> list[str]:
            params = context["params"]
            run_date = context.get("logical_date") or datetime.now(UTC)
            # A run always targets complete months: the one before the run date.
            previous = Period.from_datetime(run_date).previous()
            if params.get("first_month"):
                last = Period.parse(params["last_month"]) if params.get("last_month") else previous
                periods = periods_between(Period.parse(params["first_month"]), last)
            else:
                last = Period.parse(params["month"]) if params.get("month") else previous
                periods = trailing_periods(last, window_months)
            return [str(period) for period in periods]

        @task
        def source_periods(source_name: str, periods: list[str]) -> list[str]:
            planned = SOURCES[source_name].periods_to_process([Period.parse(p) for p in periods])
            return [str(period) for period in planned]

        @task
        def prepare_infrastructure() -> None:
            from energy_pipeline import steps

            steps.prepare_infrastructure(_settings())

        @task
        def extract(source_name: str, period: str) -> dict[str, Any]:
            from energy_pipeline import steps

            return steps.run_extract(source_name, Period.parse(period), _settings())

        @task
        def transform(manifest: dict[str, Any]) -> dict[str, Any]:
            from energy_pipeline import steps

            return steps.run_transform(manifest, _settings())

        @task
        def validate(manifest: dict[str, Any]) -> dict[str, Any]:
            from energy_pipeline import steps

            return steps.run_validate(manifest, _settings())

        @task(outlets=[lake_asset])
        def record(manifest: dict[str, Any]) -> dict[str, Any]:
            from energy_pipeline import steps

            return steps.run_record(manifest, _settings())

        # A source with no month to process (e.g. solar before 2024-04) is skipped;
        # that must not prevent publishing the months that do have data.
        @task(trigger_rule="none_failed")
        def publish(period: str) -> dict[str, Any]:
            from energy_pipeline import steps

            return steps.run_publish(Period.parse(period), _settings())

        @task(outlets=[portal_asset])
        def notify_portal() -> dict[str, Any]:
            from energy_pipeline import steps

            return steps.notify_portal(_settings())

        periods = resolve_periods()
        infrastructure = prepare_infrastructure()
        validated: dict[str, Any] = {}
        recorded = []

        for source in sources.values():
            with TaskGroup(group_id=source.name, tooltip=source.description):
                months = source_periods(source.name, periods)
                extracted = extract.partial(source_name=source.name).expand(period=months)
                transformed = transform.expand(manifest=extracted)
                validated[source.name] = validate.expand(manifest=transformed)
                recorded.append(record.expand(manifest=validated[source.name]))
            infrastructure >> extracted
            for dependency in source.depends_on:
                validated[dependency] >> transformed

        if publishes:
            published = publish.expand(period=periods)
            for done in recorded:
                done >> published
            published >> notify_portal()

    return pipeline_dag()
