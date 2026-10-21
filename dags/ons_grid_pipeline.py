from __future__ import annotations

import os
from datetime import UTC

from airflow.sdk import DAG
from pipeline_factory import build_pipeline_dag

from energy_pipeline.period import Period

FIRST_MONTH = Period.parse(os.getenv("PIPELINE_START_DATE", "2025-01-01")[:7])

dag: DAG = build_pipeline_dag(
    dag_id="ons_grid_pipeline",
    pipeline="grid",
    description="Monthly ETL of ONS load, generation and capacity data",
    schedule="@monthly",
    start_date=FIRST_MONTH.next().start.replace(tzinfo=UTC),
    catchup=True,
    window_months=1,
    publishes=False,
    doc_md=__doc__,
)
