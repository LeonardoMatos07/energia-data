from __future__ import annotations

import os
from datetime import UTC, datetime

from airflow.sdk import DAG
from pipeline_factory import build_pipeline_dag

dag: DAG = build_pipeline_dag(
    dag_id="ons_hydrology_pipeline",
    pipeline="hydrology",
    description="Monthly ETL of ONS reservoirs, stored energy, inflows and spilled energy",
    schedule="0 8 3 * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    window_months=int(os.getenv("REPROCESS_MONTHS", "3")),
    publishes=True,
    doc_md=__doc__,
)
