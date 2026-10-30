from __future__ import annotations

import os
from datetime import UTC, datetime

from airflow.sdk import DAG
from pipeline_factory import build_pipeline_dag

dag: DAG = build_pipeline_dag(
    dag_id="ons_system_pipeline",
    pipeline="system",
    description="Monthly ETL of the ONS energy balance and generation per plant, with reprocessing",
    schedule="0 7 3 * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    window_months=int(os.getenv("REPROCESS_MONTHS", "3")),
    publishes=True,
    doc_md=__doc__,
)
