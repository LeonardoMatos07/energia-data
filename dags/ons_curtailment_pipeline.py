from __future__ import annotations

import os
from datetime import UTC, datetime

from airflow.sdk import DAG
from pipeline_factory import build_pipeline_dag

dag: DAG = build_pipeline_dag(
    dag_id="ons_curtailment_pipeline",
    pipeline="curtailment",
    description="Monthly ETL of ONS wind and solar curtailment, with reprocessing window",
    schedule="0 6 3 * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    window_months=int(os.getenv("REPROCESS_MONTHS", "3")),
    publishes=True,
    doc_md=__doc__,
)
