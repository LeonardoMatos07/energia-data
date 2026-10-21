"""
### ONS curtailment pipeline (Phase 1: public curtailment monitor)

Monthly ETL of wind and solar curtailment (*constrained-off*) published by
[ONS](https://dados.ons.org.br):

| Source | Content |
|---|---|
| `marginal_cost` | Semi-hourly CMO per subsystem, used as the price reference |
| `wind/solar_cluster_curtailment` | Official cut (GNRa) and reasons per cluster |
| `wind/solar_plant_curtailment` | Estimated vs. verified generation per plant |

The official cut is split by reason, priced, allocated to plants and reconciled
with ONS totals before anything reaches the warehouse.

**Schedule.** Runs on the 3rd of each month and reprocesses the last
`REPROCESS_MONTHS` months (default 3), because ONS revises published data.
Files that did not change since the last run are not downloaded again.

**Manual runs.** `{"month": "2026-09"}` reprocesses the window ending in that
month; `{"first_month": "2023-01", "last_month": "2026-09"}` backfills a range.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

from pipeline_factory import build_pipeline_dag

dag = build_pipeline_dag(
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
