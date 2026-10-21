"""
### ONS grid pipeline

Monthly ETL of load, generation and installed capacity published by
[ONS](https://dados.ons.org.br):

| Source | Content |
|---|---|
| `load_curve` | Hourly load (demand) per subsystem |
| `plant_generation` | Hourly generation per power plant |
| `generation_capacity` | Installed capacity per generating unit |

Each run processes the month before its run date and backfills from
`PIPELINE_START_DATE`. To reprocess a month, trigger with `{"month": "YYYY-MM"}`.
"""

from __future__ import annotations

import os
from datetime import UTC

from pipeline_factory import build_pipeline_dag

from energy_pipeline.period import Period

# A run processes the previous month, so the first run happens one month later.
FIRST_MONTH = Period.parse(os.getenv("PIPELINE_START_DATE", "2025-01-01")[:7])

dag = build_pipeline_dag(
    dag_id="ons_grid_pipeline",
    pipeline="grid",
    description="Monthly ETL of ONS load, generation and capacity data",
    schedule="@monthly",
    start_date=FIRST_MONTH.next().start.replace(tzinfo=UTC),
    catchup=True,
    window_months=1,
    # Lake only for now: the grid data feeds later phases (BESS siting).
    publishes=False,
    doc_md=__doc__,
)
