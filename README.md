# energia-data

![CI](../../actions/workflows/ci.yml/badge.svg)

> **Private repository.** Data pipelines of the energy intelligence platform (product name to
> be defined). Part of Phase 1: the public curtailment monitor.

`energia-data` ingests ONS open data, standardizes and models the curtailment of wind and
solar plants, computes the aggregates the portal reads and writes them to S3 and Postgres.
**It only writes**: the database schema belongs to `energia-api` (Alembic), and the contract
between the repositories is in
[`energia-api/docs/phase1-contract.md`](../energia-api/docs/phase1-contract.md).

| Repository | Role |
|---|---|
| **`energia-data`** | Airflow: ONS ingestion, bronze → silver → gold, Postgres load |
| `energia-api` | FastAPI: schema owner, public reads, newsletter, admin |
| `energia-web` | Next.js: public portal with ISR |
| `energia-infra` | Terraform: AWS `sa-east-1` |

---

## Flow

```mermaid
flowchart LR
    ONS[("ONS<br/>public S3 bucket")]
    subgraph Airflow["Airflow 3 · ons_curtailment_pipeline"]
        E[extract] --> T[transform] --> V[validate] --> R[record]
        R --> P[publish] --> N[notify_portal]
    end
    subgraph Lake["Data lake (S3)"]
        B[(bronze)]
        S[(silver)]
        G[(gold)]
    end
    subgraph PG["Postgres (energia-api schema)"]
        T1[(plant<br/>*_monthly_curtailment)]
        T2[(load_run)]
    end
    ONS --> E --> B --> T --> S --> P --> G
    P --> T1
    R --> T2
    N -->|POST /v1/admin/revalidate| API[energia-api] --> WEB[energia-web]
```

| Step | Grain | What it does | Writes |
|---|---|---|---|
| `extract` | source × month | Downloads the ONS file unless the ETag shows it has not changed | `bronze/` (with sha256, URL and ETag in the metadata) |
| `transform` | source × month | Checks the file layout (`schema.py`), reads everything as text, casts each column explicitly, deduplicates, splits invalid rows and models curtailment using the outputs of its dependencies; keeps Brasília time and UTC | `silver/`, `quarantine/` |
| `validate` | source × month | Quality gates; ERROR checks block the load | report in XCom |
| `record` | source × month | Records the load | `load_run` |
| `publish` | month | Builds the portal aggregates and loads Postgres idempotently (deletes and reinserts the month in one transaction) | `gold/`, `plant`, `*_monthly_curtailment` |
| `notify_portal` | run | Asks `energia-api` to revalidate the portal (Cognito machine token) | — |

The pipeline runs on the 3rd of every month and **reprocesses the last 3 months**
(`REPROCESS_MONTHS`), because ONS revises data it has already published. Files with the same
ETag are not downloaded again. A failure in any task is also recorded in `load_run` and shows
up in the admin area.

## Sources

| Source | ONS dataset | Grain | Since |
|---|---|---|---|
| `marginal_cost` | `cmo_tm` | CMO per subsystem, half-hourly (R$/MWh) | 2020 |
| `generation_capacity` | `capacidade-geracao` | Capacity per generating unit (current snapshot) | — |
| `wind_cluster_curtailment` | `restricao_coff_eolica_tm` | Cluster, half-hourly: official GNRa, minutes per reason, connection point | Oct/2021 |
| `wind_plant_curtailment` | `restricao_coff_eolica_detail_tm` | Plant, half-hourly: estimated × verified generation | Oct/2021 |
| `solar_cluster_curtailment` | `restricao_coff_fotovoltaica_tm` | Same as wind | **Apr/2024** |
| `solar_plant_curtailment` | `restricao_coff_fotovoltaica_detail_tm` | Same as wind | **Apr/2024** |

There is also the `ons_grid_pipeline` (load, generation per plant and capacity), which for now
only feeds the lake; it is the groundwork for the next phases (BESS siting).

## Curtailment methodology

ONS publishes curtailment at two levels, and neither alone answers "how much did *this plant*
lose, and why":

- **cluster** (or individually dispatched plant): the official *Geração Não Realizada
  Apurada* (GNRa) and how many minutes of each half hour were restricted for each reason;
- **plant**: estimated and verified generation and a restricted half-hour flag, but neither
  the official curtailment nor the reason.

The model combines both:

1. **The official curtailment wins.** The cluster GNRa is the truth (average MW × 0.5 h = MWh).
2. **Split by reason**, proportional to the restricted minutes: `REL` (external
   unavailability, transmission), `CNF` (reliability), `ENE` (energy reason: more supply than
   the grid absorbs) and `other`. Without minutes, the reported reason code takes everything.
3. **Allocation to plants**, proportional to each plant's estimated loss (estimated − verified
   in the restricted half hours); without it, by estimated generation; and finally in equal
   parts. The sum of the plants **matches ONS by construction**, and validation checks it (1%
   tolerance).
4. **Price**: each curtailed half hour is valued at the subsystem CMO, bounded by the yearly
   PLD floor and ceiling when configured (`seeds/pld_limits.csv`).

Why allocate instead of using the per-plant estimate directly: it overstates the official
curtailment, and not by a constant factor (1.32× the GNRa in Jan/2023, 1.10× in Jan/2025,
1.02× in Sep/2026).

### What the raw data shows (and how it is handled)

| Finding | Handling |
|---|---|
| Solar data only exists from Apr/2024 (wind since Oct/2021) | Each source declares `available_from`; earlier months are skipped, not failed |
| Individually dispatched plants ("Type I") have no cluster in the plant files, but show up with their own id in the cluster files | A plant without a cluster is its own cluster |
| Plants that join a cluster mid-month appear twice per half hour | The row with the cluster is kept |
| The CMO is the DESSEM schedule: the file already includes the next day | Only complete months are processed |
| Whole days of CMO are missing in 19 of 69 months (Jan/2023 has 21 of 31) | Filled with the same half hour of the previous day, flagged `*_FILLED`; the gate fails below 60% coverage |
| The CMO is **zero** in almost every half hour of Mar–Jun/2023, and curtailment concentrates in low-CMO hours (solar at noon) | The BRL loss only makes sense with the PLD floor configured (see assumptions) |
| ONS publishes up to 56 minutes of restriction in a half hour (Sep/2023) | Warning; minutes are only used as a ratio between reasons, the GNRa is not affected |
| `dsc_restricao` only exists from Sep/2025 | Optional text |
| The schema is stable across the whole history (checked from 2021 to 2026) | The parsers still read everything as text and fail loudly if something changes |

## Quality gates

| Check | Severity |
|---|---|
| **File layout** (in `transform`, before casting): an expected column disappeared, or more than 1% of the filled values of a column do not cast to the type declared in `schema.py` | ERROR |
| Not empty, keys without nulls, unique natural key, dates within the month | ERROR |
| Accepted values (subsystem in `N, NE, S, SE`) | ERROR |
| Physical limits (generation per plant, load per subsystem) | ERROR / WARN |
| CMO coverage ≥ 60% (WARN listing the missing days) | ERROR / WARN |
| Rejected rows ≤ 1% | ERROR |
| **Reconciliation with ONS**: allocation per plant within 1% of the official GNRa | ERROR |
| Curtailed energy without a price ≤ 1% | WARN |

## Open assumptions

- **Price assumption: PLD.** The BRL loss uses the CMO bounded by each year's PLD floor and
  hourly ceiling (`src/energy_pipeline/seeds/pld_limits.csv`, 2021–2026). The values come from
  ANEEL announcements reported by the trade press; check each row against the act in the
  Diário Oficial before publishing BRL values. Add each new year's row in December, when ANEEL
  publishes the limits; without it, the year uses the raw CMO.
- **Is the PLD the right reference?** Generators with long-term contracts lose the contract
  price, not the spot price.
- **REL compensation.** ONS sends CCEE the final reference generation of the `REL` half hours
  for compensation (ESS); the portal should separate compensable from non-compensable loss.
  Confirm the current rules with someone in the sector.
- **CCEE data** (official PLD) blocks automated access (HTTP 403); hence the ONS CMO.

---

## Running

### With Docker (Airflow)

The repositories sit side by side (`../energia-api` is used by the `migrate` service).

```bash
cp .env.example .env
docker compose up -d --build
make backfill FROM=2023-01 TO=2026-09
```

Copying `.env.example` is optional.

After a backfill, reconcile what was published against the raw ONS files:

```bash
make reconcile FROM=2021-10 TO=2026-09
```

It sums the official non-realized generation (GNRa) of every month from the bronze files and compares it with the totals published per subsystem, state and plant. It exits with an error if any deviation exceeds `RECONCILIATION_TOLERANCE` (1% by default).

Generate the monthly report from the published data (a draft unless `--publish` is passed):

```bash
python -m energy_pipeline.report --month 2026-09 --submit
```

To generate every month at once, use `--from 2021-10 --to 2026-09` instead of `--month`.

It prints the Markdown report and a LinkedIn post, and with `--submit` saves it through `POST /v1/admin/reports` (needs `API_URL`).

| Service | Address | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Lake (file browser) | http://localhost:8888/buckets/energy-lake/ | |
| Lake (S3 API) | http://localhost:8333 | `lakeadmin` / `lakesecret` |
| Postgres | `localhost:5433`, database `warehouse` | `warehouse` / `warehouse` |

### Without Airflow (CLI)

The steps do not depend on the orchestrator, so everything runs as a CLI against any Postgres
and S3 endpoint. This is how backfills and debugging are done locally. The schema must exist
first: run `uv run alembic upgrade head` in `../energia-api`.

```bash
uv sync --extra local
export PYTHONPATH=src LAKE_S3_ENDPOINT_URL=http://localhost:8333
export AWS_ACCESS_KEY_ID=lakeadmin AWS_SECRET_ACCESS_KEY=lakesecret AWS_DEFAULT_REGION=us-east-1
export WAREHOUSE_DSN=postgresql://warehouse:warehouse@localhost:5433/warehouse
export API_URL=http://localhost:8000

uv run python -m energy_pipeline curtailment
uv run python -m energy_pipeline curtailment --from 2023-01 --to 2026-09
uv run python -m energy_pipeline grid --month 2026-09
```

`API_URL` is optional: when set, the portal is revalidated at the end. Without `--from/--to`,
`curtailment` runs the reprocessing window.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `REPROCESS_MONTHS` | `3` | Months reprocessed on every run |
| `RECONCILIATION_TOLERANCE` | `0.01` | Maximum deviation between the allocation and the official GNRa |
| `MAX_REJECTED_RATIO` | `0.01` | Maximum fraction of quarantined rows |
| `PIPELINE_START_DATE` | `2025-01-01` | First month of the grid pipeline |
| `LAKE_S3_ENDPOINT_URL` | SeaweedFS in Docker | Empty = AWS S3 |
| `LAKE_BUCKET` | `energy-lake` | |
| `WAREHOUSE_DSN` | Docker Postgres | |
| `API_URL` | — | `energia-api` used to revalidate the portal (empty = no notification) |
| `COGNITO_TOKEN_URL`, `DATA_CLIENT_ID`, `DATA_CLIENT_SECRET` | — | Cognito machine token (production) |

### Code quality

```bash
make check
```

`make check` runs ruff check, mypy, pytest and ruff format --check. The tests cover reading,
deduplication, the reason split, allocation, reconciliation, pricing and gap filling, the
portal aggregates and revalidation, using real rows from the ONS files as fixtures.

### Pipeline and deployment

`.github/workflows/ci.yml` runs on every push, pull request and manual run (*Actions → pipeline
→ Run workflow*):

| Job | What it checks |
|---|---|
| Lint | `ruff check` and `ruff format --check` |
| Type check | `mypy` |
| Tests | `pytest` |
| Dependency audit | `pip-audit` over the locked dependencies (including the `local` extra) |
| Secrets and misconfiguration scan | Trivy over the repository |
| Build and scan image | Builds the Airflow image, imports both DAGs inside it and scans it with Trivy (HIGH/CRITICAL with a fix available) |

On `main`, once `AIRFLOW_INSTANCE_ID` is set, the scanned image is pushed to ECR with the commit
SHA, and **Deploy to production** waits for approval in the `production` environment. After
approval it runs `deploy.sh <sha>` on the Airflow instance through SSM and fails if the command
does not succeed. Required settings: secret `AWS_ROLE_ARN`; variables `AWS_REGION`,
`ECR_REPOSITORY`, `AIRFLOW_INSTANCE_ID`; environment `production` with required reviewers.

The `Dockerfile` hardens the official Airflow image: it applies the Debian security updates,
removes components the project does not use (the Google provider with `ray` and its Java jars,
the Docker CLI, `pip` and its vendored libraries, the `prek` tool) and pins patched versions of
the Python packages the base image ships with known vulnerabilities. Dependabot proposes weekly
updates with a 14-day cooldown.

---

## Layout

| Path | Contents |
|---|---|
| `dags/pipeline_factory.py` | One task group per source, mapped per month |
| `dags/ons_curtailment_pipeline.py` | Phase 1 |
| `dags/ons_grid_pipeline.py` | Grid data for the next phases |
| `src/energy_pipeline/sources.py` | ONS datasets, availability, dependencies |
| `src/energy_pipeline/extract.py` | ETag and download with checksum |
| `src/energy_pipeline/schema.py` | Expected layout of each ONS file |
| `src/energy_pipeline/transform.py` | Reading and standardization (Polars) |
| `src/energy_pipeline/curtailment.py` | Reason split, allocation, pricing |
| `src/energy_pipeline/publish.py` | Portal aggregates (contract tables) |
| `src/energy_pipeline/quality.py` | Quality gates per source |
| `src/energy_pipeline/warehouse.py` | Idempotent load through COPY, load log |
| `src/energy_pipeline/storage.py` | Lake access (boto3) |
| `src/energy_pipeline/notify.py` | Portal revalidation |
| `src/energy_pipeline/steps.py` | extract / transform / validate / record / publish |
| `src/energy_pipeline/__main__.py` | CLI |
| `src/energy_pipeline/seeds/pld_limits.csv` | Yearly PLD floor and ceiling (ANEEL) |
| `tests/` | Tests |
| `Dockerfile` | Airflow image + dependencies from `uv.lock` |
| `docker-compose.yml` | Local: Airflow, Postgres, SeaweedFS, migrations |
| `docker-compose.prod.yml` | EC2: ECR image, S3 and RDS |
| `deploy.sh` | Called by CI through SSM |

## Data license

ONS (Operador Nacional do Sistema Elétrico) data, under the
[open data terms of use](https://dados.ons.org.br).
