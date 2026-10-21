ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION

# Runtime dependencies come from uv.lock; boto3, psycopg2 and requests are
# already part of the Airflow image (Amazon and Postgres providers).
COPY --from=ghcr.io/astral-sh/uv:0.12.21 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/project/
RUN uv export --project /tmp/project --frozen --no-dev --no-emit-project --no-hashes \
        -o /tmp/requirements.txt \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

COPY --chown=airflow:root src /opt/airflow/src
ENV PYTHONPATH=/opt/airflow/src
