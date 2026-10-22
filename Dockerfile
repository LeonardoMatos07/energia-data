ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION

USER root
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* /usr/bin/docker
USER airflow

COPY --from=ghcr.io/astral-sh/uv:0.12.21 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/project/
RUN uv export --project /tmp/project --frozen --no-dev --no-emit-project --no-hashes \
        -o /tmp/requirements.txt \
    && pip uninstall --yes apache-airflow-providers-google ray \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
        aiohttp==3.14.4 \
        certifi==2026.10.19 \
        msgpack==1.2.1 \
        setuptools==84.0.0 \
        tornado==6.5.9 \
        urllib3==2.8.0 \
        uv==0.13.4 \
        virtualenv==21.14.7 \
    && pip uninstall --yes pip \
    && rm -rf /home/airflow/.local/share/uv/tools /tmp/requirements.txt

USER root
RUN rm -rf /tmp/project /usr/python/bin/pip /usr/python/bin/pip3* /usr/python/lib/python3.12/site-packages/pip /usr/python/lib/python3.12/site-packages/pip-*
USER airflow

COPY --chown=airflow:root src /opt/airflow/src
ENV PYTHONPATH=/opt/airflow/src
