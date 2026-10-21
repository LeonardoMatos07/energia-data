.PHONY: up down clean logs psql dag-errors backfill reprocess reconcile sync test lint format types check

up:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler

psql:
	docker compose exec warehouse-db psql -U warehouse -d warehouse

dag-errors:
	docker compose exec airflow-scheduler airflow dags list-import-errors

backfill:
	docker compose exec airflow-scheduler airflow dags trigger ons_curtailment_pipeline \
		--conf '{"first_month": "$(FROM)", "last_month": "$(TO)"}'

reprocess:
	docker compose exec airflow-scheduler airflow dags trigger ons_curtailment_pipeline \
		--conf '{"month": "$(MONTH)"}'

reconcile:
	docker compose exec airflow-scheduler python -m energy_pipeline.reconcile --from $(FROM) --to $(TO)

sync:
	uv sync --extra local

test:
	uv run pytest -q

lint:
	uv run ruff check .

format:
	uv run ruff format .

types:
	uv run mypy

check: lint types test
	uv run ruff format --check .
