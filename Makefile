.PHONY: up down clean logs psql dag-errors backfill reprocess sync test lint format types check

up: ## Build images, run the energia-api migrations and start the stack
	docker compose up -d --build

down: ## Stop the stack, keep data
	docker compose down

clean: ## Stop the stack and delete all data volumes
	docker compose down -v

logs: ## Follow the scheduler logs (tasks run there)
	docker compose logs -f airflow-scheduler

psql: ## SQL shell on the warehouse
	docker compose exec warehouse-db psql -U warehouse -d warehouse

dag-errors:
	docker compose exec airflow-scheduler airflow dags list-import-errors

backfill: ## Curtailment history, e.g. make backfill FROM=2023-01 TO=2026-09
	docker compose exec airflow-scheduler airflow dags trigger ons_curtailment_pipeline \
		--conf '{"first_month": "$(FROM)", "last_month": "$(TO)"}'

reprocess: ## Reprocessing window ending in a month, e.g. make reprocess MONTH=2026-09
	docker compose exec airflow-scheduler airflow dags trigger ons_curtailment_pipeline \
		--conf '{"month": "$(MONTH)"}'

sync: ## Local virtualenv with everything needed to run the CLI and the tests
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
