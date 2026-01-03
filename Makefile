.PHONY: help up down logs test test-integration dbt-run dbt-test smoke clean clean-data reset fix-permissions

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start all services with docker compose
	docker compose up -d --build

down: ## Stop all services
	docker compose down

logs: ## Show logs from all services
	docker compose logs -f

logs-airflow: ## Show Airflow logs only
	docker compose logs -f airflow

logs-postgres: ## Show Postgres logs only
	docker compose logs -f postgres

test: ## Run unit tests (no network required)
	pytest tests/ -v -m "not integration"

test-integration: ## Run integration tests (requires docker compose services)
	pytest tests/ -v -m integration

verify-cloud: ## Verify data in Supabase cloud database
	@echo "Verifying Supabase data..."
	@python scripts/verify_cloud_data.py

test-all: ## Run all tests
	pytest tests/ -v

dbt-run: ## Run dbt models
	docker compose exec airflow dbt run --project-dir /opt/airflow --profiles-dir /opt/airflow

dbt-test: ## Run dbt tests
	docker compose exec airflow dbt test --project-dir /opt/airflow --profiles-dir /opt/airflow

dbt-build: ## Run dbt build (models + tests)
	docker compose exec airflow dbt build --project-dir /opt/airflow --profiles-dir /opt/airflow

smoke: ## Run smoke test (fetch 1 page, 3 listings max)
	@echo "Note: Make sure dependencies are installed: pip install -r requirements-local.txt && playwright install chromium"
	python scripts/smoke_run.py

smoke-local: ## Run smoke test locally (without Docker)
	python scripts/smoke_run.py --local

db-shell: ## Open Postgres shell
	docker compose exec postgres psql -U warehouse -d warehouse

db-query: ## Run SQL query (usage: make db-query QUERY="SELECT COUNT(*) FROM silver.listing_current")
	docker compose exec postgres psql -U warehouse -d warehouse -c "$(QUERY)"

fix-permissions: ## Fix database permissions for warehouse user (run this if you get permission errors)
	@echo "Fixing database permissions..."
	@docker compose exec -T postgres psql -U postgres -d warehouse < db/init/30-fix-permissions.sql

airflow-shell: ## Open Airflow container shell
	docker compose exec airflow bash

airflow-test-task: ## Test Airflow task (usage: make airflow-test-task DAG=kolesa_cars_dwh TASK=discover)
	docker compose exec airflow airflow tasks test $(DAG) $(TASK) $(shell date +%Y-%m-%d)

clean: ## Remove all containers, volumes, and generated files
	docker compose down -v
	rm -rf target/ dbt_packages/ .pytest_cache/ __pycache__/ **/__pycache__/ **/*.pyc
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-data: ## Clean only data (tables and MinIO buckets), keep containers running
	@echo "Warning: This requires containers to be running!"
	@echo "Cleaning database data..."
	@docker compose exec postgres psql -U warehouse -d warehouse -c "DROP SCHEMA IF EXISTS bronze CASCADE; DROP SCHEMA IF EXISTS silver CASCADE; DROP SCHEMA IF EXISTS gold CASCADE; DROP SCHEMA IF EXISTS ctl CASCADE;" || echo "Postgres container not running, skipping..."
	@docker compose exec postgres psql -U warehouse -d warehouse -c "CREATE SCHEMA IF NOT EXISTS ctl; CREATE SCHEMA IF NOT EXISTS bronze; CREATE SCHEMA IF NOT EXISTS silver; CREATE SCHEMA IF NOT EXISTS gold;" || echo "Postgres container not running, skipping..."
	@echo "Cleaning MinIO buckets..."
	@docker compose exec minio-mc mc rm --recursive --force local/bronze/ || echo "MinIO bucket clean skipped (may not exist)"
	@docker compose exec minio-mc mc rm --recursive --force local/gold-exports/ || echo "MinIO bucket clean skipped (may not exist)"
	@echo "Data cleaned. Trigger DAG to rebuild."

reset: clean ## Clean everything (containers, volumes, data) and restart services
	@echo "Restarting services..."
	@$(MAKE) up

install-local: ## Install local dependencies (required for smoke test)
	pip install -r requirements-local.txt
	python -m playwright install chromium

smoke-install: install-local smoke ## Install dependencies and run smoke test

setup-env: ## Create .env file from example
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env file from .env.example. Please edit it with your settings."; \
	else \
		echo ".env file already exists. Skipping."; \
	fi

