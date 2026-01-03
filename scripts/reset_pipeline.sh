#!/bin/bash
# Reset Pipeline Script: completely cleans the database and MinIO, then restarts services

set -e

echo "Resetting Kolesa DWH Pipeline..."
echo ""

echo "Stopping services..."
docker compose down

echo "Removing volumes (all data will be deleted)..."
docker compose down -v

echo "Cleaning Python cache files..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
rm -rf .pytest_cache target/ dbt_packages/ 2>/dev/null || true

echo "Starting services..."
docker compose up -d --build

echo "Waiting for services to be healthy (30 seconds)..."
sleep 30

echo "Verifying services..."
docker compose ps

echo ""
echo "✨ Reset complete! Services are running."
echo ""
echo "Next steps:"
echo "  1. Open Airflow UI: http://localhost:8085"
echo "  2. Enable and trigger the 'kolesa_cars_dwh' DAG"
echo "  3. Monitor the pipeline execution"
echo ""

