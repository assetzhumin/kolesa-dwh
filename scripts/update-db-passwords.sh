#!/bin/bash
# Script to update database user passwords after initial setup
# Usage: ./scripts/update-db-passwords.sh
# Requires: POSTGRES_PASSWORD, AIRFLOW_PASSWORD, WAREHOUSE_PASSWORD environment variables

set -e

if [ -z "$POSTGRES_PASSWORD" ]; then
    echo "ERROR: POSTGRES_PASSWORD environment variable is required"
    exit 1
fi

if [ -z "$AIRFLOW_PASSWORD" ]; then
    echo "WARNING: AIRFLOW_PASSWORD not set, skipping airflow user password update"
else
    echo "Updating airflow user password..."
    docker compose exec -T postgres psql -U postgres -d postgres -c "ALTER ROLE airflow WITH PASSWORD '$AIRFLOW_PASSWORD';" || echo "Failed to update airflow password"
fi

if [ -z "$WAREHOUSE_PASSWORD" ]; then
    echo "WARNING: WAREHOUSE_PASSWORD not set, skipping warehouse user password update"
else
    echo "Updating warehouse user password..."
    docker compose exec -T postgres psql -U postgres -d postgres -c "ALTER ROLE warehouse WITH PASSWORD '$WAREHOUSE_PASSWORD';" || echo "Failed to update warehouse password"
fi

echo "Password update complete. Update your .env file and restart services if needed."

