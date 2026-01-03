# Kolesa.kz Data Warehouse Pipeline

Dockerized data pipeline for kolesa.kz car listings using Airflow. Implements Bronze → Silver → Gold architecture with STAR schema and exports to Supabase.

## Prerequisites

- Docker & Docker Compose
- Python 3.11+ (optional, for local testing)
- Make (optional)

## Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/YOUR_USERNAME/kolesa_dwh.git
cd kolesa_dwh
cp .env.example .env

# 2. Start services
make up
# Or: docker compose up -d --build

# 3. Wait 30-60 seconds for services to be healthy
docker compose ps

# 4. Access Airflow UI: http://localhost:8085
#    Login: admin / admin

# 5. Enable and trigger DAG: kolesa_cars_dwh
```

## Service Access

- **Airflow UI**: http://localhost:8085 (admin/admin)
- **MinIO Console**: http://localhost:9006 (minioadmin/minioadmin)
- **pgAdmin**: http://localhost:5050 (admin@example.com/admin)
- **Postgres**: `psql -h localhost -p 5435 -U warehouse -d warehouse` (password: warehouse)

## Supabase Setup

1. Create account at https://supabase.com/
2. Create new project (save database password)
3. Get connection string: Settings → Database → Connection string → Connection pooling tab
4. Update `.env`:
   ```bash
   CLOUD_SINK=supabase
   SUPABASE_DB_URL=postgresql://postgres.xxxxx:YOUR_PASSWORD@aws-0-us-east-1.pooler.supabase.com:6543/postgres
   ```
5. Restart Airflow: `docker compose restart airflow`

Free tier: 500MB database, 2GB bandwidth/month

To disable cloud load: Set `CLOUD_SINK=none` in `.env`

## Reset Pipeline

**Complete reset** (removes all data):
```bash
make clean
make up
# Or: docker compose down -v && docker compose up -d --build
```

**Clean data only** (keeps containers):
```bash
make clean-data
# Then trigger DAG to rebuild
```

After reset, wait 30-60 seconds for services, then trigger DAG in Airflow UI.

## Common Commands

```bash
# Services
make up              # Start services
make down            # Stop services
make logs            # View logs
make logs-airflow    # View Airflow logs only

# Database
make db-shell        # Open Postgres shell
make db-query QUERY="SELECT COUNT(*) FROM silver.listing_current"

# Pipeline
make smoke           # Run smoke test (1 page, 3 listings)
make test            # Run unit tests
make verify-cloud    # Verify Supabase data

# Reset
make clean           # Remove containers and volumes
make clean-data      # Clean data only (keep containers)
make reset           # Clean and restart
```

## Verify Data

```bash
# Check data counts
docker compose exec postgres psql -U warehouse -d warehouse -c "SELECT COUNT(*) FROM ctl.scrape_queue"
docker compose exec postgres psql -U warehouse -d warehouse -c "SELECT COUNT(*) FROM bronze.raw_listing_html"
docker compose exec postgres psql -U warehouse -d warehouse -c "SELECT COUNT(*) FROM silver.listing_current"
docker compose exec postgres psql -U warehouse -d warehouse -c "SELECT COUNT(*) FROM gold.fact_listing_daily"
```

## Troubleshooting

**Services won't start:**
```bash
make logs
lsof -i :8085  # Check port conflicts
make down && make up
```

**Airflow DAG not appearing:**
```bash
docker compose restart airflow
make logs-airflow
```

**Database connection issues:**
```bash
make db-shell
make fix-permissions
```

**Blocking detected:**
- Set `KOLESA_CONCURRENCY=1` in `.env`
- Set `KOLESA_SLEEP_MIN_MS=2000` and `KOLESA_SLEEP_MAX_MS=5000`
- Restart services

**Cloud load fails:**
- Verify `SUPABASE_DB_URL` in `.env`
- Check Supabase project is active
- Use connection pooler (port 6543)
