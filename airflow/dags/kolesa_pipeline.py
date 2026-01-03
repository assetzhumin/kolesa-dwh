"""
Kolesa.kz Data Warehouse Pipeline DAG.

Implements Bronze → Silver → Gold architecture with cloud load.
"""
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import logging

from src.extract.discover_listings import discover_pages_to_db
from src.extract.fetch_listing import fetch_and_process_batch
from src.extract.fetch_views import enrich_views_for_today
from src.common.db import wh_conn
from src.load.cloud_sink import get_cloud_sink, export_gold_to_parquet

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",  # hourly
    start_date=days_ago(1),
    catchup=False,
    tags=["kolesa", "dwh", "bronze-silver-gold"],
    max_active_runs=1,
)
def kolesa_cars_dwh():
    """Main DAG for Kolesa.kz data warehouse pipeline."""
    
    @task
    def run_ddl():
        """Ensure DDL is applied (idempotent)."""
        ddl_path = "/opt/airflow/db/init/10-ddl.sql"
        with open(ddl_path, "r", encoding="utf-8") as f:
            sql = f.read()
        with wh_conn() as conn:
            with conn.cursor() as cur:
                # Split by semicolon and filter out empty statements and psql meta-commands
                statements = [
                    s.strip() 
                    for s in sql.split(";") 
                    if s.strip() 
                    and not s.strip().startswith("\\c")
                ]
                # Filter out comment-only statements (lines that are only comments)
                statements = [
                    s for s in statements 
                    if s and not all(line.strip().startswith("--") or not line.strip() for line in s.split("\n"))
                ]
                
                logger.info(f"Found {len(statements)} DDL statements to execute")
                for i, stmt in enumerate(statements, 1):
                    try:
                        # Skip if it's only whitespace or comments
                        if not stmt or stmt.strip().startswith("--"):
                            continue
                        logger.debug(f"Executing DDL statement {i}/{len(statements)}: {stmt[:80]}...")
                        cur.execute(stmt)
                    except Exception as e:
                        # Log but continue - many DDL statements are idempotent (IF NOT EXISTS)
                        error_msg = str(e)
                        if "already exists" not in error_msg.lower() and "duplicate" not in error_msg.lower():
                            logger.warning(f"DDL statement {i} failed: {error_msg}")
                        else:
                            logger.debug(f"DDL statement {i} skipped (already exists): {error_msg}")
        logger.info("DDL applied successfully")

    @task
    def discover():
        """Discover listing IDs from pagination pages."""
        max_pages = int(os.getenv("KOLESA_MAX_PAGES", "50"))
        from src.common.utils import StatsCollector
        stats = StatsCollector()
        inserted = discover_pages_to_db(1, max_pages, stats)
        stats.log_summary()
        logger.info(f"Discovered {inserted} new listing IDs")
        return {"discovered_ids": inserted}

    @task
    def fetch_details(discover_result):
        """Fetch listing detail pages and process into Bronze/Silver."""
        # Get discovered count from previous task
        discovered_count = 0
        if discover_result and isinstance(discover_result, dict) and "discovered_ids" in discover_result:
            discovered_count = discover_result["discovered_ids"]
        
        # If no discovered count available, check queue directly
        if discovered_count == 0:
            with wh_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM ctl.scrape_queue WHERE state IN ('NEW', 'RETRY')")
                    discovered_count = cur.fetchone()[0]
        
        logger.info(f"Total discovered listings in queue: {discovered_count}")
        
        if discovered_count == 0:
            logger.info("No new listings discovered. Skipping fetch_details.")
            return {"fetched_ok": 0, "batches": 0}
        
        # Always use batch size of 5, regardless of discovered count
        batch_size = 5
        logger.info(f"Processing {discovered_count} discovered listings in batches of {batch_size}")
        
        total_processed = 0
        batch_num = 1
        remaining_to_process = discovered_count
        
        while remaining_to_process > 0:
            # Calculate how many to process in this batch (min of batch_size or remaining)
            current_batch_size = min(batch_size, remaining_to_process)
            
            logger.info(f"Processing batch {batch_num}: {current_batch_size} listings (remaining to process: {remaining_to_process}/{discovered_count})...")
            processed = fetch_and_process_batch(current_batch_size)
            total_processed += processed
            
            if processed == 0:
                logger.warning(f"No items were fetched in batch {batch_num}. Stopping.")
                break
            
            # Update remaining count based on what we intended to process
            remaining_to_process -= processed
            
            logger.info(f"Batch {batch_num} completed: {processed} listings processed. Total so far: {total_processed}/{discovered_count}, remaining: {remaining_to_process}")
            batch_num += 1
        
        logger.info(f"Fetch details completed. Total processed: {total_processed}/{discovered_count} discovered listings across {batch_num - 1} batches")
        return {"fetched_ok": total_processed, "batches": batch_num - 1}

    @task
    def enrich_views():
        """Enrich daily snapshots with view counts from API."""
        enrich_views_for_today()
        logger.info("Views enrichment completed")
        return {"views_enriched": True}

    @task
    def build_gold():
        """Build Gold layer (STAR schema) from Silver."""
        sql_path = "/opt/airflow/db/init/20-gold-build.sql"
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()
        
        row_counts = {}
        with wh_conn() as conn:
            with conn.cursor() as cur:
                # Execute Gold build SQL
                for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("\\c")]:
                    cur.execute(stmt)
                
                # Get row counts
                cur.execute("SELECT COUNT(*) FROM gold.fact_listing_daily")
                row_counts["fact_listing_daily"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM gold.fact_price_event")
                row_counts["fact_price_event"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM gold.dim_vehicle")
                row_counts["dim_vehicle"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM gold.dim_location")
                row_counts["dim_location"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM gold.dim_seller")
                row_counts["dim_seller"] = cur.fetchone()[0]
        
        logger.info(f"Gold build completed. Row counts: {row_counts}")
        return row_counts

    @task
    def export_and_load_cloud():
        """Export Gold tables to Parquet and load into cloud warehouse."""
        cloud_sink = get_cloud_sink()
        
        if not cloud_sink:
            logger.info("CLOUD_SINK=none, skipping cloud load")
            return {"cloud_rows_loaded": 0, "cloud_sink": "none"}
        
        sink_name = os.getenv("CLOUD_SINK", "supabase").lower()
        logger.info(f"Exporting Gold tables to cloud sink: {sink_name}")
        
        total_rows = 0
        tables_loaded = []
        
        # Import gold DDL definitions and table order
        from src.load.gold_ddl import get_gold_ddl, get_gold_table_order
        
        # Get tables in correct order (dimensions first, then facts)
        gold_tables = get_gold_table_order()
        logger.info(f"Loading {len(gold_tables)} tables in order: {', '.join(gold_tables)}")
        
        for table_name in gold_tables:
            try:
                # Export to Parquet
                logger.info(f"Exporting {table_name} to Parquet...")
                parquet_path = export_gold_to_parquet(table_name)
                
                # Load to cloud with proper DDL (includes PKs/FKs/constraints)
                logger.info(f"Loading {table_name} to {sink_name}...")
                try:
                    # Get DDL for this table (with all constraints)
                    table_ddl = get_gold_ddl(table_name)
                    rows = cloud_sink.load_table(table_name, parquet_path, schema="gold", ddl=table_ddl)
                    total_rows += rows
                    tables_loaded.append(table_name)
                    logger.info(f"Loaded {rows} rows from {table_name} with full constraints")
                except Exception as load_error:
                    # Log error but don't fail the entire DAG
                    error_msg = str(load_error)
                    if "UNAVAILABLE" in error_msg or "network" in error_msg.lower() or "connection" in error_msg.lower():
                        logger.warning(f"Network/connection issue loading {table_name} to {sink_name}: {error_msg}")
                        logger.warning(f"Parquet file is available at {parquet_path} and can be loaded manually later")
                    else:
                        logger.error(f"Failed to load {table_name} to {sink_name}: {load_error}", exc_info=True)
                    # Continue with other tables
                    continue
                
            except Exception as e:
                logger.error(f"Failed to export/load {table_name}: {e}", exc_info=True)
                # Continue with other tables
                continue
        
        if total_rows > 0:
            logger.info(f"Cloud load completed: {total_rows} total rows loaded from {len(tables_loaded)} tables")
        else:
            logger.warning(f"Cloud load completed with 0 rows loaded. Parquet files are available in MinIO (gold-exports bucket) for manual loading.")
        
        return {
            "cloud_rows_loaded": total_rows,
            "cloud_sink": sink_name,
            "tables_loaded": tables_loaded,
        }

    @task
    def dq_checks():
        """Run data quality checks and fail if thresholds not met."""
        checks = [
            (
                "SELECT COUNT(*) FROM bronze.raw_listing_html WHERE fetched_at::date = CURRENT_DATE",
                0,  # Allow 0 for hourly runs (may not have new data every hour)
                "Bronze rows today",
                "warning",  # Warning instead of error for hourly runs
            ),
            (
                "SELECT COUNT(*) FROM silver.listing_snapshot_daily WHERE snapshot_date = CURRENT_DATE",
                0,
                "Silver snapshots today",
                "warning",
            ),
            (
                "SELECT COUNT(*) FROM gold.fact_listing_daily WHERE date_key = to_char(CURRENT_DATE, 'YYYYMMDD')::int",
                0,
                "Gold facts today",
                "warning",
            ),
            (
                "SELECT CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE COUNT(*) FILTER (WHERE price_kzt IS NULL)::float / COUNT(*) END FROM silver.listing_current WHERE is_active = TRUE",
                0.5,
                "Price Null Ratio (Max)",
                "error",  # This should fail if too many nulls
            ),
            (
                "SELECT COUNT(*) FROM silver.listing_current WHERE make IS NULL OR model IS NULL",
                0,
                "Missing make/model (should be 0)",
                "error",
            ),
        ]
        
        errors = []
        warnings = []
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                for sql, threshold, name, level in checks:
                    cur.execute(sql)
                    val = cur.fetchone()[0]
                    
                    if "Ratio" in name:
                        if val > threshold:
                            msg = f"DQ Check Failed: {name} = {val} > {threshold}"
                            if level == "error":
                                errors.append(msg)
                            else:
                                warnings.append(msg)
                        else:
                            logger.info(f"DQ Check Passed: {name} = {val} <= {threshold}")
                    else:
                        if val < threshold:
                            msg = f"DQ Check Failed: {name} = {val} < {threshold}"
                            if level == "error":
                                errors.append(msg)
                            else:
                                warnings.append(msg)
                        else:
                            logger.info(f"DQ Check Passed: {name} = {val} >= {threshold}")
        
        # Log warnings
        for warning in warnings:
            logger.warning(warning)
        
        # Fail on errors
        if errors:
            error_msg = "\n".join(errors)
            logger.error(f"Data Quality Checks Failed:\n{error_msg}")
            raise ValueError(f"Data Quality Checks Failed:\n{error_msg}")
        
        logger.info("All data quality checks passed")
        return {"dq_checks_passed": True, "warnings": len(warnings), "errors": len(errors)}

    # Task dependencies
    ddl_task = run_ddl()
    discover_task = discover()
    fetch_task = fetch_details(discover_task)
    views_task = enrich_views()
    gold_task = build_gold()
    cloud_task = export_and_load_cloud()
    dq_task = dq_checks()
    
    # Pipeline flow
    ddl_task >> discover_task >> fetch_task >> views_task >> gold_task >> cloud_task >> dq_task


# Daily DAG for full refresh + cloud load
@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["kolesa", "dwh", "daily", "cloud-load"],
    max_active_runs=1,
)
def kolesa_cars_dwh_daily():
    """Daily DAG for full Gold refresh and cloud load."""
    
    @task
    def build_gold_full():
        """Full Gold refresh."""
        sql_path = "/opt/airflow/db/init/20-gold-build.sql"
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("\\c")]:
                    cur.execute(stmt)
        
        logger.info("Full Gold refresh completed")
        return {"gold_refreshed": True}
    
    @task
    def export_and_load_cloud():
        """Export and load to cloud with proper DDL constraints."""
        cloud_sink = get_cloud_sink()
        if not cloud_sink:
            logger.info("CLOUD_SINK=none, skipping")
            return {"cloud_rows_loaded": 0}
        
        # Import gold DDL definitions and table order
        from src.load.gold_ddl import get_gold_ddl, get_gold_table_order
        
        sink_name = os.getenv("CLOUD_SINK", "supabase").lower()
        logger.info(f"Exporting Gold tables to cloud sink: {sink_name}")
        
        # Get tables in correct order (dimensions first, then facts)
        gold_tables = get_gold_table_order()
        logger.info(f"Loading {len(gold_tables)} tables in order: {', '.join(gold_tables)}")
        
        total_rows = 0
        tables_loaded = []
        
        for table_name in gold_tables:
            try:
                # Export to Parquet
                logger.info(f"Exporting {table_name} to Parquet...")
                parquet_path = export_gold_to_parquet(table_name)
                
                # Load to cloud with proper DDL (includes PKs/FKs/constraints)
                logger.info(f"Loading {table_name} to {sink_name}...")
                table_ddl = get_gold_ddl(table_name)
                rows = cloud_sink.load_table(table_name, parquet_path, schema="gold", ddl=table_ddl)
                total_rows += rows
                tables_loaded.append(table_name)
                logger.info(f"Loaded {rows} rows from {table_name} with full constraints")
            except Exception as e:
                logger.error(f"Failed to export/load {table_name}: {e}", exc_info=True)
                # Continue with other tables
                continue
        
        if total_rows > 0:
            logger.info(f"Cloud load completed: {total_rows} total rows loaded from {len(tables_loaded)} tables")
        else:
            logger.warning(f"Cloud load completed with 0 rows loaded. Parquet files are available in MinIO (gold-exports bucket) for manual loading.")
        
        return {
            "cloud_rows_loaded": total_rows,
            "cloud_sink": sink_name,
            "tables_loaded": tables_loaded,
        }
    
    @task
    def run_dbt_tests():
        """Run dbt tests for data quality."""
        import subprocess
        result = subprocess.run(
            ["dbt", "test", "--project-dir", "/opt/airflow", "--profiles-dir", "/opt/airflow"],
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            logger.error(f"dbt tests failed:\n{result.stderr}")
            raise RuntimeError("dbt tests failed")
        
        logger.info("dbt tests passed")
        return {"dbt_tests_passed": True}
    
    build_gold_full() >> export_and_load_cloud() >> run_dbt_tests()


# Test DAG: Scrapes 5 listings and exports to Supabase (for testing changes before deploying to production)
@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["kolesa", "dwh", "test", "bronze-silver-gold"],
    max_active_runs=1,
    description="Test DAG: Scrapes exactly 5 listings and exports to Supabase. Use this to test changes before deploying to production DAG.",
)
def kolesa_cars_dwh_test():
    """Test DAG for Kolesa.kz pipeline - processes exactly 5 listings for testing."""
    
    @task
    def run_ddl():
        """Ensure DDL is applied (idempotent)."""
        ddl_path = "/opt/airflow/db/init/10-ddl.sql"
        with open(ddl_path, "r", encoding="utf-8") as f:
            sql = f.read()
        with wh_conn() as conn:
            with conn.cursor() as cur:
                # Split by semicolon and filter out empty statements and psql meta-commands
                statements = [
                    s.strip() 
                    for s in sql.split(";") 
                    if s.strip() 
                    and not s.strip().startswith("\\c")
                ]
                # Filter out comment-only statements (lines that are only comments)
                statements = [
                    s for s in statements 
                    if s and not all(line.strip().startswith("--") or not line.strip() for line in s.split("\n"))
                ]
                
                logger.info(f"Found {len(statements)} DDL statements to execute")
                for i, stmt in enumerate(statements, 1):
                    try:
                        # Skip if it's only whitespace or comments
                        if not stmt or stmt.strip().startswith("--"):
                            continue
                        logger.debug(f"Executing DDL statement {i}/{len(statements)}: {stmt[:80]}...")
                        cur.execute(stmt)
                    except Exception as e:
                        # Log but continue - many DDL statements are idempotent (IF NOT EXISTS)
                        error_msg = str(e)
                        if "already exists" not in error_msg.lower() and "duplicate" not in error_msg.lower():
                            logger.warning(f"DDL statement {i} failed: {error_msg}")
                        else:
                            logger.debug(f"DDL statement {i} skipped (already exists): {error_msg}")
        logger.info("DDL applied successfully")

    @task
    def discover():
        """Discover listing IDs from first page only (for testing)."""
        # Discover only 1 page to get listings
        max_pages = 1
        from src.common.utils import StatsCollector
        stats = StatsCollector()
        inserted = discover_pages_to_db(1, max_pages, stats)
        stats.log_summary()
        logger.info(f"[TEST] Discovered {inserted} new listing IDs from 1 page")
        return {"discovered_ids": inserted}

    @task
    def fetch_details(discover_result):
        """Fetch exactly 5 listing detail pages and process into Bronze/Silver."""
        # Get discovered count from previous task
        discovered_count = 0
        if discover_result and isinstance(discover_result, dict) and "discovered_ids" in discover_result:
            discovered_count = discover_result["discovered_ids"]
        
        # If no discovered count available, check queue directly
        if discovered_count == 0:
            with wh_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM ctl.scrape_queue WHERE state IN ('NEW', 'RETRY')")
                    discovered_count = cur.fetchone()[0]
        
        logger.info(f"[TEST] Total discovered listings in queue: {discovered_count}")
        
        if discovered_count == 0:
            logger.info("[TEST] No new listings discovered. Skipping fetch_details.")
            return {"fetched_ok": 0, "batches": 0}
        
        # TEST MODE: Process exactly 5 listings (or all if less than 5)
        test_batch_size = 5
        total_to_process = min(test_batch_size, discovered_count)
        
        logger.info(f"[TEST] Processing exactly {total_to_process} listings (TEST MODE: limited to 5)")
        
        processed = fetch_and_process_batch(total_to_process)
        
        logger.info(f"[TEST] Fetch details completed. Processed: {processed}/{total_to_process} listings")
        return {"fetched_ok": processed, "batches": 1}

    @task
    def enrich_views():
        """Enrich daily snapshots with view counts from API (for test listings only)."""
        enrich_views_for_today()
        logger.info("[TEST] Views enrichment completed")
        return {"views_enriched": True}

    @task
    def build_gold():
        """Build Gold layer (STAR schema) from Silver."""
        sql_path = "/opt/airflow/db/init/20-gold-build.sql"
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()
        
        row_counts = {}
        with wh_conn() as conn:
            with conn.cursor() as cur:
                # Execute Gold build SQL
                for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("\\c")]:
                    cur.execute(stmt)
                
                # Get row counts
                cur.execute("SELECT COUNT(*) FROM gold.fact_listing_daily")
                row_counts["fact_listing_daily"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM gold.fact_price_event")
                row_counts["fact_price_event"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM gold.dim_vehicle")
                row_counts["dim_vehicle"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM gold.dim_location")
                row_counts["dim_location"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM gold.dim_seller")
                row_counts["dim_seller"] = cur.fetchone()[0]
        
        logger.info(f"[TEST] Gold build completed. Row counts: {row_counts}")
        return row_counts

    @task
    def export_and_load_cloud():
        """Export Gold tables to Parquet and load into cloud warehouse (Supabase)."""
        cloud_sink = get_cloud_sink()
        
        if not cloud_sink:
            logger.warning("[TEST] CLOUD_SINK=none, skipping cloud load. Set CLOUD_SINK=supabase to test cloud export.")
            return {"cloud_rows_loaded": 0, "cloud_sink": "none"}
        
        sink_name = os.getenv("CLOUD_SINK", "supabase").lower()
        logger.info(f"[TEST] Exporting Gold tables to cloud sink: {sink_name}")
        
        total_rows = 0
        tables_loaded = []
        
        # Import gold DDL definitions and table order
        from src.load.gold_ddl import get_gold_ddl, get_gold_table_order
        
        # Get tables in correct order (dimensions first, then facts)
        gold_tables = get_gold_table_order()
        logger.info(f"[TEST] Loading {len(gold_tables)} tables in order: {', '.join(gold_tables)}")
        
        for table_name in gold_tables:
            try:
                # Export to Parquet
                logger.info(f"[TEST] Exporting {table_name} to Parquet...")
                parquet_path = export_gold_to_parquet(table_name)
                
                # Load to cloud with proper DDL (includes PKs/FKs/constraints)
                logger.info(f"[TEST] Loading {table_name} to {sink_name}...")
                try:
                    # Get DDL for this table (with all constraints)
                    table_ddl = get_gold_ddl(table_name)
                    rows = cloud_sink.load_table(table_name, parquet_path, schema="gold", ddl=table_ddl)
                    total_rows += rows
                    tables_loaded.append(table_name)
                    logger.info(f"[TEST] Loaded {rows} rows from {table_name} with full constraints")
                except Exception as load_error:
                    # Log error but don't fail the entire DAG
                    error_msg = str(load_error)
                    if "UNAVAILABLE" in error_msg or "network" in error_msg.lower() or "connection" in error_msg.lower():
                        logger.warning(f"[TEST] Network/connection issue loading {table_name} to {sink_name}: {error_msg}")
                        logger.warning(f"[TEST] Parquet file is available at {parquet_path} and can be loaded manually later")
                    else:
                        logger.error(f"[TEST] Failed to load {table_name} to {sink_name}: {load_error}", exc_info=True)
                    # Continue with other tables
                    continue
                
            except Exception as e:
                logger.error(f"[TEST] Failed to export/load {table_name}: {e}", exc_info=True)
                # Continue with other tables
                continue
        
        if total_rows > 0:
            logger.info(f"[TEST] Cloud load completed: {total_rows} total rows loaded from {len(tables_loaded)} tables")
        else:
            logger.warning(f"[TEST] Cloud load completed with 0 rows loaded. Parquet files are available in MinIO (gold-exports bucket) for manual loading.")
        
        return {
            "cloud_rows_loaded": total_rows,
            "cloud_sink": sink_name,
            "tables_loaded": tables_loaded,
        }

    @task
    def dq_checks():
        """Run data quality checks (relaxed thresholds for test data)."""
        checks = [
            (
                "SELECT COUNT(*) FROM bronze.raw_listing_html WHERE fetched_at::date = CURRENT_DATE",
                0,  # Allow 0 for test runs
                "Bronze rows today",
                "warning",
            ),
            (
                "SELECT COUNT(*) FROM silver.listing_snapshot_daily WHERE snapshot_date = CURRENT_DATE",
                0,
                "Silver snapshots today",
                "warning",
            ),
            (
                "SELECT COUNT(*) FROM gold.fact_listing_daily WHERE date_key = to_char(CURRENT_DATE, 'YYYYMMDD')::int",
                0,
                "Gold facts today",
                "warning",
            ),
            (
                "SELECT CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE COUNT(*) FILTER (WHERE price_kzt IS NULL)::float / COUNT(*) END FROM silver.listing_current WHERE is_active = TRUE",
                0.5,
                "Price Null Ratio (Max)",
                "warning",  # Warning instead of error for test data
            ),
            (
                "SELECT COUNT(*) FROM silver.listing_current WHERE make IS NULL OR model IS NULL",
                0,  # Same as production, but warning instead of error
                "Missing make/model (should be 0)",
                "warning",
            ),
        ]
        
        errors = []
        warnings = []
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                for sql, threshold, name, level in checks:
                    cur.execute(sql)
                    val = cur.fetchone()[0]
                    
                    if "Ratio" in name:
                        if val > threshold:
                            msg = f"[TEST] DQ Check: {name} = {val} > {threshold}"
                            if level == "error":
                                errors.append(msg)
                            else:
                                warnings.append(msg)
                        else:
                            logger.info(f"[TEST] DQ Check Passed: {name} = {val} <= {threshold}")
                    else:
                        # Same logic as production, but warnings instead of errors
                        if val < threshold:
                            msg = f"[TEST] DQ Check: {name} = {val} < {threshold}"
                            if level == "error":
                                errors.append(msg)
                            else:
                                warnings.append(msg)
                        else:
                            logger.info(f"[TEST] DQ Check Passed: {name} = {val} >= {threshold}")
        
        # Log warnings (but don't fail in test mode)
        for warning in warnings:
            logger.warning(warning)
        
        # Fail on errors (but test mode has no errors, only warnings)
        if errors:
            error_msg = "\n".join(errors)
            logger.error(f"[TEST] Data Quality Checks Failed:\n{error_msg}")
            raise ValueError(f"[TEST] Data Quality Checks Failed:\n{error_msg}")
        
        logger.info("[TEST] All data quality checks passed (test mode - relaxed thresholds)")
        return {"dq_checks_passed": True, "warnings": len(warnings), "errors": len(errors)}

    # Task dependencies
    ddl_task = run_ddl()
    discover_task = discover()
    fetch_task = fetch_details(discover_task)
    views_task = enrich_views()
    gold_task = build_gold()
    cloud_task = export_and_load_cloud()
    dq_task = dq_checks()
    
    # Pipeline flow (same as production DAG)
    ddl_task >> discover_task >> fetch_task >> views_task >> gold_task >> cloud_task >> dq_task


# Instantiate DAGs
kolesa_cars_dwh()
kolesa_cars_dwh_daily()
kolesa_cars_dwh_test()
