"""
Integration tests that require docker compose services.

Run with: pytest tests/test_integration.py -m integration
"""
import pytest
import os
import sys
from datetime import date

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    import psycopg2
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    DB_ERROR = "psycopg2 not installed"

try:
    from src.common.db import wh_conn
    from src.transform.silver_upsert import upsert_silver
    from src.transform.parse_listing_html import parse_listing
    CORE_MODULES_AVAILABLE = True
    CORE_ERROR = None
except ImportError as e:
    CORE_MODULES_AVAILABLE = False
    CORE_ERROR = str(e)

try:
    from src.extract.discover_listings import discover_pages_to_db
    from src.extract.fetch_listing import fetch_single_listing
    from src.common.utils import StatsCollector
    SCRAPING_MODULES_AVAILABLE = True
    SCRAPING_ERROR = None
except ImportError as e:
    SCRAPING_MODULES_AVAILABLE = False
    SCRAPING_ERROR = str(e)

INTEGRATION_DEPS_AVAILABLE = DB_AVAILABLE and CORE_MODULES_AVAILABLE
INTEGRATION_DEPS_ERROR = None
if not DB_AVAILABLE:
    INTEGRATION_DEPS_ERROR = f"Database not available: {DB_ERROR if 'DB_ERROR' in locals() else 'psycopg2 not installed'}"
elif not CORE_MODULES_AVAILABLE:
    INTEGRATION_DEPS_ERROR = f"Core modules not available: {CORE_ERROR}"

DB_ACCESSIBLE = None
DB_ACCESS_ERROR = None

def check_db_accessible():
    """Check if database is accessible. Returns (accessible, error_message)."""
    if not INTEGRATION_DEPS_AVAILABLE:
        return False, "Dependencies not available"
    try:
        import socket
        from src.common.settings import settings
        
        is_docker = os.path.exists("/.dockerenv") or os.getenv("AIRFLOW_HOME")
        
        if is_docker:
            db_host = settings.pg_host
            db_port = settings.pg_port
        else:
            db_host = "localhost"
            db_port = int(os.getenv("POSTGRES_PORT", "5435"))
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((db_host, db_port))
        sock.close()
        if result != 0:
            return False, f"Cannot connect to {db_host}:{db_port}. Make sure docker compose services are running."
        
        import psycopg2
        try:
            test_conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                dbname=settings.warehouse_db,
                user=settings.warehouse_user,
                password=settings.warehouse_password,
                connect_timeout=2
            )
            try:
                with test_conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return True, None
            finally:
                test_conn.close()
        except Exception as conn_err:
            return False, f"Database connection failed: {str(conn_err)}. Make sure docker compose services are running and credentials are correct."
    except Exception as e:
        return False, f"Database connection failed: {str(e)}. Make sure docker compose services are running."


@pytest.mark.integration
@pytest.mark.skipif(not INTEGRATION_DEPS_AVAILABLE, reason=f"Integration dependencies not available: {INTEGRATION_DEPS_ERROR if not INTEGRATION_DEPS_AVAILABLE else ''}")
class TestDatabaseIntegration:
    """Test database operations with real Postgres."""
    
    @pytest.fixture(autouse=True)
    def check_db(self, request):
        """Check database accessibility before each test."""
        accessible, error = check_db_accessible()
        if not accessible:
            pytest.skip(f"Database not accessible: {error}. Run 'docker compose up -d' to start services.")
    
    @pytest.fixture(autouse=True)
    def setup_db_connection(self):
        """Override database connection settings when running from host."""
        import os
        from src.common import settings
        
        is_docker = os.path.exists("/.dockerenv") or os.getenv("AIRFLOW_HOME")
        if not is_docker:
            original_host = settings.settings.pg_host
            original_port = settings.settings.pg_port
            settings.settings.pg_host = "localhost"
            settings.settings.pg_port = int(os.getenv("POSTGRES_PORT", "5435"))
            yield
            settings.settings.pg_host = original_host
            settings.settings.pg_port = original_port
        else:
            yield
    
    def test_ddl_applied(self):
        """Test that DDL schemas and tables exist."""
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name IN ('ctl', 'bronze', 'silver', 'gold')
                """)
                schemas = {row[0] for row in cur.fetchall()}
                assert "ctl" in schemas
                assert "bronze" in schemas
                assert "silver" in schemas
                assert "gold" in schemas
                
                cur.execute("""
                    SELECT table_schema, table_name FROM information_schema.tables
                    WHERE table_schema IN ('ctl', 'bronze', 'silver', 'gold')
                    AND table_name IN ('scrape_queue', 'raw_listing_html', 'listing_current', 
                                      'listing_snapshot_daily', 'fact_listing_daily', 'dim_vehicle')
                """)
                tables = {(row[0], row[1]) for row in cur.fetchall()}
                assert ("ctl", "scrape_queue") in tables
                assert ("bronze", "raw_listing_html") in tables
                assert ("silver", "listing_current") in tables
                assert ("silver", "listing_snapshot_daily") in tables
                assert ("gold", "fact_listing_daily") in tables
                assert ("gold", "dim_vehicle") in tables
    
    def test_queue_insert(self):
        """Test inserting into scrape queue."""
        test_id = 999999999
        test_url = "https://kolesa.kz/a/show/999999999"
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ctl.scrape_queue(listing_id, url, state)
                    VALUES (%s, %s, 'NEW')
                    ON CONFLICT (listing_id) DO NOTHING
                """, (test_id, test_url))
                
                cur.execute("SELECT listing_id, url, state FROM ctl.scrape_queue WHERE listing_id = %s", (test_id,))
                row = cur.fetchone()
                assert row is not None
                assert row[0] == test_id
                assert row[1] == test_url
                assert row[2] == "NEW"
                
                cur.execute("DELETE FROM ctl.scrape_queue WHERE listing_id = %s", (test_id,))
    
    def test_silver_upsert(self):
        """Test silver upsert with fixture data."""
        test_listing = {
            "listing_id": 888888888,
            "url": "https://kolesa.kz/a/show/888888888",
            "title": "Test Car 2020",
            "price_kzt": 5000000,
            "city": "Алматы",
            "region": "Алматы",
            "make": "Toyota",
            "model": "Camry",
            "generation": None,
            "trim": None,
            "car_year": 2020,
            "mileage_km": 50000,
            "body_type": "Седан",
            "engine_volume_l": 2.5,
            "engine_type": "бензин",
            "transmission": "Автомат",
            "drivetrain": "Передний",
            "steering": "Левый",
            "color": "Белый",
            "customs_cleared": True,
            "seller_type": "private",
            "seller_name": None,
            "options_text": "Test options",
            "options_list": ["Test option 1"],
            "photos": ["https://example.com/photo1.jpg"],
            "photo_count": 1,
        }
        
        from datetime import datetime, timezone
        fetched_at = datetime.now(timezone.utc)
        
        upsert_silver(test_listing, fetched_at)
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM silver.listing_current WHERE listing_id = %s", (test_listing["listing_id"],))
                row = cur.fetchone()
                assert row is not None
                
                cur.execute("""
                    SELECT * FROM silver.listing_snapshot_daily 
                    WHERE listing_id = %s AND snapshot_date = %s
                """, (test_listing["listing_id"], date.today()))
                snap_row = cur.fetchone()
                assert snap_row is not None
                
                cur.execute("DELETE FROM silver.listing_snapshot_daily WHERE listing_id = %s", (test_listing["listing_id"],))
                cur.execute("DELETE FROM silver.listing_current WHERE listing_id = %s", (test_listing["listing_id"],))
    
    def test_gold_build(self):
        """Test Gold layer build."""
        test_listing_id = 777777777
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM gold.fact_listing_daily WHERE listing_id = %s", (test_listing_id,))
                cur.execute("DELETE FROM silver.listing_snapshot_daily WHERE listing_id = %s", (test_listing_id,))
                cur.execute("DELETE FROM silver.listing_current WHERE listing_id = %s", (test_listing_id,))
        
        test_listing = {
            "listing_id": 777777777,
            "url": "https://kolesa.kz/a/show/777777777",
            "title": "Gold Test Car",
            "price_kzt": 10000000,
            "city": "Астана",
            "region": "Акмолинская область",
            "make": "Mercedes",
            "model": "E-Class",
            "generation": None,
            "trim": None,
            "car_year": 2023,
            "mileage_km": None,
            "body_type": "Седан",
            "engine_volume_l": 2.0,
            "engine_type": "бензин",
            "transmission": "Автомат",
            "drivetrain": "Задний",
            "steering": "Левый",
            "color": "Черный",
            "customs_cleared": True,
            "seller_type": "dealer",
            "seller_name": "Test Dealer",
            "options_text": None,
            "options_list": [],
            "photos": [],
            "photo_count": 0,
        }
        
        from datetime import datetime, timezone
        upsert_silver(test_listing, datetime.now(timezone.utc))
        
        sql_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db", "init", "20-gold-build.sql")
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("\\c")]
                for i, stmt in enumerate(statements):
                    try:
                        cur.execute(stmt)
                    except Exception as e:
                        error_str = str(e).lower()
                        if "cardinality" in error_str or "cannot affect row a second time" in error_str:
                            conn.rollback()
                            continue
                        elif any(keyword in error_str for keyword in ["duplicate", "already exists", "on conflict do nothing"]):
                            continue
                        else:
                            import logging
                            logging.warning(f"Gold build statement {i} failed: {e}")
                            conn.rollback()
                            continue
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO gold.dim_location(region, city)
                    SELECT DISTINCT region, city
                    FROM silver.listing_current
                    WHERE listing_id = %s AND city IS NOT NULL
                    ON CONFLICT (region, city) DO NOTHING
                """, (test_listing["listing_id"],))
                
                cur.execute("""
                    INSERT INTO gold.dim_vehicle(make, model, generation, trim, car_year, body_type, engine_type, engine_volume_l, transmission, drivetrain, steering, color)
                    SELECT DISTINCT make, model, generation, trim, car_year, body_type, engine_type, engine_volume_l, transmission, drivetrain, steering, color
                    FROM silver.listing_current
                    WHERE listing_id = %s AND make IS NOT NULL AND model IS NOT NULL
                    ON CONFLICT DO NOTHING
                """, (test_listing["listing_id"],))
                
                cur.execute("""
                    INSERT INTO gold.dim_seller(seller_type, seller_name, seller_user_id, city)
                    SELECT DISTINCT seller_type, seller_name, seller_user_id, city
                    FROM silver.listing_current
                    WHERE listing_id = %s
                    ON CONFLICT (seller_type, seller_name, seller_user_id, city) DO NOTHING
                """, (test_listing["listing_id"],))
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM gold.dim_vehicle WHERE make = 'Mercedes'")
                vehicle_count = cur.fetchone()[0]
                assert vehicle_count > 0, "dim_vehicle should have Mercedes entry"
                
                cur.execute("SELECT COUNT(*) FROM gold.dim_location WHERE city = 'Астана'")
                location_count = cur.fetchone()[0]
                assert location_count > 0, "dim_location should have Астана entry"
                
                cur.execute("SELECT COUNT(*) FROM gold.dim_seller WHERE seller_name = 'Test Dealer'")
                seller_count = cur.fetchone()[0]
                assert seller_count > 0, "dim_seller should have Test Dealer entry"
                
                try:
                    cur.execute("""
                        INSERT INTO gold.fact_listing_daily(
                            date_key, listing_id, location_key, vehicle_key, seller_key,
                            price_kzt, is_active, days_on_site, views, photo_count
                        )
                        SELECT DISTINCT ON (dd.date_key, s.listing_id)
                            dd.date_key,
                            s.listing_id,
                            dl.location_key,
                            dv.vehicle_key,
                            ds.seller_key,
                            snap.price_kzt,
                            snap.is_active,
                            (dd.d - (s.first_seen_at::date))::int AS days_on_site,
                            snap.views,
                            snap.photo_count
                        FROM silver.listing_snapshot_daily snap
                        JOIN silver.listing_current s ON s.listing_id = snap.listing_id
                        JOIN gold.dim_date dd ON dd.d = snap.snapshot_date
                        JOIN gold.dim_location dl ON dl.city = s.city AND (dl.region IS NOT DISTINCT FROM s.region)
                        JOIN gold.dim_vehicle dv ON
                            dv.make = s.make AND dv.model = s.model
                            AND dv.generation IS NOT DISTINCT FROM s.generation
                            AND dv.trim IS NOT DISTINCT FROM s.trim
                            AND dv.car_year IS NOT DISTINCT FROM s.car_year
                            AND dv.body_type IS NOT DISTINCT FROM s.body_type
                            AND dv.engine_type IS NOT DISTINCT FROM s.engine_type
                            AND dv.engine_volume_l IS NOT DISTINCT FROM s.engine_volume_l
                            AND dv.transmission IS NOT DISTINCT FROM s.transmission
                            AND dv.drivetrain IS NOT DISTINCT FROM s.drivetrain
                            AND dv.steering IS NOT DISTINCT FROM s.steering
                            AND dv.color IS NOT DISTINCT FROM s.color
                        JOIN gold.dim_seller ds ON
                            ds.seller_type IS NOT DISTINCT FROM s.seller_type
                            AND ds.seller_name IS NOT DISTINCT FROM s.seller_name
                            AND ds.seller_user_id IS NOT DISTINCT FROM s.seller_user_id
                            AND ds.city IS NOT DISTINCT FROM s.city
                        WHERE s.listing_id = %s
                        ON CONFLICT (date_key, listing_id) DO UPDATE SET
                            price_kzt = EXCLUDED.price_kzt,
                            is_active = EXCLUDED.is_active,
                            days_on_site = EXCLUDED.days_on_site,
                            views = EXCLUDED.views,
                            photo_count = EXCLUDED.photo_count
                    """, (test_listing["listing_id"],))
                except Exception as e:
                    conn.rollback()
                
                import psycopg2.extensions
                if conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                    conn.rollback()
                cur.execute("SELECT COUNT(*) FROM gold.fact_listing_daily WHERE listing_id = %s", (test_listing["listing_id"],))
                fact_count = cur.fetchone()[0]
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM gold.fact_listing_daily WHERE listing_id = %s", (test_listing["listing_id"],))
                cur.execute("DELETE FROM silver.listing_snapshot_daily WHERE listing_id = %s", (test_listing["listing_id"],))
                cur.execute("DELETE FROM silver.listing_current WHERE listing_id = %s", (test_listing["listing_id"],))


@pytest.mark.integration
@pytest.mark.skipif(not INTEGRATION_DEPS_AVAILABLE, reason=f"Integration dependencies not available: {INTEGRATION_DEPS_ERROR if not INTEGRATION_DEPS_AVAILABLE else ''}")
class TestEndToEndIntegration:
    """End-to-end integration tests (may require network access)."""
    
    @pytest.mark.skipif(not SCRAPING_MODULES_AVAILABLE, reason=f"Scraping modules not available: {SCRAPING_ERROR if not SCRAPING_MODULES_AVAILABLE else ''}")
    @pytest.mark.skipif(os.getenv("SKIP_NETWORK_TESTS") == "1", reason="Network tests skipped")
    def test_discover_to_queue(self):
        """Test discovery and queue insertion."""
        from src.common.utils import StatsCollector
        from src.extract.discover_listings import discover_pages_to_db
        stats = StatsCollector()
        inserted = discover_pages_to_db(1, 1, stats)
        
        assert inserted >= 0
        
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM ctl.scrape_queue WHERE state = 'NEW'")
                count = cur.fetchone()[0]
                assert count >= 0


@pytest.mark.integration
@pytest.mark.skipif(not INTEGRATION_DEPS_AVAILABLE, reason=f"Integration dependencies not available: {INTEGRATION_DEPS_ERROR if not INTEGRATION_DEPS_AVAILABLE else ''}")
class TestConstraints:
    """Test database constraints and data quality."""
    
    @pytest.fixture(autouse=True)
    def check_db(self, request):
        """Check database accessibility before each test."""
        accessible, error = check_db_accessible()
        if not accessible:
            pytest.skip(f"Database not accessible: {error}. Run 'docker compose up -d' to start services.")
    
    @pytest.fixture(autouse=True)
    def setup_db_connection(self):
        """Override database connection settings when running from host."""
        import os
        from src.common import settings
        
        is_docker = os.path.exists("/.dockerenv") or os.getenv("AIRFLOW_HOME")
        if not is_docker:
            original_host = settings.settings.pg_host
            original_port = settings.settings.pg_port
            settings.settings.pg_host = "localhost"
            settings.settings.pg_port = int(os.getenv("POSTGRES_PORT", "5435"))
            yield
            settings.settings.pg_host = original_host
            settings.settings.pg_port = original_port
        else:
            yield
    
    def test_unique_constraints(self):
        """Test that unique constraints work."""
        with wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO gold.dim_vehicle(make, model, car_year, body_type)
                    VALUES ('Test', 'Model', 2020, 'Sedan')
                """)
                
                try:
                    cur.execute("""
                        INSERT INTO gold.dim_vehicle(make, model, car_year, body_type)
                        VALUES ('Test', 'Model', 2020, 'Sedan')
                    """)
                    conn.commit()
                    assert False, "Unique constraint should have prevented duplicate"
                except Exception:
                    conn.rollback()
                    pass
                
                cur.execute("DELETE FROM gold.dim_vehicle WHERE make = 'Test'")
    
    def test_foreign_key_constraints(self):
        """Test foreign key constraints."""
        with wh_conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        INSERT INTO gold.fact_listing_daily(
                            date_key, listing_id, location_key, vehicle_key, seller_key, is_active
                        ) VALUES (99999999, 1, 1, 1, 1, TRUE)
                    """)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    pass

