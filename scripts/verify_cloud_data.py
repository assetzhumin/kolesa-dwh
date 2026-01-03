#!/usr/bin/env python3
"""
Verify data in Supabase cloud database.
"""
import os
import sys
import argparse
from typing import Dict, Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from dotenv import load_dotenv
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
except ImportError:
    pass

def verify_supabase(connection_string: str) -> Dict[str, Any]:
    """Verify data in Supabase."""
    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2-binary package not installed. Install with: pip install psycopg2-binary")
        return {"error": "psycopg2 not installed"}
    
    try:
        conn = psycopg2.connect(connection_string)
        print("✓ Connected to Supabase")
        
        results = {}
        
        gold_tables = [
            "dim_date",
            "dim_location", 
            "dim_vehicle",
            "dim_seller",
            "fact_listing_daily",
            "fact_price_event"
        ]
        
        print("\n🔍 Checking Gold tables...")
        with conn.cursor() as cur:
            for table in gold_tables:
                try:
                    full_table = f"gold.{table}"
                    cur.execute(f"SELECT COUNT(*) FROM {full_table}")
                    row_count = cur.fetchone()[0]
                    results[table] = row_count
                    print(f"  ✓ {table}: {row_count:,} rows")
                    
                    if table.startswith("fact_") and row_count > 0:
                        cur.execute(f"SELECT * FROM {full_table} LIMIT 3")
                        sample = cur.fetchall()
                        if sample:
                            print(f"    Sample: {sample[0]}")
                            
                except Exception as e:
                    results[table] = f"ERROR: {str(e)}"
                    print(f"  ✗ {table}: {str(e)}")
        
        print("\n📈 Verification Queries:")
        
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT COUNT(DISTINCT listing_id) FROM gold.fact_listing_daily")
                total = cur.fetchone()
                print(f"  Total unique listings: {total[0] if total else 0:,}")
            except Exception as e:
                print(f"  ✗ Could not count listings: {e}")
            
            try:
                cur.execute("SELECT MAX(date_key) FROM gold.fact_listing_daily")
                latest = cur.fetchone()
                if latest and latest[0]:
                    print(f"  Latest date_key: {latest[0]}")
            except Exception as e:
                print(f"  ✗ Could not get latest date: {e}")
            
            try:
                cur.execute("""
                    SELECT dv.make, COUNT(*) as count
                    FROM gold.fact_listing_daily f
                    JOIN gold.dim_vehicle dv ON f.vehicle_key = dv.vehicle_key
                    GROUP BY dv.make
                    ORDER BY count DESC
                    LIMIT 5
                """)
                top_makes = cur.fetchall()
                print(f"  Top 5 makes: {[(m[0], m[1]) for m in top_makes]}")
            except Exception as e:
                print(f"  ✗ Could not get top makes: {e}")
        
        conn.close()
        return results
        
    except Exception as e:
        print(f"ERROR: Failed to connect to Supabase: {e}")
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Verify data in Supabase cloud database")
    args = parser.parse_args()
    
    connection_string = os.getenv("SUPABASE_DB_URL")
    if not connection_string:
        print("ERROR: SUPABASE_DB_URL environment variable is required")
        print("Get your connection string from Supabase: Settings → Database → Connection string (URI)")
        print("See SUPABASE_SETUP.md for detailed instructions")
        sys.exit(1)
    
    verify_supabase(connection_string)


if __name__ == "__main__":
    main()
