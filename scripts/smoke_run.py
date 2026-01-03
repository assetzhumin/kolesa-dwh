#!/usr/bin/env python3
"""
Smoke test script: fetches 1 page, 3 listings max, writes to ./out/
"""
import sys
import os
import json
import csv
from pathlib import Path

project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

try:
    from src.extract.discover_listings import discover_ids_from_page
    from src.extract.fetch_listing import fetch_single_listing
    from src.common.utils import StatsCollector
except ImportError as e:
    print(f"Error: Missing dependencies. Please install them with:")
    print(f"  pip install -r requirements-local.txt")
    print(f"  python -m playwright install chromium")
    print(f"\nOriginal error: {e}")
    sys.exit(1)

def main():
    """Run smoke test."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Smoke test: fetch 1 page, 3 listings max")
    parser.add_argument("--local", action="store_true", help="Run locally (not in Docker)")
    parser.add_argument("--output-dir", default="./out", help="Output directory (default: ./out)")
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    print("=" * 60)
    print("Kolesa.kz DWH Pipeline - Smoke Test")
    print("=" * 60)
    
    stats = StatsCollector()
    
    print("\n[1/3] Discovering listings from page 1...")
    try:
        ids = discover_ids_from_page(1, stats=stats)
        ids_list = sorted([int(x) for x in ids])[:3]  # Take first 3
        print(f"✓ Found {len(ids)} total IDs, fetching first 3: {ids_list}")
    except Exception as e:
        print(f"✗ Discovery failed: {e}")
        sys.exit(1)
    
    if not ids_list:
        print("✗ No listings found on page 1")
        sys.exit(1)
    
    print(f"\n[2/3] Fetching {len(ids_list)} listings...")
    results = []
    base_url = os.getenv("KOLESA_BASE_URL", "https://kolesa.kz")
    
    for listing_id in ids_list:
        url = f"{base_url}/a/show/{listing_id}"
        print(f"  Fetching {listing_id}...", end=" ", flush=True)
        try:
            parsed = fetch_single_listing(
                listing_id, url, 
                use_db=False, 
                use_s3=False, 
                stats=stats
            )
            if parsed:
                results.append(parsed)
                print("✓")
            else:
                print("✗ (404 or parse failure)")
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
    
    if not results:
        print("\n✗ No listings successfully fetched")
        sys.exit(1)
    
    print(f"\n✓ Successfully fetched {len(results)} listings")
    
    print(f"\n[3/3] Writing outputs to {output_dir}/...")
    
    json_path = output_dir / "smoke_test_listings.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"  ✓ JSON: {json_path}")
    
    jsonl_path = output_dir / "smoke_test_listings.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for item in results:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"  ✓ JSONL: {jsonl_path}")
    
    csv_path = output_dir / "smoke_test_listings.csv"
    if results:
        fieldnames = list(results[0].keys())
        for item in results:
            if "photos" in item and isinstance(item["photos"], list):
                item["photos"] = "; ".join(item["photos"])
        
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"  ✓ CSV: {csv_path}")
    
    print("\n" + "=" * 60)
    print("Smoke Test Summary")
    print("=" * 60)
    stats.log_summary()
    print(f"\n✓ Smoke test completed successfully!")
    print(f"  Outputs written to: {output_dir}/")
    print(f"  - {json_path.name}: {len(results)} listings")
    print(f"  - {jsonl_path.name}: {len(results)} listings")
    print(f"  - {csv_path.name}: {len(results)} listings")

if __name__ == "__main__":
    main()

