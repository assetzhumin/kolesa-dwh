"""Fetch and enrich view counts for listings from kolesa.kz API."""
import httpx
import time
import logging
from datetime import date
from typing import List, Tuple

from ..common.db import wh_conn

logger = logging.getLogger(__name__)


def enrich_views_for_today(batch_size: int = 50) -> None:
    """
    Enrich view counts for today's snapshots from kolesa.kz API.
    
    Args:
        batch_size: Number of listing IDs to fetch per API request (default: 50)
    """
    today = date.today()
    
    # Get IDs that need views
    ids_to_fetch: List[int] = []
    with wh_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT listing_id FROM silver.listing_snapshot_daily
                WHERE snapshot_date = %s AND views IS NULL
            """, (today,))
            ids_to_fetch = [row[0] for row in cur.fetchall()]
    
    if not ids_to_fetch:
        logger.info("No views to enrich today.")
        return

    logger.info(f"Enriching views for {len(ids_to_fetch)} listings...")

    # Chunk IDs for batch API requests
    chunks: List[List[int]] = [
        ids_to_fetch[i:i + batch_size] 
        for i in range(0, len(ids_to_fetch), batch_size)
    ]

    total_updated = 0
    with httpx.Client(timeout=10.0) as client:
        for chunk_idx, chunk in enumerate(chunks, 1):
            id_str = ",".join(map(str, chunk))
            url = f"https://kolesa.kz/ms/views/kolesa/live/{id_str}/"
            
            try:
                resp = client.get(
                    url, 
                    headers={"User-Agent": "Mozilla/5.0 (compatible; AnalyticsBot/1.0)"}
                )
                if resp.status_code == 200:
                    data = resp.json()
                    views_map = data.get("data", {})
                    
                    if not isinstance(views_map, dict):
                        logger.warning(f"Unexpected response format for chunk {chunk_idx}: {type(data)}")
                        continue

                    updates: List[Tuple[int, int, date]] = []
                    for lid_str, views in views_map.items():
                        try:
                            listing_id = int(lid_str)
                            
                            # Handle both integer and dict responses
                            if isinstance(views, dict):
                                # Extract view count from dict (try common keys)
                                view_count = views.get("views") or views.get("count") or views.get("value")
                                if view_count is None:
                                    # If no standard key, try to get first numeric value
                                    view_count = next((v for v in views.values() if isinstance(v, (int, float))), None)
                                if view_count is None:
                                    logger.warning(f"Could not extract view count from dict for listing {lid_str}: {views}")
                                    continue
                                view_count = int(view_count)
                            elif isinstance(views, (int, float)):
                                view_count = int(views)
                            else:
                                # Try to convert string to int
                                view_count = int(views)
                            
                            updates.append((view_count, listing_id, today))
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Error parsing view count for listing {lid_str}: {e} (value: {views}, type: {type(views)})")
                            continue

                    if updates:
                        with wh_conn() as conn:
                            with conn.cursor() as cur:
                                cur.executemany("""
                                    UPDATE silver.listing_snapshot_daily
                                    SET views = %s
                                    WHERE listing_id = %s AND snapshot_date = %s
                                """, updates)
                        total_updated += len(updates)
                        logger.info(f"Updated {len(updates)} views for chunk {chunk_idx}/{len(chunks)}")
                    
                    # Rate limiting: small delay between requests
                    time.sleep(0.5)
                else:
                    logger.warning(f"Failed to fetch views for chunk {chunk_idx}: HTTP {resp.status_code}")
            except httpx.TimeoutException:
                logger.error(f"Timeout fetching views for chunk {chunk_idx}")
            except httpx.RequestError as e:
                logger.error(f"Request error fetching views for chunk {chunk_idx}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error fetching views for chunk {chunk_idx}: {e}", exc_info=True)
    
    logger.info(f"Views enrichment completed. Total updated: {total_updated}/{len(ids_to_fetch)}")

