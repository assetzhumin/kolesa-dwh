"""
Fetch listing detail pages and process them.

Key features:
- Uses 'commit' wait strategy for faster, more reliable page loads
- Includes 2s wait after page commit for JS to execute
- Direct access (no homepage visit) for better performance
- Fallback to homepage visit if direct access times out
- Improved blocking detection (less false positives)
- Saves to MinIO (Bronze) and Postgres (Silver) when enabled
- Statistics collection for monitoring
- Idempotent: handles duplicates gracefully
"""
import boto3
import gzip
import json
import io
import time
import traceback
import hashlib
import logging
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from ..common.settings import settings
from ..common.db import wh_conn
from ..common.utils import detect_blocking, random_sleep, exponential_backoff_with_jitter, StatsCollector, BlockingDetectedError
from ..transform.parse_listing_html import parse_listing
from ..transform.silver_upsert import upsert_silver

logger = logging.getLogger(__name__)

def get_minio_client() -> Any:
    """
    Get MinIO/S3 client.
    
    Returns:
        boto3 S3 client instance
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
    )


def hashlib_sha256(data: bytes) -> str:
    """
    Calculate SHA256 hash of binary data.
    
    Args:
        data: Binary data to hash
        
    Returns:
        Hexadecimal hash string
    """
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def fetch_single_listing(
    listing_id: int,
    url: str,
    use_db: bool = False,
    use_s3: bool = False,
    stats: Optional[StatsCollector] = None
) -> Optional[Dict]:
    """
    Fetch a single listing page and return parsed data.
    If use_db=True, saves to bronze/silver. If use_s3=True, saves HTML to MinIO.
    Returns parsed dict or None on failure.
    """
    try:
        random_sleep(settings.sleep_min_ms, settings.sleep_max_ms)
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                extra_http_headers={
                    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                },
            )
            page = context.new_page()
            try:
                # Use 'load' state for navigation (faster than networkidle, more reliable than commit)
                # Then wait for specific content elements to ensure JavaScript has rendered the page
                resp = page.goto(url, timeout=15000, wait_until="load")
                logger.debug(f"Page load event fired for {listing_id}, waiting for content elements...")
                
                # Wait for key listing elements to appear (indicating page is fully rendered by JS)
                # This is event-based: we wait for the actual content, not a fixed timeout
                try:
                    # Wait for price or main content elements (indicates listing data is loaded)
                    # Using 'attached' instead of 'visible' to be more permissive (element exists in DOM)
                    page.wait_for_selector(
                        "[data-price], .offer__price, .advert__price, .price, [itemprop='price'], .offer__content, .advert__content",
                        timeout=8000,
                        state="attached"
                    )
                    logger.debug(f"Listing content elements found for {listing_id}")
                except Exception as e:
                    # If specific selectors don't exist or timeout, wait for network to be mostly idle
                    # Use a short timeout for networkidle to avoid hanging
                    try:
                        page.wait_for_load_state("networkidle", timeout=3000)
                        logger.debug(f"Network idle for {listing_id} (fallback)")
                    except Exception:
                        # Final fallback: just ensure DOM is ready
                        logger.debug(f"Using DOM ready fallback for {listing_id}")
                
                status = resp.status
                html = page.content()
            except PlaywrightTimeoutError:
                # Fallback: try with homepage visit if direct access fails
                logger.warning(f"Direct access timed out for {listing_id}, trying with homepage visit")
                try:
                    # Use 'load' for homepage (faster, more reliable)
                    page.goto("https://kolesa.kz", timeout=15000, wait_until="load")
                    # Small random delay before navigating to listing (simulate human behavior)
                    random_sleep(500, 1500)
                    resp = page.goto(url, timeout=15000, wait_until="load")
                    
                    # Wait for listing content elements
                    try:
                        page.wait_for_selector(
                            "[data-price], .offer__price, .advert__price, .price, [itemprop='price'], .offer__content, .advert__content",
                            timeout=8000,
                            state="attached"
                        )
                    except Exception:
                        # Fallback to networkidle with short timeout, then DOM ready
                        try:
                            page.wait_for_load_state("networkidle", timeout=3000)
                        except Exception:
                            pass
                    
                    status = resp.status
                    html = page.content()
                except Exception as e:
                    logger.error(f"Failed to fetch {listing_id} even with homepage visit: {e}")
                    raise
            finally:
                page.close()
                browser.close()
        
        # Check for blocking
        is_blocked, reason = detect_blocking(html, url)
        if is_blocked:
            if stats:
                stats.record_blocked()
            raise BlockingDetectedError(f"Blocking detected: {reason}")
        
        if stats:
            stats.record_fetched(status)
        
        fetched_at = datetime.now(timezone.utc)
        
        # Save to MinIO if requested
        if use_s3:
            s3 = get_minio_client()
            date_prefix = fetched_at.strftime("%Y/%m/%d")
            ts_str = fetched_at.strftime("%H%M%S")
            object_key = f"{date_prefix}/{listing_id}_{ts_str}.html.gz"
            
            gz_buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=gz_buffer, mode="wb") as f:
                f.write(html.encode("utf-8"))
            gz_buffer.seek(0)
            
            s3.put_object(
                Bucket=settings.minio_bucket,
                Key=object_key,
                Body=gz_buffer,
                ContentType="text/html",
                ContentEncoding="gzip"
            )
            
            # Save metadata to DB
            if use_db:
                sha256 = hashlib_sha256(html.encode("utf-8"))
                with wh_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO bronze.raw_listing_html(listing_id, fetched_at, bucket, object_key, sha256, http_status)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (listing_id, fetched_at, settings.minio_bucket, object_key, sha256, status))
        
        # Parse
        if status == 404:
            logger.warning(f"Listing {listing_id} returned 404 (removed/inactive)")
            if use_db:
                mark_silver_inactive(listing_id)
            return None
        
        if status >= 400:
            raise Exception(f"HTTP {status} for {url}")
        
        parsed = parse_listing(html, url, listing_id)
        
        if stats:
            stats.record_parsed()
        
        # Save to silver if requested
        if use_db:
            upsert_silver(parsed, fetched_at)
        
        return parsed
        
    except BlockingDetectedError:
        raise
    except Exception as e:
        logger.error(f"Error fetching {listing_id}: {e}")
        if stats:
            stats.record_failed(str(e))
        return None

def fetch_listings_batch(
    listing_ids: List[int],
    base_url: Optional[str] = None,
    concurrency: Optional[int] = None,
    use_db: bool = False,
    use_s3: bool = False,
    stats: Optional[StatsCollector] = None,
    seen_ids: Optional[set] = None
) -> List[Dict]:
    """
    Fetch multiple listings with controlled concurrency.
    Returns list of parsed dicts (skips duplicates if seen_ids provided).
    """
    if base_url is None:
        base_url = settings.kolesa_base_url
    if concurrency is None:
        concurrency = settings.concurrency
    
    if seen_ids is None:
        seen_ids = set()
    
    results = []
    urls = {lid: f"{base_url}/a/show/{lid}" for lid in listing_ids}
    
    def fetch_one(lid: int) -> Tuple[int, Optional[Dict]]:
        if lid in seen_ids:
            if stats:
                stats.record_duplicate()
            return lid, None
        return lid, fetch_single_listing(lid, urls[lid], use_db, use_s3, stats)
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(fetch_one, lid): lid for lid in listing_ids}
        
        for future in as_completed(futures):
            try:
                lid, parsed = future.result()
                if parsed:
                    results.append(parsed)
                    seen_ids.add(lid)
            except BlockingDetectedError as e:
                logger.error(f"Blocking detected while fetching. Stopping batch.")
                # Cancel remaining tasks
                for f in futures:
                    f.cancel()
                raise
            except Exception as e:
                logger.error(f"Error in batch fetch: {e}")
                continue
    
    return results

def fetch_and_process_batch(batch_size: int = 200) -> int:
    """
    Fetch and process a batch from the database queue (for Airflow).
    Returns number of successfully processed items.
    """
    # Claim batch
    items = []
    with wh_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT listing_id, url, attempts
                FROM ctl.scrape_queue
                WHERE state IN ('NEW', 'RETRY')
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            """, (batch_size,))
            items = cur.fetchall()

    if not items:
        logger.info("No items to fetch.")
        return 0

    stats = StatsCollector()
    processed_count = 0
    total_items = len(items)
    
    logger.info(f"Starting to fetch {total_items} listings...")

    for idx, (listing_id, url, attempts) in enumerate(items, start=1):
        try:
            # Update attempt tracking before processing
            with wh_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE ctl.scrape_queue
                        SET last_attempt_at = NOW(), attempts = attempts + 1
                        WHERE listing_id = %s
                    """, (listing_id,))
            
            parsed = fetch_single_listing(listing_id, url, use_db=True, use_s3=True, stats=stats)
            
            if parsed:
                update_state(listing_id, "FETCHED", 200, None)
                processed_count += 1
            else:
                # 404 or parse failure
                update_state(listing_id, "INACTIVE", 404, None)
            
            # Log progress every 5 listings
            if idx % 5 == 0:
                logger.info(f"Progress: {idx}/{total_items} listings processed ({processed_count} successful, {idx - processed_count} failed/skipped)")
                
        except BlockingDetectedError:
            logger.error("Blocking detected. Stopping batch processing.")
            schedule_retry(listing_id, attempts + 1, 0, "Blocking detected")
            break
        except Exception as e:
            logger.error(f"Error processing {listing_id}: {e}")
            schedule_retry(listing_id, attempts + 1, 0, str(e))
            # Still log progress even on error
            if idx % 5 == 0:
                logger.info(f"Progress: {idx}/{total_items} listings processed ({processed_count} successful, {idx - processed_count} failed/skipped)")
    
    stats.log_summary()
    return processed_count

def update_state(listing_id, state, status, error):
    """Update queue state."""
    with wh_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE ctl.scrape_queue
                SET state = %s, last_http_status = %s, last_error = %s, next_retry_at = NULL
                WHERE listing_id = %s
            """, (state, status, error, listing_id))

def schedule_retry(listing_id, attempts, status, error):
    """Schedule retry with exponential backoff."""
    backoff_minutes = min(2 ** attempts, 60*24)  # Cap at 1 day
    next_retry = datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes)
    
    with wh_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE ctl.scrape_queue
                SET state = 'RETRY', attempts = %s, last_http_status = %s, last_error = %s, next_retry_at = %s
                WHERE listing_id = %s
            """, (attempts, status, error, next_retry, listing_id))

def mark_silver_inactive(listing_id):
    """Mark listing as inactive in silver."""
    with wh_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE silver.listing_current SET is_active = FALSE WHERE listing_id = %s", (listing_id,))
            cur.execute("""
                UPDATE silver.listing_snapshot_daily 
                SET is_active = FALSE 
                WHERE listing_id = %s AND snapshot_date = CURRENT_DATE
            """, (listing_id,))
