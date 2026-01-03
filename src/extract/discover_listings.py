"""
Discover listing IDs from pagination pages.

Key features:
- Correct URL handling: Page 1 uses /cars/, page 2+ uses /cars/?page=n
- Uses 'commit' wait strategy for faster, more reliable page loads
- Includes 2s wait after page commit for JS to populate listings
- Improved blocking detection (less false positives)
- Retry logic with exponential backoff
- Statistics collection for monitoring
"""
import re
import logging
from typing import Set, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from ..common.settings import settings
from ..common.utils import detect_blocking, random_sleep, StatsCollector, BlockingDetectedError

logger = logging.getLogger(__name__)

ID_RE = re.compile(r"listing\.grid\.push\(\{\s*id:\s*(\d+)", re.MULTILINE)
HREF_RE = re.compile(r"/a/show/(\d+)", re.MULTILINE)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((PlaywrightTimeoutError, Exception)),
    reraise=True
)
def fetch_html(url: str, stats: Optional[StatsCollector] = None) -> str:
    """
    Fetch HTML from URL using Playwright.
    Raises BlockingDetectedError if blocking is detected.
    """
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
            # Use 'load' state for navigation, then wait for listings to populate
            try:
                resp = page.goto(url, timeout=15000, wait_until="load")
                logger.debug(f"Page loaded for {url}, waiting for listings to populate...")
                
                # Wait for listing grid or car cards to appear (indicates JS has populated listings)
                try:
                    # Wait for common listing container elements
                    page.wait_for_selector(
                        ".a-list, .offer-card, .advert-card, [data-listing-id], .car-card, .listing-card",
                        timeout=10000,
                        state="attached"
                    )
                    logger.debug(f"Listing elements found for {url}")
                except Exception:
                    # Fallback: wait for network to be mostly idle
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                        logger.debug(f"Network idle for {url} (fallback)")
                    except Exception:
                        # Final fallback: wait a bit longer for JS to execute
                        page.wait_for_timeout(3000)
                        logger.debug(f"Using timeout fallback for {url}")
                
                status = resp.status if resp else 200
            except PlaywrightTimeoutError:
                # Fallback: try with homepage visit if direct access fails
                logger.warning(f"Direct access timed out for {url}, trying with homepage visit")
                page.goto("https://kolesa.kz", timeout=15000, wait_until="load")
                random_sleep(500, 1500)
                resp = page.goto(url, timeout=15000, wait_until="load")
                
                # Wait for listings to populate
                try:
                    page.wait_for_selector(
                        ".a-list, .offer-card, .advert-card, [data-listing-id], .car-card, .listing-card",
                        timeout=10000,
                        state="attached"
                    )
                except Exception:
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        page.wait_for_timeout(3000)
                
                status = resp.status if resp else 200
            
            if stats:
                stats.record_fetched(status)
            
            if status >= 400:
                raise Exception(f"HTTP {status} for {url}")
            
            html = page.content()
            
            # Check for blocking
            is_blocked, reason = detect_blocking(html, url)
            if is_blocked:
                if stats:
                    stats.record_blocked()
                raise BlockingDetectedError(f"Blocking detected: {reason}")
            
            return html
        finally:
            page.close()
            browser.close()

def discover_ids_from_page(page_num: int, stats: Optional[StatsCollector] = None) -> Set[int]:
    """
    Discover listing IDs from a single pagination page.
    Returns set of listing IDs.
    Note: Page 1 is /cars/, page 2+ is /cars/?page=n
    """
    if page_num == 1:
        url = f"{settings.kolesa_base_url}/cars/"
    else:
        url = f"{settings.kolesa_base_url}/cars/?page={page_num}"
    logger.info(f"Discovering listings from page {page_num}: {url}")
    
    try:
        html = fetch_html(url, stats)
        
        # Try multiple extraction methods for robustness
        ids = set()
        
        # Method 1: JavaScript pattern (listing.grid.push)
        id_matches = ID_RE.findall(html)
        ids.update(id_matches)
        
        # Method 2: Href patterns (/a/show/ID)
        href_matches = HREF_RE.findall(html)
        ids.update(href_matches)
        
        # Method 3: Additional patterns - data attributes
        data_id_re = re.compile(r'data-listing-id=["\'](\d+)["\']', re.IGNORECASE)
        data_matches = data_id_re.findall(html)
        ids.update(data_matches)
        
        # Method 4: Additional patterns - href in various formats
        href_variants_re = re.compile(r'href=["\']/a/show/(\d+)', re.IGNORECASE)
        href_variant_matches = href_variants_re.findall(html)
        ids.update(href_variant_matches)
        
        # Convert to integers and validate
        ids = {int(sid) for sid in ids if sid.isdigit()}
        
        if stats:
            stats.record_discovered(len(ids))
        
        if len(ids) == 0:
            logger.warning(f"No listing IDs found on page {page_num}. HTML length: {len(html)}. "
                         f"First 500 chars: {html[:500]}")
        else:
            logger.info(f"Found {len(ids)} listing IDs on page {page_num}")
        
        return ids
    except BlockingDetectedError as e:
        logger.error(f"Blocking detected on page {page_num}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error discovering page {page_num}: {e}")
        if stats:
            stats.record_failed(str(e))
        raise

def discover_pages(page_from: int = 1, page_to: int = 50, stats: Optional[StatsCollector] = None) -> int:
    """
    Discover listing IDs from multiple pages and optionally insert into DB.
    Returns total number of unique IDs discovered.
    """
    all_ids = set()
    
    for page in range(page_from, page_to + 1):
        try:
            ids = discover_ids_from_page(page, stats)
            all_ids.update(ids)
            
            # Be polite between pages
            random_sleep(settings.sleep_min_ms, settings.sleep_max_ms)
            
            # Stop if no IDs found (likely reached end)
            if not ids:
                logger.info(f"No listings found on page {page}. Stopping discovery.")
                break
        except BlockingDetectedError:
            logger.error("Blocking detected. Stopping discovery to avoid further issues.")
            break
        except Exception as e:
            logger.warning(f"Failed to discover page {page}: {e}. Continuing...")
            continue
    
    logger.info(f"Total unique listing IDs discovered: {len(all_ids)}")
    return len(all_ids)

def discover_pages_to_db(page_from: int = 1, page_to: int = 50, stats: Optional[StatsCollector] = None) -> int:
    """
    Discover listing IDs and insert into database queue.
    Returns number of new IDs inserted.
    """
    from ..common.db import wh_conn
    
    all_ids = set()
    
    for page in range(page_from, page_to + 1):
        try:
            ids = discover_ids_from_page(page, stats)
            all_ids.update(ids)
            
            random_sleep(settings.sleep_min_ms, settings.sleep_max_ms)
            
            if not ids:
                logger.info(f"No listings found on page {page}. Stopping discovery.")
                break
        except BlockingDetectedError:
            logger.error("Blocking detected. Stopping discovery.")
            break
        except Exception as e:
            logger.warning(f"Failed to discover page {page}: {e}. Continuing...")
            continue
    
    # Insert into DB (using loop with individual inserts for compatibility)
    inserted = 0
    if all_ids:
        with wh_conn() as conn:
            with conn.cursor() as cur:
                base_url = settings.kolesa_base_url
                for listing_id in all_ids:
                    lurl = f"{base_url}/a/show/{listing_id}"
                    cur.execute("""
                        INSERT INTO ctl.scrape_queue(listing_id, url)
                        VALUES (%s, %s)
                        ON CONFLICT (listing_id) DO NOTHING
                    """, (listing_id, lurl))
                    # rowcount is 1 if inserted, 0 if conflict (DO NOTHING)
                    inserted += cur.rowcount
    
    logger.info(f"Inserted {inserted} new listing IDs into queue")
    return inserted
