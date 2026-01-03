"""Silver layer upsert operations for listing data."""
import hashlib
import json
import logging
from datetime import date, datetime
from typing import Dict, Any, Optional
from ..common.db import wh_conn

logger = logging.getLogger(__name__)


def payload_hash(obj: Dict[str, Any]) -> str:
    """
    Calculate SHA256 hash of JSON-serialized object.
    
    Args:
        obj: Dictionary to hash
        
    Returns:
        Hexadecimal hash string
    """
    b = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def upsert_silver(listing: Dict[str, Any], fetched_at: datetime) -> None:
    """
    Upsert listing data into silver layer tables.
    
    Args:
        listing: Dictionary containing listing data
        fetched_at: Timestamp when listing was fetched (must be timezone-aware)
        
    Raises:
        KeyError: If required listing_id key is missing
        ValueError: If fetched_at is not timezone-aware
    """
    if "listing_id" not in listing:
        raise KeyError("listing_id is required in listing dictionary")
    
    if fetched_at.tzinfo is None:
        raise ValueError("fetched_at must be timezone-aware")
    
    ph = payload_hash(listing)

    try:
        with wh_conn() as conn:
            with conn.cursor() as cur:
                # Fetch existing (for price event + first_seen)
                cur.execute(
                    "SELECT price_kzt, first_seen_at FROM silver.listing_current WHERE listing_id=%s",
                    (listing["listing_id"],)
                )
                row = cur.fetchone()
                old_price: Optional[float] = row[0] if row else None
                first_seen: datetime = row[1] if row else fetched_at

                # Price event: record if price changed
                new_price = listing.get("price_kzt")
                if (
                    old_price is not None 
                    and new_price is not None 
                    and float(old_price) != float(new_price)
                ):
                    cur.execute("""
                        INSERT INTO silver.listing_price_event(listing_id, event_ts, old_price_kzt, new_price_kzt)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (listing["listing_id"], fetched_at, old_price, new_price))

                # Upsert listing_current
                cur.execute("""
                    INSERT INTO silver.listing_current(
                        listing_id, url, title, price_kzt, city, region,
                        make, model, generation, trim, car_year, mileage_km,
                        body_type, engine_volume_l, engine_type, transmission, drivetrain, steering, color, customs_cleared,
                        seller_name, seller_type, seller_user_id, seller_months_on_site, seller_inventory_count, seller_address,
                        options_text, photos, first_seen_at, last_seen_at, is_active, payload_hash
                    )
                    VALUES (
                        %(listing_id)s, %(url)s, %(title)s, %(price_kzt)s, %(city)s, %(region)s,
                        %(make)s, %(model)s, %(generation)s, %(trim)s, %(car_year)s, %(mileage_km)s,
                        %(body_type)s, %(engine_volume_l)s, %(engine_type)s, %(transmission)s, %(drivetrain)s, %(steering)s, %(color)s, %(customs_cleared)s,
                        %(seller_name)s, %(seller_type)s, NULL, NULL, NULL, NULL,
                        %(options_text)s, %(photos_json)s, %(first_seen_at)s, %(last_seen_at)s, TRUE, %(payload_hash)s
                    )
                    ON CONFLICT (listing_id) DO UPDATE SET
                        url = EXCLUDED.url,
                        title = EXCLUDED.title,
                        price_kzt = EXCLUDED.price_kzt,
                        city = EXCLUDED.city,
                        region = EXCLUDED.region,
                        make = EXCLUDED.make,
                        model = EXCLUDED.model,
                        generation = EXCLUDED.generation,
                        trim = EXCLUDED.trim,
                        car_year = EXCLUDED.car_year,
                        mileage_km = EXCLUDED.mileage_km,
                        body_type = EXCLUDED.body_type,
                        engine_volume_l = EXCLUDED.engine_volume_l,
                        engine_type = EXCLUDED.engine_type,
                        transmission = EXCLUDED.transmission,
                        drivetrain = EXCLUDED.drivetrain,
                        steering = EXCLUDED.steering,
                        color = EXCLUDED.color,
                        customs_cleared = EXCLUDED.customs_cleared,
                        seller_name = EXCLUDED.seller_name,
                        seller_type = EXCLUDED.seller_type,
                        options_text = EXCLUDED.options_text,
                        photos = EXCLUDED.photos,
                        last_seen_at = EXCLUDED.last_seen_at,
                        is_active = TRUE,
                        payload_hash = EXCLUDED.payload_hash
                """, {
                **listing,
                "photos_json": json.dumps(listing.get("photos") or []),
                "first_seen_at": first_seen,
                "last_seen_at": fetched_at,
                    "payload_hash": ph
                })

                # Daily snapshot
                cur.execute("""
                    INSERT INTO silver.listing_snapshot_daily(listing_id, snapshot_date, price_kzt, is_active, views, photo_count)
                    VALUES (%s, %s, %s, TRUE, NULL, %s)
                    ON CONFLICT (listing_id, snapshot_date) DO UPDATE SET
                        price_kzt = EXCLUDED.price_kzt,
                        is_active = TRUE,
                        photo_count = EXCLUDED.photo_count
                """, (listing["listing_id"], date.today(), listing.get("price_kzt"), listing.get("photo_count")))
    except Exception as e:
        logger.error(f"Error upserting listing {listing.get('listing_id')}: {e}", exc_info=True)
        raise
