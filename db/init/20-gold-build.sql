\c warehouse

-- Populate dim_date for today +/- 365 days
INSERT INTO gold.dim_date(date_key, d, year, quarter, month, day_of_week)
SELECT
  (EXTRACT(YEAR FROM d)::int * 10000 + EXTRACT(MONTH FROM d)::int * 100 + EXTRACT(DAY FROM d)::int) AS date_key,
  d::date,
  EXTRACT(YEAR FROM d)::smallint,
  EXTRACT(QUARTER FROM d)::smallint,
  EXTRACT(MONTH FROM d)::smallint,
  EXTRACT(ISODOW FROM d)::smallint
FROM generate_series((current_date - 365)::date, (current_date + 365)::date, interval '1 day') g(d)
ON CONFLICT (d) DO NOTHING;

-- dim_location
INSERT INTO gold.dim_location(region, city)
SELECT DISTINCT region, city
FROM silver.listing_current
WHERE city IS NOT NULL
ON CONFLICT (region, city) DO NOTHING;

-- dim_vehicle
INSERT INTO gold.dim_vehicle(make, model, generation, trim, car_year, body_type, engine_type, engine_volume_l, transmission, drivetrain, steering, color)
SELECT DISTINCT make, model, generation, trim, car_year, body_type, engine_type, engine_volume_l, transmission, drivetrain, steering, color
FROM silver.listing_current
WHERE make IS NOT NULL AND model IS NOT NULL
ON CONFLICT DO NOTHING;

-- dim_seller
INSERT INTO gold.dim_seller(seller_type, seller_name, seller_user_id, city)
SELECT DISTINCT seller_type, seller_name, seller_user_id, city
FROM silver.listing_current
ON CONFLICT (seller_type, seller_name, seller_user_id, city) DO NOTHING;

-- fact_listing_daily
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
ORDER BY dd.date_key, s.listing_id, snap.snapshot_date DESC
ON CONFLICT (date_key, listing_id) DO UPDATE SET
  price_kzt = EXCLUDED.price_kzt,
  is_active = EXCLUDED.is_active,
  days_on_site = EXCLUDED.days_on_site,
  views = EXCLUDED.views,
  photo_count = EXCLUDED.photo_count;

-- fact_price_event
INSERT INTO gold.fact_price_event(event_ts, date_key, listing_id, old_price_kzt, new_price_kzt)
SELECT
  e.event_ts,
  (EXTRACT(YEAR FROM e.event_ts)::int * 10000 + EXTRACT(MONTH FROM e.event_ts)::int * 100 + EXTRACT(DAY FROM e.event_ts)::int) AS date_key,
  e.listing_id,
  e.old_price_kzt,
  e.new_price_kzt
FROM silver.listing_price_event e
ON CONFLICT (listing_id, event_ts) DO NOTHING;

-- ML feature view
CREATE OR REPLACE VIEW gold.ml_features_listing_day AS
SELECT
  f.date_key,
  f.listing_id,
  f.vehicle_key,
  f.location_key,
  f.seller_key,
  f.price_kzt,
  f.days_on_site,
  f.views,
  f.photo_count,
  NULL::int AS y_sold_30d_proxy
FROM gold.fact_listing_daily f;
