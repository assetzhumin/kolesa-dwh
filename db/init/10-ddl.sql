\c warehouse

CREATE SCHEMA IF NOT EXISTS ctl AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION warehouse;

-- -------------------------
-- CONTROL / RESUME
-- -------------------------
CREATE TABLE IF NOT EXISTS ctl.scrape_queue (
  listing_id       BIGINT PRIMARY KEY,
  url              TEXT NOT NULL,
  discovered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  state            TEXT NOT NULL DEFAULT 'NEW', -- NEW|FETCHED|RETRY|FAILED|INACTIVE
  attempts         INT  NOT NULL DEFAULT 0,
  last_attempt_at  TIMESTAMPTZ,
  next_retry_at    TIMESTAMPTZ,
  last_http_status INT,
  last_error       TEXT
);

-- Grant only necessary privileges (no ALTER, DROP, TRUNCATE)
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ctl.scrape_queue TO warehouse;
GRANT USAGE ON SCHEMA ctl TO warehouse;

-- -------------------------
-- BRONZE (raw objects in MinIO + metadata in Postgres)
-- -------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_listing_html (
  listing_id   BIGINT NOT NULL,
  fetched_at   TIMESTAMPTZ NOT NULL,
  bucket       TEXT NOT NULL,
  object_key   TEXT NOT NULL,
  sha256       TEXT NOT NULL,
  http_status  INT  NOT NULL,
  PRIMARY KEY (listing_id, fetched_at)
);

-- Grant only necessary privileges (no ALTER, DROP, TRUNCATE)
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE bronze.raw_listing_html TO warehouse;
GRANT USAGE ON SCHEMA bronze TO warehouse;

-- -------------------------
-- SILVER (normalized “current + daily snapshot + events”)
-- -------------------------
CREATE TABLE IF NOT EXISTS silver.listing_current (
  listing_id BIGINT PRIMARY KEY,
  url TEXT NOT NULL,

  title TEXT,
  price_kzt NUMERIC(14,2),
  currency TEXT DEFAULT 'KZT',

  city TEXT,
  region TEXT,

  make TEXT,
  model TEXT,
  generation TEXT,
  trim TEXT,

  car_year INT,
  mileage_km INT,

  body_type TEXT,
  engine_volume_l NUMERIC(3,1),
  engine_type TEXT,
  transmission TEXT,
  drivetrain TEXT,
  steering TEXT,
  color TEXT,
  customs_cleared BOOLEAN,

  seller_name TEXT,
  seller_type TEXT,              -- dealer/private
  seller_user_id BIGINT,         -- if visible in page source
  seller_months_on_site INT,
  seller_inventory_count INT,
  seller_address TEXT,

  options_text TEXT,
  photos JSONB,

  first_seen_at TIMESTAMPTZ NOT NULL,
  last_seen_at  TIMESTAMPTZ NOT NULL,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,

  payload_hash TEXT
);

CREATE TABLE IF NOT EXISTS silver.listing_snapshot_daily (
  listing_id BIGINT NOT NULL,
  snapshot_date DATE NOT NULL,
  price_kzt NUMERIC(14,2),
  is_active BOOLEAN NOT NULL,
  views INT,
  photo_count INT,
  PRIMARY KEY (listing_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS silver.listing_price_event (
  listing_id BIGINT NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL,
  old_price_kzt NUMERIC(14,2),
  new_price_kzt NUMERIC(14,2),
  PRIMARY KEY (listing_id, event_ts)
);

-- Grant only necessary privileges (no ALTER, DROP, TRUNCATE)
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE silver.listing_current TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE silver.listing_snapshot_daily TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE silver.listing_price_event TO warehouse;
GRANT USAGE ON SCHEMA silver TO warehouse;

-- -------------------------
-- GOLD STAR SCHEMA
-- -------------------------
CREATE TABLE IF NOT EXISTS gold.dim_date (
  date_key INT PRIMARY KEY,      -- YYYYMMDD
  d DATE NOT NULL UNIQUE,
  year SMALLINT NOT NULL,
  quarter SMALLINT NOT NULL,
  month SMALLINT NOT NULL,
  day_of_week SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_location (
  location_key BIGSERIAL PRIMARY KEY,
  region TEXT,
  city   TEXT NOT NULL,
  UNIQUE NULLS NOT DISTINCT (region, city)
);

CREATE TABLE IF NOT EXISTS gold.dim_vehicle (
  vehicle_key BIGSERIAL PRIMARY KEY,
  make TEXT NOT NULL,
  model TEXT NOT NULL,
  generation TEXT,
  trim TEXT,
  car_year INT,
  body_type TEXT,
  engine_type TEXT,
  engine_volume_l NUMERIC(3,1),
  transmission TEXT,
  drivetrain TEXT,
  steering TEXT,
  color TEXT,
  UNIQUE NULLS NOT DISTINCT (make, model, generation, trim, car_year, body_type, engine_type, engine_volume_l, transmission, drivetrain, steering, color)
);

CREATE TABLE IF NOT EXISTS gold.dim_seller (
  seller_key BIGSERIAL PRIMARY KEY,
  seller_type TEXT,
  seller_name TEXT,
  seller_user_id BIGINT,
  city TEXT,
  UNIQUE NULLS NOT DISTINCT (seller_type, seller_name, seller_user_id, city)
);

CREATE TABLE IF NOT EXISTS gold.fact_listing_daily (
  date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
  listing_id BIGINT NOT NULL,
  location_key BIGINT NOT NULL REFERENCES gold.dim_location(location_key),
  vehicle_key BIGINT NOT NULL REFERENCES gold.dim_vehicle(vehicle_key),
  seller_key BIGINT NOT NULL REFERENCES gold.dim_seller(seller_key),

  price_kzt NUMERIC(14,2),
  is_active BOOLEAN NOT NULL,
  days_on_site INT,
  views INT,
  photo_count INT,

  PRIMARY KEY (date_key, listing_id)
);

CREATE TABLE IF NOT EXISTS gold.fact_price_event (
  event_ts TIMESTAMPTZ NOT NULL,
  date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
  listing_id BIGINT NOT NULL,
  old_price_kzt NUMERIC(14,2),
  new_price_kzt NUMERIC(14,2),
  PRIMARY KEY (listing_id, event_ts)
);

-- Grant only necessary privileges (no ALTER, DROP, TRUNCATE)
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE gold.dim_date TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE gold.dim_location TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE gold.dim_vehicle TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE gold.dim_seller TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE gold.fact_listing_daily TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE gold.fact_price_event TO warehouse;
GRANT USAGE ON SCHEMA gold TO warehouse;

-- Grant usage on sequences for SERIAL/BIGSERIAL columns
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gold TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ctl TO warehouse;

-- Grant permissions on all existing tables (idempotent)
-- Only grant necessary privileges (no ALTER, DROP, TRUNCATE)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ctl TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO warehouse;

-- Set default privileges for future tables
-- Only grant necessary privileges (no ALTER, DROP, TRUNCATE)
ALTER DEFAULT PRIVILEGES IN SCHEMA ctl GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA ctl GRANT USAGE, SELECT ON SEQUENCES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT USAGE, SELECT ON SEQUENCES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT USAGE, SELECT ON SEQUENCES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT USAGE, SELECT ON SEQUENCES TO warehouse;