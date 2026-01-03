"""
Gold layer DDL definitions for cloud export.
Contains CREATE TABLE statements with all constraints (PKs, FKs, unique).
"""
GOLD_DDL = {
    "dim_date": """
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT PRIMARY KEY,
    d DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL
);
""",
    
    "dim_location": """
CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_key BIGSERIAL PRIMARY KEY,
    region TEXT,
    city TEXT NOT NULL,
    UNIQUE NULLS NOT DISTINCT (region, city)
);
""",
    
    "dim_vehicle": """
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
""",
    
    "dim_seller": """
CREATE TABLE IF NOT EXISTS gold.dim_seller (
    seller_key BIGSERIAL PRIMARY KEY,
    seller_type TEXT,
    seller_name TEXT,
    seller_user_id BIGINT,
    city TEXT,
    UNIQUE NULLS NOT DISTINCT (seller_type, seller_name, seller_user_id, city)
);
""",
    
    "fact_listing_daily": """
CREATE TABLE IF NOT EXISTS gold.fact_listing_daily (
    date_key INT NOT NULL,
    listing_id BIGINT NOT NULL,
    location_key BIGINT NOT NULL,
    vehicle_key BIGINT NOT NULL,
    seller_key BIGINT NOT NULL,
    price_kzt NUMERIC(14,2),
    is_active BOOLEAN NOT NULL,
    days_on_site INT,
    views INT,
    photo_count INT,
    PRIMARY KEY (date_key, listing_id),
    CONSTRAINT fk_fact_listing_daily_date_key FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
    CONSTRAINT fk_fact_listing_daily_location_key FOREIGN KEY (location_key) REFERENCES gold.dim_location(location_key),
    CONSTRAINT fk_fact_listing_daily_vehicle_key FOREIGN KEY (vehicle_key) REFERENCES gold.dim_vehicle(vehicle_key),
    CONSTRAINT fk_fact_listing_daily_seller_key FOREIGN KEY (seller_key) REFERENCES gold.dim_seller(seller_key)
);
""",
    
    "fact_price_event": """
CREATE TABLE IF NOT EXISTS gold.fact_price_event (
    event_ts TIMESTAMPTZ NOT NULL,
    date_key INT NOT NULL,
    listing_id BIGINT NOT NULL,
    old_price_kzt NUMERIC(14,2),
    new_price_kzt NUMERIC(14,2),
    PRIMARY KEY (listing_id, event_ts),
    CONSTRAINT fk_fact_price_event_date_key FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)
);
""",
}

# Table loading order: dimensions first, then facts
GOLD_TABLE_ORDER = [
    "dim_date",          # Load first - no dependencies
    "dim_location",      # Load second - no dependencies
    "dim_vehicle",       # Load third - no dependencies
    "dim_seller",        # Load fourth - no dependencies
    "fact_listing_daily", # Load fifth - depends on all dimensions
    "fact_price_event",   # Load last - depends on dim_date
]

def get_gold_ddl(table_name: str) -> str:
    """Get DDL for a gold table."""
    return GOLD_DDL.get(table_name, "")

def get_gold_table_order() -> list:
    """Get the correct loading order for gold tables (dimensions first)."""
    return GOLD_TABLE_ORDER.copy()

