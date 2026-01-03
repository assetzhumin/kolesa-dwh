-- Fix permissions for warehouse user on all tables
-- Run with: docker compose exec -T postgres psql -U postgres -d warehouse < db/init/30-fix-permissions.sql
-- SECURITY: Only grants necessary privileges (SELECT, INSERT, UPDATE, DELETE) - no ALTER, DROP, TRUNCATE

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ctl TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO warehouse;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO warehouse;

-- Grant schema usage
GRANT USAGE ON SCHEMA ctl TO warehouse;
GRANT USAGE ON SCHEMA bronze TO warehouse;
GRANT USAGE ON SCHEMA silver TO warehouse;
GRANT USAGE ON SCHEMA gold TO warehouse;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ctl TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gold TO warehouse;

-- Change ownership of views to warehouse user
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT 'ALTER VIEW ' || schemaname || '.' || viewname || ' OWNER TO warehouse;' AS sql
        FROM pg_views
        WHERE schemaname IN ('ctl', 'bronze', 'silver', 'gold')
        AND viewowner != 'warehouse'
    LOOP
        EXECUTE r.sql;
    END LOOP;
    END $$;

-- Set default privileges for future tables (only necessary privileges)
ALTER DEFAULT PRIVILEGES IN SCHEMA ctl GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA ctl GRANT USAGE, SELECT ON SEQUENCES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT USAGE, SELECT ON SEQUENCES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT USAGE, SELECT ON SEQUENCES TO warehouse;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT USAGE, SELECT ON SEQUENCES TO warehouse;

