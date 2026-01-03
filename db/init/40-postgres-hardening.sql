-- Postgres Security Hardening Configuration
-- This file enables additional security features for PostgreSQL
-- Run with: docker compose exec -T postgres psql -U postgres -d postgres < db/init/40-postgres-hardening.sql
-- Note: Some settings require PostgreSQL restart to take effect

-- Enable connection and disconnection logging
ALTER SYSTEM SET log_connections = 'on';
ALTER SYSTEM SET log_disconnections = 'on';

-- Log DDL statements (schema changes)
ALTER SYSTEM SET log_statement = 'ddl';

-- Log slow queries (queries taking more than 1 second)
ALTER SYSTEM SET log_min_duration_statement = 1000;

-- Ensure password encryption uses SCRAM-SHA-256 (Postgres 16 default, but explicit)
ALTER SYSTEM SET password_encryption = 'scram-sha-256';

-- Reload configuration (does not require full restart)
SELECT pg_reload_conf();

-- Revoke dangerous functions from PUBLIC
-- These functions can read arbitrary files from the filesystem
REVOKE EXECUTE ON FUNCTION pg_read_file(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_read_file(text, bigint, bigint) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_read_file(text, bigint, bigint, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_read_binary_file(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_read_binary_file(text, bigint, bigint) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_read_binary_file(text, bigint, bigint, boolean) FROM PUBLIC;

-- Log the changes
DO $$
BEGIN
    RAISE NOTICE 'PostgreSQL hardening configuration applied. Some settings may require container restart.';
END $$;

