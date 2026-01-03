-- Create databases and users (idempotent for init container)
-- SECURITY NOTE: Default passwords are insecure. Change them after initial setup using:
-- ALTER ROLE airflow WITH PASSWORD 'your_secure_password';
-- ALTER ROLE warehouse WITH PASSWORD 'your_secure_password';
-- Or use the update-passwords.sh script with environment variables.

DO $$
DECLARE
  airflow_pwd TEXT := COALESCE(current_setting('app.airflow_password', true), 'airflow');
  warehouse_pwd TEXT := COALESCE(current_setting('app.warehouse_password', true), 'warehouse');
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
    EXECUTE format('CREATE ROLE airflow LOGIN PASSWORD %L', airflow_pwd);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'warehouse') THEN
    EXECUTE format('CREATE ROLE warehouse LOGIN PASSWORD %L', warehouse_pwd);
  END IF;
END$$;

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE warehouse OWNER warehouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'warehouse')\gexec