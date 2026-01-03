"""
Cloud data warehouse sink implementation for Supabase (PostgreSQL).

Supabase is a free PostgreSQL hosting service:
- Free tier: 500MB database, 2GB bandwidth/month
- Easy setup: Just need connection string
- PostgreSQL compatible: Works with existing Postgres code
"""
import os
import logging
import re
from abc import ABC, abstractmethod
from typing import Optional
from datetime import date
import boto3
from psycopg2 import sql
from ..common.settings import settings
from ..common.utils import redact_secrets

logger = logging.getLogger(__name__)


def _validate_identifier(name: str, identifier_type: str = "identifier") -> None:
    """
    Validate SQL identifier is safe (alphanumeric + underscore only).
    
    Args:
        name: Identifier to validate
        identifier_type: Type of identifier (for error messages)
        
    Raises:
        ValueError: If identifier contains invalid characters
    """
    if not name:
        raise ValueError(f"Empty {identifier_type} is not allowed")
    
    # Allow alphanumeric characters and underscores only
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}'. "
            "Must start with letter or underscore and contain only alphanumeric characters and underscores."
        )


class CloudSink(ABC):
    """Abstract base class for cloud data warehouse sinks."""
    
    @abstractmethod
    def load_table(self, table_name: str, parquet_path: str, schema: str = "gold") -> int:
        """
        Load a Parquet file into the cloud warehouse.
        
        Args:
            table_name: Name of the target table
            parquet_path: S3/MinIO path to Parquet file
            schema: Schema name (default: gold)
            
        Returns:
            Number of rows loaded
        """
        pass
    
    @abstractmethod
    def create_table_if_not_exists(self, table_name: str, schema: str = "gold", ddl: Optional[str] = None) -> None:
        """Create table if it doesn't exist."""
        pass


class SupabaseSink(CloudSink):
    """Supabase sink using PostgreSQL connection (psycopg2).
    
    Supabase is a free PostgreSQL hosting service with:
    - Free tier: 500MB database, 2GB bandwidth/month
    - Easy setup: Just need connection string
    - PostgreSQL compatible: Works with existing Postgres code
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize Supabase sink.
        
        Args:
            connection_string: PostgreSQL connection string (from SUPABASE_DB_URL env var if not provided)
                              Format: postgresql://user:password@host:port/database
        """
        self.connection_string = connection_string or os.getenv("SUPABASE_DB_URL")
        if not self.connection_string:
            raise ValueError("SUPABASE_DB_URL environment variable is required for Supabase")
        
        try:
            import psycopg2
            # Test connection
            self.conn = psycopg2.connect(self.connection_string)
            self.conn.close()
            self.conn = None  # Will reconnect on demand
            logger.info(redact_secrets(f"Supabase connection string validated: {self.connection_string}"))
        except ImportError:
            raise ImportError("psycopg2-binary package is required for Supabase sink. Install with: pip install psycopg2-binary")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Supabase: {e}")
    
    def _ensure_connection(self):
        """Ensure Supabase connection is established."""
        if self.conn is None or self.conn.closed:
            import psycopg2
            self.conn = psycopg2.connect(self.connection_string)
    
    def create_table_if_not_exists(self, table_name: str, schema: str = "gold", ddl: Optional[str] = None) -> None:
        """Create table in Supabase if it doesn't exist."""
        # Validate identifiers to prevent SQL injection
        _validate_identifier(schema, "schema")
        _validate_identifier(table_name, "table_name")
        
        self._ensure_connection()
        
        # Create schema if it doesn't exist (using safe identifier quoting)
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(schema)
                )
            )
            self.conn.commit()
        
        if ddl:
            # Execute DDL
            with self.conn.cursor() as cur:
                cur.execute(ddl)
                self.conn.commit()
            logger.info(f"Table {schema}.{table_name} created/verified in Supabase")
        else:
            logger.info(f"Table {schema}.{table_name} will be created on first load")
    
    def load_table(self, table_name: str, parquet_path: str, schema: str = "gold", ddl: Optional[str] = None) -> int:
        """
        Load Parquet file from S3/MinIO into Supabase with proper DDL constraints.
        
        Args:
            table_name: Target table name
            parquet_path: S3/MinIO path (s3://bucket/key or http://minio:9000/bucket/key)
            schema: Schema name
            ddl: Optional DDL CREATE TABLE statement with PKs/FKs/constraints.
                 If provided, table will be created with full constraints.
                 If None, table will be created from pandas schema (no constraints).
            
        Returns:
            Number of rows loaded
        """
        # Validate identifiers to prevent SQL injection
        _validate_identifier(schema, "schema")
        _validate_identifier(table_name, "table_name")
        
        self._ensure_connection()
        
        # Download from MinIO/S3 to local temp file
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            # Parse S3/MinIO path
            if parquet_path.startswith("s3://"):
                bucket, key = parquet_path[5:].split("/", 1)
                endpoint = settings.minio_endpoint
            elif parquet_path.startswith("http://") or parquet_path.startswith("https://"):
                parts = parquet_path.replace("http://", "").replace("https://", "").split("/", 1)
                endpoint = f"http://{parts[0]}"
                bucket, key = parts[1].split("/", 1)
            else:
                raise ValueError(f"Invalid parquet_path format: {parquet_path}")
            
            # Download using boto3
            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=settings.minio_access_key,
                aws_secret_access_key=settings.minio_secret_key,
            )
            
            logger.info(f"Downloading {parquet_path} to {tmp_path}")
            s3_client.download_file(bucket, key, tmp_path)
            
            # Load into Supabase using COPY FROM (PostgreSQL native, fastest)
            import pandas as pd
            import io
            import csv
            
            df = pd.read_parquet(tmp_path)
            logger.info(f"Loading {len(df)} rows into {schema}.{table_name}")
            
            # Fix integer columns that may have been exported as floats
            # Convert float columns that should be integers back to integers
            integer_columns = ['views', 'photo_count', 'days_on_site', 'date_key', 'listing_id', 
                             'location_key', 'vehicle_key', 'seller_key', 'car_year', 
                             'year', 'quarter', 'month', 'day_of_week', 'seller_user_id']
            for col in integer_columns:
                if col in df.columns and pd.api.types.is_float_dtype(df[col]):
                    # Convert float to nullable integer (Int64) - handles NaN properly
                    df[col] = df[col].astype('Int64')
            
            with self.conn.cursor() as cur:
                # Create schema if needed (using safe identifier quoting)
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(schema)
                    )
                )
                
                # Drop table for idempotency (CASCADE to drop dependent objects like FKs)
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name)
                    )
                )
                
                if ddl:
                    # Use provided DDL with full constraints (PKs, FKs, unique)
                    logger.info(f"Creating table {schema}.{table_name} with DDL (includes PKs/FKs/constraints)")
                    # Replace schema name in DDL if needed (validate schema name is safe first)
                    # Use regex to replace "gold." with "{schema}." only at word boundaries
                    if schema != "gold":
                        # Validate that schema name in DDL matches expected pattern
                        if not re.search(r'\bgold\.', ddl, re.IGNORECASE):
                            logger.warning(f"DDL does not contain 'gold.' schema references, using as-is")
                        # Replace schema references safely
                        ddl_exec = re.sub(
                            r'\bgold\.',
                            f'{schema}.',
                            ddl,
                            flags=re.IGNORECASE
                        )
                    else:
                        ddl_exec = ddl
                    cur.execute(ddl_exec)
                else:
                    # Fallback: Create table from pandas schema (no constraints)
                    logger.warning(f"Creating table {schema}.{table_name} without DDL - no constraints will be applied")
                    columns = []
                    for col, dtype in df.dtypes.items():
                        # Validate column name
                        _validate_identifier(col, "column_name")
                        if pd.api.types.is_integer_dtype(dtype):
                            pg_type = "BIGINT"
                        elif pd.api.types.is_float_dtype(dtype):
                            pg_type = "NUMERIC"
                        elif pd.api.types.is_bool_dtype(dtype):
                            pg_type = "BOOLEAN"
                        elif pd.api.types.is_datetime64_any_dtype(dtype):
                            pg_type = "TIMESTAMPTZ"
                        else:
                            pg_type = "TEXT"
                        columns.append(sql.SQL("{} {}").format(
                            sql.Identifier(col),
                            sql.SQL(pg_type)
                        ))
                    
                    create_sql = sql.SQL("CREATE TABLE {}.{} ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.SQL(", ").join(columns)
                    )
                    cur.execute(create_sql)
                
                # Use COPY FROM for fast bulk insert
                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
                buffer.seek(0)
                
                # Use safe identifier quoting for COPY command
                # copy_expert requires a string, so we format the SQL object to string
                copy_sql = sql.SQL("COPY {}.{} FROM STDIN WITH (FORMAT csv, DELIMITER ',', QUOTE '\"')").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name)
                )
                # Convert SQL object to string for copy_expert
                cur.copy_expert(copy_sql.as_string(self.conn), buffer)
                
                self.conn.commit()
                
                # Get row count (using safe identifier quoting)
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name)
                    )
                )
                row_count = cur.fetchone()[0]
            
            logger.info(f"Loaded {row_count} rows into {schema}.{table_name}")
            return row_count
            
        finally:
            # Cleanup temp file
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


def get_cloud_sink() -> Optional[CloudSink]:
    """
    Factory function to get cloud sink.
    
    Returns:
        SupabaseSink instance or None if CLOUD_SINK=none
    """
    sink_type = os.getenv("CLOUD_SINK", "supabase").lower()
    
    if sink_type == "none":
        return None
    elif sink_type == "supabase":
        return SupabaseSink()
    else:
        raise ValueError(f"Unknown CLOUD_SINK value: {sink_type}. Must be 'supabase' or 'none'")


def export_gold_to_parquet(table_name: str, date_key: Optional[int] = None, output_bucket: str = "gold-exports") -> str:
    """
    Export Gold table to Parquet and upload to MinIO.
    
    Args:
        table_name: Gold table name (e.g., fact_listing_daily)
        date_key: Optional date_key filter (YYYYMMDD format)
        output_bucket: MinIO bucket for exports
        
    Returns:
        S3/MinIO path to Parquet file
    """
    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("pandas and pyarrow are required for Parquet export. Install with: pip install pandas pyarrow")
    
    from ..common.db import wh_conn
    import tempfile
    
    # Query data
    query = f"SELECT * FROM gold.{table_name}"
    if date_key:
        query += f" WHERE date_key = {date_key}"
    
    with wh_conn() as conn:
        df = pd.read_sql(query, conn)
    
    # Fix integer columns that pandas converted to float due to NULLs
    # PostgreSQL INT columns should be exported as integers, not floats
    integer_columns = ['views', 'photo_count', 'days_on_site', 'date_key', 'listing_id', 
                       'location_key', 'vehicle_key', 'seller_key', 'car_year', 
                       'year', 'quarter', 'month', 'day_of_week', 'seller_user_id']
    
    for col in integer_columns:
        if col in df.columns:
            # Convert float to nullable integer (Int64) - fills NaN with None, keeps integers as integers
            df[col] = df[col].astype('Int64')
    
    # Write to Parquet
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, tmp_path, compression="snappy")
        
        # Upload to MinIO
        date_prefix = date.today().strftime("%Y/%m/%d")
        object_key = f"{date_prefix}/{table_name}_{date.today().strftime('%Y%m%d')}.parquet"
        
        s3_client = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
        )
        
        # Ensure bucket exists
        try:
            s3_client.head_bucket(Bucket=output_bucket)
        except:
            s3_client.create_bucket(Bucket=output_bucket)
        
        logger.info(f"Uploading {tmp_path} to s3://{output_bucket}/{object_key}")
        s3_client.upload_file(tmp_path, output_bucket, object_key)
        
        parquet_path = f"s3://{output_bucket}/{object_key}"
        logger.info(f"Exported {len(df)} rows to {parquet_path}")
        return parquet_path
        
    finally:
        import os
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
