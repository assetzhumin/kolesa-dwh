"""Application settings and configuration."""
from pydantic import BaseModel, Field, field_validator
import os
import logging

logger = logging.getLogger(__name__)


def _get_int_env(key: str, default: int) -> int:
    """Safely get integer from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer value for {key}: {value}. Using default: {default}")
        return default


class Settings(BaseModel):
    """Application settings loaded from environment variables."""
    
    kolesa_base_url: str = Field(
        default_factory=lambda: os.getenv("KOLESA_BASE_URL", "https://kolesa.kz"),
        description="Base URL for kolesa.kz website"
    )
    max_pages: int = Field(
        default_factory=lambda: _get_int_env("KOLESA_MAX_PAGES", 50),
        description="Maximum number of pages to discover per run",
        gt=0
    )
    fetch_batch: int = Field(
        default_factory=lambda: _get_int_env("KOLESA_FETCH_BATCH", 200),
        description="Number of listings to fetch per batch",
        gt=0
    )
    concurrency: int = Field(
        default_factory=lambda: _get_int_env("KOLESA_CONCURRENCY", 3),
        description="Number of concurrent fetch operations",
        gt=0
    )
    sleep_min_ms: int = Field(
        default_factory=lambda: _get_int_env("KOLESA_SLEEP_MIN_MS", 800),
        description="Minimum sleep time between requests (milliseconds)",
        ge=0
    )
    sleep_max_ms: int = Field(
        default_factory=lambda: _get_int_env("KOLESA_SLEEP_MAX_MS", 2200),
        description="Maximum sleep time between requests (milliseconds)",
        ge=0
    )

    pg_host: str = Field(
        default_factory=lambda: os.getenv("POSTGRES_HOST", "postgres"),
        description="PostgreSQL host"
    )
    pg_port: int = Field(
        default_factory=lambda: _get_int_env("POSTGRES_PORT", 5432),
        description="PostgreSQL port",
        gt=0,
        lt=65536
    )
    warehouse_db: str = Field(
        default_factory=lambda: os.getenv("WAREHOUSE_DB", "warehouse"),
        description="Warehouse database name"
    )
    warehouse_user: str = Field(
        default_factory=lambda: os.getenv("WAREHOUSE_USER", "warehouse"),
        description="Warehouse database user"
    )
    warehouse_password: str = Field(
        default_factory=lambda: os.getenv("WAREHOUSE_PASSWORD", "warehouse"),
        description="Warehouse database password"
    )

    minio_endpoint: str = Field(
        default_factory=lambda: os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        description="MinIO/S3 endpoint URL"
    )
    minio_access_key: str = Field(
        default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        description="MinIO/S3 access key"
    )
    minio_secret_key: str = Field(
        default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        description="MinIO/S3 secret key"
    )
    minio_bucket: str = Field(
        default_factory=lambda: os.getenv("MINIO_BUCKET", "bronze"),
        description="MinIO/S3 bucket name for bronze layer"
    )

    @field_validator("sleep_max_ms")
    @classmethod
    def validate_sleep_range(cls, v: int, info) -> int:
        """Ensure sleep_max_ms >= sleep_min_ms."""
        if hasattr(info, "data") and "sleep_min_ms" in info.data:
            if v < info.data["sleep_min_ms"]:
                raise ValueError("sleep_max_ms must be >= sleep_min_ms")
        return v


# Global settings instance
settings = Settings()
