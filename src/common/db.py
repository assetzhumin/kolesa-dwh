"""Database connection management for the warehouse."""
import psycopg2
import psycopg2.errors
from contextlib import contextmanager
from typing import Generator
import logging

from .settings import settings

logger = logging.getLogger(__name__)


@contextmanager
def wh_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager for warehouse database connections.
    
    Yields:
        psycopg2 connection object
        
    Raises:
        psycopg2.Error: Database connection or operation errors
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=settings.pg_host,
            port=settings.pg_port,
            dbname=settings.warehouse_db,
            user=settings.warehouse_user,
            password=settings.warehouse_password,
        )
        yield conn
        conn.commit()
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error in database operation: {e}")
        raise
    finally:
        if conn and not conn.closed:
            conn.close()
