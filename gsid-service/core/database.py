# gsid-service/core/database.py
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import settings

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=settings.DB_HOST,
            database=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            port=settings.DB_PORT,
        )
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()


@contextmanager
def get_db_cursor(conn, cursor_factory=RealDictCursor):
    """Context manager for database cursors"""
    cursor = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database operation error: {e}")
        raise
    finally:
        cursor.close()
