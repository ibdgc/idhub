# gsid-service/core/database.py
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import settings

logger = logging.getLogger(__name__)


def get_db_connection():
    """Get a new database connection"""
    try:
        conn = psycopg2.connect(
            host=settings.DB_HOST,
            database=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            port=settings.DB_PORT,
        )
        logger.debug("Database connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


@contextmanager
def get_db_cursor(conn, cursor_factory=RealDictCursor):
    """
    Context manager for database cursor with automatic commit/rollback

    Usage:
        conn = get_db_connection()
        try:
            with get_db_cursor(conn) as cursor:
                cursor.execute("SELECT * FROM table")
                results = cursor.fetchall()
            conn.commit()
        finally:
            conn.close()
    """
    cursor = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cursor
    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

