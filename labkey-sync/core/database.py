import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manage database connections"""

    @staticmethod
    def get_connection_string():
        return f"host={settings.DB_HOST} port={settings.DB_PORT} dbname={settings.DB_NAME} user={settings.DB_USER} password={settings.DB_PASSWORD}"

    @staticmethod
    @contextmanager
    def get_connection():
        """Context manager for database connections"""
        conn = psycopg2.connect(
            DatabaseManager.get_connection_string(), cursor_factory=RealDictCursor
        )
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()


db_manager = DatabaseManager()
