# table-loader/core/database.py
import logging
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_values

from .config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    _instance: Optional["DatabaseManager"] = None

    def __init__(self):
        self.pool: Optional[pool.ThreadedConnectionPool] = None
        # DON'T initialize pool here - do it lazily

    @classmethod
    def get_instance(cls) -> "DatabaseManager":
        """Singleton pattern"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _ensure_pool(self):
        """Lazy initialization of connection pool"""
        if self.pool is not None:
            return

        try:
            logger.info("Initializing database connection pool...")
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                host=settings.DB_HOST,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                port=settings.DB_PORT,
            )
            logger.info("✓ Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Get connection from pool"""
        self._ensure_pool()  # Initialize only when actually needed
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    @contextmanager
    def get_cursor(self, conn, cursor_factory=RealDictCursor):
        """Get cursor with automatic commit/rollback"""
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

    def bulk_insert(self, conn, table: str, columns: list, values: list):
        """Perform bulk insert using execute_values"""
        with self.get_cursor(conn, cursor_factory=None) as cursor:
            query = f"""
                INSERT INTO {table} ({", ".join(columns)})
                VALUES %s
            """
            execute_values(cursor, query, values)
            logger.info(f"✓ Bulk inserted {len(values)} rows into {table}")

    def close(self):
        """Close all connections in pool"""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")
            self.pool = None


# Singleton instance - but doesn't connect until first use
db_manager = DatabaseManager.get_instance()
