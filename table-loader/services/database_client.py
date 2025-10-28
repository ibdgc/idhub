# table-loader/services/database_client.py
import logging
from typing import Optional

import psycopg2
from core.config import settings
from psycopg2 import pool

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Manages database connection pool"""

    def __init__(self):
        """Initialize database client using settings"""
        self.db_pool: Optional[pool.SimpleConnectionPool] = None
        logger.info("Database client initialized")

    def _ensure_pool(self):
        """Create connection pool if it doesn't exist (lazy initialization)"""
        if self.db_pool is not None:
            return

        try:
            logger.info("Creating database connection pool...")
            self.db_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
            )
            logger.info("✓ Database connection pool created")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise

    def get_connection(self):
        """Get a connection from the pool"""
        self._ensure_pool()  # Only connects when actually needed
        return self.db_pool.getconn()

    def return_connection(self, conn):
        """Return a connection to the pool"""
        if self.db_pool:
            self.db_pool.putconn(conn)

    def close(self):
        """Close all connections in the pool"""
        if self.db_pool:
            logger.info("Closing database connection pool...")
            self.db_pool.closeall()
            logger.info("✓ Database connections closed")
