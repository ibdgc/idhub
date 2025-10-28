# table-loader/services/database_client.py
import logging
from typing import Any, Dict, Optional

import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Client for PostgreSQL database operations with lazy initialization"""

    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.db_pool: Optional[pool.SimpleConnectionPool] = None
        logger.info("DatabaseClient initialized (connection pool not yet created)")

    def _ensure_pool(self):
        """Lazy initialization of connection pool - only connects when first needed"""
        if self.db_pool is not None:
            return

        logger.info("Creating database connection pool...")
        try:
            self.db_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1, maxconn=10, **self.db_config
            )
            logger.info("✓ Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise

    def get_connection(self):
        """Get a connection from the pool (initializes pool on first call)"""
        self._ensure_pool()  # Only connects when actually needed
        conn = self.db_pool.getconn()
        logger.debug("Retrieved connection from pool")
        return conn

    def return_connection(self, conn):
        """Return a connection to the pool"""
        if self.db_pool and conn:
            self.db_pool.putconn(conn)
            logger.debug("Returned connection to pool")

    def close(self):
        """Close all connections in the pool"""
        if self.db_pool:
            self.db_pool.closeall()
            self.db_pool = None
            logger.info("✓ Closed all database connections")
