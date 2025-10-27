import logging
from typing import Any, Dict

import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Client for PostgreSQL database operations"""

    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.db_pool = None
        self._ensure_pool()

    def _ensure_pool(self):
        """Lazy initialization of connection pool"""
        if self.db_pool is None:
            logger.info("Initializing database connection pool...")
            self.db_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1, maxconn=10, **self.db_config
            )
            logger.info("✓ Database connection pool initialized")

    def get_connection(self):
        """Get a connection from the pool"""
        self._ensure_pool()
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
            logger.info("✓ Closed all database connections")
