"""
Connection-pool helper for REDCap-pipeline.

Provides:
    • get_db_pool()           – lazy-initialises and returns the psycopg2 pool
    • get_db_connection()     – fetches a connection from the pool
    • return_db_connection()  – puts a connection back
    • close_db_pool()         – closes all connections
    • db_connection()         – context-manager that gives a conn and always
                                returns it to the pool
"""

import logging
import os
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)

db_pool: Optional[pool.SimpleConnectionPool] = None  # singleton


# ──────────────────────────────────────────────────────────────────────────
# Pool helpers
# ──────────────────────────────────────────────────────────────────────────
def get_db_pool() -> pool.SimpleConnectionPool:
    """Create (lazily) and return the global connection-pool."""
    global db_pool
    if db_pool is None:
        logger.info("Initializing database connection pool...")
        try:
            db_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=10,
                maxconn=50,
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
            )
            logger.info("✓ Database connection pool initialized (max connections: 20)")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    return db_pool


def get_db_connection():
    """Borrow a connection from the pool (caller *must* put it back)."""
    try:
        conn = get_db_pool().getconn()
        if conn:
            return conn
        raise Exception("Could not get connection from pool")
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
        raise


def return_db_connection(conn):
    """Return a connection to the pool."""
    try:
        if conn:
            get_db_pool().putconn(conn)
    except Exception as e:
        logger.error(f"Error returning connection to pool: {e}")


def close_db_pool():
    """Close all connections and reset the pool."""
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None
        logger.info("✓ Database connection pool closed")


# ──────────────────────────────────────────────────────────────────────────
# Convenience context-manager
# ──────────────────────────────────────────────────────────────────────────
@contextmanager
def db_connection():
    """
    Context-manager that automatically returns the connection to the pool.

    Example
    -------
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    conn = None
    try:
        conn = get_db_connection()
        yield conn
    finally:
        if conn:
            return_db_connection(conn)
