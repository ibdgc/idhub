# redcap-pipeline/core/database.py
import logging
import os

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

db_pool = None


def get_db_pool():
    global db_pool
    if db_pool is None:
        logger.info("Initializing database connection pool...")
        try:
            db_pool = psycopg2.pool.SimpleConnectionPool(
                1,
                20,  # Increased from 10 to 20
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
    """Get connection from pool"""
    try:
        conn = get_db_pool().getconn()
        if conn:
            return conn
        else:
            raise Exception("Could not get connection from pool")
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
        raise


def return_db_connection(conn):
    """Return connection to pool"""
    try:
        if conn:
            get_db_pool().putconn(conn)
    except Exception as e:
        logger.error(f"Error returning connection to pool: {e}")


def close_db_pool():
    """Close all connections in pool"""
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None
        logger.info("✓ Database connection pool closed")

