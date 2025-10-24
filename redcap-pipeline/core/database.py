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
                10,
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
            )
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    return db_pool


def get_db_connection():
    """Get connection from pool"""
    return get_db_pool().getconn()


def return_db_connection(conn):
    """Return connection to pool"""
    get_db_pool().putconn(conn)


def close_db_pool():
    """Close all connections in pool"""
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None