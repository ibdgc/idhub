# table-loader/core/database.py
import logging
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Database connection manager with connection pooling"""

    def __init__(self):
        self.connection_params = {
            "host": os.getenv("DB_HOST", "localhost"),
            "database": os.getenv("DB_NAME", "idhub"),
            "user": os.getenv("DB_USER", "idhub_user"),
            "password": os.getenv("DB_PASSWORD", ""),
            "port": int(os.getenv("DB_PORT", "5432")),
        }

    def get_connection(self):
        """Get a new database connection"""
        try:
            conn = psycopg2.connect(**self.connection_params)
            logger.debug("Database connection established")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    @contextmanager
    def get_cursor(self, cursor_factory=RealDictCursor):
        """
        Context manager for database cursor with automatic connection management

        Usage:
            with db_manager.get_cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                results = cursor.fetchall()
        """
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def execute_query(self, query: str, params: tuple = None, fetch: bool = True):
        """
        Execute a query with automatic connection management

        Args:
            query: SQL query string
            params: Query parameters tuple
            fetch: Whether to fetch results (default: True)

        Returns:
            Query results if fetch=True, else None
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            return None

    def get_table_columns(self, table_name: str) -> list[str]:
        """
        Get column names for a specific table from the information schema.

        Args:
            table_name: The name of the table.

        Returns:
            A list of column names for the table.
        """
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s;
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, (table_name,))
            return [row["column_name"] for row in cursor.fetchall()]


# Global database manager instance
db_manager = DatabaseManager()


def get_db_connection():
    """
    Get a new database connection using environment variables

    Returns:
        psycopg2 connection object
    """
    return db_manager.get_connection()


@contextmanager
def get_db_cursor(conn=None, cursor_factory=RealDictCursor):
    """
    Context manager for database cursor with automatic commit/rollback

    Args:
        conn: Existing connection (if None, creates new one)
        cursor_factory: Cursor factory class (default: RealDictCursor)

    Usage:
        # With existing connection
        conn = get_db_connection()
        try:
            with get_db_cursor(conn) as cursor:
                cursor.execute("SELECT * FROM table")
                results = cursor.fetchall()
            conn.commit()
        finally:
            conn.close()

        # Auto-managed connection
        with get_db_cursor() as cursor:
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()
    """
    close_conn = False

    if conn is None:
        conn = get_db_connection()
        close_conn = True

    cursor = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cursor
        if close_conn:
            conn.commit()
    except Exception as e:
        if close_conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        cursor.close()
        if close_conn:
            conn.close()


def execute_query(query: str, params: tuple = None, fetch: bool = True):
    """
    Execute a query with automatic connection management

    Args:
        query: SQL query string
        params: Query parameters tuple
        fetch: Whether to fetch results (default: True)

    Returns:
        Query results if fetch=True, else None
    """
    return db_manager.execute_query(query, params, fetch)
