# redcap-pipeline/tests/test_database.py
import os
from unittest.mock import MagicMock, patch

import pytest


class TestDatabase:
    """Test database connection pool functionality"""

    def test_get_db_pool_creates_pool(self):
        """Test that get_db_pool creates a connection pool"""
        import core.database as db_module
        from core.database import get_db_pool

        # Reset pool
        db_module.db_pool = None

        with patch("psycopg2.pool.SimpleConnectionPool") as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            pool = get_db_pool()

            mock_pool_class.assert_called_once()
            assert pool == mock_pool
            assert db_module.db_pool == mock_pool

    def test_get_db_pool_singleton(self):
        """Test that get_db_pool returns same pool instance"""
        import core.database as db_module
        from core.database import get_db_pool

        mock_pool = MagicMock()
        db_module.db_pool = mock_pool

        pool = get_db_pool()

        assert pool == mock_pool

    def test_get_db_connection(self):
        """Test getting a connection from the pool"""
        from core.database import get_db_connection, get_db_pool

        with patch("core.database.get_db_pool") as mock_get_pool:
            mock_pool = MagicMock()
            mock_conn = MagicMock()
            mock_pool.getconn.return_value = mock_conn
            mock_get_pool.return_value = mock_pool

            conn = get_db_connection()

            mock_pool.getconn.assert_called_once()
            assert conn == mock_conn

    def test_return_db_connection(self):
        """Test returning a connection to the pool"""
        from core.database import get_db_pool, return_db_connection

        with patch("core.database.get_db_pool") as mock_get_pool:
            mock_pool = MagicMock()
            mock_conn = MagicMock()
            mock_get_pool.return_value = mock_pool

            return_db_connection(mock_conn)

            mock_pool.putconn.assert_called_once_with(mock_conn)

    def test_close_db_pool(self):
        """Test closing the database pool"""
        import core.database as db_module
        from core.database import close_db_pool

        mock_pool = MagicMock()
        db_module.db_pool = mock_pool

        close_db_pool()

        mock_pool.closeall.assert_called_once()
        assert db_module.db_pool is None

    def test_db_connection_context_manager_success(self):
        """Test db_connection context manager success case"""
        from core.database import db_connection

        with (
            patch("core.database.get_db_connection") as mock_get_conn,
            patch("core.database.return_db_connection") as mock_return_conn,
        ):
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            with db_connection() as conn:
                assert conn == mock_conn

            mock_get_conn.assert_called_once()
            mock_return_conn.assert_called_once_with(mock_conn)

    def test_db_connection_context_manager_error(self):
        """Test db_connection context manager handles errors"""
        from core.database import db_connection

        with (
            patch("core.database.get_db_connection") as mock_get_conn,
            patch("core.database.return_db_connection") as mock_return_conn,
        ):
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            with pytest.raises(ValueError):
                with db_connection() as conn:
                    raise ValueError("Test error")

            # Connection should still be returned
            mock_return_conn.assert_called_once_with(mock_conn)

    def test_get_db_pool_with_env_vars(self):
        """Test pool creation with environment variables"""
        import core.database as db_module
        from core.database import get_db_pool

        # Reset pool
        db_module.db_pool = None

        with (
            patch.dict(
                os.environ,
                {
                    "DB_HOST": "test-host",
                    "DB_NAME": "test-db",
                    "DB_USER": "test-user",
                    "DB_PASSWORD": "test-pass",
                    "DB_PORT": "5433",
                },
            ),
            patch("psycopg2.pool.SimpleConnectionPool") as mock_pool_class,
        ):
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            pool = get_db_pool()

            # Verify pool was created
            mock_pool_class.assert_called_once()

            # Get the actual call arguments
            call_args = mock_pool_class.call_args

            # Check keyword args
            kwargs = call_args.kwargs
            assert kwargs["minconn"] == 10
            assert kwargs["maxconn"] == 50
            assert kwargs["host"] == "test-host"
            assert kwargs["database"] == "test-db"
            assert kwargs["user"] == "test-user"
            assert kwargs["password"] == "test-pass"
            assert pool == mock_pool
