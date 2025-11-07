from unittest.mock import MagicMock, patch

import pytest


class TestDatabaseService:
    """Test database service functionality"""

    def test_get_db_connection_success(self):
        """Test successful database connection"""
        with patch("psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            from core.database import get_db_connection

            conn = get_db_connection()
            assert conn is not None
            mock_connect.assert_called_once()

    def test_get_db_connection_failure(self):
        """Test database connection failure"""
        with patch("psycopg2.connect", side_effect=Exception("Connection failed")):
            from core.database import get_db_connection

            with pytest.raises(Exception) as exc_info:
                get_db_connection()

            assert "Connection failed" in str(exc_info.value)

    def test_get_db_cursor_context_manager(self, mock_db_connection):
        """Test cursor context manager"""
        from core.database import get_db_cursor

        with get_db_cursor(mock_db_connection) as cursor:
            assert cursor is not None

        # Verify cursor was closed
        cursor.close.assert_called_once()

    def test_get_db_cursor_with_custom_factory(self, mock_db_connection):
        """Test cursor with custom cursor factory"""
        from core.database import get_db_cursor
        from psycopg2.extras import RealDictCursor

        with get_db_cursor(mock_db_connection, cursor_factory=RealDictCursor) as cursor:
            assert cursor is not None

        mock_db_connection.cursor.assert_called_with(cursor_factory=RealDictCursor)

    def test_get_db_cursor_error_handling(self, mock_db_connection):
        """Test cursor error handling and rollback"""
        from core.database import get_db_cursor

        mock_cursor = mock_db_connection.cursor()
        mock_cursor.execute.side_effect = Exception("Query failed")

        with pytest.raises(Exception):
            with get_db_cursor(mock_db_connection) as cursor:
                cursor.execute("SELECT 1")

        # Verify rollback was called
        mock_db_connection.rollback.assert_called_once()
        # Verify cursor was closed
        mock_cursor.close.assert_called_once()

    def test_database_connection_settings(self):
        """Test database connection uses correct settings"""
        with patch("psycopg2.connect") as mock_connect:
            from core.database import get_db_connection

            get_db_connection()

            # Verify connection was called with settings
            call_kwargs = mock_connect.call_args[1]
            assert "host" in call_kwargs
            assert "database" in call_kwargs
            assert "user" in call_kwargs
            assert "password" in call_kwargs
            assert "port" in call_kwargs
