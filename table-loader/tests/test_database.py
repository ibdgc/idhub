# table-loader/tests/test_database.py
import pytest
from unittest.mock import MagicMock, patch

from core.database import DatabaseManager


class TestDatabaseManager:
    """Test DatabaseManager functionality"""

    def test_singleton_pattern(self):
        """Test that DatabaseManager follows singleton pattern"""
        manager1 = DatabaseManager.get_instance()
        manager2 = DatabaseManager.get_instance()

        assert manager1 is manager2
        assert DatabaseManager._instance is not None

    def test_lazy_pool_initialization(self):
        """Test that pool is not initialized until first use"""
        manager = DatabaseManager()

        # Pool should be None initially
        assert manager.pool is None

    @patch("psycopg2.pool.ThreadedConnectionPool")
    def test_ensure_pool_creates_pool(self, mock_pool):
        """Test that _ensure_pool creates connection pool"""
        manager = DatabaseManager()
        manager.pool = None  # Reset

        manager._ensure_pool()

        # Pool should be created
        mock_pool.assert_called_once()
        assert manager.pool is not None

    @patch("psycopg2.pool.ThreadedConnectionPool")
    def test_ensure_pool_only_creates_once(self, mock_pool):
        """Test that _ensure_pool doesn't recreate existing pool"""
        manager = DatabaseManager()
        manager.pool = MagicMock()  # Simulate existing pool

        manager._ensure_pool()

        # Should not create new pool
        mock_pool.assert_not_called()

    def test_get_connection_context_manager(self, mock_db_connection):
        """Test get_connection as context manager"""
        conn, cursor = mock_db_connection
        manager = DatabaseManager()
        manager.pool = MagicMock()
        manager.pool.getconn.return_value = conn

        with manager.get_connection() as connection:
            assert connection is conn

        # Should return connection to pool
        manager.pool.putconn.assert_called_once_with(conn)

    def test_get_cursor_commits_on_success(self, mock_db_connection):
        """Test that get_cursor commits on success"""
        conn, cursor = mock_db_connection
        manager = DatabaseManager()

        with manager.get_cursor(conn) as cur:
            assert cur is cursor

        conn.commit.assert_called_once()
        cursor.close.assert_called_once()

    def test_get_cursor_rolls_back_on_error(self, mock_db_connection):
        """Test that get_cursor rolls back on error"""
        conn, cursor = mock_db_connection
        manager = DatabaseManager()

        # Simulate error during cursor operation
        cursor.__enter__.side_effect = Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            with manager.get_cursor(conn) as cur:
                pass

        conn.rollback.assert_called_once()

    def test_bulk_insert(self, mock_db_connection):
        """Test bulk_insert method"""
        conn, cursor = mock_db_connection
        manager = DatabaseManager()

        table = "test_table"
        columns = ["col1", "col2"]
        values = [("val1", "val2"), ("val3", "val4")]

        with patch("core.database.execute_values") as mock_execute:
            manager.bulk_insert(conn, table, columns, values)

            # Should call execute_values
            mock_execute.assert_called_once()
            call_args = mock_execute.call_args
            assert "INSERT INTO test_table" in call_args[0][1]

    def test_close_pool(self):
        """Test closing connection pool"""
        manager = DatabaseManager()
        manager.pool = MagicMock()

        manager.close()

        manager.pool.closeall.assert_called_once()
        assert manager.pool is None
