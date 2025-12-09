# table-loader/tests/test_database.py
import pytest
from unittest.mock import MagicMock, patch

from core.database import DatabaseManager


@patch("psycopg2.connect")
class TestDatabaseManager:
    """Test DatabaseManager functionality"""

    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        manager = DatabaseManager()
        conn = manager.get_connection()
        assert conn is not None
        mock_connect.assert_called_once()

    def test_get_connection_failure(self, mock_connect):
        """Test database connection failure"""
        mock_connect.side_effect = Exception("Connection failed")
        manager = DatabaseManager()
        with pytest.raises(Exception) as exc_info:
            manager.get_connection()
        assert "Connection failed" in str(exc_info.value)

    def test_get_cursor_commits_on_success(self, mock_connect):
        """Test that get_cursor commits on success"""
        mock_conn = mock_connect.return_value
        mock_cursor = mock_conn.cursor.return_value
        manager = DatabaseManager()

        with manager.get_cursor() as cur:
            assert cur is mock_cursor

        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_get_cursor_rolls_back_on_error(self, mock_connect):
        """Test that get_cursor rolls back on error"""
        mock_conn = mock_connect.return_value
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Test error")
        manager = DatabaseManager()

        with pytest.raises(Exception, match="Test error"):
            with manager.get_cursor() as cur:
                cur.execute("SELECT 1")

        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_execute_query_fetch(self, mock_connect):
        """Test execute_query with fetch"""
        mock_conn = mock_connect.return_value
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchall.return_value = [{"id": 1}]
        manager = DatabaseManager()

        result = manager.execute_query("SELECT 1", fetch=True)

        assert result == [{"id": 1}]
        mock_cursor.execute.assert_called_once_with("SELECT 1", None)
        mock_cursor.fetchall.assert_called_once()

    def test_execute_query_no_fetch(self, mock_connect):
        """Test execute_query without fetch"""
        mock_conn = mock_connect.return_value
        mock_cursor = mock_conn.cursor.return_value
        manager = DatabaseManager()

        result = manager.execute_query("INSERT 1", fetch=False)

        assert result is None
        mock_cursor.execute.assert_called_once_with("INSERT 1", None)
        mock_cursor.fetchall.assert_not_called()

    def test_get_table_schema(self, mock_connect):
        """Test get_table_schema"""
        mock_conn = mock_connect.return_value
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {"column_name": "col1", "data_type": "text"},
            {"column_name": "col2", "data_type": "integer"},
        ]
        manager = DatabaseManager()

        schema = manager.get_table_schema("test_table")

        assert schema == {"col1": "text", "col2": "integer"}
        mock_cursor.execute.assert_called_once()
