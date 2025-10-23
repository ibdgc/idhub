# gsid-service/tests/conftest.py
import os
import sys
from unittest.mock import MagicMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

# Set environment variables
os.environ.update(
    {
        "GSID_API_KEY": "test-api-key",
        "DB_HOST": "localhost",
        "DB_NAME": "test_db",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_pass",
        "DB_PORT": "5432",
    }
)

# Mock the database module before any imports
mock_database = Mock()
mock_database.get_db_connection = MagicMock()
mock_database.get_db_cursor = MagicMock()
sys.modules["services.database"] = mock_database


@pytest.fixture(scope="session", autouse=True)
def mock_db_pool():
    """Mock database pool before any imports"""
    with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_pool.return_value.getconn.return_value = mock_conn

        # Setup mock database functions
        mock_database.get_db_connection.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_database.get_db_connection.return_value.__exit__ = MagicMock(
            return_value=False
        )
        mock_database.get_db_cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_database.get_db_cursor.return_value.__exit__ = MagicMock(
            return_value=False
        )

        yield mock_pool


@pytest.fixture
def test_client(mock_db_pool):
    """Create test client"""
    # Import after mocking
    from main import app

    return TestClient(app)


@pytest.fixture
def mock_generate_gsid():
    """Mock GSID generation"""
    with patch("main.generate_gsid") as mock:
        mock.return_value = "01HQXYZ123456789ABCDEFGHJ"
        yield mock


@pytest.fixture
def mock_db_connection():
    """Mock database connection"""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cursor
