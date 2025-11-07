import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Set test environment variables before any imports
os.environ.update({
    "GSID_API_KEY": "test-api-key-12345",
    "DB_HOST": "localhost",
    "DB_NAME": "test_gsid_db",
    "DB_USER": "test_user",
    "DB_PASSWORD": "test_password",
    "DB_PORT": "5432",
})


@pytest.fixture(scope="session", autouse=True)
def mock_db_pool():
    """Mock database pool globally for all tests"""
    with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Setup cursor behavior
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        mock_cursor.rowcount = 0
        mock_cursor.description = None

        # Context manager support for cursor
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        # Connection returns cursor
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        # Pool returns connection
        mock_pool.return_value.getconn.return_value = mock_conn
        mock_pool.return_value.putconn = MagicMock()

        yield mock_pool


@pytest.fixture
def mock_db_connection():
    """Provide a fresh mock database connection for each test"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.rowcount = 0
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    return mock_conn


@pytest.fixture
def mock_db_cursor(mock_db_connection):
    """Provide a mock cursor"""
    return mock_db_connection.cursor()


@pytest.fixture
def sample_gsid():
    """Provide a sample valid GSID"""
    return "GSID-0123456789ABCDEF"


@pytest.fixture
def sample_subject_data():
    """Provide sample subject data for testing"""
    return {
        "first_name": "John",
        "last_name": "Doe",
        "date_of_birth": "1990-01-01",
        "sex": "M",
        "source_id": "TEST001",
        "source_name": "test_source"
    }


@pytest.fixture
def sample_identity_attributes():
    """Provide sample identity attributes"""
    return {
        "first_name": "John",
        "last_name": "Doe",
        "date_of_birth": "1990-01-01",
        "sex": "M"
    }
