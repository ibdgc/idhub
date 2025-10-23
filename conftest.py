# conftest.py (root level)
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Set test environment variables before any imports
os.environ.update(
    {
        "DB_HOST": "localhost",
        "DB_NAME": "test_idhub",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_pass",
        "DB_PORT": "5432",
        "S3_BUCKET": "test-bucket",
        "GSID_API_KEY": "test-api-key",
        "GSID_SERVICE_URL": "http://localhost:8000",
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "AWS_DEFAULT_REGION": "us-east-1",
        "REDCAP_API_URL": "https://test.redcap.edu/api/",
        "REDCAP_API_TOKEN": "test-token",
    }
)


# Mock psycopg2 pool before any imports
@pytest.fixture(scope="session", autouse=True)
def mock_db_pool():
    """Mock database connection pool for all tests"""
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
        yield mock_pool


@pytest.fixture
def mock_db_connection():
    """Mock database connection"""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.rowcount = 0
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.fixture
def mock_s3_client():
    """Mock S3 client"""
    with patch("boto3.client") as mock_client:
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"Contents": []}
        s3.get_object.return_value = {"Body": MagicMock()}
        s3.put_object.return_value = {}
        mock_client.return_value = s3
        yield s3
