# redcap-pipeline/tests/conftest.py
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.update(
    {
        "DB_HOST": "localhost",
        "REDCAP_API_URL": "https://test.redcap.edu/api/",
        "REDCAP_API_TOKEN": "test-token",
        "GSID_SERVICE_URL": "http://localhost:8000",
        "S3_BUCKET": "test-bucket",
    }
)


@pytest.fixture(scope="session", autouse=True)
def mock_db_pool():
    """Mock database pool"""
    with patch("psycopg2.pool.ThreadedConnectionPool"):
        yield


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


@pytest.fixture
def mock_s3():
    """Mock S3 client"""
    with patch("boto3.client") as mock:
        s3 = MagicMock()
        s3.put_object.return_value = {}
        mock.return_value = s3
        yield s3


@pytest.fixture
def mock_requests():
    """Mock requests"""
    with patch("requests.post") as mock:
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = []
        mock.return_value = response
        yield mock
