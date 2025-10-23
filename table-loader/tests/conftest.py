# table-loader/tests/conftest.py
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.update(
    {
        "DB_HOST": "localhost",
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
    cursor.rowcount = 0
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
        s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b"col1,col2\nval1,val2")
        }
        mock.return_value = s3
        yield s3
