# table-loader/tests/conftest.py
import json
import os
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Set test environment variables before any imports
os.environ.update(
    {
        "DB_HOST": "localhost",
        "DB_NAME": "test_db",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_pass",
        "DB_PORT": "5432",
        "S3_BUCKET": "test-bucket",
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "AWS_DEFAULT_REGION": "us-east-1",
    }
)


# ============================================================================
# DATABASE FIXTURES
# ============================================================================
@pytest.fixture(scope="session", autouse=True)
def mock_db_pool():
    """Mock psycopg2 connection pool globally"""
    with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
        yield mock_pool


@pytest.fixture
def mock_db_connection():
    """Mock database connection with cursor"""
    conn = MagicMock()
    cursor = MagicMock()

    # Configure cursor behavior
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.rowcount = 0
    cursor.description = None

    # Context manager support
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    # Connection returns cursor
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    return conn, cursor


@pytest.fixture
def mock_db_manager(mock_db_connection):
    """Mock DatabaseManager with connection pool"""
    conn, cursor = mock_db_connection

    with patch("core.database.db_manager") as mock_manager:
        # Mock get_connection context manager
        mock_manager.get_connection.return_value.__enter__.return_value = conn
        mock_manager.get_connection.return_value.__exit__.return_value = False

        # Mock get_cursor context manager
        mock_manager.get_cursor.return_value.__enter__.return_value = cursor
        mock_manager.get_cursor.return_value.__exit__.return_value = False

        # Mock bulk_insert
        mock_manager.bulk_insert.return_value = None

        yield mock_manager


# ============================================================================
# S3 FIXTURES
# ============================================================================
@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client"""
    with patch("boto3.client") as mock_boto:
        s3 = MagicMock()
        mock_boto.return_value = s3

        # Default responses
        s3.list_objects_v2.return_value = {"Contents": []}
        s3.get_object.return_value = {
            "Body": BytesIO(b"col1,col2\nval1,val2")
        }
        s3.put_object.return_value = {}
        s3.copy_object.return_value = {}
        s3.delete_object.return_value = {}

        yield s3


# ============================================================================
# DATA FIXTURES
# ============================================================================
@pytest.fixture
def sample_fragment_data():
    """Sample fragment data structure"""
    return {
        "table": "blood",
        "records": [
            {
                "global_subject_id": "GSID-001",
                "sample_id": "SMP001",
                "sample_type": "Whole Blood",
                "date_collected": "2024-01-15",
                "consortium_id": "ID001",  # Should be excluded
                "identifier_type": "consortium_id",  # Should be excluded
                "action": "link_existing",  # Should be excluded
            },
            {
                "global_subject_id": "GSID-002",
                "sample_id": "SMP002",
                "sample_type": "Serum",
                "date_collected": "2024-01-16",
                "consortium_id": "ID002",
                "identifier_type": "consortium_id",
                "action": "create_new",
            },
        ],
        "metadata": {
            "key_columns": ["global_subject_id", "sample_id"],
        },
    }


@pytest.fixture
def sample_validation_report():
    """Sample validation report"""
    return {
        "batch_id": "batch_20240115_120000",
        "table_name": "blood",
        "source_name": "test_source",
        "status": "VALIDATED",
        "row_count": 2,
        "subject_id_candidates": ["consortium_id"],
        "center_id_field": None,
        "exclude_from_load": ["consortium_id", "identifier_type", "action"],
        "resolution_summary": {
            "existing_matches": 1,
            "new_gsids_minted": 1,
        },
    }


@pytest.fixture
def sample_dataframe():
    """Sample pandas DataFrame"""
    return pd.DataFrame(
        {
            "global_subject_id": ["GSID-001", "GSID-002"],
            "sample_id": ["SMP001", "SMP002"],
            "sample_type": ["Whole Blood", "Serum"],
        }
    )


# ============================================================================
# S3 MOCK HELPERS
# ============================================================================
@pytest.fixture
def s3_with_fragments(mock_s3_client, sample_fragment_data, sample_validation_report):
    """S3 client with pre-configured fragment data"""
    batch_id = "batch_20240115_120000"

    # Mock list_objects_v2 to return fragment files
    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": f"staging/validated/{batch_id}/blood.csv"},
            {"Key": f"staging/validated/{batch_id}/validation_report.json"},
        ]
    }

    # Mock get_object for CSV fragment
    csv_data = pd.DataFrame(sample_fragment_data["records"]).to_csv(index=False)
    mock_s3_client.get_object.side_effect = lambda Bucket, Key: {
        "Body": BytesIO(
            json.dumps(sample_validation_report).encode()
            if "validation_report.json" in Key
            else csv_data.encode()
        )
    }

    return mock_s3_client


# ============================================================================
# INTEGRATION FIXTURES
# ============================================================================
@pytest.fixture
def table_loader(mock_s3_client):
    """TableLoader instance with mocked S3"""
    from services.loader import TableLoader

    loader = TableLoader()
    return loader
