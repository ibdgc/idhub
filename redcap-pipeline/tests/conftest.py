import json
import os
from pathlib import Path
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
def mock_requests():
    """Mock requests library"""
    with patch("requests.Session.post") as mock:
        mock.return_value = MagicMock(
            status_code=200,
            json=lambda: [],
        )
        yield mock


@pytest.fixture
def sample_project_config():
    """Sample project configuration"""
    return {
        "key": "test_project",
        "name": "Test Project",
        "redcap_project_id": "123",
        "redcap_api_url": "https://test.redcap.edu/api/",
        "api_token": "test_token_12345678",
        "field_mappings": "test_field_mappings.json",
        "batch_size": 50,
    }


@pytest.fixture
def temp_field_mappings_file(tmp_path):
    """Create temporary field mappings file"""
    mappings = {
        "mappings": [
            {
                "source_field": "subject_id",
                "target_table": "local_subject_ids",
                "target_field": "local_subject_id",
                "identifier_type": "primary",
            },
            {
                "source_field": "alternate_id",
                "target_table": "local_subject_ids",
                "target_field": "local_subject_id",
                "identifier_type": "alternate",
            },
            {
                "source_field": "registration_date",
                "target_table": "subjects",
                "target_field": "registration_year",
            },
            {
                "source_field": "control",
                "target_table": "subjects",
                "target_field": "control",
            },
            {
                "source_field": "blood_sample_id",
                "target_table": "specimen",
                "target_field": "sample_id",
                "sample_type": "blood",
            },
            {
                "source_field": "dna_sample_id",
                "target_table": "specimen",
                "target_field": "sample_id",
                "sample_type": "dna",
            },
            {
                "source_field": "wgs_sample_id",
                "target_table": "sequence",
                "target_field": "sample_id",
                "sample_type": "wgs",
            },
        ],
        "transformations": {
            "registration_date": {"type": "extract_year"},
            "control": {
                "type": "boolean",
                "true_values": ["1", "true", "yes"],
                "false_values": ["0", "false", "no"],
            },
        },
    }

    # Create in config directory
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)

    file_path = config_dir / "test_field_mappings.json"
    with open(file_path, "w") as f:
        json.dump(mappings, f)

    return file_path


@pytest.fixture
def sample_redcap_record():
    """Sample REDCap record"""
    return {
        "record_id": "1",
        "redcap_data_access_group": "mount_sinai",
        "subject_id": "MSSM001",
        "alternate_id": "ALT001",
        "registration_date": "2024-01-15",
        "control": "0",
        "blood_sample_id": "BLOOD001",
        "dna_sample_id": "DNA001",
        "wgs_sample_id": "WGS001",
        "family_id": "FAM001",
    }
