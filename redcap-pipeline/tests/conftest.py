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
def sample_projects_config():
    """Sample projects configuration - matches structure from config/projects.json"""
    return {
        "projects": {
            "gap": {
                "name": "GAP",
                "redcap_project_id": "16894",
                "api_token": "test_token_gap",
                "field_mappings": "gap_field_mappings.json",
                "schedule": "continuous",
                "batch_size": 50,
                "enabled": True,
                "description": "Main biobank project",
            },
            "uc_demarc": {
                "name": "uc_demarc",
                "redcap_project_id": "16895",
                "api_token": "test_token_uc",
                "field_mappings": "uc_demarc_field_mappings.json",
                "schedule": "manual",
                "batch_size": 50,
                "enabled": False,
                "description": "Legacy sample collection",
            },
        }
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
            }
        ]
    }

    mappings_file = tmp_path / "test_field_mappings.json"
    mappings_file.write_text(json.dumps(mappings))
    return mappings_file


@pytest.fixture
def sample_redcap_record():
    """Sample REDCap record"""
    return {
        "record_id": "1",
        "subject_id": "TEST001",
        "center": "MSSM",
        "enrollment_date": "2024-01-15",
        "control": "0",
    }


@pytest.fixture
def setup_logs_dir():
    """Ensure logs directory exists"""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    yield logs_dir
