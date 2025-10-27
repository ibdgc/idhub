# fragment-validator/tests/conftest.py
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Set test environment variables
os.environ.update(
    {
        "S3_BUCKET": "test-bucket",
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "AWS_DEFAULT_REGION": "us-east-1",
        "GSID_SERVICE_URL": "http://localhost:8000",
        "GSID_API_KEY": "test-api-key",
        "NOCODB_URL": "http://localhost:8080",
        "NOCODB_API_TOKEN": "test-token",
        "NOCODB_BASE_ID": "test-base-id",
    }
)


# ============================================================================
# MOCK AWS/S3
# ============================================================================
@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client"""
    with patch("boto3.client") as mock:
        s3 = MagicMock()
        s3.put_object.return_value = {"ETag": "test-etag"}

        # Fix: Properly mock the Body.read() method for pandas
        def mock_get_object(*args, **kwargs):
            import io

            csv_data = b"col1,col2\nval1,val2"
            return {"Body": io.BytesIO(csv_data)}

        s3.get_object.side_effect = mock_get_object

        s3.list_objects_v2.return_value = {"Contents": [{"Key": "test.csv"}]}
        mock.return_value = s3
        yield s3


# ============================================================================
# TEST DATA FIXTURES
# ============================================================================
@pytest.fixture
def sample_blood_data():
    """Sample blood table data"""
    return pd.DataFrame(
        {
            "consortium_id": ["IBDGC001", "IBDGC002", "IBDGC003"],
            "sample_id": ["BLD001", "BLD002", "BLD003"],
            "sample_type": ["Whole Blood", "Plasma", "Serum"],
            "date_collected": ["2024-01-15", "2024-02-20", "2024-03-10"],
            "center_name": ["Mount Sinai", "Cedars-Sinai", "Johns Hopkins"],
        }
    )


@pytest.fixture
def sample_lcl_data():
    """Sample LCL table data"""
    return pd.DataFrame(
        {
            "consortium_id": ["IBDGC004", "IBDGC005"],
            "knumber": ["K12345", "K67890"],
            "niddk_no": ["N11111", "N22222"],
        }
    )


@pytest.fixture
def sample_dna_data():
    """Sample DNA table data"""
    return pd.DataFrame(
        {
            "consortium_id": ["IBDGC006", "IBDGC007"],
            "local_id": ["LOCAL123", "LOCAL456"],
            "dna_sample_id": ["DNA001", "DNA002"],
            "center_id": [1, 2],
        }
    )


# ============================================================================
# MAPPING CONFIG FIXTURES
# ============================================================================
@pytest.fixture
def blood_mapping_config():
    """Blood table mapping configuration"""
    return {
        "field_mapping": {
            "sample_id": "sample_id",
            "sample_type": "sample_type",
            "date_collected": "date_collected",
        },
        "subject_id_candidates": ["consortium_id"],
        "center_id_field": None,
        "default_center_id": 0,
    }


@pytest.fixture
def lcl_mapping_config():
    """LCL table mapping configuration"""
    return {
        "field_mapping": {"knumber": "knumber", "niddk_no": "niddk_no"},
        "subject_id_candidates": ["consortium_id"],
        "center_id_field": None,
        "default_center_id": 0,
    }


@pytest.fixture
def dna_mapping_config():
    """DNA table mapping configuration"""
    return {
        "field_mapping": {"sample_id": "dna_sample_id"},
        "subject_id_candidates": ["consortium_id", "local_id"],
        "center_id_field": "center_id",
        "default_center_id": 1,
    }


# ============================================================================
# NOCODB MOCK FIXTURES
# ============================================================================
@pytest.fixture
def mock_nocodb_client():
    """Mock NocoDB client with realistic responses"""
    client = MagicMock()

    # Mock base ID detection
    client._get_base_id.return_value = "test-base-id"

    # Mock table ID lookup - return different IDs per table
    def mock_get_table_id(table_name):
        return f"test-{table_name}-id"

    client.get_table_id.side_effect = mock_get_table_id

    # Mock table metadata - return different schemas per table
    def mock_get_table_metadata(table_name):
        # Define table-specific schemas
        schemas = {
            "blood": {
                "id": "test-blood-id",
                "table_name": "blood",
                "columns": [
                    {"column_name": "Id", "pk": True, "ai": True},
                    {"column_name": "global_subject_id", "rqd": True},
                    {"column_name": "sample_id", "rqd": True},
                    {"column_name": "sample_type", "rqd": False},
                    {"column_name": "date_collected", "rqd": False},
                    {"column_name": "created_at", "rqd": False},
                ],
            },
            "lcl": {
                "id": "test-lcl-id",
                "table_name": "lcl",
                "columns": [
                    {"column_name": "Id", "pk": True, "ai": True},
                    {"column_name": "global_subject_id", "rqd": True},
                    {"column_name": "knumber", "rqd": True},
                    {"column_name": "niddk_no", "rqd": False},
                    {"column_name": "created_at", "rqd": False},
                ],
            },
            "dna": {
                "id": "test-dna-id",
                "table_name": "dna",
                "columns": [
                    {"column_name": "Id", "pk": True, "ai": True},
                    {"column_name": "global_subject_id", "rqd": True},
                    {"column_name": "sample_id", "rqd": True},
                    {"column_name": "center_id", "rqd": False},
                    {"column_name": "created_at", "rqd": False},
                ],
            },
        }

        # Return table-specific schema, or default to blood
        return schemas.get(table_name, schemas["blood"])

    client.get_table_metadata.side_effect = mock_get_table_metadata

    # Mock local ID cache
    client.load_local_id_cache.return_value = {
        (0, "IBDGC001", "consortium_id"): "GSID-01HQXYZ123",
        (1, "LOCAL123", "local_id"): "GSID-01HQABC456",
    }

    return client


# ============================================================================
# GSID SERVICE MOCK FIXTURES
# ============================================================================
@pytest.fixture
def mock_gsid_client():
    """Mock GSID client with realistic responses"""
    client = MagicMock()

    # Mock batch registration
    def mock_register_batch(requests_list, **kwargs):
        results = []
        for i, req in enumerate(requests_list):
            local_id = req["local_subject_id"]
            # Simulate existing vs new GSIDs
            if local_id == "IBDGC001":
                results.append(
                    {
                        "gsid": "GSID-01HQXYZ123",
                        "action": "existing_match",
                        "matched_by": "consortium_id",
                    }
                )
            else:
                results.append({"gsid": f"GSID-01HQ{i:012d}", "action": "create_new"})
        return results

    client.register_batch.side_effect = mock_register_batch

    # Mock single registration
    client.register_single.return_value = {
        "gsid": "GSID-01HQTEST123",
        "action": "create_new",
    }

    return client


# ============================================================================
# TEMP FILE FIXTURES
# ============================================================================
@pytest.fixture
def temp_csv_file(tmp_path, sample_blood_data):
    """Create a temporary CSV file for testing"""
    csv_file = tmp_path / "test_blood.csv"
    sample_blood_data.to_csv(csv_file, index=False)
    return str(csv_file)


@pytest.fixture
def temp_mapping_config(tmp_path, blood_mapping_config):
    """Create a temporary mapping config JSON file"""
    import json

    config_file = tmp_path / "blood_mapping.json"
    with open(config_file, "w") as f:
        json.dump(blood_mapping_config, f)
    return str(config_file)
