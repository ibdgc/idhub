import io
import json
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from services import (
    FragmentValidator,
    GSIDClient,
    NocoDBClient,
    S3Client,
)


# ============================================================================
# MOCK CLIENT FIXTURES
# ============================================================================
@pytest.fixture
def mock_s3_client():
    """Mock S3 client"""
    with patch("boto3.client") as mock_boto:
        mock_client = Mock()
        mock_boto.return_value = mock_client

        # Mock list_objects_v2 response
        mock_client.list_objects_v2.return_value = {"Contents": []}

        # Mock get_object for download_dataframe
        csv_data = "col1,col2\nval1,val2\n"
        mock_client.get_object.return_value = {"Body": io.BytesIO(csv_data.encode())}

        yield mock_client


@pytest.fixture
def mock_nocodb_client():
    """Mock NocoDB client"""
    mock = Mock(spec=NocoDBClient)

    # Define schemas for different tables
    table_schemas = {
        "blood": {
            "columns": [
                {
                    "title": "Id",
                    "column_name": "Id",
                    "uidt": "ID",
                    "pk": True,
                    "ai": True,
                    "rqd": False,
                },
                {
                    "title": "global_subject_id",
                    "column_name": "global_subject_id",
                    "uidt": "SingleLineText",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
                {
                    "title": "sample_id",
                    "column_name": "sample_id",
                    "uidt": "SingleLineText",
                    "pk": False,
                    "ai": False,
                    "rqd": True,
                },
                {
                    "title": "sample_type",
                    "column_name": "sample_type",
                    "uidt": "SingleLineText",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
                {
                    "title": "date_collected",
                    "column_name": "date_collected",
                    "uidt": "Date",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
                {
                    "title": "created_at",
                    "column_name": "created_at",
                    "uidt": "DateTime",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
            ]
        },
        "lcl": {
            "columns": [
                {
                    "title": "Id",
                    "column_name": "Id",
                    "uidt": "ID",
                    "pk": True,
                    "ai": True,
                    "rqd": False,
                },
                {
                    "title": "global_subject_id",
                    "column_name": "global_subject_id",
                    "uidt": "SingleLineText",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
                {
                    "title": "knumber",
                    "column_name": "knumber",
                    "uidt": "SingleLineText",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
                {
                    "title": "niddk_no",
                    "column_name": "niddk_no",
                    "uidt": "SingleLineText",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
            ]
        },
        "dna": {
            "columns": [
                {
                    "title": "Id",
                    "column_name": "Id",
                    "uidt": "ID",
                    "pk": True,
                    "ai": True,
                    "rqd": False,
                },
                {
                    "title": "global_subject_id",
                    "column_name": "global_subject_id",
                    "uidt": "SingleLineText",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
                {
                    "title": "sample_id",
                    "column_name": "sample_id",
                    "uidt": "SingleLineText",
                    "pk": False,
                    "ai": False,
                    "rqd": True,
                },
                {
                    "title": "concentration",
                    "column_name": "concentration",
                    "uidt": "Number",
                    "pk": False,
                    "ai": False,
                    "rqd": False,
                },
            ]
        },
    }

    def get_table_metadata(table_name):
        """Return schema based on table name"""
        return table_schemas.get(table_name, table_schemas["blood"])

    mock.get_table_metadata.side_effect = get_table_metadata

    # Mock get_all_records for local_id cache
    mock.get_all_records.return_value = []

    # Mock table_id
    mock.get_table_id.return_value = "table_123"

    return mock


@pytest.fixture
def mock_gsid_client():
    """Mock GSID client"""
    mock = Mock(spec=GSIDClient)

    def mock_register_batch(requests_list, batch_size=100, timeout=60):
        """Mock batch registration - matches actual API structure"""
        results = []
        for req in requests_list:
            local_subject_id = req["local_subject_id"]
            center_id = req.get("center_id", 0)

            # Simulate existing GSID for known IDs
            if "IBDGC" in local_subject_id:
                results.append(
                    {
                        "gsid": f"GSID-{local_subject_id}",
                        "local_subject_id": local_subject_id,
                        "center_id": center_id,
                        "action": "existing",
                    }
                )
            else:
                # Mint new GSID
                results.append(
                    {
                        "gsid": f"GSID-NEW-{local_subject_id}",
                        "local_subject_id": local_subject_id,
                        "center_id": center_id,
                        "action": "create_new",
                    }
                )
        return results

    mock.register_batch.side_effect = mock_register_batch
    return mock


@pytest.fixture
def validator(mock_s3_client, mock_nocodb_client, mock_gsid_client):
    """FragmentValidator instance with mocked dependencies"""
    s3_client = S3Client("test-bucket")

    # Mock the upload_dataframe method that doesn't exist yet
    s3_client.upload_dataframe = Mock()

    return FragmentValidator(s3_client, mock_nocodb_client, mock_gsid_client)


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
        "field_mapping": {
            "sample_id": "sample_id",
            "concentration": "concentration",
        },
        "subject_id_candidates": ["consortium_id"],
        "center_id_field": "center_id",
        "default_center_id": 0,
    }


# ============================================================================
# SAMPLE DATA FIXTURES
# ============================================================================
@pytest.fixture
def sample_blood_data():
    """Sample blood table data"""
    return pd.DataFrame(
        {
            "consortium_id": ["IBDGC001", "IBDGC002", "IBDGC003"],
            "sample_id": ["SMP001", "SMP002", "SMP003"],
            "sample_type": ["Blood", "Plasma", "Serum"],
            "date_collected": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )


@pytest.fixture
def sample_lcl_data():
    """Sample LCL table data"""
    return pd.DataFrame(
        {
            "consortium_id": ["IBDGC001", "IBDGC002"],
            "knumber": ["K001", "K002"],
            "niddk_no": ["NIDDK001", "NIDDK002"],
        }
    )


@pytest.fixture
def sample_dna_data():
    """Sample DNA table data"""
    return pd.DataFrame(
        {
            "consortium_id": ["IBDGC001", "IBDGC002"],
            "sample_id": ["DNA001", "DNA002"],
            "concentration": [50.5, 75.3],
            "center_id": [1, 2],
        }
    )


@pytest.fixture
def temp_csv_file(tmp_path, sample_blood_data):
    """Temporary CSV file with sample data"""
    csv_file = tmp_path / "test_data.csv"
    sample_blood_data.to_csv(csv_file, index=False)
    return csv_file


@pytest.fixture
def temp_mapping_config(tmp_path, blood_mapping_config):
    """Temporary mapping config JSON file"""
    config_file = tmp_path / "mapping_config.json"
    with open(config_file, "w") as f:
        json.dump(blood_mapping_config, f)
    return config_file
