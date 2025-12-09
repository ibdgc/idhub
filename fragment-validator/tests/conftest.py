# fragment-validator/tests/conftest.py
import io
import json
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from services import (
    CenterResolver,
    FragmentValidator,
    GSIDClient,
    NocoDBClient,
    S3Client,
    SubjectIDResolver,
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

    def get_all_records(table_name):
        if table_name == "centers":
            return [
                {"center_id": 1, "name": "MSSM"},
                {"center_id": 2, "name": "Cedars-Sinai"},
            ]
        return []

    # Mock get_all_records for local_id cache
    mock.get_all_records.side_effect = get_all_records

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
            # Simulate a simple GSID generation based on the first identifier
            first_identifier = req["identifiers"][0]["local_subject_id"]
            center_id = req.get("center_id", 0)

            # Simulate existing GSID for known IDs
            if "IBDGC" in first_identifier:
                results.append(
                    {
                        "gsid": f"GSID-{first_identifier}",
                        "action": "link_existing",
                        "identifiers_linked": len(req["identifiers"]),
                        "identifiers": req["identifiers"],
                    }
                )
            else:
                # Mint new GSID
                results.append(
                    {
                        "gsid": f"GSID-NEW-{first_identifier}",
                        "action": "create_new",
                        "identifiers_linked": len(req["identifiers"]),
                        "identifiers": req["identifiers"],
                    }
                )
        return results

    mock.register_batch.side_effect = mock_register_batch
    return mock


@pytest.fixture
def center_resolver(mock_nocodb_client):
    """Create a CenterResolver instance with a mocked NocoDB client."""
    return CenterResolver(mock_nocodb_client)


@pytest.fixture
def validator(
    mock_s3_client, mock_nocodb_client, mock_gsid_client, center_resolver
):
    """FragmentValidator instance with mocked dependencies"""
    s3_client = S3Client("test-bucket")
    s3_client.upload_dataframe = Mock()

    subject_id_resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

    return FragmentValidator(s3_client, mock_nocodb_client, subject_id_resolver)
@pytest.fixture
def sample_blood_data():
    """Sample blood data as a DataFrame"""
    data = {
        "consortium_id": ["IBDGC001", "IBDGC002", "IBDGC003"],
        "sample_id": ["BS-001", "BS-002", "BS-003"],
        "sample_type": ["Blood", "Blood", "Blood"],
        "date_collected": ["2024-01-01", "2024-01-02", "2024-01-03"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def blood_mapping_config():
    """Sample mapping config for blood data"""
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
def sample_lcl_data():
    """Sample LCL data as a DataFrame"""
    data = {
        "knumber": ["K123", "K456"],
        "niddk_no": ["NIDDK-1", "NIDDK-2"],
    }
    return pd.DataFrame(data)

@pytest.fixture
def lcl_mapping_config():
    """Sample mapping config for LCL data"""
    return {
        "field_mapping": {
            "knumber": "knumber",
            "niddk_no": "niddk_no",
        },
        "subject_id_candidates": ["knumber", "niddk_no"],
    }

@pytest.fixture
def sample_dna_data():
    """Sample DNA data as a DataFrame"""
    data = {
        "sample_id": ["DNA-001", "DNA-002"],
        "concentration": [10.5, 20.2],
        "center": ["MSSM", "Cedars-Sinai"],
    }
    return pd.DataFrame(data)

@pytest.fixture
def dna_mapping_config():
    """Sample mapping config for DNA data"""
    return {
        "field_mapping": {
            "sample_id": "sample_id",
            "concentration": "concentration",
        },
        "subject_id_candidates": ["sample_id"],
        "center_id_field": "center",
    }