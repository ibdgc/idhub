import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from services.s3_client import S3Client


@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client"""
    with patch("boto3.client") as mock_client:
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        yield mock_s3


class TestS3Client:
    """Unit tests for S3Client"""

    def test_init(self, mock_s3_client):
        """Test S3Client initialization"""
        client = S3Client("test-bucket")
        assert client.bucket == "test-bucket"
        assert client.s3_client is not None

    def test_list_batch_fragments(self, mock_s3_client):
        """Test listing batch fragments"""
        batch_id = "batch_20240115_120000"

        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"staging/validated/{batch_id}/blood.csv"},
                {"Key": f"staging/validated/{batch_id}/lcl.csv"},
                {"Key": f"staging/validated/{batch_id}/local_subject_ids.csv"},
                {"Key": f"staging/validated/{batch_id}/validation_report.json"},
            ]
        }

        client = S3Client("test-bucket")
        fragments = client.list_batch_fragments(batch_id)

        # Should only return CSV files (excluding validation_report.json)
        assert len(fragments) == 3
        assert all(f["Key"].endswith(".csv") for f in fragments)

    def test_list_batch_fragments_empty(self, mock_s3_client):
        """Test listing when no fragments exist"""
        mock_s3_client.list_objects_v2.return_value = {}

        client = S3Client("test-bucket")
        fragments = client.list_batch_fragments("batch_nonexistent")

        assert fragments == []

    def test_download_fragment(self, mock_s3_client):
        """Test downloading a fragment"""
        csv_data = pd.DataFrame(
            {"global_subject_id": ["GSID-001"], "sample_id": ["SMP001"]}
        ).to_csv(index=False)

        mock_s3_client.get_object.return_value = {
            "Body": BytesIO(csv_data.encode())
        }

        client = S3Client("test-bucket")
        df = client.download_fragment("batch_20240115_120000", "blood")

        assert len(df) == 1
        assert "global_subject_id" in df.columns
        assert df.iloc[0]["global_subject_id"] == "GSID-001"

    def test_download_fragment_not_found(self, mock_s3_client):
        """Test downloading non-existent fragment"""
        error_response = {
            'Error': {
                'Code': 'NoSuchKey',
                'Message': 'The specified key does not exist.'
            }
        }
        mock_s3_client.get_object.side_effect = ClientError(error_response, 'GetObject')

        client = S3Client("test-bucket")

        with pytest.raises(FileNotFoundError, match="Fragment not found"):
            client.download_fragment("batch_nonexistent", "blood")

    def test_download_validation_report(self, mock_s3_client):
        """Test downloading validation report"""
        report_data = {"status": "VALIDATED", "row_count": 100}

        mock_s3_client.get_object.return_value = {
            "Body": BytesIO(json.dumps(report_data).encode())
        }

        client = S3Client("test-bucket")
        report = client.download_validation_report("batch_20240115_120000")

        assert report["status"] == "VALIDATED"
        assert report["row_count"] == 100

    def test_download_validation_report_not_found(self, mock_s3_client):
        """Test downloading non-existent validation report"""
        error_response = {
            'Error': {
                'Code': 'NoSuchKey',
                'Message': 'The specified key does not exist.'
            }
        }
        mock_s3_client.get_object.side_effect = ClientError(error_response, 'GetObject')

        client = S3Client("test-bucket")

        with pytest.raises(FileNotFoundError, match="File not found"):
            client.download_validation_report("batch_nonexistent")

    def test_mark_fragment_loaded(self, mock_s3_client):
        """Test marking fragment as loaded"""
        client = S3Client("test-bucket")
        client.mark_fragment_loaded("batch_20240115_120000", "blood")

        # Should call copy_object once and delete_object once
        assert mock_s3_client.copy_object.call_count == 1
        assert mock_s3_client.delete_object.call_count == 1

        # Verify copy was to correct location
        copy_call = mock_s3_client.copy_object.call_args
        assert "staging/loaded/" in copy_call.kwargs["Key"]
