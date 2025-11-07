# fragment-validator/tests/test_s3_client.py
import io
import json
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from services.s3_client import S3Client


class TestS3Client:
    """Unit tests for S3Client"""

    @pytest.fixture
    def mock_s3_client(self):
        """Mock boto3 S3 client"""
        with patch("boto3.client") as mock_boto:
            mock_client = Mock()
            mock_boto.return_value = mock_client
            yield mock_client

    def test_upload_json(self, mock_s3_client):
        """Test uploading JSON to S3"""
        client = S3Client("test-bucket")
        test_data = {"key": "value", "count": 123}
        client.upload_json(test_data, "test/report.json")

        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args.kwargs["Key"] == "test/report.json"
        assert '"key": "value"' in call_args.kwargs["Body"]

    def test_list_batch_fragments(self, mock_s3_client):
        """Test listing batch fragments"""
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "staging/validated/batch1/blood.csv"},
                {"Key": "staging/validated/batch1/lcl.csv"},
                {"Key": "staging/validated/batch1/validation_report.json"},
            ]
        }

        client = S3Client("test-bucket")
        fragments = client.list_batch_fragments("batch1")

        # Should only return CSV files, not validation_report.json
        assert len(fragments) == 2
        assert all(f["Key"].endswith(".csv") for f in fragments)

    def test_list_batch_fragments_empty(self, mock_s3_client):
        """Test listing fragments with no results"""
        mock_s3_client.list_objects_v2.return_value = {}

        client = S3Client("test-bucket")
        fragments = client.list_batch_fragments("batch1")

        assert fragments == []

    def test_download_fragment(self, mock_s3_client):
        """Test downloading a table fragment"""
        csv_data = "global_subject_id,sample_id\nGSID-001,SMP001\n"
        mock_s3_client.get_object.return_value = {"Body": io.BytesIO(csv_data.encode())}

        client = S3Client("test-bucket")
        df = client.download_fragment("batch1", "blood")

        assert len(df) == 1
        assert list(df.columns) == ["global_subject_id", "sample_id"]
        assert df.iloc[0]["global_subject_id"] == "GSID-001"

    def test_download_fragment_not_found(self, mock_s3_client):
        """Test downloading non-existent fragment"""
        error_response = {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        }
        mock_s3_client.get_object.side_effect = ClientError(error_response, "GetObject")

        client = S3Client("test-bucket")
        with pytest.raises(FileNotFoundError):
            client.download_fragment("batch1", "blood")

    def test_download_fragment_empty_csv(self, mock_s3_client):
        """Test downloading empty CSV file"""
        mock_s3_client.get_object.return_value = {"Body": io.BytesIO(b"")}

        client = S3Client("test-bucket")
        df = client.download_fragment("batch1", "blood")

        # Should return empty DataFrame
        assert len(df) == 0

    def test_download_validation_report(self, mock_s3_client):
        """Test downloading validation report"""
        report_data = {
            "status": "VALIDATED",
            "table_name": "blood",
            "exclude_fields": ["consortium_id"],
        }
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(report_data).encode())
        }

        client = S3Client("test-bucket")
        report = client.download_validation_report("batch1")

        assert report["status"] == "VALIDATED"
        assert report["table_name"] == "blood"

    def test_download_validation_report_not_found(self, mock_s3_client):
        """Test downloading non-existent validation report"""
        error_response = {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        }
        mock_s3_client.get_object.side_effect = ClientError(error_response, "GetObject")

        client = S3Client("test-bucket")
        with pytest.raises(FileNotFoundError):
            client.download_validation_report("batch1")

    def test_download_validation_report_invalid_json(self, mock_s3_client):
        """Test downloading malformed validation report"""
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b"{ invalid json }")
        }

        client = S3Client("test-bucket")
        with pytest.raises(json.JSONDecodeError):
            client.download_validation_report("batch1")

    def test_mark_fragment_loaded(self, mock_s3_client):
        """Test marking fragment as loaded"""
        client = S3Client("test-bucket")
        client.mark_fragment_loaded("batch1", "blood")

        # Should copy and then delete
        mock_s3_client.copy_object.assert_called_once()
        mock_s3_client.delete_object.assert_called_once()

        # Verify copy arguments
        copy_args = mock_s3_client.copy_object.call_args
        assert copy_args.kwargs["Bucket"] == "test-bucket"
        assert copy_args.kwargs["Key"] == "staging/loaded/batch1/blood.csv"
        assert (
            copy_args.kwargs["CopySource"]["Key"]
            == "staging/validated/batch1/blood.csv"
        )

    def test_bucket_attribute(self):
        """Test that bucket is stored correctly"""
        client = S3Client("my-test-bucket")
        assert client.bucket == "my-test-bucket"
