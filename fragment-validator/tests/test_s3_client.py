# fragment-validator/tests/test_s3_client.py
import pandas as pd
import pytest
from services.s3_client import S3Client


class TestS3Client:
    """Unit tests for S3Client"""

    def test_init(self, mock_s3_client):
        """Test S3Client initialization"""
        client = S3Client("test-bucket")
        assert client.bucket == "test-bucket"
        assert client.client is not None

    def test_upload_dataframe(self, mock_s3_client, sample_blood_data):
        """Test uploading DataFrame to S3"""
        client = S3Client("test-bucket")
        client.upload_dataframe(sample_blood_data, "test/path.csv")

        # Verify put_object was called
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args.kwargs["Bucket"] == "test-bucket"
        assert call_args.kwargs["Key"] == "test/path.csv"
        assert isinstance(call_args.kwargs["Body"], str)

    def test_upload_json(self, mock_s3_client):
        """Test uploading JSON to S3"""
        client = S3Client("test-bucket")
        test_data = {"key": "value", "count": 123}
        client.upload_json(test_data, "test/report.json")

        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args.kwargs["Key"] == "test/report.json"
        assert '"key": "value"' in call_args.kwargs["Body"]

    def test_download_dataframe(self, mock_s3_client):
        """Test downloading CSV from S3"""
        # The mock now returns io.BytesIO which pandas can read
        client = S3Client("test-bucket")
        df = client.download_dataframe("test/data.csv")

        assert len(df) == 1  # Updated expectation
        assert list(df.columns) == ["col1", "col2"]
        assert df.iloc[0]["col1"] == "val1"

    def test_list_objects(self, mock_s3_client):
        """Test listing S3 objects"""
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "prefix/file1.csv"},
                {"Key": "prefix/file2.csv"},
            ]
        }

        client = S3Client("test-bucket")
        objects = client.list_objects("prefix/")

        assert len(objects) == 2
        assert objects[0]["Key"] == "prefix/file1.csv"
