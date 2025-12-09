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
    def mock_s3_boto_client(self):
        """Mock boto3 S3 client"""
        with patch("boto3.client") as mock_boto:
            mock_client = Mock()
            mock_boto.return_value = mock_client
            yield mock_client

    def test_upload_dataframe(self, mock_s3_boto_client):
        """Test uploading a DataFrame to S3"""
        client = S3Client("test-bucket")
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        client.upload_dataframe(df, "test/data.csv")

        mock_s3_boto_client.put_object.assert_called_once()
        call_args = mock_s3_boto_client.put_object.call_args
        assert call_args.kwargs["Key"] == "test/data.csv"
        assert "col1,col2\n1,a\n2,b\n" in call_args.kwargs["Body"]

    def test_upload_json(self, mock_s3_boto_client):
        """Test uploading JSON to S3"""
        client = S3Client("test-bucket")
        test_data = {"key": "value", "count": 123}
        client.upload_json(test_data, "test/report.json")

        mock_s3_boto_client.put_object.assert_called_once()
        call_args = mock_s3_boto_client.put_object.call_args
        assert call_args.kwargs["Key"] == "test/report.json"
        assert '"key": "value"' in call_args.kwargs["Body"]

    def test_download_dataframe(self, mock_s3_boto_client):
        """Test downloading a DataFrame from S3"""
        csv_data = "col1,col2\nval1,val2\n"
        mock_s3_boto_client.get_object.return_value = {"Body": io.BytesIO(csv_data.encode())}

        client = S3Client("test-bucket")
        df = client.download_dataframe("test/data.csv")

        assert len(df) == 1
        assert list(df.columns) == ["col1", "col2"]
        assert df.iloc[0]["col1"] == "val1"

    def test_download_dataframe_not_found(self, mock_s3_boto_client):
        """Test downloading non-existent DataFrame"""
        error_response = {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        }
        mock_s3_boto_client.get_object.side_effect = ClientError(error_response, "GetObject")

        client = S3Client("test-bucket")
        with pytest.raises(ClientError):
            client.download_dataframe("test/nonexistent.csv")

    def test_bucket_attribute(self):
        """Test that bucket is stored correctly"""
        client = S3Client("my-test-bucket")
        assert client.bucket == "my-test-bucket"
