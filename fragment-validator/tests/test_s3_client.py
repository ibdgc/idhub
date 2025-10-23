# fragment-validator/tests/test_s3_client.py
from unittest.mock import MagicMock

import pytest


class TestS3Client:
    def test_list_files(self, mock_s3):
        """Test listing S3 files"""
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "file1.csv"}, {"Key": "file2.csv"}]
        }

        result = mock_s3.list_objects_v2(Bucket="test-bucket")
        assert len(result["Contents"]) == 2
        assert result["Contents"][0]["Key"] == "file1.csv"

    def test_download_file(self, mock_s3):
        """Test downloading file from S3"""
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b"test content")
        }

        result = mock_s3.get_object(Bucket="test-bucket", Key="test.csv")
        content = result["Body"].read()
        assert content == b"test content"

    def test_upload_file(self, mock_s3):
        """Test uploading file to S3"""
        mock_s3.put_object.return_value = {"ETag": "test-etag"}

        result = mock_s3.put_object(
            Bucket="test-bucket", Key="test.csv", Body=b"test content"
        )
        assert "ETag" in result
