# redcap-pipeline/tests/test_s3_uploader.py
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError


class TestS3Uploader:
    """Test S3Uploader functionality"""

    @pytest.fixture
    def mock_s3_client(self):
        """Mock boto3 S3 client"""
        with patch("boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3
            yield mock_s3

    @pytest.fixture
    def sample_fragment(self):
        """Sample curated fragment"""
        return {
            "gsid": "GSID-TEST123456789",
            "local_subject_id": "TEST001",
            "center_id": 1,
            "project": "test_project",
            "data": {
                "age": 25,
                "diagnosis": "CD",
            },
        }

    def test_s3_uploader_init(self, mock_s3_client):
        """Test S3Uploader initialization"""
        from services.s3_uploader import S3Uploader

        uploader = S3Uploader()

        assert uploader.s3_client is not None
        assert uploader.bucket == "test-bucket"  # From conftest.py env

    def test_upload_fragment_success(self):
        """Test successful fragment upload"""
        from services.s3_uploader import S3Uploader

        with patch("services.s3_uploader.boto3.client") as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock datetime
            with patch("services.s3_uploader.datetime") as mock_datetime:
                mock_now = MagicMock()
                mock_now.strftime.return_value = "20240115_103045"
                mock_datetime.utcnow.return_value = mock_now

                uploader = S3Uploader()
                fragment_data = {"test": "data"}

                # Use correct parameter order: fragment, project_key, gsid
                key = uploader.upload_fragment(
                    fragment=fragment_data,
                    project_key="test_project",
                    gsid="GSID-TEST123456789",
                )

                # Verify S3 put_object was called
                mock_s3.put_object.assert_called_once()
                call_kwargs = mock_s3.put_object.call_args[1]

                assert call_kwargs["Bucket"] == "test-bucket"
                assert "GSID-TEST123456789" in call_kwargs["Key"]
                assert call_kwargs["ContentType"] == "application/json"
                assert (
                    key
                    == "subjects/GSID-TEST123456789/test_project_20240115_103045.json"
                )

    def test_upload_fragment_formats_json_with_indent(
        self, mock_s3_client, sample_fragment
    ):
        """Test that uploaded JSON is formatted with indentation"""
        from services.s3_uploader import S3Uploader

        uploader = S3Uploader()
        uploader.upload_fragment(
            fragment=sample_fragment, project_key="test_project", gsid="GSID-TEST123"
        )

        call_kwargs = mock_s3_client.put_object.call_args[1]
        body = call_kwargs["Body"]

        # Verify JSON has indentation (pretty-printed)
        assert "\n" in body
        assert "  " in body  # 2-space indent

    def test_upload_fragment_key_structure(self, mock_s3_client, sample_fragment):
        """Test S3 key follows correct structure"""
        from services.s3_uploader import S3Uploader

        uploader = S3Uploader()

        with patch("services.s3_uploader.datetime") as mock_datetime:
            mock_datetime.utcnow.return_value.strftime.return_value = "20240115_103045"

            key = uploader.upload_fragment(
                fragment=sample_fragment, project_key="gap", gsid="GSID-ABC123"
            )

        # Verify key structure: subjects/{gsid}/{project}_{timestamp}.json
        assert key.startswith("subjects/GSID-ABC123/")
        assert "gap_20240115_103045.json" in key
        assert key.endswith(".json")

    def test_upload_fragment_client_error(self, mock_s3_client, sample_fragment):
        """Test handling of S3 ClientError"""
        from services.s3_uploader import S3Uploader

        # Simulate S3 error
        error_response = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
            }
        }
        mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

        uploader = S3Uploader()

        with pytest.raises(ClientError):
            uploader.upload_fragment(
                fragment=sample_fragment,
                project_key="test_project",
                gsid="GSID-TEST123",
            )

    def test_upload_fragment_access_denied_error(self, mock_s3_client, sample_fragment):
        """Test handling of S3 access denied error"""
        from services.s3_uploader import S3Uploader

        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

        uploader = S3Uploader()

        with pytest.raises(ClientError) as exc_info:
            uploader.upload_fragment(
                fragment=sample_fragment,
                project_key="test_project",
                gsid="GSID-TEST123",
            )

        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"

    def test_upload_fragment_with_special_characters_in_gsid(
        self, mock_s3_client, sample_fragment
    ):
        """Test upload with special characters in GSID"""
        from services.s3_uploader import S3Uploader

        uploader = S3Uploader()

        # GSID with hyphens and numbers
        gsid = "GSID-TEST-123-ABC-456"

        key = uploader.upload_fragment(
            fragment=sample_fragment, project_key="test_project", gsid=gsid
        )

        assert f"subjects/{gsid}/" in key

    def test_upload_fragment_timestamp_format(self, mock_s3_client, sample_fragment):
        """Test timestamp format in S3 key"""
        from services.s3_uploader import S3Uploader

        uploader = S3Uploader()

        with patch("services.s3_uploader.datetime") as mock_datetime:
            # Mock specific datetime
            mock_dt = MagicMock()
            mock_dt.strftime.return_value = "20240315_143022"
            mock_datetime.utcnow.return_value = mock_dt

            key = uploader.upload_fragment(
                fragment=sample_fragment, project_key="test_project", gsid="GSID-TEST"
            )

        # Verify timestamp format: YYYYMMDD_HHMMSS
        assert "20240315_143022" in key
        mock_dt.strftime.assert_called_once_with("%Y%m%d_%H%M%S")

    def test_upload_fragment_uses_utc_time(self, mock_s3_client, sample_fragment):
        """Test that uploader uses UTC time"""
        from services.s3_uploader import S3Uploader

        uploader = S3Uploader()

        with patch("services.s3_uploader.datetime") as mock_datetime:
            mock_datetime.utcnow.return_value.strftime.return_value = "20240115_103045"

            uploader.upload_fragment(
                fragment=sample_fragment, project_key="test_project", gsid="GSID-TEST"
            )

        # Verify utcnow was called (not now)
        mock_datetime.utcnow.assert_called()

    def test_upload_fragment_encryption_enabled(self, mock_s3_client, sample_fragment):
        """Test that server-side encryption is enabled"""
        from services.s3_uploader import S3Uploader

        uploader = S3Uploader()
        uploader.upload_fragment(
            fragment=sample_fragment, project_key="test_project", gsid="GSID-TEST"
        )

        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["ServerSideEncryption"] == "AES256"

    def test_upload_fragment_returns_key(self, mock_s3_client, sample_fragment):
        """Test that upload_fragment returns the S3 key"""
        from services.s3_uploader import S3Uploader

        uploader = S3Uploader()

        key = uploader.upload_fragment(
            fragment=sample_fragment, project_key="test_project", gsid="GSID-TEST123"
        )

        assert isinstance(key, str)
        assert key.startswith("subjects/")
        assert key.endswith(".json")
