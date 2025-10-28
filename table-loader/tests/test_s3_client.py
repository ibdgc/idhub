# table-loader/tests/test_s3_client.py
import json
from io import BytesIO

import pandas as pd
import pytest

from services.s3_client import S3Client


class TestS3Client:
    """Test S3Client functionality"""

    def test_init(self, mock_s3_client):
        """Test S3Client initialization"""
        client = S3Client()

        assert client.bucket == "test-bucket"
        assert client.s3_client is not None

    def test_list_batch_fragments(self, mock_s3_client):
        """Test listing batch fragments"""
        batch_id = "batch_20240115_120000"

        # Mock S3 response
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"staging/validated/{batch_id}/blood.csv"},
                {"Key": f"staging/validated/{batch_id}/lcl.csv"},
                {"Key": f"staging/validated/{batch_id}/validation_report.json"},
                {"Key": f"staging/validated/{batch_id}/local_subject_ids.csv"},
            ]
        }

        client = S3Client()
        fragments = client.list_batch_fragments(batch_id)

        # Should only return table fragments, not metadata files
        assert len(fragments) == 2
        assert "blood" in fragments
        assert "lcl" in fragments

    def test_list_batch_fragments_empty(self, mock_s3_client):
        """Test listing fragments when none exist"""
        mock_s3_client.list_objects_v2.return_value = {}

        client = S3Client()
        fragments = client.list_batch_fragments("batch_nonexistent")

        assert fragments == []

    def test_download_fragment(self, mock_s3_client):
        """Test downloading a fragment"""
        batch_id = "batch_20240115_120000"

        # Mock CSV data
        csv_data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001"],
                "sample_id": ["SMP001"],
            }
        ).to_csv(index=False)

        mock_s3_client.get_object.return_value = {
            "Body": BytesIO(csv_data.encode())
        }

        client = S3Client()
        fragment = client.download_fragment(batch_id, "blood")

        assert fragment["table"] == "blood"
        assert len(fragment["records"]) == 1
        assert fragment["records"][0]["global_subject_id"] == "GSID-001"

    def test_download_fragment_not_found(self, mock_s3_client):
        """Test downloading non-existent fragment"""
        from botocore.exceptions import ClientError

        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )
        mock_s3_client.exceptions.NoSuchKey = ClientError

        client = S3Client()

        with pytest.raises(ClientError):
            client.download_fragment("batch_nonexistent", "blood")

    def test_download_validation_report(self, mock_s3_client, sample_validation_report):
        """Test downloading validation report"""
        batch_id = "batch_20240115_120000"

        mock_s3_client.get_object.return_value = {
            "Body": BytesIO(json.dumps(sample_validation_report).encode())
        }

        client = S3Client()
        report = client.download_validation_report(batch_id)

        assert report["batch_id"] == batch_id
        assert report["status"] == "VALIDATED"

    def test_download_validation_report_not_found(self, mock_s3_client):
        """Test downloading non-existent validation report"""
        from botocore.exceptions import ClientError

        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )
        mock_s3_client.exceptions.NoSuchKey = ClientError

        client = S3Client()
        report = client.download_validation_report("batch_nonexistent")

        # Should return empty dict on not found
        assert report == {}

    def test_mark_fragment_loaded(self, mock_s3_client):
        """Test marking fragment as loaded"""
        batch_id = "batch_20240115_120000"

        # Mock list response
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"staging/validated/{batch_id}/blood.csv"},
                {"Key": f"staging/validated/{batch_id}/validation_report.json"},
            ]
        }

        client = S3Client()
        client.mark_fragment_loaded(batch_id, "blood")

        # Should copy and delete files
        assert mock_s3_client.copy_object.call_count == 2
        assert mock_s3_client.delete_object.call_count == 2
