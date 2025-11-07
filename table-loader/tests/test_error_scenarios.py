# table-loader/tests/test_error_scenarios.py
import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError, ConnectTimeoutError
from services.loader import TableLoader


class TestErrorScenarios:
    """Test error handling and recovery"""

    def test_s3_connection_timeout(self, mock_s3_client):
        """Test handling of S3 connection timeout"""
        mock_s3_client.list_batch_fragments.side_effect = ConnectTimeoutError(
            endpoint_url="https://s3.amazonaws.com"
        )

        loader = TableLoader()

        with pytest.raises(ConnectTimeoutError):
            loader.preview_load("batch_123")

    def test_s3_client_error(self, mock_s3_client):
        """Test handling of S3 ClientError"""
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        mock_s3_client.list_batch_fragments.side_effect = ClientError(
            error_response, "ListObjectsV2"
        )

        loader = TableLoader()

        with pytest.raises(ClientError):
            loader.preview_load("batch_123")

    def test_database_connection_failure(
        self, mock_s3_client, sample_validation_report
    ):
        """Test handling of database connection failure"""
        batch_id = "batch_20240115_120000"

        # Setup S3 mocks to return valid data
        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"}
        ]

        csv_data = pd.DataFrame(
            {"global_subject_id": ["GSID-001"], "sample_id": ["SMP001"]}
        )
        mock_s3_client.download_fragment.return_value = csv_data
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        # Mock database connection to fail
        with patch("services.load_strategy.db_manager") as mock_db_manager:
            mock_db_manager.get_connection.side_effect = Exception("Connection refused")

            loader = TableLoader()

            with pytest.raises(Exception, match="Connection refused"):
                loader.execute_load(batch_id)

    def test_partial_batch_failure_rollback(self):
        """Test that partial batch failures don't leave partial data"""
        # This would require actual transaction testing
        # Placeholder for future implementation
        pass

    def test_file_not_found_error(self, mock_boto3_s3):
        """Test handling of missing S3 file"""
        error_response = {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        }
        mock_boto3_s3.get_object.side_effect = ClientError(error_response, "GetObject")

        from services.s3_client import S3Client

        s3_client = S3Client("test-bucket")

        with pytest.raises(FileNotFoundError):
            s3_client.download_fragment("batch_123", "blood")

    def test_invalid_json_in_validation_report(self, mock_boto3_s3):
        """Test handling of malformed validation report JSON"""
        # Return invalid JSON
        mock_boto3_s3.get_object.return_value = {"Body": BytesIO(b"{ invalid json }")}

        from services.s3_client import S3Client

        s3_client = S3Client("test-bucket")

        with pytest.raises(json.JSONDecodeError):
            s3_client.download_validation_report("batch_123")
