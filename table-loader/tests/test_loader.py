# table-loader/tests/test_loader.py
import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from services.loader import TableLoader


@pytest.fixture
def mock_s3_client():
    """Mock S3 client"""
    with patch("services.loader.S3Client") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def sample_validation_report():
    """Sample validation report"""
    return {
        "status": "VALIDATED",
        "exclude_fields": ["identifier_type", "action", "local_subject_id"],
    }


@pytest.fixture
def mock_db_connection():
    """Mock database connection"""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cursor


class TestTableLoader:
    """Unit tests for TableLoader"""

    def test_init(self):
        """Test TableLoader initialization"""
        loader = TableLoader()
        assert loader.s3_client is not None

    def test_get_exclude_fields_from_report(
        self, mock_s3_client, sample_validation_report
    ):
        """Test loading exclude fields from validation report"""
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        loader = TableLoader()
        exclude_fields = loader._get_exclude_fields("batch_20240115_120000")

        assert "identifier_type" in exclude_fields
        assert "action" in exclude_fields

    def test_get_exclude_fields_default(self, mock_s3_client):
        """Test default exclude fields when report not found"""
        error_response = {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        }
        mock_s3_client.download_validation_report.side_effect = ClientError(
            error_response, "GetObject"
        )

        loader = TableLoader()
        exclude_fields = loader._get_exclude_fields("batch_nonexistent")

        # Should return defaults
        assert "identifier_type" in exclude_fields
        assert "action" in exclude_fields

    def test_preview_load(
        self, mock_s3_client, sample_validation_report, mock_db_connection
    ):
        """Test preview load"""
        batch_id = "batch_20240115_120000"

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

        loader = TableLoader()
        results = loader.preview_load(batch_id)

        assert "blood" in results
        # LoadStrategy returns "preview" status for dry_run=True
        assert results["blood"]["status"] == "preview"
        assert results["blood"]["rows"] == 1

    def test_preview_load_no_fragments(self, mock_s3_client):
        """Test preview with no fragments"""
        mock_s3_client.list_batch_fragments.return_value = []

        loader = TableLoader()

        with pytest.raises(ValueError, match="No table fragments found"):
            loader.preview_load("batch_empty")

    def test_execute_load(
        self, mock_s3_client, sample_validation_report, mock_db_connection
    ):
        """Test execute load"""
        batch_id = "batch_20240115_120000"
        conn, cursor = mock_db_connection

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

        with patch("services.load_strategy.db_manager") as mock_db_manager:
            mock_db_manager.get_connection.return_value.__enter__.return_value = conn
            mock_db_manager.get_connection.return_value.__exit__.return_value = False

            loader = TableLoader()
            results = loader.execute_load(batch_id)

            assert results["tables"]["blood"]["status"] == "success"
            assert results["tables"]["blood"]["rows_loaded"] == 1

    def test_execute_load_stops_on_error(self, mock_s3_client):
        """Test that execute_load stops on first error"""
        batch_id = "batch_20240115_120000"

        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"}
        ]

        # Simulate download error
        mock_s3_client.download_fragment.side_effect = Exception("Database error")

        loader = TableLoader()

        with pytest.raises(Exception, match="Database error"):
            loader.execute_load(batch_id)
