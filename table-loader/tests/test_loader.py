# table-loader/tests/test_loader.py
"""Comprehensive tests for TableLoader"""

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from services.loader import TableLoader


class TestTableLoader:
    """Comprehensive tests for TableLoader"""

    def test_init(self, mock_s3_client):
        """Test TableLoader initialization"""
        loader = TableLoader()
        assert loader.s3_client is not None

    def test_get_exclude_fields_from_report(
        self, mock_s3_client, sample_validation_report
    ):
        """Test extracting exclude fields from validation report"""
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        loader = TableLoader()
        exclude_fields = loader._get_exclude_fields("batch_123")

        assert "consortium_id" in exclude_fields
        assert "identifier_type" in exclude_fields
        assert "action" in exclude_fields

    def test_get_exclude_fields_default(self, mock_s3_client):
        """Test default exclude fields when report missing"""
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
        exclude_fields = loader._get_exclude_fields("batch_123")

        # Should return default exclusions
        assert isinstance(exclude_fields, set)
        assert "identifier_type" in exclude_fields
        assert "action" in exclude_fields

    def test_get_exclude_fields_with_missing_report(self, mock_s3_client):
        """Test exclude fields fallback when report is missing"""
        batch_id = "batch_nonexistent"
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
        exclude_fields = loader._get_exclude_fields(batch_id)

        # Should return default exclusions
        assert "identifier_type" in exclude_fields
        assert "action" in exclude_fields
        assert "local_subject_id" in exclude_fields

    def test_preview_load(self, mock_s3_client, sample_validation_report):
        """Test preview load without database operations"""
        batch_id = "batch_20240115_120000"

        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"}
        ]

        df = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP1", "SMP2"],
            }
        )
        mock_s3_client.download_fragment.return_value = df
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        loader = TableLoader()
        results = loader.preview_load(batch_id)

        assert "blood" in results
        assert results["blood"]["status"] == "preview"
        assert results["blood"]["rows"] == 2

    def test_preview_load_no_fragments(self, mock_s3_client):
        """Test preview load with no fragments"""
        mock_s3_client.list_batch_fragments.return_value = []

        loader = TableLoader()

        with pytest.raises(ValueError, match="No table fragments found"):
            loader.preview_load("batch_123")

    def test_preview_with_malformed_csv(self, mock_s3_client, sample_validation_report):
        """Test preview with empty CSV data (handled gracefully)"""
        batch_id = "batch_20240115_120000"

        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"},
        ]

        # Return empty DataFrame (S3Client handles EmptyDataError and returns empty DataFrame)
        mock_s3_client.download_fragment.return_value = pd.DataFrame()
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        loader = TableLoader()
        results = loader.preview_load(batch_id)

        # Should handle empty data gracefully with "skipped" status
        assert "blood" in results
        assert results["blood"]["status"] == "skipped"
        assert results["blood"]["reason"] == "no records"

    @patch("services.load_strategy.db_manager")
    def test_execute_load(
        self,
        mock_db_manager,
        mock_s3_client,
        sample_validation_report,
        mock_db_connection,
    ):
        """Test execute load with database operations"""
        batch_id = "batch_20240115_120000"
        conn, cursor = mock_db_connection

        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"}
        ]

        df = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001"],
                "sample_id": ["SMP1"],
                "consortium_id": ["ID001"],
            }
        )
        mock_s3_client.download_fragment.return_value = df
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        mock_db_manager.get_connection.return_value.__enter__.return_value = conn
        mock_db_manager.get_connection.return_value.__exit__.return_value = False

        loader = TableLoader()
        results = loader.execute_load(batch_id)

        assert results["batch_id"] == batch_id
        assert "blood" in results["tables"]
        assert results["tables"]["blood"]["status"] == "success"

    def test_execute_load_stops_on_error(
        self, mock_s3_client, sample_validation_report
    ):
        """Test that execute_load stops on first table error"""
        batch_id = "batch_20240115_120000"

        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"},
            {"Key": f"staging/validated/{batch_id}/lcl.csv"},
        ]

        # First table will fail with FileNotFoundError
        error_response = {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        }
        mock_s3_client.download_fragment.side_effect = ClientError(
            error_response, "GetObject"
        )
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        loader = TableLoader()

        # Should raise ClientError (which S3Client converts to FileNotFoundError)
        with pytest.raises((FileNotFoundError, ClientError)):
            loader.execute_load(batch_id)

    @patch("services.load_strategy.db_manager")
    def test_execute_load_multiple_tables(
        self,
        mock_db_manager,
        mock_s3_client,
        sample_validation_report,
        mock_db_connection,
    ):
        """Test loading multiple tables in sequence"""
        batch_id = "batch_20240115_120000"
        conn, cursor = mock_db_connection

        # Mock S3 to return multiple table fragments
        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"},
            {"Key": f"staging/validated/{batch_id}/lcl.csv"},
            {"Key": f"staging/validated/{batch_id}/dna.csv"},
        ]

        # Mock CSV data for different tables
        def mock_download_fragment(batch_id, table_name):
            if table_name == "blood":
                return pd.DataFrame(
                    {"global_subject_id": ["GSID-001"], "sample_id": ["SMP001"]}
                )
            elif table_name == "lcl":
                return pd.DataFrame(
                    {"global_subject_id": ["GSID-001"], "knumber": ["K001"]}
                )
            elif table_name == "dna":
                return pd.DataFrame(
                    {"global_subject_id": ["GSID-001"], "dna_id": ["DNA001"]}
                )

        mock_s3_client.download_fragment.side_effect = mock_download_fragment
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        mock_db_manager.get_connection.return_value.__enter__.return_value = conn
        mock_db_manager.get_connection.return_value.__exit__.return_value = False

        loader = TableLoader()
        results = loader.execute_load(batch_id)

        # Should load all three tables
        assert len(results["tables"]) == 3
        assert "blood" in results["tables"]
        assert "lcl" in results["tables"]
        assert "dna" in results["tables"]

        # All should succeed
        assert all(t["status"] == "success" for t in results["tables"].values())

    @patch("services.load_strategy.db_manager")
    def test_idempotency_running_same_batch_twice(
        self,
        mock_db_manager,
        mock_s3_client,
        sample_validation_report,
        mock_db_connection,
    ):
        """Test that running the same batch twice is handled correctly"""
        batch_id = "batch_20240115_120000"
        conn, cursor = mock_db_connection

        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"},
        ]

        csv_data = pd.DataFrame(
            {"global_subject_id": ["GSID-001"], "sample_id": ["SMP001"]}
        )
        mock_s3_client.download_fragment.return_value = csv_data
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        mock_db_manager.get_connection.return_value.__enter__.return_value = conn
        mock_db_manager.get_connection.return_value.__exit__.return_value = False

        loader = TableLoader()

        # First load
        results1 = loader.execute_load(batch_id)
        assert results1["tables"]["blood"]["status"] == "success"

        # Reset mocks
        mock_s3_client.reset_mock()
        mock_s3_client.list_batch_fragments.return_value = []  # Batch already processed

        # Second load should find no fragments
        with pytest.raises(ValueError, match="No table fragments found"):
            loader.execute_load(batch_id)
