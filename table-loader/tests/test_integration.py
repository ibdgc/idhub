# table-loader/tests/test_integration.py
import json
from io import BytesIO
from unittest.mock import patch

import pandas as pd
import pytest
from services.loader import TableLoader


class TestIntegration:
    """Integration tests for complete load workflow"""

    @patch("services.load_strategy.db_manager")  # Patch where load_strategy imports it
    def test_full_load_workflow(
        self,
        mock_db_manager,
        mock_s3_client,
        sample_validation_report,
        mock_db_connection,
    ):
        """Test complete load workflow from S3 to database"""
        batch_id = "batch_20240115_120000"
        conn, cursor = mock_db_connection

        # Setup S3 mocks
        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"},
        ]

        # Mock CSV data with actual records
        csv_data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP002"],
                "sample_type": ["Blood", "Serum"],
                "consortium_id": ["ID001", "ID002"],  # Should be excluded
            }
        )

        mock_s3_client.download_fragment.return_value = csv_data
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        # Configure db_manager mock
        mock_db_manager.get_connection.return_value.__enter__.return_value = conn
        mock_db_manager.get_connection.return_value.__exit__.return_value = False

        # Execute load
        loader = TableLoader()
        results = loader.execute_load(batch_id)

        # Verify results
        assert results["batch_id"] == batch_id
        assert "blood" in results["tables"]
        assert results["tables"]["blood"]["status"] == "success"

        # Verify bulk_insert was called (StandardLoadStrategy uses this)
        mock_db_manager.bulk_insert.assert_called()

        # Verify S3 operations
        mock_s3_client.mark_fragment_loaded.assert_called_with(batch_id, "blood")

    def test_preview_workflow(self, mock_s3_client, sample_validation_report):
        """Test preview workflow without database operations"""
        batch_id = "batch_20240115_120000"

        # Setup S3 mocks
        mock_s3_client.list_batch_fragments.return_value = [
            {"Key": f"staging/validated/{batch_id}/blood.csv"},
        ]

        csv_data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001"],
                "sample_id": ["SMP001"],
                "sample_type": ["Blood"],
            }
        )

        mock_s3_client.download_fragment.return_value = csv_data
        mock_s3_client.download_validation_report.return_value = (
            sample_validation_report
        )

        # Execute preview
        loader = TableLoader()
        results = loader.preview_load(batch_id)

        # Verify preview results
        assert "blood" in results
        assert results["blood"]["status"] == "preview"
        assert results["blood"]["rows"] == 1
