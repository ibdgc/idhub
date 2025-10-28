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
        self, mock_db_manager, mock_s3_client, 
        sample_validation_report, mock_db_connection
    ):
        """Test complete load workflow from S3 to database"""
        batch_id = "batch_20240115_120000"
        conn, cursor = mock_db_connection

        # Setup S3 mocks
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"staging/validated/{batch_id}/blood.csv"},
                {"Key": f"staging/validated/{batch_id}/validation_report.json"},
            ]
        }

        # Mock CSV data
        csv_data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP002"],
                "sample_type": ["Blood", "Serum"],
                "consortium_id": ["ID001", "ID002"],  # Should be excluded
            }
        ).to_csv(index=False)

        def mock_get_object(Bucket, Key):
            if "validation_report.json" in Key:
                return {"Body": BytesIO(json.dumps(sample_validation_report).encode())}
            else:
                return {"Body": BytesIO(csv_data.encode())}

        mock_s3_client.get_object.side_effect = mock_get_object

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
        assert mock_s3_client.copy_object.called
        assert mock_s3_client.delete_object.called

    def test_preview_workflow(self, mock_s3_client, sample_validation_report):
        """Test preview workflow without database operations"""
        batch_id = "batch_20240115_120000"

        # Setup S3 mocks
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"staging/validated/{batch_id}/blood.csv"},
                {"Key": f"staging/validated/{batch_id}/validation_report.json"},
            ]
        }

        csv_data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001"],
                "sample_id": ["SMP001"],
            }
        ).to_csv(index=False)

        def mock_get_object(Bucket, Key):
            if "validation_report.json" in Key:
                return {"Body": BytesIO(json.dumps(sample_validation_report).encode())}
            else:
                return {"Body": BytesIO(csv_data.encode())}

        mock_s3_client.get_object.side_effect = mock_get_object

        # Execute preview
        loader = TableLoader()
        results = loader.preview_load(batch_id)

        # Verify preview results
        assert "blood" in results
        assert results["blood"]["status"] == "preview"
        assert results["blood"]["rows"] == 1
