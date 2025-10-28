# table-loader/tests/test_integration.py
import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from services.loader import TableLoader


class TestIntegration:
    """Integration tests for complete load workflow"""

    @patch("core.database.db_manager")
    def test_full_load_workflow(
        self, mock_db_manager, mock_s3_client, sample_validation_report
    ):
        """Test complete load workflow from S3 to database"""
        batch_id = "batch_20240115_120000"

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

        # Mock database
        mock_conn = MagicMock()
        mock_db_manager.get_connection.return_value.__enter__.return_value = mock_conn

        # Execute load
        loader = TableLoader()

        with patch.object(loader, "_get_exclude_fields", return_value={"consortium_id"}):
            results = loader.execute_load(batch_id)

        # Verify results
        assert results["status"] == "success"
        assert results["batch_id"] == batch_id
        assert "blood" in results["tables"]

        # Verify database was called
        mock_db_manager.bulk_insert.assert_called_once()
