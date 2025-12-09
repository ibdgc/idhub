# table-loader/tests/test_integration.py
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from services.loader import TableLoader


class TestIntegration:
    """Integration tests for complete load workflow"""

    @pytest.fixture
    def mock_s3_client(self):
        with patch("services.loader.S3Client") as mock:
            yield mock.return_value

    @pytest.fixture
    def mock_resolution_service(self):
        with patch("services.loader.FragmentResolutionService") as mock:
            yield mock.return_value

    @pytest.fixture
    def mock_data_transformer(self):
        with patch("services.loader.DataTransformer") as mock:
            yield mock

    @pytest.fixture
    def mock_load_strategy(self):
        with patch("services.loader.StandardLoadStrategy") as mock:
            yield mock.return_value

    @pytest.fixture
    def mock_db_connection(self):
        with patch("services.loader.get_db_connection") as mock:
            yield mock.return_value

    def test_full_load_workflow(
        self,
        mock_s3_client,
        mock_resolution_service,
        mock_data_transformer,
        mock_load_strategy,
        mock_db_connection,
        sample_validation_report,
    ):
        """Test complete load workflow from S3 to database"""
        batch_id = "batch_20240115_120000"

        # Setup mocks
        mock_s3_client.download_validation_report.return_value = sample_validation_report
        mock_s3_client.download_fragment.return_value = pd.DataFrame(
            {"global_subject_id": ["GSID-001"]}
        )
        mock_resolution_service.get_resolved_conflicts.return_value = []
        mock_data_transformer.return_value.transform_records.return_value = [
            {"global_subject_id": "GSID-001"}
        ]
        mock_load_strategy.load.return_value = {"rows_loaded": 1}

        # Execute load
        loader = TableLoader()
        results = loader.load_batch(batch_id, dry_run=False)

        # Verify results
        assert results["status"] == "SUCCESS"
        assert results["records_loaded"] == 1
        mock_db_connection.commit.assert_called_once()

    def test_preview_workflow(
        self,
        mock_s3_client,
        mock_resolution_service,
        mock_data_transformer,
        mock_load_strategy,
        mock_db_connection,
        sample_validation_report,
    ):
        """Test preview workflow without database operations"""
        batch_id = "batch_20240115_120000"

        # Setup mocks
        mock_s3_client.download_validation_report.return_value = sample_validation_report
        mock_s3_client.download_fragment.return_value = pd.DataFrame(
            {"global_subject_id": ["GSID-001"]}
        )
        mock_resolution_service.get_resolved_conflicts.return_value = []
        mock_data_transformer.return_value.transform_records.return_value = [
            {"global_subject_id": "GSID-001"}
        ]
        mock_load_strategy.load.return_value = {"rows_loaded": 1}

        # Execute preview
        loader = TableLoader()
        results = loader.load_batch(batch_id, dry_run=True)

        # Verify results
        assert results["status"] == "SUCCESS"
        assert results["records_loaded"] == 1
        mock_db_connection.commit.assert_not_called()
        mock_db_connection.rollback.assert_called_once()
