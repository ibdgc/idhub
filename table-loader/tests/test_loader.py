# table-loader/tests/test_loader.py
"""Comprehensive tests for TableLoader"""

from unittest.mock import MagicMock, patch
import pytest
from services.loader import TableLoader
import pandas as pd


class TestTableLoader:
    """Comprehensive tests for TableLoader"""

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

    def test_init(self):
        """Test TableLoader initialization"""
        loader = TableLoader()
        assert loader.s3_client is not None
        assert loader.resolution_service is not None

    def test_load_batch_dry_run(
        self,
        mock_s3_client,
        mock_resolution_service,
        mock_data_transformer,
        mock_load_strategy,
        mock_db_connection,
    ):
        """Test load_batch in dry_run mode"""
        mock_s3_client.download_validation_report.return_value = {
            "status": "VALIDATED",
            "table_name": "blood",
        }
        mock_s3_client.download_fragment.return_value = pd.DataFrame({"a": [1]})
        mock_resolution_service.get_resolved_conflicts.return_value = []
        mock_data_transformer.return_value.transform_records.return_value = [{"a": 1}]
        mock_load_strategy.load.return_value = {"rows_loaded": 1}

        loader = TableLoader()
        result = loader.load_batch("batch_id", dry_run=True)

        assert result["status"] == "SUCCESS"
        assert result["records_loaded"] == 1
        mock_db_connection.commit.assert_not_called()
        mock_db_connection.rollback.assert_called_once()

    def test_load_batch_live_run(
        self,
        mock_s3_client,
        mock_resolution_service,
        mock_data_transformer,
        mock_load_strategy,
        mock_db_connection,
    ):
        """Test load_batch in live run mode"""
        mock_s3_client.download_validation_report.return_value = {
            "status": "VALIDATED",
            "table_name": "blood",
        }
        mock_s3_client.download_fragment.return_value = pd.DataFrame({"a": [1]})
        mock_resolution_service.get_resolved_conflicts.return_value = []
        mock_data_transformer.return_value.transform_records.return_value = [{"a": 1}]
        mock_load_strategy.load.return_value = {"rows_loaded": 1}

        loader = TableLoader()
        result = loader.load_batch("batch_id", dry_run=False)

        assert result["status"] == "SUCCESS"
        assert result["records_loaded"] == 1
        mock_db_connection.commit.assert_called_once()
        mock_db_connection.rollback.assert_not_called()

    def test_load_batch_not_validated(self, mock_s3_client):
        """Test load_batch with a non-validated batch"""
        mock_s3_client.download_validation_report.return_value = {"status": "FAILED"}

        loader = TableLoader()
        with pytest.raises(ValueError, match="not validated"):
            loader.load_batch("batch_id")
