# table-loader/tests/test_loader.py
import pytest
from unittest.mock import MagicMock, patch

from services.loader import TableLoader


class TestTableLoader:
    """Test TableLoader orchestration"""

    def test_init(self, mock_s3_client):
        """Test TableLoader initialization"""
        loader = TableLoader()

        assert loader.s3_client is not None

    def test_get_exclude_fields_from_report(
        self, mock_s3_client, sample_validation_report
    ):
        """Test extracting exclude fields from validation report"""
        batch_id = "batch_20240115_120000"

        # Mock S3 to return validation report
        with patch.object(
            TableLoader, "_get_exclude_fields", return_value={"consortium_id", "action"}
        ):
            loader = TableLoader()
            exclude_fields = loader._get_exclude_fields(batch_id)

            assert "consortium_id" in exclude_fields
            assert "action" in exclude_fields

    def test_get_exclude_fields_default(self, mock_s3_client):
        """Test default exclude fields when report not found"""
        batch_id = "batch_nonexistent"

        # Mock S3 to raise exception
        with patch.object(
            mock_s3_client, "get_object", side_effect=Exception("Not found")
        ):
            loader = TableLoader()
            exclude_fields = loader._get_exclude_fields(batch_id)

            # Should return default exclusions
            assert "identifier_type" in exclude_fields
            assert "action" in exclude_fields

    def test_preview_load(self, s3_with_fragments, sample_fragment_data):
        """Test preview load (dry run)"""
        batch_id = "batch_20240115_120000"

        loader = TableLoader()

        with patch.object(loader, "_get_exclude_fields", return_value=set()):
            results = loader.preview_load(batch_id)

            assert "blood" in results
            assert results["blood"]["status"] == "preview"
            assert results["blood"]["rows"] == 2

    def test_preview_load_no_fragments(self, mock_s3_client):
        """Test preview load with no fragments"""
        batch_id = "batch_empty"

        mock_s3_client.list_objects_v2.return_value = {}

        loader = TableLoader()

        with pytest.raises(ValueError, match="No table fragments found"):
            loader.preview_load(batch_id)

    @patch("services.load_strategy.db_manager")  # Patch where load_strategy imports it
    def test_execute_load(
        self, mock_db_manager, s3_with_fragments, mock_db_connection
    ):
        """Test execute load"""
        batch_id = "batch_20240115_120000"
        conn, cursor = mock_db_connection

        # Configure db_manager mock
        mock_db_manager.get_connection.return_value.__enter__.return_value = conn
        mock_db_manager.get_connection.return_value.__exit__.return_value = False

        loader = TableLoader()

        with patch.object(loader, "_get_exclude_fields", return_value=set()):
            results = loader.execute_load(batch_id)

            assert results["batch_id"] == batch_id
            assert "blood" in results["tables"]
            assert results["tables"]["blood"]["status"] == "success"

            # Should call bulk_insert (StandardLoadStrategy uses this)
            mock_db_manager.bulk_insert.assert_called()

    @patch("services.load_strategy.db_manager")  # Patch where it's imported
    def test_execute_load_stops_on_error(self, mock_db_manager, s3_with_fragments):
        """Test that execute_load stops on first error"""
        batch_id = "batch_20240115_120000"

        # Mock db_manager to raise error
        mock_db_manager.get_connection.side_effect = Exception("Database error")

        loader = TableLoader()

        with patch.object(loader, "_get_exclude_fields", return_value=set()):
            # Should raise exception and stop
            with pytest.raises(Exception, match="Database error"):
                loader.execute_load(batch_id)
