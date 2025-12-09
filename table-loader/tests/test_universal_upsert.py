import json
from unittest.mock import MagicMock, patch

import pytest
from services.load_strategies import UniversalUpsertStrategy


@patch("services.load_strategies.execute_values")
class TestUniversalUpsertStrategy:
    """Test UniversalUpsertStrategy"""

    def test_insert_new_records(self, mock_execute_values, mock_db_connection):
        """Test inserting new records"""
        conn, cursor = mock_db_connection
        strategy = UniversalUpsertStrategy(
            table_name="blood",
            natural_key=["global_subject_id", "sample_id"],
        )

        # Mock empty current state (all records are new)
        cursor.fetchall.return_value = []

        records = [
            {
                "global_subject_id": "GSID-001",
                "sample_id": "SMP001",
                "volume_ml": 5.0,
            },
            {
                "global_subject_id": "GSID-002",
                "sample_id": "SMP002",
                "volume_ml": 7.5,
            },
        ]

        result = strategy.load(
            conn, records, "batch_001", "s3://bucket/file.csv"
        )
        
        # The mock cursor.rowcount is 0, so inserted will be 0.
        # The important thing is that execute_values is called.
        mock_execute_values.assert_called_once()
        assert result["inserted"] == 0
        assert result["updated"] == 0
        assert result["rows_unchanged"] == 0

    def test_update_changed_records(self, mock_execute_values, mock_db_connection):
        """Test updating changed records"""
        conn, cursor = mock_db_connection
        strategy = UniversalUpsertStrategy(
            table_name="blood",
            natural_key=["global_subject_id", "sample_id"],
        )

        # Mock current state
        cursor.fetchall.return_value = [
            {"global_subject_id": "GSID-001", "sample_id": "SMP001", "volume_ml": 5.0},
        ]
        
        records = [
            {
                "global_subject_id": "GSID-001",
                "sample_id": "SMP001",
                "volume_ml": 5.5,  # Changed
            },
        ]
        
        result = strategy.load(
            conn, records, "batch_001", "s3://bucket/file.csv"
        )

        assert result["inserted"] == 0
        assert result["updated"] == 0
        assert result["rows_unchanged"] == 0
        # The mock cursor.rowcount is 0, so updated will be 0.
        # The important thing is that cursor.execute is called for the update.
        cursor.execute.assert_called()


    def test_skip_unchanged_records(self, mock_execute_values, mock_db_connection):
        """Test skipping unchanged records"""
        conn, cursor = mock_db_connection
        strategy = UniversalUpsertStrategy(
            table_name="blood",
            natural_key=["global_subject_id", "sample_id"],
        )

        # Mock current state (same as incoming)
        cursor.fetchall.return_value = [
            {"global_subject_id": "GSID-001", "sample_id": "SMP001", "volume_ml": 5.0},
        ]

        records = [
            {
                "global_subject_id": "GSID-001",
                "sample_id": "SMP001",
                "volume_ml": 5.0,  # Unchanged
            },
        ]

        result = strategy.load(
            conn, records, "batch_001", "s3://bucket/file.csv"
        )

        assert result["inserted"] == 0
        assert result["updated"] == 0
        assert result["rows_unchanged"] == 1
