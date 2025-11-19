import json
from unittest.mock import MagicMock, patch

import pytest
from services.load_strategies import UniversalUpsertStrategy


class TestUniversalUpsertStrategy:
    """Test UniversalUpsertStrategy"""

    def test_insert_new_records(self, mock_db_connection):
        """Test inserting new records"""
        strategy = UniversalUpsertStrategy(
            table_name="blood",
            natural_key=["global_subject_id", "sample_id"],
        )

        # Mock empty current state (all records are new)
        mock_db_connection.cursor().fetchall.return_value = []

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
            mock_db_connection, records, "batch_001", "s3://bucket/file.csv"
        )

        assert result["rows_inserted"] == 2
        assert result["rows_updated"] == 0
        assert result["rows_unchanged"] == 0

    def test_update_changed_records(self, mock_db_connection):
        """Test updating changed records"""
        strategy = UniversalUpsertStrategy(
            table_name="blood",
            natural_key=["global_subject_id", "sample_id"],
        )

        # Mock current state
        mock_db_connection.cursor().fetchall.return_value = [
            ("GSID-001", "SMP001", 5.0),  # Old value
        ]
        mock_db_connection.cursor().description = [
            ("global_subject_id",),
            ("sample_id",),
            ("volume_ml",),
        ]

        records = [
            {
                "global_subject_id": "GSID-001",
                "sample_id": "SMP001",
                "volume_ml": 5.5,  # Changed
            },
        ]

        result = strategy.load(
            mock_db_connection, records, "batch_001", "s3://bucket/file.csv"
        )

        assert result["rows_inserted"] == 0
        assert result["rows_updated"] == 1
        assert result["rows_unchanged"] == 0

    def test_skip_unchanged_records(self, mock_db_connection):
        """Test skipping unchanged records"""
        strategy = UniversalUpsertStrategy(
            table_name="blood",
            natural_key=["global_subject_id", "sample_id"],
        )

        # Mock current state (same as incoming)
        mock_db_connection.cursor().fetchall.return_value = [
            ("GSID-001", "SMP001", 5.0),
        ]
        mock_db_connection.cursor().description = [
            ("global_subject_id",),
            ("sample_id",),
            ("volume_ml",),
        ]

        records = [
            {
                "global_subject_id": "GSID-001",
                "sample_id": "SMP001",
                "volume_ml": 5.0,  # Unchanged
            },

