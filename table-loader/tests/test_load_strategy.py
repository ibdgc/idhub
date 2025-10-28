# table-loader/tests/test_load_strategy.py
import pytest
from unittest.mock import MagicMock, patch

from services.load_strategy import StandardLoadStrategy, UpsertLoadStrategy


class TestStandardLoadStrategy:
    """Test StandardLoadStrategy"""

    def test_load_dry_run(self, sample_fragment_data):
        """Test dry run mode"""
        strategy = StandardLoadStrategy("blood")

        result = strategy.load(sample_fragment_data, dry_run=True)

        assert result["status"] == "preview"
        assert result["table"] == "blood"
        assert result["rows"] == 2
        assert "columns" in result
        assert "sample" in result

    def test_load_with_exclusions(self, sample_fragment_data):
        """Test load with field exclusions"""
        exclude_fields = {"consortium_id", "identifier_type", "action"}
        strategy = StandardLoadStrategy("blood", exclude_fields=exclude_fields)

        result = strategy.load(sample_fragment_data, dry_run=True)

        # Check that excluded fields are not in columns
        assert "consortium_id" not in result["columns"]
        assert "identifier_type" not in result["columns"]
        assert "action" not in result["columns"]

    @patch("core.database.db_manager")
    def test_load_executes_insert(self, mock_db_manager, sample_fragment_data):
        """Test that load executes database insert"""
        mock_conn = MagicMock()
        mock_db_manager.get_connection.return_value.__enter__.return_value = mock_conn

        strategy = StandardLoadStrategy("blood")
        result = strategy.load(sample_fragment_data, dry_run=False)

        assert result["status"] == "success"
        assert result["table"] == "blood"
        assert result["rows_loaded"] == 2

        # Should call bulk_insert
        mock_db_manager.bulk_insert.assert_called_once()

    def test_load_empty_records(self):
        """Test load with no records"""
        strategy = StandardLoadStrategy("blood")
        fragment = {"table": "blood", "records": []}

        result = strategy.load(fragment, dry_run=False)

        assert result["status"] == "skipped"
        assert result["reason"] == "no records"

    def test_load_with_deduplication(self):
        """Test load with deduplication"""
        fragment = {
            "table": "blood",
            "records": [
                {"global_subject_id": "GSID-001", "sample_id": "SMP001"},
                {"global_subject_id": "GSID-001", "sample_id": "SMP001"},  # Duplicate
                {"global_subject_id": "GSID-002", "sample_id": "SMP002"},
            ],
            "metadata": {
                "key_columns": ["global_subject_id", "sample_id"],
            },
        }

        strategy = StandardLoadStrategy("blood")
        result = strategy.load(fragment, dry_run=True)

        # Should deduplicate to 2 rows
        assert result["rows"] == 2


class TestUpsertLoadStrategy:
    """Test UpsertLoadStrategy"""

    def test_upsert_dry_run(self, sample_fragment_data):
        """Test upsert dry run mode"""
        strategy = UpsertLoadStrategy(
            "blood",
            conflict_columns=["global_subject_id", "sample_id"],
            update_columns=["sample_type", "date_collected"],
        )

        result = strategy.load(sample_fragment_data, dry_run=True)

        assert result["status"] == "preview"
        assert result["strategy"] == "upsert"
        assert result["conflict_on"] == ["global_subject_id", "sample_id"]

    @patch("core.database.db_manager")
    @patch("psycopg2.extras.execute_values")
    def test_upsert_executes_with_conflict_clause(
        self, mock_execute_values, mock_db_manager, sample_fragment_data
    ):
        """Test that upsert generates correct SQL"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_db_manager.get_connection.return_value.__enter__.return_value = mock_conn
        mock_db_manager.get_cursor.return_value.__enter__.return_value = mock_cursor

        strategy = UpsertLoadStrategy(
            "blood",
            conflict_columns=["global_subject_id"],
            update_columns=["sample_type"],
        )

        result = strategy.load(sample_fragment_data, dry_run=False)

        assert result["status"] == "success"

        # Check that execute_values was called
        mock_execute_values.assert_called_once()
        call_args = mock_execute_values.call_args
        query = call_args[0][1]

        # Verify SQL contains conflict clause
        assert "ON CONFLICT" in query
        assert "DO UPDATE SET" in query
