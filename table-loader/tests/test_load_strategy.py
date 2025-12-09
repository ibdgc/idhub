# table-loader/tests/test_load_strategy.py
from unittest.mock import MagicMock, call, patch

import pytest
from services.load_strategies import StandardLoadStrategy, UpsertLoadStrategy, UniversalUpsertStrategy


@patch("services.load_strategies.execute_values")
class TestStandardLoadStrategy:
    """Test StandardLoadStrategy"""

    def test_load_executes_insert(self, mock_execute_values, mock_db_connection, sample_fragment_data):
        """Test that load executes database insert"""
        conn, cursor = mock_db_connection
        strategy = StandardLoadStrategy("blood")
        result = strategy.load(conn, sample_fragment_data["records"], "batch_id", "source")

        assert result["rows_loaded"] == 0
        mock_execute_values.assert_called_once()

    def test_load_empty_records(self, mock_execute_values, mock_db_connection):
        """Test load with no records"""
        conn, cursor = mock_db_connection
        strategy = StandardLoadStrategy("blood")
        result = strategy.load(conn, [], "batch_id", "source")

        assert result["rows_attempted"] == 0
        assert result["rows_loaded"] == 0
        mock_execute_values.assert_not_called()

    def test_load_with_exclusions(self, mock_execute_values, mock_db_connection, sample_fragment_data):
        """Test load with field exclusions"""
        conn, cursor = mock_db_connection
        exclude_fields = {"consortium_id", "identifier_type", "action"}
        strategy = StandardLoadStrategy("blood", exclude_fields=exclude_fields)

        strategy.load(conn, sample_fragment_data["records"], "batch_id", "source")

        # Verify that excluded fields are not in the insert query
        call_args = mock_execute_values.call_args
        query = call_args[0][1]
        assert "consortium_id" not in query
        assert "identifier_type" not in query
        assert "action" not in query


@patch("services.load_strategies.execute_values")
class TestUpsertLoadStrategy:
    """Test UpsertLoadStrategy"""

    def test_upsert_executes_with_conflict_clause(
        self, mock_execute_values, mock_db_connection, sample_fragment_data
    ):
        """Test that upsert generates correct SQL"""
        conn, cursor = mock_db_connection
        strategy = UpsertLoadStrategy(
            "blood",
            conflict_columns=["global_subject_id"],
        )

        result = strategy.load(conn, sample_fragment_data["records"], "batch_id", "source")
        assert result["rows_loaded"] == 0

        # Check that execute_values was called
        mock_execute_values.assert_called_once()
        call_args = mock_execute_values.call_args
        query = call_args[0][1]

        # Verify SQL contains conflict clause
        assert "ON CONFLICT" in query
        assert "DO UPDATE SET" in query

