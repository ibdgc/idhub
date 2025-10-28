# table-loader/tests/test_data_transformer.py
import pandas as pd
import pytest

from services.data_transformer import DataTransformer


class TestDataTransformer:
    """Test DataTransformer functionality"""

    def test_transform_records_basic(self, sample_fragment_data):
        """Test basic record transformation"""
        transformer = DataTransformer("blood")

        records = transformer.transform_records(sample_fragment_data)

        assert len(records) == 2
        assert all(isinstance(r, dict) for r in records)

    def test_transform_records_with_exclusions(self, sample_fragment_data):
        """Test record transformation with field exclusions"""
        exclude_fields = {"consortium_id", "identifier_type", "action"}
        transformer = DataTransformer("blood", exclude_fields=exclude_fields)

        records = transformer.transform_records(sample_fragment_data)

        # Excluded fields should not be present
        for record in records:
            assert "consortium_id" not in record
            assert "identifier_type" not in record
            assert "action" not in record

            # These fields should be present
            assert "global_subject_id" in record
            assert "sample_id" in record

    def test_transform_records_preserves_global_subject_id(self, sample_fragment_data):
        """Test that global_subject_id is always preserved"""
        exclude_fields = {"global_subject_id"}  # Try to exclude it
        transformer = DataTransformer("blood", exclude_fields=exclude_fields)

        records = transformer.transform_records(sample_fragment_data)

        # Should still be present
        for record in records:
            assert "global_subject_id" in record

    def test_transform_records_empty_data(self):
        """Test transformation with no records"""
        transformer = DataTransformer("blood")
        fragment = {"table": "blood", "records": []}

        records = transformer.transform_records(fragment)

        assert records == []

    def test_deduplicate_removes_duplicates(self):
        """Test deduplication of DataFrame"""
        transformer = DataTransformer("blood")

        df = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP001", "SMP002"],
                "sample_type": ["Blood", "Blood", "Serum"],
            }
        )

        result = transformer.deduplicate(df, ["global_subject_id", "sample_id"])

        assert len(result) == 2  # One duplicate removed
        assert list(result["global_subject_id"]) == ["GSID-001", "GSID-002"]

    def test_deduplicate_no_duplicates(self):
        """Test deduplication when no duplicates exist"""
        transformer = DataTransformer("blood")

        df = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP002"],
            }
        )

        result = transformer.deduplicate(df, ["global_subject_id"])

        assert len(result) == 2  # No change

    def test_prepare_rows(self, sample_dataframe):
        """Test prepare_rows converts DataFrame to columns and values"""
        transformer = DataTransformer("blood")

        columns, values = transformer.prepare_rows(sample_dataframe)

        assert columns == ["global_subject_id", "sample_id", "sample_type"]
        assert len(values) == 2
        assert values[0] == ("GSID-001", "SMP001", "Whole Blood")
        assert values[1] == ("GSID-002", "SMP002", "Serum")

    def test_prepare_rows_empty_dataframe(self):
        """Test prepare_rows with empty DataFrame"""
        transformer = DataTransformer("blood")
        df = pd.DataFrame()

        columns, values = transformer.prepare_rows(df)

        assert columns == []
        assert values == []
