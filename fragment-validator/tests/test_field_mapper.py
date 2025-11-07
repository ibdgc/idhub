import pandas as pd
import pytest
from services.field_mapper import FieldMapper


class TestFieldMapper:
    """Unit tests for FieldMapper"""

    def test_basic_field_mapping(self):
        """Test basic field mapping"""
        raw_data = pd.DataFrame(
            {"source_col1": [1, 2, 3], "source_col2": ["a", "b", "c"]}
        )

        field_mapping = {"target_col1": "source_col1", "target_col2": "source_col2"}

        result = FieldMapper.apply_mapping(
            raw_data, field_mapping, subject_id_candidates=[], center_id_field=None
        )

        assert list(result.columns) == ["target_col1", "target_col2"]
        assert list(result["target_col1"]) == [1, 2, 3]
        assert list(result["target_col2"]) == ["a", "b", "c"]

    def test_auto_include_subject_id_candidates(self):
        """Test auto-inclusion of subject ID candidate fields"""
        raw_data = pd.DataFrame(
            {
                "consortium_id": ["ID001", "ID002"],
                "local_id": ["LOCAL1", "LOCAL2"],
                "sample_id": ["SMP1", "SMP2"],
            }
        )

        field_mapping = {"sample_id": "sample_id"}
        subject_id_candidates = ["consortium_id", "local_id"]

        result = FieldMapper.apply_mapping(
            raw_data, field_mapping, subject_id_candidates, center_id_field=None
        )

        # Should include mapped field + auto-included candidates
        assert "sample_id" in result.columns
        assert "consortium_id" in result.columns
        assert "local_id" in result.columns
        assert len(result.columns) == 3

    def test_auto_include_center_id_field(self):
        """Test auto-inclusion of center_id field"""
        raw_data = pd.DataFrame(
            {
                "center_id": [1, 2, 3],
                "sample_id": ["SMP1", "SMP2", "SMP3"],
            }
        )

        field_mapping = {"sample_id": "sample_id"}

        result = FieldMapper.apply_mapping(
            raw_data, field_mapping, subject_id_candidates=[], center_id_field="center_id"
        )

        assert "sample_id" in result.columns
        assert "center_id" in result.columns
        assert list(result["center_id"]) == [1, 2, 3]

    def test_missing_source_field_creates_null_column(self):
        """Test that missing source fields create null columns with warning"""
        raw_data = pd.DataFrame({"existing_col": [1, 2, 3]})

        field_mapping = {
            "target_col1": "existing_col",
            "target_col2": "missing_col",
        }

        result = FieldMapper.apply_mapping(
            raw_data, field_mapping, subject_id_candidates=[], center_id_field=None
        )

        assert "target_col1" in result.columns
        assert "target_col2" in result.columns
        assert result["target_col2"].isna().all()

    def test_no_duplicate_columns_when_already_mapped(self):
        """Test that subject ID candidates aren't duplicated if already in mapping"""
        raw_data = pd.DataFrame(
            {
                "consortium_id": ["ID001", "ID002"],
                "sample_id": ["SMP1", "SMP2"],
            }
        )

        field_mapping = {"consortium_id": "consortium_id", "sample_id": "sample_id"}
        subject_id_candidates = ["consortium_id"]

        result = FieldMapper.apply_mapping(
            raw_data, field_mapping, subject_id_candidates, center_id_field=None
        )

        # Should not duplicate consortium_id
        assert list(result.columns).count("consortium_id") == 1
        assert len(result.columns) == 2

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame"""
        raw_data = pd.DataFrame()

        field_mapping = {"target_col": "source_col"}

        result = FieldMapper.apply_mapping(
            raw_data, field_mapping, subject_id_candidates=[], center_id_field=None
        )

        assert len(result) == 0
        assert "target_col" in result.columns
