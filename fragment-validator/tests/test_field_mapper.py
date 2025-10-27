# fragment-validator/tests/test_field_mapper.py
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

        result = FieldMapper.apply_mapping(
            raw_data,
            field_mapping,
            subject_id_candidates=["consortium_id", "local_id"],
            center_id_field=None,
        )

        # Should have mapped field + auto-included candidates
        assert "sample_id" in result.columns
        assert "consortium_id" in result.columns
        assert "local_id" in result.columns

    def test_auto_include_center_id_field(self):
        """Test auto-inclusion of center_id field"""
        raw_data = pd.DataFrame({"center_id": [1, 2], "sample_id": ["SMP1", "SMP2"]})

        field_mapping = {"sample_id": "sample_id"}

        result = FieldMapper.apply_mapping(
            raw_data,
            field_mapping,
            subject_id_candidates=[],
            center_id_field="center_id",
        )

        assert "center_id" in result.columns
        assert list(result["center_id"]) == [1, 2]

    def test_missing_source_field(self):
        """Test handling of missing source field"""
        raw_data = pd.DataFrame({"col1": [1, 2]})

        field_mapping = {
            "target1": "col1",
            "target2": "missing_col",  # This doesn't exist
        }

        result = FieldMapper.apply_mapping(
            raw_data, field_mapping, subject_id_candidates=[], center_id_field=None
        )

        assert "target1" in result.columns
        assert "target2" in result.columns
        assert result["target2"].isna().all()  # Should be None/NaN
