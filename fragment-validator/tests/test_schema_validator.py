# fragment-validator/tests/test_schema_validator.py
import pandas as pd
import pytest


class TestSchemaValidator:
    def test_validate_required_columns(self):
        """Test required column validation"""
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        required = ["col1", "col2"]

        missing = set(required) - set(df.columns)
        assert len(missing) == 0

    def test_validate_missing_columns(self):
        """Test missing column detection"""
        df = pd.DataFrame({"col1": [1, 2]})
        required = ["col1", "col2"]

        missing = set(required) - set(df.columns)
        assert "col2" in missing
        assert len(missing) == 1

    def test_validate_extra_columns(self):
        """Test extra column detection"""
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4], "col3": [5, 6]})
        required = ["col1", "col2"]

        extra = set(df.columns) - set(required)
        assert "col3" in extra

    def test_validate_empty_dataframe(self):
        """Test empty dataframe validation"""
        df = pd.DataFrame()
        assert len(df) == 0
        assert len(df.columns) == 0
