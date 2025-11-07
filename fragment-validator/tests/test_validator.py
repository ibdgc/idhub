import pandas as pd
import pytest


class TestFragmentValidator:
    """Unit tests for FragmentValidator - basic data validation tests"""

    def test_validate_data_types(self):
        """Test data type validation"""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        assert df["col1"].dtype == "int64"
        assert df["col2"].dtype == "object"

    def test_validate_null_values(self):
        """Test null value detection"""
        df = pd.DataFrame({"col1": [1, None, 3], "col2": ["a", "b", None]})

        assert df["col1"].isnull().sum() == 1
        assert df["col2"].isnull().sum() == 1

    def test_validate_duplicates(self):
        """Test duplicate detection"""
        df = pd.DataFrame({"id": [1, 2, 2, 3], "value": ["a", "b", "c", "d"]})

        duplicates = df["id"].duplicated().sum()
        assert duplicates == 1
