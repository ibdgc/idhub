# fragment-validator/tests/test_validator.py
import pandas as pd
import pytest


class TestFragmentValidator:
    def test_validate_data_types(self):
        """Test data type validation"""
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "str_col": ["a", "b", "c"],
                "float_col": [1.1, 2.2, 3.3],
            }
        )

        assert df["int_col"].dtype == "int64"
        assert df["str_col"].dtype == "object"
        assert df["float_col"].dtype == "float64"

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
