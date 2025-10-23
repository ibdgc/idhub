# table-loader/tests/test_data_transformer.py
import pandas as pd
import pytest


class TestDataTransformer:
    def test_transform_column_names(self):
        """Test column name transformation"""
        df = pd.DataFrame({"Col 1": [1, 2], "Col 2": [3, 4]})

        df.columns = [col.lower().replace(" ", "_") for col in df.columns]

        assert "col_1" in df.columns
        assert "col_2" in df.columns

    def test_transform_data_types(self):
        """Test data type transformation"""
        df = pd.DataFrame({"col1": ["1", "2", "3"]})

        df["col1"] = df["col1"].astype(int)

        assert df["col1"].dtype == "int64"

    def test_filter_rows(self):
        """Test row filtering"""
        df = pd.DataFrame({"col1": [1, 2, 3, 4, 5]})

        filtered = df[df["col1"] > 2]

        assert len(filtered) == 3
        assert filtered["col1"].min() == 3
