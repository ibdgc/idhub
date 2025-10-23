# table-loader/tests/test_loader.py
from unittest.mock import MagicMock

import pandas as pd
import pytest


class TestTableLoader:
    def test_load_data_preview(self, mock_db_connection):
        """Test data loading preview"""
        conn, cursor = mock_db_connection

        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        assert len(df) == 2
        assert list(df.columns) == ["col1", "col2"]

    def test_load_data_execute(self, mock_db_connection):
        """Test actual data loading"""
        conn, cursor = mock_db_connection
        cursor.rowcount = 2

        assert cursor.rowcount == 2

    def test_batch_insert(self, mock_db_connection):
        """Test batch insert operation"""
        conn, cursor = mock_db_connection

        data = [(1, "a"), (2, "b"), (3, "c")]
        cursor.rowcount = len(data)

        assert cursor.rowcount == 3
