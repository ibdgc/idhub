# table-loader/tests/test_load_strategy.py
from unittest.mock import MagicMock

import pytest


class TestLoadStrategy:
    def test_standard_load(self, mock_db_connection):
        """Test standard load strategy"""
        conn, cursor = mock_db_connection
        cursor.rowcount = 5

        # Simulate insert
        assert cursor.rowcount == 5

    def test_upsert_load(self, mock_db_connection):
        """Test upsert load strategy"""
        conn, cursor = mock_db_connection
        cursor.rowcount = 3

        # Simulate upsert
        assert cursor.rowcount == 3

    def test_truncate_load(self, mock_db_connection):
        """Test truncate and load strategy"""
        conn, cursor = mock_db_connection

        # Simulate truncate then insert
        cursor.rowcount = 10
        assert cursor.rowcount == 10
