# redcap-pipeline/tests/test_redcap_client.py
from unittest.mock import MagicMock

import pytest


class TestREDCapClient:
    def test_fetch_records_success(self, mock_requests):
        """Test successful record fetch"""
        mock_requests.return_value.json.return_value = [
            {"record_id": "1", "field1": "value1"},
            {"record_id": "2", "field1": "value2"},
        ]

        response = mock_requests.return_value
        data = response.json()

        assert len(data) == 2
        assert data[0]["record_id"] == "1"

    def test_fetch_records_empty(self, mock_requests):
        """Test empty record response"""
        mock_requests.return_value.json.return_value = []

        response = mock_requests.return_value
        data = response.json()

        assert len(data) == 0
