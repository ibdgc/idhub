# redcap-pipeline/tests/test_gsid_client.py
from unittest.mock import MagicMock

import pytest


class TestGSIDClient:
    def test_generate_gsids_success(self, mock_requests):
        """Test GSID generation request"""
        mock_requests.return_value.json.return_value = {
            "gsids": ["GSID1", "GSID2", "GSID3"],
            "count": 3,
        }

        response = mock_requests.return_value
        data = response.json()

        assert data["count"] == 3
        assert len(data["gsids"]) == 3

    def test_generate_gsids_error(self, mock_requests):
        """Test GSID generation error"""
        mock_requests.return_value.status_code = 500

        response = mock_requests.return_value
        assert response.status_code == 500
