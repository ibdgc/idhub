# redcap-pipeline/tests/test_gsid_client.py
from datetime import date
from unittest.mock import Mock, patch

import pytest
import requests
from services.gsid_client import GSIDClient


class TestGSIDClient:
    """Test GSID client functionality"""

    @pytest.fixture
    def client(self):
        """Fixture for GSIDClient"""
        return GSIDClient(service_url="http://test-gsid-service", api_key="test-key")

    def test_register_subject_with_identifiers_success(self, client):
        """Test successful subject registration with multiple identifiers"""
        with patch("requests.Session.post") as mock_post:
            mock_response = Mock()
            mock_response.json.return_value = {
                "gsid": "GSID-TEST001",
                "action": "create_new",
                "identifiers_linked": 2,
                "conflicts": None,
                "conflict_resolution": None,
            }
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response

            identifiers = [
                {"local_subject_id": "LOCAL123", "identifier_type": "primary"},
                {"local_subject_id": "ALIAS456", "identifier_type": "alias"},
            ]
            result = client.register_subject_with_identifiers(
                center_id=1,
                identifiers=identifiers,
                registration_year=date(2024, 1, 1),
                control=False,
            )

            assert result["gsid"] == "GSID-TEST001"
            assert result["action"] == "create_new"
            assert result["identifiers_linked"] == 2

    def test_register_subject_with_identifiers_api_error(self, client):
        """Test handling of API errors"""
        with patch("requests.Session.post") as mock_post:
            mock_post.side_effect = requests.exceptions.RequestException("API Error")

            with pytest.raises(requests.exceptions.RequestException):
                client.register_subject_with_identifiers(
                    center_id=1,
                    identifiers=[
                        {"local_subject_id": "LOCAL123", "identifier_type": "primary"}
                    ],
                )

    def test_init_with_retry_logic(self):
        """Test that the session is initialized with a retry adapter"""
        with patch("requests.adapters.HTTPAdapter") as mock_adapter:
            GSIDClient(service_url="http://test-gsid-service", api_key="test-key")
            mock_adapter.assert_called_once()
            # Check that max_retries is set
            assert "max_retries" in mock_adapter.call_args[1]
