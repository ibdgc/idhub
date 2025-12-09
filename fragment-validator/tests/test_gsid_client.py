import pytest
import requests
from unittest.mock import Mock, patch
from services.gsid_client import GSIDClient


class TestGSIDClient:
    """Unit tests for GSIDClient"""

    @pytest.fixture
    def client(self):
        """Fixture for GSIDClient"""
        return GSIDClient(service_url="http://test-gsid-service", api_key="test-key")

    def test_register_subject_success(self, client):
        """Test single subject ID registration"""
        with patch("requests.post") as mock_post:
            mock_post.return_value.json.return_value = {
                "gsid": "GSID-001",
                "action": "create_new",
            }
            mock_post.return_value.raise_for_status = Mock()

            result = client.register_subject(
                center_id=1,
                identifiers=[{"local_subject_id": "LOCAL001", "identifier_type": "consortium_id"}],
            )

            assert result["gsid"] == "GSID-001"
            assert result["action"] == "create_new"
            mock_post.assert_called_once()

    def test_register_batch_success(self, client):
        """Test batch registration with single batch"""
        with patch.object(client, "register_subject") as mock_register_subject:
            
            mock_register_subject.side_effect = [
                {"gsid": "GSID-001", "action": "create_new"},
                {"gsid": "GSID-002", "action": "existing_match"},
            ]

            requests_list = [
                {"center_id": 1, "identifiers": [{"local_subject_id": "ID001", "identifier_type": "consortium_id"}]},
                {"center_id": 1, "identifiers": [{"local_subject_id": "ID002", "identifier_type": "consortium_id"}]},
            ]

            results = client.register_batch(requests_list, batch_size=100)

            assert len(results) == 2
            assert results[0]["gsid"] == "GSID-001"
            assert results[1]["gsid"] == "GSID-002"
            assert mock_register_subject.call_count == 2

    def test_headers_include_api_key(self, client):
        """Test that API key is included in headers"""
        assert client.headers["x-api-key"] == "test-key"

    def test_service_url_trailing_slash_removed(self):
        """Test that trailing slash is removed from service URL"""
        client = GSIDClient("http://gsid-service/", "test-key")
        assert client.service_url == "http://gsid-service"
