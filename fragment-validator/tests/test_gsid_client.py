import pytest
import requests
from unittest.mock import Mock, patch
from services.gsid_client import GSIDClient


class TestGSIDClient:
    """Unit tests for GSIDClient"""

    def test_register_single(self):
        """Test single subject ID registration"""
        with patch("requests.post") as mock_post:
            mock_post.return_value.json.return_value = {
                "gsid": "GSID-001",
                "action": "create_new",
            }
            mock_post.return_value.raise_for_status = Mock()

            client = GSIDClient("http://gsid-service", "test-key")
            result = client.register_single(
                {
                    "center_id": 1,
                    "local_subject_id": "LOCAL001",
                    "identifier_type": "consortium_id",
                }
            )

            assert result["gsid"] == "GSID-001"
            assert result["action"] == "create_new"
            mock_post.assert_called_once()

    def test_register_batch_single_batch(self):
        """Test batch registration with single batch"""
        with patch("requests.post") as mock_post:
            mock_post.return_value.json.return_value = [
                {"gsid": "GSID-001", "action": "create_new"},
                {"gsid": "GSID-002", "action": "existing_match"},
            ]
            mock_post.return_value.raise_for_status = Mock()

            client = GSIDClient("http://gsid-service", "test-key")
            requests_list = [
                {"center_id": 1, "local_subject_id": "ID001", "identifier_type": "consortium_id"},
                {"center_id": 1, "local_subject_id": "ID002", "identifier_type": "consortium_id"},
            ]

            results = client.register_batch(requests_list, batch_size=100)

            assert len(results) == 2
            assert results[0]["gsid"] == "GSID-001"
            assert results[1]["gsid"] == "GSID-002"
            mock_post.assert_called_once()

    def test_register_batch_multiple_batches(self):
        """Test batch registration split across multiple batches"""
        with patch("requests.post") as mock_post:
            # First batch
            mock_post.return_value.json.side_effect = [
                [{"gsid": f"GSID-{i:03d}", "action": "create_new"} for i in range(1, 3)],
                [{"gsid": f"GSID-{i:03d}", "action": "create_new"} for i in range(3, 5)],
            ]
            mock_post.return_value.raise_for_status = Mock()

            client = GSIDClient("http://gsid-service", "test-key")
            requests_list = [
                {"center_id": 1, "local_subject_id": f"ID{i:03d}", "identifier_type": "consortium_id"}
                for i in range(1, 5)
            ]

            results = client.register_batch(requests_list, batch_size=2)

            assert len(results) == 4
            assert mock_post.call_count == 2

    def test_register_batch_handles_errors(self):
        """Test batch registration error handling"""
        with patch("requests.post") as mock_post:
            mock_post.side_effect = requests.exceptions.RequestException("API Error")

            client = GSIDClient("http://gsid-service", "test-key")
            requests_list = [
                {"center_id": 1, "local_subject_id": "ID001", "identifier_type": "consortium_id"}
            ]

            with pytest.raises(requests.exceptions.RequestException):
                client.register_batch(requests_list)

    def test_register_batch_timeout_parameter(self):
        """Test that timeout parameter is passed correctly"""
        with patch("requests.post") as mock_post:
            mock_post.return_value.json.return_value = [
                {"gsid": "GSID-001", "action": "create_new"}
            ]
            mock_post.return_value.raise_for_status = Mock()

            client = GSIDClient("http://gsid-service", "test-key")
            requests_list = [
                {"center_id": 1, "local_subject_id": "ID001", "identifier_type": "consortium_id"}
            ]

            client.register_batch(requests_list, timeout=120)

            call_kwargs = mock_post.call_args.kwargs
            assert call_kwargs["timeout"] == 120

    def test_headers_include_api_key(self):
        """Test that API key is included in headers"""
        client = GSIDClient("http://gsid-service", "my-secret-key")

        assert client.headers["x-api-key"] == "my-secret-key"

    def test_service_url_trailing_slash_removed(self):
        """Test that trailing slash is removed from service URL"""
        client = GSIDClient("http://gsid-service/", "test-key")

        assert client.service_url == "http://gsid-service"
