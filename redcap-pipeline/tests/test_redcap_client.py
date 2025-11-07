# redcap-pipeline/tests/test_redcap_client.py
from unittest.mock import MagicMock, patch

import pytest


class TestREDCapClient:
    """Test REDCapClient functionality"""

    def test_init_success(self, sample_project_config):
        """Test successful initialization"""
        from services.redcap_client import REDCapClient

        client = REDCapClient(sample_project_config)

        assert client.project_key == "test_project"
        assert client.project_name == "Test Project"
        assert client.api_token is not None

    def test_init_missing_api_url(self):
        """Test initialization with missing API URL"""
        from services.redcap_client import REDCapClient

        config = {
            "key": "test",
            "name": "Test",
            "api_token": "test_token",
            # No redcap_api_url
        }

        with patch("core.config.settings") as mock_settings:
            mock_settings.REDCAP_API_URL = None
            with pytest.raises(ValueError, match="redcap_api_url is required"):
                REDCapClient(config)

    def test_init_missing_api_token(self):
        """Test initialization with missing API token"""
        from services.redcap_client import REDCapClient

        config = {
            "key": "test",
            "name": "Test",
            "redcap_api_url": "https://test.redcap.edu/api/",
            # No api_token
        }

        with pytest.raises(ValueError, match="api_token is required"):
            REDCapClient(config)

    def test_fetch_records_batch_success(self, sample_project_config):
        """Test successful batch record fetch"""
        from services.redcap_client import REDCapClient

        test_records = [
            {"record_id": "1", "field1": "value1"},
            {"record_id": "2", "field1": "value2"},
        ]

        with patch("requests.Session") as mock_session:
            mock_response = MagicMock()
            mock_response.json.return_value = test_records
            mock_response.status_code = 200
            mock_session.return_value.post.return_value = mock_response

            client = REDCapClient(sample_project_config)
            records = client.fetch_records_batch(batch_size=2, offset=0)

        assert len(records) == 2
        assert records[0]["record_id"] == "1"

    def test_fetch_records_batch_pagination(self, sample_project_config):
        """Test batch fetch with pagination"""
        from services.redcap_client import REDCapClient

        # Create 75 records - REDCap returns ALL records, then we paginate manually
        all_records = [{"record_id": str(i)} for i in range(1, 76)]

        with patch("requests.Session") as mock_session:
            mock_response = MagicMock()
            mock_response.status_code = 200
            # REDCap always returns all records
            mock_response.json.return_value = all_records
            mock_session.return_value.post.return_value = mock_response

            client = REDCapClient(sample_project_config)

            # Fetch first batch (offset=0, batch_size=50)
            # Should return records[0:50] = 50 records
            batch1 = client.fetch_records_batch(batch_size=50, offset=0)

            # Fetch second batch (offset=50, batch_size=50)
            # Should return records[50:100] = 25 records (only 75 total)
            batch2 = client.fetch_records_batch(batch_size=50, offset=50)

        assert len(batch1) == 50
        assert batch1[0]["record_id"] == "1"
        assert batch1[49]["record_id"] == "50"

        assert len(batch2) == 25
        assert batch2[0]["record_id"] == "51"
        assert batch2[24]["record_id"] == "75"

    def test_fetch_records_batch_timeout_retry(self, sample_project_config):
        """Test retry on timeout"""
        import requests
        from services.redcap_client import REDCapClient

        with (
            patch("requests.Session") as mock_session,
            patch("time.sleep"),
        ):  # Mock sleep to speed up test
            # First call times out, second succeeds
            mock_success = MagicMock()
            mock_success.json.return_value = [{"record_id": "1"}]
            mock_success.status_code = 200

            mock_session.return_value.post.side_effect = [
                requests.exceptions.Timeout("Timeout"),
                mock_success,
            ]

            client = REDCapClient(sample_project_config)
            records = client.fetch_records_batch(batch_size=1, offset=0)

        assert len(records) == 1

    def test_fetch_records_batch_timeout_failure(self, sample_project_config):
        """Test failure after max retries"""
        import requests
        from services.redcap_client import REDCapClient

        with (
            patch("requests.Session") as mock_session,
            patch("time.sleep"),
        ):  # Mock sleep to speed up test
            # All calls time out
            mock_session.return_value.post.side_effect = requests.exceptions.Timeout(
                "Timeout"
            )

            client = REDCapClient(sample_project_config)

            with pytest.raises(requests.exceptions.Timeout):
                client.fetch_records_batch(batch_size=1, offset=0)

    def test_fetch_records_batch_request_error(self, sample_project_config):
        """Test handling of request errors"""
        import requests
        from services.redcap_client import REDCapClient

        with patch("requests.Session") as mock_session:
            mock_session.return_value.post.side_effect = (
                requests.exceptions.RequestException("Error")
            )

            client = REDCapClient(sample_project_config)

            with pytest.raises(requests.exceptions.RequestException):
                client.fetch_records_batch(batch_size=1, offset=0)

    def test_get_project_info_success(self, sample_project_config):
        """Test getting project info"""
        from services.redcap_client import REDCapClient

        project_info = {
            "project_id": "123",
            "project_title": "Test Project",
        }

        with patch("requests.Session") as mock_session:
            mock_response = MagicMock()
            mock_response.json.return_value = project_info
            mock_response.status_code = 200
            mock_session.return_value.post.return_value = mock_response

            client = REDCapClient(sample_project_config)
            info = client.get_project_info()

        assert info["project_id"] == "123"
        assert info["project_title"] == "Test Project"

    def test_get_project_info_error(self, sample_project_config):
        """Test error handling in get_project_info"""
        import requests
        from services.redcap_client import REDCapClient

        with patch("requests.Session") as mock_session:
            mock_session.return_value.post.side_effect = (
                requests.exceptions.RequestException("Error")
            )

            client = REDCapClient(sample_project_config)

            with pytest.raises(requests.exceptions.RequestException):
                client.get_project_info()

    def test_get_metadata_success(self, sample_project_config):
        """Test getting project metadata"""
        from services.redcap_client import REDCapClient

        metadata = [
            {"field_name": "record_id", "field_type": "text"},
            {"field_name": "subject_id", "field_type": "text"},
        ]

        with patch("requests.Session") as mock_session:
            mock_response = MagicMock()
            mock_response.json.return_value = metadata
            mock_response.status_code = 200
            mock_session.return_value.post.return_value = mock_response

            client = REDCapClient(sample_project_config)
            result = client.get_metadata()

        assert len(result) == 2
        assert result[0]["field_name"] == "record_id"

    def test_get_metadata_error(self, sample_project_config):
        """Test error handling in get_metadata"""
        import requests
        from services.redcap_client import REDCapClient

        with patch("requests.Session") as mock_session:
            mock_session.return_value.post.side_effect = (
                requests.exceptions.RequestException("Error")
            )

            client = REDCapClient(sample_project_config)

            with pytest.raises(requests.exceptions.RequestException):
                client.get_metadata()

    def test_resolve_api_token_simple(self):
        """Test resolving simple API token"""
        from services.redcap_client import resolve_api_token

        token = resolve_api_token("simple_token_123")

        assert token == "simple_token_123"

    def test_resolve_api_token_with_env_var(self):
        """Test resolving token with environment variable"""
        import os

        from services.redcap_client import resolve_api_token

        # Set environment variable
        os.environ["REDCAP_API_TOKEN"] = "actual_token_123"

        token = resolve_api_token("${REDCAP_API_TOKEN}")

        assert token == "actual_token_123"

    def test_resolve_api_token_unknown_var(self):
        """Test token resolution with unknown environment variable"""
        from services.redcap_client import resolve_api_token

        # Unknown variable should be left as-is
        token = resolve_api_token("${REDCAP_API_TOKEN_UNKNOWN}")

        assert token == "${REDCAP_API_TOKEN_UNKNOWN}"
