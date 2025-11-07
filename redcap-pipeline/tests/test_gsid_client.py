from unittest.mock import MagicMock, patch

import pytest


class TestGSIDClient:
    """Test GSIDClient functionality"""

    def test_register_subject_success(self):
        """Test successful subject registration"""
        from services.gsid_client import GSIDClient

        with patch("requests.Session") as mock_session:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "gsid": "GSID-TEST123456789",
                "action": "create_new",
            }
            mock_response.status_code = 200
            mock_session.return_value.post.return_value = mock_response

            client = GSIDClient()
            result = client.register_subject(
                center_id=1,
                local_subject_id="TEST001",
                identifier_type="primary",
            )

            assert result["gsid"] == "GSID-TEST123456789"
            assert result["action"] == "create_new"

    def test_register_subject_error(self):
        """Test subject registration error handling"""
        from services.gsid_client import GSIDClient
        import requests

        with patch("requests.Session") as mock_session:
            mock_session.return_value.post.side_effect = requests.exceptions.RequestException("Error")

            client = GSIDClient()

            with pytest.raises(requests.exceptions.RequestException):
                client.register_subject(
                    center_id=1,
                    local_subject_id="TEST001",
                )
