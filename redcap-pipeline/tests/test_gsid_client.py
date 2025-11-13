# redcap-pipeline/tests/test_gsid_client.py
from datetime import date
from unittest.mock import Mock, patch

import pytest
import requests
from services.gsid_client import GSIDClient


class TestGSIDClient:
    """Test GSID client functionality"""

    def test_register_subject_success(self):
        """Test successful subject registration"""
        with patch("requests.Session.post") as mock_post:
            mock_response = Mock()
            mock_response.json.return_value = {
                "gsid": "GSID-TEST001",
                "local_subject_id": "LOCAL123",
                "identifier_type": "consortium_id",
                "center_id": 1,
                "action": "create_new",
                "match_strategy": "no_match",
                "confidence": 1.0,
            }
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response

            client = GSIDClient()
            result = client.register_subject(
                center_id=1,
                local_subject_id="LOCAL123",
                identifier_type="consortium_id",
                registration_year=date(2024, 1, 1),
                control=False,
            )

            assert result["gsid"] == "GSID-TEST001"
            assert result["action"] == "create_new"
            assert result["match_strategy"] == "no_match"

    def test_register_batch_success(self):
        """Test batch registration"""
        with patch("requests.Session.post") as mock_post:
            mock_response = Mock()
            mock_response.json.return_value = [
                {
                    "gsid": "GSID-001",
                    "local_subject_id": "ID001",
                    "identifier_type": "consortium_id",
                    "center_id": 1,
                    "action": "create_new",
                },
                {
                    "gsid": "GSID-002",
                    "local_subject_id": "ID002",
                    "identifier_type": "consortium_id",
                    "center_id": 1,
                    "action": "link_existing",
                },
            ]
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response

            client = GSIDClient()
            subjects = [
                {
                    "center_id": 1,
                    "local_subject_id": "ID001",
                    "identifier_type": "consortium_id",
                },
                {
                    "center_id": 1,
                    "local_subject_id": "ID002",
                    "identifier_type": "consortium_id",
                },
            ]

            results = client.register_batch(subjects)

            assert len(results) == 2
            assert results[0]["gsid"] == "GSID-001"
            assert results[1]["gsid"] == "GSID-002"

    def test_register_multi_candidate_success(self):
        """Test multi-candidate registration"""
        with patch("requests.Session.post") as mock_post:
            mock_response = Mock()
            mock_response.json.return_value = {
                "gsid": "GSID-MULTI001",
                "candidate_ids": [
                    {"local_subject_id": "CONS001", "identifier_type": "consortium_id"},
                    {"local_subject_id": "LOCAL001", "identifier_type": "local_id"},
                ],
                "center_id": 1,
                "action": "create_new",
                "match_strategy": "no_match",
                "confidence": 1.0,
            }
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response

            client = GSIDClient()
            candidate_ids = [
                {"local_subject_id": "CONS001", "identifier_type": "consortium_id"},
                {"local_subject_id": "LOCAL001", "identifier_type": "local_id"},
            ]

            result = client.register_multi_candidate(
                center_id=1,
                candidate_ids=candidate_ids,
                registration_year=date(2024, 1, 1),
                control=False,
            )

            assert result["gsid"] == "GSID-MULTI001"
            assert len(result["candidate_ids"]) == 2

    def test_register_subject_api_error(self):
        """Test handling of API errors"""
        with patch("requests.Session.post") as mock_post:
            mock_post.side_effect = requests.exceptions.RequestException("API Error")

            client = GSIDClient()

            with pytest.raises(requests.exceptions.RequestException):
                client.register_subject(
                    center_id=1,
                    local_subject_id="LOCAL123",
                    identifier_type="consortium_id",
                )
