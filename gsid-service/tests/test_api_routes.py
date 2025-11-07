# gsid-service/tests/test_api_routes.py
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


class TestAPIRoutes:
    """Test API route functionality"""

    @pytest.fixture
    def client(self):
        """Create test client"""
        from main import app

        return TestClient(app)

    @pytest.fixture
    def valid_headers(self):
        """Provide valid API key headers"""
        return {"x-api-key": "test-api-key-12345"}

    @pytest.fixture
    def sample_subject_request(self):
        """Provide sample subject registration request"""
        return {
            "center_id": 1,
            "local_subject_id": "TEST001",
            "identifier_type": "primary",
            "registration_year": "2024-01-01",
            "control": False,
            "created_by": "test_user",
        }

    def test_health_endpoint_healthy(self, client, mock_db_connection):
        """Test health check endpoint when database is healthy"""
        mock_cursor = mock_db_connection.cursor()
        mock_cursor.execute.return_value = None

        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            response = client.get("/health")

            assert response.status_code == 200
            data = response.json()
            assert "status" in data
            assert data["status"] == "healthy"
            assert data["database"] == "connected"

    def test_health_endpoint_unhealthy(self, client):
        """Test health check endpoint when database is down"""
        with patch("api.routes.get_db_connection", side_effect=Exception("DB Error")):
            response = client.get("/health")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "unhealthy"
            assert data["database"] == "disconnected"

    def test_register_subject_new(
        self, client, valid_headers, mock_db_connection, sample_subject_request
    ):
        """Test registering a new subject"""
        mock_cursor = mock_db_connection.cursor()
        # Mock resolve_identity to return create_new action
        mock_cursor.fetchone.return_value = None

        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            with patch("api.routes.resolve_identity") as mock_resolve:
                mock_resolve.return_value = {
                    "action": "create_new",
                    "gsid": None,
                    "match_strategy": "no_match",
                    "confidence": 1.0,
                    "review_reason": None,
                }

                with patch("api.routes.log_resolution"):
                    response = client.post(
                        "/register", headers=valid_headers, json=sample_subject_request
                    )

                    assert response.status_code == 200
                    data = response.json()
                    assert "gsid" in data
                    assert data["gsid"].startswith("GSID-")
                    assert data["action"] == "create_new"
                    assert data["requires_review"] is False

    def test_register_subject_link_existing(
        self,
        client,
        valid_headers,
        mock_db_connection,
        sample_subject_request,
        sample_gsid,
    ):
        """Test linking to existing subject"""
        mock_cursor = mock_db_connection.cursor()

        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            with patch("api.routes.resolve_identity") as mock_resolve:
                mock_resolve.return_value = {
                    "action": "link_existing",
                    "gsid": sample_gsid,
                    "match_strategy": "exact",
                    "confidence": 1.0,
                    "review_reason": None,
                }

                with patch("api.routes.log_resolution"):
                    response = client.post(
                        "/register", headers=valid_headers, json=sample_subject_request
                    )

                    assert response.status_code == 200
                    data = response.json()
                    assert data["gsid"] == sample_gsid
                    assert data["action"] == "link_existing"
                    assert data["requires_review"] is False

    def test_register_subject_review_required(
        self,
        client,
        valid_headers,
        mock_db_connection,
        sample_subject_request,
        sample_gsid,
    ):
        """Test subject requiring review"""
        mock_cursor = mock_db_connection.cursor()

        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            with patch("api.routes.resolve_identity") as mock_resolve:
                mock_resolve.return_value = {
                    "action": "review_required",
                    "gsid": sample_gsid,
                    "match_strategy": "exact_withdrawn",
                    "confidence": 1.0,
                    "review_reason": "Subject previously withdrawn",
                }

                with patch("api.routes.log_resolution"):
                    response = client.post(
                        "/register", headers=valid_headers, json=sample_subject_request
                    )

                    assert response.status_code == 200
                    data = response.json()
                    assert data["gsid"] == sample_gsid
                    assert data["action"] == "review_required"
                    assert data["requires_review"] is True
                    assert data["review_reason"] == "Subject previously withdrawn"

    def test_register_subject_no_auth(self, client, sample_subject_request):
        """Test registration without API key"""
        response = client.post("/register", json=sample_subject_request)

        assert response.status_code == 422  # Missing required header

    def test_register_subject_invalid_auth(self, client, sample_subject_request):
        """Test registration with invalid API key"""
        response = client.post(
            "/register",
            headers={"x-api-key": "invalid-key"},
            json=sample_subject_request,
        )

        assert response.status_code == 403

    def test_register_batch(self, client, valid_headers, mock_db_connection):
        """Test batch registration"""
        batch_request = {
            "requests": [
                {
                    "center_id": 1,
                    "local_subject_id": "TEST001",
                    "identifier_type": "primary",
                    "control": False,
                    "created_by": "test_user",
                },
                {
                    "center_id": 1,
                    "local_subject_id": "TEST002",
                    "identifier_type": "primary",
                    "control": False,
                    "created_by": "test_user",
                },
            ]
        }

        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            with patch("api.routes.resolve_identity") as mock_resolve:
                mock_resolve.return_value = {
                    "action": "create_new",
                    "gsid": None,
                    "match_strategy": "no_match",
                    "confidence": 1.0,
                    "review_reason": None,
                }

                with patch("api.routes.log_resolution"):
                    response = client.post(
                        "/register/batch", headers=valid_headers, json=batch_request
                    )

                    assert response.status_code == 200
                    data = response.json()
                    assert isinstance(data, list)
                    assert len(data) == 2
                    assert all(item["gsid"].startswith("GSID-") for item in data)

    def test_get_review_queue(self, client, mock_db_connection):
        """Test getting review queue"""
        mock_cursor = mock_db_connection.cursor()
        mock_cursor.fetchall.return_value = [
            {
                "global_subject_id": "GSID-0123456789ABCDEF",
                "review_notes": "Potential duplicate",
                "center_name": "Test Center",
                "local_ids": ["TEST001", "TEST002"],
                "created_at": "2024-01-01T00:00:00",
                "withdrawn": False,
            }
        ]

        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            response = client.get("/review-queue")

            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["global_subject_id"] == "GSID-0123456789ABCDEF"

    def test_resolve_review(self, client, mock_db_connection, sample_gsid):
        """Test resolving a review"""
        mock_cursor = mock_db_connection.cursor()

        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            response = client.post(
                f"/resolve-review/{sample_gsid}",
                params={"reviewed_by": "admin", "notes": "Verified as unique"},
            )

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "resolved"
            assert data["gsid"] == sample_gsid

    def test_register_subject_validation_error(self, client, valid_headers):
        """Test registration with invalid data"""
        invalid_request = {
            "center_id": "invalid",  # Should be int
            "local_subject_id": "TEST001",
        }

        response = client.post("/register", headers=valid_headers, json=invalid_request)

        assert response.status_code == 422  # Validation error

    def test_register_subject_database_error(
        self, client, valid_headers, mock_db_connection, sample_subject_request
    ):
        """Test registration with database error during resolution"""
        # Mock resolve_identity to raise an exception
        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            with patch(
                "api.routes.resolve_identity", side_effect=Exception("DB Error")
            ):
                response = client.post(
                    "/register", headers=valid_headers, json=sample_subject_request
                )

                assert response.status_code == 500
                data = response.json()
                assert "detail" in data

    def test_batch_registration_partial_failure(
        self, client, valid_headers, mock_db_connection
    ):
        """Test batch registration with some failures"""
        batch_request = {
            "requests": [
                {
                    "center_id": 1,
                    "local_subject_id": "TEST001",
                    "identifier_type": "primary",
                    "control": False,
                    "created_by": "test_user",
                }
            ]
        }

        with patch("api.routes.get_db_connection", return_value=mock_db_connection):
            with patch(
                "api.routes.resolve_identity", side_effect=Exception("Resolution error")
            ):
                response = client.post(
                    "/register/batch", headers=valid_headers, json=batch_request
                )

                assert response.status_code == 500
