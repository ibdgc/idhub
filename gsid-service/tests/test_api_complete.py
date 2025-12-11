# gsid-service/tests/test_api_complete.py
from unittest.mock import Mock, patch

import pytest
from core.security import verify_api_key
from fastapi.testclient import TestClient
from main import app


@pytest.fixture
def client():
    """Create test client with mocked auth"""

    async def mock_verify_api_key():
        return "test-key"

    app.dependency_overrides[verify_api_key] = mock_verify_api_key
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture
def client_no_auth():
    """Create test client without auth override"""
    return TestClient(app)


# ============================================================================
# HEALTH ENDPOINT TESTS
# ============================================================================


class TestHealthEndpoint:
    """Test health check endpoint"""

    def test_health_endpoint_healthy(self, client):
        """Test health endpoint when database is connected"""
        with patch("api.routes.get_db_connection") as mock_conn:
            mock_cursor = Mock()
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.close = Mock()
            mock_conn.return_value = conn

            response = client.get("/health")

            assert response.status_code == 200
            result = response.json()
            assert result["status"] == "healthy"
            assert result["database"] == "connected"

    def test_health_endpoint_unhealthy(self, client):
        """Test health endpoint when database is disconnected"""
        with patch("api.routes.get_db_connection") as mock_conn:
            mock_conn.side_effect = Exception("Database connection failed")

            response = client.get("/health")

            assert response.status_code == 503
            result = response.json()
            assert result["detail"] == "Database connection failed"

    def test_health_check_format(self, client):
        """Test health check response format"""
        with patch("api.routes.get_db_connection"):
            response = client.get("/health")
            assert response.status_code == 200
            data = response.json()
            assert "status" in data
            assert "database" in data

# ============================================================================
# AUTHENTICATION TESTS
# ============================================================================


class TestAuthentication:
    """Test API authentication"""

    def test_api_key_validation_missing(self, client_no_auth):
        """Test API endpoint without API key"""
        response = client_no_auth.post(
            "/register/subject", json={"center_id": 1, "identifiers": [{"local_subject_id": "TEST001"}]}
        )
        assert response.status_code == 422

    def test_api_key_validation_invalid(self, client_no_auth):
        """Test API endpoint with invalid API key"""
        with patch("core.security.settings") as mock_settings:
            mock_settings.GSID_API_KEY = "correct-key"

            response = client_no_auth.post(
                "/register/subject",
                json={"center_id": 1, "identifiers": [{"local_subject_id": "TEST001"}]},
                headers={"X-API-Key": "wrong-key"},
            )
            assert response.status_code == 403

    def test_api_key_validation_valid(self, client):
        """Test API endpoint with valid API key"""
        with (
            patch("api.routes.get_db_connection"),
            patch("services.identity_resolution.resolve_subject_with_multiple_ids") as mock_resolve,
        ):
            mock_resolve.return_value = {
                "gsid": "GSID-TEST123456",
                "action": "create_new",
                "identifiers_linked": 1,
                "conflicts": None,
                "conflict_resolution": None,
                "warnings": [],
            }

            response = client.post(
                "/register/subject",
                json={"center_id": 1, "identifiers": [{"local_subject_id": "TEST001"}]},
            )
            assert response.status_code == 200

# ============================================================================
# REGISTER SUBJECT TESTS
# ============================================================================


class TestRegisterSubject:
    """Test subject registration endpoint"""

    def test_register_subject_create_new(self, client):
        """Test registering a new subject"""
        with (
            patch("api.routes.get_db_connection"),
            patch("services.identity_resolution.resolve_subject_with_multiple_ids") as mock_resolve,
        ):
            mock_resolve.return_value = {
                "gsid": "GSID-NEW123456",
                "action": "create_new",
                "identifiers_linked": 1,
                "conflicts": None,
                "conflict_resolution": None,
                "warnings": [],
            }

            response = client.post(
                "/register/subject",
                json={"center_id": 1, "identifiers": [{"local_subject_id": "TEST001"}]},
            )

            assert response.status_code == 200
            result = response.json()
            assert result["gsid"] == "GSID-NEW123456"
            assert result["action"] == "create_new"

    def test_register_subject_link_existing(self, client):
        """Test linking to existing subject"""
        with (
            patch("api.routes.get_db_connection"),
            patch("services.identity_resolution.resolve_subject_with_multiple_ids") as mock_resolve,
        ):
            mock_resolve.return_value = {
                "gsid": "GSID-EXISTING123",
                "action": "link_existing",
                "identifiers_linked": 1,
                "conflicts": None,
                "conflict_resolution": None,
                "warnings": [],
            }

            response = client.post(
                "/register/subject",
                json={"center_id": 1, "identifiers": [{"local_subject_id": "TEST001"}]},
            )

            assert response.status_code == 200
            result = response.json()
            assert result["gsid"] == "GSID-EXISTING123"
            assert result["action"] == "link_existing"

    def test_register_subject_validation_error(self, client):
        """Test validation error handling"""
        response = client.post("/register/subject", json={"center_id": "invalid"})
        assert response.status_code == 422
