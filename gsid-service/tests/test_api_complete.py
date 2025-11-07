# gsid-service/tests/test_api_complete.py
"""Comprehensive API tests - consolidates test_api.py, test_api_routes.py,
test_api_routes_extended.py, and test_api_routes_additional.py"""

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

            assert response.status_code == 200
            result = response.json()
            assert result["status"] == "unhealthy"
            assert result["database"] == "disconnected"

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
            "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
        )
        assert response.status_code == 422  # Missing required header

    def test_api_key_validation_invalid(self, client_no_auth):
        """Test API endpoint with invalid API key"""
        with patch("core.security.settings") as mock_settings:
            mock_settings.GSID_API_KEY = "correct-key"

            response = client_no_auth.post(
                "/register",
                json={"center_id": 1, "local_subject_id": "TEST001"},
                headers={"X-API-Key": "wrong-key"},
            )
            assert response.status_code == 403

    def test_api_key_validation_valid(self, client):
        """Test API endpoint with valid API key"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.generate_gsid") as mock_gen,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.fetchone = Mock(return_value=None)
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "create_new",
                "gsid": None,
                "confidence": 1.0,
                "match_strategy": "none",
                "review_required": False,
            }
            mock_gen.return_value = "GSID-TEST123456"
            mock_log.return_value = 1

            response = client.post(
                "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
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
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.generate_gsid") as mock_gen,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.fetchone = Mock(return_value=None)
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "create_new",
                "gsid": None,
                "confidence": 1.0,
                "match_strategy": "none",
                "review_required": False,
            }
            mock_gen.return_value = "GSID-NEW123456"
            mock_log.return_value = 1

            response = client.post(
                "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
            )

            assert response.status_code == 200
            result = response.json()
            assert result["gsid"] == "GSID-NEW123456"
            assert result["action"] == "create_new"
            assert result["requires_review"] is False

    def test_register_subject_link_existing(self, client):
        """Test linking to existing subject"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "link_existing",
                "gsid": "GSID-EXISTING123456",
                "confidence": 0.95,
                "match_strategy": "alias_match",
                "review_required": False,
            }
            mock_log.return_value = 1

            response = client.post(
                "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
            )

            assert response.status_code == 200
            result = response.json()
            assert result["gsid"] == "GSID-EXISTING123456"
            assert result["action"] == "link_existing"

    def test_register_subject_review_required(self, client):
        """Test subject requiring review"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "review_required",
                "gsid": "GSID-PENDING123456",
                "confidence": 0.75,
                "match_strategy": "fuzzy_match",
                "review_required": True,
                "review_reason": "Low confidence match",
            }
            mock_log.return_value = 1

            response = client.post(
                "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
            )

            assert response.status_code == 200
            result = response.json()
            assert result["requires_review"] is True

    def test_register_subject_gsid_collision_retry(self, client):
        """Test GSID collision handling with retry"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.generate_gsid") as mock_gen,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.fetchone = Mock(
                side_effect=[
                    {"exists": 1},  # Collision
                    None,  # Success on retry
                ]
            )
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "create_new",
                "gsid": None,
                "confidence": 1.0,
                "match_strategy": "none",
                "review_required": False,
            }
            mock_gen.side_effect = ["GSID-COLLISION", "GSID-SUCCESS"]
            mock_log.return_value = 1

            response = client.post(
                "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
            )

            assert response.status_code == 200
            assert mock_gen.call_count == 2

    def test_register_subject_validation_error(self, client):
        """Test validation error handling"""
        response = client.post(
            "/register", json={"center_id": "invalid", "local_subject_id": "TEST001"}
        )
        assert response.status_code == 422

    def test_register_subject_exception_handling(self, client):
        """Test exception handling in register_subject"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
        ):
            conn = Mock()
            conn.rollback = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.side_effect = Exception("Database error")

            response = client.post(
                "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
            )

            assert response.status_code == 500
            conn.rollback.assert_called_once()


# ============================================================================
# BATCH REGISTRATION TESTS
# ============================================================================


class TestBatchRegistration:
    """Test batch registration endpoint"""

    def test_batch_register_success(self, client):
        """Test successful batch registration"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.generate_gsid") as mock_gen,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.fetchone = Mock(return_value=None)
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "create_new",
                "gsid": None,
                "confidence": 1.0,
                "match_strategy": "none",
                "review_required": False,
            }
            mock_gen.side_effect = ["GSID-001", "GSID-002"]
            mock_log.return_value = 1

            response = client.post(
                "/register/batch",
                json={
                    "requests": [
                        {"center_id": 1, "local_subject_id": "TEST001"},
                        {"center_id": 1, "local_subject_id": "TEST002"},
                    ]
                },
            )

            assert response.status_code == 200
            results = response.json()
            assert len(results) == 2

    def test_batch_register_with_collision(self, client):
        """Test batch registration with GSID collision"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.generate_gsid") as mock_gen,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.fetchone = Mock(
                side_effect=[
                    {"exists": 1},  # Collision
                    None,  # Success
                    None,  # Second subject
                ]
            )
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "create_new",
                "gsid": None,
                "confidence": 1.0,
                "match_strategy": "none",
                "review_required": False,
            }
            mock_gen.side_effect = ["GSID-COLLISION", "GSID-001", "GSID-002"]
            mock_log.return_value = 1

            response = client.post(
                "/register/batch",
                json={
                    "requests": [
                        {"center_id": 1, "local_subject_id": "TEST001"},
                        {"center_id": 1, "local_subject_id": "TEST002"},
                    ]
                },
            )

            assert response.status_code == 200
            assert mock_gen.call_count == 3

    def test_batch_register_link_existing(self, client):
        """Test batch register with link_existing action"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "link_existing",
                "gsid": "GSID-EXISTING",
                "confidence": 0.95,
                "match_strategy": "alias_match",
                "review_required": False,
            }
            mock_log.return_value = 1

            response = client.post(
                "/register/batch",
                json={"requests": [{"center_id": 1, "local_subject_id": "TEST001"}]},
            )

            assert response.status_code == 200
            results = response.json()
            assert results[0]["action"] == "link_existing"

    def test_batch_register_exception_handling(self, client):
        """Test exception handling in batch register"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
        ):
            conn = Mock()
            conn.rollback = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.side_effect = Exception("Batch error")

            response = client.post(
                "/register/batch",
                json={"requests": [{"center_id": 1, "local_subject_id": "TEST001"}]},
            )

            assert response.status_code == 500
            conn.rollback.assert_called_once()


# ============================================================================
# REVIEW QUEUE TESTS
# ============================================================================


class TestReviewQueue:
    """Test review queue endpoints"""

    def test_get_review_queue(self, client):
        """Test get_review_queue endpoint"""
        with patch("api.routes.get_db_connection") as mock_conn:
            mock_cursor = Mock()
            mock_cursor.fetchall = Mock(
                return_value=[
                    {
                        "global_subject_id": "GSID-001",
                        "review_notes": "Duplicate suspected",
                        "center_name": "MSSM",
                        "local_ids": ["LOCAL001"],
                        "created_at": "2024-01-01",
                        "withdrawn": False,
                    }
                ]
            )
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.close = Mock()
            mock_conn.return_value = conn

            response = client.get("/review-queue")

            assert response.status_code == 200
            results = response.json()
            assert len(results) == 1

    def test_resolve_review(self, client):
        """Test resolve_review endpoint"""
        with patch("api.routes.get_db_connection") as mock_conn:
            mock_cursor = Mock()
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            response = client.post(
                "/resolve-review/GSID-001?reviewed_by=admin&notes=Resolved"
            )

            assert response.status_code == 200
            result = response.json()
            assert result["status"] == "resolved"
            assert result["gsid"] == "GSID-001"


# ============================================================================
# RESPONSE FORMAT TESTS
# ============================================================================


class TestResponseFormats:
    """Test API response formats"""

    def test_response_format_structure(self, client):
        """Test response format has required fields"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.generate_gsid") as mock_gen,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.fetchone = Mock(return_value=None)
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "create_new",
                "gsid": None,
                "confidence": 1.0,
                "match_strategy": "none",
                "review_required": False,
            }
            mock_gen.return_value = "GSID-TEST"
            mock_log.return_value = 1

            response = client.post(
                "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
            )

            assert response.status_code == 200
            data = response.json()
            assert "gsid" in data
            assert "action" in data
            assert "confidence_score" in data
            assert "requires_review" in data

    def test_response_format_structure(self, client):
        """Test response format has required fields"""
        with (
            patch("api.routes.get_db_connection") as mock_conn,
            patch("api.routes.resolve_identity") as mock_resolve,
            patch("api.routes.generate_gsid") as mock_gen,
            patch("api.routes.log_resolution") as mock_log,
        ):
            mock_cursor = Mock()
            mock_cursor.fetchone = Mock(return_value=None)
            mock_cursor.execute = Mock()

            conn = Mock()
            conn.cursor = Mock(return_value=mock_cursor)
            conn.commit = Mock()
            conn.close = Mock()
            mock_conn.return_value = conn

            mock_resolve.return_value = {
                "action": "create_new",
                "gsid": None,
                "confidence": 1.0,
                "match_strategy": "none",
                "review_required": False,
            }
            mock_gen.return_value = "GSID-TEST"
            mock_log.return_value = 1

            response = client.post(
                "/register", json={"center_id": 1, "local_subject_id": "TEST001"}
            )

            assert response.status_code == 200
            data = response.json()
            assert "gsid" in data
            assert "action" in data
            assert "confidence" in data  # Changed from "confidence_score"
            assert "requires_review" in data
