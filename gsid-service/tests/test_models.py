# gsid-service/tests/test_models.py
from datetime import date

import pytest
from pydantic import ValidationError

from api.models import (
    HealthResponse,
    IdentifierInput,
    SubjectRegistrationRequest,
    SubjectRegistrationResponse,
)


class TestIdentifierInput:
    """Test IdentifierInput model"""

    def test_valid_identifier(self):
        """Test valid identifier"""
        identifier = IdentifierInput(local_subject_id="LOCAL123", identifier_type="primary")
        assert identifier.local_subject_id == "LOCAL123"
        assert identifier.identifier_type == "primary"

    def test_empty_local_subject_id_raises_error(self):
        """Test that empty local_subject_id raises a validation error"""
        with pytest.raises(ValidationError):
            IdentifierInput(local_subject_id="", identifier_type="primary")

    def test_whitespace_local_subject_id_raises_error(self):
        """Test that whitespace-only local_subject_id raises a validation error"""
        with pytest.raises(ValidationError):
            IdentifierInput(local_subject_id="   ", identifier_type="primary")


class TestSubjectRegistrationRequest:
    """Test SubjectRegistrationRequest model"""

    def test_valid_request(self):
        """Test valid request with all fields"""
        request = SubjectRegistrationRequest(
            center_id=1,
            identifiers=[
                IdentifierInput(local_subject_id="LOCAL123", identifier_type="primary")
            ],
            registration_year=date(2024, 1, 1),
            control=True,
            created_by="test_user",
        )
        assert request.center_id == 1
        assert len(request.identifiers) == 1
        assert request.identifiers[0].local_subject_id == "LOCAL123"
        assert request.registration_year == date(2024, 1, 1)
        assert request.control is True
        assert request.created_by == "test_user"

    def test_request_with_multiple_identifiers(self):
        """Test request with multiple identifiers"""
        request = SubjectRegistrationRequest(
            center_id=1,
            identifiers=[
                IdentifierInput(local_subject_id="LOCAL123", identifier_type="primary"),
                IdentifierInput(local_subject_id="ALIAS456", identifier_type="alias"),
            ],
        )
        assert len(request.identifiers) == 2

    def test_empty_identifiers_list_raises_error(self):
        """Test that an empty identifiers list raises a validation error"""
        with pytest.raises(ValidationError):
            SubjectRegistrationRequest(center_id=1, identifiers=[])


class TestSubjectRegistrationResponse:
    """Test SubjectRegistrationResponse model"""

    def test_valid_response(self):
        """Test valid response"""
        response = SubjectRegistrationResponse(
            gsid="GSID-123",
            action="create_new",
            identifiers_linked=1,
        )
        assert response.gsid == "GSID-123"
        assert response.action == "create_new"
        assert response.identifiers_linked == 1

    def test_response_with_conflicts(self):
        """Test response with conflicts"""
        response = SubjectRegistrationResponse(
            gsid="GSID-123",
            action="conflict_resolved",
            identifiers_linked=1,
            conflicts=["GSID-456"],
            conflict_resolution="used_oldest",
        )
        assert response.conflicts == ["GSID-456"]
        assert response.conflict_resolution == "used_oldest"


class TestHealthResponse:
    """Test HealthResponse model"""

    def test_valid_health_response_healthy(self):
        """Test valid health response when healthy"""
        response = HealthResponse(status="healthy", database="connected")
        assert response.status == "healthy"
        assert response.database == "connected"

    def test_valid_health_response_unhealthy(self):
        """Test valid health response when unhealthy"""
        response = HealthResponse(status="unhealthy", database="disconnected")
        assert response.status == "unhealthy"
        assert response.database == "disconnected"

    def test_health_response_degraded(self):
        """Test health response with degraded status"""
        response = HealthResponse(status="degraded", database="slow")
        assert response.status == "degraded"
        assert response.database == "slow"

    def test_missing_required_fields(self):
        """Test validation error for missing required fields"""
        with pytest.raises(ValidationError):
            HealthResponse(database="connected")
