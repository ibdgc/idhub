# gsid-service/tests/test_models.py
from datetime import date

import pytest
from api.models import (
    BatchSubjectRequest,
    HealthResponse,
    ResolutionResponse,
    SubjectRequest,
)
from pydantic import ValidationError


class TestSubjectRequest:
    """Test SubjectRequest model"""

    def test_valid_request_with_all_fields(self):
        """Test valid request with all optional fields"""
        request = SubjectRequest(
            center_id=1,
            local_subject_id="LOCAL123",
            identifier_type="consortium_id",
            registration_year=date(2024, 1, 1),
            control=True,
            created_by="test_user",
        )
        assert request.center_id == 1
        assert request.local_subject_id == "LOCAL123"
        assert request.identifier_type == "consortium_id"
        assert request.registration_year == date(2024, 1, 1)
        assert request.control is True
        assert request.created_by == "test_user"

    def test_valid_request_minimal(self):
        """Test valid request with only required fields"""
        request = SubjectRequest(center_id=1, local_subject_id="LOCAL123")
        assert request.center_id == 1
        assert request.local_subject_id == "LOCAL123"
        assert request.identifier_type == "primary"  # default
        assert request.registration_year is None
        assert request.control is False  # default
        assert request.created_by == "system"  # default

    def test_missing_required_center_id(self):
        """Test validation error when center_id is missing"""
        with pytest.raises(ValidationError) as exc_info:
            SubjectRequest(local_subject_id="LOCAL123")
        assert "center_id" in str(exc_info.value)

    def test_missing_required_local_subject_id(self):
        """Test validation error when local_subject_id is missing"""
        with pytest.raises(ValidationError) as exc_info:
            SubjectRequest(center_id=1)
        assert "local_subject_id" in str(exc_info.value)

    def test_invalid_center_id_type(self):
        """Test validation error for invalid center_id type"""
        with pytest.raises(ValidationError):
            SubjectRequest(center_id="invalid", local_subject_id="LOCAL123")

    def test_registration_year_as_date_object(self):
        """Test registration_year with date object"""
        request = SubjectRequest(
            center_id=1,
            local_subject_id="LOCAL123",
            registration_year=date(2024, 6, 15),
        )
        assert request.registration_year == date(2024, 6, 15)

    def test_registration_year_as_iso_string(self):
        """Test registration_year with ISO date string"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year="2024-06-15"
        )
        assert request.registration_year == date(2024, 6, 15)

    def test_registration_year_as_year_only_string(self):
        """Test registration_year with year-only string (YYYY)"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year="2024"
        )
        assert request.registration_year == date(2024, 1, 1)

    def test_registration_year_as_integer(self):
        """Test registration_year with integer year"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year=2024
        )
        assert request.registration_year == date(2024, 1, 1)

    def test_registration_year_empty_string(self):
        """Test registration_year with empty string returns None"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year=""
        )
        assert request.registration_year is None

    def test_registration_year_whitespace_string(self):
        """Test registration_year with whitespace string returns None"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year="   "
        )
        assert request.registration_year is None

    def test_registration_year_none(self):
        """Test registration_year with None"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year=None
        )
        assert request.registration_year is None

    def test_registration_year_boundary_1900(self):
        """Test registration_year at lower boundary (1900)"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year=1900
        )
        assert request.registration_year == date(1900, 1, 1)

    def test_registration_year_boundary_2100(self):
        """Test registration_year at upper boundary (2100)"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year=2100
        )
        assert request.registration_year == date(2100, 1, 1)

    def test_registration_year_below_boundary(self):
        """Test registration_year below valid range returns None"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year=1899
        )
        assert request.registration_year is None

    def test_registration_year_above_boundary(self):
        """Test registration_year above valid range returns None"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year=2101
        )
        assert request.registration_year is None

    def test_registration_year_invalid_date_string(self):
        """Test registration_year with invalid date string"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year="invalid"
        )
        assert request.registration_year is None

    def test_registration_year_partial_date(self):
        """Test registration_year with partial date string"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", registration_year="2024-06"
        )
        # Should handle YYYY-MM-DD format, so this might return None or parse
        # Based on the validator, it tries to parse first 10 chars
        assert request.registration_year is None

    def test_control_flag_true(self):
        """Test control flag set to True"""
        request = SubjectRequest(center_id=1, local_subject_id="LOCAL123", control=True)
        assert request.control is True

    def test_control_flag_false(self):
        """Test control flag set to False"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", control=False
        )
        assert request.control is False

    def test_identifier_type_custom(self):
        """Test custom identifier_type"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", identifier_type="secondary"
        )
        assert request.identifier_type == "secondary"

    def test_created_by_custom(self):
        """Test custom created_by"""
        request = SubjectRequest(
            center_id=1, local_subject_id="LOCAL123", created_by="admin"
        )
        assert request.created_by == "admin"


class TestResolutionResponse:
    """Test ResolutionResponse model"""

    def test_valid_response_no_review(self):
        """Test valid response without review"""
        response = ResolutionResponse(
            gsid="GSID-001-0001",
            action="linked",
            match_strategy="exact",
            confidence=1.0,
            requires_review=False,
        )
        assert response.gsid == "GSID-001-0001"
        assert response.action == "linked"
        assert response.match_strategy == "exact"
        assert response.confidence == 1.0
        assert response.requires_review is False
        assert response.review_reason is None

    def test_valid_response_with_review(self):
        """Test valid response requiring review"""
        response = ResolutionResponse(
            gsid="GSID-001-0002",
            action="created",
            match_strategy="fuzzy",
            confidence=0.75,
            requires_review=True,
            review_reason="Low confidence match",
        )
        assert response.gsid == "GSID-001-0002"
        assert response.requires_review is True
        assert response.review_reason == "Low confidence match"

    def test_confidence_score_zero(self):
        """Test confidence score at 0.0"""
        response = ResolutionResponse(
            gsid="GSID-001-0003",
            action="created",
            match_strategy="none",
            confidence=0.0,
            requires_review=True,
        )
        assert response.confidence == 0.0

    def test_confidence_score_one(self):
        """Test confidence score at 1.0"""
        response = ResolutionResponse(
            gsid="GSID-001-0004",
            action="linked",
            match_strategy="exact",
            confidence=1.0,
            requires_review=False,
        )
        assert response.confidence == 1.0

    def test_missing_required_fields(self):
        """Test validation error for missing required fields"""
        with pytest.raises(ValidationError):
            ResolutionResponse(
                gsid="GSID-001-0001", action="linked", match_strategy="exact"
            )

    def test_action_types(self):
        """Test different action types"""
        actions = ["created", "linked", "review"]
        for action in actions:
            response = ResolutionResponse(
                gsid="GSID-001-0001",
                action=action,
                match_strategy="exact",
                confidence=1.0,
                requires_review=False,
            )
            assert response.action == action

    def test_match_strategy_types(self):
        """Test different match strategy types"""
        strategies = ["exact", "fuzzy", "probabilistic", "none"]
        for strategy in strategies:
            response = ResolutionResponse(
                gsid="GSID-001-0001",
                action="linked",
                match_strategy=strategy,
                confidence=1.0,
                requires_review=False,
            )
            assert response.match_strategy == strategy


class TestBatchSubjectRequest:
    """Test BatchSubjectRequest model"""

    def test_valid_batch_request(self):
        """Test valid batch request"""
        requests = [
            SubjectRequest(center_id=1, local_subject_id="LOCAL1"),
            SubjectRequest(center_id=1, local_subject_id="LOCAL2"),
        ]
        batch = BatchSubjectRequest(requests=requests)
        assert len(batch.requests) == 2
        assert batch.requests[0].local_subject_id == "LOCAL1"
        assert batch.requests[1].local_subject_id == "LOCAL2"

    def test_empty_batch_request(self):
        """Test empty batch request"""
        batch = BatchSubjectRequest(requests=[])
        assert len(batch.requests) == 0

    def test_batch_with_different_centers(self):
        """Test batch with requests from different centers"""
        requests = [
            SubjectRequest(center_id=1, local_subject_id="LOCAL1"),
            SubjectRequest(center_id=2, local_subject_id="LOCAL2"),
            SubjectRequest(center_id=3, local_subject_id="LOCAL3"),
        ]
        batch = BatchSubjectRequest(requests=requests)
        assert batch.requests[0].center_id == 1
        assert batch.requests[1].center_id == 2
        assert batch.requests[2].center_id == 3

    def test_large_batch_request(self):
        """Test batch with many requests"""
        requests = [
            SubjectRequest(center_id=1, local_subject_id=f"LOCAL{i}")
            for i in range(1000)
        ]
        batch = BatchSubjectRequest(requests=requests)
        assert len(batch.requests) == 1000

    def test_batch_with_mixed_identifier_types(self):
        """Test batch with different identifier types"""
        requests = [
            SubjectRequest(
                center_id=1, local_subject_id="LOCAL1", identifier_type="primary"
            ),
            SubjectRequest(
                center_id=1, local_subject_id="LOCAL2", identifier_type="secondary"
            ),
        ]
        batch = BatchSubjectRequest(requests=requests)
        assert batch.requests[0].identifier_type == "primary"
        assert batch.requests[1].identifier_type == "secondary"

    def test_batch_with_controls_and_cases(self):
        """Test batch with both control and case subjects"""
        requests = [
            SubjectRequest(center_id=1, local_subject_id="CONTROL1", control=True),
            SubjectRequest(center_id=1, local_subject_id="CASE1", control=False),
        ]
        batch = BatchSubjectRequest(requests=requests)
        assert batch.requests[0].control is True
        assert batch.requests[1].control is False

    def test_batch_missing_requests_field(self):
        """Test validation error when requests field is missing"""
        with pytest.raises(ValidationError):
            BatchSubjectRequest()


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
            HealthResponse(status="healthy")

        with pytest.raises(ValidationError):
            HealthResponse(database="connected")


class TestModelSerialization:
    """Test model serialization/deserialization"""

    def test_subject_request_json_serialization(self):
        """Test JSON serialization of SubjectRequest"""
        request = SubjectRequest(
            center_id=1,
            local_subject_id="LOCAL123",
            identifier_type="consortium_id",
            registration_year=date(2024, 1, 1),
        )
        json_data = request.model_dump()
        assert json_data["center_id"] == 1
        assert json_data["local_subject_id"] == "LOCAL123"
        assert json_data["identifier_type"] == "consortium_id"

    def test_resolution_response_json_serialization(self):
        """Test JSON serialization of ResolutionResponse"""
        response = ResolutionResponse(
            gsid="GSID-001-0001",
            action="linked",
            match_strategy="exact",
            confidence=1.0,
            requires_review=False,
        )
        json_data = response.model_dump()
        assert json_data["gsid"] == "GSID-001-0001"
        assert json_data["action"] == "linked"
        assert json_data["confidence"] == 1.0

    def test_batch_request_json_serialization(self):
        """Test JSON serialization of batch request"""
        requests = [
            SubjectRequest(center_id=1, local_subject_id="LOCAL1"),
            SubjectRequest(center_id=1, local_subject_id="LOCAL2"),
        ]
        batch = BatchSubjectRequest(requests=requests)
        json_data = batch.model_dump()
        assert len(json_data["requests"]) == 2

    def test_health_response_json_serialization(self):
        """Test JSON serialization of HealthResponse"""
        response = HealthResponse(status="healthy", database="connected")
        json_data = response.model_dump()
        assert json_data["status"] == "healthy"
        assert json_data["database"] == "connected"

    def test_model_from_dict(self):
        """Test creating model from dictionary"""
        data = {
            "center_id": 1,
            "local_subject_id": "LOCAL123",
            "identifier_type": "consortium_id",
        }
        request = SubjectRequest(**data)
        assert request.center_id == 1
        assert request.local_subject_id == "LOCAL123"

    def test_batch_from_dict(self):
        """Test creating batch from dictionary"""
        data = {
            "requests": [
                {"center_id": 1, "local_subject_id": "LOCAL1"},
                {"center_id": 1, "local_subject_id": "LOCAL2"},
            ]
        }
        batch = BatchSubjectRequest(**data)
        assert len(batch.requests) == 2
