# gsid-service/tests/test_multi_candidate_resolution.py
from unittest.mock import Mock, patch

import pytest
from api.models import CandidateID, MultiCandidateSubjectRequest
from services.identity_resolution import resolve_identity_multi_candidate


class TestMultiCandidateResolution:
    """Test multi-candidate identity resolution"""

    @pytest.fixture
    def mock_conn(self):
        """Mock database connection"""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value = cursor
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.fetchone = Mock(return_value=None)
        cursor.fetchall = Mock(return_value=[])
        cursor.execute = Mock()
        cursor.close = Mock()
        return conn

    def test_no_matches_create_new(self, mock_conn):
        """Test creating new subject when no candidates match"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchall.return_value = []

        candidate_ids = [
            {"local_subject_id": "CONS001", "identifier_type": "consortium_id"},
            {"local_subject_id": "LOCAL001", "identifier_type": "local_id"},
        ]

        result = resolve_identity_multi_candidate(
            mock_conn, center_id=1, candidate_ids=candidate_ids
        )

        assert result["action"] == "create_new"
        assert result["match_strategy"] == "no_match"
        assert result["confidence"] == 1.0
        assert result["candidate_ids"] == candidate_ids

    def test_single_gsid_match_link_existing(self, mock_conn):
        """Test linking to existing GSID when one candidate matches"""
        cursor = mock_conn.cursor.return_value

        # First candidate matches
        def mock_execute_side_effect(query, params):
            if "CONS001" in str(params):
                cursor.fetchall.return_value = [
                    {
                        "global_subject_id": "GSID-TEST-001",
                        "local_center_id": 1,
                        "identifier_type": "consortium_id",
                        "subject_center_id": 1,
                        "withdrawn": False,
                        "flagged_for_review": False,
                    }
                ]
            else:
                cursor.fetchall.return_value = []

        cursor.execute.side_effect = mock_execute_side_effect

        candidate_ids = [
            {"local_subject_id": "CONS001", "identifier_type": "consortium_id"},
            {"local_subject_id": "LOCAL001", "identifier_type": "local_id"},
        ]

        result = resolve_identity_multi_candidate(
            mock_conn, center_id=1, candidate_ids=candidate_ids
        )

        assert result["action"] == "link_existing"
        assert result["gsid"] == "GSID-TEST-001"
        assert result["match_strategy"] == "exact_match"
        assert result["confidence"] == 1.0

    def test_multiple_gsid_conflict(self, mock_conn):
        """Test conflict detection when candidates match different GSIDs"""
        cursor = mock_conn.cursor.return_value

        call_count = [0]

        def mock_execute_side_effect(query, params):
            call_count[0] += 1
            if call_count[0] == 1:  # First candidate
                cursor.fetchall.return_value = [
                    {
                        "global_subject_id": "GSID-TEST-001",
                        "local_center_id": 1,
                        "identifier_type": "consortium_id",
                        "subject_center_id": 1,
                        "withdrawn": False,
                        "flagged_for_review": False,
                    }
                ]
            elif call_count[0] == 2:  # Second candidate
                cursor.fetchall.return_value = [
                    {
                        "global_subject_id": "GSID-TEST-002",
                        "local_center_id": 1,
                        "identifier_type": "local_id",
                        "subject_center_id": 1,
                        "withdrawn": False,
                        "flagged_for_review": False,
                    }
                ]
            else:
                cursor.fetchall.return_value = []

        cursor.execute.side_effect = mock_execute_side_effect

        candidate_ids = [
            {"local_subject_id": "CONS001", "identifier_type": "consortium_id"},
            {"local_subject_id": "LOCAL001", "identifier_type": "local_id"},
        ]

        result = resolve_identity_multi_candidate(
            mock_conn, center_id=1, candidate_ids=candidate_ids
        )

        assert result["action"] == "review_required"
        assert result["match_strategy"] == "multiple_gsid_conflict"
        assert "GSID-TEST-001" in result["matched_gsids"]
        assert "GSID-TEST-002" in result["matched_gsids"]
        assert result["confidence"] == 0.5
        assert "Multiple GSIDs found" in result["review_reason"]

    def test_withdrawn_subject_flagged(self, mock_conn):
        """Test that withdrawn subjects are flagged for review"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchall.return_value = [
            {
                "global_subject_id": "GSID-TEST-001",
                "local_center_id": 1,
                "identifier_type": "consortium_id",
                "subject_center_id": 1,
                "withdrawn": True,
                "flagged_for_review": False,
            }
        ]

        candidate_ids = [
            {"local_subject_id": "CONS001", "identifier_type": "consortium_id"},
        ]

        result = resolve_identity_multi_candidate(
            mock_conn, center_id=1, candidate_ids=candidate_ids
        )

        assert result["action"] == "review_required"
        assert result["match_strategy"] == "exact_withdrawn"
        assert result["gsid"] == "GSID-TEST-001"
        assert "withdrawn" in result["review_reason"].lower()

    def test_cross_center_conflict(self, mock_conn):
        """Test cross-center conflict detection"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchall.return_value = [
            {
                "global_subject_id": "GSID-TEST-001",
                "local_center_id": 2,  # Different center
                "identifier_type": "consortium_id",
                "subject_center_id": 2,
                "withdrawn": False,
                "flagged_for_review": False,
            }
        ]

        candidate_ids = [
            {"local_subject_id": "CONS001", "identifier_type": "consortium_id"},
        ]

        result = resolve_identity_multi_candidate(
            mock_conn,
            center_id=3,
            candidate_ids=candidate_ids,  # Attempting from center 3
        )

        assert result["action"] == "review_required"
        assert result["match_strategy"] == "cross_center_conflict"
        assert "center 2" in result["review_reason"]
        assert "center 3" in result["review_reason"]

    def test_center_promotion(self, mock_conn):
        """Test center promotion from Unknown to known center"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchall.return_value = [
            {
                "global_subject_id": "GSID-TEST-001",
                "local_center_id": 0,  # Unknown center
                "identifier_type": "consortium_id",
                "subject_center_id": 0,  # Subject also at Unknown
                "withdrawn": False,
                "flagged_for_review": False,
            }
        ]

        candidate_ids = [
            {"local_subject_id": "CONS001", "identifier_type": "consortium_id"},
        ]

        result = resolve_identity_multi_candidate(
            mock_conn,
            center_id=5,
            candidate_ids=candidate_ids,  # Known center
        )

        assert result["action"] == "center_promoted"
        assert result["match_strategy"] == "center_promotion"
        assert result["gsid"] == "GSID-TEST-001"
        assert result["previous_center_id"] == 0
        assert result["new_center_id"] == 5
        assert "Promoted" in result["message"]

    def test_validation_warnings(self, mock_conn):
        """Test ID validation warnings are included"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchall.return_value = []

        candidate_ids = [
            {"local_subject_id": "123", "identifier_type": "consortium_id"},  # Short ID
            {"local_subject_id": "test", "identifier_type": "local_id"},  # Test ID
        ]

        result = resolve_identity_multi_candidate(
            mock_conn, center_id=1, candidate_ids=candidate_ids
        )

        assert result["action"] == "review_required"
        assert result["match_strategy"] == "validation_failed"
        assert result.get("validation_warnings") is not None
        assert len(result["validation_warnings"]) > 0


class TestIDValidator:
    """Test ID validation logic"""

    def test_valid_id(self):
        """Test validation of valid ID"""
        from services.id_validator import IDValidator

        result = IDValidator.validate_id("IBDGC12345", "consortium_id")

        assert result["valid"] is True
        assert result["severity"] == "info"
        assert len(result["warnings"]) == 0

    def test_short_id_warning(self):
        """Test warning for short IDs"""
        from services.id_validator import IDValidator

        result = IDValidator.validate_id("123", "consortium_id")

        assert result["valid"] is True
        assert result["severity"] == "warning"
        assert any("short" in w.lower() for w in result["warnings"])

    def test_test_id_error(self):
        """Test error for test IDs"""
        from services.id_validator import IDValidator

        result = IDValidator.validate_id("test123", "consortium_id")

        assert result["valid"] is False
        assert result["severity"] == "error"
        assert any("error pattern" in w.lower() for w in result["warnings"])

    def test_all_zeros_error(self):
        """Test error for all zeros"""
        from services.id_validator import IDValidator

        result = IDValidator.validate_id("0000", "consortium_id")

        assert result["valid"] is False
        assert result["severity"] == "error"

    def test_numeric_id_allowed_for_niddk(self):
        """Test numeric IDs are allowed for NIDDK numbers"""
        from services.id_validator import IDValidator

        result = IDValidator.validate_id("12345678", "niddk_no")

        # Should not have numeric warning for niddk_no type
        assert result["valid"] is True
        numeric_warnings = [w for w in result["warnings"] if "numeric" in w.lower()]
        assert len(numeric_warnings) == 0

    def test_whitespace_warning(self):
        """Test warning for whitespace"""
        from services.id_validator import IDValidator

        result = IDValidator.validate_id("  IBDGC123  ", "consortium_id")

        assert result["valid"] is True
        assert result["severity"] == "warning"
        assert any("whitespace" in w.lower() for w in result["warnings"])

    def test_batch_validation(self):
        """Test batch validation"""
        from services.id_validator import IDValidator

        ids = [
            {"id": "IBDGC123", "type": "consortium_id"},
            {"id": "test", "type": "local_id"},
            {"id": "456", "type": "sample_id"},
        ]

        results = IDValidator.validate_batch(ids)

        assert len(results) == 3
        assert results["IBDGC123"]["valid"] is True
        assert results["test"]["valid"] is False
        assert results["456"]["valid"] is True
