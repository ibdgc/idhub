# fragment-validator/tests/test_multi_candidate_integration.py
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from services.gsid_client import GSIDClient
from services.subject_id_resolver import SubjectIDResolver


class TestMultiCandidateIntegration:
    """Integration tests for multi-candidate subject ID resolution"""

    @pytest.fixture
    def mock_gsid_client(self):
        """Mock GSID client with multi-candidate support"""
        client = Mock(spec=GSIDClient)

        def mock_register_batch_multi_candidate(requests_list):
            """Mock multi-candidate batch registration"""
            results = []
            for req in requests_list:
                center_id = req["center_id"]
                candidate_ids = req["candidate_ids"]

                # Simulate different scenarios based on first candidate ID
                first_id = candidate_ids[0]["local_subject_id"]

                if "CONFLICT" in first_id:
                    # Simulate multi-GSID conflict
                    results.append(
                        {
                            "gsid": None,
                            "matched_gsids": ["GSID-001", "GSID-002"],
                            "action": "review_required",
                            "match_strategy": "multiple_gsid_conflict",
                            "confidence": 0.5,
                            "review_reason": "Multiple GSIDs found for candidate IDs",
                        }
                    )
                elif "WITHDRAWN" in first_id:
                    # Simulate withdrawn subject
                    results.append(
                        {
                            "gsid": "GSID-WITHDRAWN-001",
                            "action": "review_required",
                            "match_strategy": "exact_withdrawn",
                            "confidence": 1.0,
                            "review_reason": "Subject previously withdrawn",
                        }
                    )
                elif "EXISTING" in first_id:
                    # Simulate existing match
                    results.append(
                        {
                            "gsid": f"GSID-{first_id}",
                            "action": "link_existing",
                            "match_strategy": "exact_match",
                            "confidence": 1.0,
                        }
                    )
                elif "PROMOTE" in first_id:
                    # Simulate center promotion
                    results.append(
                        {
                            "gsid": f"GSID-{first_id}",
                            "action": "center_promoted",
                            "match_strategy": "center_promotion",
                            "confidence": 1.0,
                            "previous_center_id": 0,
                            "new_center_id": center_id,
                            "message": "Promoted from Unknown to known center",
                        }
                    )
                elif "INVALID" in first_id:
                    # Simulate validation error
                    results.append(
                        {
                            "gsid": None,
                            "action": "review_required",
                            "match_strategy": "validation_failed",
                            "confidence": 0.0,
                            "review_reason": "ID validation failed",
                            "validation_warnings": [
                                "ID is too short",
                                "Matches error pattern",
                            ],
                        }
                    )
                else:
                    # Simulate new subject
                    results.append(
                        {
                            "gsid": f"GSID-NEW-{first_id}",
                            "action": "create_new",
                            "match_strategy": "no_match",
                            "confidence": 1.0,
                        }
                    )

            return results

        client.register_batch_multi_candidate = mock_register_batch_multi_candidate
        return client

    def test_resolve_batch_with_multiple_candidates(self, mock_gsid_client):
        """Test resolving batch with multiple candidate IDs per record"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["CONS001", "CONS002", "CONS003"],
                "local_id": ["LOCAL001", "LOCAL002", "LOCAL003"],
                "alias": ["ALIAS001", "ALIAS002", "ALIAS003"],
                "sample_id": ["SMP001", "SMP002", "SMP003"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id", "alias"],
            default_center_id=1,
        )

        # Should have GSIDs for all records
        assert len(result["gsids"]) == 3
        assert all(gsid is not None for gsid in result["gsids"])

        # Should have local_id_records for ALL candidate fields
        # 3 records × 3 candidate fields = 9 local_id_records
        assert len(result["local_id_records"]) == 9

        # Verify each record has all three candidate types
        gsid_to_types = {}
        for record in result["local_id_records"]:
            gsid = record["global_subject_id"]
            if gsid not in gsid_to_types:
                gsid_to_types[gsid] = set()
            gsid_to_types[gsid].add(record["identifier_type"])

        for gsid, types in gsid_to_types.items():
            assert "consortium_id" in types
            assert "local_id" in types
            assert "alias" in types

    def test_multi_gsid_conflict_detection(self, mock_gsid_client):
        """Test detection of multi-GSID conflicts"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["CONFLICT001"],
                "local_id": ["LOCAL001"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id"],
            default_center_id=1,
        )

        # Should flag the conflict
        assert result["summary"]["flagged_for_review"] == 1
        assert result["summary"]["multi_gsid_conflicts"] == 1

        # Should have flagged record details
        assert len(result["flagged_records"]) == 1
        flagged = result["flagged_records"][0]
        assert flagged["matched_gsids"] == ["GSID-001", "GSID-002"]
        assert "Multiple GSIDs" in flagged["reason"]

    def test_withdrawn_subject_flagged(self, mock_gsid_client):
        """Test that withdrawn subjects are flagged"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["WITHDRAWN001"],
                "local_id": ["LOCAL001"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id"],
            default_center_id=1,
        )

        # Should flag for review
        assert result["summary"]["flagged_for_review"] == 1

        # Should have flagged record
        assert len(result["flagged_records"]) == 1
        flagged = result["flagged_records"][0]
        assert "withdrawn" in flagged["reason"].lower()

    def test_center_promotion_tracking(self, mock_gsid_client):
        """Test tracking of center promotions"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["PROMOTE001"],
                "local_id": ["LOCAL001"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id"],
            default_center_id=5,
        )

        # Should track promotion
        assert result["summary"]["center_promoted"] == 1
        assert result["summary"]["existing_matches"] == 1

        # Should have warning about promotion
        assert any("promoted" in w.lower() for w in result["warnings"])

    def test_validation_warnings_tracked(self, mock_gsid_client):
        """Test that validation warnings are tracked"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["INVALID001"],
                "local_id": ["LOCAL001"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id"],
            default_center_id=1,
        )

        # Should track validation warnings
        assert result["summary"]["validation_warnings"] == 1
        assert result["summary"]["flagged_for_review"] == 1

        # Should have warning messages
        assert any("validation" in w.lower() for w in result["warnings"])

    def test_mixed_scenarios(self, mock_gsid_client):
        """Test batch with mixed resolution scenarios"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": [
                    "NEW001",
                    "EXISTING001",
                    "CONFLICT001",
                    "PROMOTE001",
                    "WITHDRAWN001",
                ],
                "local_id": [
                    "LOCAL001",
                    "LOCAL002",
                    "LOCAL003",
                    "LOCAL004",
                    "LOCAL005",
                ],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id"],
            default_center_id=1,
        )

        # Check summary statistics
        assert result["summary"]["new_gsids_minted"] == 1  # NEW001
        assert result["summary"]["existing_matches"] == 2  # EXISTING001, PROMOTE001
        assert result["summary"]["center_promoted"] == 1  # PROMOTE001
        assert (
            result["summary"]["flagged_for_review"] == 3
        )  # CONFLICT, PROMOTE, WITHDRAWN
        assert result["summary"]["multi_gsid_conflicts"] == 1  # CONFLICT001

        # Should have 5 GSIDs (some may be None for conflicts)
        assert len(result["gsids"]) == 5

        # Should have flagged records
        assert len(result["flagged_records"]) == 3

    def test_empty_candidate_fields_error(self, mock_gsid_client):
        """Test error when no valid candidate IDs found"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": [None],
                "local_id": [None],
            }
        )

        with pytest.raises(ValueError, match="No valid subject ID found"):
            resolver.resolve_batch(
                data,
                candidate_fields=["consortium_id", "local_id"],
                default_center_id=1,
            )

    def test_partial_candidate_fields(self, mock_gsid_client):
        """Test handling when some candidate fields are missing"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["CONS001", None, "CONS003"],
                "local_id": [None, "LOCAL002", "LOCAL003"],
                "alias": ["ALIAS001", "ALIAS002", None],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id", "alias"],
            default_center_id=1,
        )

        # Should resolve all records
        assert len(result["gsids"]) == 3

        # Should only have records for non-null candidate fields
        # Row 0: consortium_id, alias (2)
        # Row 1: local_id, alias (2)
        # Row 2: consortium_id, local_id (2)
        assert len(result["local_id_records"]) == 6
