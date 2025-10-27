# fragment-validator/tests/test_subject_id_resolver.py
import pandas as pd
import pytest
from services.subject_id_resolver import SubjectIDResolver


class TestSubjectIDResolver:
    """Unit tests for SubjectIDResolver"""

    def test_resolve_batch_existing_gsids(self, mock_gsid_client):
        """Test resolving subjects with existing GSIDs"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {"consortium_id": ["IBDGC001", "IBDGC002"], "sample_id": ["SMP1", "SMP2"]}
        )

        result = resolver.resolve_batch(
            data, candidate_fields=["consortium_id"], default_center_id=0
        )

        assert len(result["gsids"]) == 2
        assert "GSID-" in result["gsids"][0]
        assert result["summary"]["existing_matches"] >= 1

    def test_resolve_batch_new_gsids(self, mock_gsid_client):
        """Test minting new GSIDs"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame({"consortium_id": ["NEW001", "NEW002", "NEW003"]})

        result = resolver.resolve_batch(
            data, candidate_fields=["consortium_id"], default_center_id=0
        )

        assert len(result["gsids"]) == 3
        assert result["summary"]["new_gsids_minted"] >= 2

    def test_resolve_with_center_id_field(self, mock_gsid_client):
        """Test resolution with explicit center_id field"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame({"consortium_id": ["ID001", "ID002"], "center_id": [1, 2]})

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id"],
            center_id_field="center_id",
            default_center_id=0,
        )

        # Should use provided center_ids, not default
        assert result["summary"]["unknown_center_used"] == 0

    def test_multiple_candidate_fields(self, mock_gsid_client):
        """Test resolution with multiple candidate fields"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["ID001", None, "ID003"],
                "local_id": [None, "LOCAL2", "LOCAL3"],
            }
        )

        result = resolver.resolve_batch(
            data, candidate_fields=["consortium_id", "local_id"], default_center_id=0
        )

        # Should successfully resolve all 3 using first available candidate
        assert len(result["gsids"]) == 3
        assert len(result["local_id_records"]) >= 3

    def test_no_valid_candidate_raises_error(self, mock_gsid_client):
        """Test that missing all candidates raises error"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "sample_id": ["SMP1"]
                # Missing all candidate fields
            }
        )

        with pytest.raises(ValueError, match="No valid subject ID found"):
            resolver.resolve_batch(
                data,
                candidate_fields=["consortium_id", "local_id"],
                default_center_id=0,
            )
