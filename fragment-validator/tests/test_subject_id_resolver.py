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
        assert all("GSID-" in gsid for gsid in result["gsids"])
        assert result["summary"]["existing_matches"] >= 0
        assert result["summary"]["new_gsids_minted"] >= 0

    def test_resolve_batch_new_gsids(self, mock_gsid_client):
        """Test minting new GSIDs"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame({"consortium_id": ["NEW001", "NEW002", "NEW003"]})

        result = resolver.resolve_batch(
            data, candidate_fields=["consortium_id"], default_center_id=0
        )

        assert len(result["gsids"]) == 3
        assert len(result["local_id_records"]) >= 3

    def test_multiple_candidate_fields(self, mock_gsid_client):
        """Test resolution with multiple candidate fields"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["ID001", "ID002"],
                "local_id": ["LOCAL1", "LOCAL2"],
                "niddk_no": ["NIDDK1", "NIDDK2"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id", "niddk_no"],
            default_center_id=0,
        )

        # Should create local_id_records for ALL candidate fields present
        assert len(result["local_id_records"]) == 6  # 2 rows × 3 fields

    def test_uses_first_valid_candidate(self, mock_gsid_client):
        """Test that first valid candidate field is used for lookup"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": [None, "ID002"],
                "local_id": ["LOCAL1", "LOCAL2"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id"],
            default_center_id=0,
        )

        # First row should use local_id since consortium_id is null
        assert len(result["gsids"]) == 2

    def test_missing_all_candidates_raises_error(self, mock_gsid_client):
        """Test error when no valid candidate fields found"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": [None, None],
                "local_id": [None, None],
            }
        )

        with pytest.raises(ValueError, match="No valid subject ID found"):
            resolver.resolve_batch(
                data,
                candidate_fields=["consortium_id", "local_id"],
                default_center_id=0,
            )

    def test_explicit_center_id_field(self, mock_gsid_client):
        """Test using explicit center_id from data"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["ID001", "ID002"],
                "center_id": [1, 2],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id"],
            center_id_field="center_id",
            default_center_id=0,
        )

        # Should use center_id from data, not default
        assert result["summary"]["unknown_center_used"] == 0

    def test_default_center_id_when_missing(self, mock_gsid_client):
        """Test default center_id used when field missing or null"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["ID001", "ID002", "ID003"],
                "center_id": [1, None, 2],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id"],
            center_id_field="center_id",
            default_center_id=0,
        )

        # One row should use default center_id
        assert result["summary"]["unknown_center_used"] == 1

    def test_warning_for_unknown_center(self, mock_gsid_client):
        """Test warning generated when unknown center used"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame({"consortium_id": ["ID001", "ID002"]})

        result = resolver.resolve_batch(
            data, candidate_fields=["consortium_id"], default_center_id=0
        )

        assert result["summary"]["unknown_center_used"] == 2
        assert len(result["warnings"]) > 0
        assert "center_id=0" in result["warnings"][0]

    def test_local_id_records_structure(self, mock_gsid_client):
        """Test structure of local_id_records output"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame(
            {
                "consortium_id": ["ID001"],
                "local_id": ["LOCAL1"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id"],
            default_center_id=1,
        )

        assert len(result["local_id_records"]) == 2
        for record in result["local_id_records"]:
            assert "center_id" in record
            assert "local_subject_id" in record
            assert "identifier_type" in record
            assert "global_subject_id" in record
            assert "action" in record

    def test_statistics_tracking(self, mock_gsid_client):
        """Test that statistics are properly tracked"""
        resolver = SubjectIDResolver(mock_gsid_client)

        data = pd.DataFrame({"consortium_id": ["ID001", "ID002", "ID003"]})

        result = resolver.resolve_batch(
            data, candidate_fields=["consortium_id"], default_center_id=0
        )

        summary = result["summary"]
        assert "existing_matches" in summary
        assert "new_gsids_minted" in summary
        assert "unknown_center_used" in summary
        assert "center_promoted" in summary
        assert summary["existing_matches"] + summary["new_gsids_minted"] == 3
