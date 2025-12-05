import pandas as pd
import pytest
from services.subject_id_resolver import SubjectIDResolver


class TestSubjectIDResolver:
    """Unit tests for SubjectIDResolver"""

    def test_resolve_batch_existing_gsids(self, mock_gsid_client, center_resolver):
        """Test resolving subjects with existing GSIDs"""
        resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

        data = pd.DataFrame(
            {"consortium_id": ["IBDGC001", "IBDGC002"], "sample_id": ["SMP1", "SMP2"]}
        )

        result = resolver.resolve_batch(
            data, candidate_fields=["consortium_id"], default_center_id=0
        )

        assert len(result["gsids"]) == 2
        assert all("GSID-" in gsid for gsid in result["gsids"])

    def test_resolve_batch_new_gsids(self, mock_gsid_client, center_resolver):
        """Test minting new GSIDs"""
        resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

        data = pd.DataFrame({"consortium_id": ["NEW001", "NEW002", "NEW003"]})

        result = resolver.resolve_batch(
            data, candidate_fields=["consortium_id"], default_center_id=0
        )

        assert len(result["gsids"]) == 3
        assert len(result["local_id_records"]) >= 3

    def test_multiple_candidate_fields(self, mock_gsid_client, center_resolver):
        """Test resolution with multiple candidate fields"""
        resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

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

    def test_uses_first_valid_candidate(self, mock_gsid_client, center_resolver):
        """Test that first valid candidate field is used for lookup"""
        resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

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

    def test_no_valid_candidates_continues(self, mock_gsid_client, center_resolver):
        """Test that rows with no valid candidate IDs are skipped gracefully"""
        resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

        data = pd.DataFrame(
            {
                "consortium_id": ["ID001", None, "ID003"],
                "local_id": ["LOCAL1", None, "LOCAL3"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id", "local_id"],
            default_center_id=0,
        )

        # Should resolve the two valid rows and skip the one with no IDs
        assert len(result["gsids"]) == 3
        assert result["gsids"][1] is None # The second row had no valid IDs
        assert result["summary"]["resolved"] == 2
        assert result["summary"]["unresolved"] == 1


    def test_explicit_center_name_field(self, mock_gsid_client, center_resolver):
        """Test using explicit center name from data"""
        resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

        data = pd.DataFrame(
            {
                "consortium_id": ["ID001", "ID002"],
                "center_name": ["MSSM", "Cedars-Sinai"],
            }
        )

        result = resolver.resolve_batch(
            data,
            candidate_fields=["consortium_id"],
            center_id_field="center_name",
            default_center_id=0,
        )
        
        # Check that the correct center IDs were used in the request to gsid_client
        # The mock CenterResolver in conftest maps "MSSM" -> 1 and "Cedars-Sinai" -> 2
        gsid_requests = mock_gsid_client.register_batch.call_args[0][0]
        assert gsid_requests[0]['center_id'] == 1
        assert gsid_requests[1]['center_id'] == 2

    def test_local_id_records_structure(self, mock_gsid_client, center_resolver):
        """Test structure of local_id_records output"""
        resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

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
            assert "created_by" in record
