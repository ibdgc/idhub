# fragment-validator/tests/test_subject_id_resolver_integration.py
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

        def mock_register_batch(requests_list, batch_size, timeout):
            """Mock multi-candidate batch registration"""
            results = []
            for req in requests_list:
                # Simulate new subject
                results.append(
                    {
                        "gsid": f"GSID-NEW-{''.join(str(ord(c)) for c in req['identifiers'][0]['local_subject_id'])}",
                        "action": "create_new",
                        "match_strategy": "no_match",
                        "confidence": 1.0,
                        "identifiers_linked": len(req["identifiers"]),
                    }
                )

            return results

        client.register_batch = mock_register_batch
        return client

    def test_resolve_batch_with_multiple_candidates(self, mock_gsid_client, center_resolver):
        """Test resolving batch with multiple candidate IDs per record"""
        resolver = SubjectIDResolver(mock_gsid_client, center_resolver)

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
