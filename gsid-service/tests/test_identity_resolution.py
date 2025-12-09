# gsid-service/tests/test_identity_resolution.py
from unittest.mock import Mock, patch
import pytest
from services.identity_resolution import resolve_subject_with_multiple_ids


@pytest.fixture
def mock_conn():
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


class TestResolveSubjectWithMultipleIds:
    """Test resolve_subject_with_multiple_ids function"""

    def test_create_new_subject(self, mock_conn):
        """Test creating a new subject when no identifiers match"""
        with patch("services.gsid_generator.generate_gsid") as mock_gen_gsid:
            mock_gen_gsid.return_value = "GSID-NEW"
            result = resolve_subject_with_multiple_ids(
                conn=mock_conn,
                center_id=1,
                identifiers=[{"local_subject_id": "NEW-ID", "identifier_type": "primary"}],
            )
            assert result["action"] == "create_new"
            assert result["gsid"] == "GSID-NEW"
            assert result["identifiers_linked"] == 1

    def test_link_to_existing_subject(self, mock_conn):
        """Test linking to an existing subject"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchall.return_value = [
            {
                "global_subject_id": "GSID-EXISTING",
                "created_at": "2023-01-01",
                "subject_center_id": 1,
                "withdrawn": False,
                "identifier_center_id": 1,
            }
        ]

        result = resolve_subject_with_multiple_ids(
            conn=mock_conn,
            center_id=1,
            identifiers=[{"local_subject_id": "EXISTING-ID", "identifier_type": "primary"}],
        )

        assert result["action"] == "link_existing"
        assert result["gsid"] == "GSID-EXISTING"
        assert result["identifiers_linked"] == 1

    def test_center_conflict(self, mock_conn):
        """Test scenario with a center conflict"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchall.return_value = [
            {
                "global_subject_id": "GSID-CONFLICT",
                "created_at": "2023-01-01",
                "subject_center_id": 2,  # Different center
                "withdrawn": False,
                "identifier_center_id": 2,
            }
        ]

        result = resolve_subject_with_multiple_ids(
            conn=mock_conn,
            center_id=1,
            identifiers=[{"local_subject_id": "CONFLICT-ID", "identifier_type": "primary"}],
        )

        assert result["action"] == "link_existing"
        assert result["conflict_resolution"] == "center_mismatch"
        assert "warnings" in result
        assert len(result["warnings"]) > 0

    def test_multi_gsid_conflict(self, mock_conn):
        """Test scenario with multiple GSIDs matching"""
        cursor = mock_conn.cursor.return_value

        # Simulate finding one GSID for the first identifier, and another for the second
        def fetchall_side_effect(*args, **kwargs):
            if "ID-1" in args[0]:
                return [{"global_subject_id": "GSID-1", "created_at": "2023-01-01", "subject_center_id": 1, "withdrawn": False, "identifier_center_id": 1}]
            if "ID-2" in args[0]:
                return [{"global_subject_id": "GSID-2", "created_at": "2023-02-01", "subject_center_id": 1, "withdrawn": False, "identifier_center_id": 1}]
            return []
        cursor.execute.side_effect = fetchall_side_effect
        
        # We need to mock two calls to fetchall, one for each identifier.
        # This is a simplified mock. A more robust mock would inspect the query.
        cursor.fetchall.side_effect = [
            [{"global_subject_id": "GSID-1", "created_at": "2023-01-01", "subject_center_id": 1, "withdrawn": False, "identifier_center_id": 1}],
            [{"global_subject_id": "GSID-2", "created_at": "2023-02-01", "subject_center_id": 1, "withdrawn": False, "identifier_center_id": 1}]
        ]


        result = resolve_subject_with_multiple_ids(
            conn=mock_conn,
            center_id=1,
            identifiers=[
                {"local_subject_id": "ID-1", "identifier_type": "primary"},
                {"local_subject_id": "ID-2", "identifier_type": "primary"},
            ],
        )

        assert result["action"] == "conflict_resolved"
        assert result["conflict_resolution"] == "used_oldest"
        assert result["gsid"] == "GSID-1"
        assert "GSID-1" in result["conflicts"]
        assert "GSID-2" in result["conflicts"]
