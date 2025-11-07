# gsid-service/tests/test_identity_resolution.py
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from api.models import SubjectRequest
from services.identity_resolution import log_resolution, resolve_identity


class TestResolveIdentity:
    """Test resolve_identity function"""

    @pytest.fixture
    def mock_conn(self):
        """Mock database connection"""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value = cursor
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.fetchone = Mock(return_value=None)
        cursor.execute = Mock()
        cursor.close = Mock()
        return conn

    def test_exact_match_active_subject(self, mock_conn):
        """Test exact match with active subject"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.return_value = {
            "global_subject_id": "GSID-001-0001",
            "withdrawn": False,
            "identifier_type": "primary",
        }

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="LOCAL123")

        assert result["action"] == "link_existing"
        assert result["gsid"] == "GSID-001-0001"
        assert result["match_strategy"] == "exact (original type: primary)"
        assert result["confidence"] == 1.0
        assert result["review_reason"] is None

    def test_exact_match_withdrawn_subject(self, mock_conn):
        """Test exact match with withdrawn subject"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.return_value = {
            "global_subject_id": "GSID-001-0001",
            "withdrawn": True,
            "identifier_type": "primary",
        }

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="LOCAL123")

        assert result["action"] == "review_required"
        assert result["gsid"] == "GSID-001-0001"
        assert "exact_withdrawn" in result["match_strategy"]
        assert result["confidence"] == 1.0
        assert result["review_reason"] == "Subject previously withdrawn"

    def test_alias_match_active_subject(self, mock_conn):
        """Test alias match with active subject"""
        cursor = mock_conn.cursor.return_value
        # First query returns None (no exact match)
        # Second query returns alias match
        cursor.fetchone.side_effect = [
            None,
            {"global_subject_id": "GSID-001-0002", "withdrawn": False},
        ]

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="ALIAS123")

        assert result["action"] == "link_existing"
        assert result["gsid"] == "GSID-001-0002"
        assert result["match_strategy"] == "alias"
        assert result["confidence"] == 0.95
        assert result["review_reason"] is None

    def test_alias_match_withdrawn_subject(self, mock_conn):
        """Test alias match with withdrawn subject"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [
            None,
            {"global_subject_id": "GSID-001-0002", "withdrawn": True},
        ]

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="ALIAS123")

        assert result["action"] == "review_required"
        assert result["gsid"] == "GSID-001-0002"
        assert result["match_strategy"] == "alias_withdrawn"
        assert result["confidence"] == 1.0
        assert result["review_reason"] == "Alias matches withdrawn subject"

    def test_no_match_create_new(self, mock_conn):
        """Test no match found - create new subject"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]  # No exact, no alias

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="NEW123")

        assert result["action"] == "create_new"
        assert result["gsid"] is None
        assert result["match_strategy"] == "no_match"
        assert result["confidence"] == 1.0
        assert result["review_reason"] is None

    def test_different_identifier_types_same_local_id(self, mock_conn):
        """Test that same local_id maps to same GSID regardless of identifier_type"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.return_value = {
            "global_subject_id": "GSID-001-0001",
            "withdrawn": False,
            "identifier_type": "primary",
        }

        # First call with primary
        result1 = resolve_identity(
            mock_conn,
            center_id=1,
            local_subject_id="LOCAL123",
            identifier_type="primary",
        )

        # Second call with secondary - should get same GSID
        cursor.fetchone.return_value = {
            "global_subject_id": "GSID-001-0001",
            "withdrawn": False,
            "identifier_type": "secondary",
        }

        result2 = resolve_identity(
            mock_conn,
            center_id=1,
            local_subject_id="LOCAL123",
            identifier_type="secondary",
        )

        assert result1["gsid"] == result2["gsid"]
        assert result1["action"] == "link_existing"
        assert result2["action"] == "link_existing"

    def test_query_parameters_exact_match(self, mock_conn):
        """Test that exact match query uses correct parameters"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.return_value = None

        resolve_identity(mock_conn, center_id=5, local_subject_id="LOCAL999")

        # Check first execute call (exact match query)
        first_call = cursor.execute.call_args_list[0]
        query = first_call[0][0]
        params = first_call[0][1]

        assert "local_subject_ids" in query
        assert "subjects" in query
        assert params == (5, "LOCAL999")

    def test_query_parameters_alias_match(self, mock_conn):
        """Test that alias match query uses correct parameters"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]  # No exact, no alias

        resolve_identity(mock_conn, center_id=1, local_subject_id="ALIAS123")

        # Check second execute call (alias query)
        second_call = cursor.execute.call_args_list[1]
        query = second_call[0][0]
        params = second_call[0][1]

        assert "subject_alias" in query
        assert params == ("ALIAS123",)

    def test_cursor_closed_on_success(self, mock_conn):
        """Test that cursor is closed after successful execution"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.return_value = None

        resolve_identity(mock_conn, center_id=1, local_subject_id="LOCAL123")

        cursor.close.assert_called_once()

    def test_cursor_closed_on_error(self, mock_conn):
        """Test that cursor is closed even on error"""
        cursor = mock_conn.cursor.return_value
        cursor.execute.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            resolve_identity(mock_conn, center_id=1, local_subject_id="LOCAL123")

        cursor.close.assert_called_once()

    def test_special_characters_in_local_id(self, mock_conn):
        """Test handling special characters in local_subject_id"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        result = resolve_identity(
            mock_conn, center_id=1, local_subject_id="LOCAL-123_ABC@TEST.COM"
        )

        assert result["action"] == "create_new"

    def test_unicode_in_local_id(self, mock_conn):
        """Test handling Unicode characters in local_subject_id"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        result = resolve_identity(
            mock_conn, center_id=1, local_subject_id="LOCAL_测试_123"
        )

        assert result["action"] == "create_new"

    def test_very_long_local_id(self, mock_conn):
        """Test handling very long local_subject_id"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        long_id = "LOCAL" + "X" * 500
        result = resolve_identity(mock_conn, center_id=1, local_subject_id=long_id)

        assert result["action"] == "create_new"

    def test_large_center_id(self, mock_conn):
        """Test handling large center_id"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        result = resolve_identity(
            mock_conn, center_id=999999, local_subject_id="LOCAL123"
        )

        assert result["action"] == "create_new"

    def test_default_identifier_type(self, mock_conn):
        """Test default identifier_type is 'primary'"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        # Call without identifier_type parameter
        result = resolve_identity(mock_conn, center_id=1, local_subject_id="LOCAL123")

        assert result["action"] == "create_new"

    def test_custom_identifier_type(self, mock_conn):
        """Test custom identifier_type"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        result = resolve_identity(
            mock_conn,
            center_id=1,
            local_subject_id="LOCAL123",
            identifier_type="secondary",
        )

        assert result["action"] == "create_new"

    def test_match_strategy_includes_original_type(self, mock_conn):
        """Test that match_strategy includes original identifier_type"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.return_value = {
            "global_subject_id": "GSID-001-0001",
            "withdrawn": False,
            "identifier_type": "consortium_id",
        }

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="LOCAL123")

        assert "consortium_id" in result["match_strategy"]


class TestLogResolution:
    """Test log_resolution function"""

    @pytest.fixture
    def mock_conn(self):
        """Mock database connection"""
        conn = Mock()
        return conn

    @pytest.fixture
    def mock_cursor(self):
        """Mock database cursor"""
        cursor = Mock()
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.fetchone = Mock(return_value={"resolution_id": 123})
        cursor.execute = Mock()
        return cursor

    def test_log_resolution_with_pydantic_model(self, mock_conn, mock_cursor):
        """Test logging resolution with Pydantic model"""
        with patch("services.identity_resolution.get_db_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor

            request = SubjectRequest(center_id=1, local_subject_id="LOCAL123")
            resolution = {
                "action": "link_existing",
                "gsid": "GSID-001-0001",
                "match_strategy": "exact",
                "confidence": 1.0,
                "review_reason": None,
            }

            result = log_resolution(mock_conn, resolution, request)

            assert result == 123
            mock_cursor.execute.assert_called_once()

    def test_log_resolution_with_dict(self, mock_conn, mock_cursor):
        """Test logging resolution with dict request"""
        with patch("services.identity_resolution.get_db_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor

            request = {"center_id": 1, "local_subject_id": "LOCAL123"}
            resolution = {
                "action": "create_new",
                "gsid": "GSID-001-0002",
                "match_strategy": "no_match",
                "confidence": 1.0,
                "review_reason": None,
            }

            result = log_resolution(mock_conn, resolution, request)

            assert result == 123

    def test_log_resolution_review_required(self, mock_conn, mock_cursor):
        """Test logging resolution that requires review"""
        with patch("services.identity_resolution.get_db_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor

            request = SubjectRequest(center_id=1, local_subject_id="LOCAL123")
            resolution = {
                "action": "review_required",
                "gsid": "GSID-001-0001",
                "match_strategy": "exact_withdrawn",
                "confidence": 1.0,
                "review_reason": "Subject previously withdrawn",
            }

            result = log_resolution(mock_conn, resolution, request)

            # Check that requires_review was set to True
            call_args = mock_cursor.execute.call_args[0]
            params = call_args[1]
            requires_review = params[6]  # 7th parameter
            review_reason = params[7]  # 8th parameter

            assert requires_review is True
            assert review_reason == "Subject previously withdrawn"

    def test_log_resolution_no_review(self, mock_conn, mock_cursor):
        """Test logging resolution that doesn't require review"""
        with patch("services.identity_resolution.get_db_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor

            request = SubjectRequest(center_id=1, local_subject_id="LOCAL123")
            resolution = {
                "action": "link_existing",
                "gsid": "GSID-001-0001",
                "match_strategy": "exact",
                "confidence": 1.0,
                "review_reason": None,
            }

            result = log_resolution(mock_conn, resolution, request)

            # Check that requires_review was set to False
            call_args = mock_cursor.execute.call_args[0]
            params = call_args[1]
            requires_review = params[6]

            assert requires_review is False

    def test_log_resolution_insert_query(self, mock_conn, mock_cursor):
        """Test that log_resolution uses correct INSERT query"""
        with patch("services.identity_resolution.get_db_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor

            request = SubjectRequest(center_id=1, local_subject_id="LOCAL123")
            resolution = {
                "action": "create_new",
                "gsid": "GSID-001-0001",
                "match_strategy": "no_match",
                "confidence": 1.0,
                "review_reason": None,
            }

            log_resolution(mock_conn, resolution, request)

            call_args = mock_cursor.execute.call_args[0]
            query = call_args[0]

            assert "INSERT INTO identity_resolutions" in query
            assert "input_center_id" in query
            assert "input_local_id" in query
            assert "matched_gsid" in query
            assert "RETURNING resolution_id" in query

    def test_log_resolution_parameters(self, mock_conn, mock_cursor):
        """Test that log_resolution passes correct parameters"""
        with patch("services.identity_resolution.get_db_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor

            request = SubjectRequest(center_id=5, local_subject_id="LOCAL999")
            resolution = {
                "action": "link_existing",
                "gsid": "GSID-005-0123",
                "match_strategy": "alias",
                "confidence": 0.95,
                "review_reason": None,
            }

            log_resolution(mock_conn, resolution, request)

            call_args = mock_cursor.execute.call_args[0]
            params = call_args[1]

            assert params[0] == 5  # center_id
            assert params[1] == "LOCAL999"  # local_id
            assert params[2] == "GSID-005-0123"  # gsid
            assert params[3] == "link_existing"  # action
            assert params[4] == "alias"  # match_strategy
            assert params[5] == 0.95  # confidence
            assert params[6] is False  # requires_review
            assert params[7] is None  # review_reason

    def test_log_resolution_with_review_reason(self, mock_conn, mock_cursor):
        """Test logging with review reason"""
        with patch("services.identity_resolution.get_db_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor

            request = SubjectRequest(center_id=1, local_subject_id="LOCAL123")
            resolution = {
                "action": "review_required",
                "gsid": "GSID-001-0001",
                "match_strategy": "fuzzy",
                "confidence": 0.75,
                "review_reason": "Low confidence match",
            }

            log_resolution(mock_conn, resolution, request)

            call_args = mock_cursor.execute.call_args[0]
            params = call_args[1]

            assert params[7] == "Low confidence match"

    def test_log_resolution_returns_id(self, mock_conn, mock_cursor):
        """Test that log_resolution returns resolution_id"""
        with patch("services.identity_resolution.get_db_cursor") as mock_get_cursor:
            mock_cursor.fetchone.return_value = {"resolution_id": 456}
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor

            request = SubjectRequest(center_id=1, local_subject_id="LOCAL123")
            resolution = {
                "action": "create_new",
                "gsid": "GSID-001-0001",
                "match_strategy": "no_match",
                "confidence": 1.0,
                "review_reason": None,
            }

            result = log_resolution(mock_conn, resolution, request)

            assert result == 456


class TestIdentityResolutionEdgeCases:
    """Test edge cases and error conditions"""

    @pytest.fixture
    def mock_conn(self):
        """Mock database connection"""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value = cursor
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.fetchone = Mock(return_value=None)
        cursor.execute = Mock()
        cursor.close = Mock()
        return conn

    def test_empty_local_subject_id(self, mock_conn):
        """Test handling empty local_subject_id"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="")

        assert result["action"] == "create_new"

    def test_whitespace_local_subject_id(self, mock_conn):
        """Test handling whitespace-only local_subject_id"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="   ")

        assert result["action"] == "create_new"

    def test_zero_center_id(self, mock_conn):
        """Test handling zero center_id"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        result = resolve_identity(mock_conn, center_id=0, local_subject_id="LOCAL123")

        assert result["action"] == "create_new"

    def test_negative_center_id(self, mock_conn):
        """Test handling negative center_id"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.side_effect = [None, None]

        result = resolve_identity(mock_conn, center_id=-1, local_subject_id="LOCAL123")

        assert result["action"] == "create_new"

    def test_database_error_on_exact_match(self, mock_conn):
        """Test handling database error during exact match query"""
        cursor = mock_conn.cursor.return_value
        cursor.execute.side_effect = Exception("Database connection lost")

        with pytest.raises(Exception, match="Database connection lost"):
            resolve_identity(mock_conn, center_id=1, local_subject_id="LOCAL123")

        # Cursor should still be closed
        cursor.close.assert_called_once()

    def test_null_gsid_in_result(self, mock_conn):
        """Test handling NULL GSID in database result"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.return_value = {
            "global_subject_id": None,
            "withdrawn": False,
            "identifier_type": "primary",
        }

        result = resolve_identity(mock_conn, center_id=1, local_subject_id="LOCAL123")

        # Should still return the result
        assert result["action"] == "link_existing"
        assert result["gsid"] is None
