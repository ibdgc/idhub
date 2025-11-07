# gsid-service/tests/test_gsid_generator.py
import re
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from services.gsid_generator import (
    BASE32_ALPHABET,
    encode_base32,
    generate_gsid,
    generate_unique_gsids,
    reserve_gsids,
)


class TestEncodeBase32:
    """Test encode_base32 function"""

    def test_encode_zero(self):
        """Test encoding zero"""
        result = encode_base32(0, length=5)
        assert result == "00000"
        assert len(result) == 5

    def test_encode_small_number(self):
        """Test encoding small number"""
        result = encode_base32(1, length=5)
        assert len(result) == 5
        assert result[-1] == "1"  # Last char should be 1
        assert result.startswith("0000")  # Padded with zeros

    def test_encode_large_number(self):
        """Test encoding large number"""
        result = encode_base32(1000000, length=11)
        assert len(result) == 11
        assert all(c in BASE32_ALPHABET for c in result)

    def test_encode_max_5_chars(self):
        """Test encoding maximum value for 5 characters"""
        max_val = 32**5 - 1  # Maximum for 5 base32 chars
        result = encode_base32(max_val, length=5)
        assert len(result) == 5
        assert result == "ZZZZZ"  # All max chars

    def test_encode_uses_custom_alphabet(self):
        """Test that encoding uses custom alphabet without ambiguous chars"""
        result = encode_base32(100000, length=8)
        # Should not contain I, L, O, U
        assert "I" not in result
        assert "L" not in result
        assert "O" not in result
        assert "U" not in result

    def test_encode_padding(self):
        """Test that result is padded to specified length"""
        for length in [3, 5, 8, 11, 16]:
            result = encode_base32(42, length=length)
            assert len(result) == length

    def test_encode_different_lengths(self):
        """Test encoding same number with different lengths"""
        num = 12345
        result_5 = encode_base32(num, length=5)
        result_10 = encode_base32(num, length=10)
        assert len(result_5) == 5
        assert len(result_10) == 10
        # Longer one should have more leading zeros
        assert result_10.endswith(result_5.lstrip("0"))

    def test_encode_sequential_numbers(self):
        """Test encoding sequential numbers produces different results"""
        results = [encode_base32(i, length=5) for i in range(10)]
        assert len(set(results)) == 10  # All unique

    def test_encode_base32_alphabet_length(self):
        """Test that BASE32_ALPHABET has exactly 32 characters"""
        assert len(BASE32_ALPHABET) == 32

    def test_encode_base32_alphabet_no_duplicates(self):
        """Test that BASE32_ALPHABET has no duplicate characters"""
        assert len(set(BASE32_ALPHABET)) == 32


class TestGenerateGSID:
    """Test generate_gsid function"""

    def test_gsid_format(self):
        """Test GSID has correct format"""
        gsid = generate_gsid()
        assert gsid.startswith("GSID-")
        assert len(gsid) == 21  # GSID- (5) + 16 chars

    def test_gsid_structure(self):
        """Test GSID structure matches expected pattern"""
        gsid = generate_gsid()
        pattern = r"^GSID-[0-9A-Z]{16}$"
        assert re.match(pattern, gsid)

    def test_gsid_no_ambiguous_chars(self):
        """Test GSID doesn't contain ambiguous characters"""
        gsid = generate_gsid()
        id_part = gsid[5:]  # Remove "GSID-" prefix
        assert "I" not in id_part
        assert "L" not in id_part
        assert "O" not in id_part
        assert "U" not in id_part

    def test_gsid_uniqueness(self):
        """Test that multiple GSIDs are unique"""
        gsids = [generate_gsid() for _ in range(100)]
        assert len(set(gsids)) == 100

    def test_gsid_timestamp_part(self):
        """Test that GSID contains timestamp component"""
        gsid1 = generate_gsid()
        time.sleep(0.001)  # Wait 1ms
        gsid2 = generate_gsid()
        # GSIDs should be different due to timestamp
        assert gsid1 != gsid2

    def test_gsid_sortability(self):
        """Test that GSIDs are lexicographically sortable by time"""
        gsids = []
        for _ in range(10):
            gsids.append(generate_gsid())
            time.sleep(0.002)  # Small delay

        # GSIDs should be in ascending order
        sorted_gsids = sorted(gsids)
        assert gsids == sorted_gsids

    def test_gsid_random_part_varies(self):
        """Test that random part varies even in same millisecond"""
        # Generate many GSIDs quickly
        gsids = [generate_gsid() for _ in range(50)]
        # All should be unique despite potentially same timestamp
        assert len(set(gsids)) == 50

    def test_gsid_uses_base32_alphabet(self):
        """Test that GSID uses only characters from BASE32_ALPHABET"""
        gsid = generate_gsid()
        id_part = gsid[5:]  # Remove prefix
        assert all(c in BASE32_ALPHABET for c in id_part)

    def test_gsid_timestamp_encoding(self):
        """Test that timestamp is encoded in first 5 chars after prefix"""
        gsid = generate_gsid()
        timestamp_part = gsid[5:10]  # First 5 chars after GSID-
        assert len(timestamp_part) == 5
        assert all(c in BASE32_ALPHABET for c in timestamp_part)

    def test_gsid_random_encoding(self):
        """Test that random part is last 11 chars"""
        gsid = generate_gsid()
        random_part = gsid[10:]  # Last 11 chars
        assert len(random_part) == 11
        assert all(c in BASE32_ALPHABET for c in random_part)


class TestGenerateUniqueGSIDs:
    """Test generate_unique_gsids function"""

    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection"""
        conn = Mock()
        cursor = Mock()
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.fetchone = Mock(return_value=None)  # No duplicates
        cursor.execute = Mock()
        conn.cursor = Mock(return_value=cursor)
        conn.commit = Mock()
        conn.rollback = Mock()
        conn.close = Mock()
        return conn

    def test_generate_single_gsid(self, mock_db_connection):
        """Test generating single GSID"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.fetchone = Mock(return_value=None)
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                gsids = generate_unique_gsids(1)

                assert len(gsids) == 1
                assert gsids[0].startswith("GSID-")
                mock_db_connection.commit.assert_called_once()

    def test_generate_multiple_gsids(self, mock_db_connection):
        """Test generating multiple GSIDs"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.fetchone = Mock(return_value=None)
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                gsids = generate_unique_gsids(10)

                assert len(gsids) == 10
                assert len(set(gsids)) == 10  # All unique
                mock_db_connection.commit.assert_called_once()

    def test_generate_max_gsids(self, mock_db_connection):
        """Test generating maximum allowed GSIDs"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.fetchone = Mock(return_value=None)
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                gsids = generate_unique_gsids(1000)

                assert len(gsids) == 1000
                mock_db_connection.commit.assert_called_once()

    def test_invalid_count_zero(self):
        """Test that count=0 raises ValueError"""
        with pytest.raises(ValueError, match="Count must be between 1 and 1000"):
            generate_unique_gsids(0)

    def test_invalid_count_negative(self):
        """Test that negative count raises ValueError"""
        with pytest.raises(ValueError, match="Count must be between 1 and 1000"):
            generate_unique_gsids(-5)

    def test_invalid_count_too_large(self):
        """Test that count > 1000 raises ValueError"""
        with pytest.raises(ValueError, match="Count must be between 1 and 1000"):
            generate_unique_gsids(1001)

    def test_handles_duplicate_collision(self, mock_db_connection):
        """Test handling of duplicate GSID collision"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                # First call returns duplicate, second returns None
                cursor.fetchone = Mock(side_effect=[{"gsid": "exists"}, None, None])
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                gsids = generate_unique_gsids(2)

                assert len(gsids) == 2
                # Should have made extra attempts due to collision
                assert cursor.execute.call_count > 2

    def test_database_error_rollback(self, mock_db_connection):
        """Test that database errors trigger rollback"""
        mock_db_connection.commit.side_effect = Exception("Database error")

        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.fetchone = Mock(return_value=None)
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                with pytest.raises(Exception, match="Database error"):
                    generate_unique_gsids(5)

                mock_db_connection.rollback.assert_called_once()

    def test_connection_closed_on_success(self, mock_db_connection):
        """Test that connection is closed after success"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.fetchone = Mock(return_value=None)
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                generate_unique_gsids(3)

                mock_db_connection.close.assert_called_once()

    def test_connection_closed_on_error(self, mock_db_connection):
        """Test that connection is closed even on error"""
        mock_db_connection.commit.side_effect = Exception("Error")

        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.fetchone = Mock(return_value=None)
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                with pytest.raises(Exception):
                    generate_unique_gsids(1)

                mock_db_connection.close.assert_called_once()

    def test_inserts_into_gsid_registry(self, mock_db_connection):
        """Test that GSIDs are inserted into gsid_registry table"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.fetchone = Mock(return_value=None)
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                generate_unique_gsids(2)

                # Check that INSERT was called
                insert_calls = [
                    call
                    for call in cursor.execute.call_args_list
                    if "INSERT INTO gsid_registry" in str(call)
                ]
                assert len(insert_calls) == 2

    def test_sets_status_reserved(self, mock_db_connection):
        """Test that generated GSIDs have status='reserved'"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.fetchone = Mock(return_value=None)
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                generate_unique_gsids(1)

                # Check that status='reserved' was used
                insert_call = [
                    call
                    for call in cursor.execute.call_args_list
                    if "INSERT INTO gsid_registry" in str(call)
                ][0]
                assert "'reserved'" in str(insert_call) or "reserved" in str(
                    insert_call
                )

    def test_max_attempts_exceeded(self, mock_db_connection):
        """Test that max attempts prevents infinite loop"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                # Always return duplicate
                cursor.fetchone = Mock(return_value={"gsid": "exists"})
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                with pytest.raises(
                    Exception, match="Could not generate .* unique GSIDs"
                ):
                    generate_unique_gsids(5)


class TestReserveGSIDs:
    """Test reserve_gsids function"""

    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection"""
        conn = Mock()
        cursor = Mock()
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.execute = Mock()
        conn.cursor = Mock(return_value=cursor)
        conn.commit = Mock()
        conn.rollback = Mock()
        conn.close = Mock()
        return conn

    def test_reserve_single_gsid(self, mock_db_connection):
        """Test reserving single GSID"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                reserve_gsids(["GSID-0000000000000001"])

                cursor.execute.assert_called_once()
                mock_db_connection.commit.assert_called_once()

    def test_reserve_multiple_gsids(self, mock_db_connection):
        """Test reserving multiple GSIDs"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                gsids = [
                    "GSID-0000000000000001",
                    "GSID-0000000000000002",
                    "GSID-0000000000000003",
                ]
                reserve_gsids(gsids)

                assert cursor.execute.call_count == 3
                mock_db_connection.commit.assert_called_once()

    def test_reserve_empty_list(self, mock_db_connection):
        """Test reserving empty list"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                reserve_gsids([])

                cursor.execute.assert_not_called()
                mock_db_connection.commit.assert_called_once()

    def test_reserve_uses_on_conflict(self, mock_db_connection):
        """Test that reserve uses ON CONFLICT DO NOTHING"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                reserve_gsids(["GSID-0000000000000001"])

                # Check that ON CONFLICT was used
                call_args = cursor.execute.call_args[0]
                query = call_args[0]
                assert "ON CONFLICT" in query
                assert "DO NOTHING" in query

    def test_reserve_database_error_rollback(self, mock_db_connection):
        """Test that database errors trigger rollback"""
        mock_db_connection.commit.side_effect = Exception("Database error")

        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                with pytest.raises(Exception, match="Database error"):
                    reserve_gsids(["GSID-0000000000000001"])

                mock_db_connection.rollback.assert_called_once()

    def test_reserve_connection_closed_on_success(self, mock_db_connection):
        """Test that connection is closed after success"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                reserve_gsids(["GSID-0000000000000001"])

                mock_db_connection.close.assert_called_once()

    def test_reserve_connection_closed_on_error(self, mock_db_connection):
        """Test that connection is closed even on error"""
        mock_db_connection.commit.side_effect = Exception("Error")

        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                with pytest.raises(Exception):
                    reserve_gsids(["GSID-0000000000000001"])

                mock_db_connection.close.assert_called_once()

    def test_reserve_inserts_with_reserved_status(self, mock_db_connection):
        """Test that reserved GSIDs have status='reserved'"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                reserve_gsids(["GSID-0000000000000001"])

                call_args = cursor.execute.call_args[0]
                query = call_args[0]
                assert "'reserved'" in query or "reserved" in str(call_args)

    def test_reserve_sets_created_at(self, mock_db_connection):
        """Test that reserved GSIDs have created_at timestamp"""
        with patch(
            "services.gsid_generator.get_db_connection", return_value=mock_db_connection
        ):
            with patch("services.gsid_generator.get_db_cursor") as mock_cursor:
                cursor = Mock()
                cursor.execute = Mock()
                mock_cursor.return_value.__enter__.return_value = cursor
                mock_cursor.return_value.__exit__.return_value = False

                reserve_gsids(["GSID-0000000000000001"])

                call_args = cursor.execute.call_args[0]
                query = call_args[0]
                assert "created_at" in query
                assert "NOW()" in query
