# gsid-service/tests/test_gsid_format.py
"""
Tests specifically for GSID format validation and standards compliance
"""

import re

import pytest


class TestGSIDFormat:
    """Test GSID format against specification"""

    def test_gsid_length(self):
        """Test GSID has correct total length"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        # Format: GSID-XXXXXXXXXXXX (5 + 12 = 17)
        assert len(gsid) == 17, f"Expected length 17, got {len(gsid)}"

    def test_gsid_prefix(self):
        """Test GSID has correct prefix"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        assert gsid.startswith("GSID-"), f"Expected 'GSID-' prefix, got: {gsid[:5]}"

    def test_gsid_id_part_length(self):
        """Test GSID ID part (after prefix) is correct length"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        id_part = gsid[5:]  # Remove "GSID-" prefix
        # 5 chars timestamp + 7 chars random = 12 total
        assert len(id_part) == 12, f"Expected ID part length 12, got {len(id_part)}"

    def test_gsid_character_set(self):
        """Test GSID uses only valid base32 characters"""
        from services.gsid_generator import BASE32_ALPHABET, generate_gsid

        gsid = generate_gsid()
        id_part = gsid[5:]  # Remove prefix

        for char in id_part:
            assert char in BASE32_ALPHABET, (
                f"Invalid character '{char}' not in BASE32 alphabet"
            )

    def test_gsid_no_ambiguous_chars(self):
        """Test GSID excludes ambiguous characters (I, L, O, U)"""
        from services.gsid_generator import generate_gsid

        # Generate many GSIDs to ensure no ambiguous characters
        gsids = [generate_gsid() for _ in range(100)]

        for gsid in gsids:
            id_part = gsid[5:]
            assert "I" not in id_part, f"Found ambiguous 'I' in {gsid}"
            assert "L" not in id_part, f"Found ambiguous 'L' in {gsid}"
            assert "O" not in id_part, f"Found ambiguous 'O' in {gsid}"
            assert "U" not in id_part, f"Found ambiguous 'U' in {gsid}"

    def test_gsid_regex_pattern(self):
        """Test GSID matches expected regex pattern"""
        from services.gsid_generator import generate_gsid

        # Pattern: GSID- followed by 12 base32 characters
        pattern = r"^GSID-[0-9A-HJ-NP-Z]{12}$"

        gsids = [generate_gsid() for _ in range(10)]
        for gsid in gsids:
            assert re.match(pattern, gsid), (
                f"GSID '{gsid}' doesn't match pattern '{pattern}'"
            )

    def test_gsid_structure(self):
        """Test GSID internal structure (timestamp + random)"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        id_part = gsid[5:]

        # First 5 chars: timestamp part
        timestamp_part = id_part[:5]
        assert len(timestamp_part) == 5

        # Last 7 chars: random part
        random_part = id_part[5:]
        assert len(random_part) == 7

    def test_gsid_sortability(self):
        """Test GSIDs are lexicographically sortable by creation time"""
        import time

        from services.gsid_generator import generate_gsid

        gsid1 = generate_gsid()
        time.sleep(0.002)  # Wait 2ms to ensure different timestamp
        gsid2 = generate_gsid()

        # Later GSIDs should sort after earlier ones (lexicographically)
        # This works because timestamp is encoded in first 5 chars after prefix
        assert gsid1 < gsid2 or gsid1[:10] <= gsid2[:10], (
            f"GSIDs not sortable: {gsid1} should be <= {gsid2}"
        )

    def test_gsid_collision_resistance(self):
        """Test GSIDs are unique (collision resistant)"""
        from services.gsid_generator import generate_gsid

        # Generate many GSIDs in quick succession
        count = 1000
        gsids = [generate_gsid() for _ in range(count)]

        # All should be unique
        unique_gsids = set(gsids)
        assert len(unique_gsids) == count, (
            f"Found {count - len(unique_gsids)} collisions in {count} GSIDs"
        )

    def test_encode_base32_function(self):
        """Test base32 encoding helper function"""
        from services.gsid_generator import encode_base32

        # Test zero
        assert encode_base32(0, 5) == "00000"

        # Test small number
        result = encode_base32(32, 5)
        assert len(result) == 5
        assert result == "00010"  # 32 in base32 = 10

        # Test larger number
        result = encode_base32(1234567, 7)
        assert len(result) == 7
