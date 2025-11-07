import re

import pytest


class TestGSIDGenerator:
    """Test GSID generation functionality"""

    def test_generate_gsid_format(self):
        """Test that generated GSID matches expected format"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()

        # Should match pattern: GSID-XXXXXXXXXXXXXXXX
        pattern = r'^GSID-[0-9A-Z]{16}$'
        assert re.match(pattern, gsid), f"GSID {gsid} doesn't match expected format"

    def test_generate_gsid_length(self):
        """Test GSID total length is 21 characters"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        assert len(gsid) == 21, f"Expected length 21, got {len(gsid)}"

    def test_generate_gsid_prefix(self):
        """Test GSID starts with 'GSID-' prefix"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        assert gsid.startswith("GSID-"), f"GSID should start with 'GSID-', got: {gsid}"

    def test_generate_gsid_uniqueness(self):
        """Test that multiple GSIDs are unique"""
        from services.gsid_generator import generate_gsid

        gsids = [generate_gsid() for _ in range(1000)]
        unique_gsids = set(gsids)

        assert len(unique_gsids) == 1000, "All generated GSIDs should be unique"

    def test_base32_alphabet_no_ambiguous_chars(self):
        """Test BASE32 alphabet excludes ambiguous characters"""
        from services.gsid_generator import BASE32_ALPHABET

        # Should not contain I, L, O, U (ambiguous characters)
        assert "I" not in BASE32_ALPHABET
        assert "L" not in BASE32_ALPHABET
        assert "O" not in BASE32_ALPHABET
        assert "U" not in BASE32_ALPHABET

    def test_base32_alphabet_length(self):
        """Test BASE32 alphabet has exactly 32 characters"""
        from services.gsid_generator import BASE32_ALPHABET

        assert len(BASE32_ALPHABET) == 32

    def test_base32_alphabet_valid_characters(self):
        """Test BASE32 alphabet contains expected characters"""
        from services.gsid_generator import BASE32_ALPHABET

        # Should contain digits 0-9
        for digit in "0123456789":
            assert digit in BASE32_ALPHABET

        # Should contain some letters (excluding I, L, O, U)
        for letter in "ABCDEFGHJKMNPQRSTVWXYZ":
            assert letter in BASE32_ALPHABET

    def test_gsid_id_part_uses_base32(self):
        """Test that GSID ID part only uses BASE32 alphabet characters"""
        from services.gsid_generator import BASE32_ALPHABET, generate_gsid

        gsid = generate_gsid()
        id_part = gsid[5:]  # Remove "GSID-" prefix

        for char in id_part:
            assert char in BASE32_ALPHABET, f"Character {char} not in BASE32 alphabet"

    def test_generate_multiple_gsids_different(self):
        """Test that consecutive GSIDs are different"""
        from services.gsid_generator import generate_gsid

        gsid1 = generate_gsid()
        gsid2 = generate_gsid()

        assert gsid1 != gsid2, "Consecutive GSIDs should be different"
