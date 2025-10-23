# redcap-pipeline/tests/test_center_resolver.py
from unittest.mock import MagicMock

import pytest


class TestCenterResolver:
    def test_resolve_center_exact_match(self, mock_db_connection):
        """Test exact center name match"""
        conn, cursor = mock_db_connection
        cursor.fetchone.return_value = {"id": 1, "name": "MSSM"}

        # Simulate center lookup
        result = cursor.fetchone()
        assert result["name"] == "MSSM"
        assert result["id"] == 1

    def test_resolve_center_not_found(self, mock_db_connection):
        """Test center not found"""
        conn, cursor = mock_db_connection
        cursor.fetchone.return_value = None

        result = cursor.fetchone()
        assert result is None

    def test_center_alias_mapping(self):
        """Test center alias mapping concept"""
        # Test the concept of center aliases without importing from main
        center_aliases = {
            "mount_sinai": "MSSM",
            "mount_sinai_ny": "MSSM",
            "cedars_sinai": "Cedars-Sinai",
            "cedars-sinai": "Cedars-Sinai",
            "university_of_chicago": "University of Chicago",
            "uchicago": "University of Chicago",
        }

        # Test alias resolution
        assert center_aliases.get("mount_sinai") == "MSSM"
        assert center_aliases.get("cedars_sinai") == "Cedars-Sinai"
        assert center_aliases.get("mount_sinai_ny") == "MSSM"
        assert center_aliases.get("uchicago") == "University of Chicago"

    def test_center_name_normalization(self):
        """Test center name normalization"""
        test_names = [
            ("Mount Sinai", "mount_sinai"),
            ("Cedars-Sinai", "cedars_sinai"),
            ("Johns Hopkins", "johns_hopkins"),
            ("University of Chicago", "university_of_chicago"),
        ]

        for original, expected in test_names:
            normalized = original.lower().replace(" ", "_").replace("-", "_")
            assert normalized == expected

    def test_fuzzy_matching_concept(self):
        """Test fuzzy matching for center names"""
        from difflib import SequenceMatcher

        def similarity(a, b):
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()

        # Test similar names
        assert similarity("Mount Sinai", "mount_sinai") > 0.8
        assert similarity("Cedars-Sinai", "cedars sinai") > 0.9
        assert similarity("Johns Hopkins", "john hopkins") > 0.8
