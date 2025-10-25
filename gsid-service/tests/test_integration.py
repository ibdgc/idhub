# gsid-service/tests/test_integration.py
from unittest.mock import MagicMock, patch

import pytest


class TestGSIDIntegration:
    """Integration tests that actually test the application code"""

    def test_import_main_module(self):
        """Test that main module can be imported"""
        try:
            import main

            assert hasattr(main, "app")
        except Exception as e:
            pytest.fail(f"Failed to import main: {e}")

    def test_gsid_uniqueness(self):
        """Test that generate_gsid produces unique values"""
        from services.gsid_generator import generate_gsid

        gsids = [generate_gsid() for _ in range(100)]
        assert len(set(gsids)) == 100, "GSIDs should be unique"

    def test_base32_encoding(self):
        """Test BASE32 encoding function"""
        from services.gsid_generator import BASE32_ALPHABET

        # Verify alphabet
        assert len(BASE32_ALPHABET) == 32
        assert "I" not in BASE32_ALPHABET
        assert "L" not in BASE32_ALPHABET
        assert "O" not in BASE32_ALPHABET
        assert "U" not in BASE32_ALPHABET

    def test_config_loading(self):
        """Test that config loads correctly"""
        from core.config import settings

        assert settings.GSID_API_KEY == "test-api-key"
        assert settings.DB_HOST == "localhost"
        assert settings.DB_NAME == "test_db"

    def test_generate_gsid_function_exists(self):
        """Test that generate_gsid function exists and works"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        assert gsid is not None
        assert isinstance(gsid, str)
        # Format: GSID-XXXXXXXXXXXXXXXX (5 prefix + 16 chars = 21 total)
        assert len(gsid) == 21
        assert gsid.startswith("GSID-")
        # Verify the ID part (after prefix) is 16 characters
        gsid_id = gsid[5:]  # Remove "GSID-" prefix
        assert len(gsid_id) == 16

    def test_api_models_import(self):
        """Test that API models can be imported"""
        try:
            from api.models import (
                BatchSubjectRequest,
                HealthResponse,
                ResolutionResponse,
                SubjectRequest,
            )

            assert SubjectRequest is not None
            assert ResolutionResponse is not None
            assert BatchSubjectRequest is not None
            assert HealthResponse is not None
        except Exception as e:
            pytest.fail(f"Failed to import models: {e}")

    def test_gsid_generation_with_mock(self):
        """Test GSID generation with mocked dependencies"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        assert gsid is not None
        assert isinstance(gsid, str)
        # Format: GSID-XXXXXXXXXXXXXXXX (21 chars total)
        assert len(gsid) == 21
        assert gsid.startswith("GSID-")

    def test_gsid_format_components(self):
        """Test GSID format structure"""
        from services.gsid_generator import BASE32_ALPHABET, generate_gsid

        gsid = generate_gsid()

        # Check format: GSID-XXXXXXXXXXXXXXXX (prefix + 16 chars)
        assert gsid.startswith("GSID-")
        gsid_id = gsid[5:]  # Remove prefix
        assert len(gsid_id) == 16

        # All characters should be valid base32
        for char in gsid_id:
            assert char in BASE32_ALPHABET
