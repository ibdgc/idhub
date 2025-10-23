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

    def test_generate_gsid_function_exists(self):
        """Test that generate_gsid function exists and works"""
        from services.gsid_generator import generate_gsid

        gsid = generate_gsid()
        assert gsid is not None
        assert len(gsid) == 26
        assert isinstance(gsid, str)

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

    def test_api_models_import(self):
        """Test that API models can be imported"""
        try:
            from api.models import GSIDRequest, GSIDResponse, HealthResponse

            assert GSIDRequest is not None
            assert GSIDResponse is not None
            assert HealthResponse is not None
        except Exception as e:
            pytest.fail(f"Failed to import models: {e}")

    def test_config_loading(self):
        """Test that config loads correctly"""
        from core.config import settings

        assert settings.GSID_API_KEY == "test-api-key"
        assert settings.DB_HOST == "localhost"
        assert settings.DB_NAME == "test_db"

    @patch("services.gsid_generator.secrets.randbits")
    def test_gsid_generation_with_mock(self, mock_randbits):
        """Test GSID generation with mocked randomness"""
        from services.gsid_generator import generate_gsid

        mock_randbits.return_value = 12345
        gsid = generate_gsid()

        assert gsid is not None
        assert len(gsid) == 26
