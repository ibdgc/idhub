# gsid-service/tests/test_services.py
from unittest.mock import MagicMock, patch

import pytest


class TestGSIDService:
    """Test GSID service functionality"""

    def test_gsid_generator_module(self):
        """Test GSID generator module"""
        from services import gsid_generator

        assert hasattr(gsid_generator, "generate_gsid")
        assert hasattr(gsid_generator, "BASE32_ALPHABET")

    def test_generate_multiple_gsids(self):
        """Test generating multiple GSIDs"""
        from services.gsid_generator import generate_gsid

        count = 10
        gsids = [generate_gsid() for _ in range(count)]

        # All should be valid strings
        assert all(isinstance(gsid, str) for gsid in gsids)
        # Updated: Match actual format (21 chars with GSID- prefix)
        assert all(len(gsid) == 21 for gsid in gsids)
        assert all(gsid.startswith("GSID-") for gsid in gsids)
        assert len(set(gsids)) == count  # All unique

    def test_gsid_format_validation(self):
        """Test that generated GSIDs match expected format"""
        from services.gsid_generator import BASE32_ALPHABET, generate_gsid

        gsid = generate_gsid()

        # Updated: Extract only the ID part (after "GSID-" prefix)
        gsid_id_part = gsid.split("GSID-")[1] if gsid.startswith("GSID-") else gsid

        # All characters in ID part should be in BASE32_ALPHABET
        assert all(c in BASE32_ALPHABET for c in gsid_id_part)

        # Should not contain ambiguous characters
        assert "I" not in gsid_id_part
        assert "L" not in gsid_id_part
        assert "O" not in gsid_id_part
        assert "U" not in gsid_id_part


class TestDatabaseService:
    """Test database service functionality"""

    @patch("psycopg2.pool.ThreadedConnectionPool")
    def test_database_module_import(self, mock_pool):
        """Test database module can be imported"""
        try:
            from services import database

            assert hasattr(database, "get_db_connection")
            assert hasattr(database, "get_db_cursor")
        except Exception as e:
            pytest.fail(f"Failed to import database module: {e}")


class TestSecurityService:
    """Test security service functionality"""

    def test_security_module_import(self):
        """Test security module can be imported"""
        try:
            from core import security

            assert hasattr(security, "verify_api_key")
        except Exception as e:
            pytest.fail(f"Failed to import security module: {e}")

    @pytest.mark.asyncio
    async def test_api_key_verification(self):
        """Test API key verification logic"""
        from core.config import settings
        from core.security import verify_api_key

        # Valid key - should return the key (not raise exception)
        result = await verify_api_key(settings.GSID_API_KEY)
        assert result == settings.GSID_API_KEY

        # Invalid key - should raise HTTPException
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key("wrong-key")
        assert exc_info.value.status_code == 403

        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key("")
        assert exc_info.value.status_code == 403
