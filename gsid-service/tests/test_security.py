import os
from unittest.mock import patch

import pytest
from fastapi import HTTPException


class TestSecurity:
    """Test security and authentication functionality"""

    @pytest.mark.asyncio
    async def test_verify_api_key_valid(self):
        """Test API key verification with valid key"""
        from core.security import verify_api_key

        valid_key = os.getenv("GSID_API_KEY")
        # The function returns the key itself if valid
        result = await verify_api_key(x_api_key=valid_key)
        assert result == valid_key

    @pytest.mark.asyncio
    async def test_verify_api_key_invalid(self):
        """Test API key verification with invalid key"""
        from core.security import verify_api_key

        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key(x_api_key="invalid-key")

        assert exc_info.value.status_code == 403
        assert "Invalid API key" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_verify_api_key_missing_env_var(self):
        """Test API key verification when env var not set"""
        from core.security import verify_api_key

        with patch.dict(os.environ, {"GSID_API_KEY": ""}):
            # Need to reload settings
            with patch("core.security.settings.GSID_API_KEY", ""):
                with pytest.raises(HTTPException) as exc_info:
                    await verify_api_key(x_api_key="any-key")

                assert exc_info.value.status_code == 500
                assert "not configured" in str(exc_info.value.detail)

    def test_api_key_environment_variable(self):
        """Test that API key is loaded from environment"""
        api_key = os.getenv("GSID_API_KEY")
        assert api_key is not None
        assert len(api_key) > 0
        assert api_key == "test-api-key-12345"

    @pytest.mark.asyncio
    async def test_verify_api_key_case_sensitive(self):
        """Test that API key verification is case-sensitive"""
        from core.security import verify_api_key

        valid_key = os.getenv("GSID_API_KEY")
        wrong_case_key = valid_key.lower() if valid_key.isupper() else valid_key.upper()

        if wrong_case_key != valid_key:
            with pytest.raises(HTTPException) as exc_info:
                await verify_api_key(x_api_key=wrong_case_key)

            assert exc_info.value.status_code == 403
