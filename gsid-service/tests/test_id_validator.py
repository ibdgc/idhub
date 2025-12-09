# gsid-service/tests/test_id_validator.py
from unittest.mock import Mock, patch

import pytest
from services.id_validator import IDValidator


class TestIDValidator:
    """Test ID validation logic"""

    def test_valid_id(self):
        """Test validation of valid ID"""
        result = IDValidator.validate_id("IBDGC12345", "consortium_id")

        assert result["valid"] is True
        assert result["severity"] == "info"
        assert len(result["warnings"]) == 0

    def test_short_id_warning(self):
        """Test warning for short IDs"""
        result = IDValidator.validate_id("123", "consortium_id")

        assert result["valid"] is True
        assert result["severity"] == "warning"
        assert any("short" in w.lower() for w in result["warnings"])

    def test_test_id_error(self):
        """Test error for test IDs"""
        result = IDValidator.validate_id("test123", "consortium_id")

        assert result["valid"] is False
        assert result["severity"] == "error"
        assert any("error pattern" in w.lower() for w in result["warnings"])

    def test_all_zeros_error(self):
        """Test error for all zeros"""
        result = IDValidator.validate_id("0000", "consortium_id")

        assert result["valid"] is False
        assert result["severity"] == "error"

    def test_numeric_id_allowed_for_niddk(self):
        """Test numeric IDs are allowed for NIDDK numbers"""
        result = IDValidator.validate_id("12345678", "niddk_no")

        # Should not have numeric warning for niddk_no type
        assert result["valid"] is True
        numeric_warnings = [w for w in result["warnings"] if "numeric" in w.lower()]
        assert len(numeric_warnings) == 0

    def test_internal_whitespace_warning(self):
        """Test warning for internal whitespace"""
        result = IDValidator.validate_id("IBDGC 123", "consortium_id")

        assert result["valid"] is True
        assert result["severity"] == "warning"
        assert any("whitespace" in w.lower() for w in result["warnings"])

    def test_leading_trailing_whitespace_no_warning(self):
        """Test that leading/trailing whitespace is stripped and does not produce a warning"""
        result = IDValidator.validate_id("  IBDGC123  ", "consortium_id")

        assert result["valid"] is True
        assert result["severity"] == "info"
        assert not any("whitespace" in w.lower() for w in result["warnings"])

    def test_batch_validation(self):
        """Test batch validation"""
        ids = [
            {"id": "IBDGC123", "type": "consortium_id"},
            {"id": "test", "type": "local_id"},
            {"id": "456", "type": "sample_id"},
        ]

        results = IDValidator.validate_batch(ids)

        assert len(results) == 3
        assert results["IBDGC123"]["valid"] is True
        assert results["test"]["valid"] is False
        assert results["456"]["valid"] is True
