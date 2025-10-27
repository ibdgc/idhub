# fragment-validator/tests/test_schema_validator.py
import pandas as pd
import pytest
from services.schema_validator import SchemaValidator, ValidationResult


class TestSchemaValidator:
    """Unit tests for SchemaValidator"""

    def test_valid_data_passes(self, mock_nocodb_client):
        """Test that valid data passes validation"""
        validator = SchemaValidator(mock_nocodb_client)

        data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP1", "SMP2"],
                "sample_type": ["Blood", "Plasma"],
            }
        )

        result = validator.validate(data, "blood")

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_missing_required_column(self, mock_nocodb_client):
        """Test detection of missing required column"""
        validator = SchemaValidator(mock_nocodb_client)

        # Missing 'sample_id' which is required
        data = pd.DataFrame(
            {"global_subject_id": ["GSID-001"], "sample_type": ["Blood"]}
        )

        result = validator.validate(data, "blood")

        assert result.is_valid is False
        assert any(e["type"] == "missing_required_column" for e in result.errors)
        assert any("sample_id" in e["message"] for e in result.errors)

    def test_null_in_required_column(self, mock_nocodb_client):
        """Test detection of null values in required columns"""
        validator = SchemaValidator(mock_nocodb_client)

        data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP1", None],  # Null in required field
            }
        )

        result = validator.validate(data, "blood")

        assert result.is_valid is False
        assert any(e["type"] == "null_in_required_column" for e in result.errors)

        # Check error details
        error = next(e for e in result.errors if e["type"] == "null_in_required_column")
        assert error["column"] == "sample_id"
        assert error["null_count"] == 1

    def test_skip_system_columns(self, mock_nocodb_client):
        """Test that system columns are skipped in validation"""
        validator = SchemaValidator(mock_nocodb_client)

        # Missing 'created_at' but it's a system column
        data = pd.DataFrame({"global_subject_id": ["GSID-001"], "sample_id": ["SMP1"]})

        result = validator.validate(data, "blood")

        # Should not complain about missing 'created_at'
        assert not any("created_at" in e.get("message", "") for e in result.errors)
