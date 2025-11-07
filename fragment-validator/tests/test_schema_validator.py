import pandas as pd
import pytest
from unittest.mock import Mock
from services.schema_validator import SchemaValidator, ValidationResult


class TestSchemaValidator:
    """Unit tests for SchemaValidator"""

    def test_valid_data_passes(self, mock_nocodb_client):
        """Test that valid data passes validation"""
        validator = SchemaValidator(mock_nocodb_client)

        data = pd.DataFrame(
            {
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
            {
                "sample_type": ["Blood", "Plasma"],
            }
        )

        result = validator.validate(data, "blood")

        assert result.is_valid is False
        assert len(result.errors) == 1
        assert result.errors[0]["type"] == "missing_required_column"
        assert result.errors[0]["column"] == "sample_id"

    def test_null_in_required_column(self, mock_nocodb_client):
        """Test detection of null values in required columns"""
        validator = SchemaValidator(mock_nocodb_client)

        data = pd.DataFrame(
            {
                "sample_id": ["SMP1", None, "SMP3"],
                "sample_type": ["Blood", "Plasma", "Serum"],
            }
        )

        result = validator.validate(data, "blood")

        assert result.is_valid is False
        assert len(result.errors) == 1
        assert result.errors[0]["type"] == "null_in_required_column"
        assert result.errors[0]["column"] == "sample_id"
        assert result.errors[0]["null_count"] == 1

    def test_skip_system_columns(self, mock_nocodb_client):
        """Test that system columns are skipped during validation"""
        validator = SchemaValidator(mock_nocodb_client)

        # Data without global_subject_id (which is auto-generated)
        data = pd.DataFrame(
            {
                "sample_id": ["SMP1", "SMP2"],
                "sample_type": ["Blood", "Plasma"],
            }
        )

        result = validator.validate(data, "blood")

        # Should pass even though global_subject_id is missing
        assert result.is_valid is True

    def test_skip_primary_key_and_auto_increment(self, mock_nocodb_client):
        """Test that PK and AI columns are skipped"""
        validator = SchemaValidator(mock_nocodb_client)

        data = pd.DataFrame(
            {
                "sample_id": ["SMP1", "SMP2"],
                "sample_type": ["Blood", "Plasma"],
            }
        )

        result = validator.validate(data, "blood")

        # Should not complain about missing 'Id' column
        assert result.is_valid is True

    def test_no_columns_warning(self):
        """Test warning when table has no columns"""
        # Create a fresh mock with empty columns
        mock_client = Mock()
        mock_client.get_table_metadata.return_value = {"columns": []}

        validator = SchemaValidator(mock_client)
        data = pd.DataFrame({"col1": [1, 2]})

        result = validator.validate(data, "empty_table")

        # When no columns found, validation should pass with a warning
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 1
        assert "No columns found" in result.warnings[0]

    def test_validation_exception_handling(self):
        """Test handling of validation exceptions"""
        mock_client = Mock()
        mock_client.get_table_metadata.side_effect = Exception("API Error")

        validator = SchemaValidator(mock_client)
        data = pd.DataFrame({"col1": [1, 2]})

        result = validator.validate(data, "blood")

        assert result.is_valid is False
        assert len(result.errors) == 1
        assert result.errors[0]["type"] == "schema_validation_error"
        assert "API Error" in result.errors[0]["message"]

    def test_multiple_validation_errors(self, mock_nocodb_client):
        """Test multiple validation errors are collected"""
        validator = SchemaValidator(mock_nocodb_client)

        # Data with nulls in required column sample_id
        data = pd.DataFrame(
            {
                "sample_id": ["SMP1", None, "SMP3"],
                "sample_type": ["Blood", "Plasma", "Serum"],
            }
        )

        result = validator.validate(data, "blood")

        assert result.is_valid is False
        # Should have 1 error: null in sample_id
        assert len(result.errors) == 1
        assert result.errors[0]["type"] == "null_in_required_column"
        assert result.errors[0]["column"] == "sample_id"

    def test_optional_columns_with_nulls_allowed(self, mock_nocodb_client):
        """Test that optional columns can have null values"""
        validator = SchemaValidator(mock_nocodb_client)

        data = pd.DataFrame(
            {
                "sample_id": ["SMP1", "SMP2"],
                "sample_type": ["Blood", None],  # sample_type is optional (rqd=False)
            }
        )

        result = validator.validate(data, "blood")

        # Should pass because sample_type is not required
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_extra_columns_ignored(self, mock_nocodb_client):
        """Test that extra columns in data are ignored"""
        validator = SchemaValidator(mock_nocodb_client)

        data = pd.DataFrame(
            {
                "sample_id": ["SMP1", "SMP2"],
                "sample_type": ["Blood", "Plasma"],
                "extra_column": ["extra1", "extra2"],
            }
        )

        result = validator.validate(data, "blood")

        # Should pass - extra columns don't cause validation errors
        assert result.is_valid is True
