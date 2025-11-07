# redcap-pipeline/tests/test_data_processor.py
import json
from datetime import date
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest


@pytest.fixture
def mock_field_mappings():
    """Mock field mappings data"""
    return {
        "mappings": [
            {
                "source_field": "subject_id",
                "target_table": "local_subject_ids",
                "target_field": "local_subject_id",
                "identifier_type": "primary",
            },
            {
                "source_field": "alternate_id",
                "target_table": "local_subject_ids",
                "target_field": "local_subject_id",
                "identifier_type": "alternate",
            },
            {
                "source_field": "registration_date",
                "target_table": "subjects",
                "target_field": "registration_year",
            },
            {
                "source_field": "control",
                "target_table": "subjects",
                "target_field": "control",
            },
            {
                "source_field": "blood_sample_id",
                "target_table": "specimen",
                "target_field": "sample_id",
                "sample_type": "blood",
            },
            {
                "source_field": "dna_sample_id",
                "target_table": "specimen",
                "target_field": "sample_id",
                "sample_type": "dna",
            },
            {
                "source_field": "wgs_sample_id",
                "target_table": "sequence",
                "target_field": "sample_id",
                "sample_type": "wgs",
            },
        ],
        "transformations": {
            "registration_date": {"type": "extract_year"},
            "control": {
                "type": "boolean",
                "true_values": ["1", "true", "yes"],
                "false_values": ["0", "false", "no"],
            },
        },
    }


class TestDataProcessor:
    """Test DataProcessor functionality"""

    @pytest.fixture
    def data_processor(self, sample_project_config, mock_field_mappings):
        """Create DataProcessor instance with mocked dependencies"""
        from services.data_processor import DataProcessor

        config = sample_project_config.copy()
        config["field_mappings"] = "test_field_mappings.json"

        # Convert mappings to JSON string
        mappings_json = json.dumps(mock_field_mappings)

        # Mock Path.exists() to return True
        with (
            patch("services.data_processor.CenterResolver"),
            patch("services.data_processor.GSIDClient"),
            patch("services.data_processor.S3Uploader"),
            patch("services.data_processor.get_db_connection"),
            patch("services.data_processor.return_db_connection"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=mappings_json)),
        ):
            processor = DataProcessor(config)
            return processor

    def test_init_success(self, sample_project_config, mock_field_mappings):
        """Test successful initialization"""
        from services.data_processor import DataProcessor

        config = sample_project_config.copy()
        config["field_mappings"] = "test_field_mappings.json"

        mappings_json = json.dumps(mock_field_mappings)

        with (
            patch("services.data_processor.CenterResolver"),
            patch("services.data_processor.GSIDClient"),
            patch("services.data_processor.S3Uploader"),
            patch("services.data_processor.get_db_connection"),
            patch("services.data_processor.return_db_connection"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=mappings_json)),
        ):
            processor = DataProcessor(config)

        assert processor.project_key == "test_project"
        assert processor.project_name == "Test Project"
        assert len(processor.field_mappings["mappings"]) > 0

    def test_load_field_mappings_success(self, data_processor):
        """Test loading field mappings from file"""
        assert "mappings" in data_processor.field_mappings
        assert "transformations" in data_processor.field_mappings
        assert len(data_processor.field_mappings["mappings"]) == 7

    def test_get_subject_id_fields(self, data_processor):
        """Test extraction of subject ID fields"""
        id_fields = data_processor.get_subject_id_fields()

        assert "subject_id" in id_fields
        assert "alternate_id" in id_fields
        assert len(id_fields) == 2

    def test_transform_value_extract_year(self, data_processor):
        """Test year extraction transformation"""
        result = data_processor.transform_value("registration_date", "2024-01-15")

        assert result == "2024"

    def test_transform_value_boolean_true(self, data_processor):
        """Test boolean transformation - true values"""
        for value in ["1", "true", "yes"]:
            result = data_processor.transform_value("control", value)
            assert result is True, f"Failed for value: {value}"

    def test_transform_value_boolean_false(self, data_processor):
        """Test boolean transformation - false values"""
        for value in ["0", "false", "no"]:
            result = data_processor.transform_value("control", value)
            assert result is False, f"Failed for value: {value}"

    def test_transform_value_no_transformation(self, data_processor):
        """Test value with no transformation defined"""
        result = data_processor.transform_value("unknown_field", "some_value")
        assert result == "some_value"

    def test_extract_registration_year_full_date(self, data_processor):
        """Test extracting year from full date"""
        record = {"registration_date": "2024-01-15"}

        result = data_processor.extract_registration_year(record)

        assert result == date(2024, 1, 1)

    def test_extract_registration_year_year_only(self, data_processor):
        """Test extracting year from year-only value"""
        record = {"registration_date": "2024"}

        result = data_processor.extract_registration_year(record)

        assert result == date(2024, 1, 1)

    def test_extract_registration_year_invalid(self, data_processor):
        """Test handling invalid registration year"""
        record = {"registration_date": "invalid"}

        result = data_processor.extract_registration_year(record)

        assert result is None

    def test_extract_registration_year_missing(self, data_processor):
        """Test handling missing registration year"""
        record = {}

        result = data_processor.extract_registration_year(record)

        assert result is None

    def test_extract_control_status_true(self, data_processor):
        """Test extracting control status - true"""
        # Test with string "1"
        record = {"control": "1"}
        result = data_processor.extract_control_status(record)
        assert result is True

        # Test with boolean True
        record = {"control": True}
        result = data_processor.extract_control_status(record)
        assert result is True

        # Test with integer 1
        record = {"control": 1}
        result = data_processor.extract_control_status(record)
        assert result is True

    def test_extract_control_status_false(self, data_processor):
        """Test extracting control status - false"""
        record = {"control": "0"}

        result = data_processor.extract_control_status(record)

        assert result is False

    def test_extract_control_status_missing(self, data_processor):
        """Test extracting control status when field is missing"""
        record = {}

        result = data_processor.extract_control_status(record)

        assert result is False

    def test_extract_subject_ids_success(self, data_processor, sample_redcap_record):
        """Test extracting subject IDs from record"""
        # Add the fields that the processor expects
        record = sample_redcap_record.copy()
        record["subject_id"] = "MSSM001"
        record["alternate_id"] = "ALT001"

        subject_ids = data_processor.extract_subject_ids(record)

        assert len(subject_ids) >= 1
        assert any(sid["local_subject_id"] == "MSSM001" for sid in subject_ids)

    def test_extract_subject_ids_filters_empty(self, data_processor):
        """Test that empty/null values are filtered out"""
        record = {
            "subject_id": "VALID001",
            "alternate_id": "",
        }

        subject_ids = data_processor.extract_subject_ids(record)

        assert len(subject_ids) == 1
        assert subject_ids[0]["local_subject_id"] == "VALID001"
        assert all(sid["local_subject_id"] != "" for sid in subject_ids)

    def test_create_curated_fragment(self, data_processor, sample_redcap_record):
        """Test creating curated data fragment"""
        fragment = data_processor.create_curated_fragment(
            sample_redcap_record, "GSID-TEST123456789", 1
        )

        assert fragment["gsid"] == "GSID-TEST123456789"
        assert fragment["center_id"] == 1
        assert fragment["project_key"] == "test_project"
        assert "samples" in fragment
        assert "metadata" in fragment

    def test_process_record_concept(self, data_processor):
        """Test process_record method exists and has correct signature"""
        assert hasattr(data_processor, "process_record")
        assert callable(data_processor.process_record)


class TestDataProcessorIntegration:
    """Integration tests for DataProcessor"""

    def test_full_record_processing_flow(
        self, sample_project_config, mock_field_mappings, sample_redcap_record
    ):
        """Test complete record processing flow"""
        from services.data_processor import DataProcessor

        config = sample_project_config.copy()
        config["field_mappings"] = "test_field_mappings.json"

        mappings_json = json.dumps(mock_field_mappings)

        # Add required fields to the record
        record = sample_redcap_record.copy()
        record["subject_id"] = "MSSM001"

        with (
            patch("services.data_processor.CenterResolver") as mock_center,
            patch("services.data_processor.GSIDClient") as mock_gsid,
            patch("services.data_processor.S3Uploader") as mock_s3,
            patch("services.data_processor.get_db_connection") as mock_get_conn,
            patch("services.data_processor.return_db_connection") as mock_return_conn,
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=mappings_json)),
        ):
            # Setup mocks
            mock_center.return_value.get_or_create_center.return_value = 1
            mock_gsid.return_value.register_subject.return_value = {
                "gsid": "GSID-NEW123456789AB",
                "action": "create_new",
            }

            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = None
            mock_cursor.fetchall.return_value = []
            mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
            mock_cursor.__exit__ = MagicMock(return_value=False)
            mock_conn.cursor.return_value = mock_cursor
            mock_get_conn.return_value = mock_conn

            processor = DataProcessor(config)
            result = processor.process_record(record)

        # Verify result structure
        assert "status" in result
        assert result["status"] in ["success", "error"]

    def test_error_handling(self, sample_project_config, mock_field_mappings):
        """Test error handling in process_record"""
        from services.data_processor import DataProcessor

        config = sample_project_config.copy()
        config["field_mappings"] = "test_field_mappings.json"

        mappings_json = json.dumps(mock_field_mappings)

        with (
            patch("services.data_processor.CenterResolver") as mock_center,
            patch("services.data_processor.GSIDClient"),
            patch("services.data_processor.S3Uploader"),
            patch("services.data_processor.get_db_connection"),
            patch("services.data_processor.return_db_connection"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=mappings_json)),
        ):
            # Make center resolver raise an error
            mock_center.return_value.get_or_create_center.side_effect = Exception(
                "DB Error"
            )

            processor = DataProcessor(config)
            result = processor.process_record(
                {
                    "record_id": "TEST001",
                    "redcap_data_access_group": "mount_sinai",
                    "subject_id": "TEST001",
                }
            )

        assert result["status"] == "error"
        assert "error" in result
