# fragment-validator/tests/test_integration.py
from unittest.mock import MagicMock

import pandas as pd
import pytest
from services import FragmentValidator, GSIDClient, NocoDBClient, S3Client


class TestFragmentValidatorIntegration:
    """Integration tests for the full validation pipeline"""

    @pytest.fixture
    def validator(self, mock_s3_client, mock_nocodb_client, mock_gsid_client):
        """Create validator with mocked dependencies"""
        s3_client = S3Client("test-bucket")
        return FragmentValidator(s3_client, mock_nocodb_client, mock_gsid_client)

    def test_process_blood_file_success(
        self, validator, temp_csv_file, blood_mapping_config
    ):
        """Test successful processing of blood table file"""
        report = validator.process_local_file(
            table_name="blood",
            local_file_path=temp_csv_file,
            mapping_config=blood_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        assert report["status"] == "VALIDATED"
        assert report["table_name"] == "blood"
        assert report["row_count"] == 3
        assert len(report["validation_errors"]) == 0
        assert "resolution_summary" in report
        assert "staging_location" in report

    def test_process_lcl_file(
        self, validator, tmp_path, sample_lcl_data, lcl_mapping_config
    ):
        """Test processing LCL table data"""
        # Create temp file
        csv_file = tmp_path / "test_lcl.csv"
        sample_lcl_data.to_csv(csv_file, index=False)

        report = validator.process_local_file(
            table_name="lcl",
            local_file_path=str(csv_file),
            mapping_config=lcl_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        assert report["status"] == "VALIDATED"
        assert report["row_count"] == 2

    def test_process_dna_with_center_id(
        self, validator, tmp_path, sample_dna_data, dna_mapping_config
    ):
        """Test processing DNA data with explicit center_id field"""
        csv_file = tmp_path / "test_dna.csv"
        sample_dna_data.to_csv(csv_file, index=False)

        report = validator.process_local_file(
            table_name="dna",
            local_file_path=str(csv_file),
            mapping_config=dna_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        assert report["status"] == "VALIDATED"
        # Should not use default center (since explicit center_id provided)
        assert report["resolution_summary"]["unknown_center_used"] == 0

    def test_validation_failure_missing_required(
        self, mock_s3_client, mock_gsid_client, tmp_path, blood_mapping_config
    ):
        """Test validation failure due to missing required column"""
        # Create a fresh mock with strict schema requirements
        strict_nocodb_client = MagicMock()
        strict_nocodb_client._get_base_id.return_value = "test-base-id"
        strict_nocodb_client.get_table_id.return_value = "test-blood-id"

        # Define strict schema with required field that will be missing
        strict_nocodb_client.get_table_metadata.return_value = {
            "id": "test-blood-id",
            "table_name": "blood",
            "columns": [
                {"column_name": "Id", "pk": True, "ai": True},
                {"column_name": "global_subject_id", "rqd": True},
                {"column_name": "sample_id", "rqd": True},
                {"column_name": "required_field", "rqd": True},  # This will be missing!
            ],
        }

        strict_nocodb_client.load_local_id_cache.return_value = {}

        # Create validator with strict mock
        s3_client = S3Client("test-bucket")
        validator = FragmentValidator(s3_client, strict_nocodb_client, mock_gsid_client)

        # Create data without required field
        data = pd.DataFrame(
            {
                "consortium_id": ["ID001"],
                "sample_id": ["SMP1"],
                # Missing 'required_field'
            }
        )
        csv_file = tmp_path / "invalid.csv"
        data.to_csv(csv_file, index=False)

        report = validator.process_local_file(
            table_name="blood",
            local_file_path=str(csv_file),
            mapping_config=blood_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        assert report["status"] == "FAILED"
        assert len(report["validation_errors"]) > 0
        assert any("required_field" in str(e) for e in report["validation_errors"])

    def test_auto_approve_flag(self, validator, temp_csv_file, blood_mapping_config):
        """Test auto_approve flag is properly recorded"""
        report = validator.process_local_file(
            table_name="blood",
            local_file_path=temp_csv_file,
            mapping_config=blood_mapping_config,
            source_name="test_source",
            auto_approve=True,  # Set to true
        )

        assert report["auto_approved"] is True

    def test_staging_outputs_created(
        self, validator, temp_csv_file, blood_mapping_config, mock_s3_client
    ):
        """Test that staging outputs are written to S3"""
        validator.process_local_file(
            table_name="blood",
            local_file_path=temp_csv_file,
            mapping_config=blood_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        # Check S3 upload calls
        upload_calls = mock_s3_client.put_object.call_args_list

        # Should have at least:
        # 1. Incoming raw file
        # 2. Staged blood table
        # 3. Staged local_subject_ids
        # 4. Validation report
        assert len(upload_calls) >= 4

        # Check that keys contain expected paths
        keys = [call.kwargs["Key"] for call in upload_calls]
        assert any("incoming/" in key for key in keys)
        assert any("staging/validated/" in key for key in keys)
        assert any("validation_report.json" in key for key in keys)
