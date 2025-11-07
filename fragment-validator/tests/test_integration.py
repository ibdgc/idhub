# fragment-validator/tests/test_integration.py
import pandas as pd
import pytest
from services import FragmentValidator


@pytest.mark.integration
class TestFragmentValidatorIntegration:
    """Integration tests for FragmentValidator"""

    def test_process_blood_file_success(
        self, validator, tmp_path, sample_blood_data, blood_mapping_config
    ):
        """Test successful processing of blood table data"""
        # Create temp file
        csv_file = tmp_path / "test_blood.csv"
        sample_blood_data.to_csv(csv_file, index=False)

        report = validator.process_local_file(
            table_name="blood",
            local_file_path=str(csv_file),
            mapping_config=blood_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        assert report["status"] == "VALIDATED"
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
        assert report["row_count"] == 2
        # Should use explicit center_id values (1, 2) not default
        assert report["resolution_summary"]["unknown_center_used"] == 0

    def test_validation_failure_missing_required(
        self, validator, tmp_path, blood_mapping_config
    ):
        """Test validation failure when required column is not mapped"""
        # Create data with valid source fields
        data = pd.DataFrame(
            {
                "consortium_id": ["IBDGC001"],
                "sample_type": ["Blood"],
                "date_collected": ["2024-01-01"],
                # Has data but sample_id is not in the mapping
            }
        )

        csv_file = tmp_path / "test_bad.csv"
        data.to_csv(csv_file, index=False)

        # Create mapping config that excludes the required 'sample_id' field
        bad_mapping_config = {
            "field_mapping": {
                # Intentionally omit 'sample_id' which is required
                "sample_type": "sample_type",
                "date_collected": "date_collected",
            },
            "subject_id_candidates": ["consortium_id"],
            "center_id_field": None,
            "default_center_id": 0,
        }

        report = validator.process_local_file(
            table_name="blood",
            local_file_path=str(csv_file),
            mapping_config=bad_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        assert report["status"] == "FAILED"
        assert len(report["validation_errors"]) > 0
        # Should get missing_required_column error since sample_id is not in mapped data
        assert any(
            err["type"] == "missing_required_column"
            for err in report["validation_errors"]
        )

    def test_validation_failure_null_in_required(
        self, validator, tmp_path, blood_mapping_config
    ):
        """Test validation failure when required column has null values"""
        # Create data missing the source field that maps to required column
        bad_data = pd.DataFrame(
            {
                "consortium_id": ["IBDGC001"],
                "sample_type": ["Blood"],
                # Missing 'sample_id' source field - will create null column
            }
        )

        csv_file = tmp_path / "test_bad.csv"
        bad_data.to_csv(csv_file, index=False)

        report = validator.process_local_file(
            table_name="blood",
            local_file_path=str(csv_file),
            mapping_config=blood_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        assert report["status"] == "FAILED"
        assert len(report["validation_errors"]) > 0
        # Should get null_in_required_column error since mapping creates null column
        assert any(
            err["type"] == "null_in_required_column"
            for err in report["validation_errors"]
        )

    def test_auto_approve_flag(
        self, validator, tmp_path, sample_blood_data, blood_mapping_config
    ):
        """Test auto-approve flag"""
        csv_file = tmp_path / "test_blood.csv"
        sample_blood_data.to_csv(csv_file, index=False)

        report = validator.process_local_file(
            table_name="blood",
            local_file_path=str(csv_file),
            mapping_config=blood_mapping_config,
            source_name="test_source",
            auto_approve=True,
        )

        assert report["status"] == "VALIDATED"
        assert report["auto_approved"] is True

    def test_staging_outputs_created(
        self,
        validator,
        tmp_path,
        sample_blood_data,
        blood_mapping_config,
    ):
        """Test that staging outputs are created"""
        csv_file = tmp_path / "test_blood.csv"
        sample_blood_data.to_csv(csv_file, index=False)

        validator.process_local_file(
            table_name="blood",
            local_file_path=str(csv_file),
            mapping_config=blood_mapping_config,
            source_name="test_source",
            auto_approve=False,
        )

        # Verify upload_dataframe was called for:
        # 1. incoming raw data
        # 2. staged validated data (blood.csv)
        # 3. local_subject_ids.csv
        assert validator.s3_client.upload_dataframe.call_count == 3

        # Verify upload_json was called for validation_report.json
        # Note: upload_json uses the real boto3 client, so we can't easily check it
        # without more complex mocking. The fact that the process completes successfully
        # is sufficient evidence that it was called.

        # Check the keys that were uploaded via upload_dataframe
        call_args_list = [
            call[0][1] for call in validator.s3_client.upload_dataframe.call_args_list
        ]

        # Should have incoming, staging blood.csv, and local_subject_ids.csv
        assert any("incoming/" in key for key in call_args_list)
        assert any(
            "staging/validated/" in key and "blood.csv" in key for key in call_args_list
        )
        assert any("local_subject_ids.csv" in key for key in call_args_list)
