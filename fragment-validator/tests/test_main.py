# fragment-validator/tests/test_main.py
import json
import sys
from unittest.mock import MagicMock, mock_open, patch

import pytest
from main import main


class TestMainCLI:
    """Unit tests for main CLI interface"""

    @patch("main.FragmentValidator")
    @patch("main.S3Client")
    @patch("main.NocoDBClient")
    @patch("main.GSIDClient")
    @patch("main.settings.load_mapping_config")
    @patch("os.getenv")
    def test_validate_command(
        self,
        mock_getenv,
        mock_load_config,
        mock_gsid,
        mock_nocodb,
        mock_s3,
        mock_validator,
    ):
        """Test validate command execution"""
        # Mock environment variables
        mock_getenv.side_effect = lambda key: {
            "S3_BUCKET": "test-bucket",
            "GSID_SERVICE_URL": "http://test-gsid",
            "GSID_API_KEY": "test-key",
            "NOCODB_URL": "http://test-nocodb",
            "NOCODB_API_TOKEN": "test-token",
            "NOCODB_BASE_ID": "test-base",
        }.get(key)

        mock_load_config.return_value = {"field_mapping": {}}
        mock_validator_instance = MagicMock()
        mock_validator.return_value = mock_validator_instance
        mock_validator_instance.process_local_file.return_value = {
            "status": "VALIDATED",
            "row_count": 10,
        }

        test_args = [
            "main.py",
            "--table-name",
            "blood",
            "--input-file",
            "test.csv",
            "--mapping-config",
            "config.json",
            "--source",
            "test_source",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        mock_validator_instance.process_local_file.assert_called_once()

    @patch("main.FragmentValidator")
    @patch("main.settings.load_mapping_config")
    @patch("os.getenv")
    def test_auto_approve_flag(self, mock_getenv, mock_load_config, mock_validator):
        """Test auto-approve flag is passed correctly"""
        # Mock environment variables
        mock_getenv.side_effect = lambda key: {
            "S3_BUCKET": "test-bucket",
            "GSID_SERVICE_URL": "http://test-gsid",
            "GSID_API_KEY": "test-key",
            "NOCODB_URL": "http://test-nocodb",
            "NOCODB_API_TOKEN": "test-token",
            "NOCODB_BASE_ID": "test-base",
        }.get(key)

        mock_load_config.return_value = {"field_mapping": {}}
        mock_validator_instance = MagicMock()
        mock_validator.return_value = mock_validator_instance
        mock_validator_instance.process_local_file.return_value = {
            "status": "APPROVED",
        }

        test_args = [
            "main.py",
            "--table-name",
            "blood",
            "--input-file",
            "test.csv",
            "--mapping-config",
            "config.json",
            "--source",
            "test_source",
            "--auto-approve",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        # Check that process_local_file was called with auto_approve=True
        call_args = mock_validator_instance.process_local_file.call_args
        # Arguments: table_name, input_file, mapping_config, source, auto_approve
        assert call_args.args[4] is True  # auto_approve is the 5th positional argument

    def test_missing_required_args(self):
        """Test error when required arguments are missing"""
        test_args = ["main.py", "--table-name", "blood"]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit):
                main()

    @patch("main.settings.load_mapping_config")
    def test_missing_mapping_config(self, mock_load_config):
        """Test error when mapping config file not found"""
        mock_load_config.side_effect = FileNotFoundError("Config not found")

        test_args = [
            "main.py",
            "--table-name",
            "blood",
            "--input-file",
            "test.csv",
            "--mapping-config",
            "missing.json",
            "--source",
            "test_source",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("main.FragmentValidator")
    @patch("main.settings.load_mapping_config")
    @patch("os.getenv")
    def test_validation_error_handling(
        self, mock_getenv, mock_load_config, mock_validator
    ):
        """Test handling of validation errors"""
        # Mock environment variables
        mock_getenv.side_effect = lambda key: {
            "S3_BUCKET": "test-bucket",
            "GSID_SERVICE_URL": "http://test-gsid",
            "GSID_API_KEY": "test-key",
            "NOCODB_URL": "http://test-nocodb",
            "NOCODB_API_TOKEN": "test-token",
            "NOCODB_BASE_ID": "test-base",
        }.get(key)

        mock_load_config.return_value = {"field_mapping": {}}
        mock_validator_instance = MagicMock()
        mock_validator.return_value = mock_validator_instance
        mock_validator_instance.process_local_file.side_effect = Exception(
            "Validation failed"
        )

        test_args = [
            "main.py",
            "--table-name",
            "blood",
            "--input-file",
            "test.csv",
            "--mapping-config",
            "config.json",
            "--source",
            "test_source",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("os.getenv")
    def test_missing_environment_variables(self, mock_getenv):
        """Test error when required environment variables are missing"""
        # Mock missing S3_BUCKET
        mock_getenv.side_effect = lambda key: {
            "GSID_SERVICE_URL": "http://test-gsid",
            "GSID_API_KEY": "test-key",
            "NOCODB_URL": "http://test-nocodb",
            "NOCODB_API_TOKEN": "test-token",
        }.get(key)

        test_args = [
            "main.py",
            "--table-name",
            "blood",
            "--input-file",
            "test.csv",
            "--mapping-config",
            "config.json",
            "--source",
            "test_source",
        ]

        with patch("main.settings.load_mapping_config", return_value={}):
            with patch.object(sys, "argv", test_args):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1
