# fragment-validator/tests/test_main.py
from unittest.mock import MagicMock, patch, mock_open
import pytest
from main import main
import sys
import json
from pathlib import Path

@pytest.fixture
def set_env_vars(monkeypatch):
    """Set required environment variables"""
    monkeypatch.setenv("NOCODB_TOKEN", "test-token")
    monkeypatch.setenv("GSID_API_KEY", "test-key")

@patch("main.S3Client")
@patch("main.NocoDBClient")
@patch("main.GSIDClient")
class TestMainCLI:
    """Unit tests for main CLI interface"""

    @patch("core.config.Path.exists", return_value=True)
    def test_validate_command(self, mock_exists, mock_gsid_client, mock_nocodb_client, mock_s3_client, set_env_vars, tmp_path):
        """Test validate command execution"""
        with patch("main.FragmentValidator") as mock_fragment_validator:
            mock_validator_instance = mock_fragment_validator.return_value
            mock_validator_instance.process_local_file.return_value = {
                "status": "VALIDATED",
                "row_count": 10,
            }

            test_args = [
                "main.py",
                "--table-name", "blood",
                "--input-file", "test.csv",
                "--mapping-config", "config.json",
                "--source", "test_source",
            ]

            with patch.object(sys, "argv", test_args):
                with pytest.raises(SystemExit) as exc_info:
                    with patch("builtins.open", mock_open(read_data=json.dumps({"field_mapping": {}}))):
                        main()
                assert exc_info.value.code == 0

            mock_validator_instance.process_local_file.assert_called_once()

    @patch("core.config.Path.exists", return_value=True)
    def test_auto_approve_flag(self, mock_exists, mock_gsid_client, mock_nocodb_client, mock_s3_client, set_env_vars):
        """Test auto-approve flag is passed correctly"""
        with patch("main.FragmentValidator") as mock_fragment_validator:
            mock_validator_instance = mock_fragment_validator.return_value
            mock_validator_instance.process_local_file.return_value = {
                "status": "APPROVED",
            }

            test_args = [
                "main.py",
                "--table-name", "blood",
                "--input-file", "test.csv",
                "--mapping-config", "config.json",
                "--source", "test_source",
                "--auto-approve",
            ]

            with patch.object(sys, "argv", test_args):
                with pytest.raises(SystemExit) as exc_info:
                    with patch("builtins.open", mock_open(read_data=json.dumps({"field_mapping": {}}))):
                        main()
                assert exc_info.value.code == 0

            # Check that process_local_file was called with auto_approve=True
            call_args, call_kwargs = mock_validator_instance.process_local_file.call_args
            assert call_args[4] is True

    def test_missing_required_args(self, mock_gsid_client, mock_nocodb_client, mock_s3_client, set_env_vars):
        """Test error when required arguments are missing"""
        test_args = ["main.py", "--table-name", "blood"]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code != 0

    @patch("core.config.Path.exists", return_value=False)
    def test_missing_mapping_config(self, mock_exists, mock_gsid_client, mock_nocodb_client, mock_s3_client, set_env_vars):
        """Test error when mapping config file not found"""
        test_args = [
            "main.py",
            "--table-name", "blood",
            "--input-file", "test.csv",
            "--mapping-config", "missing.json",
            "--source", "test_source",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
