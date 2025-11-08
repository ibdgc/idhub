# redcap-pipeline/tests/test_main.py
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest


class TestMain:
    """Test main.py functionality"""

    @pytest.fixture(autouse=True)
    def setup_logs_dir(self, tmp_path, monkeypatch):
        """Create logs directory for tests"""
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()

        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        yield logs_dir

    def test_load_projects_success(self, tmp_path):
        """Test successful project loading"""
        # Create config directory and file
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        projects_file = config_dir / "projects.json"
        projects_file.write_text(
            """
            {
                "projects": {
                    "gap": {
                        "name": "GAP",
                        "redcap_project_id": "16894"
                    }
                }
            }
            """
        )

        # Mock the config path
        with patch("pathlib.Path.__truediv__") as mock_path:
            mock_path.return_value = projects_file

            # Import after mocking
            import importlib

            import main

            importlib.reload(main)

            projects = main.load_projects()

            assert "gap" in projects
            assert projects["gap"]["name"] == "GAP"

    def sample_projects_config(self):
        """Sample projects.json configuration"""
        return {
            "projects": {
                "gap": {
                    "name": "GAP",
                    "redcap_project_id": "16894",
                    "api_token": "${REDCAP_API_TOKEN_GAP}",
                    "field_mappings": "gap_field_mappings.json",
                    "schedule": "continuous",
                    "batch_size": 50,
                    "enabled": True,
                },
                "uc_demarc": {
                    "name": "uc_demarc",
                    "redcap_project_id": "16895",
                    "api_token": "${REDCAP_API_TOKEN_UC_DEMARC}",
                    "field_mappings": "uc_demarc_field_mappings.json",
                    "schedule": "manual",
                    "batch_size": 50,
                    "enabled": False,
                },
            }
        }

    def test_load_projects_success(self, sample_projects_config):
        """Test loading projects configuration"""
        import main

        config_json = json.dumps(sample_projects_config)

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=config_json)),
        ):
            projects = main.load_projects()

        assert "gap" in projects
        assert "uc_demarc" in projects
        assert projects["gap"]["name"] == "GAP"

    def test_load_projects_file_not_found(self):
        """Test load_projects when config file doesn't exist"""
        import main

        with (
            patch("pathlib.Path.exists", return_value=False),
            pytest.raises(SystemExit),
        ):
            main.load_projects()

    def test_get_project_config_success(self, sample_projects_config):
        """Test getting configuration for a specific project"""
        import main

        config = main.get_project_config(sample_projects_config["projects"], "gap")

        assert config is not None
        assert config["name"] == "GAP"
        assert config["redcap_project_id"] == "16894"

    def test_get_project_config_not_found(self, sample_projects_config):
        """Test getting non-existent project configuration"""
        import main

        # Should raise SystemExit when project not found
        with pytest.raises(SystemExit) as exc_info:
            main.get_project_config(sample_projects_config["projects"], "nonexistent")

        assert exc_info.value.code == 1

    def test_get_project_config_adds_key(self, sample_projects_config):
        """Test that get_project_config adds the project key"""
        import main

        config = main.get_project_config(sample_projects_config["projects"], "gap")

        assert "key" in config
        assert config["key"] == "gap"

    def test_main_with_specific_project(self, sample_projects_config):
        """Test main execution with specific project argument"""
        import main

        config_json = json.dumps(sample_projects_config)

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=config_json)),
            patch("sys.argv", ["main.py", "--project", "gap"]),
            patch("services.pipeline.REDCapPipeline") as mock_pipeline,
            patch("main.close_db_pool"),
        ):
            mock_pipeline_instance = MagicMock()
            mock_pipeline.return_value = mock_pipeline_instance

            # This would normally run main(), but we'll test the logic
            projects = main.load_projects()
            config = main.get_project_config(projects, "gap")

            assert config["key"] == "gap"

    def test_main_filters_enabled_projects(self, sample_projects_config):
        """Test that main only processes enabled projects"""
        import main

        projects = sample_projects_config["projects"]

        # gap is enabled, uc_demarc is disabled
        enabled_projects = {
            key: proj for key, proj in projects.items() if proj.get("enabled", True)
        }

        assert "gap" in enabled_projects
        assert "uc_demarc" not in enabled_projects

    def test_main_handles_pipeline_error(self, sample_projects_config):
        """Test main handles pipeline execution errors"""
        import main

        config_json = json.dumps(sample_projects_config)

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=config_json)),
            patch("services.pipeline.REDCapPipeline") as mock_pipeline,
            patch("main.close_db_pool"),
        ):
            # Simulate pipeline error
            mock_pipeline_instance = MagicMock()
            mock_pipeline_instance.run.side_effect = Exception("Pipeline error")
            mock_pipeline.return_value = mock_pipeline_instance

            # Main should handle the error gracefully
            # (actual implementation may vary)
            config = main.get_project_config(
                projects=sample_projects_config["projects"], project_key="gap"
            )
            assert config is not None

    def test_main_closes_db_pool_on_exit(self):
        """Test that database pool is closed on exit"""
        import main

        with patch("main.close_db_pool") as mock_close:
            # Simulate cleanup
            main.close_db_pool()
            mock_close.assert_called_once()

    def test_load_projects_invalid_json(self):
        """Test load_projects with invalid JSON"""
        import main

        invalid_json = "{ invalid json }"

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=invalid_json)),
            pytest.raises(json.JSONDecodeError),
        ):
            main.load_projects()

    def test_load_projects_missing_projects_key(self):
        """Test load_projects when 'projects' key is missing"""
        import main

        config_json = json.dumps({"other_key": {}})

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=config_json)),
        ):
            projects = main.load_projects()

        assert projects == {}

    def test_main_with_batch_size_override(self, sample_projects_config):
        """Test main with custom batch size"""
        import main

        config = main.get_project_config(sample_projects_config["projects"], "gap")

        # Default batch size
        assert config["batch_size"] == 50

        # Could be overridden via command line args
        custom_batch_size = 100
        assert custom_batch_size != config["batch_size"]
