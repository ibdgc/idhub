# fragment-validator/tests/test_config.py
import os

import pytest
from core.config import Settings


class TestConfig:
    """Unit tests for configuration settings"""

    def test_settings_instance(self):
        """Test that settings instance can be created"""
        settings = Settings()
        assert settings is not None

    def test_load_mapping_config(self, tmp_path):
        """Test loading mapping configuration from file"""
        import json

        settings = Settings()
        config_data = {
            "field_mapping": {"target": "source"},
            "subject_id_candidates": ["id1", "id2"],
        }

        config_file = tmp_path / "test_mapping.json"
        config_file.write_text(json.dumps(config_data))

        loaded_config = settings.load_mapping_config(str(config_file))
        assert loaded_config == config_data

    def test_load_mapping_config_not_found(self):
        """Test error when mapping config file not found"""
        settings = Settings()
        with pytest.raises(FileNotFoundError):
            settings.load_mapping_config("nonexistent.json")

    def test_load_mapping_config_invalid_json(self, tmp_path):
        """Test error when mapping config has invalid JSON"""
        settings = Settings()
        config_file = tmp_path / "invalid.json"
        config_file.write_text("{ invalid json }")

        with pytest.raises(Exception):  # Will raise JSONDecodeError
            settings.load_mapping_config(str(config_file))
