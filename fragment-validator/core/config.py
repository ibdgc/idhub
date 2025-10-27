# fragment-validator/core/config.py
import json
from pathlib import Path


class Settings:
    """Configuration helper methods"""

    @staticmethod
    def load_mapping_config(mapping_config_path: str) -> dict:
        """Load table-specific mapping configuration from file path"""
        config_path = Path(mapping_config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Mapping config not found: {mapping_config_path}")

        with open(config_path) as f:
            return json.load(f)


settings = Settings()
