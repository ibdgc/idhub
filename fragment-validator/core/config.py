# fragment-validator/core/config.py
import json
from pathlib import Path
from typing import Dict, List


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

    @staticmethod
    def load_table_config(table_name: str) -> Dict:
        """
        Load table configuration (natural keys, immutable fields, etc.)

        Args:
            table_name: Name of the table

        Returns:
            Dict with table configuration or default config if not found
        """
        config_path = Path(__file__).parent.parent / "config" / "table_configs.json"

        if not config_path.exists():
            # Return safe defaults if config file doesn't exist
            return {
                "natural_key": ["id"],
                "immutable_fields": ["created_at"],
                "update_strategy": "upsert",
            }

        with open(config_path) as f:
            all_configs = json.load(f)

        # Return table-specific config or default
        return all_configs.get(
            table_name,
            {
                "natural_key": ["id"],
                "immutable_fields": ["created_at"],
                "update_strategy": "upsert",
            },
        )

    @staticmethod
    def get_natural_key(table_name: str) -> List[str]:
        """Get natural key for a table"""
        config = Settings.load_table_config(table_name)
        return config.get("natural_key", ["id"])

    @staticmethod
    def get_immutable_fields(table_name: str) -> List[str]:
        """Get immutable fields for a table"""
        config = Settings.load_table_config(table_name)
        return config.get("immutable_fields", ["created_at"])


settings = Settings()
