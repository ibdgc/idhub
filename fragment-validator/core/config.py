# fragment-validator/core/config.py
import json
import os
from pathlib import Path


class Settings:
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    S3_BUCKET = os.getenv("S3_BUCKET", "idhub-curated-fragments")

    @staticmethod
    def load_table_schemas():
        """Load table schema definitions"""
        config_path = Path(__file__).parent.parent / "config" / "table_schemas.json"
        with open(config_path) as f:
            return json.load(f)

    @staticmethod
    def load_table_mapping(table_name: str):
        """Load table-specific mapping configuration"""
        config_path = (
            Path(__file__).parent.parent / "config" / f"{table_name}_mapping.json"
        )
        if config_path.exists():
            with open(config_path) as f:
                return json.load(f)
        return {}


settings = Settings()
