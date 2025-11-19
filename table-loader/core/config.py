# table-loader/core/config.py
import os
from typing import Optional


class Settings:
    """Application settings"""

    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")

    # Database
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_NAME: str = os.getenv("DB_NAME", "idhub")
    DB_USER: str = os.getenv("DB_USER", "idhub_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))

    # S3 - Auto-select based on environment
    @property
    def S3_BUCKET(self) -> str:
        """Get S3 bucket based on environment"""
        # Allow explicit override
        if os.getenv("S3_BUCKET"):
            return os.getenv("S3_BUCKET")

        # Auto-select based on environment
        if self.ENVIRONMENT == "qa":
            return "idhub-curated-fragments-qa"
        else:
            return "idhub-curated-fragments"

    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
