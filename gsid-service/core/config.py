import json
import os
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "idhub")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    # REDCap
    REDCAP_API_URL: str = os.getenv("REDCAP_API_URL", "")
    REDCAP_API_TOKEN: str = os.getenv("REDCAP_API_TOKEN", "")
    REDCAP_PROJECT_ID: str = os.getenv("REDCAP_PROJECT_ID", "16894")

    # GSID Service
    GSID_SERVICE_URL: str = os.getenv("GSID_SERVICE_URL", "http://gsid-service:8000")
    GSID_API_KEY: str = os.getenv("GSID_API_KEY", "")

    # S3
    S3_BUCKET: str = os.getenv("S3_BUCKET", "idhub-curated-fragments")
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @property
    def FIELD_MAPPINGS(self):
        """Load field mappings from JSON config"""
        config_path = Path(__file__).parent.parent / "config" / "field_mappings.json"
        with open(config_path) as f:
            return json.load(f)

    # Center Aliases
    CENTER_ALIASES = {
        "mount_sinai": "MSSM",
        "mount_sinai_ny": "MSSM",
        "mount-sinai": "MSSM",
        "mt_sinai": "MSSM",
        "cedars_sinai": "Cedars-Sinai",
        "cedars-sinai": "Cedars-Sinai",
        "university_of_chicago": "University of Chicago",
        "uchicago": "University of Chicago",
        "u_chicago": "University of Chicago",
        "johns_hopkins": "Johns Hopkins",
        "jhu": "Johns Hopkins",
        "mass_general": "Massachusetts General Hospital",
        "mgh": "Massachusetts General Hospital",
        "pitt": "Pittsburgh",
        "upitt": "Pittsburgh",
        "university_of_pittsburgh": "Pittsburgh",
    }


settings = Settings()

