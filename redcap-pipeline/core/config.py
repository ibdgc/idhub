# redcap-pipeline/core/config.py
import json
import os
from functools import lru_cache
from pathlib import Path


class Settings:
    # REDCap Configuration
    REDCAP_API_URL = os.getenv("REDCAP_API_URL")
    REDCAP_API_TOKEN = os.getenv("REDCAP_API_TOKEN")
    REDCAP_PROJECT_ID = os.getenv("REDCAP_PROJECT_ID")

    # GSID Service
    GSID_SERVICE_URL = os.getenv("GSID_SERVICE_URL", "http://gsid-service:8000")
    GSID_API_KEY = os.getenv("GSID_API_KEY")

    # Database Configuration
    DB_HOST = os.getenv("DB_HOST", "idhub_db")
    DB_NAME = os.getenv("DB_NAME", "idhub")
    DB_USER = os.getenv("DB_USER", "idhub_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))

    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    S3_BUCKET = os.getenv("S3_BUCKET", "idhub-curated-fragments")

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

    @staticmethod
    @lru_cache(maxsize=None)
    def load_field_mappings():
        config_path = Path(__file__).parent.parent / "config" / "field_mappings.json"
        with open(config_path) as f:
            return json.load(f)


settings = Settings()