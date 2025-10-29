import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional


class ProjectConfig:
    """Configuration for a single REDCap project"""

    def __init__(self, project_key: str, config: dict):
        self.project_key = (
            project_key  # Our internal identifier (e.g., "primary_biobank")
        )
        self.project_name = config.get("name", project_key)
        self.redcap_project_id = config[
            "redcap_project_id"
        ]  # REDCap's numeric project ID
        self.api_token = config["api_token"]
        self.field_mappings_file = config.get("field_mappings", "field_mappings.json")
        self.schedule = config.get("schedule", "manual")  # "continuous" or "manual"
        self.batch_size = config.get("batch_size", 50)
        self.enabled = config.get("enabled", True)
        self.description = config.get("description", "")

    def load_field_mappings(self) -> dict:
        """Load project-specific field mappings"""
        config_path = Path(__file__).parent.parent / "config" / self.field_mappings_file
        with open(config_path) as f:
            return json.load(f)


class Settings:
    # REDCap Configuration (legacy - for backward compatibility)
    REDCAP_API_URL = os.getenv("REDCAP_API_URL")
    REDCAP_API_TOKEN = os.getenv("REDCAP_API_TOKEN")
    REDCAP_PROJECT_ID = os.getenv("REDCAP_PROJECT_ID", "default")

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
    def load_projects_config() -> Dict[str, ProjectConfig]:
        """Load multi-project configuration"""
        config_path = Path(__file__).parent.parent / "config" / "projects.json"

        if not config_path.exists():
            # Fallback to legacy single-project mode using env variables
            project_key = "default"
            return {
                project_key: ProjectConfig(
                    project_key,
                    {
                        "name": os.getenv("REDCAP_PROJECT_NAME", "Default Project"),
                        "redcap_project_id": os.getenv("REDCAP_PROJECT_ID", "unknown"),
                        "api_token": os.getenv("REDCAP_API_TOKEN"),
                        "field_mappings": "field_mappings.json",
                        "schedule": "continuous",
                        "enabled": True,
                    },
                )
            }

        with open(config_path) as f:
            projects_data = json.load(f)

        # Resolve environment variables in tokens and project IDs
        projects = {}
        for project_key, config in projects_data["projects"].items():
            # Replace ${ENV_VAR} with actual environment variable
            if "api_token" in config and config["api_token"].startswith("${"):
                env_var = config["api_token"].strip("${}")
                config["api_token"] = os.getenv(env_var)

            # Also support env vars for project IDs (optional)
            if "redcap_project_id" in config and str(
                config["redcap_project_id"]
            ).startswith("${"):
                env_var = str(config["redcap_project_id"]).strip("${}")
                config["redcap_project_id"] = os.getenv(env_var)

            projects[project_key] = ProjectConfig(project_key, config)

        return projects

    @staticmethod
    def get_project_config(project_key: str) -> Optional[ProjectConfig]:
        """Get configuration for a specific project by our internal key"""
        projects = Settings.load_projects_config()
        return projects.get(project_key)

    @staticmethod
    def get_project_by_redcap_id(redcap_project_id: str) -> Optional[ProjectConfig]:
        """Get configuration by REDCap's numeric project ID"""
        projects = Settings.load_projects_config()
        for project in projects.values():
            if project.redcap_project_id == str(redcap_project_id):
                return project
        return None

    @staticmethod
    @lru_cache(maxsize=None)
    def load_field_mappings():
        """Legacy method - loads default project mappings"""
        config_path = Path(__file__).parent.parent / "config" / "field_mappings.json"
        with open(config_path) as f:
            return json.load(f)


settings = Settings()
