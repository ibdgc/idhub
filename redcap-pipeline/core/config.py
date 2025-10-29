import json
import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


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

        # Validate that token was resolved
        if not self.api_token or self.api_token.startswith("${"):
            raise ValueError(
                f"API token for project '{project_key}' not properly configured. "
                f"Token value: {self.api_token}"
            )

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
    def _resolve_env_var(value: str) -> str:
        """
        Resolve environment variable references in config values.
        Supports ${ENV_VAR} and $ENV_VAR syntax.
        """
        if not isinstance(value, str):
            return value

        # Pattern to match ${VAR} or $VAR
        pattern = r"\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)"

        def replacer(match):
            var_name = match.group(1) or match.group(2)
            env_value = os.getenv(var_name)
            if env_value is None:
                logger.warning(f"Environment variable '{var_name}' not found")
                return match.group(0)  # Return original if not found
            return env_value

        return re.sub(pattern, replacer, value)

    @staticmethod
    @lru_cache(maxsize=None)
    def load_projects_config() -> Dict[str, ProjectConfig]:
        """Load multi-project configuration"""
        config_path = Path(__file__).parent.parent / "config" / "projects.json"

        if not config_path.exists():
            logger.info("No projects.json found, using legacy single-project mode")
            # Fallback to legacy single-project mode using env variables
            project_key = "default"
            api_token = os.getenv("REDCAP_API_TOKEN")

            if not api_token:
                raise ValueError("REDCAP_API_TOKEN environment variable not set")

            return {
                project_key: ProjectConfig(
                    project_key,
                    {
                        "name": os.getenv("REDCAP_PROJECT_NAME", "Default Project"),
                        "redcap_project_id": os.getenv("REDCAP_PROJECT_ID", "unknown"),
                        "api_token": api_token,
                        "field_mappings": "field_mappings.json",
                        "schedule": "continuous",
                        "enabled": True,
                    },
                )
            }

        logger.info(f"Loading projects configuration from {config_path}")

        with open(config_path) as f:
            projects_data = json.load(f)

        # Resolve environment variables in all config values
        projects = {}
        for project_key, config in projects_data["projects"].items():
            # Resolve all string values in config
            resolved_config = {}
            for key, value in config.items():
                if isinstance(value, str):
                    resolved_value = Settings._resolve_env_var(value)
                    resolved_config[key] = resolved_value

                    # Log token resolution (masked)
                    if key == "api_token":
                        if resolved_value and not resolved_value.startswith("${"):
                            logger.info(
                                f"Project '{project_key}': API token resolved "
                                f"(length: {len(resolved_value)})"
                            )
                        else:
                            logger.error(
                                f"Project '{project_key}': API token NOT resolved! "
                                f"Value: {resolved_value}"
                            )
                else:
                    resolved_config[key] = value

            try:
                projects[project_key] = ProjectConfig(project_key, resolved_config)
                logger.info(
                    f"✓ Loaded project '{project_key}': {resolved_config.get('name')} "
                    f"(REDCap ID: {resolved_config.get('redcap_project_id')})"
                )
            except ValueError as e:
                logger.error(f"✗ Failed to load project '{project_key}': {e}")
                # Don't add this project to the list
                continue

        if not projects:
            raise ValueError("No valid projects found in configuration")

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
