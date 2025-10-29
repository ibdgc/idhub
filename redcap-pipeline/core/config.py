import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ProjectConfig:
    """Configuration for a single REDCap project"""

    def __init__(
        self,
        project_key: str,
        project_name: str,
        redcap_project_id: str,
        api_token: str,
        field_mappings_file: str,
        schedule: str = "manual",
        batch_size: int = 50,
        enabled: bool = True,
        description: str = "",
        redcap_api_url: str = None,
    ):
        self.project_key = project_key
        self.project_name = project_name
        self.redcap_project_id = redcap_project_id
        self.api_token = api_token
        self.field_mappings_file = field_mappings_file
        self.schedule = schedule
        self.batch_size = batch_size
        self.enabled = enabled
        self.description = description
        # Use provided URL or fall back to settings
        self.redcap_api_url = redcap_api_url or os.getenv("REDCAP_API_URL")

    def load_field_mappings(self) -> Dict:
        """Load field mappings from JSON file"""
        config_dir = Path(__file__).parent.parent / "config"
        mapping_path = config_dir / self.field_mappings_file

        if not mapping_path.exists():
            raise FileNotFoundError(
                f"Field mappings file not found: {self.field_mappings_file}"
            )

        with open(mapping_path) as f:
            mappings = json.load(f)

        # Convert old format to new format if needed
        if "mappings" in mappings:
            # Old format - convert to new format
            converted = {
                "demographics": {},
                "specimen": {},
                "family": {},
                "clinical": {},
            }

            for mapping in mappings["mappings"]:
                target_table = mapping.get("target_table")
                source_field = mapping.get("source_field")

                if target_table == "centers":
                    converted["demographics"]["center_name"] = source_field
                elif target_table == "local_subject_ids":
                    if "local_subject_id" not in converted["demographics"]:
                        converted["demographics"]["local_subject_id"] = source_field
                elif target_table == "specimen":
                    # Use sample_type as key if available
                    sample_type = mapping.get("sample_type", "specimen_id")
                    if sample_type == "specimen_id" or not converted["specimen"]:
                        converted["specimen"]["specimen_id"] = source_field
                elif target_table == "family":
                    converted["family"]["family_id"] = source_field
                elif target_table == "subjects":
                    target_field = mapping.get("target_field")
                    converted["clinical"][target_field] = source_field

            return converted

        # New format - return as is
        return mappings


class Settings:
    # REDCap Configuration (shared across projects)
    REDCAP_API_URL = os.getenv("REDCAP_API_URL")

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
        "mssm": "MSSM",
        "cedars_sinai": "Cedars-Sinai",
        "cedars": "Cedars-Sinai",
        "university_of_pittsburgh": "Pittsburgh",
        "pitt": "Pittsburgh",
        "upmc": "Pittsburgh",
    }

    @staticmethod
    def _resolve_token(token_value: str, project_key: str) -> str:
        """Resolve token from environment variable if needed"""
        if not token_value:
            raise ValueError(f"API token for project '{project_key}' is empty")

        # Check if it's a variable reference like ${VAR_NAME}
        if token_value.startswith("${") and token_value.endswith("}"):
            var_name = token_value[2:-1]
            logger.info(
                f"Project '{project_key}': Resolving token from env var '{var_name}'"
            )

            resolved = os.getenv(var_name)
            if not resolved:
                logger.warning(f"Environment variable '{var_name}' not found")
                raise ValueError(
                    f"API token for project '{project_key}' not properly configured. "
                    f"Token value: {token_value}"
                )

            logger.info(
                f"Project '{project_key}': API token resolved (length: {len(resolved)})"
            )
            return resolved

        # Direct token value
        logger.info(
            f"Project '{project_key}': Using direct API token (length: {len(token_value)})"
        )
        return token_value

    @staticmethod
    def load_projects_config() -> Dict[str, ProjectConfig]:
        """Load all project configurations from projects.json"""
        config_dir = Path(__file__).parent.parent / "config"
        projects_file = config_dir / "projects.json"

        logger.info(f"Loading projects configuration from {projects_file}")

        if not projects_file.exists():
            raise FileNotFoundError(f"Projects config not found: {projects_file}")

        with open(projects_file) as f:
            config_data = json.load(f)

        projects = {}
        for project_key, project_data in config_data.get("projects", {}).items():
            try:
                # Resolve API token
                token = Settings._resolve_token(
                    project_data.get("api_token", ""), project_key
                )

                projects[project_key] = ProjectConfig(
                    project_key=project_key,
                    project_name=project_data.get("name", project_key),
                    redcap_project_id=project_data.get("redcap_project_id"),
                    api_token=token,
                    field_mappings_file=project_data.get("field_mappings"),
                    schedule=project_data.get("schedule", "manual"),
                    batch_size=project_data.get("batch_size", 50),
                    enabled=project_data.get("enabled", True),
                    description=project_data.get("description", ""),
                    redcap_api_url=Settings.REDCAP_API_URL,
                )

                logger.info(
                    f"✓ Loaded project '{project_key}': {projects[project_key].project_name} "
                    f"(REDCap ID: {projects[project_key].redcap_project_id})"
                )

            except Exception as e:
                logger.error(f"✗ Failed to load project '{project_key}': {e}")
                continue

        return projects


settings = Settings()
