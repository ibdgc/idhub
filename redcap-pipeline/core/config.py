import json
import logging
import os
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


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

    # Center Aliases - maps common variations to canonical center names
    CENTER_ALIASES = {
        # GAP project aliases
        "mount_sinai": "MSSM",
        "mount_sinai_ny": "MSSM",
        "mt_sinai": "MSSM",
        "sinai": "MSSM",
        "johns_hopkins": "Johns Hopkins",
        "jhu": "Johns Hopkins",
        "hopkins": "Johns Hopkins",
        "university_of_chicago": "UChicago",
        "uchicago": "UChicago",
        "u_chicago": "UChicago",
        "cedars_sinai": "Cedars-Sinai",
        "cedars": "Cedars-Sinai",
        "ucla": "UCLA",
        "university_of_california_los_angeles": "UCLA",
        "emory": "Emory",
        "emory_university": "Emory",
        "mayo": "Mayo",
        "mayo_clinic": "Mayo",
        "upenn": "Penn",
        "university_of_pennsylvania": "Penn",
        "penn": "Penn",
        
        # cd_ileal project aliases (REDCap data access groups)
        "mt_sinai_hospital": "MSSM",
        "icahn_school_of_me": "MSSM",
        "university_of_mont": "Montreal",
        "montreal": "Montreal",
        "university_of_pitt": "Pittsburgh",
        "pittsburgh": "Pittsburgh",
        "pitt": "Pittsburgh",
        "cedarssinai_medica": "Cedars-Sinai",
        "johns_hopkins_univ": "Johns Hopkins",
        "emory_university": "Emory",
    }

    # Fuzzy matching threshold (0.0-1.0)
    # Increased to 0.85 to prevent bad matches like Montreal->Miami
    FUZZY_MATCH_THRESHOLD = float(os.getenv("FUZZY_MATCH_THRESHOLD", "0.85"))


settings = Settings()

    @staticmethod
    def load_projects_config(config_path: str = None) -> List[dict]:
        """Load projects configuration from JSON file"""
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "projects.json"
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Projects config not found: {config_path}")

        logger.info(f"Loading projects configuration from {config_path}")

        with open(config_path) as f:
            config = json.load(f)

        projects_dict = config.get("projects", {})

        if not isinstance(projects_dict, dict):
            raise ValueError(
                f"Expected 'projects' to be a dictionary, got {type(projects_dict)}"
            )

        projects = []

        # Iterate through projects dictionary
        for project_key, project_config in projects_dict.items():
            if not isinstance(project_config, dict):
                logger.warning(
                    f"Project '{project_key}' config is not a dictionary, skipping"
                )
                continue

            # Add the key to the project config
            project_config["key"] = project_key

            # Resolve API token from environment variable
            token_env_var = project_config.get("api_token")
            if (
                token_env_var
                and token_env_var.startswith("${")
                and token_env_var.endswith("}")
            ):
                env_var_name = token_env_var[2:-1]
                logger.info(
                    f"Project '{project_key}': Resolving token from env var '{env_var_name}'"
                )

                api_token = os.getenv(env_var_name)
                if not api_token:
                    logger.warning(f"Environment variable '{env_var_name}' not found")
                    logger.error(
                        f"✗ Failed to load project '{project_key}': "
                        f"API token for project '{project_key}' not properly configured. "
                        f"Token value: {token_env_var}"
                    )
                    continue

                project_config["api_token"] = api_token
                logger.info(
                    f"Project '{project_key}': API token resolved (length: {len(api_token)})"
                )

            projects.append(project_config)
            logger.info(
                f"✓ Loaded project '{project_key}': {project_config.get('name')} "
                f"(REDCap ID: {project_config.get('redcap_project_id')})"
            )

        if not projects:
            logger.warning("No valid projects loaded from configuration")

        return projects

    def get_enabled_projects(self) -> List[dict]:
        """Get list of enabled project configurations"""
        projects = self.load_projects_config()
        enabled = [p for p in projects if p.get("enabled", True)]
        logger.info(
            f"Found {len(enabled)} enabled projects out of {len(projects)} total"
        )
        return enabled


settings = Settings()
