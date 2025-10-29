#!/usr/bin/env python3
"""
REDCap Configuration Diagnostic Tool
Tests configuration loading and API connectivity
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def mask_token(token: str) -> str:
    """Mask API token for display"""
    if not token or len(token) < 8:
        return "***"
    return f"{token[:4]}...{token[-4:]}"


def check_env_vars():
    """Check required environment variables"""
    logger.info("1. Environment Variables:")

    # REDCap URL
    redcap_url = os.getenv("REDCAP_API_URL")
    if redcap_url:
        logger.info(f"  ✓ REDCAP_API_URL = ***")
    else:
        logger.error("  ✗ REDCAP_API_URL = NOT SET")

    # Check various token environment variables
    token_vars = [
        "REDCAP_PRIMARY_TOKEN",
        "REDCAP_LEGACY_TOKEN",
        "REDCAP_TRIAL001_TOKEN",
        "REDCAP_API_TOKEN",
    ]

    for var in token_vars:
        token = os.getenv(var)
        if token:
            logger.info(f"  ✓ {var} = {mask_token(token)} (length: {len(token)})")
        else:
            logger.warning(f"  ✗ {var} = NOT SET")

    # REDCap Project ID (legacy)
    project_id = os.getenv("REDCAP_PROJECT_ID")
    if project_id:
        logger.info(f"  ✓ REDCAP_PROJECT_ID = {project_id}")
    else:
        logger.warning("  ✗ REDCAP_PROJECT_ID = NOT SET")


def load_projects():
    """Load and validate projects configuration"""
    logger.info("")
    logger.info("2. Loading Projects Configuration:")

    try:
        from core.config import settings

        projects = settings.load_projects_config()
        logger.info(f"  ✓ Loaded {len(projects)} projects")

        logger.info("")
        for key, project in projects.items():
            logger.info(f"Project: {key}")

            # Access the raw config dict to get the name
            project_name = getattr(project, "project_name", key)

            logger.info(f"  Name: {project_name}")
            logger.info(f"  REDCap ID: {project.redcap_project_id}")
            logger.info(f"  Enabled: {project.enabled}")
            logger.info(f"  Schedule: {project.schedule}")
            logger.info(f"  Batch Size: {project.batch_size}")

            if hasattr(project, "description"):
                logger.info(f"  Description: {project.description}")

            logger.info(
                f"  Token: {mask_token(project.api_token)} (length: {len(project.api_token)})"
            )
            logger.info(f"  Field Mappings: {project.field_mappings_file}")
            logger.info("")

        return projects

    except Exception as e:
        logger.error(f"  ✗ Failed to load configuration: {e}", exc_info=True)
        raise


def test_connections(projects):
    """Test REDCap API connections for each project"""
    logger.info("3. Testing REDCap API Connections:")

    try:
        from services.redcap_client import REDCapClient

        for key, project in projects.items():
            if not project.enabled:
                logger.info(f"  ⊘ Skipping disabled project: {key}")
                continue

            project_name = getattr(project, "project_name", key)
            logger.info(f"  Testing project: {key} ({project_name})")

            try:
                client = REDCapClient(project)
                metadata = client.get_metadata()

                if metadata:
                    logger.info(f"    ✓ Connected successfully")
                    logger.info(
                        f"    ✓ Found {len(metadata)} fields in data dictionary"
                    )
                else:
                    logger.warning(f"    ⚠ Connected but no metadata returned")

            except Exception as e:
                logger.error(f"    ✗ Connection failed: {e}")

    except Exception as e:
        logger.error(f"  ✗ Failed to test connections: {e}", exc_info=True)
        raise


def main():
    logger.info("=" * 80)
    logger.info("REDCap Configuration Diagnostic")
    logger.info("=" * 80)

    try:
        # Check environment variables
        check_env_vars()

        # Load projects configuration
        projects = load_projects()

        # Test API connections
        test_connections(projects)

        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Configuration diagnostic complete")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("✗ Configuration diagnostic failed")
        logger.error("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())
