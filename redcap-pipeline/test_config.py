#!/usr/bin/env python3
"""
Diagnostic script to test project configuration and API tokens
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 80)
    logger.info("REDCap Configuration Diagnostic")
    logger.info("=" * 80)

    # Check environment variables
    logger.info("\n1. Environment Variables:")
    env_vars = [
        "REDCAP_API_URL",
        "REDCAP_PRIMARY_TOKEN",
        "REDCAP_LEGACY_TOKEN",
        "REDCAP_TRIAL001_TOKEN",
        "REDCAP_API_TOKEN",  # Legacy
        "REDCAP_PROJECT_ID",  # Legacy
    ]

    for var in env_vars:
        value = os.getenv(var)
        if value:
            # Mask sensitive values
            if "TOKEN" in var or "KEY" in var:
                masked = f"{value[:4]}...{value[-4:]}" if len(value) > 8 else "***"
                logger.info(f"  ✓ {var} = {masked} (length: {len(value)})")
            else:
                logger.info(f"  ✓ {var} = {value}")
        else:
            logger.warning(f"  ✗ {var} = NOT SET")

    # Load projects configuration
    logger.info("\n2. Loading Projects Configuration:")
    try:
        from core.config import settings

        projects = settings.load_projects_config()
        logger.info(f"  ✓ Loaded {len(projects)} projects")

        for project_key, project_config in projects.items():
            logger.info(f"\n  Project: {project_key}")
            logger.info(f"    Name: {project_config.project_name}")
            logger.info(f"    REDCap ID: {project_config.redcap_project_id}")
            logger.info(f"    Enabled: {project_config.enabled}")
            logger.info(f"    Schedule: {project_config.schedule}")

            # Check token
            if project_config.api_token:
                masked = (
                    f"{project_config.api_token[:4]}...{project_config.api_token[-4:]}"
                )
                logger.info(
                    f"    Token: {masked} (length: {len(project_config.api_token)})"
                )
            else:
                logger.error(f"    Token: NOT SET!")

    except Exception as e:
        logger.error(f"  ✗ Failed to load configuration: {e}", exc_info=True)
        return 1

    # Test REDCap API connection
    logger.info("\n3. Testing REDCap API Connections:")
    try:
        from services.redcap_client import REDCapClient

        for project_key, project_config in projects.items():
            if not project_config.enabled:
                logger.info(f"  ⊘ Skipping disabled project: {project_key}")
                continue

            logger.info(f"\n  Testing: {project_key}")
            try:
                client = REDCapClient(project_config)
                info = client.get_project_info()
                logger.info(f"    ✓ Connected successfully")
                logger.info(f"    Project Title: {info.get('project_title')}")
                logger.info(f"    Project ID: {info.get('project_id')}")
                logger.info(f"    Record Count: {info.get('record_count', 'N/A')}")
            except Exception as e:
                logger.error(f"    ✗ Connection failed: {e}")

    except Exception as e:
        logger.error(f"  ✗ Failed to test connections: {e}", exc_info=True)
        return 1

    logger.info("\n" + "=" * 80)
    logger.info("Diagnostic Complete")
    logger.info("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())
