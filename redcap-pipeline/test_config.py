import logging
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from core.config import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_environment_variables():
    """Test that required environment variables are set"""
    logger.info("=" * 80)
    logger.info("REDCap Configuration Diagnostic")
    logger.info("=" * 80)
    logger.info("1. Environment Variables:")

    # Check API URL
    if settings.REDCAP_API_URL:
        logger.info(f"✓ REDCAP_API_URL = ***")
    else:
        logger.error("✗ REDCAP_API_URL = NOT SET")

    # Check various token environment variables
    token_vars = [
        "REDCAP_PRIMARY_TOKEN",
        "REDCAP_LEGACY_TOKEN",
        "REDCAP_TRIAL001_TOKEN",
        "REDCAP_API_TOKEN",
    ]

    for var in token_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"✓ {var} = {value[:4]}...{value[-4:]} (length: {len(value)})")
        else:
            logger.warning(f"✗ {var} = NOT SET")

    # Check project ID (legacy)
    if settings.REDCAP_PROJECT_ID:
        logger.info(f"✓ REDCAP_PROJECT_ID = {settings.REDCAP_PROJECT_ID}")
    else:
        logger.warning(f"✗ REDCAP_PROJECT_ID = NOT SET")


def load_projects():
    """Test loading projects configuration"""
    logger.info("")
    logger.info("2. Loading Projects Configuration:")

    try:
        projects = settings.load_projects_config()
        logger.info(f"✓ Loaded {len(projects)} projects")
        logger.info("")

        if projects:
            logger.info("3. Project Details:")
            for project in projects:
                key = project.get("key")
                name = project.get("name")
                redcap_id = project.get("redcap_project_id")
                enabled = project.get("enabled", True)
                has_token = bool(project.get("api_token"))

                status = "✓ ENABLED" if enabled else "○ DISABLED"
                token_status = "✓ Has token" if has_token else "✗ No token"

                logger.info(f"  {status} - {key}")
                logger.info(f"    Name: {name}")
                logger.info(f"    REDCap ID: {redcap_id}")
                logger.info(f"    Token: {token_status}")
                logger.info(f"    Batch size: {project.get('batch_size', 50)}")
                logger.info(f"    Schedule: {project.get('schedule', 'unknown')}")
                logger.info("")

        # Test get_enabled_projects
        enabled = settings.get_enabled_projects()
        logger.info(f"4. Enabled Projects: {len(enabled)} of {len(projects)}")
        for project in enabled:
            logger.info(f"   ✓ {project['key']}")

        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Configuration diagnostic complete")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"✗ Failed to load configuration: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    try:
        test_environment_variables()
        load_projects()
        sys.exit(0)
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("✗ Configuration diagnostic failed")
        logger.error("=" * 80)
        sys.exit(1)
