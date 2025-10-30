import json
import logging
import os
import sys
from pathlib import Path

import boto3
import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def test_database_connection():
    """Test PostgreSQL database connection"""
    logger.info("Testing database connection...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        logger.info(f"✓ Database connected: {version}")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False


def test_gsid_service():
    """Test GSID service API"""
    logger.info("Testing GSID service...")
    try:
        url = os.getenv("GSID_SERVICE_URL", "http://gsid-service:8000")
        headers = {"x-api-key": os.getenv("GSID_API_KEY")}

        response = requests.get(f"{url}/health", headers=headers, timeout=10)
        response.raise_for_status()

        data = response.json()
        logger.info(f"✓ GSID service connected: {data}")
        return True
    except Exception as e:
        logger.error(f"✗ GSID service connection failed: {e}")
        return False


def test_s3_access():
    """Test S3 bucket access"""
    logger.info("Testing S3 access...")
    try:
        s3_client = boto3.client("s3")
        bucket = os.getenv("S3_BUCKET", "idhub-curated-fragments")

        # Test bucket access
        s3_client.head_bucket(Bucket=bucket)
        logger.info(f"✓ S3 bucket accessible: {bucket}")
        return True
    except Exception as e:
        logger.error(f"✗ S3 access failed: {e}")
        return False


def test_redcap_api(project_config: dict):
    """Test REDCap API access"""
    project_key = project_config.get("key", "unknown")
    logger.info(f"Testing REDCap API for project '{project_key}'...")

    try:
        url = os.getenv("REDCAP_API_URL")
        token = project_config.get("api_token")

        if not token:
            logger.error(f"✗ No API token configured for project '{project_key}'")
            return False

        payload = {
            "token": token,
            "content": "record",
            "format": "json",
            "type": "flat",
            "returnFormat": "json",
        }

        response = requests.post(url, data=payload, timeout=30)
        response.raise_for_status()

        records = response.json()
        logger.info(
            f"✓ REDCap API connected for '{project_key}': {len(records)} records available"
        )
        return True
    except Exception as e:
        logger.error(f"✗ REDCap API connection failed for '{project_key}': {e}")
        return False


def test_field_mappings(project_config: dict):
    """Test field mapping configuration"""
    project_key = project_config.get("key", "unknown")
    mapping_file = project_config.get("field_mappings")

    logger.info(f"Testing field mappings for project '{project_key}'...")

    if not mapping_file:
        logger.warning(f"⚠ No field mappings configured for '{project_key}'")
        return True

    try:
        mapping_path = Path(__file__).parent / "config" / mapping_file

        if not mapping_path.exists():
            logger.error(f"✗ Mapping file not found: {mapping_path}")
            return False

        with open(mapping_path) as f:
            config = json.load(f)

        mappings = config.get("mappings", [])
        transformations = config.get("transformations", {})

        # Validate structure
        for mapping in mappings:
            if not all(
                k in mapping for k in ["source_field", "target_table", "target_field"]
            ):
                logger.error(f"✗ Invalid mapping structure: {mapping}")
                return False

        logger.info(
            f"✓ Field mappings valid for '{project_key}': "
            f"{len(mappings)} mappings, {len(transformations)} transformations"
        )
        return True
    except Exception as e:
        logger.error(f"✗ Field mapping validation failed for '{project_key}': {e}")
        return False


def load_projects() -> dict:
    """Load project configurations"""
    config_path = Path(__file__).parent / "config" / "projects.json"

    if not config_path.exists():
        logger.error(f"✗ Projects configuration not found: {config_path}")
        return {}

    with open(config_path) as f:
        config = json.load(f)

    projects = config.get("projects", {})

    # Substitute environment variables and add keys
    for key, project in projects.items():
        project["key"] = key
        if "api_token" in project:
            api_token = project["api_token"]
            if api_token.startswith("${") and api_token.endswith("}"):
                env_var = api_token[2:-1]
                project["api_token"] = os.getenv(env_var)

    return projects


def main():
    """Run all configuration tests"""
    logger.info("=" * 60)
    logger.info("REDCap Pipeline Configuration Diagnostics")
    logger.info("=" * 60)

    tests_passed = 0
    tests_failed = 0

    # Core infrastructure tests
    if test_database_connection():
        tests_passed += 1
    else:
        tests_failed += 1

    if test_gsid_service():
        tests_passed += 1
    else:
        tests_failed += 1

    if test_s3_access():
        tests_passed += 1
    else:
        tests_failed += 1

    # Load projects
    projects = load_projects()

    if not projects:
        logger.error("✗ No projects configured")
        tests_failed += 1
    else:
        logger.info(f"Found {len(projects)} project(s): {', '.join(projects.keys())}")

        # Test each project configuration
        for project_key, project_config in projects.items():
            logger.info(f"\n--- Testing project: {project_key} ---")

            if test_redcap_api(project_config):
                tests_passed += 1
            else:
                tests_failed += 1

            if test_field_mappings(project_config):
                tests_passed += 1
            else:
                tests_failed += 1

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info(f"Tests passed: {tests_passed}")
    logger.info(f"Tests failed: {tests_failed}")
    logger.info("=" * 60)

    if tests_failed > 0:
        logger.error("✗ Some tests failed")
        sys.exit(1)
    else:
        logger.info("✓ All tests passed")
        sys.exit(0)


if __name__ == "__main__":
    main()
