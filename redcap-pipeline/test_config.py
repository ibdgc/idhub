#!/usr/bin/env python3
"""
Configuration diagnostics for REDCap pipeline
Tests all connections and configurations before running pipeline
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import boto3
import psycopg2
import requests
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()


def test_database():
    """Test PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "idhub"),
            user=os.getenv("DB_USER", "idhub_user"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", "5432")),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
        conn.close()
        logger.info(f"✓ Database connected: {version}")
        return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False


def test_gsid_service():
    """Test GSID service connection"""
    try:
        url = os.getenv("GSID_SERVICE_URL", "https://api.idhub.ibdgc.org")
        response = requests.get(f"{url}/health", timeout=10)
        response.raise_for_status()
        logger.info(f"✓ GSID service connected: {response.json()}")
        return True
    except Exception as e:
        logger.error(f"✗ GSID service connection failed: {e}")
        return False


def test_s3():
    """Test S3 bucket access"""
    try:
        s3 = boto3.client("s3")
        bucket = os.getenv("S3_BUCKET", "idhub-curated-fragments")
        s3.head_bucket(Bucket=bucket)
        logger.info(f"✓ S3 bucket accessible: {bucket}")
        return True
    except ClientError as e:
        logger.error(f"✗ S3 bucket access failed: {e}")
        return False


def test_redcap_project(project_key: str, config: dict):
    """Test REDCap API connection for a specific project"""
    try:
        api_token = config.get("api_token", "").replace(
            "${REDCAP_API_TOKEN}", os.getenv("REDCAP_API_TOKEN", "")
        )

        if not api_token:
            logger.warning(f"⚠ No API token found for project '{project_key}'")
            return False

        data = {
            "token": api_token,
            "content": "record",
            "format": "json",
            "type": "flat",
            "returnFormat": "json",
        }

        response = requests.post(
            os.getenv("REDCAP_API_URL", "https://redcap.mountsinai.org/api/"),
            data=data,
            timeout=30,
        )
        response.raise_for_status()
        records = response.json()
        logger.info(
            f"✓ REDCap API connected for '{project_key}': {len(records)} records available"
        )
        return True
    except Exception as e:
        logger.error(f"✗ REDCap API connection failed for '{project_key}': {e}")
        return False


def test_field_mappings(project_key: str, config: dict):
    """Test field mappings configuration"""
    try:
        mapping_file = config.get("field_mappings")
        if not mapping_file:
            logger.warning(f"⚠ No field mappings configured for '{project_key}'")
            return False

        mapping_path = Path(__file__).parent / "config" / mapping_file
        if not mapping_path.exists():
            logger.error(
                f"✗ Field mappings file not found for '{project_key}': {mapping_path}"
            )
            return False

        with open(mapping_path) as f:
            mappings = json.load(f)

        num_mappings = len(mappings.get("mappings", []))
        num_transforms = len(mappings.get("transformations", {}))

        logger.info(
            f"✓ Field mappings valid for '{project_key}': "
            f"{num_mappings} mappings, {num_transforms} transformations"
        )
        return True
    except Exception as e:
        logger.error(f"✗ Field mappings test failed for '{project_key}': {e}")
        return False


def load_projects():
    """Load project configurations"""
    config_path = Path(__file__).parent / "config" / "projects.json"
    with open(config_path) as f:
        config = json.load(f)
    return config["projects"]


def main():
    """Run diagnostics"""
    parser = argparse.ArgumentParser(description="REDCap Pipeline Diagnostics")
    parser.add_argument(
        "--project",
        type=str,
        help="Only test specific project (optional)",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("REDCap Pipeline Configuration Diagnostics")
    logger.info("=" * 60)

    passed = 0
    failed = 0

    # Test core services
    logger.info("Testing database connection...")
    if test_database():
        passed += 1
    else:
        failed += 1

    logger.info("Testing GSID service...")
    if test_gsid_service():
        passed += 1
    else:
        failed += 1

    logger.info("Testing S3 access...")
    if test_s3():
        passed += 1
    else:
        failed += 1

    # Load projects
    try:
        projects = load_projects()
    except Exception as e:
        logger.error(f"✗ Failed to load projects configuration: {e}")
        sys.exit(1)

    # Filter projects if specific one requested
    if args.project:
        if args.project not in projects:
            logger.error(f"✗ Project '{args.project}' not found in configuration")
            sys.exit(1)
        projects = {args.project: projects[args.project]}
        logger.info(f"Testing only project: {args.project}")
    else:
        enabled_projects = {k: v for k, v in projects.items() if v.get("enabled", True)}
        project_keys = ", ".join(enabled_projects.keys())
        logger.info(f"Found {len(enabled_projects)} enabled project(s): {project_keys}")
        projects = enabled_projects

    # Test each project
    for project_key, project_config in projects.items():
        if not project_config.get("enabled", True):
            logger.info(f"⊘ Skipping disabled project: {project_key}")
            continue

        logger.info(f"--- Testing project: {project_key} ---")

        logger.info(f"Testing REDCap API for project '{project_key}'...")
        if test_redcap_project(project_key, project_config):
            passed += 1
        else:
            failed += 1

        logger.info(f"Testing field mappings for project '{project_key}'...")
        if test_field_mappings(project_key, project_config):
            passed += 1
        else:
            failed += 1

    # Summary
    logger.info("-" * 60)
    logger.info(f"Tests passed: {passed}")
    logger.info(f"Tests failed: {failed}")
    logger.info("=" * 60)

    if failed > 0:
        logger.error("✗ Some tests failed")
        sys.exit(1)
    else:
        logger.info("✓ All tests passed")
        sys.exit(0)


if __name__ == "__main__":
    main()
