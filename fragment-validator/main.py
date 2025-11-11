# fragment-validator/main.py
import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from core.config import settings
from services import (
    FragmentValidator,
    GSIDClient,
    NocoDBClient,
    S3Client,
)

# Load environment variables FIRST
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_env_var(base_name: str, environment: str, fallback: str = None) -> str:
    """
    Get environment variable with environment suffix.

    Args:
        base_name: Base variable name (e.g., 'GSID_API_KEY')
        environment: 'qa' or 'production'
        fallback: Fallback variable name if env-specific not found

    Returns:
        Environment variable value or None
    """
    env_suffix = "_QA" if environment == "qa" else "_PROD"
    env_var = f"{base_name}{env_suffix}"
    value = os.getenv(env_var)

    # If not found and fallback provided, try fallback
    if value is None and fallback:
        value = os.getenv(fallback)

    return value


def validate_url(url: str, service_name: str) -> bool:
    """Validate that URL is accessible (basic check)"""
    if not url:
        return False

    # Check if it's a Docker hostname (nocodb, gsid-service, etc.)
    if url.startswith("http://nocodb") or url.startswith("http://gsid-service"):
        logger.warning(
            f"{service_name} URL uses Docker hostname: {url}\n"
            f"  This will only work inside Docker containers.\n"
            f"  For local development, set the environment variable to use localhost or actual hostname."
        )

    return True


def main():
    parser = argparse.ArgumentParser(description="Validate and stage data fragments")
    parser.add_argument(
        "--environment",
        choices=["qa", "production"],
        default="qa",
        help="Target environment (default: qa)",
    )
    parser.add_argument(
        "--table-name", required=True, help="Target database table name"
    )
    parser.add_argument("--input-file", required=True, help="Local path to CSV file")
    parser.add_argument(
        "--mapping-config", required=True, help="Path to mapping config JSON file"
    )
    parser.add_argument("--source", required=True, help="Source system name")
    parser.add_argument(
        "--auto-approve", action="store_true", help="Auto-approve for loading"
    )
    args = parser.parse_args()

    # Log environment selection
    env_label = args.environment.upper()
    logger.info(f"{'=' * 60}")
    logger.info(f"Running in {env_label} environment")
    logger.info(f"{'=' * 60}")

    # Load mapping config using centralized method
    try:
        mapping_config = settings.load_mapping_config(args.mapping_config)
        logger.info(f"Loaded mapping config from {args.mapping_config}")
    except FileNotFoundError:
        logger.error(f"Mapping config file not found: {args.mapping_config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Invalid JSON in mapping config: {e}")
        sys.exit(1)

    # Get environment-specific variables
    s3_bucket = get_env_var("S3_BUCKET", args.environment, "S3_BUCKET")
    gsid_api_key = get_env_var("GSID_API_KEY", args.environment)
    nocodb_token = get_env_var("NOCODB_API_TOKEN", args.environment)

    # Shared variables (no environment suffix)
    # For local development, these should be overridden in .env
    gsid_service_url = os.getenv("GSID_SERVICE_URL", "http://gsid-service:8000")
    nocodb_url = os.getenv("NOCODB_URL", "http://nocodb:8080")
    nocodb_base = os.getenv("NOCODB_BASE_ID")  # Optional

    # AWS credentials (shared across environments)
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    # Validate required environment variables
    required_vars = {
        "S3_BUCKET": s3_bucket,
        "GSID_SERVICE_URL": gsid_service_url,
        "GSID_API_KEY": gsid_api_key,
        "NOCODB_URL": nocodb_url,
        "NOCODB_API_TOKEN": nocodb_token,
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
    }

    missing_vars = [k for k, v in required_vars.items() if not v]
    if missing_vars:
        logger.error(
            f"Missing required environment variables for {env_label}: {', '.join(missing_vars)}"
        )
        logger.error("\nExpected environment variables:")
        logger.error(f"  - GSID_API_KEY_{env_label.replace('PRODUCTION', 'PROD')}")
        logger.error(f"  - NOCODB_API_TOKEN_{env_label.replace('PRODUCTION', 'PROD')}")
        logger.error(
            f"  - S3_BUCKET_{env_label.replace('PRODUCTION', 'PROD')} (or S3_BUCKET)"
        )
        logger.error(f"  - GSID_SERVICE_URL (shared)")
        logger.error(f"  - NOCODB_URL (shared)")
        logger.error(f"  - AWS_ACCESS_KEY_ID (shared)")
        logger.error(f"  - AWS_SECRET_ACCESS_KEY (shared)")
        logger.error("\nFor local development, create a .env file with:")
        logger.error(f"  NOCODB_URL=https://qa.idhub.ibdgc.org")
        logger.error(f"  GSID_SERVICE_URL=https://api-qa.idhub.ibdgc.org")
        sys.exit(1)

    # Validate URLs
    validate_url(nocodb_url, "NocoDB")
    validate_url(gsid_service_url, "GSID Service")

    # Log configuration being used (mask sensitive values)
    logger.info(f"Configuration:")
    logger.info(f"  Environment: {env_label}")
    logger.info(f"  S3 Bucket: {s3_bucket}")
    logger.info(f"  AWS Region: {aws_region}")
    logger.info(f"  GSID Service: {gsid_service_url}")
    logger.info(f"  NocoDB URL: {nocodb_url}")
    logger.info(f"  NocoDB Base ID: {nocodb_base or 'auto-detect'}")
    logger.info(
        f"  GSID API Key: {'*' * 8}{gsid_api_key[-4:] if gsid_api_key else 'NOT SET'}"
    )
    logger.info(
        f"  NocoDB Token: {'*' * 8}{nocodb_token[-4:] if nocodb_token else 'NOT SET'}"
    )
    logger.info(f"{'=' * 60}")

    try:
        # Initialize clients
        logger.info("Initializing S3 client...")
        s3_client = S3Client(s3_bucket)

        logger.info("Initializing NocoDB client...")
        nocodb_client = NocoDBClient(nocodb_url, nocodb_token, nocodb_base)

        logger.info("Initializing GSID client...")
        gsid_client = GSIDClient(gsid_service_url, gsid_api_key)

        # Initialize validator
        logger.info("Initializing validator...")
        validator = FragmentValidator(s3_client, nocodb_client, gsid_client)

        # Process file
        logger.info(f"Processing file: {args.input_file}")
        logger.info(f"Target table: {args.table_name}")
        logger.info(f"Source: {args.source}")
        logger.info(f"Auto-approve: {args.auto_approve}")

        report = validator.process_local_file(
            args.table_name,
            args.input_file,
            mapping_config,
            args.source,
            args.auto_approve,
        )

        if report["status"] == "FAILED":
            logger.error("✗ Validation failed")
            sys.exit(1)
        else:
            logger.info("✓ Validation successful")
            logger.info(f"{'=' * 60}")
            sys.exit(0)

    except ConnectionError as e:
        logger.error(f"✗ Connection failed: {e}")
        logger.error("\nTroubleshooting:")
        logger.error("1. Check that service URLs are correct in your .env file")
        logger.error("2. For local development, use actual hostnames:")
        logger.error(f"   NOCODB_URL=https://qa.idhub.ibdgc.org")
        logger.error(f"   GSID_SERVICE_URL=https://api-qa.idhub.ibdgc.org")
        logger.error("3. Verify network connectivity to the services")
        logger.error("4. Check that API tokens are valid")
        sys.exit(1)
    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
