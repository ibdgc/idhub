# fragment-validator/main.py
import argparse
import logging
import sys

import boto3
from dotenv import load_dotenv

from core.config import settings
from services import (
    FragmentValidator,
    GSIDClient,
    NocoDBClient,
    S3Client,
    SubjectIDResolver,  # Add this import
)

# Load environment variables FIRST
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Environment-specific configuration
ENV_CONFIG = {
    "qa": {
        "S3_BUCKET": "idhub-curated-fragments-qa",
        "GSID_SERVICE_URL": "https://api.qa.idhub.ibdgc.org",
        "NOCODB_URL": "https://qa.idhub.ibdgc.org",
    },
    "production": {
        "S3_BUCKET": "idhub-curated-fragments",
        "GSID_SERVICE_URL": "https://api.idhub.ibdgc.org",
        "NOCODB_URL": "https://idhub.ibdgc.org",
    },
}


def get_aws_credentials():
    """Get AWS credentials from boto3 session (uses AWS CLI config)"""
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials is None:
            raise ValueError("No AWS credentials found")
        return credentials
    except Exception as e:
        raise ValueError(f"Failed to load AWS credentials: {e}")


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

    # Load mapping config
    try:
        mapping_config = settings.load_mapping_config(args.mapping_config)
        logger.info(f"Loaded mapping config from {args.mapping_config}")
    except FileNotFoundError:
        logger.error(f"Mapping config file not found: {args.mapping_config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Invalid JSON in mapping config: {e}")
        sys.exit(1)

    # Get environment-specific configuration
    env_config = ENV_CONFIG[args.environment]
    s3_bucket = env_config["S3_BUCKET"]
    gsid_service_url = env_config["GSID_SERVICE_URL"]
    nocodb_url = env_config["NOCODB_URL"]

    # Get user-provided credentials from .env
    import os

    gsid_api_key = os.getenv("GSID_API_KEY")

    # Get environment-specific NocoDB token
    env_suffix = "_QA" if args.environment == "qa" else "_PROD"
    nocodb_token = os.getenv(f"NOCODB_API_TOKEN{env_suffix}")
    nocodb_base = os.getenv("NOCODB_BASE_ID")  # Optional

    # Validate AWS credentials
    try:
        aws_credentials = get_aws_credentials()
        logger.info("✓ AWS credentials loaded from CLI configuration")
    except ValueError as e:
        logger.error(f"✗ {e}")
        logger.error("\nPlease configure AWS CLI credentials:")
        logger.error("  aws configure")
        sys.exit(1)

    # Validate required environment variables
    if not gsid_api_key:
        logger.error("Missing required environment variable: GSID_API_KEY")
        logger.error("Please add to .env file: GSID_API_KEY=your_key_here")
        sys.exit(1)

    if not nocodb_token:
        logger.error(
            f"Missing required environment variable: NOCODB_API_TOKEN{env_suffix}"
        )
        logger.error(
            f"Please add to .env file: NOCODB_API_TOKEN{env_suffix}=your_token_here"
        )
        sys.exit(1)

    # Log configuration
    logger.info(f"Configuration:")
    logger.info(f"  Environment: {env_label}")
    logger.info(f"  S3 Bucket: {s3_bucket}")
    logger.info(f"  GSID Service: {gsid_service_url}")
    logger.info(f"  NocoDB URL: {nocodb_url}")
    logger.info(f"  NocoDB Base ID: {nocodb_base or 'auto-detect'}")
    logger.info(f"  GSID API Key: {'*' * 8}{gsid_api_key[-4:]}")
    logger.info(f"  NocoDB Token: {'*' * 8}{nocodb_token[-4:]}")
    logger.info(f"{'=' * 60}")

    try:
        # Initialize clients
        logger.info("Initializing S3 client...")
        s3_client = S3Client(s3_bucket)

        logger.info("Initializing NocoDB client...")
        nocodb_client = NocoDBClient(nocodb_url, nocodb_token, nocodb_base)

        logger.info("Initializing GSID client...")
        gsid_client = GSIDClient(gsid_service_url, gsid_api_key)

        logger.info("Initializing Subject ID Resolver...")
        subject_id_resolver = SubjectIDResolver(gsid_client)

        # Initialize validator with resolver
        logger.info("Initializing validator...")
        validator = FragmentValidator(s3_client, nocodb_client, subject_id_resolver)

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
        logger.error("1. Verify network connectivity to the services")
        logger.error("2. Check that API tokens are valid")
        logger.error(f"3. Confirm service URLs are accessible:")
        logger.error(f"   - {gsid_service_url}")
        logger.error(f"   - {nocodb_url}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
