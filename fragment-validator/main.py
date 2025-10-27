# fragment-validator/main.py
import argparse
import logging
import os
import sys

from core.config import settings
from dotenv import load_dotenv
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


def main():
    parser = argparse.ArgumentParser(description="Validate and stage data fragments")
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

    # Get environment variables (read AFTER load_dotenv())
    s3_bucket = os.getenv("S3_BUCKET")
    gsid_service_url = os.getenv("GSID_SERVICE_URL")
    gsid_api_key = os.getenv("GSID_API_KEY")
    nocodb_url = os.getenv("NOCODB_URL")
    nocodb_token = os.getenv("NOCODB_API_TOKEN")
    nocodb_base = os.getenv("NOCODB_BASE_ID")  # Optional

    # Validate required environment variables
    required_vars = {
        "S3_BUCKET": s3_bucket,
        "GSID_SERVICE_URL": gsid_service_url,
        "GSID_API_KEY": gsid_api_key,
        "NOCODB_URL": nocodb_url,
        "NOCODB_API_TOKEN": nocodb_token,
    }

    missing_vars = [k for k, v in required_vars.items() if not v]
    if missing_vars:
        logger.error(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
        sys.exit(1)

    try:
        # Initialize clients
        s3_client = S3Client(s3_bucket)
        nocodb_client = NocoDBClient(nocodb_url, nocodb_token, nocodb_base)
        gsid_client = GSIDClient(gsid_service_url, gsid_api_key)

        # Initialize validator
        validator = FragmentValidator(s3_client, nocodb_client, gsid_client)

        # Process file
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
            sys.exit(0)

    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
