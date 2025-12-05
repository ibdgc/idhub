import argparse
import logging
import os
import sys

import boto3
from dotenv import load_dotenv

from core.config import settings
from services import (
    CenterResolver,
    FragmentValidator,
    GSIDClient,
    NocoDBClient,
    S3Client,
    SubjectIDResolver,
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
        if credentials:
            return {
                "aws_access_key_id": credentials.access_key,
                "aws_secret_access_key": credentials.secret_key,
                "aws_session_token": credentials.token,
            }
        return None
    except Exception as e:
        logger.warning(f"Could not get AWS credentials from session: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Validate and stage data fragments for IDhub"
    )
    parser.add_argument("--input-file", required=True, help="Path to input CSV file")
    parser.add_argument(
        "--table-name", required=True, help="Target table name (e.g., blood, dna, lcl)"
    )
    parser.add_argument(
        "--mapping-config",
        required=True,
        help="Path to field mapping JSON config file",
    )
    parser.add_argument(
        "--source",
        default="manual_upload",
        help="Source identifier (default: manual_upload)",
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Auto-approve validation (skip manual review)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Batch size for parallel GSID resolution (default: 5)",
    )
    parser.add_argument(
        "--env",
        choices=["qa", "production"],
        default="qa",
        help="Environment (qa or production)",
    )

    args = parser.parse_args()

    try:
        # Get environment config
        env_config = ENV_CONFIG[args.env]
        logger.info(f"Running in {args.env.upper()} environment")

        # Load mapping configuration
        logger.info(f"Loading mapping config from: {args.mapping_config}")
        mapping_config = settings.load_mapping_config(args.mapping_config)

        # Get required environment variables
        nocodb_token = os.getenv("NOCODB_TOKEN")
        gsid_api_key = os.getenv("GSID_API_KEY")

        if not nocodb_token:
            raise ValueError("NOCODB_TOKEN environment variable not set")
        if not gsid_api_key:
            raise ValueError("GSID_API_KEY environment variable not set")

        # Initialize clients
        logger.info("Initializing service clients...")
        s3_client = S3Client(bucket=env_config["S3_BUCKET"])
        nocodb_client = NocoDBClient(url=env_config["NOCODB_URL"], token=nocodb_token)
        gsid_client = GSIDClient(
            service_url=env_config["GSID_SERVICE_URL"], api_key=gsid_api_key
        )
        
        # Initialize resolvers
        center_resolver = CenterResolver(nocodb_client)
        subject_id_resolver = SubjectIDResolver(gsid_client, center_resolver)

        # Initialize validator
        validator = FragmentValidator(
            s3_client=s3_client,
            nocodb_client=nocodb_client,
            subject_id_resolver=subject_id_resolver,
        )

        # Process file
        logger.info(f"{'=' * 60}")
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
            batch_size=args.batch_size,
        )

        if report["status"] == "FAILED":
            logger.error("✗ Validation failed")
            sys.exit(1)
        else:
            logger.info("✓ Validation successful")

            # Print change summary if available
            if report.get("change_analysis", {}).get("enabled"):
                logger.info("\n" + "=" * 60)
                logger.info("CHANGE SUMMARY")
                logger.info("=" * 60)
                summary = report["change_analysis"]["summary"]
                logger.info(f"Total incoming records: {summary['total_incoming']}")
                logger.info(f"  New records:          {summary['new']}")
                logger.info(f"  Updated records:      {summary['updated']}")
                logger.info(f"  Unchanged records:    {summary['unchanged']}")
                logger.info(f"  Orphaned records:     {summary['orphaned']}")

                if report["change_analysis"].get("sample_updates"):
                    logger.info("\nSample Updates:")
                    for i, update in enumerate(
                        report["change_analysis"]["sample_updates"][:5], 1
                    ):
                        key_str = ", ".join(
                            f"{k}={v}" for k, v in update["natural_key"].items()
                        )
                        logger.info(f"  {i}. {key_str}")
                        logger.info(
                            f"     Fields changed: {', '.join(update['fields_changed'])}"
                        )

            logger.info(f"{'=' * 60}")
            sys.exit(0)

    except ConnectionError as e:
        logger.error(f"✗ Connection failed: {e}")
        logger.error("\nTroubleshooting:")
        logger.error("1. Verify network connectivity to the services")
        logger.error("2. Check that API tokens are valid")
        logger.error(f"3. Confirm service URLs are accessible:")
        logger.error(f"   - GSID Service: {env_config['GSID_SERVICE_URL']}")
        logger.error(f"   - NocoDB: {env_config['NOCODB_URL']}")
        logger.error(f"   - S3 Bucket: {env_config['S3_BUCKET']}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"✗ Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
