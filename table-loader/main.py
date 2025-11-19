# table-loader/main.py
import argparse
import logging
import os
import sys

from core.config import settings
from services.loader import TableLoader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Load validated fragments into database"
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Batch ID to load (e.g., batch_20251119_130611)",
    )
    parser.add_argument(
        "--approve",
        action="store_true",
        help="Approve and load the batch (required for non-auto-approved batches)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - analyze changes without committing to database",
    )
    parser.add_argument(
        "--s3-bucket",
        default=settings.S3_BUCKET,  # Use settings default
        help=f"S3 bucket name (default: {settings.S3_BUCKET} based on ENVIRONMENT={settings.ENVIRONMENT})",
    )
    parser.add_argument(
        "--environment",
        choices=["qa", "production"],
        default=settings.ENVIRONMENT,
        help="Environment (qa or production)",
    )

    args = parser.parse_args()

    # Update environment if specified
    if args.environment:
        os.environ["ENVIRONMENT"] = args.environment

    # Display configuration
    logger.info("=" * 60)
    logger.info(f"ENVIRONMENT: {args.environment.upper()}")
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be committed")
    else:
        logger.info("LIVE MODE - Changes will be committed to database")
    logger.info("=" * 60)
    logger.info(f"Batch ID: {args.batch_id}")
    logger.info(f"S3 Bucket: {args.s3_bucket}")
    logger.info(
        f"Database: {os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'idhub')}"
    )
    logger.info("")

    try:
        # Initialize loader
        loader = TableLoader(s3_bucket=args.s3_bucket)

        # Load batch
        result = loader.load_batch(
            batch_id=args.batch_id, approve=args.approve, dry_run=args.dry_run
        )

        # Display results
        logger.info("=" * 60)
        logger.info("LOAD RESULTS")
        logger.info("=" * 60)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Batch ID: {result['batch_id']}")

        if result["status"] == "SUCCESS":
            logger.info(f"Table: {result['table_name']}")
            logger.info(f"Records loaded: {result['records_loaded']}")
            logger.info(f"  - Inserted: {result['inserted']}")
            logger.info(f"  - Updated: {result['updated']}")

            if result.get("local_ids_loaded", 0) > 0:
                logger.info(f"Local IDs loaded: {result['local_ids_loaded']}")

            logger.info("=" * 60)
            logger.info("✓ Load completed successfully")
            sys.exit(0)
        elif result["status"] == "DRY_RUN":
            logger.info(f"Table: {result['table_name']}")
            logger.info(f"Would load: {result['would_load']} records")
            logger.info("=" * 60)
            logger.info("✓ Dry run completed successfully")
            sys.exit(0)
        else:
            logger.error(f"Error: {result.get('error', 'Unknown error')}")
            logger.info("=" * 60)
            logger.error("✗ Load failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"✗ Load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
