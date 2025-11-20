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
    parser.add_argument("--batch-id", required=True, help="Batch ID to load")
    parser.add_argument(
        "--environment",
        choices=["qa", "production"],
        default="qa",
        help="Target environment",
    )
    parser.add_argument(
        "--s3-bucket",
        help="S3 bucket name (overrides environment default)",
    )
    parser.add_argument(
        "--approve",
        action="store_true",
        help="Approve and commit changes (default is dry-run)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - don't commit changes",
    )

    args = parser.parse_args()

    # Determine dry_run mode
    # If --approve is passed, dry_run = False
    # If --dry-run is passed, dry_run = True
    # Default is dry_run = True (safe default)
    if args.approve and args.dry_run:
        logger.error("Cannot specify both --approve and --dry-run")
        sys.exit(1)

    dry_run = not args.approve  # If approve=True, then dry_run=False

    # Determine S3 bucket
    if args.s3_bucket:
        s3_bucket = args.s3_bucket
    elif args.environment == "qa":
        s3_bucket = "idhub-curated-fragments-qa"
    else:
        s3_bucket = "idhub-curated-fragments"

    # Display configuration
    logger.info("=" * 60)
    logger.info(f"ENVIRONMENT: {args.environment.upper()}")
    if dry_run:
        logger.info("DRY RUN MODE - No changes will be committed")
    else:
        logger.info("LIVE MODE - Changes will be committed to database")
    logger.info("=" * 60)
    logger.info(f"Batch ID: {args.batch_id}")
    logger.info(f"S3 Bucket: {s3_bucket}")
    logger.info(
        f"Database: {os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'idhub')}"
    )
    logger.info("")

    try:
        # Initialize loader
        loader = TableLoader(s3_bucket=s3_bucket)

        # Load batch
        result = loader.load_batch(
            batch_id=args.batch_id,
            dry_run=dry_run,  # ✅ Changed from approve to dry_run
        )

        # Display results
        logger.info("=" * 60)
        logger.info("LOAD RESULTS")
        logger.info("=" * 60)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Batch ID: {result['batch_id']}")
        logger.info(f"Table: {result['table_name']}")
        logger.info(f"Records loaded: {result['records_loaded']}")
        logger.info(f"  - Inserted: {result['inserted']}")
        logger.info(f"  - Updated: {result['updated']}")
        if result.get("local_ids_loaded"):
            logger.info(f"Local subject IDs loaded: {result['local_ids_loaded']}")
        logger.info("=" * 60)

        if dry_run:
            logger.info("✓ Dry run completed successfully (no changes committed)")
        else:
            logger.info("✓ Load completed successfully")

        sys.exit(0)

    except FileNotFoundError as e:
        logger.error(f"✗ File not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"✗ Validation error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"✗ Load failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
