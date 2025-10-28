# table-loader/main.py
import argparse
import logging
import sys
from pathlib import Path

# Make dotenv optional (not needed in CI/CD environments)
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # dotenv not available, assume env vars are already set

from core.config import settings
from services.loader import TableLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/loader.log"),
    ],
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Load validated fragments into database"
    )
    parser.add_argument("--batch-id", required=True, help="Batch ID to load")
    parser.add_argument(
        "--approve",
        action="store_true",
        help="Approve and execute load (default is dry-run)",
    )
    args = parser.parse_args()

    dry_run = not args.approve

    # Create logs directory if it doesn't exist
    Path("logs").mkdir(exist_ok=True)

    try:
        if dry_run:
            logger.info("=" * 60)
            logger.info("DRY RUN MODE - No changes will be made")
            logger.info("=" * 60)
        else:
            logger.info("=" * 60)
            logger.info("LIVE MODE - Changes will be committed to database")
            logger.info("=" * 60)

        logger.info(f"Batch ID: {args.batch_id}")
        logger.info(f"S3 Bucket: {settings.S3_BUCKET}")
        logger.info(
            f"Database: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        )
        logger.info("")

        # Initialize loader
        loader = TableLoader()

        # Execute load
        if dry_run:
            results = loader.preview_load(args.batch_id)
            logger.info("\n" + "=" * 60)
            logger.info("DRY RUN COMPLETE - Run with --approve to execute")
            logger.info("=" * 60)
        else:
            results = loader.execute_load(args.batch_id)
            logger.info("\n" + "=" * 60)
            logger.info("✓ LOAD COMPLETE")
            logger.info("=" * 60)

    except Exception as e:
        logger.error(f"✗ Load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
