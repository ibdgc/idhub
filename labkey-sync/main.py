import argparse
import logging
import sys

from dotenv import load_dotenv

from services.sync_service import LabKeySyncService

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Sync specimen data with LabKey")
    parser.add_argument(
        "--dry-run", action="store_true", help="Run without making database changes"
    )
    parser.add_argument(
        "--limit", type=int, help="Limit number of samples to process (for testing)"
    )

    args = parser.parse_args()

    try:
        sync_service = LabKeySyncService()
        results = sync_service.sync(dry_run=args.dry_run, limit=args.limit)

        if results.get("errors", 0) > 0:
            logger.warning(f"Completed with {results['errors']} errors")
            sys.exit(1)
        else:
            logger.info("Sync completed successfully")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Sync failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
