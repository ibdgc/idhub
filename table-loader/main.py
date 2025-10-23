# table-loader/main.py
import argparse
import logging

from core.config import settings
from services.loader import TableLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/loader.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Load validated fragments into database"
    )
    parser.add_argument("--batch-id", required=True, help="Batch ID to load")
    parser.add_argument("--table", help="Specific table to load (optional)")
    parser.add_argument(
        "--approve", action="store_true", help="Approve and execute load"
    )
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    args = parser.parse_args()

    try:
        logger.info(f"Starting table loader for batch {args.batch_id}")
        loader = TableLoader()

        if args.dry_run or not args.approve:
            logger.info("Running in preview mode")
            result = loader.preview_load(args.batch_id, args.table)
            logger.info(f"Preview: {result}")
        else:
            logger.info("Executing load")
            result = loader.execute_load(args.batch_id, args.table)
            logger.info(f"Load complete: {result}")

        exit(0)

    except Exception as e:
        logger.error(f"Load error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
