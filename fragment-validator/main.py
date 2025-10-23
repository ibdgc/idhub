# fragment-validator/main.py
import argparse
import logging

from core.config import settings
from services.validator import FragmentValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/validator.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Validate data fragments")
    parser.add_argument("--batch-id", required=True, help="Batch ID to validate")
    parser.add_argument("--table", required=True, help="Table name to validate")
    args = parser.parse_args()

    try:
        logger.info(
            f"Starting validation for batch {args.batch_id}, table {args.table}"
        )
        validator = FragmentValidator()
        result = validator.validate_batch(args.batch_id, args.table)

        if result.is_valid:
            logger.info("Validation passed")
            exit(0)
        else:
            logger.error(f"Validation failed: {result.errors}")
            exit(1)

    except Exception as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
