import argparse
import json
import logging
import os
import sys

from dotenv import load_dotenv

from services import (
    DatabaseClient,
    LoaderService,
    S3Client,
)

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/loader.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Load validated fragments to PostgreSQL"
    )
    parser.add_argument("--batch-id", required=True, help="Batch ID to load")
    parser.add_argument(
        "--approve", action="store_true", help="Execute load (default is dry-run)"
    )
    args = parser.parse_args()

    # Get environment variables
    s3_bucket = os.getenv("S3_BUCKET", "idhub-curated-fragments")
    db_config = {
        "host": os.getenv("DB_HOST", "idhub_db"),
        "database": os.getenv("DB_NAME", "idhub"),
        "user": os.getenv("DB_USER", "idhub_user"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", "5432")),
    }

    # Validate required environment variables
    if not db_config["password"]:
        logger.error("Missing required environment variable: DB_PASSWORD")
        sys.exit(1)

    try:
        # Initialize clients
        s3_client = S3Client(s3_bucket)
        db_client = DatabaseClient(db_config)

        # Initialize loader service
        loader = LoaderService(s3_client, db_client)

        # Execute load
        dry_run = not args.approve
        loader.load_batch(args.batch_id, dry_run=dry_run)

        logger.info("✓ Load completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"✗ Load failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        # Clean up database pool
        if "db_client" in locals():
            db_client.close()


if __name__ == "__main__":
    main()
