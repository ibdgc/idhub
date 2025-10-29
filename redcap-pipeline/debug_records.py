#!/usr/bin/env python3
import json
import logging

from core.config import settings
from services.redcap_client import REDCapClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Debug script to inspect REDCap records"""
    projects = settings.load_projects_config()

    # Get the gap project
    gap_config = projects.get("gap")
    if not gap_config:
        logger.error("GAP project not found")
        return

    # Fetch a few records
    client = REDCapClient(gap_config)
    records = client.fetch_records_batch(batch_size=2, offset=0)

    logger.info(f"Fetched {len(records)} records")

    # Print first record structure
    if records:
        logger.info("First record keys:")
        logger.info(json.dumps(list(records[0].keys()), indent=2))

        logger.info("\nFirst record sample data:")
        sample_data = {k: v for k, v in list(records[0].items())[:20]}
        logger.info(json.dumps(sample_data, indent=2))

        # Check for center-related fields
        center_fields = [
            k
            for k in records[0].keys()
            if "center" in k.lower() or "group" in k.lower()
        ]
        logger.info(f"\nCenter-related fields: {center_fields}")

        # Load field mappings
        mappings = gap_config.load_field_mappings()
        logger.info("\nField mappings structure:")
        logger.info(json.dumps(mappings, indent=2))


if __name__ == "__main__":
    main()
