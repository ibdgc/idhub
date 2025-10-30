import logging
from typing import Dict, List

from services.center_resolver import CenterResolver
from services.data_processor import DataProcessor
from services.gsid_client import GSIDClient
from services.redcap_client import REDCapClient
from services.s3_uploader import S3Uploader

logger = logging.getLogger(__name__)


class REDCapPipeline:
    def __init__(self, project_config: dict):
        """Initialize pipeline for a specific project"""
        self.project_config = project_config
        self.project_key = project_config.get("key")
        self.project_name = project_config.get("name")

        self.redcap_client = REDCapClient(project_config)
        self.gsid_client = GSIDClient()
        self.center_resolver = CenterResolver()
        self.data_processor = DataProcessor(project_config)
        self.s3_uploader = S3Uploader()

    def run(self, batch_size: int = 50):
        """Execute the full pipeline with batch processing"""
        logger.info(f"[{self.project_key}] Starting REDCap pipeline...")

        offset = 0
        total_success = 0
        total_errors = 0

        try:
            while True:
                records = self.redcap_client.fetch_records_batch(batch_size, offset)

                if not records:
                    logger.info(f"[{self.project_key}] No more records to process")
                    break

                logger.info(
                    f"[{self.project_key}] Processing {len(records)} records..."
                )

                for record in records:
                    result = self.data_processor.process_record(record)
                    if result["status"] == "success":
                        total_success += 1
                    else:
                        total_errors += 1

                offset += batch_size

            logger.info(
                f"[{self.project_key}] Pipeline complete: "
                f"{total_success} success, {total_errors} errors"
            )

            return {
                "project": self.project_name,
                "total_success": total_success,
                "total_errors": total_errors,
            }

        except Exception as e:
            logger.error(f"[{self.project_key}] Pipeline failed: {e}", exc_info=True)
            raise
