import logging
import time
from datetime import datetime
from typing import Dict, List

import requests

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

    def run(self, batch_size: int = 200):
        """Execute the full pipeline with batch processing"""
        logger.info(f"[{self.project_key}] Starting REDCap pipeline...")

        offset = 0
        total_success = 0
        total_errors = 0
        max_consecutive_failures = 3
        consecutive_failures = 0

        try:
            while True:
                try:
                    records = self.redcap_client.fetch_records_batch(batch_size, offset)
                    consecutive_failures = 0  # Reset on success

                    if not records:
                        logger.info(f"[{self.project_key}] No more records to process")
                        break

                    # Process each record in the batch
                    for record in records:
                        try:
                            self.process_record(record)
                            total_success += 1
                        except Exception as e:
                            total_errors += 1
                            logger.error(
                                f"[{self.project_key}] Error processing record "
                                f"{record.get('record_id', 'unknown')}: {e}"
                            )

                    offset += len(records)
                    logger.info(
                        f"[{self.project_key}] Batch complete. "
                        f"Total: {total_success} success, {total_errors} errors"
                    )

                except (
                    requests.exceptions.RetryError,
                    requests.exceptions.RequestException,
                ) as e:
                    consecutive_failures += 1
                    logger.error(
                        f"[{self.project_key}] Batch fetch failed "
                        f"(attempt {consecutive_failures}/{max_consecutive_failures}): {e}"
                    )

                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(
                            f"[{self.project_key}] Max consecutive failures reached. "
                            f"Successfully processed {total_success} records before failure."
                        )
                        # Return partial success instead of complete failure
                        return {
                            "status": "partial_success",
                            "total_success": total_success,
                            "total_errors": total_errors,
                            "last_offset": offset,
                            "error": str(e),
                        }

                    # Wait before retry with exponential backoff
                    wait_time = 30 * consecutive_failures
                    logger.info(
                        f"[{self.project_key}] Waiting {wait_time}s before retry..."
                    )
                    time.sleep(wait_time)
                    continue

            # Successful completion
            logger.info(
                f"[{self.project_key}] Pipeline complete: "
                f"{total_success} success, {total_errors} errors"
            )

            return {
                "status": "success",
                "total_success": total_success,
                "total_errors": total_errors,
            }

        except Exception as e:
            logger.error(f"[{self.project_key}] Pipeline failed: {e}", exc_info=True)
            return {
                "status": "error",
                "total_success": total_success,
                "total_errors": total_errors,
                "error": str(e),
            }

    def process_record(self, record: Dict):
        """Process a single REDCap record"""
        record_id = record.get("record_id", "unknown")
        logger.debug(f"[{self.project_key}] Processing record: {record_id}")

        # Extract subject IDs from record
        subject_ids = self.data_processor.extract_subject_ids(record)

        if not subject_ids:
            logger.warning(
                f"[{self.project_key}] No valid subject IDs found in record {record_id}"
            )
            return

        # Resolve center
        center_id = self.data_processor.resolve_center(record)

        # Resolve subject IDs to GSID
        resolution = self.data_processor.resolve_subject_ids(
            subject_ids, center_id, record
        )

        gsid = resolution["gsid"]
        logger.info(
            f"[{self.project_key}] Resolved to GSID {gsid} "
            f"(action: {resolution['action']}, "
            f"primary: {resolution.get('primary_id', 'unknown')})"
        )

        # Insert samples into database
        self.data_processor.insert_samples(record, gsid)

        # Create and upload curated fragment
        fragment = self.data_processor.create_curated_fragment(record, gsid, center_id)
        self.s3_uploader.upload_fragment(fragment, self.project_key, gsid)

        logger.debug(f"[{self.project_key}] Record {record_id} processed successfully")
