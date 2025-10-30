import logging
from typing import Dict, List

from core.config import settings

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

    def extract_local_ids(self, record: Dict) -> List[str]:
        """Extract all local IDs from record using field mappings"""
        local_ids = []
        mappings = self.data_processor.field_mappings.get("mappings", [])

        # Get local_subject_ids mappings
        local_id_mappings = [
            m for m in mappings if m.get("target_table") == "local_subject_ids"
        ]

        for mapping in local_id_mappings:
            source_field = mapping.get("source_field")
            value = record.get(source_field)
            if value and value not in ["", "NA", "N/A", "null"]:
                local_ids.append(str(value).strip())

        return local_ids

    def run(self, batch_size: int = 50):
        """Execute the full pipeline with batch processing"""
        logger.info(f"[{self.project_key}] Starting REDCap pipeline (batch mode)...")

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
                    try:
                        # Extract center
                        center_name = record.get("redcap_data_access_group", "Unknown")
                        center_id = self.center_resolver.get_or_create_center(
                            center_name
                        )

                        # Extract local IDs from record
                        local_ids = self.extract_local_ids(record)

                        if not local_ids:
                            logger.warning(
                                f"[{self.project_key}] No local IDs found in record "
                                f"{record.get('record_id')}"
                            )
                            total_errors += 1
                            continue

                        # Register with GSID service
                        gsid_result = self.gsid_client.register_subject(
                            {
                                "center_id": center_id,
                                "local_subject_ids": local_ids,
                                "registration_year": None,
                                "control": False,
                                "created_by": "redcap_pipeline",
                            }
                        )

                        gsid = gsid_result["gsid"]

                        # Process record (extract and insert specimens)
                        success = self.data_processor.process_record(record, gsid)

                        if success:
                            total_success += 1

                            # Create and upload fragment
                            fragment = self.data_processor.create_fragment(gsid, record)
                            if fragment:
                                self.s3_uploader.upload_fragment(
                                    fragment, self.project_key
                                )
                        else:
                            total_errors += 1

                    except Exception as e:
                        logger.error(
                            f"[{self.project_key}] Error processing record "
                            f"{record.get('record_id', 'unknown')}: {e}"
                        )
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
