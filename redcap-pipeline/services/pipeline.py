import logging
from datetime import datetime
from typing import Optional

from core.config import ProjectConfig, settings
from core.database import close_db_pool

from services.center_resolver import CenterResolver
from services.data_processor import DataProcessor
from services.gsid_client import GSIDClient
from services.redcap_client import REDCapClient
from services.s3_uploader import S3Uploader

logger = logging.getLogger(__name__)


class REDCapPipeline:
    def __init__(self, project_config: Optional[ProjectConfig] = None):
        """
        Initialize pipeline for a specific project

        Args:
            project_config: Project configuration. If None, uses legacy env vars.
        """
        self.project_config = project_config
        self.redcap_client = REDCapClient(project_config)
        self.gsid_client = GSIDClient()
        self.center_resolver = CenterResolver()
        self.data_processor = DataProcessor(
            self.gsid_client, self.center_resolver, project_config
        )
        self.s3_uploader = S3Uploader()

        if project_config:
            self.project_key = project_config.project_key
            self.redcap_project_id = project_config.redcap_project_id
        else:
            self.project_key = "default"
            self.redcap_project_id = settings.REDCAP_PROJECT_ID

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
                    result = self.data_processor.process_record(record)
                    if result["status"] == "success":
                        total_success += 1
                        try:
                            self.s3_uploader.upload_fragment(
                                record,
                                result["gsid"],
                                result.get("center_id", 0),
                                self.project_key,
                                self.redcap_project_id,
                            )
                        except Exception as e:
                            logger.warning(
                                f"[{self.project_key}] Failed to upload fragment for "
                                f"{result['gsid']}: {e}"
                            )
                    else:
                        total_errors += 1

                offset += batch_size

            logger.info(
                f"[{self.project_key}] Pipeline complete: "
                f"{total_success} success, {total_errors} errors"
            )

            return {
                "total_success": total_success,
                "total_errors": total_errors,
                "project_key": self.project_key,
                "redcap_project_id": self.redcap_project_id,
            }

        except Exception as e:
            logger.error(f"[{self.project_key}] Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            close_db_pool()
