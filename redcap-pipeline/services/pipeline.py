import logging
from typing import Dict, Any
from .redcap_client import REDCapClient
from .gsid_client import GSIDClient
from .center_resolver import CenterResolver
from .data_processor import DataProcessor
from .s3_uploader import S3Uploader

logger = logging.getLogger(__name__)


class REDCapPipeline:
    def __init__(self, project_config: dict):
        """Initialize pipeline for a specific project"""
        self.project_config = project_config
        self.project_key = project_config['key']
        self.project_name = project_config.get('name', self.project_key)
        self.redcap_client = REDCapClient(project_config)
        self.gsid_client = GSIDClient()
        self.center_resolver = CenterResolver()
        self.data_processor = DataProcessor(self.gsid_client, self.center_resolver)
        self.s3_uploader = S3Uploader()

    def run(self, batch_size: int = 50) -> Dict[str, Any]:
        """Execute the full pipeline with batch processing"""
        # Use project-specific batch size if configured
        batch_size = self.project_config.get('batch_size', batch_size)

        logger.info(f"Starting REDCap pipeline for {self.project_key} (batch mode, batch_size={batch_size})...")

        offset = 0
        total_success = 0
        total_errors = 0
        error_summary = {}

        try:
            while True:
                records = self.redcap_client.fetch_records_batch(batch_size, offset)

                if not records:
                    logger.info("No more records to process")
                    break

                logger.info(f"Processing batch {(offset // batch_size) + 1}: records {offset + 1}-{offset + len(records)} of total")

                for record in records:
                    result = self.data_processor.process_record(record)
                    if result["status"] == "success":
                        total_success += 1
                        try:
                            # Upload fragment to S3 with center_id
                            self.s3_uploader.upload_fragment(
                                batch_id=result["batch_id"],
                                data=result["fragment_data"],
                                center_id=result["center_id"]  # Pass center_id
                            )
                        except Exception as e:
                            logger.warning(f"Record {record.get('record_id', 'unknown')}: S3 upload failed: {e}")
                            total_errors += 1
                            error_type = f"S3 upload failed: {type(e).__name__}"
                            error_summary[error_type] = error_summary.get(error_type, 0) + 1
                    else:
                        total_errors += 1
                        error_reason = result.get("error", "Unknown error")
                        error_summary[error_reason] = error_summary.get(error_reason, 0) + 1

                offset += batch_size

            # Log error summary if there were errors
            if error_summary:
                logger.warning(f"Error summary for {self.project_key}:")
                for error_type, count in sorted(error_summary.items(), key=lambda x: x[1], reverse=True):
                    logger.warning(f"  - {error_type}: {count} occurrences")

            logger.info(f"Pipeline complete for {self.project_key}: {total_success} success, {total_errors} errors")

            return {
                "project_key": self.project_key,
                "project_name": self.project_name,
                "total_success": total_success,
                "total_errors": total_errors,
                "error_summary": error_summary
            }

        except Exception as e:
            logger.error(f"Pipeline failed for {self.project_key}: {e}", exc_info=True)
            return {
                "project_key": self.project_key,
                "project_name": self.project_name,
                "total_success": total_success,
                "total_errors": total_errors + 1,
                "error_summary": error_summary,
                "fatal_error": str(e)
            }
