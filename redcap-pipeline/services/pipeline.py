import logging
from typing import Any, Dict, List

from .center_resolver import CenterResolver
from .data_processor import DataProcessor
from .gsid_client import GSIDClient
from .redcap_client import REDCapClient
from .s3_uploader import S3Uploader

logger = logging.getLogger(__name__)


class REDCapPipeline:
    def __init__(self, project_config: dict):
        """Initialize pipeline for a specific project"""
        self.project_config = project_config
        self.project_key = project_config["key"]
        self.project_name = project_config.get("name", self.project_key)
        self.redcap_client = REDCapClient(project_config)
        self.gsid_client = GSIDClient()
        self.center_resolver = CenterResolver()
        self.data_processor = DataProcessor(project_config)
        self.s3_uploader = S3Uploader()

    def extract_samples(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract sample information from a REDCap record"""
        samples = []

        # This is a simplified version - you may need to customize based on your REDCap structure
        # Look for sample-related fields in the record
        sample_fields = [
            "specimen_id",
            "sample_type",
            "collection_date",
            "storage_location",
        ]

        # Check if this record has sample data
        if record.get("specimen_id"):
            sample = {
                "specimen_id": record.get("specimen_id"),
                "sample_type": record.get("sample_type"),
                "collection_date": record.get("collection_date"),
                "storage_location": record.get("storage_location"),
                "redcap_event": record.get("redcap_event_name"),
                "notes": record.get("sample_notes", ""),
            }
            samples.append(sample)

        return samples

    def run(self, batch_size: int = 50) -> Dict[str, Any]:
        """Execute the full pipeline with batch processing"""
        # Use project-specific batch size if configured
        batch_size = self.project_config.get("batch_size", batch_size)

        logger.info(
            f"Starting REDCap pipeline for {self.project_key} (batch mode, batch_size={batch_size})..."
        )

        offset = 0
        total_success = 0
        total_errors = 0
        error_summary = {}

        try:
            while True:
                records = self.redcap_client.fetch_records_batch(batch_size, offset)

                if not records:
                    logger.info(f"[{self.project_key}] No more records to process")
                    break

                logger.info(
                    f"[{self.project_key}] Processing batch {(offset // batch_size) + 1}: records {offset + 1}-{offset + len(records)}"
                )

                for record in records:
                    record_id = record.get("record_id", "unknown")

                    try:
                        # Resolve center
                        center_name = self.center_resolver.resolve_center(record)
                        if not center_name:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: Could not resolve center"
                            )
                            total_errors += 1
                            error_summary["Center resolution failed"] = (
                                error_summary.get("Center resolution failed", 0) + 1
                            )
                            continue

                        # Get or create GSID
                        gsid_result = self.gsid_client.get_or_create_gsid(
                            first_name=record.get("first_name"),
                            last_name=record.get("last_name"),
                            date_of_birth=record.get("date_of_birth"),
                            center_name=center_name,
                        )

                        if not gsid_result or not gsid_result.get("gsid"):
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: GSID creation failed"
                            )
                            total_errors += 1
                            error_summary["GSID creation failed"] = (
                                error_summary.get("GSID creation failed", 0) + 1
                            )
                            continue

                        gsid = gsid_result["gsid"]
                        center_id = gsid_result.get("center_id")

                        # Extract samples
                        samples = self.extract_samples(record)

                        # Process record (insert samples)
                        if not self.data_processor.process_record(
                            record, gsid, samples
                        ):
                            total_errors += 1
                            error_summary["Record processing failed"] = (
                                error_summary.get("Record processing failed", 0) + 1
                            )
                            continue

                        # Create fragment
                        fragment = self.data_processor.create_fragment(gsid, record)
                        if not fragment:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: Fragment creation failed"
                            )
                            total_errors += 1
                            error_summary["Fragment creation failed"] = (
                                error_summary.get("Fragment creation failed", 0) + 1
                            )
                            continue

                        # Upload to S3
                        try:
                            self.s3_uploader.upload_fragment(
                                batch_id=gsid, data=fragment, center_id=center_id
                            )
                            total_success += 1
                            logger.debug(
                                f"[{self.project_key}] Record {record_id} processed successfully (GSID: {gsid})"
                            )

                        except Exception as e:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: S3 upload failed: {e}"
                            )
                            total_errors += 1
                            error_type = f"S3 upload failed: {type(e).__name__}"
                            error_summary[error_type] = (
                                error_summary.get(error_type, 0) + 1
                            )

                    except Exception as e:
                        logger.error(
                            f"[{self.project_key}] Record {record_id}: Processing failed: {e}",
                            exc_info=True,
                        )
                        total_errors += 1
                        error_summary["Processing exception"] = (
                            error_summary.get("Processing exception", 0) + 1
                        )

                offset += batch_size

            # Log error summary if there were errors
            if error_summary:
                logger.warning(f"[{self.project_key}] Error summary:")
                for error_type, count in sorted(
                    error_summary.items(), key=lambda x: x[1], reverse=True
                ):
                    logger.warning(f"  - {error_type}: {count} occurrences")

            logger.info(
                f"[{self.project_key}] Pipeline complete: {total_success} success, {total_errors} errors"
            )

            return {
                "project_key": self.project_key,
                "project_name": self.project_name,
                "total_success": total_success,
                "total_errors": total_errors,
                "error_summary": error_summary,
            }

        except Exception as e:
            logger.error(f"[{self.project_key}] Pipeline failed: {e}", exc_info=True)
            return {
                "project_key": self.project_key,
                "project_name": self.project_name,
                "total_success": total_success,
                "total_errors": total_errors + 1,
                "error_summary": error_summary,
                "fatal_error": str(e),
            }
