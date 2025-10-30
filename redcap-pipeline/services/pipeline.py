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

    def extract_center_name(self, record: Dict[str, Any]) -> str:
        """Extract center name from REDCap record"""
        # Try common REDCap fields for center/site information
        center_fields = [
            "redcap_data_access_group",
            "data_access_group",
            "center",
            "site",
            "center_name",
            "site_name",
        ]

        for field in center_fields:
            if field in record and record[field]:
                return str(record[field])

        # Fall back to project's default center if configured
        default_center = self.project_config.get("default_center")
        if default_center:
            return default_center

        return "Unknown"

    def extract_local_subject_id(self, record: Dict[str, Any]) -> str:
        """Extract local subject ID from REDCap record"""
        # Try common fields for subject ID
        subject_id_fields = [
            "subject_id",
            "patient_id",
            "participant_id",
            "study_id",
            "record_id",
        ]

        for field in subject_id_fields:
            if field in record and record[field]:
                return str(record[field])

        # Fallback to record_id
        return str(record.get("record_id", "unknown"))

    def extract_samples(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract sample information from a REDCap record"""
        samples = []

        # This is a simplified version - you may need to customize based on your REDCap structure
        # Look for sample-related fields in the record

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
            f"[{self.project_key}] Starting REDCap pipeline (batch_size={batch_size})..."
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
                        # Extract and resolve center
                        center_name_raw = self.extract_center_name(record)
                        center_id = self.center_resolver.get_or_create_center(
                            center_name_raw
                        )

                        if not center_id:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: Could not resolve center '{center_name_raw}'"
                            )
                            total_errors += 1
                            error_summary["Center resolution failed"] = (
                                error_summary.get("Center resolution failed", 0) + 1
                            )
                            continue

                        # Extract local subject ID
                        local_subject_id = self.extract_local_subject_id(record)

                        # Register subject and get GSID
                        try:
                            gsid_result = self.gsid_client.register_subject(
                                center_id=center_id,
                                local_subject_id=local_subject_id,
                                identifier_type="primary",
                                registration_year=None,  # Could extract from record if available
                                control=False,
                                created_by=f"redcap_pipeline_{self.project_key}",
                            )

                            gsid = gsid_result.get("gsid")
                            if not gsid:
                                logger.warning(
                                    f"[{self.project_key}] Record {record_id}: No GSID returned"
                                )
                                total_errors += 1
                                error_summary["GSID creation failed"] = (
                                    error_summary.get("GSID creation failed", 0) + 1
                                )
                                continue

                        except Exception as e:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: GSID registration failed: {e}"
                            )
                            total_errors += 1
                            error_summary["GSID registration failed"] = (
                                error_summary.get("GSID registration failed", 0) + 1
                            )
                            continue

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
