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
        # REDCap exports data access groups as 'redcap_data_access_group'
        center_name = record.get("redcap_data_access_group")
        if center_name:
            return str(center_name)

        # Fallback to project's default center if configured
        default_center = self.project_config.get("default_center")
        if default_center:
            return default_center

        return "Unknown"

    def extract_local_subject_id(self, record: Dict[str, Any]) -> str:
        """Extract local subject ID from REDCap record based on field mappings"""
        # Look for consortium_id first, then local_id, then record_id
        for field in ["consortium_id", "local_id", "subject_id", "record_id"]:
            if field in record and record[field]:
                return str(record[field])

        return str(record.get("record_id", "unknown"))

    def extract_samples_from_mappings(
        self, record: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract samples based on field mappings configuration"""
        samples = []

        if not self.data_processor.field_mappings:
            logger.warning(f"[{self.project_key}] No field mappings available")
            return samples

        mappings = self.data_processor.field_mappings.get("mappings", [])

        for mapping in mappings:
            # Only process specimen mappings
            if mapping.get("target_table") != "specimen":
                continue

            source_field = mapping.get("source_field")
            sample_type = mapping.get("sample_type")

            if not source_field or not sample_type:
                continue

            # Get the specimen ID from the record
            specimen_id = record.get(source_field)

            # Only add if specimen_id is not empty
            if (
                specimen_id
                and str(specimen_id).strip()
                and specimen_id not in ["", "NA", "N/A"]
            ):
                sample = {
                    "specimen_id": str(specimen_id).strip(),
                    "sample_type": sample_type,
                    "collection_date": record.get("collection_date"),
                    "storage_location": record.get("storage_location"),
                    "redcap_event": record.get("redcap_event_name", ""),
                    "notes": "",
                }
                samples.append(sample)

        return samples

    def extract_control_status(self, record: Dict[str, Any]) -> bool:
        """Extract control status from record"""
        control_value = record.get("control", "0")

        # Check if there are transformations defined
        transformations = self.data_processor.field_mappings.get("transformations", {})
        control_transform = transformations.get("control", {})

        if control_transform.get("type") == "boolean":
            true_values = control_transform.get("true_values", ["1", "yes", "true"])
            return str(control_value).lower() in [v.lower() for v in true_values]

        # Default: treat 1, yes, true as control
        return str(control_value).lower() in ["1", "yes", "true"]

    def extract_registration_year(self, record: Dict[str, Any]) -> int:
        """Extract registration year from record"""
        from datetime import datetime

        reg_date = record.get("registration_date") or record.get("enrollment_date")

        if reg_date:
            try:
                # Try to parse date and extract year
                if isinstance(reg_date, str):
                    # Try common date formats
                    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y"]:
                        try:
                            dt = datetime.strptime(reg_date, fmt)
                            return dt.year
                        except ValueError:
                            continue

                    # Try just extracting first 4 digits if it's a year
                    if len(reg_date) >= 4 and reg_date[:4].isdigit():
                        return int(reg_date[:4])
            except Exception as e:
                logger.debug(f"Could not parse registration date '{reg_date}': {e}")

        return None

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
                    f"[{self.project_key}] Processing batch {(offset // batch_size) + 1}: "
                    f"records {offset + 1}-{offset + len(records)}"
                )

                for record in records:
                    record_id = record.get("record_id", "unknown")

                    try:
                        # 1. Extract and resolve center
                        center_name_raw = self.extract_center_name(record)
                        center_id = self.center_resolver.get_or_create_center(
                            center_name_raw
                        )

                        if not center_id:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: "
                                f"Could not resolve center '{center_name_raw}'"
                            )
                            total_errors += 1
                            error_summary["Center resolution failed"] = (
                                error_summary.get("Center resolution failed", 0) + 1
                            )
                            continue

                        # 2. Extract local subject ID
                        local_subject_id = self.extract_local_subject_id(record)

                        if not local_subject_id or local_subject_id == "unknown":
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: "
                                f"No valid local_subject_id found"
                            )
                            total_errors += 1
                            error_summary["Missing subject ID"] = (
                                error_summary.get("Missing subject ID", 0) + 1
                            )
                            continue

                        # 3. Extract control status and registration year
                        control = self.extract_control_status(record)
                        registration_year = self.extract_registration_year(record)

                        # 4. Register subject and get GSID
                        try:
                            gsid_result = self.gsid_client.register_subject(
                                center_id=center_id,
                                local_subject_id=local_subject_id,
                                identifier_type="primary",
                                registration_year=registration_year,
                                control=control,
                                created_by=f"redcap_pipeline_{self.project_key}",
                            )

                            gsid = gsid_result.get("gsid")
                            if not gsid:
                                logger.warning(
                                    f"[{self.project_key}] Record {record_id}: "
                                    f"No GSID returned"
                                )
                                total_errors += 1
                                error_summary["GSID creation failed"] = (
                                    error_summary.get("GSID creation failed", 0) + 1
                                )
                                continue

                        except Exception as e:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: "
                                f"GSID registration failed: {e}"
                            )
                            total_errors += 1
                            error_summary["GSID registration failed"] = (
                                error_summary.get("GSID registration failed", 0) + 1
                            )
                            continue

                        # 5. Extract samples from field mappings
                        samples = self.extract_samples_from_mappings(record)

                        logger.debug(
                            f"[{self.project_key}] Record {record_id}: "
                            f"Extracted {len(samples)} samples"
                        )

                        # 6. Process record (insert samples into database)
                        if not self.data_processor.process_record(
                            record, gsid, samples
                        ):
                            total_errors += 1
                            error_summary["Record processing failed"] = (
                                error_summary.get("Record processing failed", 0) + 1
                            )
                            continue

                        # 7. Create curated fragment
                        fragment = self.data_processor.create_fragment(gsid, record)
                        if not fragment:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: "
                                f"Fragment creation failed"
                            )
                            total_errors += 1
                            error_summary["Fragment creation failed"] = (
                                error_summary.get("Fragment creation failed", 0) + 1
                            )
                            continue

                        # 8. Upload fragment to S3
                        try:
                            self.s3_uploader.upload_fragment(
                                batch_id=gsid, data=fragment, center_id=center_id
                            )
                            total_success += 1
                            logger.debug(
                                f"[{self.project_key}] ✓ Record {record_id} processed "
                                f"successfully (GSID: {gsid}, {len(samples)} samples)"
                            )

                        except Exception as e:
                            logger.warning(
                                f"[{self.project_key}] Record {record_id}: "
                                f"S3 upload failed: {e}"
                            )
                            total_errors += 1
                            error_type = f"S3 upload failed: {type(e).__name__}"
                            error_summary[error_type] = (
                                error_summary.get(error_type, 0) + 1
                            )

                    except Exception as e:
                        logger.error(
                            f"[{self.project_key}] Record {record_id}: "
                            f"Processing failed: {e}",
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
                f"[{self.project_key}] Pipeline complete: "
                f"{total_success} success, {total_errors} errors"
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
