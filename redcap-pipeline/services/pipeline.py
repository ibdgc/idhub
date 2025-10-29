import logging
from typing import Dict, List

from core.config import ProjectConfig

from services.center_resolver import CenterResolver
from services.data_processor import DataProcessor
from services.gsid_client import GSIDClient
from services.redcap_client import REDCapClient
from services.s3_uploader import S3Uploader

logger = logging.getLogger(__name__)


class REDCapPipeline:
    def __init__(self, project_config: ProjectConfig):
        self.project_config = project_config
        self.redcap_client = REDCapClient(project_config)
        self.gsid_client = GSIDClient()
        self.center_resolver = CenterResolver()
        self.data_processor = DataProcessor(project_config)
        self.s3_uploader = S3Uploader()
        # Load field mappings to know which fields to extract
        self.field_mappings = project_config.load_field_mappings()

        # Cache all records on first fetch to avoid repeated API calls
        self._all_records = None
        self._records_fetched = False

    def run(self, batch_size: int = None):
        """Execute the full pipeline with batch processing"""
        if batch_size is None:
            batch_size = self.project_config.batch_size

        logger.info(
            f"Starting REDCap pipeline for project '{self.project_config.project_key}' "
            f"({self.project_config.project_name}) - batch size: {batch_size}"
        )

        # Fetch all records once (REDCap API doesn't support true pagination)
        if not self._records_fetched:
            logger.info("Fetching all records from REDCap (this may take a while)...")
            try:
                self._all_records = self._fetch_all_records()
                self._records_fetched = True
                logger.info(
                    f"✓ Fetched {len(self._all_records)} total records from REDCap"
                )
            except Exception as e:
                logger.error(f"Failed to fetch records from REDCap: {e}")
                raise

        total_records = len(self._all_records)
        offset = 0
        total_success = 0
        total_errors = 0
        error_summary = {}

        try:
            while offset < total_records:
                # Get batch from cached records
                batch_end = min(offset + batch_size, total_records)
                records = self._all_records[offset:batch_end]

                if not records:
                    break

                logger.info(
                    f"Processing batch {offset // batch_size + 1}: "
                    f"records {offset + 1}-{batch_end} of {total_records}"
                )

                for record in records:
                    result = self._process_single_record(record)
                    if result["status"] == "success":
                        total_success += 1
                    else:
                        total_errors += 1
                        # Track error types
                        error_type = result.get("error", "Unknown error")
                        error_summary[error_type] = error_summary.get(error_type, 0) + 1

                offset = batch_end

                # Log progress every 10 batches
                if (offset // batch_size) % 10 == 0:
                    logger.info(
                        f"Progress: {offset}/{total_records} records processed "
                        f"({total_success} success, {total_errors} errors)"
                    )

            # Log error summary
            if error_summary:
                logger.warning(f"Error summary for {self.project_config.project_key}:")
                for error_type, count in sorted(
                    error_summary.items(), key=lambda x: x[1], reverse=True
                ):
                    logger.warning(f"  - {error_type}: {count} occurrences")

            logger.info(
                f"Pipeline complete for {self.project_config.project_key}: "
                f"{total_success} success, {total_errors} errors"
            )

            return {
                "project_key": self.project_config.project_key,
                "project_name": self.project_config.project_name,
                "total_success": total_success,
                "total_errors": total_errors,
                "error_summary": error_summary,
            }

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise

    def _fetch_all_records(self) -> List[Dict]:
        """Fetch all records from REDCap in one call"""
        # Use fetch_records_batch with offset=0 and large batch_size
        # This will return all records
        return self.redcap_client.fetch_records_batch(
            batch_size=999999,  # Large number to get all records
            offset=0,
            timeout=180,  # 3 minutes for large datasets
        )

    def _process_single_record(self, record: Dict) -> Dict:
        """Process a single REDCap record"""
        record_id = record.get("record_id", "unknown")
        try:
            # Extract subject identifiers
            subject_data = self._extract_subject_data(record)

            # Validate required fields
            if not subject_data.get("center_id"):
                logger.debug(
                    f"Record {record_id}: Missing center_id "
                    f"(center_name: {subject_data.get('center_name', 'N/A')})"
                )
                return {
                    "status": "error",
                    "error": "Missing center_id",
                    "record_id": record_id,
                }

            if not subject_data.get("local_subject_id"):
                logger.debug(f"Record {record_id}: Missing local_subject_id")
                return {
                    "status": "error",
                    "error": "Missing local_subject_id",
                    "record_id": record_id,
                }

            # Register/resolve GSID
            try:
                gsid_result = self.gsid_client.register_subject(
                    center_id=subject_data["center_id"],
                    local_subject_id=subject_data["local_subject_id"],
                    identifier_type="primary",
                    created_by=f"redcap_{self.project_config.project_key}",
                )
                gsid = gsid_result["gsid"]
                logger.debug(f"Record {record_id}: Registered GSID {gsid}")
            except Exception as e:
                logger.error(f"Record {record_id}: GSID registration failed: {e}")
                return {
                    "status": "error",
                    "error": f"GSID registration failed: {str(e)}",
                    "record_id": record_id,
                }

            # Extract samples
            samples = self._extract_samples(record, gsid)
            logger.debug(f"Record {record_id}: Extracted {len(samples)} samples")

            # Process record (insert samples into DB)
            try:
                success = self.data_processor.process_record(record, gsid, samples)
                if not success:
                    return {
                        "status": "error",
                        "error": "Data processor returned False",
                        "record_id": record_id,
                    }
            except Exception as e:
                logger.error(f"Record {record_id}: Data processing failed: {e}")
                return {
                    "status": "error",
                    "error": f"Data processing failed: {str(e)}",
                    "record_id": record_id,
                }

            # Create and upload fragment to S3
            try:
                fragment = self.data_processor.create_fragment(gsid, record)
                if fragment:
                    self.s3_uploader.upload_fragment(gsid, fragment)
                    logger.debug(
                        f"Record {record_id}: Uploaded fragment for GSID {gsid}"
                    )
            except Exception as e:
                logger.warning(f"Record {record_id}: S3 upload failed: {e}")
                # Don't fail the whole record if S3 upload fails

            return {"status": "success", "gsid": gsid, "record_id": record_id}

        except Exception as e:
            logger.error(f"Record {record_id}: Unexpected error: {e}", exc_info=True)
            return {
                "status": "error",
                "error": f"Unexpected error: {str(e)}",
                "record_id": record_id,
            }

    def _extract_subject_data(self, record: Dict) -> Dict:
        """Extract subject identification data from record"""
        # Get demographics mapping
        demographics = self.field_mappings.get("demographics", {})

        # Extract center name - try multiple possible field names
        center_name = None
        center_field = demographics.get("center_name")

        if center_field and center_field in record:
            center_name = record.get(center_field)
        else:
            # Fallback: try common REDCap field names
            for field in ["redcap_data_access_group", "center_name", "center", "site"]:
                if field in record and record[field]:
                    center_name = record[field]
                    logger.debug(f"Found center name in field '{field}': {center_name}")
                    break

        # Use get_or_create_center to handle fuzzy matching and creation
        center_id = None
        if center_name:
            try:
                center_id = self.center_resolver.get_or_create_center(center_name)
            except Exception as e:
                logger.warning(f"Failed to resolve center '{center_name}': {e}")

        # Extract subject identifiers - try multiple possible fields
        local_subject_id = None
        subject_id_field = demographics.get("local_subject_id")

        if subject_id_field and subject_id_field in record:
            local_subject_id = record.get(subject_id_field)
        else:
            # Fallback: try common field names
            for field in ["local_id", "consortium_id", "subject_id", "record_id"]:
                if field in record and record[field]:
                    local_subject_id = record[field]
                    logger.debug(
                        f"Found subject ID in field '{field}': {local_subject_id}"
                    )
                    break

        # Build subject data
        subject_data = {
            "local_subject_id": local_subject_id,
            "center_id": center_id,
            "center_name": center_name,  # Include for debugging
        }

        return subject_data

    def _extract_samples(self, record: Dict, gsid: str) -> List[Dict]:
        """Extract sample data from record"""
        samples = []

        # Get specimen mapping
        specimen_fields = self.field_mappings.get("specimen", {})
        if not specimen_fields:
            logger.debug(
                f"No specimen field mappings defined for {self.project_config.project_key}"
            )
            return samples

        # Extract specimen ID
        specimen_id_field = specimen_fields.get("specimen_id", "specimen_id")
        specimen_id = record.get(specimen_id_field)

        if not specimen_id:
            logger.debug(f"No specimen_id found in record {record.get('record_id')}")
            return samples

        # Build sample record
        sample = {
            "specimen_id": specimen_id,
            "global_subject_id": gsid,
        }

        # Add optional specimen fields
        optional_specimen_fields = {
            "sample_type": "sample_type",
            "collection_date": "collection_date",
            "storage_location": "storage_location",
            "notes": "notes",
        }

        for key, default_field in optional_specimen_fields.items():
            field_name = specimen_fields.get(key, default_field)
            if field_name in record:
                value = record.get(field_name)
                if value:  # Only include non-empty values
                    sample[key] = value

        # Add REDCap event name if present
        if "redcap_event_name" in record:
            sample["redcap_event"] = record["redcap_event_name"]

        samples.append(sample)

        return samples
