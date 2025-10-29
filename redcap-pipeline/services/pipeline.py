import logging
from typing import Dict, List, Optional

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

    def run(self, batch_size: int = None):
        """Execute the full pipeline with batch processing"""
        if batch_size is None:
            batch_size = self.project_config.batch_size

        logger.info(
            f"Starting REDCap pipeline for project '{self.project_config.project_key}' "
            f"({self.project_config.project_name}) - batch size: {batch_size}"
        )

        offset = 0
        total_success = 0
        total_errors = 0

        try:
            while True:
                records = self.redcap_client.fetch_records_batch(batch_size, offset)

                if not records:
                    logger.info("No more records to process")
                    break

                logger.info(f"Processing {len(records)} records...")

                for record in records:
                    result = self._process_single_record(record)
                    if result["status"] == "success":
                        total_success += 1
                    else:
                        total_errors += 1

                offset += batch_size

            logger.info(
                f"Pipeline complete for {self.project_config.project_key}: "
                f"{total_success} success, {total_errors} errors"
            )

            return {
                "project_key": self.project_config.project_key,
                "project_name": self.project_config.project_name,
                "total_success": total_success,
                "total_errors": total_errors,
            }

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise

    def _process_single_record(self, record: Dict) -> Dict:
        """Process a single REDCap record"""
        try:
            # Extract subject identifiers
            subject_data = self._extract_subject_data(record)

            # Validate required fields
            if not subject_data.get("center_id"):
                return {"status": "error", "error": "Missing center_id"}
            if not subject_data.get("local_subject_id"):
                return {"status": "error", "error": "Missing local_subject_id"}

            # Register/resolve GSID
            gsid_result = self.gsid_client.register_subject(
                center_id=subject_data["center_id"],
                local_subject_id=subject_data["local_subject_id"],
                identifier_type="primary",
                created_by=f"redcap_{self.project_config.project_key}",
            )
            gsid = gsid_result["gsid"]

            # Extract samples
            samples = self._extract_samples(record, gsid)

            # Process record (insert samples into DB)
            success = self.data_processor.process_record(record, gsid, samples)

            if not success:
                return {"status": "error", "error": "Failed to process record"}

            # Create and upload fragment to S3
            fragment = self.data_processor.create_fragment(gsid, record)
            if fragment:
                self.s3_uploader.upload_fragment(gsid, fragment)

            return {"status": "success", "gsid": gsid}

        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)
            return {"status": "error", "error": str(e)}

    def _extract_subject_data(self, record: Dict) -> Dict:
        """Extract subject identification data from record"""
        # Get demographics mapping
        demographics = self.field_mappings.get("demographics", {})

        # Extract center name and resolve to center_id
        center_field = demographics.get("center_name", "center_name")
        center_name = record.get(center_field, "")

        # Use get_or_create_center to handle fuzzy matching and creation
        center_id = None
        if center_name:
            center_id = self.center_resolver.get_or_create_center(center_name)

        # Extract subject identifiers
        subject_id_field = demographics.get("local_subject_id", "subject_id")
        local_subject_id = record.get(subject_id_field, record.get("record_id"))

        # Build subject data
        subject_data = {
            "local_subject_id": local_subject_id,
            "center_id": center_id,
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
