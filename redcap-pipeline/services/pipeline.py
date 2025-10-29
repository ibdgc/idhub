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

            # Register/resolve GSID
            gsid_result = self.gsid_client.register_subject(subject_data)
            gsid = gsid_result["gsid"]

            # Extract samples
            samples = self._extract_samples(record)

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
            logger.error(f"Error processing record: {e}")
            return {"status": "error", "error": str(e)}

    def _extract_subject_data(self, record: Dict) -> Dict:
        """Extract subject identification data from record"""
        # This should be implemented based on your field mappings
        # For now, a placeholder
        return {
            "local_subject_id": record.get("subject_id"),
            "center_id": self.center_resolver.resolve_center(
                record.get("center_name", "")
            ),
        }

    def _extract_samples(self, record: Dict) -> List[Dict]:
        """Extract sample data from record"""
        # This should be implemented based on your field mappings
        # For now, a placeholder
        samples = []
        # Add your sample extraction logic here
        return samples
