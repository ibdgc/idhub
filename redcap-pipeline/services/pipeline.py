import logging
from datetime import datetime
from typing import List

from services.center_resolver import CenterResolver
from services.data_processor import DataProcessor
from services.gsid_client import GSIDClient
from services.redcap_client import REDCapClient
from services.s3_uploader import S3Uploader

logger = logging.getLogger(__name__)


class REDCapPipeline:
    def __init__(self):
        self.redcap_client = REDCapClient()
        self.gsid_client = GSIDClient()
        self.center_resolver = CenterResolver()
        self.data_processor = DataProcessor(self.gsid_client, self.center_resolver)
        self.s3_uploader = S3Uploader()

    def run(self, batch_size: int = 50):
        """Execute the full pipeline with batch processing"""
        logger.info("Starting REDCap pipeline (batch mode)...")
        
        offset = 0
        total_success = 0
        total_errors = 0
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        all_results = []

        try:
            while True:
                logger.info(f"Fetching batch: offset={offset}, limit={batch_size}")
                records = self.redcap_client.fetch_records_batch(batch_size, offset)
                
                if not records:
                    logger.info("No more records to process")
                    break

                logger.info(f"Processing {len(records)} records...")
                
                for record in records:
                    result = self.data_processor.process_record(record)
                    all_results.append(result)
                    
                    if result["status"] == "success":
                        total_success += 1
                        # Upload fragment for successful records
                        try:
                            gsid = result["gsid"]
                            center_name = record.get("redcap_data_access_group", "Unknown")
                            center_id = self.center_resolver.get_or_create_center(center_name)
                            self.s3_uploader.upload_fragment(record, gsid, center_id)
                        except Exception as e:
                            logger.error(f"Failed to upload fragment for {gsid}: {e}")
                    else:
                        total_errors += 1

                offset += batch_size

            # Upload batch summary
            self.s3_uploader.upload_batch_summary(all_results, batch_id)

            logger.info(
                f"Pipeline complete: {total_success} success, {total_errors} errors"
            )
            
            return {
                "batch_id": batch_id,
                "total_success": total_success,
                "total_errors": total_errors,
                "results": all_results
            }

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

