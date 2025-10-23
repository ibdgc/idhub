# redcap-pipeline/services/pipeline.py
import logging

from .center_resolver import CenterResolver
from .data_processor import DataProcessor
from .gsid_client import GSIDClient
from .redcap_client import REDCapClient
from .s3_uploader import S3Uploader

logger = logging.getLogger(__name__)


class REDCapPipeline:
    def __init__(self):
        self.redcap_client = REDCapClient()
        self.center_resolver = CenterResolver()
        self.gsid_client = GSIDClient()
        self.data_processor = DataProcessor(self.center_resolver, self.gsid_client)
        self.s3_uploader = S3Uploader()

    def run(self):
        """Execute the full pipeline"""
        logger.info("Fetching records from REDCap")
        records = self.redcap_client.fetch_records()

        logger.info("Processing records")
        self.data_processor.process_records(records)

        logger.info("Uploading fragment to S3")
        self.s3_uploader.upload_fragment(records, "redcap_export")

        logger.info("Pipeline execution complete")
