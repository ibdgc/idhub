# fragment-validator/services/s3_client.py
import json
import logging
from typing import Any, Dict

import boto3
from core.config import settings

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.bucket = settings.S3_BUCKET

    def download_fragment(self, batch_id: str, table: str) -> Dict[str, Any]:
        """Download fragment from S3"""
        key = f"curated/{table}/{batch_id}.json"

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read())
            logger.info(f"Downloaded fragment from s3://{self.bucket}/{key}")
            return data
        except Exception as e:
            logger.error(f"Error downloading fragment: {e}")
            raise

    def upload_validation_report(
        self, batch_id: str, table: str, report: Dict[str, Any]
    ):
        """Upload validation report to S3"""
        key = f"validation/{table}/{batch_id}_report.json"

        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(report, indent=2),
                ContentType="application/json",
            )
            logger.info(f"Uploaded validation report to s3://{self.bucket}/{key}")
        except Exception as e:
            logger.error(f"Error uploading report: {e}")
            raise
