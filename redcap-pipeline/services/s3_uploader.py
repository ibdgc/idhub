# redcap-pipeline/services/s3_uploader.py
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import boto3
from core.config import settings

logger = logging.getLogger(__name__)


class S3Uploader:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.bucket = settings.S3_BUCKET

    def upload_fragment(self, data: List[Dict[str, Any]], fragment_name: str):
        """Upload data fragment to S3"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"redcap/{fragment_name}_{timestamp}.json"

        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(data, indent=2),
                ContentType="application/json",
            )
            logger.info(f"Uploaded fragment to s3://{self.bucket}/{key}")
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            raise
