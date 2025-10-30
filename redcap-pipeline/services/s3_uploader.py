import json
import logging
from datetime import datetime
from typing import Dict

import boto3
from botocore.exceptions import ClientError
from core.config import settings

logger = logging.getLogger(__name__)


class S3Uploader:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.bucket = settings.S3_BUCKET

    def upload_fragment(self, fragment: Dict, project_key: str, gsid: str):
        """Upload curated fragment to S3"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"subjects/{gsid}/{project_key}_{timestamp}.json"

        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(fragment, indent=2),
                ContentType="application/json",
                ServerSideEncryption="AES256",
            )
            logger.info(f"Uploaded fragment to s3://{self.bucket}/{key}")
            return key
        except ClientError as e:
            logger.error(f"Failed to upload fragment: {e}")
            raise
