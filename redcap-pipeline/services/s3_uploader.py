import json
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from core.config import settings

logger = logging.getLogger(__name__)


class S3Uploader:
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_DEFAULT_REGION,
        )
        self.bucket = settings.S3_BUCKET

    def upload_fragment(self, batch_id: str, data: dict, center_id: int):
        """Upload PHI-free fragment to S3"""
        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            key = f"incoming/{batch_id}_{timestamp}.json"

            # Add metadata
            fragment = {
                "batch_id": batch_id,
                "center_id": center_id,
                "uploaded_at": datetime.utcnow().isoformat(),
                "data": data,
            }

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

    def upload_batch_summary(self, batch_id: str, summary: dict):
        """Upload batch processing summary"""
        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            key = f"summaries/{batch_id}_{timestamp}_summary.json"

            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(summary, indent=2),
                ContentType="application/json",
                ServerSideEncryption="AES256",
            )
            logger.info(f"Uploaded batch summary to s3://{self.bucket}/{key}")
            return key
        except ClientError as e:
            logger.error(f"Failed to upload batch summary: {e}")
            raise
