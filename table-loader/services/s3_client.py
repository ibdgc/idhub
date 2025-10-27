import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Client:
    """Client for S3 operations"""

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = boto3.client("s3")
        logger.info(f"Initialized S3 client for bucket: {bucket_name}")

    def download_file_content(self, key: str) -> bytes:
        """Download file content from S3 as bytes"""
        try:
            logger.debug(f"Downloading s3://{self.bucket_name}/{key}")
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            content = response["Body"].read()
            logger.debug(f"Downloaded {len(content)} bytes from {key}")
            return content
        except ClientError as e:
            logger.error(f"Failed to download {key}: {e}")
            raise

    def upload_file(self, local_path: str, key: str):
        """Upload file to S3"""
        try:
            logger.info(f"Uploading {local_path} to s3://{self.bucket_name}/{key}")
            self.client.upload_file(local_path, self.bucket_name, key)
            logger.info(f"✓ Uploaded to s3://{self.bucket_name}/{key}")
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            raise

    def list_objects(self, prefix: str):
        """List objects in S3 with given prefix"""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )
            return response.get("Contents", [])
        except ClientError as e:
            logger.error(f"Failed to list objects with prefix {prefix}: {e}")
            raise
