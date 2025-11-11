import io
import json
import logging
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Client:
    """S3 operations for fragment validation pipeline"""

    def __init__(self, bucket: str = "idhub-curated-fragments"):
        self.bucket = bucket
        self.s3 = boto3.client("s3")

    def upload_dataframe(self, df: pd.DataFrame, s3_key: str) -> None:
        """Upload DataFrame as CSV to S3

        Args:
            df: DataFrame to upload
            s3_key: S3 key (path) for the file
        """
        try:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv",
            )
            logger.info(f"Uploaded DataFrame to s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload DataFrame to S3: {e}")
            raise

    def upload_json(self, data: Dict[str, Any], s3_key: str) -> None:
        """Upload JSON data to S3

        Args:
            data: Dictionary to upload as JSON
            s3_key: S3 key (path) for the file
        """
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json.dumps(data, indent=2, default=str),
                ContentType="application/json",
            )
            logger.info(f"Uploaded JSON to s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload JSON to S3: {e}")
            raise

    def download_dataframe(self, s3_key: str) -> pd.DataFrame:
        """Download CSV from S3 as DataFrame

        Args:
            s3_key: S3 key (path) of the file

        Returns:
            DataFrame with the CSV data
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            return pd.read_csv(io.BytesIO(response["Body"].read()))
        except ClientError as e:
            logger.error(f"Failed to download from S3: {e}")
            raise

    def download_json(self, s3_key: str) -> Dict[str, Any]:
        """Download JSON from S3

        Args:
            s3_key: S3 key (path) of the file

        Returns:
            Dictionary with the JSON data
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except ClientError as e:
            logger.error(f"Failed to download JSON from S3: {e}")
            raise

    def list_objects(self, prefix: str) -> List[Dict[str, Any]]:
        """List objects in S3 with given prefix

        Args:
            prefix: S3 prefix to filter objects

        Returns:
            List of object metadata dictionaries
        """
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return response.get("Contents", [])
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            raise

    def object_exists(self, s3_key: str) -> bool:
        """Check if an object exists in S3

        Args:
            s3_key: S3 key (path) to check

        Returns:
            True if object exists, False otherwise
        """
        try:
            self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Error checking S3 object existence: {e}")
            raise

    def copy_object(self, source_key: str, dest_key: str) -> None:
        """Copy object within S3 bucket

        Args:
            source_key: Source S3 key
            dest_key: Destination S3 key
        """
        try:
            copy_source = {"Bucket": self.bucket, "Key": source_key}
            self.s3.copy_object(
                CopySource=copy_source, Bucket=self.bucket, Key=dest_key
            )
            logger.info(f"Copied s3://{self.bucket}/{source_key} to {dest_key}")
        except ClientError as e:
            logger.error(f"Failed to copy S3 object: {e}")
            raise

    def delete_object(self, s3_key: str) -> None:
        """Delete object from S3

        Args:
            s3_key: S3 key (path) to delete
        """
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=s3_key)
            logger.info(f"Deleted s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            logger.error(f"Failed to delete S3 object: {e}")
            raise

    def get_object_metadata(self, s3_key: str) -> Dict[str, Any]:
        """Get metadata for an S3 object

        Args:
            s3_key: S3 key (path) of the object

        Returns:
            Dictionary with object metadata
        """
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            return {
                "size": response["ContentLength"],
                "last_modified": response["LastModified"],
                "content_type": response.get("ContentType"),
                "metadata": response.get("Metadata", {}),
            }
        except ClientError as e:
            logger.error(f"Failed to get S3 object metadata: {e}")
            raise
