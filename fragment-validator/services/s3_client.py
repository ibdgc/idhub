# fragment-validator/services/s3_client.py
import io
import json
import logging
from typing import Dict

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

    def upload_json(self, data: Dict, s3_key: str) -> None:
        """Upload JSON data to S3

        Args:
            data: Dictionary to upload as JSON
            s3_key: S3 key (path) for the file
        """
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json.dumps(data, indent=2),
                ContentType="application/json",
            )
            logger.info(f"Uploaded JSON to s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload JSON to S3: {e}")
            raise

    def download_dataframe(self, s3_key: str) -> pd.DataFrame:
        """Download CSV from S3 as DataFrame

        Args:
            s3_key: S3 key (path) for the file

        Returns:
            DataFrame
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            df = pd.read_csv(io.BytesIO(response["Body"].read()))
            logger.info(f"Downloaded DataFrame from s3://{self.bucket}/{s3_key}")
            return df
        except ClientError as e:
            logger.error(f"Failed to download DataFrame from S3: {e}")
            raise
