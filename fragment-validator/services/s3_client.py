# fragment-validator/services/s3_client.py
import json
import logging
from typing import Any

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


class S3Client:
    """S3 operations client"""

    def __init__(self, bucket: str):
        self.bucket = bucket
        self.client = boto3.client("s3")

    def upload_dataframe(self, df: pd.DataFrame, key: str):
        """Upload DataFrame as CSV to S3"""
        csv_data = df.to_csv(index=False)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=csv_data)
        logger.info(f"Uploaded to s3://{self.bucket}/{key}")

    def upload_json(self, data: dict, key: str):
        """Upload JSON data to S3"""
        json_data = json.dumps(data, indent=2)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=json_data)
        logger.info(f"Uploaded JSON to s3://{self.bucket}/{key}")

    def download_dataframe(self, key: str) -> pd.DataFrame:
        """Download CSV from S3 as DataFrame"""
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(obj["Body"])

    def list_objects(self, prefix: str) -> list:
        """List objects with given prefix"""
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return response.get("Contents", [])
