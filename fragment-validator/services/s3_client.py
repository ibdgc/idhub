import json
import logging
from typing import Any

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Client:
    """S3 operations client"""

    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3_client = boto3.client("s3")

    def list_batch_fragments(self, batch_id: str) -> list[dict]:
        """List all table fragment files for a batch"""
        prefix = f"staging/validated/{batch_id}/"

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=prefix
            )

            if "Contents" not in response:
                return []

            # Filter out validation_report.json and only return CSV files
            fragments = [
                obj
                for obj in response["Contents"]
                if obj["Key"].endswith(".csv")
            ]

            return fragments

        except ClientError as e:
            logger.error(f"Error listing batch fragments: {e}")
            raise

    def download_fragment(self, batch_id: str, table_name: str) -> pd.DataFrame:
        """Download a table fragment as DataFrame"""
        key = f"staging/validated/{batch_id}/{table_name}.csv"

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            df = pd.read_csv(response["Body"])
            logger.info(
                f"Downloaded {len(df)} rows from s3://{self.bucket}/{key}"
            )
            return df

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"Fragment not found: {key}")
                raise FileNotFoundError(f"Fragment not found: {key}")
            else:
                logger.error(f"Error downloading fragment {key}: {e}")
                raise
        except pd.errors.EmptyDataError:
            logger.warning(f"Empty CSV file: {key}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Unexpected error downloading fragment {key}: {e}")
            raise

    def download_validation_report(self, batch_id: str) -> dict:
        """Download validation report JSON"""
        key = f"staging/validated/{batch_id}/validation_report.json"

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            report = json.loads(response["Body"].read())
            logger.info(f"Downloaded validation report from s3://{self.bucket}/{key}")
            return report

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"Validation report not found: {key}")
                raise FileNotFoundError(f"Validation report not found: {key}")
            else:
                logger.error(f"Error downloading validation report {key}: {e}")
                raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in validation report {key}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error downloading validation report {key}: {e}")
            raise

    def mark_fragment_loaded(self, batch_id: str, table_name: str):
        """Move fragment to loaded/ prefix after successful load"""
        source_key = f"staging/validated/{batch_id}/{table_name}.csv"
        dest_key = f"staging/loaded/{batch_id}/{table_name}.csv"

        try:
            # Copy to new location
            self.s3_client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": source_key},
                Key=dest_key,
            )

            # Delete original
            self.s3_client.delete_object(Bucket=self.bucket, Key=source_key)

            logger.info(f"Moved fragment from {source_key} to {dest_key}")

        except ClientError as e:
            logger.error(f"Error marking fragment as loaded: {e}")
            raise

    def upload_json(self, data: dict, key: str):
        """Upload JSON data to S3"""
        try:
            json_data = json.dumps(data, indent=2)
            self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=json_data)
            logger.info(f"Uploaded JSON to s3://{self.bucket}/{key}")
        except ClientError as e:
            logger.error(f"Error uploading JSON to {key}: {e}")
            raise
