# table-loader/services/s3_client.py
import json
import logging
from typing import Any, Dict, List

import boto3
from core.config import settings

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.bucket = settings.S3_BUCKET

    def download_fragment(self, batch_id: str, table: str) -> Dict[str, Any]:
        """Download validated fragment from S3"""
        key = f"curated/{table}/{batch_id}.json"

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read())
            logger.info(f"Downloaded fragment from s3://{self.bucket}/{key}")
            return data
        except Exception as e:
            logger.error(f"Error downloading fragment: {e}")
            raise

    def list_batch_fragments(self, batch_id: str) -> List[str]:
        """List all fragments for a batch"""
        prefix = f"curated/"
        tables = []

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]
                    if batch_id in key and key.endswith(".json"):
                        # Extract table name from path: curated/{table}/{batch_id}.json
                        parts = key.split("/")
                        if len(parts) >= 3:
                            tables.append(parts[1])

            logger.info(f"Found {len(tables)} fragments for batch {batch_id}")
            return tables

        except Exception as e:
            logger.error(f"Error listing fragments: {e}")
            raise

    def upload_load_report(self, batch_id: str, report: Dict[str, Any]):
        """Upload load report to S3"""
        key = f"load_reports/{batch_id}_report.json"

        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(report, indent=2),
                ContentType="application/json",
            )
            logger.info(f"Uploaded load report to s3://{self.bucket}/{key}")
        except Exception as e:
            logger.error(f"Error uploading report: {e}")
            raise
