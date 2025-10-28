# table-loader/services/s3_client.py
import json
import logging
from typing import Any, Dict, List

import boto3
import pandas as pd
from core.config import settings

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.bucket = settings.S3_BUCKET

    def list_batch_fragments(self, batch_id: str) -> List[str]:
        """List all table fragments for a batch"""
        # Updated to match actual S3 structure
        prefix = f"staging/validated/{batch_id}/"

        try:
            logger.info(
                f"Listing fragments for batch {batch_id} at s3://{self.bucket}/{prefix}"
            )

            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

            if "Contents" not in response:
                logger.warning(f"No fragments found for batch {batch_id}")
                return []

            # Extract table names from fragment files
            fragments = []
            for obj in response["Contents"]:
                key = obj["Key"]
                # Skip metadata files
                if key.endswith("validation_report.json") or key.endswith(
                    "local_subject_ids.csv"
                ):
                    continue
                # Extract table name from CSV files
                if key.endswith(".csv"):
                    table_name = key.split("/")[-1].replace(".csv", "")
                    fragments.append(table_name)
                    logger.info(f"Found table fragment: {table_name}")

            logger.info(f"Found {len(fragments)} fragments: {fragments}")
            return fragments

        except Exception as e:
            logger.error(f"Error listing batch fragments: {e}")
            raise

    def download_fragment(self, batch_id: str, table: str) -> Dict[str, Any]:
        """Download a table fragment as a dictionary with records"""
        key = f"staging/validated/{batch_id}/{table}.csv"

        try:
            logger.info(f"Downloading fragment from s3://{self.bucket}/{key}")

            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            df = pd.read_csv(response["Body"])

            # Convert DataFrame to list of dicts
            records = df.to_dict("records")

            logger.info(f"✓ Downloaded fragment: {table} ({len(records)} records)")

            return {"table": table, "records": records}

        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"Fragment not found: s3://{self.bucket}/{key}")
            raise
        except Exception as e:
            logger.error(f"Error downloading fragment: {e}")
            raise

    def download_validation_report(self, batch_id: str) -> Dict[str, Any]:
        """Download validation report for a batch"""
        key = f"staging/validated/{batch_id}/validation_report.json"

        try:
            logger.info(f"Downloading validation report from s3://{self.bucket}/{key}")

            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            report_data = json.loads(response["Body"].read().decode("utf-8"))

            logger.info(f"✓ Downloaded validation report for batch {batch_id}")
            return report_data

        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"Validation report not found: s3://{self.bucket}/{key}")
            return {}
        except Exception as e:
            logger.error(f"Error downloading validation report: {e}")
            return {}

    def mark_fragment_loaded(self, batch_id: str, table: str):
        """Mark a fragment as loaded by moving it to processed folder"""
        source_prefix = f"staging/validated/{batch_id}"
        dest_prefix = f"staging/processed/{batch_id}"

        try:
            # List all files for this batch
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=source_prefix
            )

            if "Contents" not in response:
                logger.warning(f"No files found to move for batch {batch_id}")
                return

            # Move all files from validated to processed
            for obj in response["Contents"]:
                source_key = obj["Key"]
                # Create destination key by replacing validated with processed
                dest_key = source_key.replace("staging/validated", "staging/processed")

                # Copy to processed
                self.s3_client.copy_object(
                    Bucket=self.bucket,
                    CopySource={"Bucket": self.bucket, "Key": source_key},
                    Key=dest_key,
                )

                # Delete from validated
                self.s3_client.delete_object(Bucket=self.bucket, Key=source_key)

            logger.info(f"✓ Moved batch {batch_id} to processed folder")

        except Exception as e:
            logger.warning(f"Could not mark batch as loaded: {e}")
            # Don't fail on this - it's just housekeeping
