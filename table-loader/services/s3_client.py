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

    def list_batch_fragments(self, batch_id: str) -> List[str]:
        """List all table fragments for a batch"""
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
                filename = key.split("/")[-1]

                # Skip metadata files - only process actual table CSVs
                if filename in ["validation_report.json", "local_subject_ids.csv"]:
                    continue

                # Extract table name from CSV files
                if filename.endswith(".csv"):
                    table_name = filename.replace(".csv", "")
                    fragments.append(table_name)
                    logger.info(f"Found table fragment: {table_name}")

            logger.info(f"Found {len(fragments)} fragments: {fragments}")
            return fragments

        except Exception as e:
            logger.error(f"Error listing batch fragments: {e}")
            raise

    def download_fragment(self, batch_id: str, table: str) -> Dict[str, Any]:
        """Download a table fragment from S3"""
        # Updated to download CSV files, not JSON
        key = f"staging/validated/{batch_id}/{table}.csv"

        try:
            logger.info(f"Downloading fragment from s3://{self.bucket}/{key}")

            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)

            # Read CSV data
            import io

            import pandas as pd

            csv_data = response["Body"].read().decode("utf-8")
            df = pd.read_csv(io.StringIO(csv_data))

            # Convert DataFrame to records format expected by transformer
            records = df.to_dict("records")

            logger.info(f"✓ Downloaded fragment: {table} ({len(records)} records)")

            return {
                "table": table,
                "records": records,
                "metadata": {"batch_id": batch_id, "row_count": len(records)},
            }

        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"Fragment not found: s3://{self.bucket}/{key}")
            raise FileNotFoundError(f"Fragment not found: {table}")
        except Exception as e:
            logger.error(f"Error downloading fragment {table}: {e}")
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

    def mark_batch_loaded(self, batch_id: str, table: str):
        """Mark a fragment as loaded by moving it to processed/"""
        source_key = f"staging/validated/{batch_id}/{table}.json"
        dest_key = f"staging/processed/{batch_id}/{table}.json"

        try:
            # Copy to processed
            self.s3_client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": source_key},
                Key=dest_key,
            )

            # Delete from validated
            self.s3_client.delete_object(Bucket=self.bucket, Key=source_key)

            logger.info(f"✓ Moved fragment to processed: {table}")
        except Exception as e:
            logger.warning(f"Could not mark fragment as loaded: {e}")
            # Don't fail on this - it's just housekeeping
