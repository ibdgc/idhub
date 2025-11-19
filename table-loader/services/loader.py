# table-loader/services/loader.py
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
from core.database import get_db_connection, get_db_cursor
from psycopg2.extras import execute_values

from .data_transformer import DataTransformer
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Handles loading validated fragments into database tables"""

    def __init__(
        self,
        s3_bucket: str,
        db_connection=None,
    ):
        """
        Initialize table loader

        Args:
            s3_bucket: S3 bucket name containing validated fragments
            db_connection: Database connection (optional, will create if not provided)
        """
        self.s3_client = S3Client(s3_bucket)
        self.db_connection = db_connection or get_db_connection()
        self.validation_report: Dict[str, Any] = {}

    def load_batch(
        self, batch_id: str, approve: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Load all fragments from a validated batch

        Args:
            batch_id: Batch identifier
            approve: If True, proceed with loading (required for non-dry-run)
            dry_run: If True, analyze without making changes

        Returns:
            Dictionary with load results
        """
        logger.info(f"Loading batch: {batch_id}")

        try:
            # Download and parse validation report
            self.validation_report = self.s3_client.download_validation_report(batch_id)

            # Check if batch requires approval
            if not dry_run and not approve:
                if not self.validation_report.get("auto_approved", False):
                    raise ValueError(
                        f"Batch {batch_id} requires manual approval (use --approve flag)"
                    )

            table_name = self.validation_report.get("table_name")
            if not table_name:
                raise ValueError("Validation report missing table_name")

            # Download fragment data
            fragment_df = self.s3_client.download_fragment(batch_id, table_name)
            logger.info(f"Downloaded {len(fragment_df)} records")

            # Load main table records
            result = self._load_records(table_name, fragment_df, dry_run)

            # Load local_subject_ids if present
            local_ids_result = self._load_local_subject_ids(batch_id, dry_run)

            # Build response
            if dry_run:
                return {
                    "status": "DRY_RUN",
                    "batch_id": batch_id,
                    "table_name": table_name,
                    "would_load": result.get("rows_analyzed", 0),
                    "local_ids_would_load": local_ids_result.get("rows_analyzed", 0)
                    if local_ids_result
                    else 0,
                }
            else:
                return {
                    "status": "SUCCESS",
                    "batch_id": batch_id,
                    "table_name": table_name,
                    "records_loaded": result.get("rows_loaded", 0),
                    "inserted": result.get("rows_loaded", 0),
                    "updated": 0,  # No updates in current implementation
                    "local_ids_loaded": local_ids_result.get("rows_loaded", 0)
                    if local_ids_result
                    else 0,
                }

        except Exception as e:
            logger.error(f"Failed to load batch {batch_id}: {e}")
            return {
                "status": "FAILED",
                "batch_id": batch_id,
                "error": str(e),
            }

    def _load_records(
        self, table_name: str, fragment_df, dry_run: bool = False
    ) -> Dict[str, Any]:
        """Load records using appropriate strategy"""

        # Get exclude fields from validation report
        exclude_from_load = self.validation_report.get("exclude_from_load", [])
        if isinstance(exclude_from_load, str):
            exclude_fields = {exclude_from_load}
        else:
            exclude_fields = set(exclude_from_load)

        logger.info(f"Exclude fields from validation report: {exclude_fields}")

        # Create transformer with exclude fields
        transformer = DataTransformer(
            table_name=table_name, exclude_fields=exclude_fields
        )

        # Transform records
        transformed_records = transformer.transform_records(fragment_df)

        if not transformed_records:
            logger.warning(f"No records to load for {table_name}")
            return {
                "status": "skipped",
                "rows_loaded": 0,
                "message": "No valid records after transformation",
            }

        # Execute load
        if dry_run:
            logger.info(
                f"DRY RUN: Would load {len(transformed_records)} records to {table_name}"
            )
            return {
                "status": "dry_run",
                "rows_analyzed": len(transformed_records),
            }

        return self._insert_records(table_name, transformed_records)

    def _insert_records(self, table_name: str, records: List[Dict]) -> Dict[str, Any]:
        """Insert records into table"""

        if not records:
            return {"status": "skipped", "rows_loaded": 0}

        # Get column names from first record
        columns = list(records[0].keys())

        # Build INSERT query
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        try:
            with get_db_cursor(self.db_connection) as cursor:
                # Execute batch insert
                cursor.executemany(query, records)
                rows_inserted = cursor.rowcount

                self.db_connection.commit()
                logger.info(f"Inserted {rows_inserted} rows into {table_name}")

                return {
                    "status": "success",
                    "rows_loaded": rows_inserted,
                }

        except psycopg2.Error as e:
            self.db_connection.rollback()
            logger.error(f"Failed to insert records: {e}")
            raise

    def _load_local_subject_ids(
        self, batch_id: str, dry_run: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Load local subject IDs if present in batch"""

        try:
            local_ids_key = f"staging/validated/{batch_id}/local_subject_ids.csv"

            # Try to download - will raise exception if not found
            import pandas as pd

            response = self.s3_client.s3_client.get_object(
                Bucket=self.s3_client.bucket, Key=local_ids_key
            )
            local_ids_df = pd.read_csv(response["Body"])

            if local_ids_df.empty:
                logger.info("No local_subject_ids to load")
                return None

            logger.info(f"Loading {len(local_ids_df)} local subject ID mappings")

            # Transform and load
            transformer = DataTransformer(
                table_name="local_subject_ids",
                exclude_fields=set(),  # No exclusions for local_subject_ids
            )
            transformed_records = transformer.transform_records(local_ids_df)

            if dry_run:
                logger.info(
                    f"DRY RUN: Would load {len(transformed_records)} local subject IDs"
                )
                return {
                    "status": "dry_run",
                    "rows_analyzed": len(transformed_records),
                }

            return self._insert_records("local_subject_ids", transformed_records)

        except Exception as e:
            # Local subject IDs are optional, so just log warning
            logger.warning(f"Could not load local_subject_ids: {e}")
            return None

    def close(self):
        """Close database connection"""
        if self.db_connection:
            self.db_connection.close()
