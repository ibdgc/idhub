# table-loader/services/loader.py
import logging
from typing import Dict, List, Optional

import pandas as pd
from core.database import get_db_connection, get_db_cursor

from services.data_transformer import DataTransformer
from services.s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Loads validated fragments from S3 into database tables"""

    # Tables that use UPSERT strategy (conflict resolution)
    UPSERT_TABLES = {
        "local_subject_ids": ["center_id", "local_subject_id", "identifier_type"],
        "subjects": ["global_subject_id"],
        # Add other tables that need upsert
    }

    def __init__(self, s3_bucket: str = "idhub-curated-fragments"):
        self.s3_client = S3Client(bucket=s3_bucket)

    def load_batch(
        self, batch_id: str, approve: bool = False, dry_run: bool = False
    ) -> Dict:
        """
        Load a validated batch from S3 into database

        Args:
            batch_id: Batch identifier (e.g., "batch_20251119_130611")
            approve: Whether to approve and load the batch
            dry_run: If True, analyze but don't commit changes

        Returns:
            Load result summary
        """
        logger.info(f"Loading batch: {batch_id}")

        try:
            # Download validation report
            report = self.s3_client.download_validation_report(batch_id)

            if report["status"] != "VALIDATED":
                raise ValueError(
                    f"Batch {batch_id} is not validated (status: {report['status']})"
                )

            if not approve and not report.get("auto_approved", False):
                raise ValueError(
                    f"Batch {batch_id} requires manual approval (use --approve flag)"
                )

            table_name = report["table_name"]
            logger.info(f"Loading table: {table_name}")

            # Download fragment data
            fragment_df = self.s3_client.download_fragment(batch_id, table_name)
            logger.info(f"Downloaded {len(fragment_df)} records")

            # Get exclude fields from report
            exclude_fields = set(report.get("exclude_from_load", []))

            # Transform data
            transformer = DataTransformer(table_name, exclude_fields)
            records = transformer.transform_records(fragment_df)

            if dry_run:
                logger.info(
                    f"DRY RUN: Would load {len(records)} records into {table_name}"
                )
                return {
                    "status": "DRY_RUN",
                    "batch_id": batch_id,
                    "table_name": table_name,
                    "records_loaded": 0,
                    "inserted": 0,
                    "updated": 0,
                    "would_load": len(records),
                }

            # Load into database
            load_result = self._load_records(table_name, records)

            # Load local_subject_ids if present
            local_ids_result = None
            try:
                local_ids_df = self.s3_client.download_fragment(
                    batch_id, "local_subject_ids"
                )
                if not local_ids_df.empty:
                    logger.info(
                        f"Loading {len(local_ids_df)} local_subject_ids records"
                    )
                    local_ids_transformer = DataTransformer("local_subject_ids", set())
                    local_ids_records = local_ids_transformer.transform_records(
                        local_ids_df
                    )
                    local_ids_result = self._load_records(
                        "local_subject_ids", local_ids_records
                    )
            except Exception as e:
                logger.warning(f"No local_subject_ids to load: {e}")

            return {
                "status": "SUCCESS",
                "batch_id": batch_id,
                "table_name": table_name,
                "records_loaded": load_result["inserted"] + load_result["updated"],
                "inserted": load_result["inserted"],
                "updated": load_result["updated"],
                "local_ids_loaded": local_ids_result["inserted"]
                + local_ids_result["updated"]
                if local_ids_result
                else 0,
            }

        except Exception as e:
            logger.error(f"Failed to load batch {batch_id}: {e}", exc_info=True)
            return {
                "status": "FAILED",
                "batch_id": batch_id,
                "error": str(e),
            }

    def _load_records(self, table_name: str, records: List[Dict]) -> Dict:
        """
        Load records into database table

        Args:
            table_name: Target table name
            records: List of record dictionaries

        Returns:
            {"inserted": int, "updated": int}
        """
        if not records:
            logger.warning(f"No records to load for table {table_name}")
            return {"inserted": 0, "updated": 0}

        # Determine load strategy
        if table_name in self.UPSERT_TABLES:
            return self._upsert_records(table_name, records)
        else:
            return self._insert_records(table_name, records)

    def _insert_records(self, table_name: str, records: List[Dict]) -> Dict:
        """Insert records (fail on conflict)"""
        conn = get_db_connection()
        inserted = 0

        try:
            with get_db_cursor(conn) as cursor:
                for record in records:
                    columns = list(record.keys())
                    values = [record[col] for col in columns]

                    placeholders = ", ".join(["%s"] * len(columns))
                    columns_str = ", ".join(columns)

                    query = f"""
                        INSERT INTO {table_name} ({columns_str})
                        VALUES ({placeholders})
                    """

                    cursor.execute(query, values)
                    inserted += 1

            conn.commit()
            logger.info(f"✓ Inserted {inserted} records into {table_name}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to insert records: {e}")
            raise
        finally:
            conn.close()

        return {"inserted": inserted, "updated": 0}

    def _upsert_records(self, table_name: str, records: List[Dict]) -> Dict:
        """Upsert records (insert or update on conflict)"""
        conn = get_db_connection()
        inserted = 0
        updated = 0

        conflict_columns = self.UPSERT_TABLES[table_name]

        try:
            with get_db_cursor(conn) as cursor:
                for record in records:
                    columns = list(record.keys())
                    values = [record[col] for col in columns]

                    placeholders = ", ".join(["%s"] * len(columns))
                    columns_str = ", ".join(columns)

                    # Build UPDATE clause (exclude conflict columns)
                    update_columns = [
                        col for col in columns if col not in conflict_columns
                    ]
                    update_clause = ", ".join(
                        [f"{col} = EXCLUDED.{col}" for col in update_columns]
                    )

                    conflict_str = ", ".join(conflict_columns)

                    query = f"""
                        INSERT INTO {table_name} ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT ({conflict_str})
                        DO UPDATE SET {update_clause}
                        RETURNING (xmax = 0) AS inserted
                    """

                    cursor.execute(query, values)
                    result = cursor.fetchone()

                    if result and result[0]:
                        inserted += 1
                    else:
                        updated += 1

            conn.commit()
            logger.info(
                f"✓ Upserted {inserted + updated} records into {table_name} (inserted={inserted}, updated={updated})"
            )

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to upsert records: {e}")
            raise
        finally:
            conn.close()

        return {"inserted": inserted, "updated": updated}
