# table-loader/services/loader_service.py
import logging
from typing import List

from .data_transformer import DataTransformer
from .database_client import DatabaseClient
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class LoaderService:
    """Service for loading validated fragments into database"""

    def __init__(self, s3_client: S3Client, db_client: DatabaseClient):
        self.s3_client = s3_client
        self.db_client = db_client

    def load_batch(self, batch_id: str, dry_run: bool = True):
        """Load all tables in a batch"""
        try:
            logger.info(f"Loading batch {batch_id}")

            # List all table fragments in batch
            tables = self.s3_client.list_batch_fragments(batch_id)

            if not tables:
                raise ValueError(f"No table fragments found for batch {batch_id}")

            logger.info(f"Found {len(tables)} table(s) to load: {tables}")

            # Load each table
            for table in tables:
                logger.info("")
                logger.info("=" * 60)
                logger.info(f"Processing table: {table}")
                logger.info("=" * 60)

                try:
                    self._load_table(batch_id, table, dry_run)
                except Exception as e:
                    logger.error(f"Failed to load table {table}: {e}")
                    raise

            logger.info("")
            logger.info("=" * 60)
            logger.info(f"✓ Batch {batch_id} loaded successfully")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Batch load failed: {e}")
            raise

    def _load_table(self, batch_id: str, table: str, dry_run: bool):
        """Load a single table fragment"""

        # Download validation report to get metadata about resolution fields
        try:
            report = self.s3_client.download_validation_report(batch_id)
            logger.info(f"Loaded validation report for batch {batch_id}")
        except Exception as e:
            logger.warning(
                f"Could not load validation report: {e}. Using default exclusions."
            )
            report = {}

        # Download fragment
        fragment = self.s3_client.download_fragment(batch_id, table)

        logger.info(f"Target table: {table}")
        logger.info(f"Row count: {len(fragment.get('records', []))}")

        # Build exclusion list from validation report
        exclude_fields = set()

        # Option 1: Use explicit exclude_from_load if present (preferred)
        if "exclude_from_load" in report:
            exclude_fields.update(report["exclude_from_load"])
            logger.info(
                f"Using explicit exclude_from_load: {report['exclude_from_load']}"
            )
        else:
            # Option 2: Derive from subject_id_candidates and center_id_field
            subject_id_candidates = report.get("subject_id_candidates", [])
            if subject_id_candidates:
                exclude_fields.update(subject_id_candidates)
                logger.info(f"Excluding subject_id_candidates: {subject_id_candidates}")

            center_id_field = report.get("center_id_field")
            if center_id_field:
                exclude_fields.add(center_id_field)
                logger.info(f"Excluding center_id_field: {center_id_field}")

        # Always exclude these resolution-only fields
        exclude_fields.update(["identifier_type", "action", "local_subject_id"])

        logger.info(f"Total fields to exclude: {sorted(exclude_fields)}")

        # Create transformer for this specific table with exclusions
        transformer = DataTransformer(table, exclude_fields=exclude_fields)

        # Transform data
        records = transformer.transform_records(fragment)

        # Validate that we have data to load
        if not records or len(records) == 0:
            error_msg = f"No records found for table {table} in batch {batch_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if dry_run:
            logger.info(f"[DRY RUN] Would load {len(records)} records to {table}")
            if records:
                logger.info(f"[DRY RUN] Sample record keys: {list(records[0].keys())}")
                logger.info(f"[DRY RUN] Sample record: {records[0]}")
            return

        # Load to database
        conn = self.db_client.get_connection()
        try:
            with conn.cursor() as cursor:
                # Insert records
                if records:
                    # Build INSERT statement
                    columns = list(records[0].keys())
                    placeholders = ", ".join(["%s"] * len(columns))
                    columns_str = ", ".join(columns)

                    insert_sql = f"""
                        INSERT INTO {table} ({columns_str})
                        VALUES ({placeholders})
                    """

                    # Execute batch insert
                    values = [
                        tuple(record[col] for col in columns) for record in records
                    ]
                    cursor.executemany(insert_sql, values)

                    conn.commit()
                    logger.info(
                        f"✓ Loaded {len(records)}/{len(records)} records to {table}"
                    )

            # Mark fragment as loaded (move to processed)
            self.s3_client.mark_fragment_loaded(batch_id, table)

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error loading {table}: {e}")
            raise
        finally:
            self.db_client.return_connection(conn)
