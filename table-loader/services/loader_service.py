# table-loader/services/loader_service.py
import logging
from typing import List

from services.data_transformer import DataTransformer
from services.database_client import DatabaseClient
from services.s3_client import S3Client

logger = logging.getLogger(__name__)


class LoaderService:
    def __init__(self, s3_client: S3Client, db_client: DatabaseClient):
        self.s3_client = s3_client
        self.db_client = db_client
        self.transformer = DataTransformer()

    def load_batch(self, batch_id: str, dry_run: bool = True):
        """Load all fragments for a batch"""
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Loading batch {batch_id}")

        try:
            # Get list of table fragments
            tables = self.s3_client.list_batch_fragments(batch_id)

            if not tables:
                logger.error(f"No fragments found for batch {batch_id}")
                return

            logger.info(f"Found {len(tables)} table(s) to load: {tables}")

            for table in tables:
                logger.info(f"\n{'=' * 60}")
                logger.info(f"Processing table: {table}")
                logger.info(f"{'=' * 60}")

                try:
                    self._load_table(batch_id, table, dry_run)

                    if not dry_run:
                        # Mark as loaded (move to processed/)
                        self.s3_client.mark_batch_loaded(batch_id, table)

                except Exception as e:
                    logger.error(f"Failed to load table {table}: {e}")
                    if not dry_run:
                        raise  # Fail fast on actual loads

            logger.info(f"\n{'=' * 60}")
            logger.info(f"✓ Batch {batch_id} loaded successfully")
            logger.info(f"{'=' * 60}")

        except Exception as e:
            logger.error(f"Batch load failed: {e}")
            raise

    def _load_table(self, batch_id: str, table: str, dry_run: bool):
        """Load a single table fragment"""
        # Download fragment
        fragment = self.s3_client.download_fragment(batch_id, table)

        logger.info(f"Target table: {table}")
        logger.info(f"Row count: {len(fragment.get('records', []))}")

        # Transform data
        records = self.transformer.transform_records(fragment, table)

        if dry_run:
            logger.info(f"[DRY RUN] Would load {len(records)} records to {table}")
            if records:
                logger.info(f"[DRY RUN] Sample record: {records[0]}")
            return

        # Load to database
        conn = self.db_client.get_connection()
        try:
            with conn.cursor() as cursor:
                # Insert records
                for record in records:
                    self._insert_record(cursor, table, record)

                conn.commit()
                logger.info(f"✓ Loaded {len(records)} records to {table}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            self.db_client.return_connection(conn)

    def _insert_record(self, cursor, table: str, record: dict):
        """Insert a single record into the database"""
        # Build INSERT statement
        columns = ", ".join(record.keys())
        placeholders = ", ".join(["%s"] * len(record))
        values = list(record.values())

        query = f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """

        cursor.execute(query, values)
