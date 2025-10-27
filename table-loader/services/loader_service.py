import json
import logging
from datetime import datetime
from typing import Any, Dict

import pandas as pd
from psycopg2.extras import RealDictCursor, execute_values

from .database_client import DatabaseClient
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class LoaderService:
    """Service for loading validated batches into PostgreSQL"""

    def __init__(self, s3_client: S3Client, db_client: DatabaseClient):
        self.s3_client = s3_client
        self.db_client = db_client

    def load_batch(self, batch_id: str, dry_run: bool = True):
        """Load a validated batch into PostgreSQL"""
        logger.info(f"{'DRY RUN: ' if dry_run else ''}Loading batch {batch_id}")

        # Download validation report
        report = self._download_validation_report(batch_id)
        table_name = report["table_name"]

        logger.info(f"Target table: {table_name}")
        logger.info(f"Row count: {report['row_count']}")

        # Download data files
        data_df, local_ids_df = self._download_data_files(batch_id, table_name)

        logger.info(f"Loaded {len(data_df)} sample records")
        logger.info(f"Loaded {len(local_ids_df)} local ID mappings")

        if dry_run:
            self._log_dry_run(table_name, data_df, local_ids_df)
            return

        # Execute load
        conn = self.db_client.get_connection()
        try:
            # 1. Load subjects (if new GSIDs)
            new_subjects = self._load_subjects(conn, local_ids_df)

            # 2. Load local_subject_ids
            new_local_ids = self._load_local_ids(conn, local_ids_df)

            # 3. Load sample table
            upserted = self._load_samples(conn, table_name, data_df)

            conn.commit()

            logger.info(f"✓ Successfully loaded batch {batch_id}")
            logger.info(f"  - {new_subjects} new subjects")
            logger.info(f"  - {new_local_ids} local ID mappings")
            logger.info(f"  - {upserted} records to {table_name}")

        except Exception as e:
            conn.rollback()
            logger.error(f"✗ Load failed: {e}")
            raise
        finally:
            self.db_client.return_connection(conn)

    def _download_validation_report(self, batch_id: str) -> Dict[str, Any]:
        """Download and parse validation report from S3"""
        report_key = f"staging/validated/{batch_id}/validation_report.json"
        report_content = self.s3_client.download_file_content(report_key)
        return json.loads(report_content)

    def _download_data_files(self, batch_id: str, table_name: str):
        """Download data and local_ids CSV files from S3"""
        data_key = f"staging/validated/{batch_id}/{table_name}.csv"
        local_ids_key = f"staging/validated/{batch_id}/local_subject_ids.csv"

        # Download and parse CSVs
        data_content = self.s3_client.download_file_content(data_key)
        data_df = pd.read_csv(pd.io.common.BytesIO(data_content))

        local_ids_content = self.s3_client.download_file_content(local_ids_key)
        local_ids_df = pd.read_csv(pd.io.common.BytesIO(local_ids_content))

        return data_df, local_ids_df

    def _log_dry_run(
        self, table_name: str, data_df: pd.DataFrame, local_ids_df: pd.DataFrame
    ):
        """Log what would happen in a dry run"""
        logger.info("DRY RUN - No changes will be made")
        logger.info(
            f"Would insert {len(local_ids_df[local_ids_df['action'] == 'create_new'])} new subjects"
        )
        logger.info(f"Would insert {len(local_ids_df)} local ID mappings")
        logger.info(f"Would upsert {len(data_df)} records to {table_name}")

    def _load_subjects(self, conn, local_ids_df: pd.DataFrame) -> int:
        """Insert new subjects from local_subject_ids"""
        new_subjects = local_ids_df[local_ids_df["action"] == "create_new"]

        if len(new_subjects) == 0:
            logger.info("No new subjects to create")
            return 0

        subjects = new_subjects[["global_subject_id", "center_id"]].drop_duplicates()
        current_year = datetime.now().year

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO subjects (global_subject_id, center_id, registration_year)
                VALUES %s
                ON CONFLICT (global_subject_id) DO NOTHING
                """,
                [
                    (row["global_subject_id"], row["center_id"], current_year)
                    for _, row in subjects.iterrows()
                ],
            )

        logger.info(f"✓ Inserted {len(subjects)} new subjects")
        return len(subjects)

    def _load_local_ids(self, conn, local_ids_df: pd.DataFrame) -> int:
        """Insert local_subject_ids with conflict detection"""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            inserted = 0
            conflicts = 0

            for _, row in local_ids_df.iterrows():
                # Check for existing mapping
                cur.execute(
                    """
                    SELECT global_subject_id
                    FROM local_subject_ids
                    WHERE center_id = %s AND local_subject_id = %s AND identifier_type = %s
                    """,
                    (row["center_id"], row["local_subject_id"], row["identifier_type"]),
                )
                existing = cur.fetchone()

                if (
                    existing
                    and existing["global_subject_id"] != row["global_subject_id"]
                ):
                    # Conflict detected
                    logger.warning(
                        f"CONFLICT: {row['local_subject_id']} already linked to "
                        f"{existing['global_subject_id']}, attempting {row['global_subject_id']}"
                    )
                    conflicts += 1

                    # Flag both subjects for review
                    cur.execute(
                        """
                        UPDATE subjects
                        SET flagged_for_review = TRUE,
                            review_notes = COALESCE(review_notes || E'\n', '') || %s
                        WHERE global_subject_id IN (%s, %s)
                        """,
                        (
                            f"[{datetime.utcnow().isoformat()}] Duplicate local_id: "
                            f"{row['local_subject_id']} ({row['identifier_type']})",
                            row["global_subject_id"],
                            existing["global_subject_id"],
                        ),
                    )
                    continue

                # Insert or skip if already correct
                cur.execute(
                    """
                    INSERT INTO local_subject_ids
                    (center_id, local_subject_id, identifier_type, global_subject_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                    """,
                    (
                        row["center_id"],
                        row["local_subject_id"],
                        row["identifier_type"],
                        row["global_subject_id"],
                    ),
                )
                inserted += 1

        if conflicts > 0:
            logger.warning(f"⚠ {conflicts} conflicts detected and flagged for review")

        logger.info(f"✓ Inserted {inserted} local ID mappings")
        return inserted

    def _load_samples(self, conn, table_name: str, data_df: pd.DataFrame) -> int:
        """Upsert sample records with dynamic schema detection"""
        with conn.cursor() as cur:
            # Get primary key
            cur.execute(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                """,
                (table_name,),
            )
            result = cur.fetchone()
            if not result:
                raise ValueError(f"No primary key found for table {table_name}")

            pk_col = result[0]

            # Get all actual columns in the table (excluding auto-generated)
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name NOT IN ('created_at', 'updated_at')
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            db_columns = {row[0] for row in cur.fetchall()}

        logger.info(f"Primary key: {pk_col}")
        logger.info(f"Table columns: {sorted(db_columns)}")

        # Filter DataFrame to only include columns that exist in the database
        available_columns = [col for col in data_df.columns if col in db_columns]
        missing_columns = [
            col
            for col in data_df.columns
            if col not in db_columns and col not in ["created_at", "updated_at"]
        ]

        if missing_columns:
            logger.warning(f"Skipping columns not in table schema: {missing_columns}")

        logger.info(f"Inserting columns: {available_columns}")

        # Build upsert query
        update_cols = [col for col in available_columns if col != pk_col]
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

        query = f"""
            INSERT INTO {table_name} ({",".join(available_columns)})
            VALUES %s
            ON CONFLICT ({pk_col})
            DO UPDATE SET {update_clause}
        """

        # Execute in batches
        with conn.cursor() as cur:
            execute_values(
                cur,
                query,
                [
                    tuple(
                        row[col] if pd.notna(row[col]) else None
                        for col in available_columns
                    )
                    for _, row in data_df.iterrows()
                ],
                page_size=1000,
            )

        logger.info(f"✓ Upserted {len(data_df)} records to {table_name}")
        return len(data_df)
