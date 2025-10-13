import argparse
import json
import logging
import os
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values, RealDictCursor
import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/loader.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class TableLoader:
    def __init__(self):
        self.s3_bucket = os.getenv("S3_BUCKET", "idhub-curated-fragments")
        self.s3_client = boto3.client("s3")

        self.db_config = {
            "host": os.getenv("DB_HOST", "idhub_db"),
            "database": os.getenv("DB_NAME", "idhub"),
            "user": os.getenv("DB_USER", "idhub_user"),
            "password": os.getenv("DB_PASSWORD"),
            "port": int(os.getenv("DB_PORT", 5432)),
        }
        self.db_pool = None

    def ensure_pool(self):
        """Lazy initialization of connection pool"""
        if self.db_pool is None:
            logger.info("Initializing database connection pool...")
            self.db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **self.db_config)

    def get_db_connection(self):
        self.ensure_pool()
        return self.db_pool.getconn()

    def return_db_connection(self, conn):
        if self.db_pool:
            self.db_pool.putconn(conn)

    def load_batch(self, batch_id: str, dry_run: bool = True):
        """Load a validated batch into PostgreSQL"""

        logger.info(f"{'DRY RUN: ' if dry_run else ''}Loading batch {batch_id}")

        # Download validation report
        report_key = f"staging/validated/{batch_id}/validation_report.json"
        report_obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=report_key)
        report = json.loads(report_obj["Body"].read())

        table_name = report["table_name"]
        logger.info(f"Target table: {table_name}")
        logger.info(f"Row count: {report['row_count']}")

        # Download data files using boto3
        data_key = f"staging/validated/{batch_id}/{table_name}.csv"
        local_ids_key = f"staging/validated/{batch_id}/local_subject_ids.csv"

        # Read CSVs directly from S3 using boto3
        data_obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=data_key)
        data_df = pd.read_csv(data_obj['Body'])

        local_ids_obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=local_ids_key)
        local_ids_df = pd.read_csv(local_ids_obj['Body'])

        logger.info(f"Loaded {len(data_df)} sample records")
        logger.info(f"Loaded {len(local_ids_df)} local ID mappings")

        if dry_run:
            logger.info("DRY RUN - No changes will be made")
            logger.info(f"Would insert {len(local_ids_df[local_ids_df['action'] == 'create_new'])} new subjects")
            logger.info(f"Would insert {len(local_ids_df)} local ID mappings")
            logger.info(f"Would upsert {len(data_df)} records to {table_name}")
            return

        conn = self.get_db_connection()
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
            self.return_db_connection(conn)

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
                [(row["global_subject_id"], row["center_id"], current_year) 
                 for _, row in subjects.iterrows()],
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

                if existing and existing["global_subject_id"] != row["global_subject_id"]:
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
                    (row["center_id"], row["local_subject_id"], 
                     row["identifier_type"], row["global_subject_id"]),
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
        missing_columns = [col for col in data_df.columns if col not in db_columns and col not in ['created_at', 'updated_at']]

        if missing_columns:
            logger.warning(f"Skipping columns not in table schema: {missing_columns}")

        logger.info(f"Inserting columns: {available_columns}")

        # Build upsert query
        update_cols = [col for col in available_columns if col != pk_col]
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

        query = f"""
            INSERT INTO {table_name} ({','.join(available_columns)})
            VALUES %s
            ON CONFLICT ({pk_col})
            DO UPDATE SET {update_clause}
        """

        # Execute in batches
        with conn.cursor() as cur:
            execute_values(
                cur,
                query,
                [tuple(row[col] if pd.notna(row[col]) else None for col in available_columns)
                 for _, row in data_df.iterrows()],
                page_size=1000,
            )

        logger.info(f"✓ Upserted {len(data_df)} records to {table_name}")
        return len(data_df)

    def run(self, batch_id: str, dry_run: bool = True):
        """Execute loader"""
        try:
            self.load_batch(batch_id, dry_run)
        finally:
            if self.db_pool:
                self.db_pool.closeall()


def main():
    parser = argparse.ArgumentParser(description="Load validated fragments to PostgreSQL")
    parser.add_argument("--batch-id", required=True, help="Batch ID to load")
    parser.add_argument("--approve", action="store_true", help="Execute load (default is dry-run)")
    args = parser.parse_args()

    loader = TableLoader()
    loader.run(args.batch_id, dry_run=not args.approve)


if __name__ == "__main__":
    main()
