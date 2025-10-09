# table-loader/main.py

import argparse
import json
import logging
import os
import sys
from typing import Dict

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class TableLoader:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.s3_bucket = os.getenv("S3_BUCKET")
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }

        self.table_pks = {
            "lcl": "knumber",
            "dna": "sample_id",
            "blood": "sample_id",
            "wgs": "sample_id",
            "immunochip": "sample_id",
            "bge": "sample_id",
            "exomechip": "sample_id",
            "gwas2": "sample_id",
            "plasma": "sample_id",
            "enteroid": "sample_id",
            "olink": "sample_id",
            "rnaseq": "sample_id",
            "wes": "seq_id",
            "genotyping": "genotype_id",
        }

    def load_batch(self, batch_id: str) -> dict:
        """Load validated batch into database"""

        batch_path = f"staging/validated/{batch_id}"

        logger.info(f"Loading validated batch: {batch_id}")

        # Load validation report
        try:
            obj = self.s3_client.get_object(
                Bucket=self.s3_bucket, Key=f"{batch_path}/validation_report.json"
            )
            report = json.load(obj["Body"])
            table_name = report["table_name"]
        except Exception as e:
            raise ValueError(f"Batch {batch_id} not found or invalid: {e}")

        results = {}

        # Load main table
        try:
            row_count = self._load_table(table_name, f"{batch_path}/{table_name}.csv")
            results[table_name] = {"status": "success", "rows": row_count}
            logger.info(f"✓ Loaded {row_count} rows into {table_name}")
        except Exception as e:
            results[table_name] = {"status": "error", "message": str(e)}
            logger.error(f"Failed to load {table_name}: {e}")

        # Load local_subject_ids
        try:
            row_count = self._load_local_subject_ids(
                f"{batch_path}/local_subject_ids.csv"
            )
            results["local_subject_ids"] = {"status": "success", "rows": row_count}
            logger.info(f"✓ Loaded {row_count} local_subject_ids")
        except Exception as e:
            results["local_subject_ids"] = {"status": "error", "message": str(e)}
            logger.error(f"Failed to load local_subject_ids: {e}")

        # Move batch to loaded
        self._move_to_loaded(batch_id)

        self._print_summary(batch_id, results)

        return results

    def _load_table(self, table_name: str, s3_key: str) -> int:
        """Load single table from S3"""

        logger.info(f"Loading {table_name} from {s3_key}")

        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
        df = pd.read_csv(obj["Body"])

        conn = psycopg2.connect(**self.db_config)
        try:
            cur = conn.cursor()

            columns = df.columns.tolist()
            values = [tuple(row) for row in df.values]

            conflict_clause = self._get_conflict_clause(table_name, columns)
            query = f"""
                INSERT INTO {table_name} ({",".join(columns)})
                VALUES %s
                {conflict_clause}
            """

            execute_values(cur, query, values)
            rows_affected = cur.rowcount
            conn.commit()

            logger.info(f"✓ Loaded {rows_affected} rows into {table_name}")
            return rows_affected

        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _load_local_subject_ids(self, s3_key: str) -> int:
        """Load local_subject_ids with deduplication"""

        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
        df = pd.read_csv(obj["Body"])

        # Deduplicate
        df = df.drop_duplicates(
            subset=["center_id", "local_subject_id", "identifier_type"]
        )

        conn = psycopg2.connect(**self.db_config)
        try:
            cur = conn.cursor()

            records = [
                (
                    int(row["center_id"]),
                    str(row["local_subject_id"]),
                    str(row["identifier_type"]),
                    str(row["global_subject_id"]),
                )
                for _, row in df.iterrows()
            ]

            execute_values(
                cur,
                """
                INSERT INTO local_subject_ids 
                  (center_id, local_subject_id, identifier_type, global_subject_id)
                VALUES %s
                ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                """,
                records,
            )

            rows_affected = cur.rowcount
            conn.commit()

            return rows_affected

        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _get_conflict_clause(self, table_name: str, columns: list) -> str:
        """Generate ON CONFLICT clause"""
        pk = self.table_pks.get(table_name)

        if not pk:
            return "ON CONFLICT DO NOTHING"

        pk_clause = f"({pk})"
        update_cols = [
            c for c in columns if c != pk and c not in ["created_at", "global_subject_id"]
        ]

        if not update_cols:
            return f"ON CONFLICT {pk_clause} DO NOTHING"

        updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        return f"ON CONFLICT {pk_clause} DO UPDATE SET {updates}"

    def _move_to_loaded(self, batch_id: str):
        """Move batch from validated/ to loaded/"""

        src_prefix = f"staging/validated/{batch_id}/"
        dest_prefix = f"staging/loaded/{batch_id}/"

        response = self.s3_client.list_objects_v2(
            Bucket=self.s3_bucket, Prefix=src_prefix
        )

        for obj in response.get("Contents", []):
            src_key = obj["Key"]
            dest_key = src_key.replace("validated/", "loaded/")

            self.s3_client.copy_object(
                Bucket=self.s3_bucket,
                CopySource={"Bucket": self.s3_bucket, "Key": src_key},
                Key=dest_key,
            )

            self.s3_client.delete_object(Bucket=self.s3_bucket, Key=src_key)

        logger.info(f"Moved batch to staging/loaded/{batch_id}/")

    def _print_summary(self, batch_id: str, results: Dict):
        """Print load summary"""
        print("\n" + "=" * 70)
        print(f"BATCH LOAD COMPLETE - {batch_id}")
        print("=" * 70)

        for table, result in results.items():
            if result["status"] == "success":
                print(f"✓ {table}: {result['rows']} rows loaded")
            else:
                print(f"✗ {table}: {result.get('message', 'Unknown error')}")

        print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Load validated batches into database")
    parser.add_argument("batch_id", help="Batch ID from staging/validated/")

    args = parser.parse_args()

    loader = TableLoader()

    try:
        loader.load_batch(args.batch_id)
        logger.info("✓ Load complete")

    except Exception as e:
        logger.error(f"✗ Load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
