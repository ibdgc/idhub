import os
import sys
import argparse
import pandas as pd
import boto3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Dict, Optional
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/loader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TableLoader:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = os.getenv('S3_BUCKET', 'idhub-curated-fragments')
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }

        self.table_pks = {
            'lcl': 'knumber',
            'dna': 'sample_id',
            'blood': 'sample_id',
            'genotyping': 'genotype_id',
            'local_subject_ids': ['center_id', 'local_subject_id', 'identifier_type']
        }

    def load_validated_batch(self, batch_id: str):
        """Load validated batch from staging/validated/"""

        staging_prefix = f"staging/validated/{batch_id}/"

        logger.info(f"Loading validated batch: {batch_id}")

        # Check metadata
        try:
            obj = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=f"{staging_prefix}metadata.json"
            )
            metadata = json.load(obj['Body'])
        except:
            raise ValueError(f"Batch {batch_id} not found in staging/validated/")

        # List all CSV files
        response = self.s3_client.list_objects_v2(
            Bucket=self.s3_bucket,
            Prefix=staging_prefix
        )

        results = {}

        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                table_name = key.split('/')[-1].replace('.csv', '')

                try:
                    rows = self._load_table(table_name, key)
                    results[table_name] = {'status': 'success', 'rows': rows}

                    # Archive to curated-tables
                    self._archive_to_curated(table_name, key, batch_id)

                except Exception as e:
                    results[table_name] = {'status': 'error', 'error': str(e)}
                    logger.error(f"Failed to load {table_name}: {e}")

        # Move batch to loaded/
        self._move_to_loaded(batch_id)

        # Print summary
        self._print_summary(batch_id, results)

        return results

    def _load_table(self, table_name: str, s3_key: str) -> int:
        """Load single table from S3"""

        logger.info(f"Loading {table_name} from {s3_key}")

        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
        df = pd.read_csv(obj['Body'])

        conn = psycopg2.connect(**self.db_config)
        try:
            cur = conn.cursor()

            columns = df.columns.tolist()
            values = [tuple(row) for row in df.values]

            conflict_clause = self._get_conflict_clause(table_name, columns)
            query = f"""
                INSERT INTO {table_name} ({','.join(columns)})
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

    def load_batch(self, batch_id: str) -> dict:
        """Load validated batch into database"""
    
        batch_path = f"staging/validated/{batch_id}"
    
        logger.info(f"Loading validated batch: {batch_id}")
    
        # Load validation report
        report = self._load_report(batch_path)
        table_name = report['table_name']
    
        results = {}
    
        try:
            # Load main table
            row_count = self._load_table(batch_path, table_name)
            results[table_name] = {'status': 'success', 'rows': row_count}
            logger.info(f"✓ Loaded {row_count} rows into {table_name}")
    
            # Archive main table
            self._archive_table(batch_path, table_name, batch_id)
    
        except Exception as e:
            results[table_name] = {'status': 'error', 'message': str(e)}
            logger.error(f"Failed to load {table_name}: {e}")
    
        try:
            # Load local_subject_ids with special logic
            row_count = self._load_local_subject_ids(batch_path)
            results['local_subject_ids'] = {'status': 'success', 'rows': row_count}
            logger.info(f"✓ Loaded {row_count} rows into local_subject_ids")
    
        except Exception as e:
            results['local_subject_ids'] = {'status': 'error', 'message': str(e)}
            logger.error(f"Failed to load local_subject_ids: {e}")
    
        # Move batch to loaded
        self._move_to_loaded(batch_id)
    
        self._print_summary(batch_id, results)
    
        return results
    
    def _load_local_subject_ids(self, batch_path: str) -> int:
        """Load local_subject_ids with center promotion logic"""
    
        local_ids_key = f"{batch_path}/local_subject_ids.csv"
    
        try:
            obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=local_ids_key)
            df = pd.read_csv(obj['Body'])
    
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
    
            # Prepare records
            records = [
                (row['center_id'], row['local_subject_id'], row['identifier_type'], row['global_subject_id'])
                for _, row in df.iterrows()
            ]
    
            # Insert with center promotion logic
            cursor.executemany("""
                INSERT INTO local_subject_ids 
                  (center_id, local_subject_id, identifier_type, global_subject_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (center_id, local_subject_id, identifier_type) 
                DO UPDATE SET 
                  center_id = CASE 
                    WHEN local_subject_ids.center_id = 0 AND EXCLUDED.center_id != 0 
                    THEN EXCLUDED.center_id
                    ELSE local_subject_ids.center_id
                  END,
                  global_subject_id = COALESCE(EXCLUDED.global_subject_id, local_subject_ids.global_subject_id)
            """, records)
    
            conn.commit()
            row_count = len(records)
    
            cursor.close()
            conn.close()
    
            return row_count
    
        except Exception as e:
            logger.error(f"Failed to load local_subject_ids: {e}")
            raise

    def _get_conflict_clause(self, table_name: str, columns: list) -> str:
        """Generate ON CONFLICT clause"""
        pk = self.table_pks.get(table_name)

        if not pk:
            return "ON CONFLICT DO NOTHING"

        if isinstance(pk, list):
            pk_clause = f"({', '.join(pk)})"
            update_cols = [c for c in columns if c not in pk and c != 'created_at']
        else:
            pk_clause = f"({pk})"
            update_cols = [c for c in columns if c != pk and c != 'created_at']

        if not update_cols:
            return f"ON CONFLICT {pk_clause} DO NOTHING"

        updates = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        return f"ON CONFLICT {pk_clause} DO UPDATE SET {updates}"

    def _archive_to_curated(self, table_name: str, source_key: str, batch_id: str):
        """Archive to curated-tables/"""
        dest_key = f"curated-tables/{table_name}/{table_name}_{batch_id}.csv"

        self.s3_client.copy_object(
            Bucket=self.s3_bucket,
            CopySource={'Bucket': self.s3_bucket, 'Key': source_key},
            Key=dest_key
        )

        logger.info(f"Archived to {dest_key}")

    def _move_to_loaded(self, batch_id: str):
        """Move batch from validated/ to loaded/"""

        src_prefix = f"staging/validated/{batch_id}/"
        dest_prefix = f"staging/loaded/{batch_id}/"

        response = self.s3_client.list_objects_v2(
            Bucket=self.s3_bucket,
            Prefix=src_prefix
        )

        for obj in response.get('Contents', []):
            src_key = obj['Key']
            dest_key = src_key.replace('validated/', 'loaded/')

            self.s3_client.copy_object(
                Bucket=self.s3_bucket,
                CopySource={'Bucket': self.s3_bucket, 'Key': src_key},
                Key=dest_key
            )

            self.s3_client.delete_object(Bucket=self.s3_bucket, Key=src_key)

        logger.info(f"Moved batch to staging/loaded/{batch_id}/")

    def _print_summary(self, batch_id: str, results: Dict):
        """Print load summary"""
        print("\n" + "="*70)
        print(f"BATCH LOAD COMPLETE - {batch_id}")
        print("="*70)

        for table, result in results.items():
            if result['status'] == 'success':
                print(f"✓ {table}: {result['rows']} rows loaded")
            else:
                print(f"✗ {table}: {result['error']}")

        print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Load validated batches into database')
    parser.add_argument('batch_id', help='Batch ID from staging/validated/')

    args = parser.parse_args()

    loader = TableLoader()

    try:
        loader.load_validated_batch(args.batch_id)
        logger.info("✓ Load complete")

    except Exception as e:
        logger.error(f"✗ Load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
