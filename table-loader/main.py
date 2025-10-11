# table-loader/main.py

import argparse
import json
import logging
import os
import sys
from typing import Dict

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class TableLoader:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.s3_bucket = os.getenv("S3_BUCKET", "idhub-curated-fragments")

        # NocoDB configuration
        self.nocodb_url = os.getenv("NOCODB_URL")
        self.nocodb_token = os.getenv("NOCODB_API_TOKEN")
        self.nocodb_base = os.getenv("NOCODB_BASE_ID")

        if not all([self.nocodb_url, self.nocodb_token]):
            raise ValueError(
                "Missing required environment variables: NOCODB_URL, NOCODB_API_TOKEN"
            )

        # Cache for table IDs
        self._base_id_cache = None
        self._table_id_cache = {}

    def _get_base_id(self) -> str:
        """Get base ID (auto-detect if not provided, cached after first call)"""
        if self._base_id_cache:
            return self._base_id_cache

        if self.nocodb_base:
            self._base_id_cache = self.nocodb_base
            logger.info(f"Using provided NocoDB base ID: {self.nocodb_base}")
            return self._base_id_cache

        # Auto-detect base ID
        url = f"{self.nocodb_url}/api/v2/meta/bases"
        headers = {"xc-token": self.nocodb_token}

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        bases = response.json().get("list", [])

        if not bases:
            raise ValueError("No NocoDB bases found")

        base_id = bases[0]["id"]
        base_title = bases[0].get("title", "Unknown")
        self._base_id_cache = base_id

        logger.info(f"Auto-detected NocoDB base: '{base_title}' (ID: {base_id})")
        return base_id

    def _get_table_id(self, table_name: str) -> str:
        """Get table ID by name (cached after first lookup)"""
        if table_name in self._table_id_cache:
            return self._table_id_cache[table_name]

        base_id = self._get_base_id()
        url = f"{self.nocodb_url}/api/v2/meta/bases/{base_id}/tables"
        headers = {"xc-token": self.nocodb_token}

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = response.json().get("list", [])

        table = next((t for t in tables if t["table_name"] == table_name), None)

        if not table:
            raise ValueError(f"Table '{table_name}' not found in NocoDB base")

        table_id = table["id"]
        self._table_id_cache[table_name] = table_id

        logger.info(f"Found table '{table_name}' (ID: {table_id})")
        return table_id

    def load_batch(self, batch_id: str, dry_run: bool = False) -> dict:
        """Load validated batch into database via NocoDB API"""

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

        if dry_run:
            logger.info("DRY RUN MODE - No data will be loaded")

        results = {}

        # Load main table
        try:
            row_count = self._load_table(
                table_name, f"{batch_path}/{table_name}.csv", dry_run
            )
            results[table_name] = {"status": "success", "rows": row_count}
            logger.info(
                f"✓ {'Would load' if dry_run else 'Loaded'} {row_count} rows into {table_name}"
            )
        except Exception as e:
            results[table_name] = {"status": "error", "message": str(e)}
            logger.error(f"Failed to load {table_name}: {e}")
            raise

        # Load local_subject_ids
        try:
            row_count = self._load_local_subject_ids(
                f"{batch_path}/local_subject_ids.csv", dry_run
            )
            results["local_subject_ids"] = {"status": "success", "rows": row_count}
            logger.info(
                f"✓ {'Would load' if dry_run else 'Loaded'} {row_count} local_subject_ids"
            )
        except Exception as e:
            results["local_subject_ids"] = {"status": "error", "message": str(e)}
            logger.error(f"Failed to load local_subject_ids: {e}")

        # Move batch to loaded (skip in dry run)
        if not dry_run:
            self._move_to_loaded(batch_id)

        self._print_summary(batch_id, results, dry_run)

        return results

    def _load_table(self, table_name: str, s3_key: str, dry_run: bool = False) -> int:
        """Load single table from S3 via NocoDB API with upsert logic"""

        logger.info(f"Loading {table_name} from {s3_key}")

        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
        df = pd.read_csv(obj["Body"])

        # Separate FK columns from data
        fk_data = {}
        if "global_subject_id" in df.columns:
            fk_data["global_subject_id"] = df["global_subject_id"].copy()
            df = df.drop(columns=["global_subject_id"])

        # Exclude system columns
        system_columns = ["created_at", "updated_at", "Id"]
        df = df.drop(columns=[col for col in system_columns if col in df.columns])

        if dry_run:
            return len(df)

        table_id = self._get_table_id(table_name)
        records_url = f"{self.nocodb_url}/api/v2/tables/{table_id}/records"
        headers = {"xc-token": self.nocodb_token, "Content-Type": "application/json"}

        # Get primary key field for upsert
        pk_field = self._get_primary_key_field(table_name)
        logger.info(f"Primary key field: {pk_field}")

        # Convert DataFrame to list of dicts
        records = df.replace({pd.NA: None, pd.NaT: None}).to_dict("records")

        # Bulk insert with conflict handling
        batch_size = 1000
        total_loaded = 0
        inserted_records = []

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            # Try bulk insert first
            response = requests.post(
                records_url, headers=headers, json=batch, timeout=120
            )

            if response.status_code in [200, 201]:
                inserted_records.extend(response.json())
                total_loaded += len(batch)
                logger.info(
                    f"Loaded batch {i // batch_size + 1}: {total_loaded}/{len(records)} records"
                )
            elif response.status_code == 400 and "already exists" in response.text:
                # Handle duplicates - try individual upserts
                logger.warning(f"Batch has duplicates, trying individual upserts...")
                for record in batch:
                    pk_value = record.get(pk_field)

                    # Try to update existing record
                    update_url = f"{records_url}/{pk_value}"
                    update_response = requests.patch(
                        update_url, headers=headers, json=record, timeout=30
                    )

                    if update_response.status_code in [200, 201]:
                        inserted_records.append(update_response.json())
                        total_loaded += 1
                    elif update_response.status_code == 404:
                        # Record doesn't exist, insert it
                        insert_response = requests.post(
                            records_url, headers=headers, json=[record], timeout=30
                        )
                        if insert_response.status_code in [200, 201]:
                            inserted_records.extend(insert_response.json())
                            total_loaded += 1
                        else:
                            logger.warning(
                                f"Failed to insert {pk_value}: {insert_response.text}"
                            )
                    else:
                        logger.warning(
                            f"Failed to update {pk_value}: {update_response.text}"
                        )

                logger.info(
                    f"Processed batch {i // batch_size + 1}: {total_loaded}/{len(records)} records"
                )
            else:
                logger.error(
                    f"NocoDB API error: {response.status_code} - {response.text}"
                )
                response.raise_for_status()

        # Link foreign keys if needed
        if fk_data and inserted_records:
            self._link_foreign_keys(table_id, table_name, inserted_records, fk_data)

        return total_loaded

    def _get_primary_key_field(self, table_name: str) -> str:
        """Get the primary key field name for a table"""

        table_id = self._get_table_id(table_name)
        url = f"{self.nocodb_url}/api/v2/meta/tables/{table_id}"
        headers = {"xc-token": self.nocodb_token}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        columns = response.json().get("columns", [])

        for col in columns:
            if col.get("pk"):
                return col.get("column_name")

        raise ValueError(f"No primary key found for table {table_name}")

    def _link_foreign_keys(
        self, table_id: str, table_name: str, inserted_records: list, fk_data: dict
    ):
        """Link foreign key relationships using NocoDB Links API"""

        if "global_subject_id" not in fk_data:
            return

        logger.info("Linking global_subject_id to subjects table...")

        # Get set of valid GSIDs
        valid_gsids = self._get_subject_id_map()

        # Get primary key of blood table to identify records
        pk_field = self._get_primary_key_field(table_name)

        # Link column ID for blood->subjects relationship
        link_column_id = "cl4v00vn975ouzw"

        linked_count = 0
        failed_count = 0

        for idx, record in enumerate(inserted_records):
            pk_value = record.get(pk_field)
            gsid = fk_data["global_subject_id"].iloc[idx]

            if pd.isna(gsid) or not gsid:
                continue

            gsid_str = str(gsid).strip()

            # Check if GSID exists in subjects table
            if gsid_str not in valid_gsids:
                if failed_count < 10:
                    logger.warning(f"Subject not found for GSID '{gsid_str}'")
                failed_count += 1
                continue

            # Use Links API - link by primary key value
            link_url = f"{self.nocodb_url}/api/v2/tables/{table_id}/links/{link_column_id}/records/{pk_value}"
            headers = {
                "xc-token": self.nocodb_token,
                "Content-Type": "application/json",
            }

            try:
                # Pass the GSID (primary key of subjects table) as the link value
                response = requests.post(
                    link_url, headers=headers, json=[gsid_str], timeout=30
                )

                if response.status_code in [200, 201]:
                    linked_count += 1
                else:
                    if failed_count < 10:
                        logger.warning(
                            f"Failed to link {pk_value}: {response.status_code} - {response.text}"
                        )
                    failed_count += 1
            except Exception as e:
                if failed_count < 10:
                    logger.warning(f"Error linking {pk_value}: {e}")
                failed_count += 1

            if (idx + 1) % 1000 == 0:
                logger.info(f"Linked {linked_count}/{idx + 1} records...")

        logger.info(f"✓ Linked {linked_count} records, {failed_count} failed")

    def _get_subject_id_map(self) -> dict:
        """Build map of global_subject_id -> global_subject_id (it's the PK)"""

        subjects_table_id = self._get_table_id("subjects")
        records_url = f"{self.nocodb_url}/api/v2/tables/{subjects_table_id}/records"
        headers = {"xc-token": self.nocodb_token}

        gsid_set = set()
        offset = 0
        limit = 1000

        logger.info("Loading subjects for FK mapping...")

        while True:
            response = requests.get(
                records_url,
                headers=headers,
                params={
                    "limit": limit,
                    "offset": offset,
                    "fields": "global_subject_id",
                },
                timeout=60,
            )
            response.raise_for_status()

            data = response.json()
            records = data.get("list", [])

            if not records:
                break

            for record in records:
                gsid = record.get("global_subject_id")
                if gsid:
                    gsid_set.add(str(gsid))

            offset += limit
            logger.info(f"Loaded {len(gsid_set)} subject GSIDs...")

            if len(records) < limit:
                break

        logger.info(f"✓ Loaded {len(gsid_set)} subject GSIDs")

        # Return a map where GSID maps to itself (it's the PK)
        return {gsid: gsid for gsid in gsid_set}

    def _load_local_subject_ids(self, s3_key: str, dry_run: bool = False) -> int:
        """Load local_subject_ids with deduplication via NocoDB API"""

        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
        df = pd.read_csv(obj["Body"])

        # Deduplicate
        df = df.drop_duplicates(
            subset=["center_id", "local_subject_id", "identifier_type"]
        )

        if dry_run:
            return len(df)

        table_id = self._get_table_id("local_subject_ids")
        records_url = f"{self.nocodb_url}/api/v2/tables/{table_id}/records"
        headers = {"xc-token": self.nocodb_token, "Content-Type": "application/json"}

        # Convert to records
        records = df.replace({pd.NA: None, pd.NaT: None}).to_dict("records")

        # Load in batches
        batch_size = 1000
        total_loaded = 0
        skipped = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            # NocoDB doesn't have native upsert, so we need to check for conflicts
            # For now, we'll try to insert and catch conflicts
            try:
                response = requests.post(
                    records_url, headers=headers, json=batch, timeout=120
                )

                if response.status_code in [200, 201]:
                    total_loaded += len(batch)
                elif response.status_code == 400:
                    # Likely duplicate key conflict - try one by one
                    for record in batch:
                        try:
                            resp = requests.post(
                                records_url, headers=headers, json=[record], timeout=30
                            )
                            if resp.status_code in [200, 201]:
                                total_loaded += 1
                            else:
                                skipped += 1
                        except:
                            skipped += 1
                else:
                    logger.error(
                        f"NocoDB API error: {response.status_code} - {response.text}"
                    )
                    response.raise_for_status()

                logger.info(
                    f"Loaded batch {i // batch_size + 1}: {total_loaded}/{len(records)} records ({skipped} skipped)"
                )

            except Exception as e:
                logger.warning(f"Batch insert failed, trying individual inserts: {e}")
                for record in batch:
                    try:
                        resp = requests.post(
                            records_url, headers=headers, json=[record], timeout=30
                        )
                        if resp.status_code in [200, 201]:
                            total_loaded += 1
                        else:
                            skipped += 1
                    except:
                        skipped += 1

        logger.info(f"Total loaded: {total_loaded}, skipped (duplicates): {skipped}")
        return total_loaded

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

    def _print_summary(self, batch_id: str, results: Dict, dry_run: bool = False):
        """Print load summary"""
        print("\n" + "=" * 70)
        print(f"BATCH LOAD {'PREVIEW' if dry_run else 'COMPLETE'} - {batch_id}")
        print("=" * 70)

        for table, result in results.items():
            if result["status"] == "success":
                action = "Would load" if dry_run else "Loaded"
                print(f"✓ {table}: {action} {result['rows']} rows")
            else:
                print(f"✗ {table}: {result.get('message', 'Unknown error')}")

        print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Load validated batches into database")
    parser.add_argument(
        "--batch-id", required=True, help="Batch ID from staging/validated/"
    )
    parser.add_argument(
        "--approve", action="store_true", help="Actually load data (default is dry run)"
    )

    args = parser.parse_args()

    loader = TableLoader()

    try:
        dry_run = not args.approve
        loader.load_batch(args.batch_id, dry_run=dry_run)

        if dry_run:
            logger.info("✓ Dry run complete - use --approve to actually load data")
        else:
            logger.info("✓ Load complete")

    except Exception as e:
        logger.error(f"✗ Load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
