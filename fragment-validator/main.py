# fragment-validator/main.py

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import boto3
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

# Load .env file from current directory or parent directories
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FragmentValidator:
    def __init__(
        self,
        s3_bucket: str,
        gsid_service_url: str,
        gsid_api_key: str,
        nocodb_config: dict,
    ):
        self.s3_bucket = s3_bucket
        self.gsid_service_url = gsid_service_url
        self.gsid_api_key = gsid_api_key
        self.s3_client = boto3.client("s3")

        # NocoDB configuration
        self.nocodb_url = nocodb_config["url"]
        self.nocodb_token = nocodb_config["token"]
        self.nocodb_base = nocodb_config.get("base")  # Optional - will auto-detect

        # Cache for base and table IDs
        self._base_id_cache = None
        self._table_id_cache = {}

        self.local_id_cache = {}
        self._load_local_id_cache()

    def _get_base_id(self) -> str:
        """Get base ID (auto-detect if not provided, cached after first call)"""
        if self._base_id_cache:
            return self._base_id_cache

        # Use provided base ID if available
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

        # Use first base (or you could match by name if needed)
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

        # Find table by name
        table = next((t for t in tables if t["table_name"] == table_name), None)

        if not table:
            raise ValueError(f"Table '{table_name}' not found in NocoDB base")

        table_id = table["id"]
        self._table_id_cache[table_name] = table_id

        logger.info(f"Found table '{table_name}' (ID: {table_id})")

        return table_id

    def _load_local_id_cache(self):
        """Pre-load all local_subject_ids into memory for fast lookups via NocoDB API"""
        try:
            logger.info("Loading local_subject_ids cache from NocoDB...")

            table_id = self._get_table_id("local_subject_ids")
            records_url = f"{self.nocodb_url}/api/v2/tables/{table_id}/records"
            headers = {"xc-token": self.nocodb_token}

            offset = 0
            limit = 1000
            total_loaded = 0

            while True:
                response = requests.get(
                    records_url,
                    headers=headers,
                    params={"limit": limit, "offset": offset},
                )
                response.raise_for_status()
                data = response.json()

                records = data.get("list", [])
                if not records:
                    break

                for record in records:
                    key = (
                        record["center_id"],
                        record["local_subject_id"],
                        record["identifier_type"],
                    )
                    self.local_id_cache[key] = record["global_subject_id"]
                    total_loaded += 1

                offset += limit

                # Check pagination
                page_info = data.get("pageInfo", {})
                if page_info.get("isLastPage", True):
                    break

            logger.info(f"Loaded {total_loaded} unique local IDs into cache")

        except Exception as e:
            logger.error(f"Failed to load local ID cache: {e}")
            raise

    def process_local_file(
        self,
        table_name: str,
        local_file_path: str,
        mapping_config: dict,
        source_name: str,
        auto_approve: bool = False,
    ) -> dict:
        """Process local CSV file through validation pipeline"""

        logger.info(f"Processing {local_file_path} for table {table_name}")

        raw_data = pd.read_csv(local_file_path)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"incoming/{table_name}/{table_name}_{timestamp}.csv"

        logger.info(f"Uploading to s3://{self.s3_bucket}/{s3_key}")
        self.s3_client.put_object(
            Bucket=self.s3_bucket, Key=s3_key, Body=raw_data.to_csv(index=False)
        )

        # Apply mapping
        mapped_data = self._apply_mapping(raw_data, mapping_config)

        # Validate schema
        validation_errors = self._validate_schema(mapped_data, table_name)

        # Resolve subject IDs
        resolution_results = self._resolve_subject_ids(
            mapped_data,
            mapping_config.get("subject_id_candidates", []),
            mapping_config.get("center_id_field"),
        )

        # Generate batch ID
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Prepare report
        report = {
            "batch_id": batch_id,
            "table_name": table_name,
            "source_name": source_name,
            "timestamp": datetime.now().isoformat(),
            "input_file": local_file_path,
            "s3_location": s3_key,  # NEW: Added S3 location
            "row_count": len(mapped_data),
            "validation_errors": validation_errors,
            "resolution_summary": resolution_results["summary"],
            "warnings": resolution_results["warnings"],
            "auto_approved": auto_approve,
        }

        # Check if validation passed
        if validation_errors:
            report["status"] = "FAILED"
            logger.error(f"✗ Validation failed with {len(validation_errors)} errors")
            self._print_summary(report)
            return report

        # Add resolved GSIDs to data
        mapped_data["global_subject_id"] = resolution_results["gsids"]

        # Write outputs to staging
        self._write_staging_outputs(
            batch_id,
            table_name,
            mapped_data,
            resolution_results["local_id_records"],
            report,
        )

        report["status"] = "VALIDATED"
        report["staging_location"] = (
            f"s3://{self.s3_bucket}/staging/validated/{batch_id}/"
        )

        logger.info(f"✓ Validation complete: {batch_id}")
        self._print_summary(report)

        return report

    def _apply_mapping(
        self, raw_data: pd.DataFrame, mapping_config: dict
    ) -> pd.DataFrame:
        """Apply field mapping from config"""

        field_map = mapping_config.get("field_mapping", {})
        subject_id_candidates = mapping_config.get("subject_id_candidates", [])
        center_id_field = mapping_config.get("center_id_field")

        mapped_data = pd.DataFrame()

        # Map explicitly defined fields
        for target_field, source_field in field_map.items():
            if source_field in raw_data.columns:
                mapped_data[target_field] = raw_data[source_field]
            else:
                logger.warning(f"Source field '{source_field}' not found in input data")
                mapped_data[target_field] = None

        # Auto-include subject ID candidate fields if not already mapped
        for candidate in subject_id_candidates:
            if candidate not in mapped_data.columns and candidate in raw_data.columns:
                mapped_data[candidate] = raw_data[candidate]
                logger.info(f"Auto-included subject ID candidate field: {candidate}")

        # Auto-include center_id field if specified and not already mapped
        if (
            center_id_field
            and center_id_field not in mapped_data.columns
            and center_id_field in raw_data.columns
        ):
            mapped_data[center_id_field] = raw_data[center_id_field]
            logger.info(f"Auto-included center_id field: {center_id_field}")

        return mapped_data

    def _validate_schema(self, data: pd.DataFrame, table_name: str) -> List[dict]:
        """Validate data against target table schema via NocoDB API"""
        errors = []

        try:
            table_id = self._get_table_id(table_name)

            # Get columns for this table
            columns_url = f"{self.nocodb_url}/api/v2/meta/tables/{table_id}/columns"
            headers = {"xc-token": self.nocodb_token}

            response = requests.get(columns_url, headers=headers)
            response.raise_for_status()
            columns = response.json().get("list", [])

            # Check required columns exist
            for col in columns:
                col_name = col["column_name"]

                # Skip auto-generated columns
                if col_name in ["created_at", "updated_at", "global_subject_id"]:
                    continue

                is_required = col.get("rqd", False)

                if col_name not in data.columns and is_required:
                    errors.append(
                        {
                            "type": "missing_required_column",
                            "column": col_name,
                            "message": f"Required column '{col_name}' not found in data",
                        }
                    )

            # Check for null values in NOT NULL columns
            for col in columns:
                col_name = col["column_name"]

                if col_name in ["created_at", "updated_at", "global_subject_id"]:
                    continue

                is_required = col.get("rqd", False)

                if col_name in data.columns and is_required:
                    null_count = data[col_name].isna().sum()
                    if null_count > 0:
                        errors.append(
                            {
                                "type": "null_in_required_column",
                                "column": col_name,
                                "null_count": int(null_count),
                                "message": f"Column '{col_name}' has {null_count} null values but is NOT NULL",
                            }
                        )

        except Exception as e:
            errors.append({"type": "schema_validation_error", "message": str(e)})

        return errors

    def _resolve_subject_ids(
        self,
        data: pd.DataFrame,
        candidate_fields: List[str],
        center_id_field: Optional[str] = None,
    ) -> dict:
        """Resolve using GSID service for consistency"""

        gsids = []
        local_id_records = []
        warnings = []
        stats = {
            "existing_matches": 0,
            "new_gsids_minted": 0,
            "unknown_center_used": 0,
            "center_promoted": 0,
        }

        for idx, row in data.iterrows():
            # Handle center_id - default to 0 (Unknown) if not provided
            if (
                center_id_field
                and center_id_field in row
                and pd.notna(row[center_id_field])
            ):
                center_id = int(row[center_id_field])
            else:
                center_id = 0
                stats["unknown_center_used"] += 1

            # Try each candidate field through GSID service
            found_gsid = None
            attempted_lookups = []

            for field in candidate_fields:
                if field not in row or pd.isna(row[field]):
                    continue

                local_id = str(row[field])
                attempted_lookups.append(f"{field}={local_id}")

                # Use GSID service for resolution
                payload = {
                    "center_id": center_id,
                    "local_subject_id": local_id,
                    "identifier_type": field,
                    "created_by": "fragment_validator",
                }

                try:
                    response = requests.post(
                        f"{self.gsid_service_url}/register",
                        json=payload,
                        headers={"x-api-key": self.gsid_api_key},
                        timeout=30,
                    )
                    response.raise_for_status()
                    result = response.json()

                    found_gsid = result["gsid"]

                    if result["action"] == "create_new":
                        stats["new_gsids_minted"] += 1
                        logger.info(
                            f"Row {idx}: Minted new GSID {found_gsid} for {field}={local_id}"
                        )
                    else:
                        stats["existing_matches"] += 1
                        logger.debug(
                            f"Row {idx}: Matched existing GSID {found_gsid} for {field}={local_id}"
                        )

                    # Record all IDs for this subject
                    for alt_field in candidate_fields:
                        if alt_field in row and pd.notna(row[alt_field]):
                            local_id_records.append(
                                {
                                    "center_id": center_id,
                                    "local_subject_id": str(row[alt_field]),
                                    "identifier_type": alt_field,
                                    "global_subject_id": found_gsid,
                                    "action": result["action"],
                                }
                            )

                    break  # Found match, stop trying candidates

                except requests.exceptions.HTTPError as e:
                    logger.error(
                        f"Row {idx}: GSID API error for {field}={local_id}: {e.response.status_code} - {e.response.text}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"Row {idx}: GSID resolution failed for {field}={local_id}: {e}"
                    )
                    continue

            if not found_gsid:
                logger.error(
                    f"Row {idx}: Failed to resolve subject\n"
                    f"  center_id: {center_id}\n"
                    f"  Attempted lookups: {attempted_lookups}\n"
                    f"  Available candidate fields: {candidate_fields}\n"
                    f"  Row data: {row.to_dict()}"
                )
                raise ValueError(f"Failed to resolve subject at row {idx}")

            gsids.append(found_gsid)

        # Generate warnings
        if stats["unknown_center_used"] > 0:
            warnings.append(
                f"{stats['unknown_center_used']} records used center_id=0 (Unknown)"
            )

        if stats["center_promoted"] > 0:
            warnings.append(
                f"{stats['center_promoted']} records promoted from Unknown to known center"
            )

        return {
            "gsids": gsids,
            "local_id_records": local_id_records,
            "summary": stats,
            "warnings": warnings,
        }

    def _mint_new_gsid(self) -> str:
        """Request new GSID from gsid-service"""
        try:
            # Use /register endpoint with minimal payload
            payload = {
                "center_id": 0,  # Unknown center
                "local_subject_id": f"temp_{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
                "created_by": "fragment_validator",
            }
            response = requests.post(
                f"{self.gsid_service_url}/register",
                json=payload,
                headers={
                    "x-api-key": self.gsid_api_key
                },  # CHANGED: Added API key header
            )
            response.raise_for_status()
            return response.json()["gsid"]
        except Exception as e:
            logger.error(f"Failed to mint GSID: {e}")
            raise

    def _write_staging_outputs(
        self,
        batch_id: str,
        table_name: str,
        data: pd.DataFrame,
        local_id_records: List[dict],
        report: dict,
    ):
        """Write validated data and metadata to staging area"""

        staging_prefix = f"staging/validated/{batch_id}"

        # Write main table data
        table_csv = data.to_csv(index=False)
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=f"{staging_prefix}/{table_name}.csv",
            Body=table_csv,
        )

        # Write local_subject_ids records
        if local_id_records:
            # NEW: Deduplicate local_id_records before writing
            local_ids_df = pd.DataFrame(local_id_records).drop_duplicates(
                subset=["center_id", "local_subject_id", "identifier_type"]
            )
            local_ids_csv = local_ids_df.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=f"{staging_prefix}/local_subject_ids.csv",
                Body=local_ids_csv,
            )

        # Write validation report
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=f"{staging_prefix}/validation_report.json",
            Body=json.dumps(report, indent=2),
        )

        logger.info(
            f"Staging outputs written to s3://{self.s3_bucket}/{staging_prefix}/"
        )

    def _load_from_s3(self, s3_path: str) -> pd.DataFrame:
        """Load CSV from S3"""
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj["Body"])

    def _print_summary(self, report: dict):
        """Print validation summary"""
        print("\n" + "=" * 70)
        print(f"VALIDATION SUMMARY - {report['batch_id']}")
        print("=" * 70)
        print(f"Table: {report['table_name']}")
        print(f"Source: {report['source_name']}")
        print(f"Rows: {report['row_count']}")
        print(f"Status: {report['status']}")

        if report["status"] == "VALIDATED":
            stats = report["resolution_summary"]
            print(f"\nSubject Resolution:")
            print(f"  - Existing matches: {stats['existing_matches']}")
            print(f"  - New GSIDs minted: {stats['new_gsids_minted']}")
            print(f"  - Unknown center used: {stats['unknown_center_used']}")
            print(f"  - Centers promoted: {stats['center_promoted']}")

            if report["warnings"]:
                print(f"\nWarnings:")
                for warning in report["warnings"]:
                    print(f"  ⚠ {warning}")

            print(f"\nStaging: {report['staging_location']}")
        else:
            print(f"\nValidation Errors ({len(report['validation_errors'])}):")
            for error in report["validation_errors"]:
                print(f"  ✗ [{error['type']}] {error['message']}")
                if "column" in error:
                    print(f"    Column: {error['column']}")
                if "null_count" in error:
                    print(f"    Null count: {error['null_count']}")

        print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Validate and stage data fragments")
    parser.add_argument("table_name", help="Target database table name")
    parser.add_argument(
        "input_file", help="Local path to CSV file"
    )  # CHANGED: Now accepts local file path
    parser.add_argument("mapping_config", help="Path to mapping config JSON file")
    parser.add_argument("--source", required=True, help="Source system name")
    parser.add_argument(
        "--auto-approve", action="store_true", help="Auto-approve for loading"
    )

    args = parser.parse_args()

    # Load mapping config
    with open(args.mapping_config, "r") as f:
        mapping_config = json.load(f)

        nocodb_config = {
            "url": os.getenv("NOCODB_URL"),
            "token": os.getenv("NOCODB_API_TOKEN"),
            "base": os.getenv("NOCODB_BASE_ID"),  # Optional - will auto-detect if None
        }

    s3_bucket = os.getenv("S3_BUCKET", "idhub-curated-fragments")
    gsid_service_url = os.getenv("GSID_SERVICE_URL", "https://api.idhub.ibdgc.org")
    gsid_api_key = os.getenv("GSID_API_KEY")

    if not all([nocodb_config["url"], nocodb_config["token"], gsid_api_key]):
        logger.error(
            "Missing required environment variables: NOCODB_URL, NOCODB_API_TOKEN, GSID_API_KEY"
        )
        sys.exit(1)

    if not all(
        [
            nocodb_config["url"],
            nocodb_config["token"],
            nocodb_config["base"],
            gsid_api_key,
        ]
    ):
        logger.error(
            "Missing required environment variables: NOCODB_URL, NOCODB_API_TOKEN, NOCODB_BASE_ID, GSID_API_KEY"
        )
        sys.exit(1)

    try:
        validator = FragmentValidator(
            s3_bucket, gsid_service_url, gsid_api_key, nocodb_config
        )
        report = validator.process_local_file(
            args.table_name,
            args.input_file,
            mapping_config,
            args.source,
            args.auto_approve,
        )

        if report["status"] == "FAILED":
            logger.error("✗ Validation failed")
            sys.exit(1)
        else:
            logger.info("✓ Validation successful")
            sys.exit(0)

    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
