# fragment-validator/main.py

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import boto3
import pandas as pd
import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FragmentValidator:
    def __init__(self, db_config: dict, s3_bucket: str, gsid_service_url: str):
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.gsid_service_url = gsid_service_url
        self.s3_client = boto3.client("s3")

    def process_incoming_file(
        self,
        table_name: str,
        s3_key: str,
        mapping_config: dict,
        source_name: str,
        auto_approve: bool = False,
    ) -> dict:
        """Process incoming fragment file through validation pipeline"""

        logger.info(f"Processing {s3_key} for table {table_name}")

        # Load raw data
        raw_data = self._load_from_s3(f"s3://{self.s3_bucket}/{s3_key}")

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
            "input_file": s3_key,
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
        """Apply field mapping from config, preserving candidate fields"""
    
        field_map = mapping_config.get("field_mapping", {})
        candidate_fields = mapping_config.get("subject_id_candidates", [])
        center_field = mapping_config.get("center_id_field")
    
        mapped_data = pd.DataFrame()
    
        # Map specified fields
        for target_field, source_field in field_map.items():
            if source_field in raw_data.columns:
                mapped_data[target_field] = raw_data[source_field]
            else:
                logger.warning(f"Source field '{source_field}' not found in input data")
                mapped_data[target_field] = None
    
        # Preserve candidate fields for subject resolution
        for field in candidate_fields:
            if field in raw_data.columns and field not in mapped_data.columns:
                mapped_data[field] = raw_data[field]
    
        # Preserve center_id field if specified
        if center_field and center_field in raw_data.columns and center_field not in mapped_data.columns:
            mapped_data[center_field] = raw_data[center_field]
    
        return mapped_data

    def _validate_schema(self, data: pd.DataFrame, table_name: str) -> List[dict]:
        """Validate data against target table schema"""

        errors = []

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Get table schema
            cursor.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name != 'global_subject_id'
                AND column_name != 'created_at'
                ORDER BY ordinal_position
            """,
                (table_name,),
            )

            schema = cursor.fetchall()
            cursor.close()
            conn.close()

            # Check required columns exist
            for col_name, data_type, is_nullable in schema:
                if col_name not in data.columns:
                    if is_nullable == "NO":
                        errors.append(
                            {
                                "type": "missing_required_column",
                                "column": col_name,
                                "message": f"Required column '{col_name}' not found in data",
                            }
                        )

            # Check for null values in NOT NULL columns
            for col_name, data_type, is_nullable in schema:
                if col_name in data.columns and is_nullable == "NO":
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
        """Resolve subject IDs using GSID service"""
    
        gsids = []
        local_id_records = []
        warnings = []
        stats = {
            "existing_matches": 0,
            "new_gsids_created": 0,
            "unknown_center_used": 0,
            "center_promoted": 0,
            "conflicts_flagged": 0,
        }
    
        for idx, row in data.iterrows():
            # Determine center_id
            center_id = 0  # Default to Unknown
            if center_id_field and pd.notna(row.get(center_id_field)):
                try:
                    center_id = int(row[center_id_field])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid center_id at row {idx}, using Unknown")
    
            if center_id == 0:
                stats["unknown_center_used"] += 1
    
            # Try to resolve using first available candidate field
            found_gsid = None
            resolution_errors = []
    
            for field in candidate_fields:
                if field not in row or pd.isna(row[field]):
                    continue
    
                local_id = str(row[field]).strip()
                if not local_id:
                    continue
    
                # Register/resolve through GSID service
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
                        timeout=10,
                    )
                    response.raise_for_status()
                    result = response.json()
    
                    # Successfully got a GSID
                    found_gsid = result["gsid"]
    
                    if result["action"] == "create_new":
                        stats["new_gsids_created"] += 1
                    elif result["action"] == "link_existing":
                        stats["existing_matches"] += 1
    
                        # Check for center promotion
                        if center_id != 0:
                            promoted = self._promote_center_if_needed(found_gsid, center_id)
                            if promoted:
                                stats["center_promoted"] += 1
    
                    elif result["action"] == "review_required":
                        stats["conflicts_flagged"] += 1
                        warnings.append(
                            f"Row {idx}: {result.get('review_reason', 'Conflict detected')}"
                        )
    
                    # Record ALL local IDs for this subject
                    for alt_field in candidate_fields:
                        if alt_field in row and pd.notna(row[alt_field]):
                            alt_id = str(row[alt_field]).strip()
                            if alt_id:
                                local_id_records.append({
                                    "center_id": center_id,
                                    "local_subject_id": alt_id,
                                    "identifier_type": alt_field,
                                    "global_subject_id": found_gsid,
                                })
    
                    break  # Success - stop trying other fields
    
                except requests.exceptions.RequestException as e:
                    error_msg = f"Field '{field}' value '{local_id}': {str(e)}"
                    if hasattr(e, 'response') and e.response is not None:
                        error_msg += f" (HTTP {e.response.status_code})"
                    resolution_errors.append(error_msg)
                    logger.error(f"GSID service error: {error_msg}")
                    continue  # Try next candidate field
    
            if not found_gsid:
                error_detail = "\n  ".join(resolution_errors) if resolution_errors else "No valid candidate fields found"
                raise ValueError(
                    f"Row {idx}: Failed to resolve subject ID\n  Candidates: {candidate_fields}\n  Errors:\n  {error_detail}"
                )
    
            gsids.append(found_gsid)
    
        # Generate warnings
        if stats["unknown_center_used"] > 0:
            warnings.append(
                f"{stats['unknown_center_used']} records used center_id=0 (Unknown)"
            )
    
        return {
            "gsids": gsids,
            "local_id_records": local_id_records,
            "summary": stats,
            "warnings": warnings,
        }

    def _promote_center_if_needed(self, gsid: str, new_center_id: int) -> bool:
        """Update subject's center from Unknown (0) to known center"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Check if subject has center_id = 0
            cursor.execute(
                "SELECT center_id FROM subjects WHERE global_subject_id = %s",
                (gsid,),
            )
            result = cursor.fetchone()

            if result and result[0] == 0:
                # Promote to known center
                cursor.execute(
                    """
                    UPDATE subjects 
                    SET center_id = %s, 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE global_subject_id = %s
                    """,
                    (new_center_id, gsid),
                )
                conn.commit()
                logger.info(f"Promoted {gsid} from Unknown to center {new_center_id}")
                cursor.close()
                conn.close()
                return True

            cursor.close()
            conn.close()
            return False

        except Exception as e:
            logger.error(f"Failed to promote center for {gsid}: {e}")
            return False

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
    
        # Remove candidate fields that aren't part of the table schema
        # Keep only: table columns + global_subject_id
        output_data = data.copy()
    
        # Get table schema to know which columns to keep
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                (table_name,)
            )
            valid_columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
    
            # Keep only valid columns that exist in data
            output_data = output_data[[col for col in output_data.columns if col in valid_columns]]
    
        except Exception as e:
            logger.warning(f"Could not validate columns against schema: {e}")
    
        # Write main table data
        table_csv = output_data.to_csv(index=False)
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=f"{staging_prefix}/{table_name}.csv",
            Body=table_csv,
        )
    
        # Write local_subject_ids records (deduplicated)
        if local_id_records:
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
            print(f"  - New GSIDs created: {stats['new_gsids_created']}")
            print(f"  - Unknown center used: {stats['unknown_center_used']}")
            print(f"  - Centers promoted: {stats['center_promoted']}")
            print(f"  - Conflicts flagged: {stats['conflicts_flagged']}")

            if report["warnings"]:
                print(f"\nWarnings ({len(report['warnings'])}):")
                for warning in report["warnings"][:10]:  # Show first 10
                    print(f"  ⚠ {warning}")
                if len(report["warnings"]) > 10:
                    print(f"  ... and {len(report['warnings']) - 10} more")

            print(f"\nStaging: {report['staging_location']}")
        else:
            print(f"\nValidation Errors ({len(report['validation_errors'])}):")
            for error in report["validation_errors"]:
                print(f"  ✗ [{error['type']}] {error['message']}")

        print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Validate and stage data fragments")
    parser.add_argument("table_name", help="Target database table name")
    parser.add_argument("s3_key", help="S3 key of input file (relative to bucket)")
    parser.add_argument("mapping_config", help="Path to mapping config JSON file")
    parser.add_argument("--source", required=True, help="Source system name")
    parser.add_argument(
        "--auto-approve", action="store_true", help="Auto-approve for loading"
    )

    args = parser.parse_args()

    # Load mapping config
    with open(args.mapping_config, "r") as f:
        mapping_config = json.load(f)

    # Database config
    db_config = {
        "host": os.getenv("DB_HOST", "idhub_db"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }

    s3_bucket = os.getenv("S3_BUCKET")
    gsid_service_url = os.getenv("GSID_SERVICE_URL", "http://gsid-service:8000")

    try:
        validator = FragmentValidator(db_config, s3_bucket, gsid_service_url)
        report = validator.process_incoming_file(
            args.table_name, args.s3_key, mapping_config, args.source, args.auto_approve
        )

        if report["status"] == "FAILED":
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
