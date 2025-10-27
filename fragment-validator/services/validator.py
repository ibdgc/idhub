# fragment-validator/services/validator.py
import json
import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd

from .field_mapper import FieldMapper
from .gsid_client import GSIDClient
from .nocodb_client import NocoDBClient
from .s3_client import S3Client
from .schema_validator import SchemaValidator
from .subject_id_resolver import SubjectIDResolver

logger = logging.getLogger(__name__)


class FragmentValidator:
    """Main validator orchestrating the validation pipeline"""

    def __init__(
        self,
        s3_client: S3Client,
        nocodb_client: NocoDBClient,
        gsid_client: GSIDClient,
    ):
        self.s3_client = s3_client
        self.nocodb_client = nocodb_client
        self.gsid_client = gsid_client
        self.schema_validator = SchemaValidator(nocodb_client)
        self.subject_id_resolver = SubjectIDResolver(gsid_client)

        # Pre-load local ID cache (for potential future optimizations)
        self.local_id_cache = nocodb_client.load_local_id_cache()

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

        # Load raw data
        raw_data = pd.read_csv(local_file_path)

        # Upload to S3 incoming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"incoming/{table_name}/{table_name}_{timestamp}.csv"
        logger.info(f"Uploading to s3://{self.s3_client.bucket}/{s3_key}")
        self.s3_client.upload_dataframe(raw_data, s3_key)

        # Apply mapping
        mapped_data = FieldMapper.apply_mapping(
            raw_data,
            mapping_config.get("field_mapping", {}),
            mapping_config.get("subject_id_candidates", []),
            mapping_config.get("center_id_field"),
        )

        # Validate schema
        validation_result = self.schema_validator.validate(mapped_data, table_name)

        # Generate batch ID
        batch_id = f"batch_{timestamp}"

        # Prepare report
        report = {
            "batch_id": batch_id,
            "table_name": table_name,
            "source_name": source_name,
            "timestamp": datetime.now().isoformat(),
            "input_file": local_file_path,
            "s3_location": s3_key,
            "row_count": len(mapped_data),
            "validation_errors": validation_result.errors,
            "warnings": validation_result.warnings,
            "auto_approved": auto_approve,
        }

        # Check if validation passed
        if not validation_result.is_valid:
            report["status"] = "FAILED"
            logger.error(
                f"✗ Validation failed with {len(validation_result.errors)} errors"
            )
            self._print_summary(report)
            return report

        # Resolve subject IDs
        resolution_results = self.subject_id_resolver.resolve_batch(
            mapped_data,
            mapping_config.get("subject_id_candidates", []),
            mapping_config.get("center_id_field"),
            mapping_config.get("default_center_id", 0),
        )

        # Add resolved GSIDs to data
        mapped_data["global_subject_id"] = resolution_results["gsids"]

        # Update report with resolution results
        report["resolution_summary"] = resolution_results["summary"]
        report["warnings"].extend(resolution_results["warnings"])

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
            f"s3://{self.s3_client.bucket}/staging/validated/{batch_id}/"
        )

        logger.info(f"✓ Validation complete: {batch_id}")
        self._print_summary(report)
        return report

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
        self.s3_client.upload_dataframe(data, f"{staging_prefix}/{table_name}.csv")

        # Write local_subject_ids records (deduplicated)
        if local_id_records:
            local_ids_df = pd.DataFrame(local_id_records).drop_duplicates(
                subset=["center_id", "local_subject_id", "identifier_type"]
            )
            self.s3_client.upload_dataframe(
                local_ids_df, f"{staging_prefix}/local_subject_ids.csv"
            )

        # Write validation report
        self.s3_client.upload_json(report, f"{staging_prefix}/validation_report.json")

        logger.info(
            f"Staging outputs written to s3://{self.s3_client.bucket}/{staging_prefix}/"
        )

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
