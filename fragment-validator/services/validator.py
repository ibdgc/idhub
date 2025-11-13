# fragment-validator/services/validator.py
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .field_mapper import FieldMapper
from .nocodb_client import NocoDBClient
from .s3_client import S3Client
from .schema_validator import SchemaValidator
from .subject_id_resolver import SubjectIDResolver

logger = logging.getLogger(__name__)


class FragmentValidator:
    """Validates and stages data fragments for loading"""

    def __init__(
        self,
        s3_client: S3Client,
        nocodb_client: NocoDBClient,
        subject_id_resolver: SubjectIDResolver,
    ):
        self.s3_client = s3_client
        self.nocodb_client = nocodb_client
        self.subject_id_resolver = subject_id_resolver
        self.schema_validator = SchemaValidator(nocodb_client)

    def process_local_file(
        self,
        table_name: str,
        local_file_path: str,
        mapping_config: Dict,
        source_name: str,
        auto_approve: bool = False,
    ) -> Dict:
        """
        Process and validate a local CSV file

        Args:
            table_name: Target table name
            local_file_path: Path to local CSV file
            mapping_config: Field mapping configuration
            source_name: Source identifier
            auto_approve: Auto-approve for loading

        Returns:
            Validation report dict
        """
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Processing batch: {batch_id}")
        logger.info(f"Table: {table_name}")
        logger.info(f"Source: {source_name}")

        # Load raw data
        raw_data = pd.read_csv(local_file_path)
        logger.info(f"Loaded {len(raw_data)} rows from {local_file_path}")

        # Extract configuration
        field_mapping = mapping_config.get("field_mapping", {})
        subject_id_candidates = mapping_config.get("subject_id_candidates", [])
        center_id_field = mapping_config.get("center_id_field")
        default_center_id = mapping_config.get("default_center_id", 1)
        exclude_from_load = set(mapping_config.get("exclude_from_load", []))

        # Apply field mapping
        mapped_data = FieldMapper.apply_mapping(
            raw_data, field_mapping, subject_id_candidates, center_id_field
        )

        # Validate schema
        validation_result = self.schema_validator.validate(mapped_data, table_name)

        # Resolve subject IDs (with multi-candidate support)
        logger.info(f"Resolving subject IDs with candidates: {subject_id_candidates}")
        resolution_results = self.subject_id_resolver.resolve_batch(
            mapped_data,
            candidate_fields=subject_id_candidates,
            center_id_field=center_id_field,
            default_center_id=default_center_id,
        )

        # Add GSIDs to mapped data
        mapped_data["global_subject_id"] = resolution_results["gsids"]

        # Combine exclude fields
        all_exclude_fields = exclude_from_load.copy()
        all_exclude_fields.update(subject_id_candidates)
        if center_id_field:
            all_exclude_fields.add(center_id_field)

        # Upload to S3 staging
        s3_key = f"staging/validated/{batch_id}/{table_name}.csv"
        self.s3_client.upload_dataframe(mapped_data, s3_key)

        # Prepare report
        report = {
            "batch_id": batch_id,
            "table_name": table_name,
            "source_name": source_name,
            "subject_id_candidates": subject_id_candidates,
            "center_id_field": mapping_config.get("center_id_field"),
            "exclude_from_load": sorted(list(all_exclude_fields)),
            "timestamp": datetime.now().isoformat(),
            "input_file": local_file_path,
            "s3_location": s3_key,
            "row_count": len(mapped_data),
            "validation_errors": validation_result.errors,
            "warnings": validation_result.warnings + resolution_results["warnings"],
            "auto_approved": auto_approve,
            "resolution_summary": resolution_results["summary"],
            "flagged_records": resolution_results.get("flagged_records", []),
        }

        # Check if validation passed
        if not validation_result.is_valid:
            report["status"] = "FAILED"
            logger.error(
                f"✗ Validation failed with {len(validation_result.errors)} errors"
            )
            return report

        # Write staging outputs
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
            # Deduplicate based on (center_id, local_subject_id, identifier_type)
            seen = set()
            unique_records = []
            for record in local_id_records:
                key = (
                    record["center_id"],
                    record["local_subject_id"],
                    record["identifier_type"],
                )
                if key not in seen:
                    seen.add(key)
                    unique_records.append(record)

            local_ids_df = pd.DataFrame(unique_records)
            self.s3_client.upload_dataframe(
                local_ids_df, f"{staging_prefix}/local_subject_ids.csv"
            )
            logger.info(
                f"Wrote {len(unique_records)} unique local_subject_ids records "
                f"(deduplicated from {len(local_id_records)})"
            )

        # Write validation report
        self.s3_client.upload_json(report, f"{staging_prefix}/validation_report.json")

        # Write flagged records if any
        if report.get("flagged_records"):
            flagged_df = pd.DataFrame(report["flagged_records"])
            self.s3_client.upload_dataframe(
                flagged_df, f"{staging_prefix}/flagged_records.csv"
            )
            logger.warning(
                f"⚠️  Wrote {len(report['flagged_records'])} flagged records requiring review"
            )

    def _print_summary(self, report: dict):
        """Print validation summary"""
        logger.info("=" * 60)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Batch ID: {report['batch_id']}")
        logger.info(f"Table: {report['table_name']}")
        logger.info(f"Status: {report['status']}")
        logger.info(f"Total Records: {report['row_count']}")

        if report.get("resolution_summary"):
            summary = report["resolution_summary"]
            logger.info("\nSubject ID Resolution:")
            logger.info(f"  - New GSIDs minted: {summary.get('new_gsids_minted', 0)}")
            logger.info(f"  - Existing matches: {summary.get('existing_matches', 0)}")
            logger.info(f"  - Center promotions: {summary.get('center_promoted', 0)}")
            logger.info(
                f"  - Flagged for review: {summary.get('flagged_for_review', 0)}"
            )
            logger.info(
                f"  - Validation warnings: {summary.get('validation_warnings', 0)}"
            )
            logger.info(
                f"  - Multi-GSID conflicts: {summary.get('multi_gsid_conflicts', 0)}"
            )

        if report.get("validation_errors"):
            logger.error(f"\nValidation Errors: {len(report['validation_errors'])}")
            for error in report["validation_errors"][:5]:
                logger.error(f"  - {error}")

        if report.get("warnings"):
            logger.warning(f"\nWarnings: {len(report['warnings'])}")
            for warning in report["warnings"][:10]:
                logger.warning(f"  - {warning}")

        if report.get("flagged_records"):
            logger.warning(
                f"\n⚠️  {len(report['flagged_records'])} records flagged for manual review"
            )
            logger.warning(
                f"   Review file: staging/validated/{report['batch_id']}/flagged_records.csv"
            )

        logger.info("=" * 60)
