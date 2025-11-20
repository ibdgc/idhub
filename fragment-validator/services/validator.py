# fragment-validator/services/validator.py
import logging
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from .conflict_detector import ConflictDetector
from .field_mapper import FieldMapper
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
        subject_id_resolver: SubjectIDResolver,
    ):
        self.s3_client = s3_client
        self.nocodb_client = nocodb_client
        self.subject_id_resolver = subject_id_resolver
        self.schema_validator = SchemaValidator(nocodb_client)
        self.conflict_detector = ConflictDetector()

    def process_local_file(
        self,
        table_name: str,
        local_file_path: str,
        mapping_config: Dict,
        source_name: str,
        auto_approve: bool = False,
        batch_size: int = 20,
    ) -> Dict:
        """
        Process and validate a local CSV file

        Args:
            table_name: Target database table name
            local_file_path: Path to local CSV file
            mapping_config: Field mapping configuration
            source_name: Source identifier (e.g., "redcap_pipeline", "manual_upload")
            auto_approve: Whether to auto-approve validation
            batch_size: Batch size for parallel GSID resolution

        Returns:
            Validation report dictionary
        """
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Processing local file: {local_file_path}")
        logger.info(f"Target table: {table_name}")
        logger.info(f"Batch ID: {batch_id}")

        try:
            # Step 1: Load raw data
            logger.info("Loading raw data from file...")
            raw_data = pd.read_csv(local_file_path)
            logger.info(f"Loaded {len(raw_data)} rows")

            # Step 2: Apply field mapping
            logger.info("Applying field mapping...")
            field_mapping = mapping_config.get("field_mapping", {})
            subject_id_candidates = mapping_config.get("subject_id_candidates", [])
            center_id_field = mapping_config.get("center_id_field")

            mapped_data = FieldMapper.apply_mapping(
                raw_data, field_mapping, subject_id_candidates, center_id_field
            )
            logger.info(f"Mapped to {len(mapped_data.columns)} columns")

            # Step 3: Validate against schema
            logger.info("Validating against target schema...")
            validation_result = self.schema_validator.validate(mapped_data, table_name)

            if not validation_result.is_valid:
                logger.error("Schema validation failed")
                return self._build_failure_report(
                    batch_id, validation_result.errors, validation_result.warnings
                )

            # Step 4: Resolve subject IDs to GSIDs
            logger.info("Resolving subject IDs to GSIDs...")
            default_center_id = mapping_config.get("default_center_id", 0)

            resolution_result = self.subject_id_resolver.resolve_batch(
                data=mapped_data,
                candidate_fields=subject_id_candidates,
                center_id_field=center_id_field,
                default_center_id=default_center_id,
                created_by=source_name,
                batch_size=batch_size,
            )

            # Add GSIDs to mapped data
            mapped_data["global_subject_id"] = resolution_result["gsids"]

            # Step 4.5: Detect conflicts
            logger.info("Detecting data conflicts...")
            conflicts = self.conflict_detector.detect_conflicts(
                resolution_result, batch_id
            )

            # Upload conflicts to NocoDB if any exist
            if conflicts:
                logger.warning(
                    f"⚠️  {len(conflicts)} conflicts detected - uploading to NocoDB"
                )
                self._upload_conflicts_to_nocodb(conflicts)
                # Force manual review if conflicts exist
                auto_approve = False

            # Step 5: Upload to S3
            logger.info("Uploading validated data to S3...")
            s3_key = f"staging/validated/{batch_id}/{table_name}.csv"
            self.s3_client.upload_dataframe(mapped_data, s3_key)

            # Upload local_subject_ids separately
            if resolution_result["local_id_records"]:
                local_ids_df = pd.DataFrame(resolution_result["local_id_records"])
                local_ids_key = f"staging/validated/{batch_id}/local_subject_ids.csv"
                self.s3_client.upload_dataframe(local_ids_df, local_ids_key)
                logger.info(
                    f"Wrote {len(local_ids_df)} unique local_subject_ids records"
                )

            # Step 6: Build validation report
            report = self._build_success_report(
                batch_id,
                mapped_data,
                resolution_result,
                s3_key,
                table_name,
                source_name,
                auto_approve,
                validation_result.warnings,
                conflicts,
            )

            # Step 7: Upload validation report
            report_key = f"staging/validated/{batch_id}/validation_report.json"
            self.s3_client.upload_json(report, report_key)
            logger.info(f"Uploaded validation report to {report_key}")

            return report

        except Exception as e:
            logger.error(f"Validation failed with error: {e}", exc_info=True)
            return self._build_failure_report(
                batch_id, [{"type": "exception", "message": str(e)}], []
            )

    def _upload_conflicts_to_nocodb(self, conflicts: List[Dict]) -> None:
        """Upload conflict records to NocoDB for review"""
        if not conflicts:
            return

        try:
            for conflict in conflicts:
                # Upload to conflict_resolutions table
                self.nocodb_client.create_record(
                    table_name="conflict_resolutions", data=conflict
                )
            logger.info(f"Uploaded {len(conflicts)} conflicts to NocoDB")
        except Exception as e:
            logger.error(f"Failed to upload conflicts to NocoDB: {e}")
            # Don't fail the entire validation if conflict upload fails
            logger.warning("Continuing validation despite conflict upload failure")

    def _build_success_report(
        self,
        batch_id: str,
        mapped_data: pd.DataFrame,
        resolution_result: Dict,
        s3_key: str,
        table_name: str,
        source_name: str,
        auto_approve: bool,
        warnings: List[str],
        conflicts: List[Dict] = None,
    ) -> Dict:
        """Build success validation report with conflict information"""
        summary = resolution_result["summary"]
        conflicts = conflicts or []
        has_conflicts = len(conflicts) > 0

        # Force manual review if conflicts exist
        if has_conflicts:
            auto_approve = False

        # Define which fields should be excluded from database load per table
        TABLE_EXCLUDE_FIELDS = {
            "lcl": ["consortium_id", "center_id"],
            "blood": ["consortium_id", "center_id"],
            "dna": ["consortium_id", "center_id"],
            "rna": ["consortium_id", "center_id"],
            "serum": ["consortium_id", "center_id"],
            "plasma": ["consortium_id", "center_id"],
            "stool": ["consortium_id", "center_id"],
            "tissue": ["consortium_id", "center_id"],
            "local_subject_ids": [],
        }

        # Get exclude fields for this table
        exclude_from_load = TABLE_EXCLUDE_FIELDS.get(
            table_name,
            ["consortium_id"],  # Default: exclude consortium_id
        )

        # Only include fields that actually exist in the data
        exclude_from_load = [f for f in exclude_from_load if f in mapped_data.columns]

        # Build base report
        report = {
            "status": "VALIDATED",
            "batch_id": batch_id,
            "table_name": table_name,
            "source": source_name,
            "timestamp": datetime.now().isoformat(),
            "auto_approved": auto_approve,
            "s3_location": s3_key,
            "row_count": len(mapped_data),
            "column_count": len(mapped_data.columns),
            "columns": list(mapped_data.columns),
            "exclude_from_load": exclude_from_load,
            "validation_warnings": warnings,
            "has_conflicts": has_conflicts,
            "conflict_summary": ConflictDetector.format_conflict_summary(conflicts),
            "gsid_resolution": {
                "total_rows": summary["total_rows"],
                "resolved": summary["resolved"],
                "unresolved": summary["unresolved"],
                "unique_gsids": summary["unique_gsids"],
                "new_subjects": summary["created"],
                "existing_subjects": summary["linked"],
                "multi_gsid_conflicts": summary["multi_gsid_conflicts"],
                "center_conflicts": summary["center_conflicts"],
                "local_id_records_count": len(
                    resolution_result.get("local_id_records", [])
                ),
            },
        }

        return report

    def _build_failure_report(
        self, batch_id: str, errors: List[Dict], warnings: List[str]
    ) -> Dict:
        """Build failure validation report"""
        return {
            "status": "FAILED",
            "batch_id": batch_id,
            "timestamp": datetime.now().isoformat(),
            "validation_errors": errors,
            "validation_warnings": warnings,
        }
