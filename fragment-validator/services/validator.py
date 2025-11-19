# fragment-validator/services/validator.py
import logging
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from .field_mapper import FieldMapper
from .nocodb_client import NocoDBClient
from .s3_client import S3Client
from .schema_validator import SchemaValidator
from .subject_id_resolver import SubjectIDResolver
from .update_detector import UpdateDetector

logger = logging.getLogger(__name__)


class FragmentValidator:
    """Main validator orchestrating the validation pipeline"""

    def __init__(
        self,
        s3_client: S3Client,
        nocodb_client: NocoDBClient,
        subject_id_resolver: SubjectIDResolver,
        db_config: Optional[Dict] = None,
    ):
        self.s3_client = s3_client
        self.nocodb_client = nocodb_client
        self.subject_id_resolver = subject_id_resolver
        self.schema_validator = SchemaValidator(nocodb_client)
        self.update_detector = UpdateDetector(db_config)

    def process_local_file(
        self,
        table_name: str,
        local_file_path: str,
        mapping_config: Dict,
        source_name: str,
        auto_approve: bool = False,
        batch_size: int = 20,
        detect_changes: bool = True,
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
            detect_changes: Whether to detect changes against current DB state

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

            # Step 5: Detect changes (NEW)
            change_analysis = None
            if detect_changes:
                logger.info("Analyzing changes against current database state...")
                try:
                    from core.config import settings

                    natural_key = settings.get_natural_key(table_name)

                    change_analysis = self.update_detector.analyze_changes(
                        incoming_data=mapped_data,
                        table_name=table_name,
                        natural_key=natural_key,
                    )

                    # Log change summary
                    summary = change_analysis["summary"]
                    logger.info(
                        f"Change detection complete: "
                        f"{summary['new']} new, "
                        f"{summary['updated']} updated, "
                        f"{summary['unchanged']} unchanged"
                    )

                    # Print detailed summary
                    logger.info(
                        self.update_detector.format_change_summary(change_analysis)
                    )

                except Exception as e:
                    logger.warning(f"Change detection failed (non-fatal): {e}")
                    change_analysis = {
                        "error": str(e),
                        "summary": {
                            "total_incoming": len(mapped_data),
                            "new": len(mapped_data),
                            "updated": 0,
                            "unchanged": 0,
                            "orphaned": 0,
                        },
                    }

            # Step 6: Upload to S3
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

            # Step 7: Build validation report
            report = self._build_success_report(
                batch_id,
                mapped_data,
                resolution_result,
                s3_key,
                table_name,
                source_name,
                auto_approve,
                validation_result.warnings,
                change_analysis,  # NEW: Include change analysis
            )

            # Step 8: Upload validation report
            report_key = f"staging/validated/{batch_id}/validation_report.json"
            self.s3_client.upload_json(report, report_key)
            logger.info(f"Uploaded validation report to {report_key}")

            return report

        except Exception as e:
            logger.error(f"Validation failed with error: {e}", exc_info=True)
            return self._build_failure_report(
                batch_id, [{"type": "exception", "message": str(e)}], []
            )

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
        change_analysis: Optional[Dict] = None,  # NEW parameter
    ) -> Dict:
        """Build success validation report with change analysis"""

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
            "validation_warnings": warnings,
            "gsid_resolution": {
                "total_rows": resolution_result["summary"]["total_rows"],
                "gsids_resolved": resolution_result["summary"]["gsids_resolved"],
                "new_subjects": resolution_result["summary"]["new_subjects"],
                "existing_subjects": resolution_result["summary"]["existing_subjects"],
                "conflicts": resolution_result["summary"]["conflicts"],
            },
        }

        # Add change analysis if available (NEW)
        if change_analysis:
            report["change_analysis"] = {
                "enabled": True,
                "summary": change_analysis["summary"],
                "new_records_count": len(change_analysis.get("new_records", [])),
                "updated_records_count": len(change_analysis.get("updates", [])),
                "unchanged_records_count": len(change_analysis.get("unchanged", [])),
                "orphaned_records_count": len(change_analysis.get("orphaned", [])),
            }

            # Include sample of updates (first 10)
            if change_analysis.get("updates"):
                report["change_analysis"]["sample_updates"] = [
                    {
                        "natural_key": update["natural_key"],
                        "fields_changed": list(update["changes"].keys()),
                        "change_count": len(update["changes"]),
                    }
                    for update in change_analysis["updates"][:10]
                ]

            # Flag if there are orphaned records (data in DB but not in incoming)
            if change_analysis["summary"]["orphaned"] > 0:
                report["validation_warnings"].append(
                    f"Warning: {change_analysis['summary']['orphaned']} records exist in database but not in incoming data"
                )
        else:
            report["change_analysis"] = {
                "enabled": False,
                "reason": "Change detection disabled or failed",
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
