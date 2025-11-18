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
            source_name: Source system identifier
            auto_approve: Whether to auto-approve for loading
            batch_size: Number of parallel workers for GSID resolution

        Returns:
            Validation report dictionary
        """
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"Processing batch: {batch_id}")
        logger.info(f"Table: {table_name}")
        logger.info(f"Source: {source_name}")

        # Extract config
        field_mapping = mapping_config.get("field_mapping", {})
        subject_id_candidates = mapping_config.get("subject_id_candidates", [])
        center_id_field = mapping_config.get("center_id_field")
        default_center_id = mapping_config.get("default_center_id", 0)
        exclude_from_load = mapping_config.get("exclude_from_load", [])

        if not subject_id_candidates:
            raise ValueError(
                "subject_id_candidates must be specified in mapping config"
            )

        # Load and parse CSV
        logger.info(f"Loading CSV file: {local_file_path}")
        raw_data = pd.read_csv(local_file_path)
        logger.info(f"Loaded {len(raw_data)} rows from {local_file_path}")

        # Apply field mapping (ONLY explicitly mapped fields)
        logger.info("Applying field mapping...")
        mapped_data = FieldMapper.apply_mapping(
            raw_data=raw_data,
            field_mapping=field_mapping,
            subject_id_candidates=subject_id_candidates,
            center_id_field=center_id_field,
        )
        logger.info(
            f"Mapped data: {len(mapped_data)} rows, {len(mapped_data.columns)} columns"
        )

        # Validate schema (before adding global_subject_id)
        logger.info(f"Validating against table schema: {table_name}")
        validation_result = self.schema_validator.validate(mapped_data, table_name)

        if not validation_result.is_valid:
            logger.error("✗ Schema validation failed")
            for error in validation_result.errors:
                logger.error(f"  - {error}")
            return self._create_failed_report(
                batch_id, table_name, validation_result.errors
            )

        if validation_result.warnings:
            for warning in validation_result.warnings:
                logger.warning(f"  ⚠ {warning}")

        logger.info("✓ Schema validation passed")

        # Subject ID resolution (use raw_data to access all fields)
        logger.info("Starting subject ID resolution...")
        resolution_result = self.subject_id_resolver.resolve_batch(
            data=raw_data,  # Use raw data so we have access to resolution fields
            candidate_fields=subject_id_candidates,
            center_id_field=center_id_field,
            default_center_id=default_center_id,
            created_by=source_name,
            batch_size=batch_size,
        )

        # Add resolved GSIDs to mapped_data
        mapped_data["global_subject_id"] = resolution_result["gsids"]

        # Remove fields marked as exclude_from_load
        if exclude_from_load:
            fields_to_remove = [
                f for f in exclude_from_load if f in mapped_data.columns
            ]
            if fields_to_remove:
                logger.info(f"Removing excluded fields: {fields_to_remove}")
                mapped_data = mapped_data.drop(columns=fields_to_remove)

        logger.info(
            f"Final data: {len(mapped_data)} rows, {len(mapped_data.columns)} columns"
        )
        logger.info(f"Final columns: {list(mapped_data.columns)}")

        # Check for missing GSIDs
        missing_gsids = mapped_data["global_subject_id"].isna().sum()
        if missing_gsids > 0:
            logger.error(f"✗ {missing_gsids} rows missing global_subject_id")
            return self._create_failed_report(
                batch_id,
                table_name,
                [f"{missing_gsids} rows failed GSID resolution"],
            )

        # Upload to S3
        logger.info("Uploading validated fragment to S3...")
        s3_key = f"staging/validated/{batch_id}/{table_name}.csv"
        self.s3_client.upload_dataframe(mapped_data, s3_key)

        # Create validation report
        report = {
            "batch_id": batch_id,
            "table_name": table_name,
            "source": source_name,
            "status": "VALIDATED",
            "timestamp": datetime.utcnow().isoformat(),
            "row_count": len(mapped_data),
            "s3_key": s3_key,
            "validation_errors": [],
            "validation_warnings": validation_result.warnings,
            "resolution_summary": resolution_result["summary"],
            "auto_approved": auto_approve,
        }

        # Upload validation report
        report_key = f"staging/validated/{batch_id}/validation_report.json"
        self.s3_client.upload_json(report, report_key)
        logger.info(
            f"Uploaded validation report to s3://{self.s3_client.bucket}/{report_key}"
        )

        logger.info("✓ Validation complete")
        return report

    def _create_failed_report(
        self, batch_id: str, table_name: str, errors: List[str]
    ) -> Dict:
        """Create a failed validation report"""
        return {
            "batch_id": batch_id,
            "table_name": table_name,
            "status": "FAILED",
            "timestamp": datetime.utcnow().isoformat(),
            "validation_errors": errors,
        }
