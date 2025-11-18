# fragment-validator/services/validator.py
import logging
from datetime import datetime
from typing import Dict

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

        Returns:
            Validation report dictionary
        """
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"Processing batch: {batch_id}")
        logger.info(f"Table: {table_name}")
        logger.info(f"Source: {source_name}")

        # Step 1: Load CSV with optimizations
        logger.info(f"Loading CSV file: {local_file_path}")
        raw_data = pd.read_csv(
            local_file_path,
            dtype=str,  # Read all as strings initially
            na_filter=False,  # Faster, we'll handle NaN ourselves
            low_memory=False,
        )
        logger.info(f"Loaded {len(raw_data)} rows from {local_file_path}")

        # Step 2: Apply field mapping
        logger.info("Applying field mapping...")
        mapped_data = FieldMapper.apply_mapping(
            raw_data,
            mapping_config.get("field_mapping", {}),
            mapping_config.get("subject_id_candidates", []),
            mapping_config.get("center_id_field"),
        )
        logger.info(
            f"Mapped data: {len(mapped_data)} rows, {len(mapped_data.columns)} columns"
        )

        # Step 3: Schema validation
        logger.info(f"Validating against table schema: {table_name}")
        validation_result = self.schema_validator.validate(mapped_data, table_name)

        if not validation_result.is_valid:
            logger.error("✗ Schema validation failed")
            return self._build_failure_report(batch_id, validation_result)

        logger.info("✓ Schema validation passed")

        # Step 4: Subject ID resolution (OPTIMIZED)
        logger.info("Starting subject ID resolution...")
        resolution_result = self.subject_id_resolver.resolve_batch(
            mapped_data,
            mapping_config.get("subject_id_candidates", []),
            mapping_config.get("center_id_field"),
            mapping_config.get("default_center_id", 0),
            created_by=source_name,
            batch_size=batch_size,
        )

        # Add GSIDs to mapped data
        mapped_data["global_subject_id"] = resolution_result["gsids"]

        # Step 5: Upload to S3
        logger.info("Uploading validated data to S3...")
        s3_key = f"staging/validated/{batch_id}/{table_name}.csv"
        self.s3_client.upload_dataframe(mapped_data, s3_key)

        # Upload local_subject_ids separately
        if resolution_result["local_id_records"]:
            local_ids_df = pd.DataFrame(resolution_result["local_id_records"])
            local_ids_key = f"staging/validated/{batch_id}/local_subject_ids.csv"
            self.s3_client.upload_dataframe(local_ids_df, local_ids_key)
            logger.info(f"Wrote {len(local_ids_df)} unique local_subject_ids records")

        # Step 6: Build validation report
        report = self._build_success_report(
            batch_id,
            mapped_data,
            resolution_result,
            s3_key,
            auto_approve,
        )

        # Upload report to S3
        report_key = f"staging/validated/{batch_id}/validation_report.json"
        self.s3_client.upload_json(report, report_key)

        logger.info(f"✓ Validation complete: {batch_id}")
        return report

    def _build_success_report(
        self,
        batch_id: str,
        data: pd.DataFrame,
        resolution_result: Dict,
        s3_key: str,
        auto_approve: bool,
    ) -> Dict:
        """Build success validation report"""
        return {
            "batch_id": batch_id,
            "status": "VALIDATED",
            "timestamp": datetime.now().isoformat(),
            "row_count": len(data),
            "column_count": len(data.columns),
            "errors": [],
            "warnings": [],
            "s3_location": s3_key,
            "auto_approved": auto_approve,
            "resolution_summary": resolution_result["summary"],
        }

    def _build_failure_report(self, batch_id: str, validation_result) -> Dict:
        """Build failure validation report"""
        return {
            "batch_id": batch_id,
            "status": "FAILED",
            "timestamp": datetime.now().isoformat(),
            "errors": validation_result.errors,
            "warnings": validation_result.warnings,
        }
