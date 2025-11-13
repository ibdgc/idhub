import logging
from datetime import datetime
from typing import Dict

from services.data_transformer import DataTransformer
from services.fragment_resolution import FragmentResolutionService
from services.load_strategy import (
    LoadStrategy,
    StandardLoadStrategy,
    UpsertLoadStrategy,
)
from services.s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Handles loading of validated fragments into database tables"""

    # Tables that should use upsert strategy with their conflict columns
    UPSERT_TABLES = {
        "subject": ["global_subject_id"],
        "lcl": ["niddk_no"],
        "local_subject_ids": ["center_id", "local_subject_id", "identifier_type"],
    }

    # Table-specific field requirements (fields that should NEVER be excluded)
    REQUIRED_FIELDS = {
        "local_subject_ids": {
            "center_id",
            "local_subject_id",
            "identifier_type",
            "global_subject_id",
        },
        "lcl": {"niddk_no", "global_subject_id"},
        "subject": {"global_subject_id"},
    }

    # Table-specific default exclusions (fields that should ALWAYS be excluded)
    TABLE_DEFAULT_EXCLUSIONS = {
        "local_subject_ids": {
            "action"
        },  # action is GSID resolution metadata, not a DB column
    }

    def __init__(self):
        self.s3_client = S3Client()
        self.resolution_service = FragmentResolutionService()

    def _get_load_strategy(self, table_name: str, exclude_fields: set) -> LoadStrategy:
        """Get appropriate load strategy for table"""
        if table_name in self.UPSERT_TABLES:
            return UpsertLoadStrategy(
                table_name=table_name,
                conflict_columns=self.UPSERT_TABLES[table_name],
                exclude_fields=exclude_fields,
            )
        else:
            return StandardLoadStrategy(
                table_name=table_name, exclude_fields=exclude_fields
            )

    def _extract_table_name(self, s3_key: str) -> str:
        """Extract table name from S3 key"""
        filename = s3_key.split("/")[-1]
        return filename.replace(".csv", "").replace(".json", "")

    def _get_exclude_fields(self, batch_id: str, table_name: str) -> set:
        """Get fields to exclude from loading for a specific table"""
        # Default exclusions for all tables
        default_exclude = {"Id", "created_at", "updated_at"}

        # Table-specific default exclusions
        table_defaults = self.TABLE_DEFAULT_EXCLUSIONS.get(table_name, set())

        exclude_fields = default_exclude.copy()
        exclude_fields.update(table_defaults)

        # Get exclude fields from validation report
        try:
            report = self.s3_client.download_validation_report(batch_id)
            report_exclude = set(report.get("exclude_from_load", []))
            exclude_fields.update(report_exclude)

            # Preserve required fields for this table
            required = self.REQUIRED_FIELDS.get(table_name, set())
            exclude_fields -= required

            logger.info(f"Preserved required fields for {table_name}: {required}")
            logger.info(f"Exclude fields for {table_name}: {exclude_fields}")

        except Exception as e:
            logger.warning(f"Could not load validation report: {e}")
            logger.info(
                f"Using default exclude fields for {table_name}: {exclude_fields}"
            )

        return exclude_fields

    def preview_load(self, batch_id: str) -> Dict:
        """Preview what would be loaded without executing"""
        logger.info(f"Previewing load for batch: {batch_id}")

        fragments = self.s3_client.list_batch_fragments(batch_id)
        if not fragments:
            raise ValueError(f"No fragments found for batch {batch_id}")

        preview_results = {}

        for fragment in fragments:
            s3_key = fragment["Key"]
            table_name = self._extract_table_name(s3_key)

            if table_name == "validation_report":
                continue

            try:
                exclude_fields = self._get_exclude_fields(batch_id, table_name)
                data = self.s3_client.download_fragment(batch_id, table_name)

                # Get load strategy (which creates its own transformer)
                strategy = self._get_load_strategy(table_name, exclude_fields)

                # Use strategy's load method in dry_run mode
                preview_result = strategy.load(data, dry_run=True)

                preview_results[table_name] = preview_result

            except Exception as e:
                logger.error(f"Error previewing {table_name}: {e}")
                preview_results[table_name] = {
                    "status": "error",
                    "error": str(e),
                }

        return preview_results

    def execute_load(self, batch_id: str) -> Dict:
        """Execute load of validated fragments into database"""
        logger.info(f"Executing load for batch: {batch_id}")

        # Get list of fragments
        fragments = self.s3_client.list_batch_fragments(batch_id)
        if not fragments:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        results = {
            "batch_id": batch_id,
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
        }

        for fragment in fragments:
            s3_key = fragment["Key"]
            table_name = self._extract_table_name(s3_key)

            # Skip validation report
            if table_name == "validation_report":
                continue

            try:
                # Get table-specific exclude fields
                exclude_fields = self._get_exclude_fields(batch_id, table_name)

                # Download fragment
                data = self.s3_client.download_fragment(batch_id, table_name)

                # Get load strategy
                strategy = self._get_load_strategy(table_name, exclude_fields)

                # Execute load
                start_time = datetime.utcnow()
                load_result = strategy.load(data, dry_run=False)
                execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000

                # Log resolution using the strategy name from the result
                self.resolution_service.create_resolution(
                    batch_id=batch_id,
                    table_name=table_name,
                    fragment_key=s3_key,
                    load_status="success",
                    load_strategy=load_result.get("strategy", "unknown"),
                    rows_attempted=load_result.get("rows_attempted", len(data)),
                    rows_loaded=load_result.get("rows_loaded", 0),
                    rows_failed=load_result.get("rows_failed", 0),
                    execution_time_ms=int(execution_time),
                )

                # Archive fragment
                self.s3_client.mark_fragment_loaded(batch_id, table_name)

                logger.info(
                    f"Successfully loaded {load_result.get('rows_loaded', 0)} rows into {table_name}"
                )

                results["tables"][table_name] = {
                    "status": "success",
                    "rows_loaded": load_result.get("rows_loaded", 0),
                    "strategy": load_result.get("strategy", "unknown"),
                }

            except Exception as e:
                logger.error(f"Error loading {table_name}: {e}")

                # Log failed resolution
                self.resolution_service.create_resolution(
                    batch_id=batch_id,
                    table_name=table_name,
                    fragment_key=s3_key,
                    load_status="failed",
                    load_strategy="unknown",
                    rows_attempted=len(data) if "data" in locals() else 0,
                    rows_loaded=0,
                    rows_failed=len(data) if "data" in locals() else 0,
                    execution_time_ms=0,
                    error_message=str(e),
                    requires_review=True,
                    review_reason=f"Load failed: {str(e)}",
                )

                results["tables"][table_name] = {
                    "status": "failed",
                    "error": str(e),
                }

                # Re-raise to stop processing
                raise

        return results
