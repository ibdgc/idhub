# table-loader/services/loader.py
import logging
from datetime import datetime
from typing import Dict, List

from botocore.exceptions import ClientError

from .fragment_resolution import FragmentResolutionService
from .load_strategy import LoadStrategy, StandardLoadStrategy, UpsertLoadStrategy
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Orchestrates loading of validated data fragments into database"""

    # Tables that should use upsert strategy with their conflict columns
    UPSERT_TABLES = {
        "subject": ["global_subject_id"],
        "lcl": ["niddk_no"],
        "local_subject_ids": ["center_id", "local_subject_id", "identifier_type"],
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
        return StandardLoadStrategy(
            table_name=table_name, exclude_fields=exclude_fields
        )

    def _get_exclude_fields(self, batch_id: str) -> set:
        """Get fields to exclude from validation report"""
        try:
            report = self.s3_client.download_validation_report(batch_id)
            exclude_fields = set(report.get("exclude_from_load", []))

            # Always exclude system fields
            exclude_fields.update({"Id", "created_at", "updated_at"})

            logger.info(
                f"Loaded exclude_from_load from validation report: {exclude_fields}"
            )
            return exclude_fields

        except (FileNotFoundError, ClientError) as e:
            logger.warning(
                f"Could not load validation report for {batch_id}: {e}. Using defaults."
            )
            # Default exclusions for GSID resolution fields
            return {
                "Id",
                "created_at",
                "updated_at",
                "consortium_id",
                "identifier_type",
                "action",
                "local_subject_id",
            }

    def _extract_table_name(self, s3_key: str) -> str:
        """Extract table name from S3 key"""
        # Example: staging/validated/batch_123/blood.csv -> blood
        filename = s3_key.split("/")[-1]
        return filename.replace(".csv", "")

    def _track_fragment_load(
        self,
        batch_id: str,
        table_name: str,
        fragment_key: str,
        load_result: dict,
    ):
        """Track fragment load in fragment_resolutions table"""
        try:
            # Determine load strategy from result
            load_strategy = load_result.get("strategy", "standard_insert")

            # Map strategy names to valid database values
            strategy_mapping = {
                "insert": "standard_insert",
                "upsert": "upsert",
                "standard": "standard_insert",
                "standard_insert": "standard_insert",
            }

            # Normalize the strategy value
            load_strategy = strategy_mapping.get(load_strategy, "standard_insert")

            self.resolution_service.create_resolution(
                batch_id=batch_id,
                table_name=table_name,
                fragment_key=fragment_key,
                load_status=load_result.get("status", "unknown"),
                load_strategy=load_strategy,
                rows_attempted=load_result.get("rows", 0),
                rows_loaded=load_result.get("rows_loaded", 0),
                rows_failed=load_result.get("rows_failed", 0),
                error_message=load_result.get("error"),
                requires_review=load_result.get("status") == "failed",
                review_reason=f"Load failed: {load_result.get('error')}"
                if load_result.get("status") == "failed"
                else None,
                metadata={
                    "s3_key": fragment_key,
                    "columns": load_result.get("columns", []),
                },
            )
        except Exception as e:
            logger.error(f"Failed to track fragment load: {e}")

    def preview_load(self, batch_id: str) -> Dict:
        """Preview load without executing database operations"""
        logger.info(f"Previewing load for batch: {batch_id}")

        # Get list of fragments
        fragments = self.s3_client.list_batch_fragments(batch_id)
        if not fragments:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        # Get exclude fields
        exclude_fields = self._get_exclude_fields(batch_id)
        logger.info(f"Preview using exclude_fields: {exclude_fields}")

        results = {}

        for fragment in fragments:
            s3_key = fragment["Key"]
            table_name = self._extract_table_name(s3_key)

            # Skip validation report
            if table_name == "validation_report":
                continue

            try:
                # Download fragment
                data = self.s3_client.download_fragment(batch_id, table_name)

                # Get strategy and preview
                strategy = self._get_load_strategy(table_name, exclude_fields)
                preview_result = strategy.load(data, dry_run=True)

                results[table_name] = preview_result

            except Exception as e:
                logger.error(f"Error previewing {table_name}: {e}")
                results[table_name] = {
                    "status": "error",
                    "error": str(e),
                    "table": table_name,
                }

        return results

    def execute_load(self, batch_id: str) -> Dict:
        """Execute load of validated fragments into database"""
        logger.info(f"Executing load for batch: {batch_id}")

        # Get list of fragments
        fragments = self.s3_client.list_batch_fragments(batch_id)
        if not fragments:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        # Get exclude fields
        exclude_fields = self._get_exclude_fields(batch_id)
        logger.info(f"Execute load using exclude_fields: {exclude_fields}")

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
                # Download fragment
                data = self.s3_client.download_fragment(batch_id, table_name)

                # Get strategy and execute load
                strategy = self._get_load_strategy(table_name, exclude_fields)
                load_result = strategy.load(data, dry_run=False)

                # Track the load
                self._track_fragment_load(batch_id, table_name, s3_key, load_result)

                # Mark as loaded in S3
                self.s3_client.mark_fragment_loaded(batch_id, table_name)

                results["tables"][table_name] = {
                    "status": load_result["status"],
                    "rows_loaded": load_result.get("rows_loaded", 0),
                    "timestamp": datetime.utcnow().isoformat(),
                }

                logger.info(
                    f"Successfully loaded {load_result.get('rows_loaded', 0)} rows into {table_name}"
                )

            except Exception as e:
                logger.error(f"Error loading {table_name}: {e}")

                # Track the failed load
                error_result = {
                    "status": "failed",
                    "error": str(e),
                    "rows": 0,
                    "rows_loaded": 0,
                    "rows_failed": 0,
                }
                self._track_fragment_load(batch_id, table_name, s3_key, error_result)

                results["tables"][table_name] = {
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }

                # Re-raise to stop processing
                raise

        # Add summary statistics
        stats = self.resolution_service.get_load_statistics(batch_id)
        results["summary"] = stats

        return results
