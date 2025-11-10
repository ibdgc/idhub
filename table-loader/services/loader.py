# table-loader/services/loader.py
import logging
from datetime import datetime
from typing import Dict, List

from botocore.exceptions import ClientError

from .data_transformer import DataTransformer
from .fragment_resolution import FragmentResolutionService
from .load_strategy import LoadStrategy, StandardLoadStrategy, UpsertLoadStrategy
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Orchestrates loading of validated data fragments into database"""

    # Tables that should use upsert strategy
    UPSERT_TABLES = {"subject"}

    def __init__(self):
        self.s3_client = S3Client()
        self.resolution_service = FragmentResolutionService()

    def _get_load_strategy(self, table_name: str, exclude_fields: set) -> LoadStrategy:
        """Get appropriate load strategy for table"""
        if table_name in self.UPSERT_TABLES:
            return UpsertLoadStrategy(
                table_name=table_name,
                conflict_columns=["global_subject_id"],
                exclude_fields=exclude_fields,
            )
        return StandardLoadStrategy(
            table_name=table_name, exclude_fields=exclude_fields
        )

    def _get_exclude_fields(self, batch_id: str) -> set:
        """Get fields to exclude from validation report"""
        try:
            report = self.s3_client.download_validation_report(batch_id)
            # Changed from "exclude_fields" to "exclude_from_load"
            exclude_fields = set(report.get("exclude_from_load", []))
            logger.info(
                f"Loaded exclude_from_load from validation report: {exclude_fields}"
            )
            return exclude_fields
        except (FileNotFoundError, ClientError) as e:
            logger.warning(
                f"Could not load validation report: {e}. Using default exclusions."
            )
            # Default exclusions if report not found
            return {
                "identifier_type",
                "action",
                "local_subject_id",
                "consortium_id",
                "local_id",
                "match_strategy",
                "confidence",
                "Id",
                "created_at",
                "updated_at",
            }
        except Exception as e:
            logger.warning(
                f"Could not load validation report: {e}. Using default exclusions."
            )
            return {
                "identifier_type",
                "action",
                "local_subject_id",
                "consortium_id",
                "local_id",
                "match_strategy",
                "confidence",
                "Id",
                "created_at",
                "updated_at",
            }

    def _determine_review_requirement(
        self, load_result: Dict, table_name: str
    ) -> tuple[bool, str]:
        """Determine if a load result requires manual review

        Returns:
            (requires_review, review_reason)
        """
        # Failed loads always require review
        if load_result.get("status") == "failed":
            return (
                True,
                f"Load failed: {load_result.get('error_message', 'Unknown error')}",
            )

        # Partial loads (some rows failed) require review
        rows_failed = load_result.get("rows_failed", 0)
        if rows_failed > 0:
            rows_attempted = load_result.get("rows_attempted", 0)
            return True, f"Partial load: {rows_failed}/{rows_attempted} rows failed"

        # No rows loaded might require review
        rows_loaded = load_result.get("rows_loaded", 0)
        if rows_loaded == 0 and load_result.get("status") != "skipped":
            return True, "No rows loaded despite non-skipped status"

        return False, None

    def _track_fragment_load(
        self, batch_id: str, table_name: str, fragment_key: str, load_result: Dict
    ):
        """Track fragment load in fragment_resolutions table"""
        requires_review, review_reason = self._determine_review_requirement(
            load_result, table_name
        )

        # Extract metadata
        metadata = {
            "s3_key": fragment_key,
            "columns": load_result.get("columns", []),
            "sample_data": load_result.get("sample", []),
        }

        try:
            self.resolution_service.create_resolution(
                batch_id=batch_id,
                table_name=table_name,
                fragment_key=fragment_key,
                load_status=load_result.get("status", "unknown"),
                load_strategy=load_result.get("strategy", "unknown"),
                rows_attempted=load_result.get("rows_attempted", 0),
                rows_loaded=load_result.get("rows_loaded", 0),
                rows_failed=load_result.get("rows_failed", 0),
                execution_time_ms=load_result.get("execution_time_ms"),
                error_message=load_result.get("error_message"),
                requires_review=requires_review,
                review_reason=review_reason,
                metadata=metadata,
            )
        except Exception as e:
            logger.error(f"Failed to track fragment load: {e}")
            # Don't fail the entire load if tracking fails

    def preview_load(self, batch_id: str) -> Dict:
        """Preview what would be loaded without executing"""
        fragments = self.s3_client.list_batch_fragments(batch_id)
        if not fragments:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        exclude_fields = self._get_exclude_fields(batch_id)
        logger.info(f"Preview using exclude_fields: {exclude_fields}")

        results = {}

        for fragment in fragments:
            table_name = fragment["Key"].split("/")[-1].replace(".csv", "")
            fragment_key = fragment["Key"]

            try:
                # Download fragment
                data = self.s3_client.download_fragment(batch_id, table_name)

                # Get strategy and preview
                strategy = self._get_load_strategy(table_name, exclude_fields)
                preview = strategy.load(data, dry_run=True)

                # Track preview
                self._track_fragment_load(batch_id, table_name, fragment_key, preview)

                results[table_name] = preview

            except Exception as e:
                logger.error(f"Error previewing {table_name}: {e}")
                error_result = {
                    "status": "error",
                    "error": str(e),
                    "strategy": "unknown",
                    "rows_attempted": 0,
                    "rows_loaded": 0,
                    "rows_failed": 0,
                }
                results[table_name] = error_result

                # Track error
                self._track_fragment_load(
                    batch_id, table_name, fragment_key, error_result
                )

        return results

    def execute_load(self, batch_id: str) -> Dict:
        """Execute actual data load"""
        fragments = self.s3_client.list_batch_fragments(batch_id)
        if not fragments:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        exclude_fields = self._get_exclude_fields(batch_id)
        logger.info(f"Execute load using exclude_fields: {exclude_fields}")

        results = {
            "batch_id": batch_id,
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
        }

        for fragment in fragments:
            table_name = fragment["Key"].split("/")[-1].replace(".csv", "")
            fragment_key = fragment["Key"]

            try:
                # Download fragment
                data = self.s3_client.download_fragment(batch_id, table_name)

                # Get strategy and execute load
                strategy = self._get_load_strategy(table_name, exclude_fields)
                load_result = strategy.load(data, dry_run=False)

                # Track the load
                self._track_fragment_load(
                    batch_id, table_name, fragment_key, load_result
                )

                # Mark as loaded in S3
                self.s3_client.mark_fragment_loaded(batch_id, table_name)

                results["tables"][table_name] = {
                    "status": load_result["status"],
                    "rows_loaded": load_result.get("rows_loaded", 0),
                    "rows_failed": load_result.get("rows_failed", 0),
                    "execution_time_ms": load_result.get("execution_time_ms"),
                    "timestamp": datetime.utcnow().isoformat(),
                }

                logger.info(
                    f"Successfully loaded {load_result.get('rows_loaded', 0)} rows into {table_name}"
                )

            except Exception as e:
                logger.error(f"Error loading {table_name}: {e}")
                error_result = {
                    "status": "failed",
                    "error_message": str(e),
                    "strategy": "unknown",
                    "rows_attempted": 0,
                    "rows_loaded": 0,
                    "rows_failed": 0,
                }

                # Track the error
                self._track_fragment_load(
                    batch_id, table_name, fragment_key, error_result
                )

                results["tables"][table_name] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }

                # Stop on first error
                raise

        # Add summary statistics
        stats = self.resolution_service.get_load_statistics(batch_id)
        results["statistics"] = stats

        return results
