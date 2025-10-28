# table-loader/services/loader.py
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from .load_strategy import StandardLoadStrategy, UpsertLoadStrategy
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Main table loader orchestrator"""

    def __init__(self):
        self.s3_client = S3Client()

    def _get_strategy(
        self, table: str, exclude_fields: set = None
    ) -> StandardLoadStrategy:
        """Get load strategy for a table"""
        # Define table-specific strategies here
        strategies = {
            # Example: if you need upsert logic for specific tables
            # "subjects": UpsertLoadStrategy(
            #     "subjects",
            #     conflict_columns=["global_subject_id"],
            #     update_columns=["updated_at"],
            #     exclude_fields=exclude_fields,
            # ),
        }

        # Default to StandardLoadStrategy
        return strategies.get(
            table, StandardLoadStrategy(table, exclude_fields=exclude_fields)
        )

    def _get_exclude_fields(self, batch_id: str) -> set:
        """Get fields to exclude from validation report"""
        try:
            report = self.s3_client.download_validation_report(batch_id)

            exclude_fields = set()

            # Option 1: Use explicit exclude_from_load if present (preferred)
            if "exclude_from_load" in report:
                exclude_fields.update(report["exclude_from_load"])
                logger.info(
                    f"Using explicit exclude_from_load: {report['exclude_from_load']}"
                )
            else:
                # Option 2: Derive from subject_id_candidates and center_id_field
                subject_id_candidates = report.get("subject_id_candidates", [])
                if subject_id_candidates:
                    exclude_fields.update(subject_id_candidates)
                    logger.info(
                        f"Excluding subject_id_candidates: {subject_id_candidates}"
                    )

                center_id_field = report.get("center_id_field")
                if center_id_field:
                    exclude_fields.add(center_id_field)
                    logger.info(f"Excluding center_id_field: {center_id_field}")

            # Always exclude these resolution-only fields
            exclude_fields.update(["identifier_type", "action", "local_subject_id"])

            logger.info(f"Total fields to exclude: {sorted(exclude_fields)}")
            return exclude_fields

        except Exception as e:
            logger.warning(
                f"Could not load validation report: {e}. Using default exclusions."
            )
            # Default exclusions
            return {"identifier_type", "action", "local_subject_id"}

    def preview_load(
        self, batch_id: str, table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Preview load without executing"""
        logger.info(f"Previewing load for batch {batch_id}")

        tables = [table] if table else self.s3_client.list_batch_fragments(batch_id)

        if not tables:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        # Get exclusion fields once for the batch
        exclude_fields = self._get_exclude_fields(batch_id)

        preview_results = {}
        for tbl in tables:
            try:
                logger.info(f"\nPreviewing table: {tbl}")

                # Download fragment
                data = self.s3_client.download_fragment(batch_id, tbl)

                # Get strategy with exclusions
                strategy = self._get_strategy(tbl, exclude_fields)

                # Preview
                result = strategy.load(data, dry_run=True)
                preview_results[tbl] = result

                logger.info(f"Preview result: {result}")

            except Exception as e:
                logger.error(f"Error previewing {tbl}: {e}")
                preview_results[tbl] = {"status": "error", "error": str(e)}

        return preview_results

    def execute_load(
        self, batch_id: str, table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute load for batch"""
        logger.info(f"Executing load for batch {batch_id}")

        tables = [table] if table else self.s3_client.list_batch_fragments(batch_id)

        if not tables:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        # Get exclusion fields once for the batch
        exclude_fields = self._get_exclude_fields(batch_id)

        load_results = {
            "batch_id": batch_id,
            "started_at": datetime.utcnow().isoformat(),
            "tables": {},
        }

        for tbl in tables:
            try:
                logger.info("")
                logger.info("=" * 60)
                logger.info(f"Loading table: {tbl}")
                logger.info("=" * 60)

                # Download fragment
                data = self.s3_client.download_fragment(batch_id, tbl)

                # Get strategy with exclusions
                strategy = self._get_strategy(tbl, exclude_fields)

                # Execute load
                result = strategy.load(data, dry_run=False)
                load_results["tables"][tbl] = result

                logger.info(f"✓ Completed {tbl}: {result}")

                # Mark as loaded
                self.s3_client.mark_fragment_loaded(batch_id, tbl)

            except Exception as e:
                logger.error(f"Error loading {tbl}: {e}", exc_info=True)
                load_results["tables"][tbl] = {"status": "error", "error": str(e)}
                raise  # Stop on first error

        load_results["completed_at"] = datetime.utcnow().isoformat()

        logger.info("")
        logger.info("=" * 60)
        logger.info(f"✓ Batch {batch_id} loaded successfully")
        logger.info("=" * 60)

        return load_results
