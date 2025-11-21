# table-loader/services/loader.py
import logging
from typing import Dict

from core.database import get_db_connection

from services.data_transformer import DataTransformer
from services.fragment_resolution import FragmentResolutionService
from services.load_strategies import (
    LoadStrategy,
    StandardLoadStrategy,
    UpsertLoadStrategy,
)
from services.s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Loads validated fragments from S3 into database tables"""

    # Tables that use UPSERT strategy (with conflict columns)
    UPSERT_TABLES = {
        "lcl": ["global_subject_id", "knumber"],
        "blood": ["global_subject_id", "sample_id"],
        "dna": ["global_subject_id", "sample_id"],
        "rna": ["global_subject_id", "sample_id"],
        "serum": ["global_subject_id", "sample_id"],
        "plasma": ["global_subject_id", "sample_id"],
        "stool": ["global_subject_id", "sample_id"],
        "tissue": ["global_subject_id", "sample_id"],
        "local_subject_ids": ["center_id", "local_subject_id", "identifier_type"],
    }

    # Table-specific default exclusions (fields that should ALWAYS be excluded)
    TABLE_DEFAULT_EXCLUSIONS = {
        "local_subject_ids": {
            "action",  # GSID resolution metadata, not a DB column
        },
    }

    def __init__(self, s3_bucket: str = None):
        """
        Initialize TableLoader

        Args:
            s3_bucket: S3 bucket name for fragments (optional, uses settings default if None)
        """
        self.s3_client = S3Client(bucket=s3_bucket)
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

    def load_batch(self, batch_id: str, dry_run: bool = True) -> Dict:
        """
        Load a validated batch from S3 into database

        Args:
            batch_id: Batch identifier
            dry_run: If True, don't commit changes

        Returns:
            Load result summary
        """
        logger.info(f"Loading batch: {batch_id}")

        try:
            # Download validation report
            report = self.s3_client.download_validation_report(batch_id)

            # Validate report status
            if report.get("status") != "VALIDATED":
                raise ValueError(f"Batch {batch_id} is not validated")

            table_name = report["table_name"]
            source_name = report.get("source", "unknown")

            # Get exclude fields from validation report
            exclude_from_report = set(report.get("exclude_from_load", []))

            # Add table-specific default exclusions
            table_defaults = self.TABLE_DEFAULT_EXCLUSIONS.get(table_name, set())
            exclude_fields = exclude_from_report | table_defaults

            logger.info(f"Exclude fields from validation report: {exclude_from_report}")
            if table_defaults:
                logger.info(f"Table-specific exclusions: {table_defaults}")

            # Get resolved conflicts
            resolved_conflicts = self.resolution_service.get_resolved_conflicts(
                batch_id
            )
            logger.info(
                f"Found {len(resolved_conflicts)} resolved conflicts for batch {batch_id}"
            )

            # Build exclusion set from conflicts
            exclude_ids = set()
            if resolved_conflicts:
                for conflict in resolved_conflicts:
                    action = conflict.get("resolution_action")
                    local_id = conflict.get("local_subject_id")
                    id_type = conflict.get("identifier_type")

                    if action == "use_existing":
                        # Skip incoming record
                        exclude_ids.add((local_id, id_type))
                        logger.info(f"Will skip {id_type}={local_id} (use_existing)")
                    elif action == "manual_review":
                        # Skip for manual review
                        exclude_ids.add((local_id, id_type))
                        logger.info(f"Will skip {id_type}={local_id} (manual_review)")
                    elif action == "use_incoming":
                        logger.info(f"Will load {id_type}={local_id} (use_incoming)")

            # Download fragment data
            fragment_df = self.s3_client.download_fragment(batch_id, table_name)
            logger.info(f"Downloaded {len(fragment_df)} records")

            # Filter fragment based on conflicts
            if exclude_ids:
                # Find the identifier column (consortium_id, niddk_no, etc.)
                id_columns = [
                    col
                    for col in fragment_df.columns
                    if col in ["consortium_id", "niddk_no", "local_subject_id"]
                ]

                if id_columns:
                    id_col = id_columns[0]
                    original_count = len(fragment_df)
                    fragment_df = fragment_df[
                        ~fragment_df[id_col].isin(
                            [local_id for local_id, _ in exclude_ids]
                        )
                    ]
                    filtered_count = original_count - len(fragment_df)
                    if filtered_count > 0:
                        logger.info(
                            f"Filtered out {filtered_count} records from {table_name} based on conflict resolution"
                        )

            # Transform data
            transformer = DataTransformer(
                table_name=table_name, exclude_fields=exclude_fields
            )
            records = transformer.transform_records(fragment_df)

            # Get load strategy
            strategy = self._get_load_strategy(table_name, exclude_fields)

            # Load data
            conn = get_db_connection()
            try:
                result = strategy.load(
                    conn,
                    records,
                    batch_id,
                    source_name,
                )

                if not dry_run:
                    conn.commit()
                    logger.info(f"✓ Changes committed to {table_name}")
                else:
                    conn.rollback()
                    logger.info(f"✓ Dry run - changes rolled back")
            finally:
                conn.close()

            # Load local_subject_ids if present
            local_ids_result = None
            try:
                local_ids_result = self._load_local_subject_ids(
                    batch_id, dry_run, exclude_fields, source_name, exclude_ids
                )
            except Exception as e:
                logger.warning(f"Could not load local_subject_ids: {e}", exc_info=True)

            # Mark conflicts as applied after successful load
            if not dry_run and resolved_conflicts:
                try:
                    self.resolution_service.mark_conflicts_as_applied(batch_id)
                    logger.info(
                        f"Marked {len(resolved_conflicts)} conflicts as applied"
                    )
                except Exception as e:
                    logger.warning(f"Could not mark conflicts as applied: {e}")

            # Record load in fragment_resolutions
            if not dry_run:
                try:
                    self.resolution_service.record_load(
                        batch_id=batch_id,
                        table_name=table_name,
                        records_loaded=result.get("rows_loaded", 0),
                        status="loaded",
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not record load in fragment_resolutions: {e}"
                    )

            return {
                "status": "SUCCESS",
                "batch_id": batch_id,
                "table_name": table_name,
                "records_loaded": result.get("rows_loaded", 0),
                "inserted": result.get("inserted", result.get("rows_loaded", 0)),
                "updated": result.get("updated", 0),
                "local_ids_loaded": (
                    local_ids_result.get("rows_loaded", 0) if local_ids_result else 0
                ),
                "conflicts_resolved": len(resolved_conflicts),
            }

        except Exception as e:
            logger.error(f"Failed to load batch {batch_id}: {e}", exc_info=True)
            raise

    def _load_local_subject_ids(
        self,
        batch_id: str,
        dry_run: bool,
        exclude_fields: set = None,
        source_name: str = "unknown",
        exclude_ids: set = None,
    ) -> Dict:
        """Load local_subject_ids fragment if present"""
        try:
            fragment_df = self.s3_client.download_fragment(
                batch_id, "local_subject_ids"
            )

            if fragment_df.empty:
                logger.info("No local_subject_ids to load")
                return {"rows_loaded": 0}

            logger.info(f"Loading {len(fragment_df)} local subject ID mappings")

            # Filter local_subject_ids based on conflicts
            if exclude_ids:
                original_count = len(fragment_df)
                fragment_df = fragment_df[
                    ~fragment_df.apply(
                        lambda row: (row["local_subject_id"], row["identifier_type"])
                        in exclude_ids,
                        axis=1,
                    )
                ]
                filtered_count = original_count - len(fragment_df)
                if filtered_count > 0:
                    logger.info(
                        f"Filtered out {filtered_count} local_subject_ids based on conflict resolution"
                    )

            # Get table-specific exclusions for local_subject_ids
            table_defaults = self.TABLE_DEFAULT_EXCLUSIONS.get(
                "local_subject_ids", set()
            )
            exclude_fields = (exclude_fields or set()) | table_defaults

            # Transform data
            transformer = DataTransformer(
                table_name="local_subject_ids", exclude_fields=exclude_fields
            )
            records = transformer.transform_records(fragment_df)

            logger.info(
                f"Prepared {len(records)} local_subject_ids records for loading"
            )

            # Get load strategy (upsert handles conflicts automatically)
            strategy = self._get_load_strategy("local_subject_ids", exclude_fields)

            # Load data
            conn = get_db_connection()
            try:
                result = strategy.load(
                    conn,
                    records,
                    batch_id,
                    source_name,
                )

                if not dry_run:
                    conn.commit()
                    logger.info("✓ local_subject_ids changes committed")
                else:
                    conn.rollback()
                    logger.info("✓ local_subject_ids dry run - changes rolled back")

                return result
            finally:
                conn.close()

        except FileNotFoundError:
            logger.info("No local_subject_ids fragment found")
            return {"rows_loaded": 0}
