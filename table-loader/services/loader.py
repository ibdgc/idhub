# table-loader/services/loader.py
import logging
from typing import Dict, Optional, Set

from core.database import get_db_connection

from services.data_transformer import DataTransformer
from services.fragment_resolution import FragmentResolutionService
from services.load_strategies import (
    LoadStrategy,
    StandardLoadStrategy,
    UniversalUpsertStrategy,
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
            # Use UniversalUpsertStrategy with proper change detection

            return UniversalUpsertStrategy(
                table_name=table_name,
                natural_key=self.UPSERT_TABLES[
                    table_name
                ],  # These ARE your natural keys
                exclude_fields=exclude_fields,
                changed_by="table_loader",
            )
        else:
            return StandardLoadStrategy(
                table_name=table_name, exclude_fields=exclude_fields
            )

    def load_batch(self, batch_id: str, dry_run: bool = True) -> Dict:
        """Load a validated batch from S3 into database"""
        logger.info(f"Loading batch: {batch_id}")

        try:
            # Download validation report
            report = self.s3_client.download_validation_report(batch_id)

            if report.get("status") != "VALIDATED":
                raise ValueError(f"Batch {batch_id} is not validated")

            table_name = report["table_name"]
            source_name = report.get("source", "unknown")

            # Get exclude fields
            exclude_from_report = set(report.get("exclude_from_load", []))
            table_defaults = self.TABLE_DEFAULT_EXCLUSIONS.get(table_name, set())
            exclude_fields = exclude_from_report | table_defaults

            # Get resolved conflicts
            resolved_conflicts = self.resolution_service.get_resolved_conflicts(
                batch_id
            )
            logger.info(
                f"Found {len(resolved_conflicts)} resolved conflicts for batch {batch_id}"
            )

            # 🔍 DEBUG: Log what we found
            logger.info(f"🔍 DEBUG: dry_run = {dry_run}")
            logger.info(f"🔍 DEBUG: resolved_conflicts = {len(resolved_conflicts)}")
            if resolved_conflicts:
                logger.info(f"🔍 DEBUG: First conflict: {resolved_conflicts[0]}")

            # Get database connection (reuse for all operations)
            conn = get_db_connection()
            try:
                # ✅ Step 1: Apply center updates to subjects table
                logger.info("🔍 DEBUG: Checking if we should update subjects...")
                if not dry_run and resolved_conflicts:
                    logger.info(
                        "🔍 DEBUG: YES - Calling apply_center_updates_to_subjects"
                    )
                    subjects_updated = (
                        self.resolution_service.apply_center_updates_to_subjects(
                            batch_id, conn
                        )
                    )
                    logger.info(
                        f"✅ Updated {subjects_updated} subjects with new center_id"
                    )
                else:
                    logger.info(
                        f"🔍 DEBUG: NO - Skipping subjects update. "
                        f"dry_run={dry_run}, has_conflicts={len(resolved_conflicts) > 0}"
                    )

                # Build exclusion set from conflicts
                exclude_ids = set()
                if resolved_conflicts:
                    for conflict in resolved_conflicts:
                        action = conflict.get("resolution_action")
                        local_id = conflict.get("local_subject_id")
                        id_type = conflict.get("identifier_type")

                        if action == "use_existing":
                            exclude_ids.add((local_id, id_type))
                            logger.info(
                                f"Will skip {id_type}={local_id} (use_existing)"
                            )
                        elif action == "manual_review":
                            exclude_ids.add((local_id, id_type))
                            logger.info(
                                f"Will skip {id_type}={local_id} (manual_review)"
                            )
                        elif action == "use_incoming":
                            logger.info(
                                f"Will load {id_type}={local_id} (use_incoming)"
                            )

                # Download fragment data
                fragment_df = self.s3_client.download_fragment(batch_id, table_name)
                logger.info(f"Downloaded {len(fragment_df)} records")

                # Filter fragment based on conflicts
                if exclude_ids:
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
                                f"Filtered out {filtered_count} records from {table_name}"
                            )

                # Transform data
                transformer = DataTransformer(
                    table_name=table_name, exclude_fields=exclude_fields
                )
                records = transformer.transform_records(fragment_df)

                # Get load strategy
                strategy = self._get_load_strategy(table_name, exclude_fields)

                # Load main table data
                result = strategy.load(conn, records, batch_id, source_name)

                # ✅ Step 2: Handle local_subject_ids separately
                local_ids_result = None
                try:
                    # First, delete old local_subject_ids records for center conflicts
                    if not dry_run and resolved_conflicts:
                        local_ids_updated = (
                            self.resolution_service.apply_center_updates_to_local_ids(
                                batch_id, conn
                            )
                        )
                        logger.info(
                            f"Prepared {local_ids_updated} local_subject_id records for update"
                        )

                    # Then load new/updated records
                    local_ids_result = self._load_local_subject_ids(
                        batch_id=batch_id,
                        dry_run=dry_run,
                        exclude_fields=None,
                        source_name=source_name,
                        exclude_ids=exclude_ids,
                        conn=conn,  # ✅ Pass same connection
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not load local_subject_ids: {e}", exc_info=True
                    )

                # Commit or rollback
                if not dry_run:
                    conn.commit()
                    logger.info(f"✓ Changes committed to {table_name}")

                    # Mark conflicts as applied
                    if resolved_conflicts:
                        self.resolution_service.mark_conflicts_as_applied(batch_id)
                        logger.info(
                            f"Marked {len(resolved_conflicts)} conflicts as applied"
                        )

                    # Record load
                    self.resolution_service.record_load(
                        batch_id=batch_id,
                        table_name=table_name,
                        records_loaded=result.get("rows_loaded", 0),
                        status="success",
                    )
                else:
                    conn.rollback()
                    logger.info(f"✓ Dry run - changes rolled back")

            finally:
                conn.close()

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
        exclude_fields: Optional[set],
        source_name: str,
        exclude_ids: set,
        conn=None,
    ) -> Optional[Dict]:
        """Load local_subject_ids fragment if present"""
        try:
            # Download local_subject_ids fragment
            local_ids_df = self.s3_client.download_fragment(
                batch_id, "local_subject_ids"
            )

            if local_ids_df.empty:
                logger.info("No local_subject_ids fragment found")
                return None

            logger.info(f"Downloaded {len(local_ids_df)} local_subject_id records")

            # Filter based on conflict resolutions
            if exclude_ids:
                original_count = len(local_ids_df)
                local_ids_df = local_ids_df[
                    ~local_ids_df.apply(
                        lambda row: (row["local_subject_id"], row["identifier_type"])
                        in exclude_ids,
                        axis=1,
                    )
                ]
                filtered_count = original_count - len(local_ids_df)
                if filtered_count > 0:
                    logger.info(
                        f"Filtered out {filtered_count} local_subject_id records based on conflict resolution"
                    )

            # Add table-specific exclusions
            table_defaults = self.TABLE_DEFAULT_EXCLUSIONS.get(
                "local_subject_ids", set()
            )
            all_exclusions = (exclude_fields or set()) | table_defaults

            # Transform data
            transformer = DataTransformer(
                table_name="local_subject_ids", exclude_fields=all_exclusions
            )
            records = transformer.transform_records(local_ids_df)

            logger.info(
                f"Prepared {len(records)} local_subject_ids records for loading"
            )

            # Get load strategy
            strategy = self._get_load_strategy("local_subject_ids", all_exclusions)

            # Load data using provided or new connection
            should_close = False
            if conn is None:
                conn = get_db_connection()
                should_close = True

            try:
                result = strategy.load(conn, records, batch_id, source_name)

                if not dry_run:
                    if should_close:
                        conn.commit()
                    logger.info(f"✓ local_subject_ids changes committed")
                else:
                    if should_close:
                        conn.rollback()
                    logger.info(f"✓ local_subject_ids dry run - changes rolled back")

                return result

            finally:
                if should_close:
                    conn.close()

        except FileNotFoundError:
            logger.info("No local_subject_ids fragment in batch")
            return None
        except Exception as e:
            logger.error(f"Failed to load local_subject_ids: {e}", exc_info=True)
            raise
