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
            "source",  # Legacy field name, should be created_by
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
            s3_location = report["s3_location"]
            source_name = report.get("source", "unknown")

            # Get exclude fields from validation report
            exclude_from_report = set(report.get("exclude_from_load", []))

            # Add table-specific default exclusions
            table_defaults = self.TABLE_DEFAULT_EXCLUSIONS.get(table_name, set())
            exclude_fields = exclude_from_report | table_defaults

            logger.info(f"Exclude fields from validation report: {exclude_from_report}")
            if table_defaults:
                logger.info(f"Table-specific exclusions: {table_defaults}")

            # Download fragment data
            fragment_df = self.s3_client.download_fragment(batch_id, table_name)
            logger.info(f"Downloaded {len(fragment_df)} records")

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

                    # Mark conflicts as applied after successful commit
                    try:
                        self.resolution_service.mark_conflicts_as_applied(batch_id)
                    except Exception as e:
                        logger.warning(f"Could not mark conflicts as applied: {e}")
                else:
                    conn.rollback()
                    logger.info(f"✓ Dry run - changes rolled back")

            finally:
                conn.close()

            # Load local_subject_ids if present
            local_ids_result = None
            try:
                local_ids_result = self._load_local_subject_ids(
                    batch_id, dry_run, exclude_fields, source_name
                )
            except Exception as e:
                logger.warning(f"Could not load local_subject_ids: {e}")

            return {
                "status": "SUCCESS",
                "batch_id": batch_id,
                "table_name": table_name,
                "records_loaded": result.get("rows_loaded", 0),
                "inserted": result.get("rows_loaded", 0),
                "updated": 0,
                "local_ids_loaded": (
                    local_ids_result.get("rows_loaded", 0) if local_ids_result else 0
                ),
            }

        except Exception as e:
            logger.error(f"Failed to load batch {batch_id}: {e}")
            raise

    def _load_local_subject_ids(
        self,
        batch_id: str,
        dry_run: bool,
        exclude_fields: set = None,
        source_name: str = "unknown",
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

            # ✅ Apply conflict resolutions
            records = self.resolution_service.apply_conflict_resolutions(
                records, batch_id
            )

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
