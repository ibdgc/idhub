# table-loader/services/loader.py
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

from core.database import get_db_connection

from services.data_transformer import DataTransformer
from services.fragment_resolution_service import FragmentResolutionService
from services.load_strategies import (
    LoadStrategy,
    StandardLoadStrategy,
    UniversalUpsertStrategy,
    UpsertLoadStrategy,
)
from services.s3_client import S3Client

logger = logging.getLogger(__name__)


class FragmentLoader:
    """Loads validated fragments from S3 into database tables"""

    # System columns to always exclude
    SYSTEM_COLUMNS = {
        "Id",
        "CreatedAt",
        "UpdatedAt",
        "created_at",
        "updated_at",
    }

    # Table configurations with natural keys
    TABLE_CONFIGS = {
        "blood": {
            "natural_key": ["global_subject_id", "sample_id"],
            "strategy": "universal_upsert",
        },
        "dna": {
            "natural_key": ["global_subject_id", "sample_id"],
            "strategy": "universal_upsert",
        },
        "rna": {
            "natural_key": ["global_subject_id", "sample_id"],
            "strategy": "universal_upsert",
        },
        "plasma": {
            "natural_key": ["global_subject_id", "sample_id"],
            "strategy": "universal_upsert",
        },
        "serum": {
            "natural_key": ["global_subject_id", "sample_id"],
            "strategy": "universal_upsert",
        },
        "stool": {
            "natural_key": ["global_subject_id", "sample_id"],
            "strategy": "universal_upsert",
        },
        "lcl": {
            "natural_key": ["global_subject_id", "niddk_no"],
            "strategy": "universal_upsert",
        },
        "specimen": {"natural_key": ["sample_id"], "strategy": "universal_upsert"},
        "local_subject_ids": {
            "natural_key": ["center_id", "local_subject_id", "identifier_type"],
            "strategy": "universal_upsert",
        },
        "subjects": {
            "natural_key": ["global_subject_id"],
            "strategy": "universal_upsert",
        },
    }

    # Legacy UPSERT tables (backward compatibility)
    LEGACY_UPSERT_TABLES = {
        "local_subject_ids": ["center_id", "local_subject_id", "identifier_type"],
    }

    # Table-specific default exclusions
    TABLE_DEFAULT_EXCLUSIONS = {
        "local_subject_ids": {"action"},
    }

    def __init__(self):
        self.s3_client = S3Client()
        self.resolution_service = FragmentResolutionService()

    def _get_load_strategy(
        self,
        table_name: str,
        exclude_fields: set,
        batch_id: str,
        changed_by: str = "table_loader",
    ) -> LoadStrategy:
        """Get appropriate load strategy for table"""

        # Check if table has configuration
        if table_name in self.TABLE_CONFIGS:
            config = self.TABLE_CONFIGS[table_name]

            if config["strategy"] == "universal_upsert":
                return UniversalUpsertStrategy(
                    table_name=table_name,
                    natural_key=config["natural_key"],
                    exclude_fields=exclude_fields,
                    changed_by=changed_by,
                )

        # Legacy upsert tables
        if table_name in self.LEGACY_UPSERT_TABLES:
            return UpsertLoadStrategy(
                table_name=table_name,
                conflict_columns=self.LEGACY_UPSERT_TABLES[table_name],
                exclude_fields=exclude_fields,
            )

        # Default to standard insert
        return StandardLoadStrategy(
            table_name=table_name, exclude_fields=exclude_fields
        )

    def _extract_table_name(self, s3_key: str) -> str:
        """Extract table name from S3 key"""
        # Example: staging/validated/batch_20240115_120000/blood.csv -> blood
        parts = s3_key.split("/")
        filename = parts[-1]
        return filename.replace(".csv", "")

    def _get_exclude_fields(self, table_name: str) -> set:
        """Get fields to exclude for a table"""
        exclude = self.SYSTEM_COLUMNS.copy()

        # Add table-specific exclusions
        if table_name in self.TABLE_DEFAULT_EXCLUSIONS:
            exclude.update(self.TABLE_DEFAULT_EXCLUSIONS[table_name])

        return exclude

    def load_batch(
        self, batch_id: str, preview: bool = False, changed_by: str = "table_loader"
    ) -> Dict:
        """
        Load all fragments for a batch

        Args:
            batch_id: Batch identifier
            preview: If True, don't commit changes
            changed_by: Source identifier for audit log
        """
        logger.info(f"Loading batch: {batch_id} (preview={preview})")

        # List all CSV files in batch
        prefix = f"staging/validated/{batch_id}/"
        fragments = self.s3_client.list_fragments(prefix)

        if not fragments:
            logger.warning(f"No fragments found for batch {batch_id}")
            return {
                "batch_id": batch_id,
                "status": "no_fragments",
                "fragments_loaded": 0,
                "total_rows": 0,
            }

        # Filter out validation report
        csv_fragments = [
            f for f in fragments if f.endswith(".csv") and "validation_report" not in f
        ]

        logger.info(f"Found {len(csv_fragments)} fragments to load")

        conn = get_db_connection()
        results = []

        try:
            for s3_key in csv_fragments:
                table_name = self._extract_table_name(s3_key)
                logger.info(f"Loading fragment: {table_name} from {s3_key}")

                result = self._load_fragment(
                    conn=conn,
                    s3_key=s3_key,
                    table_name=table_name,
                    batch_id=batch_id,
                    changed_by=changed_by,
                )
                results.append(result)

                # Log to fragment_resolutions table
                self.resolution_service.log_fragment_resolution(
                    conn=conn,
                    batch_id=batch_id,
                    table_name=table_name,
                    fragment_key=s3_key,
                    load_result=result,
                )

            if preview:
                logger.info("Preview mode: Rolling back transaction")
                conn.rollback()
            else:
                logger.info("Committing transaction")
                conn.commit()

            summary = self._build_summary(batch_id, results, preview)
            logger.info(f"Batch load complete: {summary}")

            return summary

        except Exception as e:
            logger.error(f"Batch load failed: {e}", exc_info=True)
            conn.rollback()
            raise
        finally:
            conn.close()

    def _load_fragment(
        self,
        conn,
        s3_key: str,
        table_name: str,
        batch_id: str,
        changed_by: str,
    ) -> Dict:
        """Load a single fragment"""
        start_time = datetime.now()

        try:
            # Download fragment
            df = self.s3_client.download_dataframe(s3_key)
            logger.info(f"Downloaded {len(df)} rows from {s3_key}")

            # Transform data
            exclude_fields = self._get_exclude_fields(table_name)
            transformer = DataTransformer(
                table_name=table_name, exclude_fields=exclude_fields
            )
            records = transformer.transform_records(df)

            # Get load strategy
            strategy = self._get_load_strategy(
                table_name, exclude_fields, batch_id, changed_by
            )

            # Load records
            load_result = strategy.load(conn, records, batch_id, s3_key)

            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return {
                "table_name": table_name,
                "fragment_key": s3_key,
                "load_status": "success"
                if load_result["rows_failed"] == 0
                else "partial",
                "load_strategy": strategy.__class__.__name__,
                "rows_attempted": load_result["rows_attempted"],
                "rows_loaded": load_result["rows_loaded"],
                "rows_failed": load_result["rows_failed"],
                "rows_inserted": load_result.get("rows_inserted", 0),
                "rows_updated": load_result.get("rows_updated", 0),
                "rows_unchanged": load_result.get("rows_unchanged", 0),
                "execution_time_ms": execution_time,
                "errors": load_result.get("errors", []),
            }

        except Exception as e:
            logger.error(f"Failed to load fragment {s3_key}: {e}", exc_info=True)
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return {
                "table_name": table_name,
                "fragment_key": s3_key,
                "load_status": "failed",
                "load_strategy": "unknown",
                "rows_attempted": 0,
                "rows_loaded": 0,
                "rows_failed": 0,
                "execution_time_ms": execution_time,
                "errors": [str(e)],
            }

    def _build_summary(self, batch_id: str, results: List[Dict], preview: bool) -> Dict:
        """Build summary of batch load"""
        total_rows = sum(r["rows_loaded"] for r in results)
        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        total_updated = sum(r.get("rows_updated", 0) for r in results)
        total_unchanged = sum(r.get("rows_unchanged", 0) for r in results)
        failed_fragments = [r for r in results if r["load_status"] == "failed"]

        return {
            "batch_id": batch_id,
            "status": "preview"
            if preview
            else ("success" if not failed_fragments else "partial"),
            "preview_mode": preview,
            "fragments_loaded": len(results),
            "fragments_failed": len(failed_fragments),
            "total_rows_loaded": total_rows,
            "total_rows_inserted": total_inserted,
            "total_rows_updated": total_updated,
            "total_rows_unchanged": total_unchanged,
            "fragments": results,
        }
