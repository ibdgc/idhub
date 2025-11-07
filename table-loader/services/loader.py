# table-loader/services/loader.py
import logging
from datetime import datetime
from typing import Dict, List

from botocore.exceptions import ClientError

from .data_transformer import DataTransformer
from .load_strategy import LoadStrategy, StandardLoadStrategy, UpsertLoadStrategy
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Orchestrates loading of validated data fragments into database"""

    # Tables that should use upsert strategy
    UPSERT_TABLES = {"subject"}

    def __init__(self):
        self.s3_client = S3Client()

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
            exclude_fields = set(report.get("exclude_fields", []))
            logger.info(
                f"Loaded exclude_fields from validation report: {exclude_fields}"
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
            }

    def preview_load(self, batch_id: str) -> Dict:
        """Preview what would be loaded without executing"""
        fragments = self.s3_client.list_batch_fragments(batch_id)

        if not fragments:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        exclude_fields = self._get_exclude_fields(batch_id)
        results = {}

        for fragment in fragments:
            table_name = fragment["Key"].split("/")[-1].replace(".csv", "")

            try:
                # Download fragment
                data = self.s3_client.download_fragment(batch_id, table_name)

                # Get strategy and preview
                strategy = self._get_load_strategy(table_name, exclude_fields)
                preview = strategy.load(data, dry_run=True)

                results[table_name] = preview

            except Exception as e:
                logger.error(f"Error previewing {table_name}: {e}")
                results[table_name] = {
                    "status": "error",
                    "error": str(e),
                }

        return results

    def execute_load(self, batch_id: str) -> Dict:
        """Execute actual data load"""
        fragments = self.s3_client.list_batch_fragments(batch_id)

        if not fragments:
            raise ValueError(f"No table fragments found for batch {batch_id}")

        exclude_fields = self._get_exclude_fields(batch_id)
        results = {
            "batch_id": batch_id,
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
        }

        for fragment in fragments:
            table_name = fragment["Key"].split("/")[-1].replace(".csv", "")

            try:
                # Download fragment
                data = self.s3_client.download_fragment(batch_id, table_name)

                # Get strategy and execute load
                strategy = self._get_load_strategy(table_name, exclude_fields)
                load_result = strategy.load(data, dry_run=False)

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
                results["tables"][table_name] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                # Stop on first error
                raise

        return results
