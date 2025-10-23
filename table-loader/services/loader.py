# table-loader/services/loader.py
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from core.config import settings

from .load_strategy import StandardLoadStrategy, UpsertLoadStrategy
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class TableLoader:
    """Main table loader orchestrator"""

    def __init__(self):
        self.s3_client = S3Client()
        self.strategies = self._init_strategies()

    def _init_strategies(self) -> Dict[str, Any]:
        """Initialize load strategies for different tables"""
        return {
            "subjects": UpsertLoadStrategy(
                "subjects",
                conflict_columns=["gsid"],
                update_columns=["center_id", "local_id", "updated_at"],
            ),
            "specimen": UpsertLoadStrategy(
                "specimen",
                conflict_columns=["subject_id", "sample_id"],
                update_columns=["sample_type", "updated_at"],
            ),
            "centers": StandardLoadStrategy("centers"),
            "phenotype": StandardLoadStrategy("phenotype"),
            "genotype": StandardLoadStrategy("genotype"),
        }

    def preview_load(
        self, batch_id: str, table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Preview load without executing"""
        logger.info(f"Previewing load for batch {batch_id}")

        if table:
            tables = [table]
        else:
            tables = self.s3_client.list_batch_fragments(batch_id)

        preview_results = {}

        for tbl in tables:
            try:
                data = self.s3_client.download_fragment(batch_id, tbl)
                strategy = self.strategies.get(tbl, StandardLoadStrategy(tbl))
                result = strategy.load(data, dry_run=True)
                preview_results[tbl] = result
            except Exception as e:
                logger.error(f"Error previewing {tbl}: {e}")
                preview_results[tbl] = {"status": "error", "error": str(e)}

        return preview_results

    def execute_load(
        self, batch_id: str, table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute load for batch"""
        logger.info(f"Executing load for batch {batch_id}")

        if table:
            tables = [table]
        else:
            tables = self.s3_client.list_batch_fragments(batch_id)

        load_results = {
            "batch_id": batch_id,
            "started_at": datetime.utcnow().isoformat(),
            "tables": {},
        }

        for tbl in tables:
            try:
                logger.info(f"Loading table: {tbl}")
                data = self.s3_client.download_fragment(batch_id, tbl)
                strategy = self.strategies.get(tbl, StandardLoadStrategy(tbl))
                result = strategy.load(data, dry_run=False)
                load_results["tables"][tbl] = result
                logger.info(f"Completed {tbl}: {result}")

            except Exception as e:
                logger.error(f"Error loading {tbl}: {e}", exc_info=True)
                load_results["tables"][tbl] = {"status": "error", "error": str(e)}

        load_results["completed_at"] = datetime.utcnow().isoformat()

        # Upload report
        self.s3_client.upload_load_report(batch_id, load_results)

        return load_results
