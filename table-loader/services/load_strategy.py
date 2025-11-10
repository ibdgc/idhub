# table-loader/services/load_strategy.py
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set

import pandas as pd
from core.database import db_manager

from .data_transformer import DataTransformer

logger = logging.getLogger(__name__)


class LoadStrategy(ABC):
    """Abstract base class for load strategies"""

    def __init__(self, table_name: str, exclude_fields: Optional[Set[str]] = None):
        self.table_name = table_name
        self.exclude_fields = exclude_fields or set()
        self.transformer = DataTransformer(table_name, self.exclude_fields)

    @abstractmethod
    def load(self, fragment: pd.DataFrame, dry_run: bool = False) -> Dict:
        """Execute load strategy"""
        pass

    def _get_strategy_name(self) -> str:
        """Get strategy name for tracking"""
        return self.__class__.__name__.replace("LoadStrategy", "").lower()


class StandardLoadStrategy(LoadStrategy):
    """Standard INSERT strategy"""

    def load(self, fragment: pd.DataFrame, dry_run: bool = False) -> Dict:
        """Execute standard insert load"""
        start_time = time.time()

        # Transform data
        records = self.transformer.transform_records(fragment)
        rows_attempted = len(records)

        if not records:
            return {
                "status": "skipped",
                "reason": "no records",
                "table": self.table_name,
                "strategy": "standard_insert",
                "rows_attempted": 0,
                "rows_loaded": 0,
                "rows_failed": 0,
                "execution_time_ms": int((time.time() - start_time) * 1000),
            }

        if dry_run:
            columns = list(records[0].keys()) if records else []
            return {
                "status": "preview",
                "table": self.table_name,
                "strategy": "standard_insert",
                "rows": len(records),
                "rows_attempted": rows_attempted,
                "columns": columns,
                "sample": records[:5],
                "execution_time_ms": int((time.time() - start_time) * 1000),
            }

        # Execute insert
        rows_loaded = 0
        rows_failed = 0
        error_message = None

        try:
            with db_manager.get_connection() as conn:
                # Get columns from first record
                columns = list(records[0].keys())
                values = [[rec[col] for col in columns] for rec in records]
                db_manager.bulk_insert(conn, self.table_name, columns, values)
                conn.commit()
                rows_loaded = len(records)
        except Exception as e:
            rows_failed = rows_attempted
            error_message = str(e)
            logger.error(f"Failed to load {self.table_name}: {e}")
            raise

        execution_time_ms = int((time.time() - start_time) * 1000)

        return {
            "status": "success" if rows_failed == 0 else "failed",
            "table": self.table_name,
            "strategy": "standard_insert",
            "rows_attempted": rows_attempted,
            "rows_loaded": rows_loaded,
            "rows_failed": rows_failed,
            "execution_time_ms": execution_time_ms,
            "error_message": error_message,
        }


class UpsertLoadStrategy(LoadStrategy):
    """UPSERT strategy for tables with unique constraints"""

    def __init__(
        self,
        table_name: str,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        exclude_fields: Optional[Set[str]] = None,
    ):
        super().__init__(table_name, exclude_fields)
        self.conflict_columns = conflict_columns
        self.update_columns = update_columns

    def load(self, fragment: pd.DataFrame, dry_run: bool = False) -> Dict:
        """Execute upsert load"""
        start_time = time.time()

        # Transform data
        records = self.transformer.transform_records(fragment)
        rows_attempted = len(records)

        if not records:
            return {
                "status": "skipped",
                "reason": "no records",
                "table": self.table_name,
                "strategy": "upsert",
                "rows_attempted": 0,
                "rows_loaded": 0,
                "rows_failed": 0,
                "execution_time_ms": int((time.time() - start_time) * 1000),
            }

        if dry_run:
            columns = list(records[0].keys()) if records else []
            return {
                "status": "preview",
                "strategy": "upsert",
                "table": self.table_name,
                "rows": len(records),
                "rows_attempted": rows_attempted,
                "columns": columns,
                "conflict_on": self.conflict_columns,
                "sample": records[:5],
                "execution_time_ms": int((time.time() - start_time) * 1000),
            }

        # Execute upsert
        rows_loaded = 0
        rows_failed = 0
        error_message = None

        try:
            from psycopg2.extras import execute_values

            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Get columns from first record
                    columns = list(records[0].keys())
                    values = [[rec[col] for col in columns] for rec in records]

                    # Build upsert query
                    column_names = ", ".join(columns)

                    # Determine which columns to update
                    if self.update_columns:
                        update_cols = self.update_columns
                    else:
                        # Update all columns except conflict columns
                        update_cols = [
                            c for c in columns if c not in self.conflict_columns
                        ]

                    update_clause = ", ".join(
                        [f"{col} = EXCLUDED.{col}" for col in update_cols]
                    )
                    conflict_clause = ", ".join(self.conflict_columns)

                    query = f"""
                        INSERT INTO {self.table_name} ({column_names})
                        VALUES %s
                        ON CONFLICT ({conflict_clause})
                        DO UPDATE SET {update_clause}
                    """

                    execute_values(cursor, query, values)
                    conn.commit()
                    rows_loaded = len(records)
        except Exception as e:
            rows_failed = rows_attempted
            error_message = str(e)
            logger.error(f"Failed to upsert {self.table_name}: {e}")
            raise

        execution_time_ms = int((time.time() - start_time) * 1000)

        return {
            "status": "success" if rows_failed == 0 else "failed",
            "strategy": "upsert",
            "table": self.table_name,
            "rows_attempted": rows_attempted,
            "rows_loaded": rows_loaded,
            "rows_failed": rows_failed,
            "execution_time_ms": execution_time_ms,
            "error_message": error_message,
        }
