# table-loader/services/load_strategy.py
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd
from core.database import db_manager

from .data_transformer import DataTransformer

logger = logging.getLogger(__name__)


class LoadStrategy(ABC):
    """Abstract base class for table load strategies"""

    def __init__(self, table_name: str, exclude_fields: set = None):
        self.table_name = table_name
        self.transformer = DataTransformer(table_name, exclude_fields)

    @abstractmethod
    def load(self, data: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
        """Execute load strategy"""
        pass


class StandardLoadStrategy(LoadStrategy):
    """Standard load strategy for most tables"""

    def load(self, data: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
        # Transform records (apply exclusions)
        records = self.transformer.transform_records(data)

        if not records:
            return {"status": "skipped", "reason": "no records", "rows_loaded": 0}

        # Convert to DataFrame for deduplication
        df = pd.DataFrame(records)

        # Deduplicate if key columns specified
        key_columns = data.get("metadata", {}).get("key_columns", [])
        if key_columns:
            df = self.transformer.deduplicate(df, key_columns)

        columns, values = self.transformer.prepare_rows(df)

        if dry_run:
            return {
                "status": "preview",
                "table": self.table_name,
                "rows": len(values),
                "columns": columns,
                "sample": values[:5] if len(values) > 0 else [],
            }

        # Database connection happens here, not at import
        with db_manager.get_connection() as conn:
            db_manager.bulk_insert(conn, self.table_name, columns, values)

        return {
            "status": "success",
            "table": self.table_name,
            "rows_loaded": len(values),
        }


class UpsertLoadStrategy(LoadStrategy):
    """Upsert strategy for tables with conflict resolution"""

    def __init__(
        self,
        table_name: str,
        conflict_columns: List[str],
        update_columns: List[str],
        exclude_fields: set = None,
    ):
        super().__init__(table_name, exclude_fields)
        self.conflict_columns = conflict_columns
        self.update_columns = update_columns

    def load(self, data: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
        # Transform records
        records = self.transformer.transform_records(data)

        if not records:
            return {"status": "skipped", "reason": "no records", "rows_loaded": 0}

        df = pd.DataFrame(records)
        columns, values = self.transformer.prepare_rows(df)

        if dry_run:
            return {
                "status": "preview",
                "table": self.table_name,
                "rows": len(values),
                "strategy": "upsert",
                "conflict_on": self.conflict_columns,
                "sample": values[:5] if len(values) > 0 else [],
            }

        # Database connection happens here
        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn, cursor_factory=None) as cursor:
                conflict_clause = f"({', '.join(self.conflict_columns)})"
                update_clause = ", ".join(
                    [f"{col} = EXCLUDED.{col}" for col in self.update_columns]
                )

                query = f"""
                    INSERT INTO {self.table_name} ({", ".join(columns)})
                    VALUES %s
                    ON CONFLICT {conflict_clause}
                    DO UPDATE SET {update_clause}
                """

                from psycopg2.extras import execute_values

                execute_values(cursor, query, values)

        return {
            "status": "success",
            "table": self.table_name,
            "rows_loaded": len(values),
        }
