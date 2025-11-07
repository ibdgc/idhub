# table-loader/services/load_strategy.py
import logging
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


class StandardLoadStrategy(LoadStrategy):
    """Standard INSERT strategy"""

    def load(self, fragment: pd.DataFrame, dry_run: bool = False) -> Dict:
        """Execute standard insert load"""
        # Transform data
        records = self.transformer.transform_records(fragment)

        if not records:
            return {
                "status": "skipped",
                "reason": "no records",
                "table": self.table_name,
            }

        if dry_run:
            columns = list(records[0].keys()) if records else []
            return {
                "status": "preview",
                "table": self.table_name,
                "rows": len(records),
                "columns": columns,
                "sample": records[:5],
            }

        # Execute insert
        with db_manager.get_connection() as conn:
            # Get columns from first record
            columns = list(records[0].keys())
            values = [[rec[col] for col in columns] for rec in records]

            db_manager.bulk_insert(conn, self.table_name, columns, values)
            conn.commit()

        return {
            "status": "success",
            "table": self.table_name,
            "rows_loaded": len(records),
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
        # Transform data
        records = self.transformer.transform_records(fragment)

        if not records:
            return {
                "status": "skipped",
                "reason": "no records",
                "table": self.table_name,
            }

        if dry_run:
            columns = list(records[0].keys()) if records else []
            return {
                "status": "preview",
                "strategy": "upsert",
                "table": self.table_name,
                "rows": len(records),
                "columns": columns,
                "conflict_on": self.conflict_columns,
                "sample": records[:5],
            }

        # Execute upsert
        from psycopg2.extras import execute_values

        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get columns from first record
                columns = list(records[0].keys())
                values = [[rec[col] for col in columns] for rec in records]

                # Build upsert query
                placeholders = ", ".join(["%s"] * len(columns))
                column_names = ", ".join(columns)

                # Determine which columns to update
                if self.update_columns:
                    update_cols = self.update_columns
                else:
                    # Update all columns except conflict columns
                    update_cols = [c for c in columns if c not in self.conflict_columns]

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

        return {
            "status": "success",
            "strategy": "upsert",
            "table": self.table_name,
            "rows_loaded": len(records),
        }
