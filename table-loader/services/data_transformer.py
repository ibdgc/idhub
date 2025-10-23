# table-loader/services/data_transformer.py
import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform fragment data for database insertion"""

    def __init__(self, table_name: str):
        self.table_name = table_name

    def prepare_rows(self, df: pd.DataFrame) -> Tuple[List[str], List[Tuple]]:
        """Prepare DataFrame rows for bulk insert"""
        # Add metadata columns
        df["loaded_at"] = datetime.utcnow()
        df["loaded_by"] = "table-loader"

        # Handle NaN values
        df = df.where(pd.notnull(df), None)

        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]

        logger.info(f"Prepared {len(values)} rows with {len(columns)} columns")
        return columns, values

    def validate_foreign_keys(
        self, df: pd.DataFrame, fk_config: Dict[str, Any]
    ) -> List[str]:
        """Validate foreign key constraints"""
        errors = []

        for fk_col, ref_info in fk_config.items():
            if fk_col not in df.columns:
                continue

            ref_table = ref_info["table"]
            ref_column = ref_info["column"]

            # Check for null values in required FK columns
            if ref_info.get("required", False):
                null_count = df[fk_col].isnull().sum()
                if null_count > 0:
                    errors.append(f"{fk_col}: {null_count} null values in required FK")

        return errors

    def deduplicate(self, df: pd.DataFrame, key_columns: List[str]) -> pd.DataFrame:
        """Remove duplicate rows based on key columns"""
        initial_count = len(df)
        df = df.drop_duplicates(subset=key_columns, keep="first")
        final_count = len(df)

        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate rows")

        return df
