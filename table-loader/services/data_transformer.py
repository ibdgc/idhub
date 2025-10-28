# table-loader/services/data_transformer.py
import logging
from typing import Any, Dict, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms fragment data for database insertion"""

    def __init__(self, table_name: str, exclude_fields: set = None):
        self.table_name = table_name
        self.exclude_fields = exclude_fields or set()

    def transform_records(self, fragment: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform fragment records for database insertion"""
        records = fragment.get("records", [])

        if not records:
            logger.warning(f"No records found in fragment for {self.table_name}")
            return []

        # Get the first record to determine fields
        sample_record = records[0]

        # Determine which fields to keep (exclude resolution-only fields)
        fields_to_keep = set(sample_record.keys()) - self.exclude_fields

        # Always include global_subject_id if present
        if "global_subject_id" in sample_record:
            fields_to_keep.add("global_subject_id")

        logger.info(f"Fields to load for {self.table_name}: {sorted(fields_to_keep)}")

        if self.exclude_fields:
            excluded_present = self.exclude_fields & set(sample_record.keys())
            if excluded_present:
                logger.info(f"Excluded resolution fields: {sorted(excluded_present)}")

        # Filter records to only include fields we want to load
        transformed_records = []
        for record in records:
            filtered_record = {k: v for k, v in record.items() if k in fields_to_keep}
            transformed_records.append(filtered_record)

        logger.info(
            f"Transformed {len(transformed_records)} records for {self.table_name}"
        )
        return transformed_records

    def deduplicate(self, df: pd.DataFrame, key_columns: List[str]) -> pd.DataFrame:
        """Remove duplicate rows based on key columns"""
        original_count = len(df)
        df = df.drop_duplicates(subset=key_columns, keep="first")
        deduped_count = len(df)

        if original_count > deduped_count:
            logger.info(
                f"Deduplicated {original_count - deduped_count} rows from {self.table_name}"
            )

        return df

    def prepare_rows(self, df: pd.DataFrame) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """Prepare DataFrame for bulk insert"""
        columns = list(df.columns)
        values = [tuple(row) for row in df.values]
        return columns, values
