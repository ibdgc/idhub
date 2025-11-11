# table-loader/services/data_transformer.py
import logging
from typing import Any, Dict, List, Set, Tuple, Union

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms fragment data for database insertion"""

    def __init__(self, table_name: str, exclude_fields: set = None):
        self.table_name = table_name
        self.exclude_fields = exclude_fields or set()

    def transform_records(
        self, fragment: Union[pd.DataFrame, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Transform fragment records for database insertion

        Args:
            fragment: Either a DataFrame or a dict with 'records' key

        Returns:
            List of record dictionaries ready for database insertion
        """
        # Handle DataFrame input
        if isinstance(fragment, pd.DataFrame):
            if fragment.empty:
                logger.warning(f"No records found in fragment for {self.table_name}")
                return []

            # Convert DataFrame to list of dicts
            records = fragment.to_dict("records")
        else:
            # Handle dict input (legacy format)
            records = fragment.get("records", [])
            if not records:
                logger.warning(f"No records found in fragment for {self.table_name}")
                return []

        # Get the first record to determine fields
        sample_record = records[0]

        # Only exclude fields that actually exist in the data
        # This prevents excluding required fields that aren't in the source
        fields_present = set(sample_record.keys())
        exclude_fields_present = self.exclude_fields & fields_present

        # Determine which fields to keep
        fields_to_keep = fields_present - exclude_fields_present

        # Always include global_subject_id if present
        if "global_subject_id" in sample_record:
            fields_to_keep.add("global_subject_id")

        logger.info(f"Fields to load for {self.table_name}: {sorted(fields_to_keep)}")

        if exclude_fields_present:
            logger.info(f"Excluded resolution fields: {sorted(exclude_fields_present)}")

        # Log if exclude_fields contains fields not in the data
        exclude_fields_not_present = self.exclude_fields - fields_present
        if exclude_fields_not_present:
            logger.debug(
                f"Exclude list contains fields not in data (ignored): {sorted(exclude_fields_not_present)}"
            )

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
