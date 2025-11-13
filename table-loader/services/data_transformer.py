# table-loader/services/data_transformer.py
import logging
from typing import Any, Dict, List, Set, Tuple, Union

import numpy as np
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

            # CRITICAL: Filter out records with invalid global_subject_id
            if "global_subject_id" in fragment.columns:
                original_count = len(fragment)

                # Remove rows where global_subject_id is NaN, None, or empty
                fragment = fragment[
                    fragment["global_subject_id"].notna()
                    & (fragment["global_subject_id"] != "")
                    & (fragment["global_subject_id"] != "NaN")
                    & (fragment["global_subject_id"] != "nan")
                ]

                filtered_count = original_count - len(fragment)
                if filtered_count > 0:
                    logger.warning(
                        f"Filtered out {filtered_count} records with invalid global_subject_id "
                        f"from {self.table_name} (NaN, None, or empty values)"
                    )

                if fragment.empty:
                    logger.error(
                        f"All records in {self.table_name} have invalid global_subject_id - "
                        f"nothing to load"
                    )
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
            # Additional validation: skip records with invalid global_subject_id
            if "global_subject_id" in record:
                gsid = record.get("global_subject_id")

                # Check for various forms of invalid values
                if (
                    gsid is None
                    or gsid == ""
                    or pd.isna(gsid)
                    or str(gsid).lower() == "nan"
                ):
                    logger.warning(
                        f"Skipping record with invalid global_subject_id: {gsid}"
                    )
                    continue

            filtered_record = {k: v for k, v in record.items() if k in fields_to_keep}
            transformed_records.append(filtered_record)

        logger.info(
            f"Transformed {len(transformed_records)} records for {self.table_name}"
        )

        if len(transformed_records) == 0 and len(records) > 0:
            logger.error(
                f"All {len(records)} records were filtered out due to invalid data"
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
