# table-loader/services/data_transformer.py
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Set, Union

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms fragment data for database loading"""

    # System columns that should never be loaded
    SYSTEM_COLUMNS = {
        "Id",
        "created_at",
        "updated_at",
        "CreatedAt",
        "UpdatedAt",
    }

    # Subject identifier fields that are stored in local_subject_ids table
    # These should NOT be loaded into data tables
    SUBJECT_ID_FIELDS = {
        "consortium_id",
        "local_subject_id",
        "niddk_no",
        "knumber",
        "local_id",
        "subject_id",
        "patient_id",
        "record_id",  # REDCap record ID
    }

    # Metadata fields that should not be loaded
    METADATA_FIELDS = {
        "center_id",
        "source",
        "batch_id",
    }

    def __init__(self, table_name: str, exclude_fields: Set[str] = None):
        """
        Initialize transformer

        Args:
            table_name: Target table name
            exclude_fields: Additional fields to exclude (from validation report)
        """
        self.table_name = table_name

        # Build complete exclusion set
        self.exclude_fields = set()
        self.exclude_fields.update(self.SYSTEM_COLUMNS)
        self.exclude_fields.update(self.METADATA_FIELDS)

        # For data tables (not local_subject_ids), exclude subject ID fields
        if table_name != "local_subject_ids":
            self.exclude_fields.update(self.SUBJECT_ID_FIELDS)

        # Add custom exclusions from validation report
        if exclude_fields:
            self.exclude_fields.update(exclude_fields)

    def transform_records(
        self, data: Union[pd.DataFrame, Dict]
    ) -> List[Dict[str, Any]]:
        """
        Transform data for database insertion

        Args:
            data: DataFrame or dict with 'records' key

        Returns:
            List of transformed record dictionaries
        """
        # Convert to DataFrame if needed
        if isinstance(data, dict):
            if "records" in data:
                df = pd.DataFrame(data["records"])
            else:
                df = pd.DataFrame([data])
        else:
            df = data

        if df.empty:
            logger.warning(f"No data to transform for {self.table_name}")
            return []

        # Get all columns
        all_columns = set(df.columns)

        # Determine which fields to keep
        fields_to_keep = all_columns - self.exclude_fields

        logger.info(f"Fields to load for {self.table_name}: {sorted(fields_to_keep)}")

        if self.exclude_fields & all_columns:
            logger.info(
                f"Excluding fields: {sorted(self.exclude_fields & all_columns)}"
            )

        # Convert DataFrame to records
        records = df.to_dict("records")

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

        return transformed_records
