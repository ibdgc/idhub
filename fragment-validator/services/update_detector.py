# fragment-validator/services/update_detector.py
import logging
from typing import Dict, List, Optional, Set

import pandas as pd

from .nocodb_client import NocoDBClient

logger = logging.getLogger(__name__)


class UpdateDetector:
    """Detects changes between incoming data and existing database records via NocoDB API"""

    # Natural keys for each table (used to match records)
    TABLE_NATURAL_KEYS = {
        "lcl": ["global_subject_id", "niddk_no"],
        "olink": ["global_subject_id", "sample_id"],
        "specimen": ["global_subject_id", "sample_id"],
        "enteroid": ["global_subject_id", "sample_id"],
        "sequence": ["global_subject_id", "sample_id"],
        "genotype": ["global_subject_id", "sample_id"],
    }

    # Fields to ignore when comparing for changes
    IGNORE_FIELDS = {
        "Id",
        "created_at",
        "updated_at",
        "CreatedAt",
        "UpdatedAt",
    }

    def __init__(self, nocodb_client: NocoDBClient):
        self.nocodb_client = nocodb_client

    def detect_changes(
        self, table_name: str, incoming_data: pd.DataFrame
    ) -> Dict[str, any]:
        """
        Detect changes between incoming data and existing records via NocoDB API

        Args:
            table_name: Target table name
            incoming_data: DataFrame with incoming records

        Returns:
            Dictionary with change analysis:
            {
                "summary": {
                    "total_incoming": int,
                    "new": int,
                    "updated": int,
                    "unchanged": int,
                    "orphaned": int
                },
                "new_records": List[Dict],
                "updates": List[Dict],
                "unchanged": List[Dict],
                "orphaned": List[Dict]
            }
        """
        logger.info(f"Analyzing changes for table '{table_name}' via NocoDB API")

        # Get natural key for this table
        natural_key = self.TABLE_NATURAL_KEYS.get(table_name)
        if not natural_key:
            logger.warning(
                f"No natural key defined for table '{table_name}', treating all as new"
            )
            return self._all_new_analysis(incoming_data)

        logger.info(f"Using natural key: {natural_key}")

        # Fetch existing records from NocoDB
        try:
            existing_records = self.nocodb_client.get_all_records(table_name)
            logger.info(f"Fetched {len(existing_records)} existing records from NocoDB")
        except Exception as e:
            logger.warning(
                f"Failed to fetch existing data from NocoDB: {e}. Treating all as new."
            )
            return self._all_new_analysis(incoming_data)

        if not existing_records:
            logger.info(f"No existing data in '{table_name}' - all records are new")
            return self._all_new_analysis(incoming_data)

        # Convert to DataFrame for easier comparison
        existing_df = pd.DataFrame(existing_records)

        # Perform comparison
        return self._compare_dataframes(
            incoming_data, existing_df, natural_key, table_name
        )

    def _all_new_analysis(self, incoming_data: pd.DataFrame) -> Dict:
        """Return analysis indicating all records are new"""
        new_records = incoming_data.to_dict("records")
        return {
            "summary": {
                "total_incoming": len(incoming_data),
                "new": len(incoming_data),
                "updated": 0,
                "unchanged": 0,
                "orphaned": 0,
            },
            "new_records": new_records,
            "updates": [],
            "unchanged": [],
            "orphaned": [],
        }

    def _compare_dataframes(
        self,
        incoming_df: pd.DataFrame,
        existing_df: pd.DataFrame,
        natural_key: List[str],
        table_name: str,
    ) -> Dict:
        """Compare incoming and existing DataFrames to detect changes"""

        # Verify natural key columns exist
        missing_in_incoming = [k for k in natural_key if k not in incoming_df.columns]
        missing_in_existing = [k for k in natural_key if k not in existing_df.columns]

        if missing_in_incoming:
            logger.warning(
                f"Natural key columns missing in incoming data: {missing_in_incoming}"
            )
            return self._all_new_analysis(incoming_df)

        if missing_in_existing:
            logger.warning(
                f"Natural key columns missing in existing data: {missing_in_existing}"
            )
            return self._all_new_analysis(incoming_df)

        # Create composite keys for matching
        incoming_df["_composite_key"] = incoming_df[natural_key].apply(
            lambda row: tuple(row), axis=1
        )
        existing_df["_composite_key"] = existing_df[natural_key].apply(
            lambda row: tuple(row), axis=1
        )

        # Build lookup of existing records
        existing_lookup = {
            row["_composite_key"]: row for row in existing_df.to_dict("records")
        }

        # Classify records
        new_records = []
        updates = []
        unchanged = []

        for incoming_record in incoming_df.to_dict("records"):
            composite_key = incoming_record["_composite_key"]

            # Remove temporary key from record
            incoming_clean = {
                k: v for k, v in incoming_record.items() if k != "_composite_key"
            }

            if composite_key not in existing_lookup:
                # New record
                new_records.append(incoming_clean)
            else:
                # Existing record - check if changed
                existing_record = existing_lookup[composite_key]
                if self._records_differ(incoming_clean, existing_record):
                    updates.append(
                        {
                            "incoming": incoming_clean,
                            "existing": existing_record,
                            "changes": self._get_field_changes(
                                incoming_clean, existing_record
                            ),
                        }
                    )
                else:
                    unchanged.append(incoming_clean)

        # Find orphaned records (in DB but not in incoming data)
        incoming_keys = set(incoming_df["_composite_key"])
        existing_keys = set(existing_df["_composite_key"])
        orphaned_keys = existing_keys - incoming_keys

        orphaned = [
            {k: v for k, v in existing_lookup[key].items() if k != "_composite_key"}
            for key in orphaned_keys
        ]

        logger.info(
            f"Change detection complete: {len(new_records)} new, "
            f"{len(updates)} updated, {len(unchanged)} unchanged, "
            f"{len(orphaned)} orphaned"
        )

        # Clean up temporary columns from incoming_df (IMPORTANT!)
        # This ensures _composite_key doesn't get uploaded to S3
        if "_composite_key" in incoming_df.columns:
            incoming_df.drop(columns=["_composite_key"], inplace=True)

        return {
            "summary": {
                "total_incoming": len(incoming_df),
                "new": len(new_records),
                "updated": len(updates),
                "unchanged": len(unchanged),
                "orphaned": len(orphaned),
            },
            "new_records": new_records,
            "updates": updates,
            "unchanged": unchanged,
            "orphaned": orphaned,
        }

    def _records_differ(self, record1: Dict, record2: Dict) -> bool:
        """Check if two records differ (ignoring system fields)"""

        # Get comparable fields (exclude system fields and fields not in both records)
        fields1 = set(record1.keys()) - self.IGNORE_FIELDS
        fields2 = set(record2.keys()) - self.IGNORE_FIELDS
        common_fields = fields1 & fields2

        for field in common_fields:
            val1 = record1.get(field)
            val2 = record2.get(field)

            # Normalize None/NaN/empty string
            val1 = None if pd.isna(val1) or val1 == "" else val1
            val2 = None if pd.isna(val2) or val2 == "" else val2

            if val1 != val2:
                return True

        return False

    def _get_field_changes(self, incoming: Dict, existing: Dict) -> List[Dict]:
        """Get list of field-level changes"""
        changes = []

        fields = (set(incoming.keys()) | set(existing.keys())) - self.IGNORE_FIELDS

        for field in fields:
            val_incoming = incoming.get(field)
            val_existing = existing.get(field)

            # Normalize
            val_incoming = (
                None if pd.isna(val_incoming) or val_incoming == "" else val_incoming
            )
            val_existing = (
                None if pd.isna(val_existing) or val_existing == "" else val_existing
            )

            if val_incoming != val_existing:
                changes.append(
                    {
                        "field": field,
                        "old_value": val_existing,
                        "new_value": val_incoming,
                    }
                )

        return changes
