# fragment-validator/services/update_detector.py
import logging
from typing import Dict, List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class UpdateDetector:
    """Detect changes between incoming data and current database state"""

    def __init__(self, db_config: Optional[Dict] = None):
        """
        Initialize update detector

        Args:
            db_config: Database connection config (host, database, user, password, port)
                      If None, will use environment variables
        """
        self.db_config = db_config or self._get_db_config_from_env()

    def _get_db_config_from_env(self) -> Dict:
        """Get database config from environment variables"""
        import os

        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "database": os.getenv("DB_NAME", "idhub"),
            "user": os.getenv("DB_USER", "idhub_user"),
            "password": os.getenv("DB_PASSWORD", ""),
            "port": int(os.getenv("DB_PORT", "5432")),
        }

    def analyze_changes(
        self, incoming_data: pd.DataFrame, table_name: str, natural_key: List[str]
    ) -> Dict:
        """
        Compare incoming data against current database state

        Args:
            incoming_data: DataFrame with new/updated data
            table_name: Target table name
            natural_key: List of columns that uniquely identify a record

        Returns:
            {
                "new_records": [...],
                "updates": [...],
                "unchanged": [...],
                "orphaned": [...],
                "summary": {...}
            }
        """
        logger.info(
            f"Analyzing changes for table '{table_name}' with natural key {natural_key}"
        )

        # Validate natural key exists in incoming data
        missing_keys = [k for k in natural_key if k not in incoming_data.columns]
        if missing_keys:
            raise ValueError(f"Natural key columns missing from data: {missing_keys}")

        # Fetch current database state
        current_data = self._fetch_current_data(table_name, natural_key)

        if current_data.empty:
            # No existing data - everything is new
            logger.info(f"No existing data in '{table_name}' - all records are new")
            return {
                "new_records": incoming_data.to_dict("records"),
                "updates": [],
                "unchanged": [],
                "orphaned": [],
                "summary": {
                    "total_incoming": len(incoming_data),
                    "new": len(incoming_data),
                    "updated": 0,
                    "unchanged": 0,
                    "orphaned": 0,
                },
            }

        # Perform comparison
        result = self._compare_dataframes(incoming_data, current_data, natural_key)

        logger.info(
            f"Change analysis complete: {result['summary']['new']} new, "
            f"{result['summary']['updated']} updated, "
            f"{result['summary']['unchanged']} unchanged"
        )

        return result

    def _fetch_current_data(
        self, table_name: str, natural_key: List[str]
    ) -> pd.DataFrame:
        """Fetch current data from database"""
        try:
            conn = psycopg2.connect(**self.db_config)

            # Get all columns for the table
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
                columns = [desc[0] for desc in cursor.description]

            # Fetch all data
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)

            conn.close()

            logger.info(f"Fetched {len(df)} existing records from '{table_name}'")
            return df

        except psycopg2.Error as e:
            logger.error(f"Database error fetching current data: {e}")
            # Return empty DataFrame on error (treat all as new)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching current data: {e}")
            return pd.DataFrame()

    def _compare_dataframes(
        self, incoming: pd.DataFrame, current: pd.DataFrame, natural_key: List[str]
    ) -> Dict:
        """Compare incoming data against current database state"""

        new_records = []
        updates = []
        unchanged = []

        # Create a key for matching
        incoming["_merge_key"] = (
            incoming[natural_key].astype(str).agg("||".join, axis=1)
        )
        current["_merge_key"] = current[natural_key].astype(str).agg("||".join, axis=1)

        # Get common columns (exclude system columns)
        exclude_cols = {"_merge_key", "created_at", "updated_at", "id", "Id"}
        common_cols = [
            c
            for c in incoming.columns
            if c in current.columns and c not in exclude_cols
        ]

        for idx, incoming_row in incoming.iterrows():
            merge_key = incoming_row["_merge_key"]

            # Check if record exists in current data
            matching_rows = current[current["_merge_key"] == merge_key]

            if matching_rows.empty:
                # New record
                record = incoming_row.drop("_merge_key").to_dict()
                new_records.append(record)
            else:
                # Existing record - check for changes
                current_row = matching_rows.iloc[0]
                changes = self._detect_field_changes(
                    incoming_row, current_row, common_cols
                )

                if changes:
                    # Record has changes
                    natural_key_values = {k: incoming_row[k] for k in natural_key}
                    updates.append(
                        {
                            "natural_key": natural_key_values,
                            "changes": changes,
                            "record": incoming_row.drop("_merge_key").to_dict(),
                        }
                    )
                else:
                    # No changes
                    unchanged.append(incoming_row.drop("_merge_key").to_dict())

        # Detect orphaned records (in DB but not in incoming)
        incoming_keys = set(incoming["_merge_key"])
        current_keys = set(current["_merge_key"])
        orphaned_keys = current_keys - incoming_keys

        orphaned = []
        for key in orphaned_keys:
            orphaned_row = current[current["_merge_key"] == key].iloc[0]
            orphaned.append(orphaned_row.drop("_merge_key").to_dict())

        return {
            "new_records": new_records,
            "updates": updates,
            "unchanged": unchanged,
            "orphaned": orphaned,
            "summary": {
                "total_incoming": len(incoming),
                "new": len(new_records),
                "updated": len(updates),
                "unchanged": len(unchanged),
                "orphaned": len(orphaned),
            },
        }

    def _detect_field_changes(
        self, incoming_row: pd.Series, current_row: pd.Series, fields: List[str]
    ) -> Dict:
        """Detect which fields changed between two rows"""
        changes = {}

        for field in fields:
            incoming_val = incoming_row.get(field)
            current_val = current_row.get(field)

            # Handle NaN/None comparisons
            incoming_is_null = pd.isna(incoming_val)
            current_is_null = pd.isna(current_val)

            if incoming_is_null and current_is_null:
                # Both null - no change
                continue

            if incoming_is_null != current_is_null:
                # One is null, other isn't - this is a change
                changes[field] = {
                    "old": None if current_is_null else current_val,
                    "new": None if incoming_is_null else incoming_val,
                }
            elif incoming_val != current_val:
                # Both have values but they differ
                changes[field] = {"old": current_val, "new": incoming_val}

        return changes

    def format_change_summary(self, changes: Dict) -> str:
        """Format change summary for display"""
        summary = changes["summary"]
        lines = [
            "\nChange Summary:",
            "=" * 60,
            f"Total incoming records: {summary['total_incoming']}",
            f"  New records:          {summary['new']}",
            f"  Updated records:      {summary['updated']}",
            f"  Unchanged records:    {summary['unchanged']}",
            f"  Orphaned records:     {summary['orphaned']}",
            "",
        ]

        if changes["updates"]:
            lines.append("Updates Preview:")
            lines.append("-" * 60)
            for i, update in enumerate(changes["updates"][:5], 1):  # Show first 5
                key_str = ", ".join(
                    f"{k}={v}" for k, v in update["natural_key"].items()
                )
                lines.append(f"\n  Record {i}: {key_str}")
                for field, change in update["changes"].items():
                    lines.append(f"    - {field}: {change['old']} → {change['new']}")

            if len(changes["updates"]) > 5:
                lines.append(f"\n  ... and {len(changes['updates']) - 5} more updates")

        return "\n".join(lines)
