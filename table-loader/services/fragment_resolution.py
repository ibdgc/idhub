# table-loader/services/fragment_resolution.py
import logging
from typing import Dict, List, Optional

import pandas as pd
from core.database import db_manager, get_db_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class FragmentResolutionService:
    """
    Analyzes changes between incoming fragments and current database state
    """

    def __init__(self):
        self.db_manager = db_manager

    def analyze_changes(
        self,
        table_name: str,
        incoming_data: pd.DataFrame,
        natural_key: List[str],
    ) -> Dict:
        """
        Compare incoming data with current database state

        Args:
            table_name: Target table name
            incoming_data: DataFrame with new data
            natural_key: List of columns that form natural key

        Returns:
            Dictionary with change analysis:
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

        try:
            # Fetch current data from database
            current_data = self._fetch_current_data(table_name)

            if current_data.empty:
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
            new_records = []
            updates = []
            unchanged = []

            # Create lookup index for current data
            current_index = self._create_index(current_data, natural_key)
            incoming_index = self._create_index(incoming_data, natural_key)

            # Check each incoming record
            for idx, row in incoming_data.iterrows():
                key = tuple(row[col] for col in natural_key)

                if key not in current_index:
                    # New record
                    new_records.append(row.to_dict())
                else:
                    # Existing record - check for changes
                    current_row = current_data.iloc[current_index[key]]
                    changes = self._detect_changes(row, current_row)

                    if changes:
                        updates.append(
                            {
                                "natural_key": dict(zip(natural_key, key)),
                                "changes": changes,
                            }
                        )
                    else:
                        unchanged.append(dict(zip(natural_key, key)))

            # Find orphaned records (in DB but not in incoming)
            orphaned = []
            for key in current_index:
                if key not in incoming_index:
                    orphaned.append(dict(zip(natural_key, key)))

            summary = {
                "total_incoming": len(incoming_data),
                "new": len(new_records),
                "updated": len(updates),
                "unchanged": len(unchanged),
                "orphaned": len(orphaned),
            }

            logger.info(
                f"Change detection complete: "
                f"{summary['new']} new, "
                f"{summary['updated']} updated, "
                f"{summary['unchanged']} unchanged"
            )

            return {
                "new_records": new_records,
                "updates": updates,
                "unchanged": unchanged,
                "orphaned": orphaned,
                "summary": summary,
            }

        except Exception as e:
            logger.error(f"Failed to analyze changes: {e}", exc_info=True)
            raise

    def get_resolved_conflicts(self, batch_id: str) -> List[Dict]:
        """
        Get all resolved conflicts for a batch from NocoDB

        Args:
            batch_id: Batch identifier

        Returns:
            List of resolved conflict records
        """
        logger.info(f"Fetching resolved conflicts for batch {batch_id}...")

        try:
            # Get all conflicts from NocoDB
            all_conflicts = self.nocodb_client.get_all_records("conflict_resolutions")

            # Filter to this batch and resolved status
            resolved_conflicts = [
                c
                for c in all_conflicts
                if c.get("batch_id") == batch_id and c.get("status") == "resolved"
            ]

            logger.info(
                f"Found {len(resolved_conflicts)} resolved conflicts for batch {batch_id}"
            )

            # Debug: show what we found
            if resolved_conflicts:
                for conflict in resolved_conflicts[:5]:  # Show first 5
                    logger.info(
                        f"  - {conflict.get('local_subject_id')} "
                        f"({conflict.get('identifier_type')}): "
                        f"action={conflict.get('resolution_action')}"
                    )

            return resolved_conflicts

        except Exception as e:
            logger.error(f"Failed to fetch resolved conflicts: {e}")
            return []

    def apply_conflict_resolutions(
        self, records: List[Dict], batch_id: str
    ) -> List[Dict]:
        """
        Filter records based on conflict resolutions

        Args:
            records: List of records to load
            batch_id: Batch identifier

        Returns:
            Filtered list of records to load
        """
        resolutions = self.get_resolved_conflicts(batch_id)

        if not resolutions:
            logger.info("No resolved conflicts found - loading all records")
            return records

        filtered = []
        skipped = 0
        updated = 0

        for record in records:
            local_id = record.get("local_subject_id")
            id_type = record.get("identifier_type", "primary")
            key = f"{local_id}:{id_type}"

            resolution = resolutions.get(key)

            if resolution == "keep_existing":
                # Skip this record - keep what's in the database
                logger.debug(f"Skipping {key} - keeping existing record")
                skipped += 1
                continue
            elif resolution == "use_incoming":
                # Load this record (will upsert)
                logger.debug(f"Loading {key} - using incoming data")
                filtered.append(record)
                updated += 1
            elif resolution == "delete_both":
                # Skip this record - will be manually deleted
                logger.debug(f"Skipping {key} - marked for deletion")
                skipped += 1
                continue
            elif resolution == "merge":
                # Load this record - merge logic handled by upsert
                logger.debug(f"Loading {key} - merging data")
                filtered.append(record)
                updated += 1
            elif resolution is None:
                # No conflict for this record - load normally
                filtered.append(record)
            else:
                logger.warning(
                    f"Unknown resolution action '{resolution}' for {key} - loading anyway"
                )
                filtered.append(record)

        logger.info(
            f"Applied conflict resolutions: {len(filtered)} to load, "
            f"{skipped} skipped, {updated} resolved conflicts"
        )
        return filtered

    def _fetch_current_data(self, table_name: str) -> pd.DataFrame:
        """Fetch current data from database table"""
        try:
            query = f"SELECT * FROM {table_name}"
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()

            if not results:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(results)
            logger.info(f"Fetched {len(df)} existing records from '{table_name}'")
            return df

        except Exception as e:
            logger.error(f"Database error fetching current data: {e}")
            # Return empty DataFrame on error (treat as no existing data)
            return pd.DataFrame()

    def _create_index(self, df: pd.DataFrame, key_columns: List[str]) -> Dict:
        """Create lookup index from DataFrame using natural key"""
        index = {}
        for idx, row in df.iterrows():
            key = tuple(row[col] for col in key_columns)
            index[key] = idx
        return index

    def _detect_changes(self, new_row: pd.Series, current_row: pd.Series) -> Dict:
        """
        Detect changes between two rows

        Returns:
            Dictionary of changed fields: {field: {"old": value, "new": value}}
        """
        changes = {}
        for col in new_row.index:
            if col not in current_row.index:
                continue

            new_val = new_row[col]
            current_val = current_row[col]

            # Handle NaN/None comparison
            if pd.isna(new_val) and pd.isna(current_val):
                continue

            if new_val != current_val:
                changes[col] = {
                    "old": current_val,
                    "new": new_val,
                }

        return changes

    def record_load(
        self,
        batch_id: str,
        table_name: str,
        records_loaded: int,
        status: str = "success",
        rows_attempted: int = None,
        rows_failed: int = 0,
        error_message: str = None,
    ) -> None:
        """
        Record a successful load in fragment_resolutions table

        Args:
            batch_id: Batch identifier
            table_name: Table that was loaded
            records_loaded: Number of records successfully loaded
            status: Load status (success, partial, failed, skipped, preview)
            rows_attempted: Total rows attempted (defaults to records_loaded + rows_failed)
            rows_failed: Number of failed rows
            error_message: Error message if load failed
        """
        try:
            from core.database import get_db_connection

            # Validate status
            valid_statuses = ["success", "partial", "failed", "skipped", "preview"]
            if status not in valid_statuses:
                logger.warning(f"Invalid status '{status}', defaulting to 'success'")
                status = "success"

            # Calculate rows_attempted if not provided
            if rows_attempted is None:
                rows_attempted = records_loaded + rows_failed

            # Determine load strategy based on table
            load_strategy = (
                "upsert"
                if table_name
                in [
                    "lcl",
                    "blood",
                    "dna",
                    "rna",
                    "serum",
                    "plasma",
                    "stool",
                    "tissue",
                    "local_subject_ids",
                ]
                else "standard_insert"
            )

            # Build fragment key
            fragment_key = f"staging/validated/{batch_id}/{table_name}.csv"

            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO fragment_resolutions 
                            (batch_id, table_name, fragment_key, load_status, load_strategy,
                             rows_attempted, rows_loaded, rows_failed, error_message, created_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'table_loader')
                        ON CONFLICT (batch_id, table_name, fragment_key) 
                        DO UPDATE SET
                            load_status = EXCLUDED.load_status,
                            rows_attempted = EXCLUDED.rows_attempted,
                            rows_loaded = EXCLUDED.rows_loaded,
                            rows_failed = EXCLUDED.rows_failed,
                            error_message = EXCLUDED.error_message,
                            created_at = CURRENT_TIMESTAMP
                        """,
                        (
                            batch_id,
                            table_name,
                            fragment_key,
                            status,
                            load_strategy,
                            rows_attempted,
                            records_loaded,
                            rows_failed,
                            error_message,
                        ),
                    )
                conn.commit()
                logger.info(
                    f"Recorded load for batch {batch_id}/{table_name}: "
                    f"{records_loaded}/{rows_attempted} rows loaded"
                )
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Failed to record load in fragment_resolutions: {e}")
            # Don't raise - this is just for tracking

    def mark_conflicts_as_applied(self, batch_id: str) -> None:
        """
        Mark all resolved conflicts for a batch as 'applied'

        Args:
            batch_id: Batch identifier
        """
        try:
            from core.database import get_db_connection

            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE conflict_resolutions
                        SET 
                            status = 'applied',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE batch_id = %s 
                          AND status = 'resolved'
                        """,
                        (batch_id,),
                    )
                    rows_updated = cursor.rowcount
                conn.commit()

                if rows_updated > 0:
                    logger.info(
                        f"Marked {rows_updated} conflicts as applied for batch {batch_id}"
                    )
                else:
                    logger.debug(
                        f"No resolved conflicts to mark as applied for batch {batch_id}"
                    )

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Failed to mark conflicts as applied: {e}")
            # Don't raise - this is just for tracking

    def apply_center_updates_to_subjects(self, batch_id: str) -> int:
        """
        Apply center_id updates from conflict resolutions to subjects table

        When a conflict is resolved with 'use_incoming', we need to update
        the subject's center_id in the subjects table, not just local_subject_ids.

        Args:
            batch_id: Batch identifier

        Returns:
            Number of subjects updated
        """
        try:
            from core.database import get_db_connection

            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    # Get all resolved conflicts with use_incoming action
                    cursor.execute(
                        """
                        SELECT DISTINCT
                            cr.existing_gsid,
                            cr.incoming_center_id
                        FROM conflict_resolutions cr
                        WHERE cr.batch_id = %s
                          AND cr.conflict_type = 'center_mismatch'
                          AND cr.resolution_action = 'use_incoming'
                          AND cr.status = 'resolved'
                        """,
                        (batch_id,),
                    )

                    conflicts = cursor.fetchall()

                    if not conflicts:
                        logger.info(f"No center updates needed for batch {batch_id}")
                        return 0

                    # Update each subject's center_id
                    updated_count = 0
                    for conflict in conflicts:
                        gsid = conflict["existing_gsid"]
                        new_center_id = conflict["incoming_center_id"]

                        cursor.execute(
                            """
                            UPDATE subjects
                            SET 
                                center_id = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE global_subject_id = %s
                              AND center_id != %s  -- Only update if different
                            """,
                            (new_center_id, gsid, new_center_id),
                        )

                        if cursor.rowcount > 0:
                            updated_count += cursor.rowcount
                            logger.info(
                                f"Updated subject {gsid} center_id to {new_center_id}"
                            )

                    conn.commit()
                    logger.info(
                        f"Applied center updates to {updated_count} subjects for batch {batch_id}"
                    )
                    return updated_count

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Failed to apply center updates to subjects: {e}")
            raise
