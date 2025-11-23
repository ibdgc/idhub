# table-loader/services/fragment_resolution.py
import logging
import os
import sys
from typing import Dict, List, Optional

import pandas as pd
from core.database import db_manager, get_db_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class FragmentResolutionService:
    """Analyzes changes between incoming fragments and current database state"""

    def __init__(self):
        self.db_manager = db_manager

        # Initialize NocoDB client
        try:
            # Add fragment-validator to path
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            fragment_validator_path = os.path.join(project_root, "fragment-validator")

            if fragment_validator_path not in sys.path:
                sys.path.insert(0, fragment_validator_path)

            from clients.nocodb_client import NocoDBClient  # type: ignore

            self.nocodb_client = NocoDBClient()
            logger.info("✓ NocoDB client initialized")
        except Exception as e:
            logger.warning(f"Could not initialize NocoDB client: {e}")
            self.nocodb_client = None

    def analyze_changes(
        self,
        table_name: str,
        incoming_data: pd.DataFrame,
        natural_key: List[str],
    ) -> Dict:
        """Compare incoming data with current database state"""
        logger.info(
            f"Analyzing changes for table '{table_name}' with natural key {natural_key}"
        )

        try:
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

            new_records = []
            updates = []
            unchanged = []

            current_index = self._create_index(current_data, natural_key)
            incoming_index = self._create_index(incoming_data, natural_key)

            for idx, row in incoming_data.iterrows():
                key = tuple(row[col] for col in natural_key)

                if key not in current_index:
                    new_records.append(row.to_dict())
                else:
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

    def apply_conflict_resolutions(
        self, records: List[Dict], batch_id: str
    ) -> List[Dict]:
        """Filter records based on conflict resolutions"""
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
                logger.debug(f"Skipping {key} - keeping existing record")
                skipped += 1
                continue
            elif resolution == "use_incoming":
                logger.debug(f"Loading {key} - using incoming data")
                filtered.append(record)
                updated += 1
            elif resolution == "delete_both":
                logger.debug(f"Skipping {key} - marked for deletion")
                skipped += 1
                continue
            elif resolution == "merge":
                logger.debug(f"Loading {key} - merging data")
                filtered.append(record)
                updated += 1
            elif resolution is None:
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

            df = pd.DataFrame(results)
            logger.info(f"Fetched {len(df)} existing records from '{table_name}'")
            return df

        except Exception as e:
            logger.error(f"Database error fetching current data: {e}")
            return pd.DataFrame()

    def _create_index(self, df: pd.DataFrame, key_columns: List[str]) -> Dict:
        """Create lookup index from DataFrame using natural key"""
        index = {}
        for idx, row in df.iterrows():
            key = tuple(row[col] for col in key_columns)
            index[key] = idx
        return index

    def _detect_changes(self, new_row: pd.Series, current_row: pd.Series) -> Dict:
        """Detect changes between two rows"""
        changes = {}
        for col in new_row.index:
            if col not in current_row.index:
                continue

            new_val = new_row[col]
            current_val = current_row[col]

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
        """Record a successful load in fragment_resolutions table"""
        try:
            valid_statuses = ["success", "partial", "failed", "skipped", "preview"]
            if status not in valid_statuses:
                logger.warning(f"Invalid status '{status}', defaulting to 'success'")
                status = "success"

            if rows_attempted is None:
                rows_attempted = records_loaded + rows_failed

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

    def get_resolved_conflicts(self, batch_id: str) -> List[Dict]:
        """Get all resolved conflicts for a batch from PostgreSQL"""
        logger.info(f"Fetching resolved conflicts for batch {batch_id}...")

        try:
            conn = get_db_connection()
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT 
                            batch_id,
                            local_subject_id,
                            identifier_type,
                            conflict_type,
                            resolution_action,
                            resolved,
                            existing_gsid,
                            existing_center_id,
                            incoming_center_id
                        FROM conflict_resolutions
                        WHERE batch_id = %s 
                          AND resolution_action IS NOT NULL
                          AND resolved = FALSE
                        """,
                        (batch_id,),
                    )

                    resolved_conflicts = cursor.fetchall()

                    logger.info(
                        f"Found {len(resolved_conflicts)} resolved conflicts for batch {batch_id}"
                    )

                    if resolved_conflicts:
                        for conflict in resolved_conflicts[:5]:
                            logger.info(
                                f"  - {conflict.get('local_subject_id')} "
                                f"({conflict.get('identifier_type')}): "
                                f"action={conflict.get('resolution_action')}"
                            )

                    return resolved_conflicts
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Failed to fetch resolved conflicts: {e}", exc_info=True)
            return []

    def mark_conflicts_as_applied(self, batch_id: str) -> None:
        """Mark all resolved conflicts for a batch as applied"""
        try:
            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE conflict_resolutions
                        SET 
                            resolved = TRUE,
                            resolved_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE batch_id = %s 
                          AND resolution_action IS NOT NULL
                          AND resolved = FALSE
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

    def apply_center_updates_to_subjects(self, batch_id: str, conn=None) -> int:
        """Apply center_id updates from conflict resolutions to subjects table"""
        logger.info(
            f"🔍 DEBUG: apply_center_updates_to_subjects called for batch {batch_id}"
        )

        should_close = False
        if conn is None:
            logger.info("🔍 DEBUG: Creating new connection")
            conn = get_db_connection()
            should_close = True
        else:
            logger.info("🔍 DEBUG: Using provided connection")

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logger.info("🔍 DEBUG: Executing query to find conflicts...")

                cursor.execute(
                    """
                    SELECT DISTINCT
                        cr.existing_gsid,
                        cr.incoming_center_id,
                        cr.existing_center_id
                    FROM conflict_resolutions cr
                    WHERE cr.batch_id = %s
                      AND cr.conflict_type = 'center_mismatch'
                      AND cr.resolution_action = 'use_incoming'
                      AND cr.resolved = FALSE
                    """,
                    (batch_id,),
                )

                conflicts = cursor.fetchall()
                logger.info(f"🔍 DEBUG: Query returned {len(conflicts)} conflicts")

                if not conflicts:
                    logger.info(f"No center updates needed for batch {batch_id}")
                    return 0

                logger.info(f"Found {len(conflicts)} subjects to update")

                updated_count = 0
                for conflict in conflicts:
                    gsid = conflict["existing_gsid"]
                    new_center_id = conflict["incoming_center_id"]
                    old_center_id = conflict["existing_center_id"]

                    logger.info(
                        f"Updating subject {gsid}: center_id {old_center_id} → {new_center_id}"
                    )

                    cursor.execute(
                        """
                        UPDATE subjects
                        SET 
                            center_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE global_subject_id = %s
                          AND center_id = %s
                        """,
                        (new_center_id, gsid, old_center_id),
                    )

                    if cursor.rowcount > 0:
                        updated_count += cursor.rowcount
                        logger.info(
                            f"✓ Updated subject {gsid} center_id to {new_center_id}"
                        )
                    else:
                        logger.warning(
                            f"⚠️  Subject {gsid} not updated (may have been changed already)"
                        )

                if should_close:
                    conn.commit()

                logger.info(
                    f"Applied center updates to {updated_count} subjects for batch {batch_id}"
                )
                return updated_count

        except Exception as e:
            if should_close:
                conn.rollback()
            logger.error(
                f"Failed to apply center updates to subjects: {e}", exc_info=True
            )
            raise
        finally:
            if should_close:
                conn.close()

    def apply_center_updates_to_local_ids(self, batch_id: str, conn) -> int:
        """Apply center_id updates from conflict resolutions to local_subject_ids table"""
        logger.info(
            f"🔍 DEBUG: apply_center_updates_to_local_ids called for batch {batch_id}"
        )

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logger.info("🔍 DEBUG: Querying for center_id conflicts...")

                cursor.execute(
                    """
                    SELECT 
                        cr.local_subject_id,
                        cr.identifier_type,
                        cr.existing_center_id,
                        cr.incoming_center_id,
                        cr.existing_gsid
                    FROM conflict_resolutions cr
                    WHERE cr.batch_id = %s
                      AND cr.conflict_type = 'center_mismatch'
                      AND cr.resolution_action = 'use_incoming'
                      AND cr.resolved = FALSE
                    """,
                    (batch_id,),
                )

                conflicts = cursor.fetchall()
                logger.info(
                    f"🔍 DEBUG: Found {len(conflicts)} local_subject_id conflicts"
                )

                if not conflicts:
                    logger.info(
                        f"No center updates needed for local_subject_ids in batch {batch_id}"
                    )
                    return 0

                deleted_count = 0
                for conflict in conflicts:
                    local_id = conflict["local_subject_id"]
                    id_type = conflict["identifier_type"]
                    old_center = conflict["existing_center_id"]
                    new_center = conflict["incoming_center_id"]

                    logger.info(
                        f"Deleting old local_subject_id: center={old_center}, "
                        f"id={local_id}, type={id_type}"
                    )

                    cursor.execute(
                        """
                        DELETE FROM local_subject_ids
                        WHERE center_id = %s
                          AND local_subject_id = %s
                          AND identifier_type = %s
                        """,
                        (old_center, local_id, id_type),
                    )

                    deleted = cursor.rowcount

                    if deleted > 0:
                        deleted_count += deleted
                        logger.info(
                            f"✓ Deleted old record (will be replaced with center={new_center})"
                        )
                    else:
                        logger.warning(
                            f"⚠️  No old record found to delete for {id_type}={local_id}"
                        )

                logger.info(
                    f"Deleted {deleted_count} old local_subject_id records for batch {batch_id}"
                )
                return deleted_count

        except Exception as e:
            logger.error(
                f"Failed to apply center updates to local_subject_ids: {e}",
                exc_info=True,
            )
            raise
