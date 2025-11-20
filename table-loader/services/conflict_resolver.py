# table-loader/services/conflict_resolver.py
import logging
from typing import Dict, List, Optional

from core.database import get_db_cursor

logger = logging.getLogger(__name__)


class ConflictResolver:
    """Applies conflict resolutions before loading data"""

    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get_pending_resolutions(self, batch_id: str) -> List[Dict]:
        """Get all resolved conflicts for a batch"""
        query = """
            SELECT * FROM conflict_resolutions
            WHERE batch_id = %s
              AND status = 'resolved'
              AND resolution_action IS NOT NULL
            ORDER BY id
        """

        with get_db_cursor(self.db_connection) as cursor:
            cursor.execute(query, (batch_id,))
            return cursor.fetchall()

    def apply_resolutions(self, batch_id: str) -> Dict:
        """
        Apply all conflict resolutions for a batch

        Returns:
            Summary of actions taken
        """
        resolutions = self.get_pending_resolutions(batch_id)

        if not resolutions:
            logger.info(f"No conflict resolutions to apply for batch {batch_id}")
            return {"total": 0, "actions": {}}

        logger.info(f"Applying {len(resolutions)} conflict resolutions...")

        actions_taken = {
            "keep_existing": 0,
            "use_incoming": 0,
            "delete_both": 0,
            "merge": 0,
        }

        for resolution in resolutions:
            action = resolution["resolution_action"]

            if action == "keep_existing":
                # Do nothing - existing record stays, incoming will be skipped
                actions_taken["keep_existing"] += 1
                logger.info(
                    f"Keeping existing record for {resolution['local_subject_id']}"
                )

            elif action == "use_incoming":
                # Delete existing record, incoming will be loaded
                self._delete_existing_record(resolution)
                actions_taken["use_incoming"] += 1
                logger.info(
                    f"Deleted existing record for {resolution['local_subject_id']}, "
                    f"will load incoming"
                )

            elif action == "delete_both":
                # Delete existing record, mark incoming to skip
                self._delete_existing_record(resolution)
                self._mark_incoming_skip(resolution)
                actions_taken["delete_both"] += 1
                logger.warning(
                    f"Deleted both records for {resolution['local_subject_id']}"
                )

            elif action == "merge":
                # Complex merge logic - for now, log and skip
                logger.warning(
                    f"Merge action not yet implemented for {resolution['local_subject_id']}"
                )
                actions_taken["merge"] += 1

            # Mark resolution as applied
            self._mark_resolution_applied(resolution["id"])

        self.db_connection.commit()

        logger.info(f"Applied conflict resolutions: {actions_taken}")
        return {"total": len(resolutions), "actions": actions_taken}

    def _delete_existing_record(self, resolution: Dict) -> None:
        """Delete existing conflicting record from local_subject_ids"""
        query = """
            DELETE FROM local_subject_ids
            WHERE center_id = %s
              AND local_subject_id = %s
              AND identifier_type = %s
        """

        with get_db_cursor(self.db_connection) as cursor:
            cursor.execute(
                query,
                (
                    resolution["existing_center_id"],
                    resolution["local_subject_id"],
                    resolution["identifier_type"],
                ),
            )
            deleted_count = cursor.rowcount
            logger.debug(f"Deleted {deleted_count} existing record(s)")

    def _mark_incoming_skip(self, resolution: Dict) -> None:
        """Mark incoming record to be skipped during load"""
        # This will be checked by the loader before inserting
        query = """
            UPDATE conflict_resolutions
            SET resolution_notes = COALESCE(resolution_notes, '') || ' [SKIP_INCOMING]'
            WHERE id = %s
        """

        with get_db_cursor(self.db_connection) as cursor:
            cursor.execute(query, (resolution["id"],))

    def _mark_resolution_applied(self, resolution_id: int) -> None:
        """Mark conflict resolution as applied"""
        query = """
            UPDATE conflict_resolutions
            SET status = 'applied',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """

        with get_db_cursor(self.db_connection) as cursor:
            cursor.execute(query, (resolution_id,))

    def should_skip_record(
        self, batch_id: str, local_subject_id: str, center_id: int
    ) -> bool:
        """Check if a record should be skipped based on conflict resolution"""
        query = """
            SELECT resolution_action, resolution_notes
            FROM conflict_resolutions
            WHERE batch_id = %s
              AND local_subject_id = %s
              AND incoming_center_id = %s
              AND status IN ('resolved', 'applied')
        """

        with get_db_cursor(self.db_connection) as cursor:
            cursor.execute(query, (batch_id, local_subject_id, center_id))
            result = cursor.fetchone()

            if not result:
                return False

            action = result["resolution_action"]
            notes = result.get("resolution_notes", "")

            # Skip if action is keep_existing or delete_both
            if action in ("keep_existing", "delete_both"):
                return True

            # Skip if marked with SKIP_INCOMING flag
            if "[SKIP_INCOMING]" in notes:
                return True

            return False
