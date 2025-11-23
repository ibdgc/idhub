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
        """Get all resolved conflicts for a batch (resolution_action set, not yet applied)"""
        query = """
            SELECT * FROM conflict_resolutions
            WHERE batch_id = %s
              AND resolution_action IS NOT NULL
              AND resolved = FALSE
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
                actions_taken["keep_existing"] += 1
                logger.info(
                    f"Keeping existing record for {resolution['local_subject_id']}"
                )

            elif action == "use_incoming":
                self._delete_existing_record(resolution)
                actions_taken["use_incoming"] += 1
                logger.info(
                    f"Deleted existing record for {resolution['local_subject_id']}, "
                    f"will load incoming"
                )

            elif action == "delete_both":
                self._delete_existing_record(resolution)
                self._mark_incoming_skip(resolution)
                actions_taken["delete_both"] += 1
                logger.warning(
                    f"Deleted both records for {resolution['local_subject_id']}"
                )

            elif action == "merge":
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
            SET resolved = TRUE, 
                resolved_at = CURRENT_TIMESTAMP
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
              AND existing_center_id = %s
              AND resolution_action IS NOT NULL
              AND resolved = FALSE
        """

        with get_db_cursor(self.db_connection) as cursor:
            cursor.execute(query, (batch_id, local_subject_id, center_id))
            result = cursor.fetchone()

            if not result:
                return False

            action = result["resolution_action"]
            notes = result.get("resolution_notes", "")

            if action in ("keep_existing", "delete_both"):
                return True

            if "[SKIP_INCOMING]" in notes:
                return True

            return False
