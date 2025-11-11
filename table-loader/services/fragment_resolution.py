# table-loader/services/fragment_resolution.py
import json
import logging
from datetime import datetime
from typing import Dict, Optional

from core.database import db_manager

logger = logging.getLogger(__name__)


class FragmentResolutionService:
    """Tracks fragment load operations and their outcomes"""

    def create_resolution(
        self,
        batch_id: str,
        table_name: str,
        fragment_key: str,
        load_status: str,
        load_strategy: str,
        rows_attempted: int,
        rows_loaded: int,
        rows_failed: int,
        execution_time_ms: Optional[int] = None,
        error_message: Optional[str] = None,
        requires_review: bool = False,
        review_reason: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> int:
        """Create a fragment resolution record

        Args:
            batch_id: Batch identifier
            table_name: Target table name
            fragment_key: S3 key of the fragment
            load_status: Status of the load (success, failed, skipped, preview)
            load_strategy: Strategy used (standard_insert, upsert)
            rows_attempted: Number of rows attempted to load
            rows_loaded: Number of rows successfully loaded
            rows_failed: Number of rows that failed
            execution_time_ms: Execution time in milliseconds
            error_message: Error message if failed
            requires_review: Whether manual review is needed
            review_reason: Reason for review requirement
            metadata: Additional metadata as dict

        Returns:
            resolution_id of created record
        """
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                # Convert metadata dict to JSON string
                metadata_json = json.dumps(metadata) if metadata else None

                cur.execute(
                    """
                    INSERT INTO fragment_resolutions (
                        batch_id, table_name, fragment_key, load_status, load_strategy,
                        rows_attempted, rows_loaded, rows_failed, execution_time_ms,
                        error_message, requires_review, review_reason, metadata, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING resolution_id
                    """,
                    (
                        batch_id,
                        table_name,
                        fragment_key,
                        load_status,
                        load_strategy,
                        rows_attempted,
                        rows_loaded,
                        rows_failed,
                        execution_time_ms,
                        error_message,
                        requires_review,
                        review_reason,
                        metadata_json,  # Use JSON string instead of dict
                    ),
                )
                result = cur.fetchone()
                conn.commit()
                return result[0]

    def mark_reviewed(
        self,
        resolution_id: int,
        reviewed_by: str,
        resolution_notes: Optional[str] = None,
    ):
        """Mark a fragment resolution as reviewed"""
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE fragment_resolutions
                    SET reviewed_by = %s,
                        reviewed_at = NOW(),
                        resolution_notes = %s,
                        requires_review = FALSE
                    WHERE resolution_id = %s
                    """,
                    (reviewed_by, resolution_notes, resolution_id),
                )
                conn.commit()

    def get_pending_reviews(self, batch_id: Optional[str] = None) -> list:
        """Get fragment resolutions requiring review"""
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                if batch_id:
                    cur.execute(
                        """
                        SELECT resolution_id, batch_id, table_name, fragment_key,
                               load_status, review_reason, created_at
                        FROM fragment_resolutions
                        WHERE requires_review = TRUE AND batch_id = %s
                        ORDER BY created_at DESC
                        """,
                        (batch_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT resolution_id, batch_id, table_name, fragment_key,
                               load_status, review_reason, created_at
                        FROM fragment_resolutions
                        WHERE requires_review = TRUE
                        ORDER BY created_at DESC
                        """
                    )
                return cur.fetchall()

    def get_load_statistics(self, batch_id: str) -> Dict:
        """Get load statistics for a batch"""
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        COUNT(*) as total_fragments,
                        SUM(rows_attempted) as total_rows_attempted,
                        SUM(rows_loaded) as total_rows_loaded,
                        SUM(rows_failed) as total_rows_failed,
                        SUM(CASE WHEN load_status = 'success' THEN 1 ELSE 0 END) as successful_loads,
                        SUM(CASE WHEN load_status = 'failed' THEN 1 ELSE 0 END) as failed_loads,
                        SUM(CASE WHEN requires_review THEN 1 ELSE 0 END) as pending_reviews
                    FROM fragment_resolutions
                    WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
                row = cur.fetchone()

                if row:
                    return {
                        "total_fragments": row[0] or 0,
                        "total_rows_attempted": row[1] or 0,
                        "total_rows_loaded": row[2] or 0,
                        "total_rows_failed": row[3] or 0,
                        "successful_loads": row[4] or 0,
                        "failed_loads": row[5] or 0,
                        "pending_reviews": row[6] or 0,
                    }
                else:
                    return {
                        "total_fragments": 0,
                        "total_rows_attempted": 0,
                        "total_rows_loaded": 0,
                        "total_rows_failed": 0,
                        "successful_loads": 0,
                        "failed_loads": 0,
                        "pending_reviews": 0,
                    }
