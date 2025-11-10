# table-loader/services/fragment_resolution.py
import logging
from datetime import datetime
from typing import Dict, Optional

from core.database import db_manager

logger = logging.getLogger(__name__)


class FragmentResolutionService:
    """Service for tracking fragment load operations"""

    def __init__(self):
        self.db_manager = db_manager

    def create_resolution(
        self,
        batch_id: str,
        table_name: str,
        fragment_key: str,
        load_status: str,
        load_strategy: str,
        rows_attempted: int = 0,
        rows_loaded: int = 0,
        rows_failed: int = 0,
        execution_time_ms: Optional[int] = None,
        error_message: Optional[str] = None,
        requires_review: bool = False,
        review_reason: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> int:
        """Create a new fragment resolution record

        Args:
            batch_id: Batch identifier
            table_name: Target table name
            fragment_key: S3 key of the fragment
            load_status: Status of load operation
            load_strategy: Strategy used (standard_insert, upsert)
            rows_attempted: Number of rows attempted to load
            rows_loaded: Number of rows successfully loaded
            rows_failed: Number of rows that failed
            execution_time_ms: Execution time in milliseconds
            error_message: Error message if failed
            requires_review: Whether this load requires manual review
            review_reason: Reason for review requirement
            metadata: Additional metadata as JSON

        Returns:
            resolution_id of created record
        """
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fragment_resolutions (
                        batch_id, table_name, fragment_key, load_status, load_strategy,
                        rows_attempted, rows_loaded, rows_failed, execution_time_ms,
                        error_message, requires_review, review_reason, metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        metadata,
                    ),
                )
                result = cursor.fetchone()
                conn.commit()
                resolution_id = result[0]
                logger.info(
                    f"Created fragment resolution {resolution_id} for {batch_id}/{table_name}"
                )
                return resolution_id

    def get_batch_resolutions(self, batch_id: str) -> list:
        """Get all resolutions for a batch"""
        with self.db_manager.get_connection() as conn:
            with self.db_manager.get_cursor(conn) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM fragment_resolutions
                    WHERE batch_id = %s
                    ORDER BY created_at DESC
                    """,
                    (batch_id,),
                )
                return cursor.fetchall()

    def get_resolutions_requiring_review(self) -> list:
        """Get all resolutions that require review"""
        with self.db_manager.get_connection() as conn:
            with self.db_manager.get_cursor(conn) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM fragment_resolutions
                    WHERE requires_review = TRUE
                    AND reviewed_at IS NULL
                    ORDER BY created_at DESC
                    """
                )
                return cursor.fetchall()

    def mark_reviewed(
        self, resolution_id: int, reviewed_by: str, resolution_notes: str
    ):
        """Mark a resolution as reviewed"""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fragment_resolutions
                    SET reviewed_by = %s,
                        reviewed_at = CURRENT_TIMESTAMP,
                        resolution_notes = %s,
                        requires_review = FALSE
                    WHERE resolution_id = %s
                    """,
                    (reviewed_by, resolution_notes, resolution_id),
                )
                conn.commit()
                logger.info(f"Marked resolution {resolution_id} as reviewed")

    def get_load_statistics(self, batch_id: Optional[str] = None) -> Dict:
        """Get load statistics, optionally filtered by batch"""
        with self.db_manager.get_connection() as conn:
            with self.db_manager.get_cursor(conn) as cursor:
                if batch_id:
                    cursor.execute(
                        """
                        SELECT 
                            COUNT(*) as total_loads,
                            SUM(rows_loaded) as total_rows_loaded,
                            SUM(rows_failed) as total_rows_failed,
                            COUNT(*) FILTER (WHERE load_status = 'success') as successful_loads,
                            COUNT(*) FILTER (WHERE load_status = 'failed') as failed_loads,
                            COUNT(*) FILTER (WHERE requires_review = TRUE) as loads_requiring_review,
                            AVG(execution_time_ms) as avg_execution_time_ms
                        FROM fragment_resolutions
                        WHERE batch_id = %s
                        """,
                        (batch_id,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT 
                            COUNT(*) as total_loads,
                            SUM(rows_loaded) as total_rows_loaded,
                            SUM(rows_failed) as total_rows_failed,
                            COUNT(*) FILTER (WHERE load_status = 'success') as successful_loads,
                            COUNT(*) FILTER (WHERE load_status = 'failed') as failed_loads,
                            COUNT(*) FILTER (WHERE requires_review = TRUE) as loads_requiring_review,
                            AVG(execution_time_ms) as avg_execution_time_ms
                        FROM fragment_resolutions
                        """
                    )
                return dict(cursor.fetchone())
