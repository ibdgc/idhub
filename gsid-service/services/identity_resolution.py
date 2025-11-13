# gsid-service/services/identity_resolution.py
import logging
from typing import Any, Dict, Optional

import psycopg2.extras

logger = logging.getLogger(__name__)


def resolve_identity(
    conn, center_id: int, local_subject_id: str, identifier_type: str = "primary"
) -> Dict[str, Any]:
    """
    Resolve subject identity using local_subject_id and center_id
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # First: Check if this local_subject_id exists for this center (ANY identifier_type)
        cur.execute(
            """
            SELECT s.global_subject_id, s.withdrawn, l.identifier_type
            FROM local_subject_ids l
            JOIN subjects s ON l.global_subject_id = s.global_subject_id
            WHERE l.center_id = %s AND l.local_subject_id = %s
            ORDER BY l.created_at ASC
            LIMIT 1
            """,
            (center_id, local_subject_id),
        )
        exact = cur.fetchone()

        if exact:
            if exact["withdrawn"]:
                return {
                    "action": "review_required",
                    "gsid": exact["global_subject_id"],
                    "match_strategy": f"exact_withdrawn (original type: {exact['identifier_type']})",
                    "confidence": 1.0,
                    "review_reason": "Subject previously withdrawn",
                }
            return {
                "action": "link_existing",
                "gsid": exact["global_subject_id"],
                "match_strategy": f"exact_match (type: {exact['identifier_type']})",
                "confidence": 1.0,
            }

        # Second: Check if this local_subject_id exists at a DIFFERENT center
        cur.execute(
            """
            SELECT s.global_subject_id, s.center_id, s.withdrawn, l.identifier_type
            FROM local_subject_ids l
            JOIN subjects s ON l.global_subject_id = s.global_subject_id
            WHERE l.local_subject_id = %s AND l.center_id != %s
            ORDER BY l.created_at ASC
            LIMIT 1
            """,
            (local_subject_id, center_id),
        )
        cross_center = cur.fetchone()

        if cross_center:
            existing_center_id = cross_center["center_id"]
            gsid = cross_center["global_subject_id"]

            # If existing center is "Unknown" (0 or 1), promote to the new center
            if existing_center_id in [0, 1]:
                return {
                    "action": "center_promoted",
                    "gsid": gsid,
                    "match_strategy": "cross_center_promotion",
                    "confidence": 1.0,
                    "previous_center_id": existing_center_id,
                    "new_center_id": center_id,
                    "message": f"Promoted from center {existing_center_id} to {center_id}",
                }
            else:
                # Different known centers - flag for review
                return {
                    "action": "review_required",
                    "gsid": gsid,
                    "existing_center_id": existing_center_id,
                    "match_strategy": "cross_center_conflict",
                    "confidence": 0.9,
                    "review_reason": f"Subject exists at center {existing_center_id}, attempting to register at {center_id}",
                }

        # No match found - create new subject
        return {
            "action": "create_new",
            "gsid": None,
            "match_strategy": "no_match",
            "confidence": 1.0,
        }

    except Exception as e:
        logger.error(f"Error in resolve_identity: {str(e)}", exc_info=True)
        raise


def log_resolution(
    conn,
    local_subject_id: str,
    identifier_type: str,
    action: str,
    gsid: Optional[str],
    matched_gsid: Optional[str],
    match_strategy: str,
    confidence: float,
    center_id: int,
    metadata: Optional[Dict] = None,
    created_by: str = "system",
):
    """Log identity resolution decision to audit table"""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO identity_resolutions (
                local_subject_id,
                identifier_type,
                input_center_id,
                input_local_id,
                gsid,
                matched_gsid,
                action,
                match_strategy,
                confidence,
                requires_review,
                metadata,
                created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                local_subject_id,
                identifier_type,
                center_id,
                local_subject_id,  # input_local_id is the same as local_subject_id
                gsid,
                matched_gsid,
                action,
                match_strategy,
                confidence,
                action == "review_required",
                psycopg2.extras.Json(metadata or {}),
                created_by,
            ),
        )
    except Exception as e:
        logger.error(f"Error logging resolution: {str(e)}", exc_info=True)
