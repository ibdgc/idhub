# gsid-service/services/identity_resolution.py
import logging
from typing import Dict, Optional

import psycopg2.extras

logger = logging.getLogger(__name__)


def resolve_identity(
    conn, center_id: int, local_subject_id: str, identifier_type: str = "consortium_id"
) -> Dict:
    """
    Resolve subject identity with center promotion logic
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # First: Check if this local_subject_id exists for this center (ANY identifier_type)
        cur.execute(
            """
            SELECT s.global_subject_id, s.withdrawn, s.center_id, l.identifier_type
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
                    "existing_gsid": exact["global_subject_id"],
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

        # Second: Check if this local_subject_id exists at center_id=0 (Unknown)
        cur.execute(
            """
            SELECT s.global_subject_id, s.center_id, l.identifier_type
            FROM local_subject_ids l
            JOIN subjects s ON l.global_subject_id = s.global_subject_id
            WHERE l.center_id = 0 AND l.local_subject_id = %s
            ORDER BY l.created_at ASC
            LIMIT 1
            """,
            (local_subject_id,),
        )
        unknown_match = cur.fetchone()

        if unknown_match and center_id != 0:
            # Auto-promote: Update subject's center_id and add new local_subject_id entry
            gsid = unknown_match["global_subject_id"]

            # Update subject's center_id
            cur.execute(
                """
                UPDATE subjects
                SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE global_subject_id = %s
                """,
                (center_id, gsid),
            )

            # Add new local_subject_id entry for the known center
            cur.execute(
                """
                INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                """,
                (center_id, local_subject_id, identifier_type, gsid),
            )

            return {
                "action": "center_promoted",
                "gsid": gsid,
                "match_strategy": "center_promotion",
                "confidence": 1.0,
                "previous_center_id": 0,
                "new_center_id": center_id,
                "message": f"Subject promoted from Unknown (0) to center {center_id}",
            }

        # Third: Check if this local_subject_id exists at a DIFFERENT center
        cur.execute(
            """
            SELECT s.global_subject_id, s.center_id, l.center_id as local_center_id, l.identifier_type
            FROM local_subject_ids l
            JOIN subjects s ON l.global_subject_id = s.global_subject_id
            WHERE l.local_subject_id = %s AND l.center_id != %s
            ORDER BY l.created_at ASC
            LIMIT 1
            """,
            (local_subject_id, center_id),
        )
        other_center = cur.fetchone()

        if other_center:
            return {
                "action": "review_required",
                "gsid": other_center["global_subject_id"],
                "existing_gsid": other_center["global_subject_id"],
                "match_strategy": "cross_center_conflict",
                "confidence": 0.8,
                "review_reason": f"Local ID exists at different center (center_id={other_center['local_center_id']})",
                "existing_center_id": other_center["local_center_id"],
            }

        # No match found - create new subject
        return {
            "action": "create_new",
            "gsid": None,
            "match_strategy": "no_match",
            "confidence": 1.0,
        }

    except Exception as e:
        logger.error(f"Error in resolve_identity: {e}", exc_info=True)
        raise
    finally:
        cur.close()


def log_resolution(
    conn,
    local_subject_id: str,
    identifier_type: str,
    action: str,
    gsid: Optional[str],
    matched_gsid: Optional[str],
    match_strategy: str,
    confidence: float,
    metadata: Optional[Dict] = None,
):
    """
    Log identity resolution to audit table
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO identity_resolutions 
            (local_subject_id, identifier_type, action, gsid, matched_gsid, 
             match_strategy, confidence, metadata, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            (
                local_subject_id,
                identifier_type,
                action,
                gsid,
                matched_gsid,
                match_strategy,
                confidence,
                psycopg2.extras.Json(metadata) if metadata else None,
            ),
        )
        cur.close()
    except Exception as e:
        logger.error(f"Error logging resolution: {e}", exc_info=True)
        # Don't raise - logging failure shouldn't break the main flow
