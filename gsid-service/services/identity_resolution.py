# gsid-service/services/identity_resolution.py
import logging
from typing import Any, Dict

import psycopg2.extras

logger = logging.getLogger(__name__)


def resolve_identity(
    conn, center_id: int, local_subject_id: str, identifier_type: str = "primary"
) -> Dict[str, Any]:
    """
    Resolve subject identity with cross-center duplicate detection and auto-promotion

    Strategy:
    1. Check if this exact (center_id, local_subject_id, identifier_type) exists
    2. Check if this local_subject_id exists in ANY center (potential duplicate)
    3. If found in center_id=1 (Unknown) and incoming center is known, promote the center
    4. Otherwise flag for review or create new
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # First: Check if this exact combination exists
        cur.execute(
            """
            SELECT s.global_subject_id, s.withdrawn, l.identifier_type, l.center_id
            FROM local_subject_ids l
            JOIN subjects s ON l.global_subject_id = s.global_subject_id
            WHERE l.center_id = %s AND l.local_subject_id = %s AND l.identifier_type = %s
            ORDER BY l.created_at ASC
            LIMIT 1
            """,
            (center_id, local_subject_id, identifier_type),
        )
        exact = cur.fetchone()

        if exact:
            if exact["withdrawn"]:
                return {
                    "action": "review_required",
                    "gsid": exact["global_subject_id"],
                    "match_strategy": f"exact_withdrawn (type: {exact['identifier_type']})",
                    "confidence": 1.0,
                    "review_reason": "Subject previously withdrawn",
                }
            return {
                "action": "link_existing",
                "gsid": exact["global_subject_id"],
                "match_strategy": f"exact_match (center: {exact['center_id']}, type: {exact['identifier_type']})",
                "confidence": 1.0,
            }

        # Second: Check if this local_subject_id exists in ANY other center
        cur.execute(
            """
            SELECT 
                s.global_subject_id, 
                s.withdrawn, 
                l.identifier_type,
                l.center_id,
                l.created_at
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
            existing_gsid = cross_center["global_subject_id"]

            # AUTO-PROMOTION LOGIC:
            # If existing record is in "Unknown" center (center_id=1) and incoming is known
            if existing_center_id == 1 and center_id != 1:
                logger.info(
                    f"Auto-promoting {local_subject_id} from Unknown (center_id=1) "
                    f"to center_id={center_id}"
                )

                # Update the existing record's center_id
                update_cur = conn.cursor()
                update_cur.execute(
                    """
                    UPDATE local_subject_ids
                    SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE local_subject_id = %s 
                      AND center_id = 1 
                      AND identifier_type = %s
                    """,
                    (center_id, local_subject_id, cross_center["identifier_type"]),
                )
                rows_updated = update_cur.rowcount
                update_cur.close()

                if rows_updated > 0:
                    return {
                        "action": "center_promoted",
                        "gsid": existing_gsid,
                        "match_strategy": f"unknown_center_promoted (1 -> {center_id})",
                        "confidence": 1.0,
                        "previous_center_id": 1,
                        "new_center_id": center_id,
                        "message": f"Updated center from Unknown to {center_id}",
                    }

            # If incoming is "Unknown" but existing has a known center, link to existing
            elif center_id == 1 and existing_center_id != 1:
                logger.info(
                    f"Incoming Unknown center for {local_subject_id}, "
                    f"linking to existing center_id={existing_center_id}"
                )
                return {
                    "action": "link_existing",
                    "gsid": existing_gsid,
                    "match_strategy": f"defer_to_known_center (existing center: {existing_center_id})",
                    "confidence": 0.95,
                    "message": f"Linked to existing record in center {existing_center_id}",
                }

            # Both are known centers but different - requires review
            else:
                return {
                    "action": "review_required",
                    "gsid": None,
                    "match_strategy": f"cross_center_conflict (existing: {existing_center_id}, incoming: {center_id})",
                    "confidence": 0.8,
                    "review_reason": (
                        f"Same local_subject_id '{local_subject_id}' exists in different centers. "
                        f"Existing: center {existing_center_id} with GSID {existing_gsid}. "
                        f"Incoming: center {center_id}. Requires manual review."
                    ),
                    "existing_gsid": existing_gsid,
                    "existing_center_id": existing_center_id,
                }

        # Third: Check for similar identifiers in same center (different type)
        cur.execute(
            """
            SELECT s.global_subject_id, s.withdrawn, l.identifier_type
            FROM local_subject_ids l
            JOIN subjects s ON l.global_subject_id = s.global_subject_id
            WHERE l.center_id = %s AND l.local_subject_id = %s AND l.identifier_type != %s
            ORDER BY l.created_at ASC
            LIMIT 1
            """,
            (center_id, local_subject_id, identifier_type),
        )
        same_center_diff_type = cur.fetchone()

        if same_center_diff_type:
            # Same local_subject_id in same center but different identifier_type
            # This is likely the same person, link to existing GSID
            return {
                "action": "link_existing",
                "gsid": same_center_diff_type["global_subject_id"],
                "match_strategy": f"same_center_different_type (existing type: {same_center_diff_type['identifier_type']})",
                "confidence": 0.95,
            }

        # No matches found - create new subject
        return {
            "action": "create_new",
            "gsid": None,
            "match_strategy": "no_match",
            "confidence": 1.0,
        }

    except Exception as e:
        logger.error(f"Error in resolve_identity: {e}")
        raise
    finally:
        cur.close()


def log_resolution(
    conn,
    local_subject_id: str,
    identifier_type: str,
    action: str,
    gsid: str = None,
    matched_gsid: str = None,
    match_strategy: str = None,
    confidence: float = None,
    metadata: dict = None,
):
    """Log identity resolution decision"""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO identity_resolutions 
            (local_subject_id, identifier_type, action, gsid, matched_gsid, match_strategy, confidence, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
    except Exception as e:
        logger.error(f"Error logging resolution: {e}")
        raise
    finally:
        cur.close()
