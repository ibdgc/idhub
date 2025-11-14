# gsid-service/services/identity_resolution.py
import json
import logging
from datetime import date
from typing import Any, Dict, List, Optional

from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


def resolve_subject_with_multiple_ids(
    conn,
    center_id: int,
    identifiers: List[Dict[str, str]],
    registration_year: Optional[date] = None,
    control: bool = False,
    created_by: str = "system",
) -> Dict[str, Any]:
    """
    Core identity resolution for a subject with multiple identifiers.

    Logic:
    1. Query local_subject_ids for ALL identifiers
    2. Collect all matched GSIDs
    3. If 0 matches → create new GSID
    4. If 1 match → use that GSID
    5. If 2+ matches → CONFLICT (flag all, use oldest)
    6. Link ALL identifiers to the chosen GSID

    Returns:
        {
            "gsid": "GSID-XXX",
            "action": "create_new" | "link_existing" | "conflict_resolved",
            "identifiers_linked": int,
            "conflicts": ["GSID-YYY", ...] or None,
            "conflict_resolution": "used_oldest" or None
        }
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Step 1: Find all existing GSIDs for these identifiers
        matched_gsids = set()

        for identifier in identifiers:
            cur.execute(
                """
                SELECT s.global_subject_id, s.created_at, s.withdrawn
                FROM local_subject_ids l
                JOIN subjects s ON l.global_subject_id = s.global_subject_id
                WHERE l.center_id = %s 
                  AND l.local_subject_id = %s 
                  AND l.identifier_type = %s
                """,
                (
                    center_id,
                    identifier["local_subject_id"],
                    identifier["identifier_type"],
                ),
            )
            result = cur.fetchone()
            if result:
                matched_gsids.add(
                    (
                        result["global_subject_id"],
                        result["created_at"],
                        result["withdrawn"],
                    )
                )

        # Step 2: Determine action based on matches
        if len(matched_gsids) == 0:
            # No matches → create new GSID
            from services.gsid_generator import generate_gsid

            gsid = generate_gsid()
            action = "create_new"
            conflicts = None
            conflict_resolution = None

            # Create subject record
            cur.execute(
                """
                INSERT INTO subjects (
                    global_subject_id, center_id, registration_year, 
                    control, created_by
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (gsid, center_id, registration_year, control, created_by),
            )

            logger.info(f"Created new GSID: {gsid} for center_id={center_id}")

        elif len(matched_gsids) == 1:
            # Single match → use that GSID
            gsid_data = list(matched_gsids)[0]
            gsid = gsid_data[0]
            action = "link_existing"
            conflicts = None
            conflict_resolution = None

            logger.info(f"Linked to existing GSID: {gsid}")

        else:
            # Multiple matches → CONFLICT
            # Sort by created_at (oldest first), then by GSID
            sorted_gsids = sorted(matched_gsids, key=lambda x: (x[1], x[0]))
            gsid = sorted_gsids[0][0]  # Use oldest
            action = "conflict_resolved"
            conflicts = [g[0] for g in sorted_gsids]
            conflict_resolution = "used_oldest"

            logger.warning(
                f"Multi-GSID conflict detected! Found {len(conflicts)} GSIDs: {conflicts}. "
                f"Using oldest: {gsid}"
            )

            # Flag ALL conflicting GSIDs for review
            for gsid_data in sorted_gsids:
                conflict_gsid = gsid_data[0]
                cur.execute(
                    """
                    UPDATE subjects
                    SET flagged_for_review = TRUE,
                        review_notes = COALESCE(review_notes || E'\n', '') || 
                                      'Multi-GSID conflict detected on ' || CURRENT_TIMESTAMP::TEXT ||
                                      '. Conflicting GSIDs: ' || %s
                    WHERE global_subject_id = %s
                    """,
                    (", ".join(conflicts), conflict_gsid),
                )

            logger.info(f"Flagged {len(conflicts)} GSIDs for review")

        # Step 3: Link ALL identifiers to the chosen GSID
        identifiers_linked = 0
        for identifier in identifiers:
            cur.execute(
                """
                INSERT INTO local_subject_ids (
                    center_id, local_subject_id, identifier_type, 
                    global_subject_id, created_by
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (center_id, local_subject_id, identifier_type) 
                DO UPDATE SET 
                    global_subject_id = EXCLUDED.global_subject_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    center_id,
                    identifier["local_subject_id"],
                    identifier["identifier_type"],
                    gsid,
                    created_by,
                ),
            )
            identifiers_linked += 1

        logger.info(f"Linked {identifiers_linked} identifier(s) to {gsid}")

        # Step 4: Log resolution in identity_resolutions table
        candidate_ids_json = json.dumps(
            [
                {
                    "local_subject_id": i["local_subject_id"],
                    "identifier_type": i["identifier_type"],
                }
                for i in identifiers
            ]
        )
        matched_gsids_json = json.dumps(conflicts) if conflicts else None

        cur.execute(
            """
            INSERT INTO identity_resolutions (
                local_subject_id,
                identifier_type,
                input_center_id,
                gsid,
                matched_gsid,
                action,
                match_strategy,
                confidence,
                candidate_ids,
                matched_gsids,
                requires_review,
                review_reason,
                created_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
            """,
            (
                identifiers[0]["local_subject_id"],  # Primary identifier
                identifiers[0]["identifier_type"],
                center_id,
                gsid,
                gsid,
                action,
                "multiple_gsid_conflict"
                if conflicts
                else "exact_match"
                if action == "link_existing"
                else "no_match",
                1.0 if not conflicts else 0.5,
                candidate_ids_json,
                matched_gsids_json,
                conflicts is not None,
                f"Multiple GSIDs found: {conflicts}" if conflicts else None,
                created_by,
            ),
        )

        conn.commit()

        logger.info(
            f"Resolution complete: gsid={gsid}, action={action}, "
            f"identifiers={len(identifiers)}, conflicts={len(conflicts) if conflicts else 0}"
        )

        return {
            "gsid": gsid,
            "action": action,
            "identifiers_linked": identifiers_linked,
            "conflicts": conflicts,
            "conflict_resolution": conflict_resolution,
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"Error resolving subject: {e}", exc_info=True)
        raise
    finally:
        cur.close()
