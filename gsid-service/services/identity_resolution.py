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

    MATCHING LOGIC (REVISED):
    - Match on local_subject_id ALONE (center-agnostic)
    - If same ID exists with different center → FLAG CONFLICT but link to same GSID
    - Center ID represents recruitment site (should be consistent per subject)

    Steps:
    1. Query local_subject_ids for ALL identifiers (ignore center_id in matching)
    2. Collect all matched GSIDs
    3. If 0 matches → create new GSID
    4. If 1 match → use that GSID, check for center conflicts
    5. If 2+ matches → MULTI-GSID CONFLICT (flag all, use oldest)
    6. Link ALL identifiers to the chosen GSID

    Returns:
        {
            "gsid": "GSID-XXX",
            "action": "create_new" | "link_existing" | "conflict_resolved",
            "identifiers_linked": int,
            "conflicts": ["GSID-YYY", ...] or None,
            "conflict_resolution": "used_oldest" | "center_mismatch" or None,
            "warnings": ["center mismatch detected"] or []
        }
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    warnings = []

    try:
        # Step 1: Find all existing GSIDs for these identifiers
        # CRITICAL: Match on local_subject_id ONLY (center-agnostic)
        matched_gsids = {}  # gsid -> {created_at, center_id, withdrawn}
        center_conflicts = []

        for identifier in identifiers:
            cur.execute(
                """
                SELECT 
                    s.global_subject_id, 
                    s.created_at, 
                    s.center_id as subject_center_id,
                    s.withdrawn,
                    l.center_id as identifier_center_id
                FROM local_subject_ids l
                JOIN subjects s ON l.global_subject_id = s.global_subject_id
                WHERE lower(l.local_subject_id) = lower(%s)
                """,
                (identifier["local_subject_id"],),
            )

            results = cur.fetchall()

            for result in results:
                gsid = result["global_subject_id"]

                # Track this GSID
                if gsid not in matched_gsids:
                    matched_gsids[gsid] = {
                        "created_at": result["created_at"],
                        "center_id": result["subject_center_id"],
                        "withdrawn": result["withdrawn"],
                    }

                # Check for center conflicts
                existing_center = result["identifier_center_id"]
                if existing_center != center_id:
                    # Center mismatch detected
                    if existing_center == 0:
                        # Existing is unknown, incoming has real center → UPDATE
                        warnings.append(
                            f"Updating unknown center (0) to known center ({center_id}) "
                            f"for {identifier['identifier_type']}={identifier['local_subject_id']}"
                        )
                    elif center_id == 0:
                        # Incoming is unknown, existing has real center → KEEP existing
                        warnings.append(
                            f"Ignoring unknown center (0) - keeping existing center ({existing_center}) "
                            f"for {identifier['identifier_type']}={identifier['local_subject_id']}"
                        )
                    else:
                        # Both are real centers but different → CONFLICT
                        center_conflicts.append(
                            f"{identifier['identifier_type']}={identifier['local_subject_id']}: "
                            f"existing center={existing_center}, incoming center={center_id}"
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
            gsid = list(matched_gsids.keys())[0]
            gsid_data = matched_gsids[gsid]
            action = "link_existing"
            conflicts = None
            conflict_resolution = None

            # Check if we need to update subject's center_id
            existing_center = gsid_data["center_id"]
            if existing_center == 0 and center_id != 0:
                # Update from unknown to known center
                cur.execute(
                    """
                    UPDATE subjects
                    SET center_id = %s,
                        review_notes = COALESCE(review_notes || E'\n', '') ||
                                      'Updated center from 0 (unknown) to ' || %s || 
                                      ' on ' || CURRENT_TIMESTAMP::TEXT
                    WHERE global_subject_id = %s
                    """,
                    (center_id, center_id, gsid),
                )
                logger.info(f"Updated GSID {gsid} center from 0 to {center_id}")

            # Flag center conflicts
            if center_conflicts:
                conflict_resolution = "center_mismatch"
                cur.execute(
                    """
                    UPDATE subjects
                    SET flagged_for_review = TRUE,
                        review_notes = COALESCE(review_notes || E'\n', '') ||
                                      'CENTER CONFLICT detected on ' || CURRENT_TIMESTAMP::TEXT || E'\n' ||
                                      %s
                    WHERE global_subject_id = %s
                    """,
                    ("\n".join(center_conflicts), gsid),
                )
                warnings.extend(center_conflicts)
                logger.warning(
                    f"Center conflict flagged for GSID {gsid}: {center_conflicts}"
                )

            logger.info(f"Linked to existing GSID: {gsid}")

        else:
            # Multiple GSIDs matched → MULTI-GSID CONFLICT
            # This means the SAME identifier is linked to DIFFERENT GSIDs (data error)
            sorted_gsids = sorted(
                matched_gsids.items(), key=lambda x: (x[1]["created_at"], x[0])
            )
            gsid = sorted_gsids[0][0]  # Use oldest
            action = "conflict_resolved"
            conflicts = [g[0] for g in sorted_gsids]
            conflict_resolution = "used_oldest"

            logger.error(
                f"MULTI-GSID CONFLICT! Same identifier linked to {len(conflicts)} GSIDs: {conflicts}. "
                f"Using oldest: {gsid}"
            )

            # Flag ALL conflicting GSIDs for review
            for gsid_tuple in sorted_gsids:
                conflict_gsid = gsid_tuple[0]
                cur.execute(
                    """
                    UPDATE subjects
                    SET flagged_for_review = TRUE,
                        review_notes = COALESCE(review_notes || E'\n', '') ||
                                      'MULTI-GSID CONFLICT detected on ' || CURRENT_TIMESTAMP::TEXT || E'\n' ||
                                      'Conflicting GSIDs: ' || %s || E'\n' ||
                                      'Resolution: Using oldest GSID ' || %s
                    WHERE global_subject_id = %s
                    """,
                    (", ".join(conflicts), gsid, conflict_gsid),
                )

            warnings.append(
                f"Multi-GSID conflict: {len(conflicts)} GSIDs found for same identifier"
            )

        # Step 3: Link ALL identifiers to the chosen GSID
        identifiers_linked = 0
        for identifier in identifiers:
            # Determine which center_id to use for this identifier
            identifier_center = center_id

            # Check if this identifier already exists with a different center
            cur.execute(
                """
                SELECT center_id, global_subject_id
                FROM local_subject_ids
                WHERE local_subject_id = %s
                  AND identifier_type = %s
                LIMIT 1
                """,
                (identifier["local_subject_id"], identifier["identifier_type"]),
            )
            existing = cur.fetchone()

            if existing:
                existing_center = existing["center_id"]
                if existing_center != 0 and center_id == 0:
                    # Keep existing real center, don't overwrite with unknown
                    identifier_center = existing_center
                elif existing_center == 0 and center_id != 0:
                    # Update from unknown to known
                    identifier_center = center_id
                elif (
                    existing_center != center_id
                    and existing_center != 0
                    and center_id != 0
                ):
                    # Real conflict - use incoming but flag
                    identifier_center = center_id

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
                    identifier_center,
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
                identifiers[0]["local_subject_id"],
                identifiers[0]["identifier_type"],
                center_id,
                gsid,
                gsid,
                action,
                "multiple_gsid_conflict"
                if conflicts
                else "center_agnostic_match"
                if action == "link_existing"
                else "no_match",
                1.0
                if not conflicts and not center_conflicts
                else 0.7
                if center_conflicts
                else 0.5,
                candidate_ids_json,
                matched_gsids_json,
                (conflicts is not None) or (len(center_conflicts) > 0),
                f"Multi-GSID conflict: {conflicts}"
                if conflicts
                else f"Center conflicts: {center_conflicts}"
                if center_conflicts
                else None,
                created_by,
            ),
        )

        conn.commit()

        logger.info(
            f"Resolution complete: gsid={gsid}, action={action}, "
            f"identifiers={len(identifiers)}, conflicts={len(conflicts) if conflicts else 0}, "
            f"warnings={len(warnings)}"
        )

        return {
            "gsid": gsid,
            "action": action,
            "identifiers_linked": identifiers_linked,
            "conflicts": conflicts,
            "conflict_resolution": conflict_resolution,
            "warnings": warnings,
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"Error resolving subject: {e}", exc_info=True)
        raise
    finally:
        cur.close()
