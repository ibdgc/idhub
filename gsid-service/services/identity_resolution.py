# gsid-service/services/identity_resolution.py
import logging
from typing import Any, Dict, List, Optional

import psycopg2.extras

from .id_validator import IDValidator

logger = logging.getLogger(__name__)


def resolve_identity_multi_candidate(
    conn,
    center_id: int,
    candidate_ids: List[Dict[str, str]],
) -> Dict[str, Any]:
    """
    Resolve subject identity using multiple candidate IDs

    Args:
        conn: Database connection
        center_id: Center ID for the incoming record
        candidate_ids: List of dicts with 'local_subject_id' and 'identifier_type'

    Returns:
        Resolution result with action, GSIDs, flags, etc.
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Step 1: Validate all candidate IDs
        validation_results = IDValidator.validate_batch(
            [
                {"id": c["local_subject_id"], "type": c["identifier_type"]}
                for c in candidate_ids
            ]
        )

        # Check for validation errors
        validation_warnings = []
        has_validation_errors = False
        for candidate in candidate_ids:
            local_id = candidate["local_subject_id"]
            validation = validation_results.get(local_id, {})
            if not validation.get("valid", True):
                has_validation_errors = True
                validation_warnings.extend(validation.get("warnings", []))
            elif validation.get("warnings"):
                validation_warnings.extend(validation.get("warnings", []))

        # Step 2: Query for ALL candidate IDs
        matched_gsids = {}  # Maps GSID -> list of matched candidates
        matched_centers = {}  # Maps GSID -> center_id from subjects table

        for candidate in candidate_ids:
            local_id = candidate["local_subject_id"]
            identifier_type = candidate["identifier_type"]

            # Check if this ID exists in local_subject_ids
            cur.execute(
                """
                SELECT 
                    l.global_subject_id,
                    l.center_id as local_center_id,
                    l.identifier_type,
                    s.center_id as subject_center_id,
                    s.withdrawn,
                    s.flagged_for_review
                FROM local_subject_ids l
                JOIN subjects s ON l.global_subject_id = s.global_subject_id
                WHERE l.local_subject_id = %s
                ORDER BY l.created_at ASC
                """,
                (local_id,),
            )
            matches = cur.fetchall()

            for match in matches:
                gsid = match["global_subject_id"]
                if gsid not in matched_gsids:
                    matched_gsids[gsid] = []
                    matched_centers[gsid] = match["subject_center_id"]

                matched_gsids[gsid].append(
                    {
                        "local_subject_id": local_id,
                        "identifier_type": identifier_type,
                        "local_center_id": match["local_center_id"],
                        "subject_center_id": match["subject_center_id"],
                        "withdrawn": match["withdrawn"],
                        "flagged": match["flagged_for_review"],
                    }
                )

        # Step 3: Analyze matches and determine action
        num_gsids_found = len(matched_gsids)

        # Case 1: No matches - create new subject
        if num_gsids_found == 0:
            if has_validation_errors:
                return {
                    "action": "review_required",
                    "gsid": None,
                    "match_strategy": "validation_failed",
                    "confidence": 0.0,
                    "review_reason": f"ID validation failed: {'; '.join(validation_warnings)}",
                    "candidate_ids": candidate_ids,
                    "validation_warnings": validation_warnings,
                }

            return {
                "action": "create_new",
                "gsid": None,
                "match_strategy": "no_match",
                "confidence": 1.0,
                "candidate_ids": candidate_ids,
                "validation_warnings": validation_warnings,
            }

        # Case 2: Exactly one GSID found
        if num_gsids_found == 1:
            gsid = list(matched_gsids.keys())[0]
            matches = matched_gsids[gsid]
            subject_center_id = matched_centers[gsid]

            # Check if subject is withdrawn
            if any(m["withdrawn"] for m in matches):
                return {
                    "action": "review_required",
                    "gsid": gsid,
                    "match_strategy": "exact_withdrawn",
                    "confidence": 1.0,
                    "review_reason": "Subject previously withdrawn",
                    "candidate_ids": candidate_ids,
                    "matched_candidates": matches,
                    "validation_warnings": validation_warnings,
                }

            # Check for cross-center conflicts
            cross_center_conflict = False
            conflict_details = []

            for match in matches:
                local_center = match["local_center_id"]
                # Conflict if: different known centers (not Unknown)
                if (
                    local_center != center_id
                    and local_center not in [0, 1]
                    and center_id not in [0, 1]
                ):
                    cross_center_conflict = True
                    conflict_details.append(
                        f"ID '{match['local_subject_id']}' exists at center {local_center}, "
                        f"attempting to link at center {center_id}"
                    )

            if cross_center_conflict:
                return {
                    "action": "review_required",
                    "gsid": gsid,
                    "match_strategy": "cross_center_conflict",
                    "confidence": 0.8,
                    "review_reason": "; ".join(conflict_details),
                    "candidate_ids": candidate_ids,
                    "matched_candidates": matches,
                    "validation_warnings": validation_warnings,
                }

            # Check if center promotion is needed
            if subject_center_id in [0, 1] and center_id not in [0, 1]:
                return {
                    "action": "center_promoted",
                    "gsid": gsid,
                    "match_strategy": "center_promotion",
                    "confidence": 1.0,
                    "previous_center_id": subject_center_id,
                    "new_center_id": center_id,
                    "message": f"Promoted from center {subject_center_id} (Unknown) to {center_id}",
                    "candidate_ids": candidate_ids,
                    "matched_candidates": matches,
                    "validation_warnings": validation_warnings,
                }

            # Normal case: link to existing GSID
            return {
                "action": "link_existing",
                "gsid": gsid,
                "match_strategy": "exact_match",
                "confidence": 1.0,
                "candidate_ids": candidate_ids,
                "matched_candidates": matches,
                "validation_warnings": validation_warnings,
            }

        # Case 3: Multiple GSIDs found - CONFLICT!
        return {
            "action": "review_required",
            "gsid": None,
            "matched_gsids": list(matched_gsids.keys()),
            "match_strategy": "multiple_gsid_conflict",
            "confidence": 0.5,
            "review_reason": f"Multiple GSIDs found for candidate IDs: {', '.join([c['local_subject_id'] for c in candidate_ids])}. "
            f"Matched GSIDs: {', '.join(matched_gsids.keys())}. This may indicate subjects that should be merged.",
            "candidate_ids": candidate_ids,
            "all_matches": matched_gsids,
            "validation_warnings": validation_warnings,
        }

    except Exception as e:
        logger.error(
            f"Error in resolve_identity_multi_candidate: {str(e)}", exc_info=True
        )
        raise


def resolve_identity(
    conn, center_id: int, local_subject_id: str, identifier_type: str = "primary"
) -> Dict[str, Any]:
    """
    Single-candidate resolution (backward compatibility)
    Wraps multi-candidate resolution
    """
    return resolve_identity_multi_candidate(
        conn,
        center_id,
        [{"local_subject_id": local_subject_id, "identifier_type": identifier_type}],
    )


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
                review_reason,
                metadata,
                created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                local_subject_id,
                identifier_type,
                center_id,
                local_subject_id,
                gsid,
                matched_gsid,
                action,
                match_strategy,
                confidence,
                action == "review_required",
                metadata.get("review_reason") if metadata else None,
                psycopg2.extras.Json(metadata or {}),
                created_by,
            ),
        )
    except Exception as e:
        logger.error(f"Error logging resolution: {str(e)}", exc_info=True)
        raise
