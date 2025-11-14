# gsid-service/api/routes.py
import logging
from typing import List

from core.database import get_db_connection
from core.security import verify_api_key
from fastapi import APIRouter, Depends, HTTPException
from services.gsid_generator import generate_gsid
from services.identity_resolution import log_resolution, resolve_identity

from .models import (
    BatchSubjectRequest,
    HealthResponse,
    MultiIdentifierSubjectRequest,
    MultiIdentifierSubjectResponse,
    SubjectRequest,
    SubjectResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(status="healthy", service="gsid-service", version="1.0.0")


@router.post("/register", dependencies=[Depends(verify_api_key)])
async def register_subject(request: SubjectRequest):
    """
    Register a single subject and resolve identity
    Use case: Manual testing, single record registration
    """
    logger.info(
        f">>> REGISTER REQUEST: center_id={request.center_id}, "
        f"local_id={request.local_subject_id}, type={request.identifier_type}"
    )

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        resolution = resolve_identity(
            conn,
            request.center_id,
            request.local_subject_id,
            request.identifier_type,
        )

        logger.info(
            f">>> RESOLUTION: action={resolution['action']}, "
            f"gsid={resolution.get('gsid')}, strategy={resolution.get('match_strategy')}"
        )

        gsid = resolution.get("gsid")

        if resolution["action"] == "create_new":
            gsid = generate_gsid()

            # Insert into subjects table with created_by
            cur.execute(
                """
                INSERT INTO subjects (
                    global_subject_id, center_id, control,
                    registration_year, created_by
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    gsid,
                    request.center_id,
                    request.control,
                    request.registration_year,
                    request.created_by,
                ),
            )

            # Insert into local_subject_ids with created_by
            cur.execute(
                """
                INSERT INTO local_subject_ids (
                    center_id, local_subject_id, identifier_type,
                    global_subject_id, created_by
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    request.center_id,
                    request.local_subject_id,
                    request.identifier_type,
                    gsid,
                    request.created_by,
                ),
            )

            resolution["gsid"] = gsid

        elif resolution["action"] == "link_existing":
            gsid = resolution["gsid"]

            # Insert into local_subject_ids with created_by
            cur.execute(
                """
                INSERT INTO local_subject_ids (
                    center_id, local_subject_id, identifier_type,
                    global_subject_id, created_by
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                """,
                (
                    request.center_id,
                    request.local_subject_id,
                    request.identifier_type,
                    gsid,
                    request.created_by,
                ),
            )

        elif resolution["action"] == "center_promoted":
            gsid = resolution["gsid"]
            logger.info(
                f"Center promotion for GSID {gsid}: "
                f"Unknown (1) -> {request.center_id} for {request.local_subject_id}"
            )

            # Update subjects table
            cur.execute(
                """
                UPDATE subjects
                SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE global_subject_id = %s AND center_id = 1
                """,
                (request.center_id, gsid),
            )
            subject_rows = cur.rowcount
            logger.info(f"Updated {subject_rows} subject record(s)")

            # Update local_subject_ids
            cur.execute(
                """
                UPDATE local_subject_ids
                SET center_id = %s
                WHERE global_subject_id = %s
                  AND local_subject_id = %s
                  AND identifier_type = %s
                  AND center_id = 1
                """,
                (
                    request.center_id,
                    gsid,
                    request.local_subject_id,
                    request.identifier_type,
                ),
            )
            local_rows = cur.rowcount
            logger.info(f"Updated {local_rows} local_subject_id record(s)")

        elif resolution["action"] == "review_required":
            gsid = resolution["gsid"]

            # Insert into local_subject_ids with created_by
            cur.execute(
                """
                INSERT INTO local_subject_ids (
                    center_id, local_subject_id, identifier_type,
                    global_subject_id, created_by
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                """,
                (
                    request.center_id,
                    request.local_subject_id,
                    request.identifier_type,
                    gsid,
                    request.created_by,
                ),
            )

        # Log the resolution with correct parameters
        log_resolution(
            conn,
            request.local_subject_id,
            request.identifier_type,
            resolution.get("action", "unknown"),
            gsid,
            resolution.get("gsid"),
            resolution.get("match_strategy", "unknown"),
            resolution.get("confidence", 0.0),
            request.center_id,
            metadata={"review_reason": resolution.get("review_reason")},
            created_by=request.created_by,
        )

        conn.commit()
        logger.info(f"✓ Transaction committed for {gsid}")

        return {
            "gsid": gsid,
            "action": resolution["action"],
            "match_strategy": resolution.get("match_strategy"),
            "confidence": resolution.get("confidence"),
            "local_subject_id": request.local_subject_id,
            "center_id": request.center_id,
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Error registering subject: {e}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()


@router.post("/register/batch", response_model=List[SubjectResponse])
async def register_batch(
    batch: BatchSubjectRequest, api_key: str = Depends(verify_api_key)
):
    """
    Register multiple subjects in batch - each in its own transaction
    Use case: Fragment-validator, REDCap pipeline, bulk imports

    Each request in the batch contains:
    - center_id: The research center ID
    - local_subject_id: The local identifier (e.g., consortium_id, local_id)
    - identifier_type: Type of identifier (e.g., "consortium_id", "local_id", "alias")
    - registration_year: Optional registration date
    - control: Whether this is a control subject
    - created_by: Source system creating this record
    """
    results = []
    logger.info(f"Processing batch of {len(batch.requests)} subjects")

    for idx, request in enumerate(batch.requests, 1):
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            # Resolve identity
            resolution = resolve_identity(
                conn,
                request.center_id,
                request.local_subject_id,
                request.identifier_type,
            )

            gsid = resolution.get("gsid")

            if resolution["action"] == "create_new":
                gsid = generate_gsid()

                # Insert into subjects with created_by
                cur.execute(
                    """
                    INSERT INTO subjects (
                        global_subject_id, center_id, registration_year,
                        control, created_by
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        gsid,
                        request.center_id,
                        request.registration_year,
                        request.control,
                        request.created_by,
                    ),
                )

                # Insert into local_subject_ids with created_by
                cur.execute(
                    """
                    INSERT INTO local_subject_ids (
                        center_id, local_subject_id, identifier_type,
                        global_subject_id, created_by
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        request.center_id,
                        request.local_subject_id,
                        request.identifier_type,
                        gsid,
                        request.created_by,
                    ),
                )

                resolution["gsid"] = gsid

            elif resolution["action"] == "link_existing":
                gsid = resolution["gsid"]

                # Insert into local_subject_ids with created_by
                cur.execute(
                    """
                    INSERT INTO local_subject_ids (
                        center_id, local_subject_id, identifier_type,
                        global_subject_id, created_by
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                    """,
                    (
                        request.center_id,
                        request.local_subject_id,
                        request.identifier_type,
                        gsid,
                        request.created_by,
                    ),
                )

            elif resolution["action"] == "center_promoted":
                gsid = resolution["gsid"]

                # Update subjects table
                cur.execute(
                    """
                    UPDATE subjects
                    SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE global_subject_id = %s AND center_id = 1
                    """,
                    (request.center_id, gsid),
                )

                # Update local_subject_ids
                cur.execute(
                    """
                    UPDATE local_subject_ids
                    SET center_id = %s
                    WHERE global_subject_id = %s
                      AND local_subject_id = %s
                      AND identifier_type = %s
                      AND center_id = 1
                    """,
                    (
                        request.center_id,
                        gsid,
                        request.local_subject_id,
                        request.identifier_type,
                    ),
                )

                logger.info(
                    f"[BATCH {idx}/{len(batch.requests)}] Center promoted {gsid}: "
                    f"1 -> {request.center_id}"
                )

            elif resolution["action"] == "review_required":
                gsid = resolution["gsid"]

                # Insert into local_subject_ids with created_by
                cur.execute(
                    """
                    INSERT INTO local_subject_ids (
                        center_id, local_subject_id, identifier_type,
                        global_subject_id, created_by
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                    """,
                    (
                        request.center_id,
                        request.local_subject_id,
                        request.identifier_type,
                        gsid,
                        request.created_by,
                    ),
                )

            # Log resolution
            log_resolution(
                conn,
                request.local_subject_id,
                request.identifier_type,
                resolution.get("action", "unknown"),
                resolution.get("gsid"),
                resolution.get("gsid"),
                resolution.get("match_strategy", "unknown"),
                resolution.get("confidence", 0.0),
                request.center_id,
                metadata={"review_reason": resolution.get("review_reason")},
                created_by=request.created_by,
            )

            conn.commit()

            results.append(
                SubjectResponse(
                    gsid=resolution.get("gsid"),
                    local_subject_id=request.local_subject_id,
                    identifier_type=request.identifier_type,
                    center_id=request.center_id,
                    action=resolution["action"],
                    match_strategy=resolution.get("match_strategy"),
                    confidence=resolution.get("confidence"),
                    message=resolution.get("message"),
                    review_reason=resolution.get("review_reason"),
                )
            )

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(
                f"[BATCH {idx}/{len(batch.requests)}] Error processing "
                f"subject {request.local_subject_id}: {e}"
            )
            results.append(
                SubjectResponse(
                    gsid=None,
                    local_subject_id=request.local_subject_id,
                    identifier_type=request.identifier_type,
                    center_id=request.center_id,
                    action="error",
                    message=str(e),
                )
            )
        finally:
            if conn:
                conn.close()

    logger.info(
        f"Batch complete: {len([r for r in results if r.action != 'error'])}/{len(results)} successful"
    )
    return results


@router.post(
    "/register/multi-identifier", response_model=MultiIdentifierSubjectResponse
)
async def register_subject_multi_identifier(
    request: MultiIdentifierSubjectRequest, api_key: str = Depends(verify_api_key)
):
    """
    Register a single subject with multiple identifiers

    This endpoint handles the case where one subject has multiple local IDs
    (e.g., consortium_id, local_id, alias) and ensures they all map to the
    same GSID.

    Process:
    1. Check each identifier against existing records
    2. If any identifier already exists, link all others to that GSID
    3. If multiple different GSIDs are found, flag as conflict
    4. If no matches, create new GSID and link all identifiers
    """
    if not request.identifiers:
        raise HTTPException(status_code=400, detail="At least one identifier required")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Step 1: Check all identifiers for existing GSIDs
        found_gsids = set()
        identifier_resolutions = []

        for identifier in request.identifiers:
            resolution = resolve_identity(
                conn,
                request.center_id,
                identifier.local_subject_id,
                identifier.identifier_type,
            )

            identifier_resolutions.append(
                {
                    "local_subject_id": identifier.local_subject_id,
                    "identifier_type": identifier.identifier_type,
                    "resolution": resolution,
                }
            )

            if resolution.get("gsid"):
                found_gsids.add(resolution["gsid"])

        # Step 2: Determine final action
        if len(found_gsids) > 1:
            # CONFLICT: Multiple different GSIDs found
            logger.error(
                f"GSID conflict for center {request.center_id}: "
                f"Found {len(found_gsids)} different GSIDs: {found_gsids}"
            )

            # Use the first GSID but flag for review
            gsid = sorted(found_gsids)[0]
            action = "conflict_detected"

            # Flag subject for review
            cur.execute(
                """
                UPDATE subjects
                SET flagged_for_review = TRUE,
                    review_notes = %s
                WHERE global_subject_id = %s
                """,
                (
                    f"Multiple GSIDs detected during multi-identifier registration: {found_gsids}",
                    gsid,
                ),
            )

        elif len(found_gsids) == 1:
            # LINK: One GSID found, link all identifiers to it
            gsid = found_gsids.pop()
            action = "link_existing"

        else:
            # CREATE: No existing GSID, create new one
            gsid = generate_gsid()
            action = "create_new"

            # Insert into subjects table
            cur.execute(
                """
                INSERT INTO subjects (
                    global_subject_id, center_id, registration_year,
                    control, created_by
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    gsid,
                    request.center_id,
                    request.registration_year,
                    request.control,
                    request.created_by,
                ),
            )

        # Step 3: Link all identifiers to the final GSID
        identifiers_processed = []

        for identifier in request.identifiers:
            cur.execute(
                """
                INSERT INTO local_subject_ids (
                    center_id, local_subject_id, identifier_type,
                    global_subject_id, created_by
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (center_id, local_subject_id, identifier_type) 
                DO UPDATE SET global_subject_id = EXCLUDED.global_subject_id
                """,
                (
                    request.center_id,
                    identifier.local_subject_id,
                    identifier.identifier_type,
                    gsid,
                    request.created_by,
                ),
            )

            identifiers_processed.append(
                {
                    "local_subject_id": identifier.local_subject_id,
                    "identifier_type": identifier.identifier_type,
                    "linked_to_gsid": gsid,
                }
            )

        # Step 4: Log the resolution
        log_resolution(
            conn,
            ", ".join([i.local_subject_id for i in request.identifiers]),
            "multi_identifier",
            action,
            gsid,
            gsid,
            "multi_identifier_registration",
            1.0 if len(found_gsids) <= 1 else 0.5,
            request.center_id,
            metadata={
                "identifier_count": len(request.identifiers),
                "conflicts": list(found_gsids) if len(found_gsids) > 1 else None,
            },
            created_by=request.created_by,
        )

        conn.commit()

        logger.info(
            f"✓ Multi-identifier registration complete: {gsid} "
            f"({len(request.identifiers)} identifiers, action={action})"
        )

        return MultiIdentifierSubjectResponse(
            gsid=gsid,
            center_id=request.center_id,
            action=action,
            identifiers_processed=identifiers_processed,
            conflicts=list(found_gsids) if len(found_gsids) > 1 else None,
            match_strategy="multi_identifier_registration",
            confidence=1.0 if len(found_gsids) <= 1 else 0.5,
            message=f"Successfully registered {len(request.identifiers)} identifiers",
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Error in multi-identifier registration: {e}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()


@router.get("/subjects/{gsid}")
async def get_subject(gsid: str, api_key: str = Depends(verify_api_key)):
    """Get subject details by GSID"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 
                s.global_subject_id,
                s.center_id,
                c.name as center_name,
                s.registration_year,
                s.control,
                s.withdrawn,
                s.flagged_for_review,
                s.created_by,
                s.created_at
            FROM subjects s
            LEFT JOIN centers c ON s.center_id = c.center_id
            WHERE s.global_subject_id = %s
            """,
            (gsid,),
        )
        subject = cur.fetchone()

        if not subject:
            raise HTTPException(status_code=404, detail="Subject not found")

        # Get all local IDs
        cur.execute(
            """
            SELECT 
                center_id,
                local_subject_id,
                identifier_type,
                created_by,
                created_at
            FROM local_subject_ids
            WHERE global_subject_id = %s
            ORDER BY created_at ASC
            """,
            (gsid,),
        )
        local_ids = cur.fetchall()

        return {
            "gsid": subject[0],
            "center_id": subject[1],
            "center_name": subject[2],
            "registration_year": subject[3],
            "control": subject[4],
            "withdrawn": subject[5],
            "flagged_for_review": subject[6],
            "created_by": subject[7],
            "created_at": subject[8],
            "local_ids": [
                {
                    "center_id": lid[0],
                    "local_subject_id": lid[1],
                    "identifier_type": lid[2],
                    "created_by": lid[3],
                    "created_at": lid[4],
                }
                for lid in local_ids
            ],
        }
    finally:
        conn.close()


@router.post("/subjects/{gsid}/withdraw", dependencies=[Depends(verify_api_key)])
async def withdraw_subject(gsid: str, reason: str = None):
    """Mark a subject as withdrawn"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE subjects 
            SET withdrawn = TRUE,
                flagged_for_review = TRUE,
                review_notes = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE global_subject_id = %s
            RETURNING global_subject_id
            """,
            (reason or "Subject withdrawn", gsid),
        )

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Subject not found")

        conn.commit()
        return {"status": "withdrawn", "gsid": gsid, "reason": reason}
    finally:
        conn.close()


@router.post("/subjects/{gsid}/resolve", dependencies=[Depends(verify_api_key)])
async def resolve_review(gsid: str, reviewed_by: str, notes: str = None):
    """Resolve a subject flagged for review"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE subjects 
            SET flagged_for_review = FALSE,
                review_notes = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE global_subject_id = %s
            """,
            (notes, gsid),
        )

        cur.execute(
            """
            UPDATE identity_resolutions
            SET reviewed_by = %s,
                reviewed_at = CURRENT_TIMESTAMP,
                resolution_notes = %s
            WHERE matched_gsid = %s AND requires_review = TRUE
            """,
            (reviewed_by, notes, gsid),
        )

        conn.commit()
        return {"status": "resolved", "gsid": gsid}
    finally:
        conn.close()
