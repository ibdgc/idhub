# gsid-service/api/routes.py
import logging
from typing import List

from core.database import get_db_connection
from core.security import verify_api_key
from fastapi import APIRouter, Depends, HTTPException
from services.gsid_generator import generate_gsid
from services.identity_resolution import log_resolution, resolve_identity

from .models import (
    BatchMultiCandidateRequest,
    BatchSubjectRequest,
    HealthResponse,
    MultiCandidateResponse,
    MultiCandidateSubjectRequest,
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
    """Register a subject and resolve identity"""

    logger.info(
        f">>> REGISTER REQUEST: center_id={request.center_id}, local_id={request.local_subject_id}, type={request.identifier_type}"
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
            f">>> RESOLUTION: action={resolution['action']}, gsid={resolution.get('gsid')}, strategy={resolution.get('match_strategy')}"
        )

        gsid = resolution.get("gsid")  # Initialize gsid

        if resolution["action"] == "create_new":
            gsid = generate_gsid()

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

            cur.execute(
                """
                INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    request.center_id,
                    request.local_subject_id,
                    request.identifier_type,
                    gsid,
                ),
            )

            resolution["gsid"] = gsid

        elif resolution["action"] == "link_existing":
            gsid = resolution["gsid"]

            cur.execute(
                """
                INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                """,
                (
                    request.center_id,
                    request.local_subject_id,
                    request.identifier_type,
                    gsid,
                ),
            )

        elif resolution["action"] == "center_promoted":
            gsid = resolution["gsid"]

            logger.info("=" * 80)
            logger.info(f"ENTERING CENTER_PROMOTED BLOCK")
            logger.info(f"  GSID: {gsid}")
            logger.info(f"  local_subject_id: {request.local_subject_id}")
            logger.info(f"  From center: 1 (Unknown)")
            logger.info(f"  To center: {request.center_id}")
            logger.info("=" * 80)

            # Check current state BEFORE any updates
            cur.execute(
                """
                SELECT s.center_id, 
                       (SELECT COUNT(*) FROM local_subject_ids WHERE global_subject_id = %s) as local_id_count,
                       (SELECT COUNT(*) FROM local_subject_ids WHERE global_subject_id = %s AND center_id = 1) as unknown_count
                FROM subjects s 
                WHERE s.global_subject_id = %s
                """,
                (gsid, gsid, gsid),
            )
            pre_state = cur.fetchone()

            if pre_state:
                logger.info(f"PRE-UPDATE STATE:")
                logger.info(f"  Subject center_id: {pre_state[0]}")
                logger.info(f"  Total local_subject_ids: {pre_state[1]}")
                logger.info(f"  Unknown center local_ids: {pre_state[2]}")

            # 1. Update subjects table
            logger.info(
                f"EXECUTING: UPDATE subjects SET center_id={request.center_id} WHERE global_subject_id={gsid} AND center_id=1"
            )

            cur.execute(
                """
                UPDATE subjects
                SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE global_subject_id = %s 
                  AND center_id = 1
                """,
                (request.center_id, gsid),
            )

            subject_rows = cur.rowcount
            logger.info(f"RESULT: {subject_rows} rows updated in subjects table")

            # Verify the update
            cur.execute(
                "SELECT center_id FROM subjects WHERE global_subject_id = %s", (gsid,)
            )
            new_center = cur.fetchone()
            logger.info(
                f"VERIFICATION: Subject center_id is now: {new_center[0] if new_center else 'NOT FOUND'}"
            )

            # 2. Update local_subject_ids
            logger.info(
                f"EXECUTING: UPDATE local_subject_ids SET center_id={request.center_id} WHERE gsid={gsid} AND local_id={request.local_subject_id} AND center_id=1"
            )

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
            logger.info(f"RESULT: {local_rows} rows updated in local_subject_ids table")

            # Verify local_subject_ids state
            cur.execute(
                """
                SELECT center_id, COUNT(*) 
                FROM local_subject_ids 
                WHERE global_subject_id = %s 
                GROUP BY center_id
                """,
                (gsid,),
            )
            local_state = cur.fetchall()
            logger.info(f"VERIFICATION: local_subject_ids distribution: {local_state}")

            if local_rows == 0:
                logger.warning("NO ROWS UPDATED - checking if record exists...")

                cur.execute(
                    """
                    SELECT center_id, local_subject_id, identifier_type
                    FROM local_subject_ids
                    WHERE global_subject_id = %s
                      AND local_subject_id = %s
                      AND identifier_type = %s
                    """,
                    (gsid, request.local_subject_id, request.identifier_type),
                )
                existing = cur.fetchall()
                logger.info(f"Existing records for this local_subject_id: {existing}")

                # Check if already promoted
                if any(row[0] == request.center_id for row in existing):
                    logger.info(
                        f"✓ Record already exists with center {request.center_id}"
                    )
                else:
                    logger.info(f"Inserting new record with center {request.center_id}")
                    cur.execute(
                        """
                        INSERT INTO local_subject_ids (
                            center_id, local_subject_id, identifier_type, global_subject_id
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                        """,
                        (
                            request.center_id,
                            request.local_subject_id,
                            request.identifier_type,
                            gsid,
                        ),
                    )
                    logger.info(f"INSERT completed, rowcount: {cur.rowcount}")

            logger.info("=" * 80)
            logger.info("EXITING CENTER_PROMOTED BLOCK")
            logger.info("=" * 80)

        elif resolution["action"] == "review_required":
            gsid = resolution["gsid"]

            cur.execute(
                """
                INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                """,
                (
                    request.center_id,
                    request.local_subject_id,
                    request.identifier_type,
                    gsid,
                ),
            )

        # Log the resolution
        log_resolution(conn, request, resolution)

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
    """Register multiple subjects in batch - each in its own transaction"""
    results = []

    for request in batch.requests:
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
                cur.execute(
                    """
                    INSERT INTO subjects (global_subject_id, center_id, registration_year, control)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        gsid,
                        request.center_id,
                        request.registration_year,
                        request.control,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        request.center_id,
                        request.local_subject_id,
                        request.identifier_type,
                        gsid,
                    ),
                )

                resolution["gsid"] = gsid

            elif resolution["action"] == "link_existing":
                gsid = resolution["gsid"]
                cur.execute(
                    """
                    INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                    """,
                    (
                        request.center_id,
                        request.local_subject_id,
                        request.identifier_type,
                        gsid,
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

                logger.info(f"[BATCH] Center promoted {gsid}: 1 -> {request.center_id}")

            # Log resolution with correct parameters
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
                created_by="api_batch",
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
            logger.error(f"Error processing subject {request.local_subject_id}: {e}")
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

    return results


@router.post("/register/multi-candidate", response_model=MultiCandidateResponse)
async def register_multi_candidate(
    request: MultiCandidateSubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register subject with multiple candidate IDs"""
    conn = None
    try:
        conn = get_db_connection()

        # For now, use the first candidate as primary
        # TODO: Implement full multi-candidate resolution logic
        primary_candidate = request.candidate_ids[0]

        resolution = resolve_identity(
            conn,
            request.center_id,
            primary_candidate.local_subject_id,
            primary_candidate.identifier_type,
        )

        cur = conn.cursor()

        if resolution["action"] == "create_new":
            gsid = generate_gsid()

            cur.execute(
                """
                INSERT INTO subjects (global_subject_id, center_id, registration_year, control)
                VALUES (%s, %s, %s, %s)
                """,
                (gsid, request.center_id, request.registration_year, request.control),
            )

            # Insert all candidate IDs
            for candidate in request.candidate_ids:
                cur.execute(
                    """
                    INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                    """,
                    (
                        request.center_id,
                        candidate.local_subject_id,
                        candidate.identifier_type,
                        gsid,
                    ),
                )

            resolution["gsid"] = gsid

        elif resolution["action"] == "link_existing":
            gsid = resolution["gsid"]

            # Link all candidate IDs to existing GSID
            for candidate in request.candidate_ids:
                cur.execute(
                    """
                    INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                    """,
                    (
                        request.center_id,
                        candidate.local_subject_id,
                        candidate.identifier_type,
                        gsid,
                    ),
                )

        # Log resolution for primary candidate with correct parameters
        log_resolution(
            conn,
            primary_candidate.local_subject_id,
            primary_candidate.identifier_type,
            resolution.get("action", "unknown"),
            resolution.get("gsid"),
            resolution.get("gsid"),  # matched_gsid same as gsid for now
            resolution.get("match_strategy", "unknown"),
            resolution.get("confidence", 0.0),
            request.center_id,
            metadata={
                "review_reason": resolution.get("review_reason"),
                "candidate_count": len(request.candidate_ids),
            },
            created_by="api_multi_candidate",
        )

        conn.commit()

        return MultiCandidateResponse(
            gsid=resolution.get("gsid"),
            candidate_ids=request.candidate_ids,
            center_id=request.center_id,
            action=resolution["action"],
            match_strategy=resolution.get("match_strategy"),
            confidence=resolution.get("confidence"),
            message=resolution.get("message"),
            review_reason=resolution.get("review_reason"),
        )

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error in multi-candidate registration: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.post(
    "/register/batch/multi-candidate", response_model=List[MultiCandidateResponse]
)
async def register_batch_multi_candidate(
    batch: BatchMultiCandidateRequest, api_key: str = Depends(verify_api_key)
):
    """Register multiple subjects with multiple candidate IDs - each in its own transaction"""
    results = []

    for request in batch.requests:
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            primary_candidate = request.candidate_ids[0]

            resolution = resolve_identity(
                conn,
                request.center_id,
                primary_candidate.local_subject_id,
                primary_candidate.identifier_type,
            )

            gsid = resolution.get("gsid")  # Initialize gsid

            if resolution["action"] == "create_new":
                gsid = generate_gsid()
                cur.execute(
                    """
                    INSERT INTO subjects (global_subject_id, center_id, registration_year, control)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        gsid,
                        request.center_id,
                        request.registration_year,
                        request.control,
                    ),
                )

                for candidate in request.candidate_ids:
                    cur.execute(
                        """
                        INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                        """,
                        (
                            request.center_id,
                            candidate.local_subject_id,
                            candidate.identifier_type,
                            gsid,
                        ),
                    )

                resolution["gsid"] = gsid

            elif resolution["action"] == "link_existing":
                gsid = resolution["gsid"]

                for candidate in request.candidate_ids:
                    cur.execute(
                        """
                        INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                        """,
                        (
                            request.center_id,
                            candidate.local_subject_id,
                            candidate.identifier_type,
                            gsid,
                        ),
                    )

            elif resolution["action"] == "center_promoted":
                gsid = resolution["gsid"]

                logger.info(
                    f"[BATCH] Center promotion for GSID {gsid}: "
                    f"Unknown (1) -> {request.center_id} for {primary_candidate.local_subject_id}"
                )

                # 1. Update subjects table - promote from Unknown to known center
                cur.execute(
                    """
                    UPDATE subjects
                    SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE global_subject_id = %s 
                      AND center_id = 1
                    """,
                    (request.center_id, gsid),
                )

                subject_rows = cur.rowcount
                logger.info(f"[BATCH] Updated {subject_rows} subject records")

                # 2. Update all local_subject_ids for this GSID from Unknown to known center
                # This handles ALL candidate IDs that were previously registered with Unknown
                cur.execute(
                    """
                    UPDATE local_subject_ids
                    SET center_id = %s
                    WHERE global_subject_id = %s 
                      AND center_id = 1
                    """,
                    (request.center_id, gsid),
                )

                local_rows = cur.rowcount
                logger.info(f"[BATCH] Updated {local_rows} local_subject_id records")

                # 3. Insert any new candidate IDs that don't exist yet
                for candidate in request.candidate_ids:
                    cur.execute(
                        """
                        INSERT INTO local_subject_ids (
                            center_id, local_subject_id, identifier_type, global_subject_id
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                        """,
                        (
                            request.center_id,
                            candidate.local_subject_id,
                            candidate.identifier_type,
                            gsid,
                        ),
                    )

                logger.info(
                    f"[BATCH] Center promotion complete for {gsid}: "
                    f"{subject_rows} subject(s), {local_rows} local_id(s) updated"
                )

            # Log resolution with correct parameters
            log_resolution(
                conn,
                primary_candidate.local_subject_id,
                primary_candidate.identifier_type,
                resolution.get("action", "unknown"),
                resolution.get("gsid"),
                resolution.get("gsid"),  # matched_gsid same as gsid for now
                resolution.get("match_strategy", "unknown"),
                resolution.get("confidence", 0.0),
                request.center_id,
                metadata={
                    "review_reason": resolution.get("review_reason"),
                    "candidate_count": len(request.candidate_ids),
                },
                created_by="api_batch_multi_candidate",
            )

            conn.commit()

            results.append(
                MultiCandidateResponse(
                    gsid=resolution.get("gsid"),
                    candidate_ids=request.candidate_ids,
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
                f"Error processing multi-candidate subject {request.candidate_ids[0].local_subject_id}: {e}"
            )
            logger.exception("Full traceback:")

            results.append(
                MultiCandidateResponse(
                    gsid=None,
                    candidate_ids=request.candidate_ids,
                    center_id=request.center_id,
                    action="error",
                    message=str(e),
                )
            )

        finally:
            if conn:
                conn.close()

    return results
