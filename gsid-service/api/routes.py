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
    conn = get_db_connection()
    try:
        resolution = resolve_identity(
            conn,
            request.center_id,
            request.local_subject_id,
            request.identifier_type,
        )

        # Initialize gsid variable
        gsid = resolution.get("gsid")  # ← Add this line for safety

        if resolution["action"] == "create_new":
            gsid = generate_gsid()
            cur = conn.cursor()
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
            cur = conn.cursor()
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
            # Handle center promotion - update from Unknown (center_id=1) to known center
            gsid = resolution["gsid"]
            cur = conn.cursor()

            logger.info(
                f"Center promotion for GSID {gsid}: "
                f"Unknown (1) -> {request.center_id} for {request.local_subject_id}"
            )

            # 1. Update subjects table - promote from Unknown to known center
            cur.execute(
                """
                UPDATE subjects
                SET center_id = %s
                WHERE global_subject_id = %s 
                  AND center_id = 1
                RETURNING center_id
                """,
                (request.center_id, gsid),
            )

            subject_updated = cur.fetchone()
            if subject_updated:
                logger.info(f"Updated subject {gsid} center: 1 -> {request.center_id}")
            else:
                logger.warning(
                    f"Subject {gsid} center was not 1 (Unknown), "
                    f"may have been previously promoted"
                )

            # 2. Update local_subject_ids - change center from Unknown to known
            cur.execute(
                """
                UPDATE local_subject_ids
                SET center_id = %s
                WHERE global_subject_id = %s 
                  AND local_subject_id = %s
                  AND identifier_type = %s
                  AND center_id = 1
                RETURNING center_id
                """,
                (
                    request.center_id,
                    gsid,
                    request.local_subject_id,
                    request.identifier_type,
                ),
            )

            local_id_updated = cur.fetchone()

            # 3. If the local_subject_id wasn't in Unknown, insert it
            if not local_id_updated:
                logger.info(
                    f"No Unknown record found for {request.local_subject_id}, "
                    f"inserting with center {request.center_id}"
                )
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
            else:
                logger.info(
                    f"Updated local_subject_id {request.local_subject_id} "
                    f"center: 1 -> {request.center_id}"
                )

        elif resolution["action"] == "review_required":
            gsid = resolution["gsid"]
            # Just link the ID, don't modify the subject
            cur = conn.cursor()
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

        return {
            "gsid": gsid,  # ← Changed from resolution["gsid"] for consistency
            "action": resolution["action"],
            "match_strategy": resolution.get("match_strategy"),
            "confidence": resolution.get("confidence"),
            "local_subject_id": request.local_subject_id,
            "center_id": request.center_id,
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"Error registering subject: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
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

            # Log resolution with correct parameters
            log_resolution(
                conn,
                request.local_subject_id,
                request.identifier_type,
                resolution.get("action", "unknown"),
                resolution.get("gsid"),
                resolution.get("gsid"),  # matched_gsid same as gsid for now
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
