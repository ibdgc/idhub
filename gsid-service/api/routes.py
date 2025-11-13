# gsid-service/api/routes.py
import logging
from typing import List

import psycopg2.extras
from core.database import get_db_connection
from core.security import verify_api_key
from fastapi import APIRouter, Depends, HTTPException
from services.gsid_generator import generate_gsid
from services.identity_resolution import (
    log_resolution,
    resolve_identity,
    resolve_identity_multi_candidate,
)

from .models import (
    BatchMultiCandidateRequest,
    BatchSubjectRequest,
    HealthResponse,
    MultiCandidateSubjectRequest,
    ResolutionResponse,
    SubjectRequest,
    UpdateCenterRequest,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return HealthResponse(status="healthy", database="connected")
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")


@router.post("/register", response_model=ResolutionResponse)
async def register_subject(
    request: SubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register a single subject (single candidate ID)"""
    conn = None
    try:
        conn = get_db_connection()

        # Resolve identity
        resolution = resolve_identity(
            conn,
            center_id=request.center_id,
            local_subject_id=request.local_subject_id,
            identifier_type=request.identifier_type,
        )

        # Handle different actions
        if resolution["action"] == "create_new":
            gsid = generate_gsid()
            cur = conn.cursor()

            # Insert into subjects table
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

            # Insert into local_subject_ids
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
            gsid = resolution["gsid"]
            cur = conn.cursor()

            # Update subjects table with new center
            cur.execute(
                """
                UPDATE subjects
                SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE global_subject_id = %s
                """,
                (request.center_id, gsid),
            )

            # Insert new local_subject_id link
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

            logger.info(
                f"Center promoted for GSID {gsid}: "
                f"{resolution['previous_center_id']} -> {request.center_id}"
            )

        elif resolution["action"] == "review_required":
            # Flag subject for review if GSID exists
            if resolution.get("gsid"):
                gsid = resolution["gsid"]
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE subjects
                    SET flagged_for_review = TRUE,
                        review_notes = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE global_subject_id = %s
                    """,
                    (resolution.get("review_reason"), gsid),
                )

        # Log resolution
        log_resolution(
            conn,
            local_subject_id=request.local_subject_id,
            identifier_type=request.identifier_type,
            action=resolution["action"],
            gsid=resolution.get("gsid"),
            matched_gsid=resolution.get("gsid"),
            match_strategy=resolution["match_strategy"],
            confidence=resolution["confidence"],
            center_id=request.center_id,
            metadata=resolution,
            created_by=request.created_by,
        )

        conn.commit()

        return ResolutionResponse(
            gsid=resolution.get("gsid"),
            action=resolution["action"],
            match_strategy=resolution["match_strategy"],
            confidence=resolution["confidence"],
            requires_review=resolution["action"] == "review_required",
            review_reason=resolution.get("review_reason"),
            message=resolution.get("message"),
            validation_warnings=resolution.get("validation_warnings"),
        )

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error registering subject: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.post("/register/multi-candidate", response_model=ResolutionResponse)
async def register_subject_multi_candidate(
    request: MultiCandidateSubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register a subject with multiple candidate IDs"""
    conn = None
    try:
        conn = get_db_connection()

        # Convert candidate IDs to dict format
        candidate_ids = [
            {
                "local_subject_id": c.local_subject_id,
                "identifier_type": c.identifier_type,
            }
            for c in request.candidate_ids
        ]

        # Resolve identity with all candidates
        resolution = resolve_identity_multi_candidate(
            conn, center_id=request.center_id, candidate_ids=candidate_ids
        )

        # Handle different actions
        if resolution["action"] == "create_new":
            gsid = generate_gsid()
            cur = conn.cursor()

            # Insert into subjects table
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

            # Insert ALL candidate IDs into local_subject_ids
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
            logger.info(
                f"Created new GSID {gsid} with {len(request.candidate_ids)} candidate IDs"
            )

        elif resolution["action"] == "link_existing":
            gsid = resolution["gsid"]
            cur = conn.cursor()

            # Link ALL candidate IDs to existing GSID
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

            logger.info(
                f"Linked {len(request.candidate_ids)} candidate IDs to existing GSID {gsid}"
            )

        elif resolution["action"] == "center_promoted":
            gsid = resolution["gsid"]
            cur = conn.cursor()

            # Update subjects table with new center
            cur.execute(
                """
                UPDATE subjects
                SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE global_subject_id = %s
                """,
                (request.center_id, gsid),
            )

            # Link ALL candidate IDs
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

            logger.info(
                f"Center promoted for GSID {gsid}: "
                f"{resolution['previous_center_id']} -> {request.center_id}, "
                f"linked {len(request.candidate_ids)} candidate IDs"
            )

        elif resolution["action"] == "review_required":
            # Flag subject(s) for review
            gsids_to_flag = []
            if resolution.get("gsid"):
                gsids_to_flag.append(resolution["gsid"])
            if resolution.get("matched_gsids"):
                gsids_to_flag.extend(resolution["matched_gsids"])

            cur = conn.cursor()
            for gsid in gsids_to_flag:
                cur.execute(
                    """
                    UPDATE subjects
                    SET flagged_for_review = TRUE,
                        review_notes = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE global_subject_id = %s
                    """,
                    (resolution.get("review_reason"), gsid),
                )

            logger.warning(
                f"Flagged {len(gsids_to_flag)} subjects for review: {resolution.get('review_reason')}"
            )

        # Log resolution for each candidate ID
        for candidate in request.candidate_ids:
            log_resolution(
                conn,
                local_subject_id=candidate.local_subject_id,
                identifier_type=candidate.identifier_type,
                action=resolution["action"],
                gsid=resolution.get("gsid"),
                matched_gsid=resolution.get("gsid"),
                match_strategy=resolution["match_strategy"],
                confidence=resolution["confidence"],
                center_id=request.center_id,
                metadata=resolution,
                created_by=request.created_by,
            )

        conn.commit()

        return ResolutionResponse(
            gsid=resolution.get("gsid"),
            action=resolution["action"],
            match_strategy=resolution["match_strategy"],
            confidence=resolution["confidence"],
            requires_review=resolution["action"] == "review_required",
            review_reason=resolution.get("review_reason"),
            message=resolution.get("message"),
            matched_gsids=resolution.get("matched_gsids"),
            validation_warnings=resolution.get("validation_warnings"),
        )

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error registering multi-candidate subject: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.post("/register/batch")
async def register_subjects_batch(
    request: BatchSubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register multiple subjects in batch (single candidate per subject)"""
    results = []
    conn = None

    try:
        conn = get_db_connection()

        for idx, subject_request in enumerate(request.requests):
            try:
                # Resolve identity
                resolution = resolve_identity(
                    conn,
                    center_id=subject_request.center_id,
                    local_subject_id=subject_request.local_subject_id,
                    identifier_type=subject_request.identifier_type,
                )

                # Handle different actions
                if resolution["action"] == "create_new":
                    gsid = generate_gsid()
                    cur = conn.cursor()

                    cur.execute(
                        """
                        INSERT INTO subjects (global_subject_id, center_id, registration_year, control)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            gsid,
                            subject_request.center_id,
                            subject_request.registration_year,
                            subject_request.control,
                        ),
                    )

                    cur.execute(
                        """
                        INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            subject_request.center_id,
                            subject_request.local_subject_id,
                            subject_request.identifier_type,
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
                            subject_request.center_id,
                            subject_request.local_subject_id,
                            subject_request.identifier_type,
                            gsid,
                        ),
                    )

                elif resolution["action"] == "center_promoted":
                    gsid = resolution["gsid"]
                    cur = conn.cursor()

                    cur.execute(
                        """
                        UPDATE subjects
                        SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE global_subject_id = %s
                        """,
                        (subject_request.center_id, gsid),
                    )

                    cur.execute(
                        """
                        INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                        """,
                        (
                            subject_request.center_id,
                            subject_request.local_subject_id,
                            subject_request.identifier_type,
                            gsid,
                        ),
                    )

                elif resolution["action"] == "review_required":
                    if resolution.get("gsid"):
                        gsid = resolution["gsid"]
                        cur = conn.cursor()
                        cur.execute(
                            """
                            UPDATE subjects
                            SET flagged_for_review = TRUE,
                                review_notes = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE global_subject_id = %s
                            """,
                            (resolution.get("review_reason"), gsid),
                        )

                # Log resolution
                log_resolution(
                    conn,
                    local_subject_id=subject_request.local_subject_id,
                    identifier_type=subject_request.identifier_type,
                    action=resolution["action"],
                    gsid=resolution.get("gsid"),
                    matched_gsid=resolution.get("gsid"),
                    match_strategy=resolution["match_strategy"],
                    confidence=resolution["confidence"],
                    center_id=subject_request.center_id,
                    metadata=resolution,
                    created_by=subject_request.created_by,
                )

                results.append(
                    {
                        "gsid": resolution.get("gsid"),
                        "local_subject_id": subject_request.local_subject_id,
                        "center_id": subject_request.center_id,
                        "action": resolution["action"],
                        "match_strategy": resolution["match_strategy"],
                        "confidence": resolution["confidence"],
                        "requires_review": resolution["action"] == "review_required",
                        "review_reason": resolution.get("review_reason"),
                        "validation_warnings": resolution.get("validation_warnings"),
                    }
                )

            except Exception as e:
                logger.error(
                    f"Error processing subject {subject_request.local_subject_id} (item {idx}): {e}",
                    exc_info=True,
                )
                results.append(
                    {
                        "gsid": None,
                        "local_subject_id": subject_request.local_subject_id,
                        "center_id": subject_request.center_id,
                        "action": "error",
                        "error": str(e),
                    }
                )

        conn.commit()
        logger.info(f"Batch processing complete: {len(results)} results")
        return results

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Batch processing failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.post("/register/batch/multi-candidate")
async def register_subjects_batch_multi_candidate(
    request: BatchMultiCandidateRequest, api_key: str = Depends(verify_api_key)
):
    """Register multiple subjects in batch (multiple candidates per subject)"""
    results = []
    conn = None

    try:
        conn = get_db_connection()

        for idx, subject_request in enumerate(request.requests):
            try:
                # Convert candidate IDs to dict format
                candidate_ids = [
                    {
                        "local_subject_id": c.local_subject_id,
                        "identifier_type": c.identifier_type,
                    }
                    for c in subject_request.candidate_ids
                ]

                # Resolve identity with all candidates
                resolution = resolve_identity_multi_candidate(
                    conn,
                    center_id=subject_request.center_id,
                    candidate_ids=candidate_ids,
                )

                # Handle different actions (same logic as single multi-candidate endpoint)
                if resolution["action"] == "create_new":
                    gsid = generate_gsid()
                    cur = conn.cursor()

                    cur.execute(
                        """
                        INSERT INTO subjects (global_subject_id, center_id, registration_year, control)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            gsid,
                            subject_request.center_id,
                            subject_request.registration_year,
                            subject_request.control,
                        ),
                    )

                    for candidate in subject_request.candidate_ids:
                        cur.execute(
                            """
                            INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                            """,
                            (
                                subject_request.center_id,
                                candidate.local_subject_id,
                                candidate.identifier_type,
                                gsid,
                            ),
                        )

                    resolution["gsid"] = gsid

                elif resolution["action"] == "link_existing":
                    gsid = resolution["gsid"]
                    cur = conn.cursor()

                    for candidate in subject_request.candidate_ids:
                        cur.execute(
                            """
                            INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                            """,
                            (
                                subject_request.center_id,
                                candidate.local_subject_id,
                                candidate.identifier_type,
                                gsid,
                            ),
                        )

                elif resolution["action"] == "center_promoted":
                    gsid = resolution["gsid"]
                    cur = conn.cursor()

                    cur.execute(
                        """
                        UPDATE subjects
                        SET center_id = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE global_subject_id = %s
                        """,
                        (subject_request.center_id, gsid),
                    )

                    for candidate in subject_request.candidate_ids:
                        cur.execute(
                            """
                            INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
                            """,
                            (
                                subject_request.center_id,
                                candidate.local_subject_id,
                                candidate.identifier_type,
                                gsid,
                            ),
                        )

                elif resolution["action"] == "review_required":
                    gsids_to_flag = []
                    if resolution.get("gsid"):
                        gsids_to_flag.append(resolution["gsid"])
                    if resolution.get("matched_gsids"):
                        gsids_to_flag.extend(resolution["matched_gsids"])

                    cur = conn.cursor()
                    for gsid in gsids_to_flag:
                        cur.execute(
                            """
                            UPDATE subjects
                            SET flagged_for_review = TRUE,
                                review_notes = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE global_subject_id = %s
                            """,
                            (resolution.get("review_reason"), gsid),
                        )

                # Log resolution for each candidate ID
                for candidate in subject_request.candidate_ids:
                    log_resolution(
                        conn,
                        local_subject_id=candidate.local_subject_id,
                        identifier_type=candidate.identifier_type,
                        action=resolution["action"],
                        gsid=resolution.get("gsid"),
                        matched_gsid=resolution.get("gsid"),
                        match_strategy=resolution["match_strategy"],
                        confidence=resolution["confidence"],
                        center_id=subject_request.center_id,
                        metadata=resolution,
                        created_by=subject_request.created_by,
                    )

                results.append(
                    {
                        "gsid": resolution.get("gsid"),
                        "candidate_ids": [
                            c.local_subject_id for c in subject_request.candidate_ids
                        ],
                        "center_id": subject_request.center_id,
                        "action": resolution["action"],
                        "match_strategy": resolution["match_strategy"],
                        "confidence": resolution["confidence"],
                        "requires_review": resolution["action"] == "review_required",
                        "review_reason": resolution.get("review_reason"),
                        "matched_gsids": resolution.get("matched_gsids"),
                        "validation_warnings": resolution.get("validation_warnings"),
                    }
                )

            except Exception as e:
                logger.error(
                    f"Error processing multi-candidate subject (item {idx}): {e}",
                    exc_info=True,
                )
                results.append(
                    {
                        "gsid": None,
                        "candidate_ids": [
                            c.local_subject_id for c in subject_request.candidate_ids
                        ],
                        "center_id": subject_request.center_id,
                        "action": "error",
                        "error": str(e),
                    }
                )

        conn.commit()
        logger.info(
            f"Batch multi-candidate processing complete: {len(results)} results"
        )
        return results

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Batch multi-candidate processing failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/subjects/{gsid}")
async def get_subject(gsid: str, api_key: str = Depends(verify_api_key)):
    """Get subject details by GSID"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Get subject info
        cur.execute(
            """
            SELECT s.*, c.name as center_name
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
            SELECT center_id, local_subject_id, identifier_type, created_at
            FROM local_subject_ids
            WHERE global_subject_id = %s
            ORDER BY created_at ASC
            """,
            (gsid,),
        )
        local_ids = cur.fetchall()

        return {
            "subject": dict(subject),
            "local_ids": [dict(row) for row in local_ids],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching subject {gsid}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/review/flagged")
async def get_flagged_subjects(
    limit: int = 100, api_key: str = Depends(verify_api_key)
):
    """Get subjects flagged for review"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            """
            SELECT s.global_subject_id, s.center_id, s.review_notes, s.created_at, s.updated_at,
                   c.name as center_name,
                   COUNT(l.local_subject_id) as num_local_ids
            FROM subjects s
            LEFT JOIN centers c ON s.center_id = c.center_id
            LEFT JOIN local_subject_ids l ON s.global_subject_id = l.global_subject_id
            WHERE s.flagged_for_review = TRUE
            GROUP BY s.global_subject_id, s.center_id, s.review_notes, s.created_at, s.updated_at, c.name
            ORDER BY s.updated_at DESC
            LIMIT %s
            """,
            (limit,),
        )

        flagged = cur.fetchall()
        return [dict(row) for row in flagged]

    except Exception as e:
        logger.error(f"Error fetching flagged subjects: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()
