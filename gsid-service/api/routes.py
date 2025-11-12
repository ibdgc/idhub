# gsid-service/api/routes.py
import logging
from typing import List

from core.database import get_db_connection
from core.security import verify_api_key
from fastapi import APIRouter, Depends, HTTPException
from services.gsid_generator import generate_gsid
from services.identity_resolution import (
    log_resolution,
    resolve_identity,
)

from .models import (
    BatchSubjectRequest,
    HealthResponse,
    ResolutionResponse,
    SubjectRequest,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/register", response_model=ResolutionResponse)
async def register_subject(
    request: SubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register or link a subject with identity resolution (requires API key)"""
    conn = get_db_connection()
    try:
        resolution = resolve_identity(
            conn,
            request.center_id,
            request.local_subject_id,
            request.identifier_type,
        )

        if resolution["action"] == "create_new":
            gsid = generate_gsid()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM subjects WHERE global_subject_id = %s", (gsid,))
            if cur.fetchone():
                gsid = generate_gsid()  # Regenerate if collision (rare)

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

        elif resolution["action"] == "review_required":
            gsid = resolution["gsid"]
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE subjects 
                SET flagged_for_review = TRUE,
                    review_notes = %s
                WHERE global_subject_id = %s
                """,
                (resolution["review_reason"], gsid),
            )

        log_resolution(conn, resolution, request)

        conn.commit()

        return ResolutionResponse(
            gsid=resolution["gsid"],
            action=resolution["action"],
            match_strategy=resolution["match_strategy"],
            confidence=resolution["confidence"],
            requires_review=resolution["action"] == "review_required",
            review_reason=resolution.get("review_reason"),
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"Registration error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/register/batch", response_model=List[ResolutionResponse])
async def register_subjects_batch(
    batch: BatchSubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register multiple subjects in a single request (requires API key)"""
    conn = get_db_connection()
    results = []

    try:
        for request in batch.requests:
            resolution = resolve_identity(
                conn,
                request.center_id,
                request.local_subject_id,
                request.identifier_type,
            )

            if resolution["action"] == "create_new":
                gsid = generate_gsid()
                cur = conn.cursor()
                cur.execute(
                    "SELECT 1 FROM subjects WHERE global_subject_id = %s", (gsid,)
                )
                if cur.fetchone():
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

            elif resolution["action"] == "review_required":
                gsid = resolution["gsid"]
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE subjects 
                    SET flagged_for_review = TRUE, review_notes = %s
                    WHERE global_subject_id = %s
                    """,
                    (resolution["review_reason"], gsid),
                )

            log_resolution(conn, resolution, request)

            results.append(
                ResolutionResponse(
                    gsid=resolution["gsid"],
                    action=resolution["action"],
                    match_strategy=resolution["match_strategy"],
                    confidence=resolution["confidence"],
                    requires_review=resolution["action"] == "review_required",
                    review_reason=resolution.get("review_reason"),
                )
            )

        conn.commit()
        return results

    except Exception as e:
        conn.rollback()
        logger.error(f"Batch registration error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/review-queue")
async def get_review_queue():
    """Get subjects flagged for manual review"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 
                s.global_subject_id,
                s.review_notes,
                c.name as center_name,
                array_agg(DISTINCT l.local_subject_id) as local_ids,
                s.created_at,
                s.withdrawn
            FROM subjects s
            JOIN centers c ON s.center_id = c.center_id
            LEFT JOIN local_subject_ids l ON s.global_subject_id = l.global_subject_id
            WHERE s.flagged_for_review = TRUE
            GROUP BY s.global_subject_id, s.review_notes, c.name, s.created_at, s.withdrawn
            ORDER BY s.created_at DESC
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


@router.post("/resolve-review/{gsid}")
async def resolve_review(gsid: str, reviewed_by: str, notes: str = None):
    """Mark a review as resolved"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE subjects 
            SET flagged_for_review = FALSE,
                review_notes = %s
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


@router.get("/health")
async def health():
    """Health check endpoint (public access)"""
    conn = None
    try:
        conn = get_db_connection()
        conn.cursor().execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception:
        return {"status": "unhealthy", "database": "disconnected"}
    finally:
        if conn:
            conn.close()


# In gsid-service routes
@router.patch("/subjects/{gsid}/center")
async def update_subject_center(
    gsid: str,
    request: UpdateCenterRequest,  # Pydantic model with center_id
    api_key: str = Depends(verify_api_key),
):
    """Update center_id for existing subject"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE subjects
                SET center_id = %s, updated_at = NOW()
                WHERE global_subject_id = %s
                RETURNING global_subject_id
                """,
                (request.center_id, gsid),
            )

            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="GSID not found")

            conn.commit()

            return {
                "gsid": gsid,
                "center_id": request.center_id,
                "action": "center_updated",
            }
    finally:
        return_db_connection(conn)
