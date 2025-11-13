# gsid-service/api/routes.py
import logging
from typing import List

import psycopg2.extras
from core.database import get_db_connection
from core.security import verify_api_key
from fastapi import APIRouter, Depends, HTTPException
from services.gsid_generator import generate_gsid
from services.identity_resolution import log_resolution, resolve_identity

from .models import (
    BatchSubjectRequest,
    HealthResponse,
    ResolutionResponse,
    SubjectRequest,
    UpdateCenterRequest,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/register", response_model=ResolutionResponse)
async def register_subject(
    request: SubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register or link a subject with identity resolution (requires API key)"""
    conn = None
    try:
        conn = get_db_connection()
        resolution = resolve_identity(
            conn,
            request.center_id,
            request.local_subject_id,
            request.identifier_type,
        )

        # Handle center promotion case
        if resolution["action"] == "center_promoted":
            gsid = resolution["gsid"]

            log_resolution(
                conn,
                local_subject_id=request.local_subject_id,
                identifier_type=request.identifier_type,
                action="center_promoted",
                gsid=gsid,
                matched_gsid=gsid,
                match_strategy=resolution["match_strategy"],
                confidence=resolution["confidence"],
                center_id=request.center_id,
                metadata={
                    "previous_center_id": resolution["previous_center_id"],
                    "new_center_id": resolution["new_center_id"],
                    "message": resolution["message"],
                },
            )
            conn.commit()

            return ResolutionResponse(
                gsid=gsid,
                action="center_promoted",
                match_strategy=resolution["match_strategy"],
                confidence=resolution["confidence"],
                requires_review=False,
                review_reason=None,
                message=resolution["message"],
            )

        # Handle review required case
        elif resolution["action"] == "review_required":
            gsid = resolution.get("existing_gsid")

            # Flag the subject for review
            if gsid:
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

            log_resolution(
                conn,
                local_subject_id=request.local_subject_id,
                identifier_type=request.identifier_type,
                action="review_required",
                gsid=gsid,
                matched_gsid=gsid,
                match_strategy=resolution["match_strategy"],
                confidence=resolution["confidence"],
                center_id=request.center_id,
                metadata={
                    "review_reason": resolution["review_reason"],
                    "existing_center_id": resolution.get("existing_center_id"),
                },
            )
            conn.commit()

            return ResolutionResponse(
                gsid=gsid,
                action="review_required",
                match_strategy=resolution["match_strategy"],
                confidence=resolution["confidence"],
                requires_review=True,
                review_reason=resolution["review_reason"],
                message=resolution["review_reason"],
            )

        # Handle create new subject
        elif resolution["action"] == "create_new":
            gsid = generate_gsid()
            cur = conn.cursor()

            # Check for collision (rare)
            cur.execute("SELECT 1 FROM subjects WHERE global_subject_id = %s", (gsid,))
            if cur.fetchone():
                gsid = generate_gsid()

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

            log_resolution(
                conn,
                local_subject_id=request.local_subject_id,
                identifier_type=request.identifier_type,
                action="create_new",
                gsid=gsid,
                matched_gsid=None,
                match_strategy=resolution["match_strategy"],
                confidence=resolution["confidence"],
                center_id=request.center_id,
                metadata={},
            )

        # Handle link to existing subject
        elif resolution["action"] == "link_existing":
            gsid = resolution["gsid"]
            cur = conn.cursor()

            # Add new identifier link
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

            log_resolution(
                conn,
                local_subject_id=request.local_subject_id,
                identifier_type=request.identifier_type,
                action="link_existing",
                gsid=gsid,
                matched_gsid=gsid,
                match_strategy=resolution["match_strategy"],
                confidence=resolution["confidence"],
                center_id=request.center_id,
                metadata={},
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
        )

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Registration error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.post("/register/batch", response_model=List[ResolutionResponse])
async def register_subjects_batch(
    batch: BatchSubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register multiple subjects in a single request (requires API key)"""
    conn = None
    results = []

    try:
        conn = get_db_connection()
        conn.autocommit = False

        for idx, request in enumerate(batch.requests):
            savepoint_name = f"sp_{idx}"
            cur = conn.cursor()

            try:
                # Create savepoint for this record
                cur.execute(f"SAVEPOINT {savepoint_name}")

                logger.debug(
                    f"Processing batch item {idx + 1}/{len(batch.requests)}: {request.local_subject_id}"
                )

                resolution = resolve_identity(
                    conn,
                    request.center_id,
                    request.local_subject_id,
                    request.identifier_type,
                )

                # Handle center promotion
                if resolution["action"] == "center_promoted":
                    gsid = resolution["gsid"]

                    log_resolution(
                        conn,
                        local_subject_id=request.local_subject_id,
                        identifier_type=request.identifier_type,
                        action="center_promoted",
                        gsid=gsid,
                        matched_gsid=gsid,
                        match_strategy=resolution["match_strategy"],
                        confidence=resolution["confidence"],
                        center_id=request.center_id,
                        metadata={
                            "previous_center_id": resolution["previous_center_id"],
                            "new_center_id": resolution["new_center_id"],
                            "message": resolution["message"],
                        },
                    )

                # Handle review required
                elif resolution["action"] == "review_required":
                    gsid = resolution.get("existing_gsid")

                    if gsid:
                        cur.execute(
                            """
                            UPDATE subjects 
                            SET flagged_for_review = TRUE, review_notes = %s
                            WHERE global_subject_id = %s
                            """,
                            (resolution["review_reason"], gsid),
                        )

                    log_resolution(
                        conn,
                        local_subject_id=request.local_subject_id,
                        identifier_type=request.identifier_type,
                        action="review_required",
                        gsid=gsid,
                        matched_gsid=gsid,
                        match_strategy=resolution["match_strategy"],
                        confidence=resolution["confidence"],
                        center_id=request.center_id,
                        metadata={
                            "review_reason": resolution["review_reason"],
                            "existing_center_id": resolution.get("existing_center_id"),
                        },
                    )

                # Handle create new
                elif resolution["action"] == "create_new":
                    gsid = generate_gsid()

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

                    log_resolution(
                        conn,
                        local_subject_id=request.local_subject_id,
                        identifier_type=request.identifier_type,
                        action="create_new",
                        gsid=gsid,
                        matched_gsid=None,
                        match_strategy=resolution["match_strategy"],
                        confidence=resolution["confidence"],
                        center_id=request.center_id,
                        metadata={},
                    )

                # Handle link existing
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

                    log_resolution(
                        conn,
                        local_subject_id=request.local_subject_id,
                        identifier_type=request.identifier_type,
                        action="link_existing",
                        gsid=gsid,
                        matched_gsid=gsid,
                        match_strategy=resolution["match_strategy"],
                        confidence=resolution["confidence"],
                        center_id=request.center_id,
                        metadata={},
                    )

                # Release savepoint - commit this record
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")

                results.append(
                    ResolutionResponse(
                        gsid=resolution.get("gsid"),
                        action=resolution["action"],
                        match_strategy=resolution["match_strategy"],
                        confidence=resolution["confidence"],
                        requires_review=resolution["action"] == "review_required",
                        review_reason=resolution.get("review_reason"),
                        message=resolution.get("message"),
                    )
                )

            except Exception as e:
                # Rollback to savepoint - only this record fails
                try:
                    cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                except:
                    pass

                error_msg = str(e)
                logger.error(
                    f"Error processing subject {request.local_subject_id} (item {idx + 1}): {error_msg}",
                    exc_info=True,
                )

                # Continue processing other subjects
                results.append(
                    ResolutionResponse(
                        gsid=None,
                        action="error",
                        match_strategy="none",
                        confidence=0.0,
                        requires_review=True,
                        review_reason=error_msg,
                        message=f"Error: {error_msg}",
                    )
                )
            finally:
                cur.close()

        # Commit all successful records
        conn.commit()
        logger.info(f"Batch processing complete: {len(results)} results")
        return results

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Batch registration error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/review-queue")
async def get_review_queue(api_key: str = Depends(verify_api_key)):
    """Get subjects flagged for manual review"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 
                s.global_subject_id,
                s.review_notes,
                c.name as center_name,
                s.center_id,
                array_agg(DISTINCT l.local_subject_id) as local_ids,
                array_agg(DISTINCT l.identifier_type) as identifier_types,
                s.created_at,
                s.withdrawn
            FROM subjects s
            JOIN centers c ON s.center_id = c.center_id
            LEFT JOIN local_subject_ids l ON s.global_subject_id = l.global_subject_id
            WHERE s.flagged_for_review = TRUE
            GROUP BY s.global_subject_id, s.review_notes, c.name, s.center_id, s.created_at, s.withdrawn
            ORDER BY s.created_at DESC
            """
        )
        return cur.fetchall()
    finally:
        if conn:
            conn.close()


@router.post("/resolve-review/{gsid}")
async def resolve_review(
    gsid: str,
    reviewed_by: str,
    notes: str = None,
    api_key: str = Depends(verify_api_key),
):
    """Mark a review as resolved"""
    conn = None
    try:
        conn = get_db_connection()
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
        if conn:
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


@router.patch("/subjects/{gsid}/center")
async def update_subject_center(
    gsid: str,
    request: UpdateCenterRequest,
    api_key: str = Depends(verify_api_key),
):
    """Update center_id for existing subject"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE subjects
            SET center_id = %s, updated_at = CURRENT_TIMESTAMP
            WHERE global_subject_id = %s
            RETURNING global_subject_id, center_id
            """,
            (request.center_id, gsid),
        )

        result = cur.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="GSID not found")

        conn.commit()

        return {
            "gsid": gsid,
            "center_id": request.center_id,
            "action": "center_updated",
        }
    finally:
        if conn:
            conn.close()

