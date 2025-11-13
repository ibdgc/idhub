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
                INSERT INTO local_subject_ids (
                    center_id, local_subject_id, identifier_type, global_subject_id
                )
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
    """
    Register multiple subjects in batch - each in its own transaction

    Use case: Fragment-validator, REDCap pipeline, bulk imports

    Each request in the batch contains:
    - center_id: The research center ID
    - local_subject_id: The local identifier (e.g., consortium_id, local_id)
    - identifier_type: Type of identifier (e.g., "consortium_id", "local_id", "alias")
    - registration_year: Optional registration date
    - control: Whether this is a control subject
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

                cur.execute(
                    """
                    INSERT INTO subjects (
                        global_subject_id, center_id, registration_year, control
                    )
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
                    INSERT INTO local_subject_ids (
                        center_id, local_subject_id, identifier_type, global_subject_id
                    )
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
