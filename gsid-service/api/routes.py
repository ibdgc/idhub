# gsid-service/api/routes.py
import logging

from core.database import get_db_connection
from core.security import verify_api_key
from fastapi import APIRouter, Depends, HTTPException

from .models import (
    HealthResponse,
    SubjectRegistrationRequest,
    SubjectRegistrationResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/register/subject", dependencies=[Depends(verify_api_key)])
async def register_subject(
    request: SubjectRegistrationRequest,
) -> SubjectRegistrationResponse:
    """
    Register ONE subject with one or more identifiers.

    This is the primary endpoint for both REDCap pipeline and fragment validator.

    Logic:
    - Checks if ANY identifier already exists
    - If multiple GSIDs found → flags conflict, uses oldest
    - Links ALL identifiers to chosen GSID

    Example request:
    {
        "center_id": 24,
        "identifiers": [
            {"local_subject_id": "IBDGC-013", "identifier_type": "local_id"},
            {"local_subject_id": "C813149-963575", "identifier_type": "consortium_id"}
        ],
        "registration_year": "2024-01-15",
        "control": false,
        "created_by": "redcap_pipeline"
    }
    """
    from services.identity_resolution import resolve_subject_with_multiple_ids

    conn = get_db_connection()
    try:
        # Convert Pydantic models to dicts
        identifiers = [
            {
                "local_subject_id": id.local_subject_id,
                "identifier_type": id.identifier_type,
            }
            for id in request.identifiers
        ]

        result = resolve_subject_with_multiple_ids(
            conn=conn,
            center_id=request.center_id,
            identifiers=identifiers,
            registration_year=request.registration_year,
            control=request.control,
            created_by=request.created_by,
        )

        return SubjectRegistrationResponse(
            gsid=result["gsid"],
            action=result["action"],
            identifiers_linked=result["identifiers_linked"],
            conflicts=result["conflicts"],
            conflict_resolution=result["conflict_resolution"],
            message=f"Successfully registered subject with {result['identifiers_linked']} identifier(s)",
        )

    except Exception as e:
        logger.error(f"Error registering subject: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/health")
async def health() -> HealthResponse:
    """Health check endpoint (public access)"""
    conn = None
    try:
        conn = get_db_connection()
        conn.cursor().execute("SELECT 1")
        return HealthResponse(status="healthy", database="connected")
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")
    finally:
        if conn:
            conn.close()


@router.get("/subjects/{gsid}")
async def get_subject(gsid: str, _: str = Depends(verify_api_key)):
    """Get subject details by GSID"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # Get subject
        cur.execute(
            """
            SELECT s.*, c.name as center_name
            FROM subjects s
            JOIN centers c ON s.center_id = c.center_id
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
            SELECT center_id, local_subject_id, identifier_type, created_by, created_at
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
            "center_name": subject[7],
            "registration_year": subject[2].isoformat() if subject[2] else None,
            "control": subject[3],
            "withdrawn": subject[4],
            "flagged_for_review": subject[6],
            "review_notes": subject[7],
            "created_by": subject[8],
            "created_at": subject[9].isoformat(),
            "local_ids": [
                {
                    "center_id": lid[0],
                    "local_subject_id": lid[1],
                    "identifier_type": lid[2],
                    "created_by": lid[3],
                    "created_at": lid[4].isoformat(),
                }
                for lid in local_ids
            ],
        }

    finally:
        conn.close()


@router.post("/subjects/{gsid}/withdraw", dependencies=[Depends(verify_api_key)])
async def withdraw_subject(gsid: str, reason: str = None):
    """Withdraw a subject"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE subjects 
            SET withdrawn = TRUE,
                review_notes = COALESCE(review_notes || E'\n', '') || 
                              'Withdrawn on ' || CURRENT_TIMESTAMP::TEXT ||
                              CASE WHEN %s IS NOT NULL THEN '. Reason: ' || %s ELSE '' END
            WHERE global_subject_id = %s
            RETURNING global_subject_id
            """,
            (reason, reason, gsid),
        )

        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="Subject not found")

        conn.commit()
        return {"status": "withdrawn", "gsid": gsid}

    finally:
        conn.close()
