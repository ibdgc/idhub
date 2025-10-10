# gsid-service/main.py

import logging
import os
import secrets
import time
from typing import List

import psycopg2
from fastapi import Depends, FastAPI, Header, HTTPException
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, field_validator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

BASE32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


# API Key Authentication
async def verify_api_key(x_api_key: str = Header(..., alias="x-api-key")):
    """Verify API key from request header"""
    valid_key = os.getenv("GSID_API_KEY")
    if not valid_key:
        logger.error("GSID_API_KEY not configured in environment")
        raise HTTPException(status_code=500, detail="API key not configured")
    if x_api_key != valid_key:
        logger.warning(f"Invalid API key attempt from client")
        raise HTTPException(status_code=403, detail="Invalid API key")
    return x_api_key


def generate_gsid() -> str:
    """Generate 12-character GSID"""
    timestamp_ms = int(time.time() * 1000)
    timestamp_b32 = ""
    for _ in range(6):
        timestamp_b32 = BASE32_ALPHABET[timestamp_ms % 32] + timestamp_b32
        timestamp_ms //= 32

    random_b32 = "".join(secrets.choice(BASE32_ALPHABET) for _ in range(6))

    return timestamp_b32 + random_b32


# MODELS - Define these BEFORE endpoints
class SubjectRequest(BaseModel):
    center_id: int
    local_subject_id: str
    identifier_type: str = "primary"
    registration_year: int | None = None
    control: bool = False
    created_by: str = "system"

    @field_validator("registration_year")
    @classmethod
    def validate_year(cls, v):
        if v is None:
            return None
        if isinstance(v, int):
            if 1900 <= v <= 2100:
                return v
            return None
        if "-" in str(v):
            v = str(v).split("-")[0]
        if len(str(v)) == 4 and str(v).isdigit():
            year = int(v)
            if 1900 <= year <= 2100:
                return year
        return None


class ResolutionResponse(BaseModel):
    gsid: str
    action: str
    match_strategy: str
    confidence: float
    requires_review: bool
    review_reason: str | None = None


class BatchSubjectRequest(BaseModel):
    requests: List[SubjectRequest]


# DATABASE FUNCTIONS
def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", 5432),
        cursor_factory=RealDictCursor,
    )


def resolve_identity(
    conn, center_id: int, local_subject_id: str, identifier_type: str = "primary"
) -> dict:
    """Core identity resolution logic with identifier_type support"""
    cur = conn.cursor()

    cur.execute(
        """
        SELECT s.global_subject_id, s.withdrawn 
        FROM local_subject_ids l
        JOIN subjects s ON l.global_subject_id = s.global_subject_id
        WHERE l.center_id = %s AND l.local_subject_id = %s AND l.identifier_type = %s
        """,
        (center_id, local_subject_id, identifier_type),
    )
    exact = cur.fetchone()

    if exact:
        if exact["withdrawn"]:
            return {
                "action": "review_required",
                "gsid": exact["global_subject_id"],
                "match_strategy": "exact_withdrawn",
                "confidence": 1.0,
                "review_reason": "Subject previously withdrawn",
            }
        return {
            "action": "link_existing",
            "gsid": exact["global_subject_id"],
            "match_strategy": "exact",
            "confidence": 1.0,
            "review_reason": None,
        }

    cur.execute(
        """
        SELECT s.global_subject_id, s.withdrawn
        FROM subject_alias a
        JOIN subjects s ON a.global_subject_id = s.global_subject_id
        WHERE a.alias = %s
        """,
        (local_subject_id,),
    )
    alias = cur.fetchone()

    if alias:
        if alias["withdrawn"]:
            return {
                "action": "review_required",
                "gsid": alias["global_subject_id"],
                "match_strategy": "alias_withdrawn",
                "confidence": 1.0,
                "review_reason": "Alias matches withdrawn subject",
            }
        return {
            "action": "link_existing",
            "gsid": alias["global_subject_id"],
            "match_strategy": "alias",
            "confidence": 0.95,
            "review_reason": None,
        }

    return {
        "action": "create_new",
        "gsid": None,
        "match_strategy": "no_match",
        "confidence": 1.0,
        "review_reason": None,
    }


def log_resolution(conn, resolution: dict, request: SubjectRequest):
    """Log identity resolution for audit trail"""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO identity_resolutions 
        (input_center_id, input_local_id, matched_gsid, action, 
         match_strategy, confidence_score, requires_review, review_reason, created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING resolution_id
        """,
        (
            request.center_id,
            request.local_subject_id,
            resolution["gsid"],
            resolution["action"],
            resolution["match_strategy"],
            resolution["confidence"],
            resolution["action"] == "review_required",
            resolution.get("review_reason"),
            request.created_by,
        ),
    )
    return cur.fetchone()["resolution_id"]


# ENDPOINTS
@app.post("/register", response_model=ResolutionResponse)
async def register_subject(
    request: SubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register or link a subject with identity resolution (requires API key)"""
    conn = get_db()
    try:
        resolution = resolve_identity(
            conn, request.center_id, request.local_subject_id, request.identifier_type
        )

        if resolution["action"] == "create_new":
            gsid = generate_gsid()

            cur = conn.cursor()
            cur.execute("SELECT 1 FROM subjects WHERE global_subject_id = %s", (gsid,))
            if cur.fetchone():
                gsid = generate_gsid()

            cur.execute(
                """
                INSERT INTO subjects (global_subject_id, center_id, registration_year, control)
                VALUES (%s, %s, %s, %s)
                """,
                (gsid, request.center_id, request.registration_year, request.control),
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


@app.post("/register/batch", response_model=List[ResolutionResponse])
async def register_subjects_batch(
    batch: BatchSubjectRequest, api_key: str = Depends(verify_api_key)
):
    """Register multiple subjects in a single request (requires API key)"""
    conn = get_db()
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


@app.get("/review-queue")
async def get_review_queue():
    """Get subjects flagged for manual review"""
    conn = get_db()
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


@app.post("/resolve-review/{gsid}")
async def resolve_review(gsid: str, reviewed_by: str, notes: str = None):
    """Mark a review as resolved"""
    conn = get_db()
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


@app.get("/health")
async def health():
    """Health check endpoint (public access)"""
    try:
        conn = get_db()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except:
        return {"status": "unhealthy", "database": "disconnected"}
