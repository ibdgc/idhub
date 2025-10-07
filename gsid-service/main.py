# gsid-service/main.py

import os
import secrets
import string
import time
from datetime import date

import psycopg2
from fastapi import FastAPI, HTTPException
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, field_validator

app = FastAPI()

# Base32 alphabet (Crockford's Base32 - no ambiguous characters)
BASE32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_gsid() -> str:
    """
    Generate 12-character GSID: TTTTTTRRRRRR
    - 6 chars: timestamp (milliseconds since epoch, base32)
    - 6 chars: random (base32)
    """
    # Timestamp portion (6 chars)
    timestamp_ms = int(time.time() * 1000)
    timestamp_b32 = ""
    for _ in range(6):
        timestamp_b32 = BASE32_ALPHABET[timestamp_ms % 32] + timestamp_b32
        timestamp_ms //= 32

    # Random portion (6 chars)
    random_b32 = "".join(secrets.choice(BASE32_ALPHABET) for _ in range(6))

    return timestamp_b32 + random_b32


class SubjectRequest(BaseModel):
    center_id: int
    local_subject_id: str
    registration_year: str | None = None
    control: bool = False
    created_by: str = "system"

    @field_validator('registration_year')
    @classmethod
    def validate_year(cls, v):
        if v is None:
            return None
        # Accept YYYY format or convert YYYY-MM-DD to YYYY
        if '-' in v:
            v = v.split('-')[0]
        # Validate it's a 4-digit year
        if len(v) == 4 and v.isdigit():
            year = int(v)
            if 1900 <= year <= 2100:
                return v  # Return just the year string, not a date
        return None


class ResolutionResponse(BaseModel):
    gsid: str  # Changed from int
    action: str
    match_strategy: str
    confidence: float
    requires_review: bool
    review_reason: str | None = None


def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", 5432),
        cursor_factory=RealDictCursor,
    )


def resolve_identity(conn, center_id: int, local_subject_id: str) -> dict:
    """Core identity resolution logic"""
    cur = conn.cursor()

    # Check exact match
    cur.execute(
        """
        SELECT s.global_subject_id, s.withdrawn 
        FROM local_subject_ids l
        JOIN subjects s ON l.global_subject_id = s.global_subject_id
        WHERE l.center_id = %s AND l.local_subject_id = %s
    """,
        (center_id, local_subject_id),
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

    # Check alias match
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

    # No match found
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


@app.post("/register", response_model=ResolutionResponse)
async def register_subject(request: SubjectRequest):
    """Register or link a subject with identity resolution"""
    conn = get_db()
    try:
        # Resolve identity
        resolution = resolve_identity(conn, request.center_id, request.local_subject_id)

        # Handle based on action
        if resolution["action"] == "create_new":
            # Generate new GSID
            gsid = generate_gsid()

            # Ensure uniqueness (extremely rare collision check)
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM subjects WHERE global_subject_id = %s", (gsid,))
            if cur.fetchone():
                # Collision detected, regenerate
                gsid = generate_gsid()

            # Create subject
            cur.execute(
                """
                INSERT INTO subjects (global_subject_id, center_id, registration_year, control)
                VALUES (%s, %s, %s, %s)
            """,
                (gsid, request.center_id, request.registration_year, request.control),
            )

            # Create local_subject_id entry
            cur.execute(
                """
                INSERT INTO local_subject_ids (center_id, local_subject_id, global_subject_id)
                VALUES (%s, %s, %s)
            """,
                (request.center_id, request.local_subject_id, gsid),
            )

            resolution["gsid"] = gsid

        elif resolution["action"] == "link_existing":
            # Add new local_subject_id to existing GSID
            gsid = resolution["gsid"]
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO local_subject_ids (center_id, local_subject_id, global_subject_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (center_id, local_subject_id) DO NOTHING
            """,
                (request.center_id, request.local_subject_id, gsid),
            )

        elif resolution["action"] == "review_required":
            # Flag for manual review
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

        # Log resolution
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
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/review-queue")
async def get_review_queue():
    """Get subjects flagged for manual review"""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
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
        """)
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
    try:
        conn = get_db()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except:
        return {"status": "unhealthy", "database": "disconnected"}
