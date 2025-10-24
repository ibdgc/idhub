# gsid-service/services/identity_resolution.py
import logging
import secrets
import time
from typing import Any, Dict

from core.database import get_db_cursor
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

BASE32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_gsid() -> str:
    """Generate 12-character GSID"""
    timestamp_ms = int(time.time() * 1000)
    timestamp_b32 = ""
    for _ in range(6):
        timestamp_b32 = BASE32_ALPHABET[timestamp_ms % 32] + timestamp_b32
        timestamp_ms //= 32

    random_b32 = "".join(secrets.choice(BASE32_ALPHABET) for _ in range(6))
    return timestamp_b32 + random_b32


def resolve_identity(
    conn, center_id: int, local_subject_id: str, identifier_type: str = "primary"
) -> dict:
    """Core identity resolution logic with identifier_type support"""
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
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
    finally:
        cur.close()


def log_resolution(conn, resolution: Dict[str, Any], request: Any):
    """
    Log identity resolution result

    Args:
        conn: Database connection
        resolution: Resolution result dictionary
        request: SubjectRequest Pydantic model or dict
    """
    with get_db_cursor(conn, cursor_factory=RealDictCursor) as cur:
        # Handle both Pydantic models and dicts
        center_id = (
            request.center_id
            if hasattr(request, "center_id")
            else request.get("center_id")
        )
        local_id = (
            request.local_subject_id
            if hasattr(request, "local_subject_id")
            else request.get("local_subject_id")
        )

        # Determine if review is required
        requires_review = resolution.get("action") == "review_required"

        cur.execute(
            """
            INSERT INTO identity_resolutions 
            (input_center_id, input_local_id, matched_gsid, action, match_strategy, 
             confidence_score, requires_review, review_reason, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING resolution_id
            """,
            (
                center_id,
                local_id,
                resolution.get("gsid"),
                resolution.get("action"),
                resolution.get("match_strategy"),
                resolution.get("confidence"),
                requires_review,
                resolution.get("review_reason"),
            ),
        )
        result = cur.fetchone()
        return result["resolution_id"]
