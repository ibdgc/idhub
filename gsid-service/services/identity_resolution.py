# gsid-service/services/identity_resolution.py
import secrets
import time

from api.models import SubjectRequest

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