# gsid-service/services/gsid_generator.py
import logging
import secrets
import time
from typing import List

from core.database import get_db_connection, get_db_cursor

logger = logging.getLogger(__name__)

BASE32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_gsid() -> str:
    """Generate 12-character GSID (6 chars timestamp + 6 chars random)"""
    timestamp_ms = int(time.time() * 1000)
    timestamp_b32 = ""
    for _ in range(6):
        timestamp_b32 = BASE32_ALPHABET[timestamp_ms % 32] + timestamp_b32
        timestamp_ms //= 32

    random_b32 = "".join(secrets.choice(BASE32_ALPHABET) for _ in range(6))
    return timestamp_b32 + random_b32


def generate_unique_gsids(count: int) -> List[str]:
    """Generate multiple unique GSIDs"""
    gsids = []
    max_attempts = count * 10
    attempts = 0

    with get_db_connection() as conn:
        with get_db_cursor(conn) as cursor:
            while len(gsids) < count and attempts < max_attempts:
                gsid = generate_gsid()
                cursor.execute(
                    "SELECT 1 FROM subjects WHERE global_subject_id = %s", (gsid,)
                )
                if not cursor.fetchone():
                    gsids.append(gsid)
                attempts += 1

    if len(gsids) < count:
        raise Exception(
            f"Could not generate {count} unique GSIDs after {max_attempts} attempts"
        )

    return gsids


def reserve_gsids(gsids: List[str]) -> None:
    """Reserve GSIDs in the database by creating placeholder subject records"""
    with get_db_connection() as conn:
        with get_db_cursor(conn) as cursor:
            for gsid in gsids:
                # Insert placeholder - center_id=0 means "reserved but not yet assigned"
                cursor.execute(
                    """
                    INSERT INTO subjects (global_subject_id, center_id, created_at)
                    VALUES (%s, 0, NOW())
                    ON CONFLICT (global_subject_id) DO NOTHING
                    """,
                    (gsid,),
                )
