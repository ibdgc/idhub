# gsid-service/services/gsid_generator.py
import logging
import secrets
import time
from typing import List

from .database import get_db_connection, get_db_cursor

logger = logging.getLogger(__name__)

BASE32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_gsid() -> str:
    """Generate a ULID-based GSID"""
    timestamp = int(time.time() * 1000)
    randomness = secrets.randbits(80)

    ulid_int = (timestamp << 80) | randomness

    gsid = ""
    for _ in range(26):
        gsid = BASE32_ALPHABET[ulid_int % 32] + gsid
        ulid_int //= 32

    return gsid


def generate_unique_gsids(count: int) -> List[str]:
    """Generate multiple unique GSIDs"""
    gsids = []
    max_attempts = count * 10
    attempts = 0

    with get_db_connection() as conn:
        with get_db_cursor(conn) as cursor:
            while len(gsids) < count and attempts < max_attempts:
                gsid = generate_gsid()

                cursor.execute("SELECT 1 FROM subjects WHERE gsid = %s", (gsid,))
                if not cursor.fetchone():
                    gsids.append(gsid)

                attempts += 1

    if len(gsids) < count:
        raise Exception(
            f"Could not generate {count} unique GSIDs after {max_attempts} attempts"
        )

    return gsids


def reserve_gsids(gsids: List[str]) -> None:
    """Reserve GSIDs in the database"""
    with get_db_connection() as conn:
        with get_db_cursor(conn) as cursor:
            for gsid in gsids:
                cursor.execute(
                    """
                    INSERT INTO subjects (gsid, created_at)
                    VALUES (%s, NOW())
                    ON CONFLICT (gsid) DO NOTHING
                    """,
                    (gsid,),
                )
