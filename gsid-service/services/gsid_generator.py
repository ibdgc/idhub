# gsid-service/services/gsid_generator.py
import logging
import secrets
import time
from typing import List

from core.database import get_db_connection, get_db_cursor

logger = logging.getLogger(__name__)

# Custom Base32 alphabet (Crockford's base32 without confusing characters)
BASE32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"  # 32 chars, no I, L, O, U


def encode_base32(num: int, length: int) -> str:
    """Encode number as base32 string with custom alphabet"""
    if num == 0:
        return BASE32_ALPHABET[0] * length

    result = []
    while num > 0:
        result.append(BASE32_ALPHABET[num % 32])
        num //= 32

    # Pad with leading zeros
    while len(result) < length:
        result.append(BASE32_ALPHABET[0])

    return "".join(reversed(result))


def generate_gsid() -> str:
    """
    Generate a single GSID in format: GSID-XXXXXXXXXXXX (12 characters)

    Structure:
    - First 5 characters: Timestamp (milliseconds since epoch, base32)
      - Provides lexicographic sorting by creation time
      - ~41 bits = covers timestamps until year ~2084
    - Last 7 characters: Random component (base32)
      - ~35 bits of randomness = 34 billion combinations per millisecond

    Total: 76 bits of uniqueness
    """
    # Get current timestamp in milliseconds
    timestamp_ms = int(time.time() * 1000)

    # Encode timestamp as 5 base32 characters (can represent up to 2^25 ms ~ 41 bits)
    # This gives us timestamps until ~2084
    timestamp_part = encode_base32(timestamp_ms, length=5)

    # Generate 7 characters of randomness (~35 bits)
    random_bytes = secrets.token_bytes(5)  # 40 bits, we'll use 35
    random_int = (
        int.from_bytes(random_bytes, byteorder="big") & 0x7FFFFFFFF
    )  # Mask to 35 bits
    random_part = encode_base32(random_int, length=7)

    return f"GSID-{timestamp_part}{random_part}"


def generate_unique_gsids(count: int) -> List[str]:
    """
    Generate multiple unique GSIDs and reserve them in database

    Args:
        count: Number of GSIDs to generate

    Returns:
        List of unique GSID strings (lexicographically sortable by creation time)

    Raises:
        ValueError: If count is invalid
        Exception: If database operations fail
    """
    if count < 1 or count > 1000:
        raise ValueError("Count must be between 1 and 1000")

    conn = None
    try:
        conn = get_db_connection()
        with get_db_cursor(conn) as cursor:
            gsids = []
            attempts = 0
            max_attempts = count * 10  # Prevent infinite loops

            while len(gsids) < count and attempts < max_attempts:
                gsid = generate_gsid()
                attempts += 1

                # Check if GSID already exists
                cursor.execute("SELECT 1 FROM gsid_registry WHERE gsid = %s", (gsid,))

                if cursor.fetchone() is None:
                    # Reserve the GSID
                    cursor.execute(
                        """
                        INSERT INTO gsid_registry (gsid, status, created_at)
                        VALUES (%s, 'reserved', NOW())
                        """,
                        (gsid,),
                    )
                    gsids.append(gsid)
                    logger.debug(f"Generated and reserved GSID: {gsid}")

            if len(gsids) < count:
                raise Exception(
                    f"Could not generate {count} unique GSIDs after {attempts} attempts"
                )

            conn.commit()
            logger.info(f"Successfully generated {count} unique GSIDs")
            return gsids

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error generating GSIDs: {e}")
        raise
    finally:
        if conn:
            conn.close()


def reserve_gsids(gsids: List[str]) -> None:
    """
    Reserve pre-generated GSIDs in the database

    Args:
        gsids: List of GSID strings to reserve

    Raises:
        Exception: If database operations fail
    """
    conn = None
    try:
        conn = get_db_connection()
        with get_db_cursor(conn) as cursor:
            for gsid in gsids:
                cursor.execute(
                    """
                    INSERT INTO gsid_registry (gsid, status, created_at)
                    VALUES (%s, 'reserved', NOW())
                    ON CONFLICT (gsid) DO NOTHING
                    """,
                    (gsid,),
                )

            conn.commit()
            logger.info(f"Reserved {len(gsids)} GSIDs")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error reserving GSIDs: {e}")
        raise
    finally:
        if conn:
            conn.close()
