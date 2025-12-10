# GSID Format

## Structure

!!! abstract "GSID Structure"
    Global Subject IDs (GSIDs) in this system use a custom format designed to be unique, roughly sortable by time, and human-readable.

    The format is a 21-character string: `GSID-XXXXXXXXXXXXXXXX`

    ```
    GSID-TTTTTRRRRRRRRRRR
    │     │   │
    │     │   └─ Random component (11 characters)
    │     └───── Timestamp component (5 characters)
    └─────────── Static Prefix "GSID-"
    ```

    -   **Prefix**: Always `GSID-`.
    -   **Timestamp (`TTTTT`)**: A 5-character Base32 string representing the milliseconds since the Unix epoch (modulo 32^5). This allows GSIDs to be roughly sortable by creation time.
    -   **Randomness (`RRRRRRRRRRR`)**: An 11-character Base32 string generated from a cryptographically secure random number. This provides approximately 55 bits of randomness, ensuring a high degree of uniqueness for IDs generated within the same millisecond.

!!! info "Characteristics"
    -   **Length**: 21 characters total.
    -   **Character set**: A custom Base32 alphabet (`0123456789ABCDEFGHJKMNPQRSTVWXYZ`) that excludes potentially confusing characters like 'I', 'L', 'O', and 'U'.
    -   **Sortable**: Roughly sortable by creation time due to the timestamp component.
    -   **Unique**: Offers ~80 bits of uniqueness (25 from timestamp + 55 from random), making collisions extremely unlikely.

## Generation Algorithm

!!! example "GSID Generation Logic (Python)"
    The generation logic is custom and does not use the standard ULID library.

    ```python
    import logging
    import secrets
    import time

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
        Generate a single GSID in format: GSID-XXXXXXXXXXXXXXXX (21 characters total)
        """
        # Get current timestamp in milliseconds
        timestamp_ms = int(time.time() * 1000)

        # Encode timestamp as 5 base32 characters
        timestamp_part = encode_base32(timestamp_ms % (32**5), length=5)

        # Generate 11 characters of randomness
        max_random = 32**11
        random_int = secrets.randbelow(max_random)
        random_part = encode_base32(random_int, length=11)

        return f"GSID-{timestamp_part}{random_part}"
    ```

## Validation

!!! example "GSID Validation Logic (Python)"
    Validation must check for the prefix and the 16-character payload.

    ```python
    import re

    def validate_gsid(gsid: str) -> bool:
        """Validate the custom GSID format."""
        if not gsid.startswith("GSID-"):
            return False

        payload = gsid[5:]
        if len(payload) != 16:
            return False

        # Check character set
        pattern = r'^[0-9A-HJKMNP-TV-Z]{16}$'
        return bool(re.match(pattern, payload.upper()))

    # Example
    # validate_gsid("GSID-4A1B2C3D4E5F6G7H") -> True
    ```