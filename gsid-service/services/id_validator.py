# gsid-service/services/id_validator.py
import logging
import re
from typing import Dict, List

logger = logging.getLogger(__name__)


class IDValidator:
    """Validates subject IDs and provides warnings for suspicious patterns"""

    # Error patterns that should fail validation
    ERROR_PATTERNS = [
        r"^test",  # Test IDs
        r"^demo",  # Demo IDs
        r"^example",  # Example IDs
        r"^0+$",  # All zeros
        r"^9+$",  # All nines
        r"^x+$",  # All x's
    ]

    # Warning patterns that should flag for review
    WARNING_PATTERNS = [
        r"^\d{1,3}$",  # Very short numeric IDs (1-3 digits)
        r"^[a-z]{1,2}$",  # Very short alpha IDs
        r"\s",  # Contains whitespace
        r"[^a-zA-Z0-9_-]",  # Special characters (except underscore/hyphen)
    ]

    # Identifier types that allow numeric-only IDs
    NUMERIC_ALLOWED_TYPES = {
        "niddk_no",
        "sample_id",
        "record_id",
    }

    @classmethod
    def validate_id(
        cls, local_id: str, identifier_type: str = "primary"
    ) -> Dict[str, any]:
        """
        Validate a single ID

        Returns:
            {
                "valid": bool,
                "severity": "info" | "warning" | "error",
                "warnings": List[str]
            }
        """
        warnings = []
        severity = "info"

        if not local_id or not local_id.strip():
            return {
                "valid": False,
                "severity": "error",
                "warnings": ["ID is empty or whitespace"],
            }

        local_id_stripped = local_id.strip()

        # Check error patterns
        for pattern in cls.ERROR_PATTERNS:
            if re.search(pattern, local_id_stripped, re.IGNORECASE):
                return {
                    "valid": False,
                    "severity": "error",
                    "warnings": [f"ID matches error pattern: {pattern}"],
                }

        # Check warning patterns
        for pattern in cls.WARNING_PATTERNS:
            if re.search(pattern, local_id_stripped):
                if pattern == r"^\d{1,3}$":
                    warnings.append(
                        f"ID is very short ({len(local_id_stripped)} digits)"
                    )
                    severity = "warning"
                elif pattern == r"^[a-z]{1,2}$":
                    warnings.append(
                        f"ID is very short ({len(local_id_stripped)} characters)"
                    )
                    severity = "warning"
                elif pattern == r"\s":
                    warnings.append("ID contains whitespace")
                    severity = "warning"
                elif pattern == r"[^a-zA-Z0-9_-]":
                    warnings.append("ID contains special characters")
                    severity = "warning"

        # Check if purely numeric (unless allowed for this type)
        if (
            local_id_stripped.isdigit()
            and identifier_type not in cls.NUMERIC_ALLOWED_TYPES
        ):
            warnings.append(
                f"ID is purely numeric for type '{identifier_type}' (may be ambiguous)"
            )
            severity = "warning"

        # Check length
        if len(local_id_stripped) < 3:
            warnings.append(f"ID is very short ({len(local_id_stripped)} characters)")
            severity = "warning"

        return {
            "valid": True,
            "severity": severity,
            "warnings": warnings,
        }

    @classmethod
    def validate_batch(cls, ids: List[Dict[str, str]]) -> Dict[str, Dict]:
        """
        Validate a batch of IDs

        Args:
            ids: List of dicts with 'id' and 'type' keys

        Returns:
            Dict mapping ID to validation result
        """
        results = {}
        for item in ids:
            local_id = item.get("id")
            id_type = item.get("type", "primary")
            results[local_id] = cls.validate_id(local_id, id_type)

        return results

    @classmethod
    def validate_candidate_ids(cls, candidate_ids: List[Dict]) -> List[str]:
        """
        Validate all candidate IDs and return list of warnings

        Args:
            candidate_ids: List of dicts with 'local_subject_id' and 'identifier_type'

        Returns:
            List of warning messages
        """
        all_warnings = []

        for candidate in candidate_ids:
            local_id = candidate.get("local_subject_id")
            id_type = candidate.get("identifier_type", "primary")

            result = cls.validate_id(local_id, id_type)

            if not result["valid"]:
                all_warnings.append(
                    f"{id_type}='{local_id}': {', '.join(result['warnings'])}"
                )
            elif result["severity"] in ["warning", "error"]:
                all_warnings.append(
                    f"{id_type}='{local_id}': {', '.join(result['warnings'])}"
                )

        return all_warnings
