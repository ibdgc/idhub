# fragment-validator/services/schema_validator.py
import logging
from dataclasses import dataclass
from typing import List, Set

import pandas as pd

from .nocodb_client import NocoDBClient

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of schema validation"""

    is_valid: bool
    errors: List[dict]
    warnings: List[str]


class SchemaValidator:
    """Validates data against NocoDB table schema"""

    # System/auto-generated columns to skip
    SKIP_COLUMNS: Set[str] = {
        "created_at",
        "updated_at",
        "global_subject_id",  # Resolved during processing
        "Id",
    }

    def __init__(self, nocodb_client: NocoDBClient):
        self.nocodb_client = nocodb_client

    def validate(self, data: pd.DataFrame, table_name: str) -> ValidationResult:
        """Validate data against target table schema"""
        errors = []
        warnings = []

        try:
            table_meta = self.nocodb_client.get_table_metadata(table_name)
            columns = table_meta.get("columns", [])

            if not columns:
                warning_msg = f"No columns found for table {table_name}, skipping schema validation"
                logger.warning(warning_msg)
                warnings.append(warning_msg)
                return ValidationResult(is_valid=True, errors=[], warnings=warnings)

            # Check required columns exist
            for col in columns:
                col_name = col.get("column_name")

                if not col_name or col_name in self.SKIP_COLUMNS:
                    continue

                # Skip primary keys and auto-increment
                if col.get("pk", False) or col.get("ai", False):
                    continue

                is_required = col.get("rqd", False)

                # Check missing required column
                if col_name not in data.columns and is_required:
                    errors.append(
                        {
                            "type": "missing_required_column",
                            "column": col_name,
                            "message": f"Required column '{col_name}' not found in data",
                        }
                    )

            # Check for null values in NOT NULL columns
            for col in columns:
                col_name = col.get("column_name")

                if not col_name or col_name in self.SKIP_COLUMNS:
                    continue

                if col.get("pk", False) or col.get("ai", False):
                    continue

                is_required = col.get("rqd", False)

                if col_name in data.columns and is_required:
                    null_count = data[col_name].isna().sum()
                    if null_count > 0:
                        errors.append(
                            {
                                "type": "null_in_required_column",
                                "column": col_name,
                                "null_count": int(null_count),
                                "message": f"Column '{col_name}' has {null_count} null values but is NOT NULL",
                            }
                        )

        except Exception as e:
            logger.error(f"Schema validation error: {e}")
            errors.append({"type": "schema_validation_error", "message": str(e)})

        is_valid = len(errors) == 0
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)
