# fragment-validator/services/schema_validator.py
import logging
from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
from core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    row_count: int


class SchemaValidator:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.schemas = settings.load_table_schemas()
        self.table_config = settings.load_table_mapping(table_name)

        if table_name not in self.schemas:
            raise ValueError(f"No schema defined for table: {table_name}")

        self.schema = self.schemas[table_name]

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationResult:
        """Validate DataFrame against schema"""
        errors = []
        warnings = []

        # Check required columns
        required_cols = [
            col["name"] for col in self.schema["columns"] if col.get("required", False)
        ]
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")

        # Validate data types
        for col_def in self.schema["columns"]:
            col_name = col_def["name"]
            if col_name not in df.columns:
                continue

            expected_type = col_def["type"]
            self._validate_column_type(df, col_name, expected_type, errors, warnings)
