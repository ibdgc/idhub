# fragment-validator/services/field_mapper.py
import logging
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class FieldMapper:
    """Handles field mapping from source to target schema"""

    @staticmethod
    def apply_mapping(
        raw_data: pd.DataFrame,
        field_mapping: Dict[str, str],
        subject_id_candidates: List[str],
        center_id_field: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Apply field mapping with auto-inclusion of subject ID and center ID fields

        Args:
            raw_data: Source DataFrame
            field_mapping: Dict mapping target_field -> source_field
            subject_id_candidates: List of potential subject ID field names
            center_id_field: Optional center ID field name

        Returns:
            Mapped DataFrame
        """
        mapped_data = pd.DataFrame()

        # Map explicitly defined fields
        for target_field, source_field in field_mapping.items():
            if source_field in raw_data.columns:
                mapped_data[target_field] = raw_data[source_field]
            else:
                logger.warning(f"Source field '{source_field}' not found in input data")
                mapped_data[target_field] = None

        # Auto-include subject ID candidate fields if not already mapped
        for candidate in subject_id_candidates:
            if candidate not in mapped_data.columns and candidate in raw_data.columns:
                mapped_data[candidate] = raw_data[candidate]
                logger.info(f"Auto-included subject ID candidate field: {candidate}")

        # Auto-include center_id field if specified and not already mapped
        if (
            center_id_field
            and center_id_field not in mapped_data.columns
            and center_id_field in raw_data.columns
        ):
            mapped_data[center_id_field] = raw_data[center_id_field]
            logger.info(f"Auto-included center_id field: {center_id_field}")

        return mapped_data
