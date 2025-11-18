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
        Apply field mapping from source to target fields.

        Only maps explicitly defined fields in field_mapping.
        Resolution fields (subject_id_candidates, center_id) should be
        handled separately and excluded via exclude_from_load config.

        Args:
            raw_data: Source DataFrame
            field_mapping: Dict mapping target_field -> source_field
            subject_id_candidates: List of subject ID fields (for logging only)
            center_id_field: Center ID field name (for logging only)

        Returns:
            Mapped DataFrame with only explicitly mapped fields
        """
        mapped_data = pd.DataFrame()

        # Map explicitly defined fields
        for target_field, source_field in field_mapping.items():
            if source_field in raw_data.columns:
                mapped_data[target_field] = raw_data[source_field]
                logger.debug(f"Mapped: {source_field} -> {target_field}")
            else:
                logger.warning(
                    f"Source field '{source_field}' not found in data "
                    f"(target: {target_field})"
                )

        logger.info(f"Mapped {len(field_mapping)} fields from source data")

        return mapped_data
