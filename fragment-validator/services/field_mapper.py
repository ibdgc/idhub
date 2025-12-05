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
        subject_id_candidates: (List[str] | Dict[str, str]),
        center_id_field: Optional[str] = None,
        static_fields: Optional[Dict[str, any]] = None,
    ) -> pd.DataFrame:
        """
        Apply field mapping from source to target fields.

        Args:
            raw_data: Source DataFrame
            field_mapping: Dict mapping target_field -> source_field
            subject_id_candidates: List or Dict of subject ID fields
            center_id_field: Center ID field name
            static_fields: Dict of target_field -> static_value assignments

        Returns:
            Mapped DataFrame with all transformations applied.
        """
        mapped_data = pd.DataFrame()

        # 1. Map explicitly defined fields from source columns
        for target_field, source_field in field_mapping.items():
            if source_field in raw_data.columns:
                mapped_data[target_field] = raw_data[source_field]
                logger.debug(f"Mapped column: {source_field} -> {target_field}")
            else:
                logger.warning(
                    f"Source field '{source_field}' for target '{target_field}' not found in data"
                )

        # 2. Apply static field values
        if static_fields:
            for target_field, static_value in static_fields.items():
                mapped_data[target_field] = static_value
                logger.debug(f"Applied static value to '{target_field}'")

        # 3. Auto-include subject ID candidate fields for resolution
        candidate_field_names = []
        if isinstance(subject_id_candidates, dict):
            candidate_field_names = list(subject_id_candidates.keys())
        elif isinstance(subject_id_candidates, list):
            candidate_field_names = subject_id_candidates

        for candidate_field in candidate_field_names:
            if candidate_field in raw_data.columns:
                if candidate_field not in mapped_data.columns:
                    mapped_data[candidate_field] = raw_data[candidate_field]
                    logger.debug(f"Auto-included subject ID candidate: {candidate_field}")
            else:
                logger.warning(f"Subject ID candidate '{candidate_field}' not found in source data")

        # 4. Auto-include center_id field for resolution
        if center_id_field and center_id_field in raw_data.columns:
            if center_id_field not in mapped_data.columns:
                mapped_data[center_id_field] = raw_data[center_id_field]
                logger.debug(f"Auto-included center_id field: {center_id_field}")

        logger.info(
            f"Total columns in mapped data: {len(mapped_data.columns)}"
        )

        return mapped_data
