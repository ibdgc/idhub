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

        Auto-includes subject_id_candidates and center_id_field for resolution,
        even if not in field_mapping (they'll be excluded later via exclude_from_load).

        Args:
            raw_data: Source DataFrame
            field_mapping: Dict mapping target_field -> source_field
            subject_id_candidates: List of subject ID fields (auto-included)
            center_id_field: Center ID field name (auto-included if present)

        Returns:
            Mapped DataFrame with explicitly mapped fields + resolution fields
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

        # Auto-include subject ID candidate fields (needed for resolution)
        for candidate_field in subject_id_candidates:
            if candidate_field in raw_data.columns:
                # Only add if not already mapped
                if candidate_field not in mapped_data.columns:
                    mapped_data[candidate_field] = raw_data[candidate_field]
                    logger.debug(
                        f"Auto-included subject ID candidate: {candidate_field}"
                    )
            else:
                logger.warning(
                    f"Subject ID candidate '{candidate_field}' not found in source data"
                )

        # Auto-include center_id field if specified
        if center_id_field and center_id_field in raw_data.columns:
            if center_id_field not in mapped_data.columns:
                mapped_data[center_id_field] = raw_data[center_id_field]
                logger.debug(f"Auto-included center_id field: {center_id_field}")

        logger.info(f"Mapped {len(field_mapping)} fields from source data")
        logger.info(
            f"Total columns in mapped data: {len(mapped_data.columns)} "
            f"(includes {len(subject_id_candidates)} subject ID candidates)"
        )

        return mapped_data
