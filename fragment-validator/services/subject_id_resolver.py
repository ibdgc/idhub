# fragment-validator/services/subject_id_resolver.py
import logging
from typing import Dict, List, Optional

import pandas as pd

from .gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class SubjectIDResolver:
    """Resolves subject IDs using GSID service"""

    def __init__(self, gsid_client: GSIDClient):
        self.gsid_client = gsid_client

    def resolve_batch(
        self,
        data: pd.DataFrame,
        candidate_fields: List[str],
        center_id_field: Optional[str] = None,
        default_center_id: int = 0,
        created_by: str = "fragment_validator",
    ) -> Dict:
        """
        Resolve subject IDs for entire dataset.

        Now uses the unified /register/subject endpoint which handles
        multiple identifiers per subject correctly.

        Returns dict with:
            - gsids: List of resolved GSIDs (one per row)
            - local_id_records: List of local ID records to insert
            - summary: Statistics
            - warnings: List of warnings
        """
        gsids = []
        local_id_records = []
        warnings = []
        stats = {
            "existing_matches": 0,
            "new_gsids_minted": 0,
            "conflicts_detected": 0,
        }

        for idx, row in data.iterrows():
            # Determine center_id for this row
            if (
                center_id_field
                and center_id_field in row
                and pd.notna(row[center_id_field])
            ):
                center_id = int(row[center_id_field])
            else:
                center_id = default_center_id

            # Collect all non-null candidate IDs for this row
            identifiers = []
            for field in candidate_fields:
                if field in row and pd.notna(row[field]) and str(row[field]).strip():
                    identifiers.append(
                        {
                            "local_subject_id": str(row[field]).strip(),
                            "identifier_type": field,
                        }
                    )

            if not identifiers:
                warnings.append(f"Row {idx}: No valid subject IDs found")
                gsids.append(None)
                continue

            try:
                # Call unified endpoint with all identifiers for this subject
                result = self.gsid_client.register_subject(
                    center_id=center_id, identifiers=identifiers, created_by=created_by
                )

                gsid = result["gsid"]
                gsids.append(gsid)

                # Track statistics
                if result["action"] == "create_new":
                    stats["new_gsids_minted"] += 1
                elif result["action"] == "link_existing":
                    stats["existing_matches"] += 1
                elif result["action"] == "conflict_resolved":
                    stats["conflicts_detected"] += 1
                    warnings.append(
                        f"Row {idx}: GSID conflict - {result['conflicts']} "
                        f"(using {gsid})"
                    )

                # Collect local_id_records for database insertion
                for identifier in identifiers:
                    local_id_records.append(
                        {
                            "center_id": center_id,
                            "local_subject_id": identifier["local_subject_id"],
                            "identifier_type": identifier["identifier_type"],
                            "global_subject_id": gsid,
                        }
                    )

            except Exception as e:
                logger.error(f"Row {idx}: Failed to resolve subject IDs: {e}")
                warnings.append(f"Row {idx}: Resolution failed - {str(e)}")
                gsids.append(None)

        return {
            "gsids": gsids,
            "local_id_records": local_id_records,
            "summary": stats,
            "warnings": warnings,
        }
