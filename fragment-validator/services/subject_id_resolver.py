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
        Resolve subject IDs for entire dataset

        Returns dict with:
            - gsids: List of resolved GSIDs
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
            "unknown_center_used": 0,
            "center_promoted": 0,
        }

        # Prepare batch requests
        batch_requests = []
        row_indices = []

        for idx, row in data.iterrows():
            # Handle center_id - default to 0 (Unknown) if not provided
            if (
                center_id_field
                and center_id_field in row
                and pd.notna(row[center_id_field])
            ):
                center_id = int(row[center_id_field])
            else:
                center_id = default_center_id
                stats["unknown_center_used"] += 1

            # Get first valid candidate field
            local_id = None
            used_field = None
            for field in candidate_fields:
                if field in row and pd.notna(row[field]):
                    local_id = str(row[field])
                    used_field = field
                    break

            if not local_id:
                raise ValueError(
                    f"Row {idx}: No valid subject ID found in candidate fields: {candidate_fields}"
                )

            batch_requests.append(
                {
                    "center_id": center_id,
                    "local_subject_id": local_id,
                    "identifier_type": used_field,
                    "created_by": created_by,
                }
            )
            row_indices.append((idx, row, used_field, center_id))

        # Process via GSID service
        results = self.gsid_client.register_batch(batch_requests)

        # Process results
        for i, result in enumerate(results):
            idx, row, used_field, center_id = row_indices[i]
            found_gsid = result["gsid"]

            if result["action"] == "create_new":
                stats["new_gsids_minted"] += 1
            else:
                stats["existing_matches"] += 1

            gsids.append(found_gsid)

            # Record ALL local IDs for this subject (not just the one used for lookup)
            for alt_field in candidate_fields:
                if alt_field in row and pd.notna(row[alt_field]):
                    local_id_records.append(
                        {
                            "center_id": center_id,
                            "local_subject_id": str(row[alt_field]),
                            "identifier_type": alt_field,
                            "global_subject_id": found_gsid,
                            "action": result["action"],
                        }
                    )

        # Generate warnings
        if stats["unknown_center_used"] > 0:
            warnings.append(
                f"{stats['unknown_center_used']} records used center_id={default_center_id} (Unknown)"
            )
        if stats["center_promoted"] > 0:
            warnings.append(
                f"{stats['center_promoted']} records promoted from Unknown to known center"
            )

        return {
            "gsids": gsids,
            "local_id_records": local_id_records,
            "summary": stats,
            "warnings": warnings,
        }
