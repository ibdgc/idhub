# fragment-validator/services/subject_id_resolver.py
import logging
from typing import Dict, List, Optional

import pandas as pd

from .gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class SubjectIDResolver:
    """Resolves subject IDs using GSID service with multi-candidate support"""

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
        Resolve subject IDs for entire dataset with multi-candidate support

        Returns dict with:
            - gsids: List of resolved GSIDs
            - local_id_records: List of local ID records to insert
            - summary: Statistics
            - warnings: List of warnings
            - flagged_records: List of records requiring review
        """
        gsids = []
        local_id_records = []
        warnings = []
        flagged_records = []
        stats = {
            "existing_matches": 0,
            "new_gsids_minted": 0,
            "unknown_center_used": 0,
            "center_promoted": 0,
            "flagged_for_review": 0,
            "validation_warnings": 0,
            "multi_gsid_conflicts": 0,
        }

        # Prepare batch requests with ALL candidate IDs per record
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

            # Collect ALL valid candidate IDs for this record
            candidate_ids = []
            for field in candidate_fields:
                if field in row and pd.notna(row[field]):
                    local_id = str(row[field]).strip()
                    if local_id:  # Only include non-empty IDs
                        candidate_ids.append(
                            {
                                "local_subject_id": local_id,
                                "identifier_type": field,
                            }
                        )

            if not candidate_ids:
                raise ValueError(
                    f"Row {idx}: No valid subject ID found in candidate fields: {candidate_fields}"
                )

            batch_requests.append(
                {
                    "center_id": center_id,
                    "candidate_ids": candidate_ids,
                    "created_by": created_by,
                }
            )
            row_indices.append((idx, row, candidate_ids, center_id))

        # Process via GSID service using multi-candidate endpoint
        logger.info(
            f"Sending {len(batch_requests)} records with multi-candidate IDs to GSID service"
        )
        results = self.gsid_client.register_batch_multi_candidate(batch_requests)

        # Process results
        for i, result in enumerate(results):
            idx, row, candidate_ids, center_id = row_indices[i]

            # Check for errors
            if result.get("action") == "error":
                error_msg = result.get("error", "Unknown error")
                warnings.append(f"Row {idx}: {error_msg}")
                gsids.append(None)
                continue

            found_gsid = result.get("gsid")
            action = result["action"]

            # Update statistics
            if action == "create_new":
                stats["new_gsids_minted"] += 1
            elif action == "link_existing":
                stats["existing_matches"] += 1
            elif action == "center_promoted":
                stats["center_promoted"] += 1
                stats["existing_matches"] += 1
            elif action == "review_required":
                stats["flagged_for_review"] += 1

                # Track flagged records
                flagged_records.append(
                    {
                        "row_index": idx,
                        "candidate_ids": [c["local_subject_id"] for c in candidate_ids],
                        "center_id": center_id,
                        "gsid": found_gsid,
                        "matched_gsids": result.get("matched_gsids"),
                        "reason": result.get("review_reason"),
                        "match_strategy": result.get("match_strategy"),
                        "confidence": result.get("confidence"),
                    }
                )

                # Check for multi-GSID conflicts
                if result.get("matched_gsids") and len(result["matched_gsids"]) > 1:
                    stats["multi_gsid_conflicts"] += 1

            # Track validation warnings
            if result.get("validation_warnings"):
                stats["validation_warnings"] += 1
                for warning in result["validation_warnings"]:
                    warnings.append(f"Row {idx}: {warning}")

            gsids.append(found_gsid)

            # Record ALL local IDs for this subject
            # The GSID service already inserted these, but we track them for the fragment
            for candidate in candidate_ids:
                local_id_records.append(
                    {
                        "center_id": center_id,
                        "local_subject_id": candidate["local_subject_id"],
                        "identifier_type": candidate["identifier_type"],
                        "global_subject_id": found_gsid,
                        "action": action,
                    }
                )

        # Generate summary warnings
        if stats["unknown_center_used"] > 0:
            warnings.append(
                f"{stats['unknown_center_used']} records used center_id={default_center_id} (Unknown)"
            )

        if stats["center_promoted"] > 0:
            warnings.append(
                f"{stats['center_promoted']} records promoted from Unknown to known center"
            )

        if stats["flagged_for_review"] > 0:
            warnings.append(
                f"⚠️  {stats['flagged_for_review']} records flagged for manual review"
            )

        if stats["multi_gsid_conflicts"] > 0:
            warnings.append(
                f"⚠️  {stats['multi_gsid_conflicts']} records have multiple GSID conflicts (potential merges needed)"
            )

        if stats["validation_warnings"] > 0:
            warnings.append(
                f"⚠️  {stats['validation_warnings']} records have ID validation warnings"
            )

        return {
            "gsids": gsids,
            "local_id_records": local_id_records,
            "summary": stats,
            "warnings": warnings,
            "flagged_records": flagged_records,
        }
