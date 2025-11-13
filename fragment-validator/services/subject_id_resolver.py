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

        This method now handles the "multiple candidate IDs per subject" case
        by registering each ID separately and detecting conflicts.

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
            "unknown_center_used": 0,
            "center_promoted": 0,
            "conflicts_detected": 0,
        }

        # Build batch requests - one request per candidate ID per row
        batch_requests = []
        row_to_requests = {}  # Track which requests belong to which row

        logger.info(f"Resolving subject IDs with candidates: {candidate_fields}")

        for idx, row in data.iterrows():
            # Get center_id for this row
            if center_id_field and center_id_field in row:
                center_id = int(row[center_id_field])
            else:
                center_id = default_center_id

            # Track if we're using Unknown center
            if center_id == 0 or center_id == 1:
                stats["unknown_center_used"] += 1

            # Collect all candidate IDs for this row
            row_requests = []
            for field in candidate_fields:
                if field in row and pd.notna(row[field]) and str(row[field]).strip():
                    local_id = str(row[field]).strip()

                    # Skip invalid values
                    if local_id.upper() in ["NA", "N/A", "NULL", "NONE", ""]:
                        continue

                    request = {
                        "center_id": center_id,
                        "local_subject_id": local_id,
                        "identifier_type": field,
                        "control": False,
                        "created_by": created_by,
                    }
                    batch_requests.append(request)
                    row_requests.append(len(batch_requests) - 1)  # Track request index

            if row_requests:
                row_to_requests[idx] = row_requests
            else:
                warnings.append(f"Row {idx}: No valid subject IDs found")

        if not batch_requests:
            logger.warning("No valid subject IDs found in dataset")
            return {
                "gsids": [],
                "local_id_records": [],
                "summary": stats,
                "warnings": warnings,
            }

        # Send all requests to GSID service
        logger.info(
            f"Sending {len(batch_requests)} registration requests to GSID service"
        )
        try:
            results = self.gsid_client.register_batch(batch_requests)
        except Exception as e:
            logger.error(f"GSID batch registration failed: {e}")
            raise

        # Process results - group by original row
        for idx, request_indices in row_to_requests.items():
            # Get all results for this row
            row_results = [results[i] for i in request_indices]

            # Extract GSIDs from results
            row_gsids = set()
            for result in row_results:
                if result.get("gsid"):
                    row_gsids.add(result["gsid"])

                    # Track action statistics
                    action = result.get("action", "unknown")
                    if action == "create_new":
                        stats["new_gsids_minted"] += 1
                    elif action in ["link_existing", "exact_match"]:
                        stats["existing_matches"] += 1

            # Check for conflicts (multiple GSIDs for same subject)
            if len(row_gsids) > 1:
                stats["conflicts_detected"] += 1
                warnings.append(
                    f"Row {idx}: Multiple GSIDs detected: {row_gsids}. "
                    f"Using first GSID: {list(row_gsids)[0]}"
                )
                logger.warning(
                    f"Row {idx}: GSID conflict - multiple IDs resolved to different GSIDs: {row_gsids}"
                )
                gsid = list(row_gsids)[0]  # Use first GSID
            elif len(row_gsids) == 1:
                gsid = list(row_gsids)[0]
            else:
                # No GSID returned (all failed)
                warnings.append(f"Row {idx}: Failed to resolve GSID")
                gsid = None

            gsids.append(gsid)

            # Build local_id_records for this row
            if gsid:
                for i, request_idx in enumerate(request_indices):
                    request = batch_requests[request_idx]
                    result = row_results[i]

                    # Only add if registration was successful
                    if result.get("gsid") == gsid:
                        local_id_records.append(
                            {
                                "global_subject_id": gsid,
                                "center_id": request["center_id"],
                                "local_subject_id": request["local_subject_id"],
                                "identifier_type": request["identifier_type"],
                            }
                        )

        # Deduplicate local_id_records
        unique_records = []
        seen = set()
        for record in local_id_records:
            key = (
                record["center_id"],
                record["local_subject_id"],
                record["identifier_type"],
            )
            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        logger.info(
            f"Resolution complete: {len(gsids)} subjects resolved, "
            f"{len(unique_records)} unique local IDs, "
            f"{stats['conflicts_detected']} conflicts detected"
        )

        return {
            "gsids": gsids,
            "local_id_records": unique_records,
            "summary": stats,
            "warnings": warnings,
        }
