# fragment-validator/services/subject_id_resolver.py
import logging
from typing import Dict, List, Optional

import pandas as pd

from .gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class SubjectIDResolver:
    """Resolves subject IDs using GSID service with optimized parallel processing"""

    def __init__(self, gsid_client: GSIDClient):
        self.gsid_client = gsid_client

    def resolve_batch(
        self,
        data: pd.DataFrame,
        candidate_fields: List[str],
        center_id_field: Optional[str] = None,
        default_center_id: int = 0,
        created_by: str = "fragment_validator",
        batch_size: int = 20,  # REDUCED from 50 to 20
    ) -> Dict:
        """
        Resolve subject IDs for entire dataset with parallel processing.

        Uses parallel calls to /register/subject endpoint for better performance.

        Args:
            data: DataFrame with subject data
            candidate_fields: List of fields that may contain subject IDs
            center_id_field: Optional field containing center_id
            default_center_id: Default center_id if not specified
            created_by: Source identifier
            batch_size: Number of parallel workers (default 20, max recommended 50)

        Returns dict with:
            - gsids: List of resolved GSIDs (one per row)
            - local_id_records: List of local ID records to insert
            - summary: Statistics
        """
        logger.info(f"Resolving subject IDs with candidates: {candidate_fields}")
        logger.info(f"Total rows to process: {len(data)}")

        # Build registration requests
        requests_list = []
        row_to_request_map = []  # Track which request corresponds to which row

        for idx, row in data.iterrows():
            # Get center_id
            if center_id_field and center_id_field in row:
                center_id = (
                    int(row[center_id_field])
                    if pd.notna(row[center_id_field])
                    else default_center_id
                )
            else:
                center_id = default_center_id

            # Extract subject IDs from candidate fields
            subject_ids = []
            for field in candidate_fields:
                if field in row and pd.notna(row[field]) and str(row[field]).strip():
                    subject_ids.append(str(row[field]).strip())

            if not subject_ids:
                logger.warning(f"Row {idx}: No subject IDs found in candidate fields")
                continue

            # Determine primary identifier type based on field name
            primary_field = candidate_fields[0]
            if "consortium" in primary_field.lower():
                primary_type = "consortium_id"
            elif "niddk" in primary_field.lower():
                primary_type = "niddk_no"
            else:
                primary_type = "primary"

            # Create registration request
            requests_list.append(
                {
                    "local_subject_id": subject_ids[0],
                    "center_id": center_id,
                    "primary_type": primary_type,
                    "alternate_ids": subject_ids[1:] if len(subject_ids) > 1 else [],
                    "created_by": created_by,
                }
            )
            row_to_request_map.append(idx)

        if not requests_list:
            raise ValueError("No valid subject IDs found in dataset")

        logger.info(f"Prepared {len(requests_list)} registration requests")

        # Parallel register with GSID service
        logger.info(f"Calling GSID service with {batch_size} parallel workers...")
        results = self.gsid_client.register_batch(
            requests_list, batch_size=batch_size, timeout=120
        )

        # Map results back to DataFrame rows
        gsids = [None] * len(data)
        local_id_records = []
        failed_rows = []

        for i, result in enumerate(results):
            if result is None:
                failed_rows.append(row_to_request_map[i])
                continue

            row_idx = row_to_request_map[i]
            gsid = result["gsid"]
            gsids[row_idx] = gsid

            # Build local_subject_ids records
            identifiers_linked = result.get("identifiers_linked", 1)
            for j in range(identifiers_linked):
                local_id = (
                    requests_list[i]["local_subject_id"]
                    if j == 0
                    else requests_list[i]["alternate_ids"][j - 1]
                )
                local_id_records.append(
                    {
                        "global_subject_id": gsid,
                        "local_subject_id": local_id,
                        "center_id": requests_list[i]["center_id"],
                    }
                )

        # Calculate summary statistics
        summary = {
            "total_rows": len(data),
            "resolved": len([g for g in gsids if g is not None]),
            "unresolved": len([g for g in gsids if g is None]),
            "unique_gsids": len(set(g for g in gsids if g is not None)),
            "local_id_records": len(local_id_records),
            "unknown_center_used": sum(
                1 for req in requests_list if req["center_id"] == default_center_id
            ),
        }

        if failed_rows:
            logger.warning(f"⚠ {len(failed_rows)} rows failed to resolve GSIDs")

        logger.info(
            f"✓ Resolution complete: "
            f"{summary['resolved']} resolved, "
            f"{summary['unresolved']} unresolved, "
            f"{summary['unique_gsids']} unique GSIDs"
        )

        return {
            "gsids": gsids,
            "local_id_records": local_id_records,
            "summary": summary,
        }
