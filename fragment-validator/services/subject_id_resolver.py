# fragment-validator/services/subject_id_resolver.py
import logging
from typing import Dict, List, Optional

import pandas as pd

from .gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class SubjectIDResolver:
    """Resolves subject IDs using GSID service with center-agnostic matching"""

    def __init__(self, gsid_client: GSIDClient):
        self.gsid_client = gsid_client

    def resolve_batch(
        self,
        data: pd.DataFrame,
        candidate_fields: List[str],
        center_id_field: Optional[str] = None,
        default_center_id: int = 0,
        created_by: str = "fragment_validator",
        batch_size: int = 20,
    ) -> Dict:
        """
        Resolve subject IDs for entire dataset with parallel processing.

        Uses the unified /register/subject endpoint which:
        - Matches on local_subject_id ALONE (center-agnostic)
        - Flags center conflicts for review
        - Links all identifiers to the same GSID

        Args:
            data: DataFrame with subject data
            candidate_fields: List of fields that may contain subject IDs
            center_id_field: Optional field containing center_id
            default_center_id: Default center_id if not specified (use 0 for unknown)
            created_by: Source identifier
            batch_size: Number of parallel workers (default 20)

        Returns dict with:
            - gsids: List of resolved GSIDs (one per row)
            - summary: Statistics including conflicts and warnings
        """
        logger.info(f"Resolving subject IDs with candidates: {candidate_fields}")
        logger.info(f"Total rows to process: {len(data)}")

        # Build registration requests
        requests_list = []
        row_to_request_map = []

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

            # Extract ALL non-null identifiers from candidate fields
            identifiers = []
            for field in candidate_fields:
                if field in row and pd.notna(row[field]) and str(row[field]).strip():
                    local_id = str(row[field]).strip()

                    # Determine identifier type from field name
                    if "consortium" in field.lower():
                        id_type = "consortium_id"
                    elif "niddk" in field.lower():
                        id_type = "niddk_no"
                    elif "knumber" in field.lower() or "k_number" in field.lower():
                        id_type = "knumber"
                    elif field.lower() in ["local_subject_id", "subject_id"]:
                        id_type = "primary"
                    else:
                        id_type = field  # Use field name as type

                    identifiers.append(
                        {
                            "local_subject_id": local_id,
                            "identifier_type": id_type,
                        }
                    )

            if not identifiers:
                logger.warning(f"Row {idx}: No valid identifiers found")
                continue

            # Create registration request matching API schema
            requests_list.append(
                {
                    "center_id": center_id,
                    "identifiers": identifiers,
                    "created_by": created_by,
                }
            )
            row_to_request_map.append(idx)

        if not requests_list:
            raise ValueError("No valid subject IDs found in dataset")

        logger.info(f"Prepared {len(requests_list)} registration requests")

        # Call GSID service with parallel workers
        logger.info(f"Calling GSID service with {batch_size} parallel workers...")
        results = self.gsid_client.register_batch(
            requests_list, batch_size=batch_size, timeout=120
        )

        # Map results back to DataFrame rows
        gsids = [None] * len(data)
        failed_rows = []
        warnings_list = []
        conflicts_count = 0
        center_conflicts_count = 0

        for i, result in enumerate(results):
            if result is None:
                failed_rows.append(row_to_request_map[i])
                continue

            row_idx = row_to_request_map[i]
            gsids[row_idx] = result["gsid"]

            # Collect warnings
            if result.get("warnings"):
                warnings_list.extend(result["warnings"])
                # Count center conflicts
                center_conflicts_count += sum(
                    1 for w in result["warnings"] if "center" in w.lower()
                )

            # Count multi-GSID conflicts
            if result.get("conflicts"):
                conflicts_count += 1

        # Calculate summary statistics
        actions = [r.get("action") for r in results if r is not None]
        summary = {
            "total_rows": len(data),
            "resolved": len([g for g in gsids if g is not None]),
            "unresolved": len([g for g in gsids if g is None]),
            "unique_gsids": len(set(g for g in gsids if g is not None)),
            "created": actions.count("create_new"),
            "linked": actions.count("link_existing"),
            "multi_gsid_conflicts": conflicts_count,
            "center_conflicts": center_conflicts_count,
            "warnings": warnings_list,
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
            f"{summary['unique_gsids']} unique GSIDs, "
            f"{summary['created']} created, "
            f"{summary['linked']} linked"
        )

        if summary["multi_gsid_conflicts"] > 0:
            logger.warning(
                f"⚠ {summary['multi_gsid_conflicts']} multi-GSID conflicts detected (flagged for review)"
            )

        if summary["center_conflicts"] > 0:
            logger.warning(
                f"⚠ {summary['center_conflicts']} center conflicts detected (flagged for review)"
            )

        if warnings_list:
            logger.warning(f"Warnings detected ({len(warnings_list)}):")
            for warning in warnings_list[:10]:  # Show first 10
                logger.warning(f"  - {warning}")
            if len(warnings_list) > 10:
                logger.warning(f"  ... and {len(warnings_list) - 10} more")

        return {
            "gsids": gsids,
            "summary": summary,
        }
