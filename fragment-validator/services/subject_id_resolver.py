# fragment-validator/services/subject_id_resolver.py
import logging
from typing import Dict, List, Optional

import pandas as pd

from .gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class SubjectIDResolver:
    """Resolves subject IDs to GSIDs using GSID service"""

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
        subject_id_type_field: Optional[str] = None,
    ) -> Dict:
        """
        Resolve subject IDs for entire dataset with parallel processing.

        Returns dict with:
            - gsids: List of resolved GSIDs (one per row)
            - local_id_records: List of local ID records to insert
            - summary: Statistics
        """
        logger.info(f"Resolving subject IDs with candidates: {candidate_fields}")
        logger.info(f"Total rows to process: {len(data)}")

        # Build registration requests
        requests_list = []
        row_to_request_map = []

        for idx, row in data.iterrows():
            # Determine center_id
            if center_id_field and center_id_field in data.columns:
                center_id = int(row[center_id_field])
            else:
                center_id = default_center_id

            # Collect all non-null candidate IDs for this row
            identifiers = []
            
            # Determine the identifier type from the data row if the field is provided
            id_type_from_data = None
            if subject_id_type_field and subject_id_type_field in row and pd.notna(row[subject_id_type_field]):
                id_type_from_data = str(row[subject_id_type_field]).strip()

            for field in candidate_fields:
                if field in data.columns:
                    value = row[field]
                    if pd.notna(value) and str(value).strip():
                        # Use the type from the data if available, otherwise default to the column name
                        effective_identifier_type = id_type_from_data if id_type_from_data else field
                        identifiers.append(
                            {
                                "local_subject_id": str(value).strip(),
                                "identifier_type": effective_identifier_type,
                            }
                        )

            if not identifiers:
                logger.warning(f"Row {idx}: No valid subject IDs found in candidates")
                continue
            
            # Create registration request
            request = {
                "center_id": center_id,
                "identifiers": identifiers,
                "created_by": created_by,
            }

            requests_list.append(request)
            row_to_request_map.append(idx)

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
                    requests_list[i]["identifiers"][j]["local_subject_id"]
                    if j < len(requests_list[i]["identifiers"])
                    else requests_list[i]["identifiers"][0]["local_subject_id"]
                )
                id_type = (
                    requests_list[i]["identifiers"][j]["identifier_type"]
                    if j < len(requests_list[i]["identifiers"])
                    else requests_list[i]["identifiers"][0]["identifier_type"]
                )

                local_id_records.append(
                    {
                        "center_id": requests_list[i]["center_id"],
                        "local_subject_id": local_id,
                        "identifier_type": id_type,
                        "global_subject_id": gsid,
                        "created_by": created_by,  # ✅ Changed from "source"
                    }
                )

        # Build unique local_id_records (deduplicate)
        unique_records = {}
        for record in local_id_records:
            key = (
                record["center_id"],
                record["local_subject_id"],
                record["identifier_type"],
            )
            if key not in unique_records:
                unique_records[key] = record

        local_id_records = list(unique_records.values())
        logger.info(f"Built {len(local_id_records)} unique local_subject_id records")

        # Build summary
        summary = {
            "total_rows": len(data),
            "resolved": len([g for g in gsids if g is not None]),
            "unresolved": len([g for g in gsids if g is None]),
            "unique_gsids": len(set(g for g in gsids if g is not None)),
            "created": sum(1 for r in results if r and r.get("action") == "create_new"),
            "linked": sum(
                1 for r in results if r and r.get("action") == "link_existing"
            ),
            "multi_gsid_conflicts": 0,  # Placeholder
            "center_conflicts": 0,  # Placeholder
        }

        logger.info(
            f"✓ Resolution complete: {summary['resolved']} resolved, "
            f"{summary['unresolved']} unresolved, {summary['unique_gsids']} unique GSIDs, "
            f"{summary['created']} created, {summary['linked']} linked"
        )

        return {
            "gsids": gsids,
            "local_id_records": local_id_records,
            "summary": summary,
        }