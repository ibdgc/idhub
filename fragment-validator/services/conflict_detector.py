# fragment-validator/services/conflict_detector.py
import logging
from typing import Dict, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class ConflictDetector:
    """Detects data conflicts between incoming fragments and existing database state"""

    def __init__(self, nocodb_client):
        """
        Args:
            nocodb_client: NocoDBClient instance
        """
        self.nocodb_client = nocodb_client

    def detect_conflicts(
        self,
        batch_id: str,
        local_subject_ids_df: pd.DataFrame,
    ) -> Tuple[List[Dict], Dict]:
        """Detect conflicts including duplicate prevention"""
        logger.info("Detecting data conflicts against NocoDB database...")

        conflicts = []
        unique_pairs = local_subject_ids_df[
            ["local_subject_id", "identifier_type", "center_id", "global_subject_id"]
        ].drop_duplicates()

        logger.info(f"Checking {len(unique_pairs)} unique ID pairs for conflicts...")

        # Get ALL existing records (not just unique keys)
        all_existing = self._fetch_all_existing_records(
            unique_pairs["local_subject_id"].tolist(),
            unique_pairs["identifier_type"].tolist(),
        )

        logger.info(f"Found {len(all_existing)} existing records in NocoDB")

        # Check each incoming record
        for _, row in unique_pairs.iterrows():
            local_id = row["local_subject_id"]
            id_type = row["identifier_type"]
            incoming_center = row["center_id"]
            incoming_gsid = row["global_subject_id"]

            # Find ALL existing records for this (local_id, id_type)
            matching_records = [
                r
                for r in all_existing
                if r["local_subject_id"] == local_id and r["identifier_type"] == id_type
            ]

            for existing in matching_records:
                existing_center = existing["center_id"]
                existing_gsid = existing["global_subject_id"]

                # Check for center mismatch
                if existing_center != incoming_center:
                    conflict = {
                        "batch_id": batch_id,
                        "conflict_type": "center_mismatch",
                        "local_subject_id": local_id,
                        "identifier_type": id_type,
                        "existing_center_id": int(existing_center),
                        "incoming_center_id": int(incoming_center),
                        "existing_gsid": existing_gsid,
                        "incoming_gsid": incoming_gsid,
                        "resolution_action": None,
                    }
                    conflicts.append(conflict)

                    logger.warning(
                        f"⚠️  Center conflict: {id_type}={local_id} - "
                        f"NocoDB has center_id={existing_center}, incoming has center_id={incoming_center}"
                    )

                # Check for GSID mismatch
                elif existing_gsid != incoming_gsid:
                    conflict = {
                        "batch_id": batch_id,
                        "conflict_type": "multi_gsid",
                        "local_subject_id": local_id,
                        "identifier_type": id_type,
                        "existing_center_id": int(existing_center),
                        "incoming_center_id": int(incoming_center),
                        "existing_gsid": existing_gsid,
                        "incoming_gsid": incoming_gsid,
                        "resolution_action": None,
                    }
                    conflicts.append(conflict)

                    logger.warning(
                        f"⚠️  GSID conflict: {id_type}={local_id} - "
                        f"NocoDB has GSID={existing_gsid}, incoming has GSID={incoming_gsid}"
                    )

        summary = self._build_conflict_summary(conflicts)
        logger.info(f"Detected {len(conflicts)} conflicts in batch {batch_id}")

        return conflicts, summary

    def _fetch_all_existing_records(
        self, local_ids: List[str], id_types: List[str]
    ) -> List[Dict]:
        """Fetch ALL existing records (including duplicates)"""
        try:
            all_records = self.nocodb_client.get_all_records("local_subject_ids")

            local_ids_set = set(local_ids)
            id_types_set = set(id_types)

            # Return all matching records, not deduplicated
            matching = [
                r
                for r in all_records
                if r.get("local_subject_id") in local_ids_set
                and r.get("identifier_type") in id_types_set
            ]

            return matching

        except Exception as e:
            logger.error(f"Failed to fetch records from NocoDB: {e}")
            return []

    def _fetch_existing_mappings_from_nocodb(
        self, local_ids: List[str], id_types: List[str]
    ) -> Dict[Tuple[str, str], Dict]:
        """Fetch existing mappings, handling potential duplicates"""
        try:
            all_records = self.nocodb_client.get_all_records("local_subject_ids")

            mappings = {}
            duplicates = {}  # Track duplicates
            local_ids_set = set(local_ids)
            id_types_set = set(id_types)

            for record in all_records:
                local_id = record.get("local_subject_id")
                id_type = record.get("identifier_type")

                if local_id in local_ids_set and id_type in id_types_set:
                    key = (local_id, id_type)

                    # Check if we already have a record for this key
                    if key in mappings:
                        # Duplicate detected!
                        if key not in duplicates:
                            duplicates[key] = [mappings[key]]
                        duplicates[key].append(
                            {
                                "center_id": record.get("center_id"),
                                "global_subject_id": record.get("global_subject_id"),
                            }
                        )
                        logger.warning(
                            f"⚠️  Duplicate found in NocoDB: {id_type}={local_id} "
                            f"has multiple center_ids: {[d['center_id'] for d in duplicates[key]]}"
                        )
                    else:
                        mappings[key] = {
                            "center_id": record.get("center_id"),
                            "global_subject_id": record.get("global_subject_id"),
                        }

            # For conflict detection, use ALL existing center_ids
            # If duplicates exist, we need to check against all of them
            if duplicates:
                logger.warning(
                    f"Found {len(duplicates)} keys with duplicate records in NocoDB"
                )
                # Return the first occurrence but log the issue

            return mappings

        except Exception as e:
            logger.error(f"Failed to fetch existing mappings from NocoDB: {e}")
            return {}

    def _build_conflict_summary(self, conflicts: List[Dict]) -> Dict:
        """Build summary statistics for conflicts"""
        summary = {
            "total_conflicts": len(conflicts),
            "by_type": {},
            "requires_review": len(conflicts) > 0,
        }

        for conflict in conflicts:
            conflict_type = conflict["conflict_type"]
            summary["by_type"][conflict_type] = (
                summary["by_type"].get(conflict_type, 0) + 1
            )

        return summary

    def upload_conflicts_to_nocodb(self, conflicts: List[Dict]) -> None:
        """Upload conflicts to conflict_resolutions table in NocoDB"""
        if not conflicts:
            logger.info("No conflicts to upload")
            return

        try:
            self.nocodb_client.upload_conflicts(conflicts)
            logger.info(f"✓ Uploaded {len(conflicts)} conflicts to NocoDB")

        except Exception as e:
            logger.error(f"Failed to upload conflicts to NocoDB: {e}")
            raise
