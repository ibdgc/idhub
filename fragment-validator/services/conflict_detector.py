# fragment-validator/services/conflict_detector.py
import logging
from typing import Dict, List, Tuple

import pandas as pd
from core.database import get_db_connection  # Add this import

logger = logging.getLogger(__name__)


class ConflictDetector:
    """Detects data conflicts between incoming fragments and existing database state"""

    def __init__(self, nocodb_client):
        """
        Args:
            nocodb_client: NocoDBClient instance (used for uploading conflicts)
        """
        self.nocodb_client = nocodb_client

    def detect_conflicts(
        self,
        batch_id: str,
        local_subject_ids_df: pd.DataFrame,
    ) -> Tuple[List[Dict], Dict]:
        """
        Detect conflicts between incoming data and existing PostgreSQL records

        Args:
            batch_id: Batch identifier
            local_subject_ids_df: DataFrame with local_subject_id mappings

        Returns:
            Tuple of (conflicts_list, summary_dict)
        """
        logger.info("Detecting data conflicts against PostgreSQL database...")

        conflicts = []

        # Get unique (local_subject_id, identifier_type) pairs
        unique_pairs = local_subject_ids_df[
            ["local_subject_id", "identifier_type", "center_id", "global_subject_id"]
        ].drop_duplicates()

        logger.info(f"Checking {len(unique_pairs)} unique ID pairs for conflicts...")

        # Query PostgreSQL for existing mappings
        existing_mappings = self._fetch_existing_mappings_from_postgres(
            unique_pairs["local_subject_id"].tolist(),
            unique_pairs["identifier_type"].tolist(),
        )

        logger.info(f"Found {len(existing_mappings)} existing mappings in PostgreSQL")

        # Check each incoming record for conflicts
        for _, row in unique_pairs.iterrows():
            local_id = row["local_subject_id"]
            id_type = row["identifier_type"]
            incoming_center = row["center_id"]
            incoming_gsid = row["global_subject_id"]

            key = (local_id, id_type)

            if key in existing_mappings:
                existing = existing_mappings[key]
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
                        "status": "pending",
                    }
                    conflicts.append(conflict)

                    logger.warning(
                        f"⚠️  Center conflict: {id_type}={local_id} - "
                        f"DB has center_id={existing_center} (GSID={existing_gsid}), "
                        f"incoming has center_id={incoming_center} (GSID={incoming_gsid})"
                    )

                # Check for GSID mismatch (same ID maps to different GSIDs)
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
                        "status": "pending",
                    }
                    conflicts.append(conflict)

                    logger.warning(
                        f"⚠️  GSID conflict: {id_type}={local_id} - "
                        f"DB has GSID={existing_gsid}, incoming has GSID={incoming_gsid}"
                    )

        # Build summary
        summary = self._build_conflict_summary(conflicts)

        logger.info(f"Detected {len(conflicts)} conflicts in batch {batch_id}")

        return conflicts, summary

    def _fetch_existing_mappings_from_postgres(
        self, local_ids: List[str], id_types: List[str]
    ) -> Dict[Tuple[str, str], Dict]:
        """
        Fetch existing mappings from PostgreSQL database

        Args:
            local_ids: List of local_subject_ids to check
            id_types: List of identifier_types to check

        Returns:
            Dict mapping (local_subject_id, identifier_type) -> {center_id, global_subject_id}
        """
        if not local_ids:
            return {}

        try:
            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    # Query for existing mappings
                    query = """
                        SELECT 
                            local_subject_id,
                            identifier_type,
                            center_id,
                            global_subject_id
                        FROM local_subject_ids
                        WHERE local_subject_id = ANY(%s)
                          AND identifier_type = ANY(%s)
                    """

                    cursor.execute(query, (local_ids, id_types))
                    rows = cursor.fetchall()

                    # Build lookup dict
                    mappings = {}
                    for row in rows:
                        key = (row[0], row[1])  # (local_subject_id, identifier_type)
                        mappings[key] = {
                            "center_id": row[2],
                            "global_subject_id": row[3],
                        }

                    return mappings

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Failed to fetch existing mappings from PostgreSQL: {e}")
            return {}

    def _fetch_existing_mappings_from_nocodb(
        self, local_ids: List[str], id_types: List[str]
    ) -> Dict[Tuple[str, str], Dict]:
        """
        DEPRECATED: Use _fetch_existing_mappings_from_postgres instead

        Kept for backward compatibility only.
        """
        logger.warning(
            "Using deprecated NocoDB conflict detection - switch to PostgreSQL"
        )

        try:
            all_records = self.nocodb_client.get_all_records("local_subject_ids")

            mappings = {}
            local_ids_set = set(local_ids)
            id_types_set = set(id_types)

            for record in all_records:
                local_id = record.get("local_subject_id")
                id_type = record.get("identifier_type")

                if local_id in local_ids_set and id_type in id_types_set:
                    key = (local_id, id_type)
                    mappings[key] = {
                        "center_id": record.get("center_id"),
                        "global_subject_id": record.get("global_subject_id"),
                    }

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
        """
        Upload conflicts to conflict_resolutions table in NocoDB

        Args:
            conflicts: List of conflict dictionaries
        """
        if not conflicts:
            logger.info("No conflicts to upload")
            return

        try:
            self.nocodb_client.upload_conflicts(conflicts)
            logger.info(f"✓ Uploaded {len(conflicts)} conflicts to NocoDB")

        except Exception as e:
            logger.error(f"Failed to upload conflicts to NocoDB: {e}")
            raise
