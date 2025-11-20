# fragment-validator/services/conflict_detector.py
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class ConflictDetector:
    """Detects and categorizes data conflicts during validation"""

    @staticmethod
    def detect_conflicts(resolution_result: Dict, batch_id: str) -> List[Dict]:
        """
        Detect conflicts from GSID resolution results

        Args:
            resolution_result: Result from SubjectIDResolver.resolve_batch()
            batch_id: Current batch identifier

        Returns:
            List of conflict records
        """
        conflicts = []
        local_id_records = resolution_result.get("local_id_records", [])

        for record in local_id_records:
            action = record.get("action", "")

            # Center mismatch conflict
            if action == "review_required":
                conflict = {
                    "batch_id": batch_id,
                    "conflict_type": "center_mismatch",
                    "local_subject_id": record["local_subject_id"],
                    "identifier_type": record["identifier_type"],
                    "incoming_center_id": record["center_id"],
                    "incoming_gsid": record["global_subject_id"],
                    "existing_center_id": record.get("existing_center_id"),
                    "existing_gsid": record.get("existing_gsid"),
                    "resolution_action": "pending",
                    "status": "pending",
                }
                conflicts.append(conflict)
                logger.warning(
                    f"Center conflict: {record['local_subject_id']} - "
                    f"existing center {record.get('existing_center_id')} vs "
                    f"incoming center {record['center_id']}"
                )

            # Multi-GSID conflict
            elif action == "multi_gsid_conflict":
                matched_gsids = record.get("matched_gsids", [])
                conflict = {
                    "batch_id": batch_id,
                    "conflict_type": "multi_gsid",
                    "local_subject_id": record["local_subject_id"],
                    "identifier_type": record["identifier_type"],
                    "incoming_center_id": record["center_id"],
                    "existing_gsid": matched_gsids[0] if matched_gsids else None,
                    "incoming_gsid": matched_gsids[1]
                    if len(matched_gsids) > 1
                    else None,
                    "resolution_action": "pending",
                    "status": "pending",
                    "resolution_notes": f"Multiple GSIDs found: {', '.join(matched_gsids)}",
                }
                conflicts.append(conflict)
                logger.error(
                    f"Multi-GSID conflict: {record['local_subject_id']} "
                    f"maps to {len(matched_gsids)} different GSIDs"
                )

        logger.info(f"Detected {len(conflicts)} conflicts in batch {batch_id}")
        return conflicts

    @staticmethod
    def format_conflict_summary(conflicts: List[Dict]) -> Dict:
        """Format conflicts for validation report"""
        by_type = {}
        for conflict in conflicts:
            conflict_type = conflict["conflict_type"]
            by_type[conflict_type] = by_type.get(conflict_type, 0) + 1

        return {
            "total_conflicts": len(conflicts),
            "by_type": by_type,
            "requires_review": len(conflicts) > 0,
        }
