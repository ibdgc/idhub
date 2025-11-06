import logging
from datetime import datetime
from typing import Dict, List

from core.database import db_manager

logger = logging.getLogger(__name__)


class SpecimenUpdater:
    """Update specimen table based on LabKey data"""

    def get_sample_ids(self, limit: int = None) -> List[str]:
        """Get all sample IDs from specimen table"""
        with db_manager.get_connection() as conn:
            cur = conn.cursor()

            query = "SELECT sample_id FROM specimen"
            if limit:
                query += f" LIMIT {limit}"

            cur.execute(query)
            rows = cur.fetchall()

            sample_ids = [row["sample_id"] for row in rows]
            logger.info(f"Retrieved {len(sample_ids)} sample IDs from database")
            return sample_ids

    def update_specimens(
        self, labkey_data: Dict[str, Dict], dry_run: bool = False
    ) -> Dict:
        """
        Update specimen records based on LabKey data

        Args:
            labkey_data: Dict mapping sample_id to {status, date}
            dry_run: If True, don't commit changes

        Returns:
            Summary of updates
        """
        stats = {
            "total_samples": len(labkey_data),
            "consumed_updates": 0,
            "date_updates": 0,
            "errors": 0,
        }

        with db_manager.get_connection() as conn:
            cur = conn.cursor()

            for sample_id, info in labkey_data.items():
                try:
                    updates = []
                    params = []

                    # Check if status is "consumed"
                    if info.get("status", "").lower() == "consumed":
                        updates.append("sample_available = %s")
                        params.append(False)
                        stats["consumed_updates"] += 1

                    # Check if date is available
                    if info.get("date"):
                        updates.append("year_collected = %s")
                        params.append(info["date"].date())
                        stats["date_updates"] += 1

                    # Execute update if there are changes
                    if updates:
                        params.append(sample_id)
                        query = f"""
                        UPDATE specimen 
                        SET {", ".join(updates)}
                        WHERE sample_id = %s
                        """

                        if dry_run:
                            logger.info(
                                f"[DRY RUN] Would update {sample_id}: {updates}"
                            )
                        else:
                            cur.execute(query, params)
                            logger.debug(f"Updated {sample_id}")

                except Exception as e:
                    logger.error(f"Error updating {sample_id}: {e}")
                    stats["errors"] += 1

            if dry_run:
                conn.rollback()
                logger.info("DRY RUN - No changes committed")
            else:
                conn.commit()
                logger.info("Changes committed to database")

        return stats
