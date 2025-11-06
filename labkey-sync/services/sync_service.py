import logging
from typing import Dict

from core.config import settings

from .labkey_client import LabKeyClient
from .specimen_updater import SpecimenUpdater

logger = logging.getLogger(__name__)


class LabKeySyncService:
    """Orchestrate LabKey synchronization"""

    def __init__(self):
        self.labkey_client = LabKeyClient()
        self.specimen_updater = SpecimenUpdater()

    def sync(self, dry_run: bool = None, limit: int = None) -> Dict:
        """
        Synchronize specimen data with LabKey

        Args:
            dry_run: Override config dry_run setting
            limit: Limit number of samples to process (for testing)

        Returns:
            Summary of sync operation
        """
        if dry_run is None:
            dry_run = settings.DRY_RUN

        logger.info("=" * 60)
        logger.info("Starting LabKey Sync")
        logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
        logger.info("=" * 60)

        # Get sample IDs from database
        sample_ids = self.specimen_updater.get_sample_ids(limit=limit)

        if not sample_ids:
            logger.warning("No samples found in database")
            return {"status": "no_samples"}

        # Process in batches
        batch_size = settings.BATCH_SIZE
        total_stats = {
            "total_samples": 0,
            "consumed_updates": 0,
            "date_updates": 0,
            "errors": 0,
        }

        for i in range(0, len(sample_ids), batch_size):
            batch = sample_ids[i : i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}: {len(batch)} samples")

            # Query LabKey
            labkey_data = self.labkey_client.get_sample_info(batch)

            # Update specimens
            batch_stats = self.specimen_updater.update_specimens(labkey_data, dry_run)

            # Aggregate stats
            for key in total_stats:
                total_stats[key] += batch_stats.get(key, 0)

        logger.info("=" * 60)
        logger.info("Sync Complete")
        logger.info(f"Total samples processed: {total_stats['total_samples']}")
        logger.info(f"Consumed status updates: {total_stats['consumed_updates']}")
        logger.info(f"Date updates: {total_stats['date_updates']}")
        logger.info(f"Errors: {total_stats['errors']}")
        logger.info("=" * 60)

        return total_stats
