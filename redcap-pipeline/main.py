import sys
import logging
from services.pipeline import REDCapPipeline
from core.database import close_db_pool
from core.config import settings

logger = logging.getLogger(__name__)


def main():
    """Main entry point for REDCap pipeline - processes all enabled projects"""
    try:
        # Load all enabled projects
        projects = settings.get_enabled_projects()

        if not projects:
            logger.warning("No enabled projects found")
            return 0

        logger.info(f"Processing {len(projects)} enabled projects: {[p['key'] for p in projects]}")

        total_success = 0
        total_errors = 0

        # Process each project
        for project_config in projects:
            project_key = project_config['key']
            logger.info("")
            logger.info("=" * 80)
            logger.info(f"Starting pipeline for project: {project_key}")
            logger.info("=" * 80)

            try:
                pipeline = REDCapPipeline(project_config)
                result = pipeline.run(batch_size=project_config.get('batch_size', 50))

                total_success += result.get('total_success', 0)
                total_errors += result.get('total_errors', 0)

                logger.info(f"✓ {project_key} complete: {result.get('total_success', 0)} success, {result.get('total_errors', 0)} errors")

            except Exception as e:
                logger.error(f"✗ Pipeline failed for {project_key}: {e}", exc_info=True)
                total_errors += 1

        logger.info("")
        logger.info("=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total: {total_success} success, {total_errors} errors")

        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1
    finally:
        close_db_pool()


if __name__ == "__main__":
    sys.exit(main())
