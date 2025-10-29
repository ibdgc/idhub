#!/usr/bin/env python3
import logging
import sys

from core.config import settings
from core.database import close_db_pool
from services.pipeline import REDCapPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point for REDCap pipeline"""
    try:
        # Load all project configurations
        projects = settings.load_projects_config()

        # Filter to only enabled projects with continuous schedule
        continuous_projects = {
            key: config
            for key, config in projects.items()
            if config.enabled and config.schedule == "continuous"
        }

        if not continuous_projects:
            logger.warning("No enabled continuous projects found")
            return 0

        logger.info(f"Running pipeline for {len(continuous_projects)} projects")

        total_results = []

        for project_key, project_config in continuous_projects.items():
            logger.info("=" * 80)
            logger.info(
                f"Processing project: {project_key} ({project_config.project_name})"
            )
            logger.info("=" * 80)

            try:
                pipeline = REDCapPipeline(project_config)
                result = pipeline.run()
                total_results.append(result)

                logger.info(
                    f"✓ {project_key} complete: {result['total_success']} success, "
                    f"{result['total_errors']} errors"
                )
            except Exception as e:
                logger.error(f"✗ {project_key} failed: {e}", exc_info=True)
                total_results.append(
                    {
                        "project_key": project_key,
                        "project_name": project_config.project_name,
                        "total_success": 0,
                        "total_errors": 0,
                        "error": str(e),
                    }
                )

        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)
        for result in total_results:
            status = "✓" if "error" not in result else "✗"
            logger.info(
                f"{status} {result['project_key']}: "
                f"{result['total_success']} success, {result['total_errors']} errors"
            )

        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1
    finally:
        close_db_pool()


if __name__ == "__main__":
    sys.exit(main())
