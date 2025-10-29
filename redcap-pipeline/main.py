import argparse
import logging
import sys
from datetime import datetime

from core.config import settings
from core.database import close_db_pool
from services.pipeline import REDCapPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"logs/pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point for REDCap pipeline"""
    parser = argparse.ArgumentParser(description="REDCap to IDHub Pipeline")
    parser.add_argument(
        "--project",
        type=str,
        help="Specific project key to process (e.g., 'primary_biobank')",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all enabled projects",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Batch size for processing (default: 50)",
    )

    args = parser.parse_args()

    try:
        # Determine which projects to run
        if args.project:
            # Run specific project
            project_config = settings.get_project_config(args.project)
            if not project_config:
                logger.error(f"Project '{args.project}' not found in configuration")
                return 1

            if not project_config.enabled:
                logger.warning(f"Project '{args.project}' is disabled in configuration")
                return 1

            projects_to_run = [project_config]
            logger.info(f"Running pipeline for project: {project_config.project_name}")

        elif args.all:
            # Run all enabled projects
            all_projects = settings.load_projects_config()
            projects_to_run = [p for p in all_projects.values() if p.enabled]
            logger.info(f"Running pipeline for {len(projects_to_run)} enabled projects")

        else:
            # Default: run all continuous projects
            all_projects = settings.load_projects_config()
            projects_to_run = [
                p
                for p in all_projects.values()
                if p.enabled and p.schedule == "continuous"
            ]
            logger.info(
                f"Running pipeline for {len(projects_to_run)} continuous projects"
            )

        if not projects_to_run:
            logger.warning("No projects to process")
            return 0

        # Process each project
        total_success = 0
        total_errors = 0

        for project_config in projects_to_run:
            logger.info("")
            logger.info("=" * 80)
            logger.info(f"Processing: {project_config.project_name}")
            logger.info(f"Project Key: {project_config.project_key}")
            logger.info(f"REDCap ID: {project_config.redcap_project_id}")
            logger.info("=" * 80)

            try:
                pipeline = REDCapPipeline(project_config=project_config)
                batch_size = args.batch_size or project_config.batch_size
                result = pipeline.run(batch_size=batch_size)

                total_success += result["total_success"]
                total_errors += result["total_errors"]

                logger.info(
                    f"✓ {project_config.project_name} complete: "
                    f"{result['total_success']} success, {result['total_errors']} errors"
                )

            except Exception as e:
                logger.error(
                    f"✗ {project_config.project_name} failed: {e}", exc_info=True
                )
                total_errors += 1

        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info(f"Projects processed: {len(projects_to_run)}")
        logger.info(f"Total records succeeded: {total_success}")
        logger.info(f"Total records failed: {total_errors}")
        logger.info("=" * 80)

        return 0 if total_errors == 0 else 1

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1
    finally:
        close_db_pool()


if __name__ == "__main__":
    sys.exit(main())
