import argparse
import json
import logging
import os
import sys
from pathlib import Path

from core.database import close_db_pool
from services.pipeline import REDCapPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def load_projects() -> dict:
    """Load project configurations from config/projects.json"""
    config_path = Path(__file__).parent / "config" / "projects.json"

    if not config_path.exists():
        logger.error(f"Projects configuration not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)

    return config.get("projects", {})


def get_project_config(projects: dict, project_key: str) -> dict:
    """Get configuration for a specific project"""
    if project_key not in projects:
        logger.error(f"Project '{project_key}' not found in configuration")
        logger.info(f"Available projects: {', '.join(projects.keys())}")
        sys.exit(1)

    project = projects[project_key]

    # Add the key to the config
    project["key"] = project_key

    # Substitute environment variables in api_token
    if "api_token" in project:
        api_token = project["api_token"]
        if api_token.startswith("${") and api_token.endswith("}"):
            env_var = api_token[2:-1]
            project["api_token"] = os.getenv(env_var)

    return project


def run_project(project_key: str, project_config: dict) -> dict:
    """Run pipeline for a single project with improved error handling"""
    try:
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Starting pipeline for project: {project_key}")
        logger.info(f"{'=' * 60}\n")

        # Check if project is enabled
        if not project_config.get("enabled", True):
            logger.warning(f"Project '{project_key}' is disabled. Skipping.")
            return {
                "project": project_key,
                "status": "skipped",
                "reason": "disabled",
            }

        # Initialize and run pipeline
        pipeline = REDCapPipeline(project_config)
        batch_size = project_config.get("batch_size", 50)
        result = pipeline.run(batch_size=batch_size)

        # Handle different result statuses
        if result.get("status") == "partial_success":
            logger.warning(
                f"\n{'=' * 60}\n"
                f"⚠ Project '{project_key}' partially completed:\n"
                f"  - Records processed: {result['total_success']}\n"
                f"  - Errors: {result['total_errors']}\n"
                f"  - Last offset: {result['last_offset']}\n"
                f"  - Error: {result.get('error', 'Unknown')}\n"
                f"{'=' * 60}\n"
            )
            return {
                "project": project_key,
                "status": "partial_success",
                "total_success": result["total_success"],
                "total_errors": result["total_errors"],
                "last_offset": result["last_offset"],
                "error": result.get("error"),
            }

        elif result.get("status") == "error":
            logger.error(
                f"\n{'=' * 60}\n"
                f"✗ Project '{project_key}' failed:\n"
                f"  - Records processed: {result['total_success']}\n"
                f"  - Errors: {result['total_errors']}\n"
                f"  - Error: {result.get('error', 'Unknown')}\n"
                f"{'=' * 60}\n"
            )
            return {
                "project": project_key,
                "status": "error",
                "total_success": result["total_success"],
                "total_errors": result["total_errors"],
                "error": result.get("error"),
            }

        else:  # success
            logger.info(
                f"\n{'=' * 60}\n"
                f"✓ Project '{project_key}' completed successfully:\n"
                f"  - Records processed: {result['total_success']}\n"
                f"  - Errors: {result['total_errors']}\n"
                f"{'=' * 60}\n"
            )
            return {
                "project": project_key,
                "status": "success",
                "total_success": result["total_success"],
                "total_errors": result["total_errors"],
            }

    except Exception as e:
        logger.error(f"✗ Project '{project_key}' failed: {e}", exc_info=True)
        return {
            "project": project_key,
            "status": "error",
            "error": str(e),
            "total_success": 0,
            "total_errors": 0,
        }


def main():
    """Main entry point for REDCap pipeline"""
    parser = argparse.ArgumentParser(description="REDCap Integration Pipeline")
    parser.add_argument(
        "--project",
        type=str,
        help="Run pipeline for a specific project (e.g., 'gap', 'cd_ileal')",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run pipeline for all enabled projects",
    )

    args = parser.parse_args()

    # Load project configurations
    projects = load_projects()

    if not projects:
        logger.error("No projects configured")
        sys.exit(1)

    logger.info(f"Loaded {len(projects)} project(s): {', '.join(projects.keys())}")

    results = []

    try:
        if args.project:
            # Run specific project
            project_config = get_project_config(projects, args.project)
            result = run_project(args.project, project_config)
            results.append(result)

        elif args.all:
            # Run all enabled projects
            logger.info(f"\nRunning pipeline for all enabled projects...\n")

            for project_key in projects.keys():
                project_config = get_project_config(projects, project_key)
                result = run_project(project_key, project_config)
                results.append(result)

        else:
            # Default: check environment variable or run all
            project_key = os.getenv("PROJECT_KEY")

            if project_key:
                logger.info(f"Using PROJECT_KEY from environment: {project_key}")
                project_config = get_project_config(projects, project_key)
                result = run_project(project_key, project_config)
                results.append(result)
            else:
                logger.info(
                    "No --project or --all specified, running all enabled projects"
                )
                for project_key in projects.keys():
                    project_config = get_project_config(projects, project_key)
                    result = run_project(project_key, project_config)
                    results.append(result)

        # Print summary
        logger.info(f"\n{'=' * 60}")
        logger.info("PIPELINE SUMMARY")
        logger.info(f"{'=' * 60}")

        for result in results:
            if result["status"] == "skipped":
                logger.info(f"  {result['project']}: SKIPPED ({result['reason']})")
            elif result["status"] == "partial_success":
                logger.warning(
                    f"  {result['project']}: PARTIAL SUCCESS - "
                    f"{result['total_success']} processed, "
                    f"{result['total_errors']} errors, "
                    f"stopped at offset {result['last_offset']}"
                )
            elif result["status"] == "error":
                logger.error(f"  {result['project']}: ERROR - {result['error']}")
            else:
                logger.info(
                    f"  {result['project']}: SUCCESS - "
                    f"{result['total_success']} processed, {result['total_errors']} errors"
                )

        logger.info(f"{'=' * 60}\n")

        # Exit with error if any project completely failed (not partial success)
        if any(r["status"] == "error" for r in results):
            logger.error("One or more projects failed completely")
            sys.exit(1)

        # Exit with warning code if any partial successes
        if any(r["status"] == "partial_success" for r in results):
            logger.warning("One or more projects partially completed")
            sys.exit(2)  # Different exit code for partial success

        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1

    finally:
        close_db_pool()


if __name__ == "__main__":
    sys.exit(main())
