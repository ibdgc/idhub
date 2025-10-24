import logging
import sys

from services.pipeline import REDCapPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ],
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point for REDCap pipeline"""
    try:
        pipeline = REDCapPipeline()
        result = pipeline.run(batch_size=50)
        
        logger.info(f"Pipeline completed successfully: {result['batch_id']}")
        logger.info(f"Success: {result['total_success']}, Errors: {result['total_errors']}")
        
        return 0
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

