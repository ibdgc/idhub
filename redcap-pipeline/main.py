# redcap-pipeline/main.py
import logging

from core.config import settings
from services.pipeline import REDCapPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main():
    """Main pipeline execution"""
    try:
        logger.info("Starting REDCap pipeline")
        pipeline = REDCapPipeline()
        pipeline.run()
        logger.info("REDCap pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
