# table-loader/services/data_transformer.py
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform fragment data for database insertion"""

    def __init__(self, table_name: str):
        self.table_name = table_name
        logger.debug(f"DataTransformer initialized for table: {table_name}")

    def transform_records(self, fragment: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform fragment records for the target table

        Args:
            fragment: Fragment data from S3 with 'records' key

        Returns:
            List of transformed records ready for database insertion
        """
        records = fragment.get("records", [])

        if not records:
            logger.warning(f"No records found in fragment for {self.table_name}")
            return []

        logger.info(f"Transforming {len(records)} records for {self.table_name}")

        # Apply table-specific transformations
        transformed = []
        for record in records:
            try:
                transformed_record = self._transform_record(record)
                if transformed_record:
                    transformed.append(transformed_record)
            except Exception as e:
                logger.error(f"Failed to transform record: {e}")
                # Continue processing other records

        logger.info(f"Transformed {len(transformed)}/{len(records)} records")
        return transformed

    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record"""
        # For now, pass through as-is
        # Add table-specific transformations here as needed
        return record
