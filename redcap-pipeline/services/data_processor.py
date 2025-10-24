# redcap-pipeline/services/data_processor.py
import logging
from typing import Any, Dict, Optional

from core.database import db_manager
from services.center_resolver import CenterResolver
from services.gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, center_resolver: CenterResolver, gsid_client: GSIDClient):
        self.center_resolver = center_resolver
        self.gsid_client = gsid_client
        self.stats = {
            "processed": 0,
            "skipped_no_center": 0,
            "created": 0,
            "existing": 0,
            "errors": 0,
        }

    def process_records(self, records: list[Dict[str, Any]]) -> None:
        """Process REDCap records and insert into database"""
        logger.info(f"Processing {len(records)} records")
        
        for record in records:
            try:
                self._process_record(record)
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(
                    f"Error processing record {record.get('record_id')}: {e}"
                )
                raise
        
        logger.info(
            f"Processing complete: {self.stats['processed']} processed, "
            f"{self.stats['created']} created, {self.stats['existing']} existing, "
            f"{self.stats['skipped_no_center']} skipped (no center), "
            f"{self.stats['errors']} errors"
        )

    def _process_record(self, record: Dict[str, Any]) -> None:
        """Process a single record"""
        # Resolve center using the correct field name
        center_name = record.get("redcap_data_access_group", "")
        
        if not center_name:
            self.stats["skipped_no_center"] += 1
            logger.warning(
                f"Skipping record {record.get('record_id')} - no redcap_data_access_group"
            )
            return
        
        center_id = self.center_resolver.resolve_center_id(center_name)

        if not center_id:
            self.stats["skipped_no_center"] += 1
            logger.warning(
                f"Skipping record {record.get('record_id')} - "
                f"center '{center_name}' not found"
            )
            return

        # Register subject using GSID service (original logic)
        global_subject_id, action = self._register_subject(record, center_id)
        
        if action == "created":
            self.stats["created"] += 1
        else:
            self.stats["existing"] += 1
        
        self.stats["processed"] += 1

        logger.info(
            f"Processed record {record.get('record_id')} -> GSID: {global_subject_id} ({action})"
        )

    def _register_subject(self, record: Dict[str, Any], center_id: int) -> tuple[str, str]:
        """Register subject using GSID service /register endpoint
        
        Returns:
            tuple: (global_subject_id, action) where action is 'created' or 'existing'
        """
        # Get primary identifier (consortium_id or local_id)
        local_subject_id = record.get("consortium_id") or record.get("local_id")
        
        if not local_subject_id:
            raise ValueError(
                f"No local_subject_id found in record: {record.get('record_id')}"
            )
        
        # Extract registration year and control status
        registration_date = record.get("registration_date")
        registration_year = None
        if registration_date:
            # Extract year from date (e.g., "2023-01-15" -> "2023")
            registration_year = registration_date.split("-")[0] if "-" in registration_date else registration_date
        
        control = record.get("control", "0")
        # Convert to boolean
        control_bool = control in ["1", "yes", "true", True]
        
        # Call GSID service /register endpoint
        result = self.gsid_client.register_subject(
            center_id=center_id,
            local_subject_id=local_subject_id,
            registration_year=registration_year,
            control=control_bool,
        )
        
        return result["gsid"], result["action"]
