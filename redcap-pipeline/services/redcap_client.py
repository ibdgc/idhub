# redcap-pipeline/services/redcap_client.py
import logging
from typing import Any, Dict, List

import requests
from core.config import settings

logger = logging.getLogger(__name__)


class REDCapClient:
    def __init__(self):
        self.api_url = settings.REDCAP_API_URL
        self.api_token = settings.REDCAP_API_TOKEN

    def fetch_records(self) -> List[Dict[str, Any]]:
        """Fetch all records from REDCap with data access groups"""
        payload = {
            "token": self.api_token,
            "content": "record",
            "format": "json",
            "type": "flat",
            "rawOrLabel": "raw",
            "exportDataAccessGroups": "true",  # This is critical!
        }

        try:
            logger.info(f"Fetching records from REDCap API: {self.api_url}")
            logger.debug(f"Payload: {payload}")
            
            response = requests.post(self.api_url, data=payload, timeout=60)
            response.raise_for_status()
            records = response.json()
            
            logger.info(f"Fetched {len(records)} records from REDCap")
            
            # Debug: Check first record
            if records:
                logger.info(f"Sample record keys: {list(records[0].keys())}")
                logger.info(f"Sample record: {records[0]}")
                
                # Check for DAG field
                dag_field = records[0].get("redcap_data_access_group")
                logger.info(f"First record DAG value: '{dag_field}'")
                
                # Count records with DAG
                records_with_dag = sum(1 for r in records if r.get("redcap_data_access_group"))
                logger.info(f"Records with DAG: {records_with_dag}/{len(records)}")
            
            return records
            
        except Exception as e:
            logger.error(f"Error fetching REDCap records: {e}")
            raise

    def fetch_records_batch(
        self, batch_size: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch records in batches"""
        logger.info(f"Fetching batch: offset={offset}, limit={batch_size}")

        all_records = self.fetch_records()
        return all_records[offset : offset + batch_size]

    def fetch_metadata(self) -> List[Dict[str, Any]]:
        """Fetch field metadata from REDCap"""
        payload = {
            "token": self.api_token,
            "content": "metadata",
            "format": "json",
        }

        try:
            response = requests.post(self.api_url, data=payload, timeout=60)
            response.raise_for_status()
            metadata = response.json()
            logger.info(f"Fetched metadata for {len(metadata)} fields")
            return metadata
        except Exception as e:
            logger.error(f"Error fetching REDCap metadata: {e}")
            raise

