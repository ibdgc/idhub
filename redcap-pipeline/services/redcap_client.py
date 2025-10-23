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

    def fetch_records(self, fields: List[str] = None) -> List[Dict[str, Any]]:
        """Fetch records from REDCap"""
        payload = {
            "token": self.api_token,
            "content": "record",
            "format": "json",
            "type": "flat",
            "rawOrLabel": "raw",
        }

        if fields:
            payload["fields"] = ",".join(fields)

        try:
            response = requests.post(self.api_url, data=payload, timeout=60)
            response.raise_for_status()
            records = response.json()
            logger.info(f"Fetched {len(records)} records from REDCap")
            return records
        except Exception as e:
            logger.error(f"Error fetching REDCap records: {e}")
            raise

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
