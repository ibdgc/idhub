# redcap-pipeline/services/gsid_client.py
import logging
from typing import List

import requests
from core.config import settings

logger = logging.getLogger(__name__)


class GSIDClient:
    def __init__(self):
        self.service_url = settings.GSID_SERVICE_URL
        self.api_key = settings.GSID_API_KEY

    def generate_gsids(self, count: int) -> List[str]:
        """Request GSIDs from GSID service"""
        headers = {"x-api-key": self.api_key}
        payload = {"count": count}

        try:
            response = requests.post(
                f"{self.service_url}/generate",
                json=payload,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            logger.info(f"Generated {len(data['gsids'])} GSIDs")
            return data["gsids"]
        except Exception as e:
            logger.error(f"Error generating GSIDs: {e}")
            raise
