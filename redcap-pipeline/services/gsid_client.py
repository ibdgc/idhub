# redcap-pipeline/services/gsid_client.py
import logging
from typing import Dict, List

import requests
from core.config import settings

logger = logging.getLogger(__name__)


class GSIDClient:
    def __init__(self):
        self.service_url = settings.GSID_SERVICE_URL
        self.api_key = settings.GSID_API_KEY

    def generate_gsids(self, count: int) -> List[str]:
        """Request GSIDs from GSID service"""
        if not self.api_key:
            raise ValueError(
                "GSID_API_KEY environment variable is not set. "
                "Please configure the API key to authenticate with the GSID service."
            )

        headers = {"x-api-key": self.api_key}
        payload = {"count": count}
        url = f"{self.service_url}/generate"

        try:
            logger.info(f"Requesting {count} GSIDs from {url}")
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            if "gsids" not in data:
                raise ValueError(f"Unexpected response format: {data}")

            logger.info(f"Successfully generated {len(data['gsids'])} GSIDs")
            return data["gsids"]

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.error(
                    "Authentication failed: Invalid API key. "
                    "Please check that GSID_API_KEY matches the service configuration."
                )
            elif e.response.status_code == 500:
                logger.error(f"GSID service error: {e.response.text}")
            else:
                logger.error(f"HTTP {e.response.status_code}: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error generating GSIDs: {e}")
            raise

    def register_subject(
        self,
        center_id: int,
        local_subject_id: str,
        registration_year: str = None,
        control: bool = False,
    ) -> Dict[str, str]:
        """Register subject with GSID service
        
        Returns:
            dict: {"gsid": "...", "action": "created|existing"}
        """
        if not self.api_key:
            raise ValueError(
                "GSID_API_KEY environment variable is not set. "
                "Please configure the API key to authenticate with the GSID service."
            )

        headers = {"x-api-key": self.api_key}
        payload = {
            "center_id": center_id,
            "local_subject_id": local_subject_id,
            "registration_year": registration_year,
            "control": control,
            "created_by": "redcap_pipeline",
        }
        url = f"{self.service_url}/register"

        try:
            logger.debug(
                f"Registering subject: center_id={center_id}, "
                f"local_subject_id={local_subject_id}"
            )
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            logger.debug(f"Registration response: {data}")
            return data

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.error("Authentication failed: Invalid API key")
            else:
                logger.error(f"HTTP {e.response.status_code}: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error registering subject: {e}")
            raise
