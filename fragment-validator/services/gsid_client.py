# fragment-validator/services/gsid_client.py
import logging
from typing import Dict, List

import requests

logger = logging.getLogger(__name__)


class GSIDClient:
    """Client for GSID service interactions"""

    def __init__(self, service_url: str, api_key: str):
        self.service_url = service_url.rstrip("/")
        self.api_key = api_key
        self.headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}

    def register_subject(
        self,
        center_id: int,
        identifiers: List[Dict[str, str]],
        created_by: str = "fragment_validator",
    ) -> Dict:
        """
        Register a subject with one or more identifiers.

        Args:
            center_id: Research center ID
            identifiers: List of {"local_subject_id": "X", "identifier_type": "Y"}
            created_by: Source system name

        Returns:
            {
                "gsid": "GSID-XXX",
                "action": "create_new" | "link_existing" | "conflict_resolved",
                "identifiers_linked": int,
                "conflicts": [...] or None
            }
        """
        payload = {
            "center_id": center_id,
            "identifiers": identifiers,
            "created_by": created_by,
        }

        try:
            response = requests.post(
                f"{self.service_url}/register/subject",
                json=payload,
                headers=self.headers,
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()

            if result.get("conflicts"):
                logger.warning(
                    f"GSID conflict: {result['gsid']} (conflicts: {result['conflicts']})"
                )

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to register subject: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise
