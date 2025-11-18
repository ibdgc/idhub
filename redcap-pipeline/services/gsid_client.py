# redcap-pipeline/services/gsid_client.py
import logging
from datetime import date
from typing import Any, Dict, List, Optional

import requests
from core.config import settings

logger = logging.getLogger(__name__)


class GSIDClient:
    def __init__(self, service_url: str, api_key: str):
        self.base_url = service_url.rstrip("/")
        self.api_key = api_key

        # Create session with connection pooling for better performance
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=50,
            max_retries=requests.adapters.Retry(
                total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504]
            ),
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"X-API-Key": self.api_key})

        logger.info(
            f"GSID client initialized with connection pooling (base_url={self.base_url})"
        )

    def register_subject_with_identifiers(
        self,
        center_id: int,
        identifiers: List[Dict[str, str]],
        registration_year: Optional[date] = None,
        control: bool = False,
    ) -> Dict[str, Any]:
        """
        Register a subject with multiple identifiers using the unified endpoint.

        Args:
            center_id: Research center ID
            identifiers: List of {"local_subject_id": "X", "identifier_type": "Y"}
            registration_year: Optional registration year
            control: Whether subject is a control

        Returns:
            {
                "gsid": "GSID-XXX",
                "action": "create_new" | "link_existing" | "conflict_resolved",
                "identifiers_linked": int,
                "conflicts": [...] or None,
                "conflict_resolution": "used_oldest" or None
            }
        """
        payload = {
            "center_id": center_id,
            "identifiers": identifiers,
            "registration_year": registration_year.isoformat()
            if registration_year
            else None,
            "control": control,
            "created_by": "redcap_pipeline",
        }

        try:
            logger.debug(
                f"Registering subject with {len(identifiers)} identifier(s) "
                f"for center_id={center_id}"
            )

            response = self.session.post(
                f"{self.base_url}/register/subject", json=payload, timeout=30
            )
            response.raise_for_status()
            result = response.json()

            if result.get("conflicts"):
                logger.warning(
                    f"GSID conflict detected: {result['gsid']} "
                    f"(conflicts: {result['conflicts']})"
                )
            else:
                logger.info(
                    f"Subject registered: {result['gsid']} "
                    f"(action={result['action']}, identifiers={result['identifiers_linked']})"
                )

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to register subject: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise
