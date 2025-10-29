# redcap-pipeline/services/gsid_client.py
import logging
from typing import Any, Dict, List, Optional

import requests
from core.config import settings

logger = logging.getLogger(__name__)


class GSIDClient:
    def __init__(self):
        self.base_url = settings.GSID_SERVICE_URL
        self.api_key = settings.GSID_API_KEY
        self.session = requests.Session()
        self.session.headers.update(
            {"x-api-key": self.api_key, "Content-Type": "application/json"}
        )

    def register_subject(
        self,
        center_id: int,
        local_subject_id: str,
        identifier_type: str = "primary",
        registration_year: Optional[int] = None,
        control: bool = False,
        created_by: str = "redcap_pipeline",
    ) -> Dict[str, Any]:
        """Register subject with GSID service"""
        payload = {
            "center_id": center_id,
            "local_subject_id": local_subject_id,
            "identifier_type": identifier_type,
            "registration_year": registration_year,
            "control": control,
            "created_by": created_by,
        }

        try:
            response = self.session.post(
                f"{self.base_url}/register", json=payload, timeout=30
            )
            response.raise_for_status()
            result = response.json()
            logger.info(
                f"[{created_by}] Registered {local_subject_id} ({identifier_type}) -> "
                f"GSID {result['gsid']} ({result['action']})"
            )
            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"[{created_by}] GSID registration failed: {e}")
            raise

    def register_batch(self, subjects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Register multiple subjects in batch"""
        payload = {"requests": subjects}

        try:
            response = self.session.post(
                f"{self.base_url}/register/batch", json=payload, timeout=60
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Batch registration failed: {e}")
            raise

