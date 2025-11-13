# redcap-pipeline/services/gsid_client.py
import logging
from datetime import date
from typing import Any, Dict, List, Optional

import requests
from core.config import settings

logger = logging.getLogger(__name__)


class GSIDClient:
    def __init__(self):
        self.base_url = settings.GSID_SERVICE_URL.rstrip("/")
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
        registration_year: Optional[date] = None,
        control: bool = False,
        created_by: str = "redcap_pipeline",
    ) -> Dict[str, Any]:
        """Register subject with GSID service"""
        payload = {
            "center_id": center_id,
            "local_subject_id": local_subject_id,
            "identifier_type": identifier_type,
            "registration_year": registration_year.isoformat()
            if registration_year
            else None,
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
                f"GSID {result.get('gsid')} ({result.get('action')}) "
                f"[strategy={result.get('match_strategy')}, confidence={result.get('confidence')}]"
            )

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"[{created_by}] GSID registration failed: {e}")
            raise

    def register_batch(
        self, subjects: List[Dict[str, Any]], timeout: int = 60
    ) -> List[Dict[str, Any]]:
        """
        Register multiple subjects in batch

        Args:
            subjects: List of subject registration requests
            timeout: Request timeout in seconds

        Returns:
            List of registration responses
        """
        payload = {"requests": subjects}

        try:
            response = self.session.post(
                f"{self.base_url}/register/batch", json=payload, timeout=timeout
            )
            response.raise_for_status()
            results = response.json()

            # Log summary
            success_count = sum(1 for r in results if r.get("action") != "error")
            error_count = len(results) - success_count

            logger.info(
                f"Batch registration complete: {success_count} success, {error_count} errors"
            )

            return results

        except requests.exceptions.RequestException as e:
            logger.error(f"Batch registration failed: {e}")
            raise

    def register_multi_candidate(
        self,
        center_id: int,
        candidate_ids: List[Dict[str, str]],
        registration_year: Optional[date] = None,
        control: bool = False,
        created_by: str = "redcap_pipeline",
    ) -> Dict[str, Any]:
        """
        Register subject with multiple candidate IDs

        Args:
            center_id: Center ID
            candidate_ids: List of dicts with 'local_subject_id' and 'identifier_type'
            registration_year: Registration year
            control: Control status
            created_by: Creator identifier

        Returns:
            Registration response
        """
        payload = {
            "center_id": center_id,
            "candidate_ids": candidate_ids,
            "registration_year": registration_year.isoformat()
            if registration_year
            else None,
            "control": control,
            "created_by": created_by,
        }

        try:
            response = self.session.post(
                f"{self.base_url}/register/multi-candidate", json=payload, timeout=30
            )
            response.raise_for_status()
            result = response.json()

            logger.info(
                f"[{created_by}] Multi-candidate registration: {len(candidate_ids)} IDs -> "
                f"GSID {result.get('gsid')} ({result.get('action')})"
            )

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"[{created_by}] Multi-candidate registration failed: {e}")
            raise

    def register_batch_multi_candidate(
        self, requests: List[Dict[str, Any]], timeout: int = 120
    ) -> List[Dict[str, Any]]:
        """
        Register multiple subjects with multiple candidate IDs in batch

        Args:
            requests: List of multi-candidate registration requests
            timeout: Request timeout in seconds

        Returns:
            List of registration responses
        """
        payload = {"requests": requests}

        try:
            response = self.session.post(
                f"{self.base_url}/register/batch/multi-candidate",
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
            results = response.json()

            # Log summary
            success_count = sum(1 for r in results if r.get("action") != "error")
            error_count = len(results) - success_count

            logger.info(
                f"Batch multi-candidate registration complete: "
                f"{success_count} success, {error_count} errors"
            )

            return results

        except requests.exceptions.RequestException as e:
            logger.error(f"Batch multi-candidate registration failed: {e}")
            raise

    def update_subject_center(self, gsid: str, new_center_id: int) -> Dict[str, Any]:
        """
        Update center_id for an existing GSID

        Note: This endpoint may not exist in current GSID service.
        This is a placeholder for future implementation.
        """
        url = f"{self.base_url}/subjects/{gsid}/center"

        try:
            response = self.session.patch(
                url,
                json={"center_id": new_center_id},
                timeout=30,
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update center for {gsid}: {e}")
            raise
