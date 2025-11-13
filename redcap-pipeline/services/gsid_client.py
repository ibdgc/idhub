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
        """
        Register a single subject with GSID service

        Args:
            center_id: Research center ID
            local_subject_id: Local identifier (e.g., consortium_id, local_id)
            identifier_type: Type of identifier (e.g., "consortium_id", "local_id", "alias")
            registration_year: Registration date
            control: Whether this is a control subject
            created_by: Creator identifier

        Returns:
            Registration response with gsid, action, match_strategy, etc.
        """
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

        Each subject dict should contain:
        - center_id: int
        - local_subject_id: str
        - identifier_type: str (e.g., "consortium_id", "local_id", "alias")
        - registration_year: Optional[str] (ISO format)
        - control: bool

        Args:
            subjects: List of subject registration requests
            timeout: Request timeout in seconds

        Returns:
            List of registration responses

        Example:
            subjects = [
                {
                    "center_id": 2,
                    "local_subject_id": "IBDGC001",
                    "identifier_type": "consortium_id",
                    "registration_year": "2024-01-15",
                    "control": False
                },
                {
                    "center_id": 2,
                    "local_subject_id": "LOCAL-123",
                    "identifier_type": "local_id",
                    "registration_year": "2024-01-15",
                    "control": False
                }
            ]
            results = client.register_batch(subjects)
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

            # Count actions
            actions = {}
            for r in results:
                action = r.get("action", "unknown")
                actions[action] = actions.get(action, 0) + 1

            logger.info(
                f"Batch registration complete: {success_count} success, {error_count} errors"
            )
            logger.info(f"Actions: {actions}")

            return results

        except requests.exceptions.RequestException as e:
            logger.error(f"Batch registration failed: {e}")
            raise

    def register_subject_with_multiple_ids(
        self,
        center_id: int,
        subject_ids: List[Dict[str, str]],
        registration_year: Optional[date] = None,
        control: bool = False,
    ) -> Dict[str, Any]:
        """
        Register a single subject with multiple local IDs in one call

        This is a convenience method that calls register_batch with multiple
        entries for the same subject (same center, same metadata, different IDs).

        Args:
            center_id: Research center ID
            subject_ids: List of dicts with 'local_subject_id' and 'identifier_type'
                        e.g., [
                            {"local_subject_id": "IBDGC001", "identifier_type": "consortium_id"},
                            {"local_subject_id": "LOCAL-123", "identifier_type": "local_id"},
                            {"local_subject_id": "ALIAS-ABC", "identifier_type": "alias"}
                        ]
            registration_year: Registration date
            control: Control status

        Returns:
            Dict with:
            - gsid: The resolved GSID (should be same for all IDs)
            - results: List of individual registration results
            - conflicts: List of any GSID conflicts detected
        """
        if not subject_ids:
            raise ValueError("subject_ids cannot be empty")

        # Build batch request - one entry per ID
        batch_requests = []
        for id_info in subject_ids:
            batch_requests.append(
                {
                    "center_id": center_id,
                    "local_subject_id": id_info["local_subject_id"],
                    "identifier_type": id_info["identifier_type"],
                    "registration_year": registration_year.isoformat()
                    if registration_year
                    else None,
                    "control": control,
                }
            )

        # Register all IDs
        results = self.register_batch(batch_requests)

        # Check for GSID conflicts
        gsids = set(r.get("gsid") for r in results if r.get("gsid"))

        if len(gsids) > 1:
            logger.warning(
                f"GSID conflict detected! Multiple GSIDs returned for same subject: {gsids}"
            )
            return {
                "gsid": None,
                "results": results,
                "conflicts": list(gsids),
                "action": "review_required",
                "message": f"Multiple GSIDs detected: {gsids}. Manual review required.",
            }

        # All IDs resolved to same GSID (or all failed)
        gsid = gsids.pop() if gsids else None

        return {
            "gsid": gsid,
            "results": results,
            "conflicts": [],
            "action": results[0].get("action") if results else "error",
            "message": f"Successfully registered {len(subject_ids)} IDs for GSID {gsid}",
        }
