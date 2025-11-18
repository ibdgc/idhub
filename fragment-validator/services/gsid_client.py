# fragment-validator/services/gsid_client.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class GSIDClient:
    """Client for GSID service API"""

    def __init__(self, service_url: str, api_key: str):
        self.service_url = service_url.rstrip("/")
        self.api_key = api_key
        self.headers = {"x-api-key": api_key, "Content-Type": "application/json"}

    def register_subject(
        self,
        center_id: int,
        identifiers: List[Dict[str, str]],
        registration_year: Optional[str] = None,
        control: bool = False,
        created_by: str = "system",
    ) -> Dict:
        """
        Register a subject with one or more identifiers.

        Args:
            center_id: Center ID
            identifiers: List of {"local_subject_id": str, "identifier_type": str}
            registration_year: Optional registration year
            control: Whether subject is a control
            created_by: Source system identifier

        Returns:
            {
                "gsid": str,
                "action": str,
                "identifiers_linked": int,
                "conflicts": Optional[List[str]],
                "conflict_resolution": Optional[str],
                "warnings": Optional[List[str]]
            }
        """
        payload = {
            "center_id": center_id,
            "identifiers": identifiers,
            "registration_year": registration_year,
            "control": control,
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

            # Log conflicts
            if result.get("conflicts"):
                logger.warning(
                    f"Multi-GSID conflict: {result['gsid']} "
                    f"(conflicts: {result['conflicts']})"
                )

            # Log warnings
            if result.get("warnings"):
                for warning in result["warnings"]:
                    logger.warning(f"GSID warning: {warning}")

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to register subject: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise

    def register_batch(
        self,
        requests_list: List[Dict],
        batch_size: int = 20,
        timeout: int = 120,
    ) -> List[Optional[Dict]]:
        """
        Register multiple subjects in parallel.

        Args:
            requests_list: List of registration request dicts
            batch_size: Number of parallel workers
            timeout: Timeout per request in seconds

        Returns:
            List of results (same order as input)
        """
        logger.info(
            f"Starting parallel registration of {len(requests_list)} subjects..."
        )
        logger.info(f"Parallel workers: {batch_size}")

        results = [None] * len(requests_list)
        stats = {"created": 0, "existing": 0, "conflicts": 0, "errors": 0}

        def register_one(idx: int, req: Dict) -> tuple:
            """Register single subject and return (index, result)"""
            try:
                result = self.register_subject(
                    center_id=req["center_id"],
                    identifiers=req["identifiers"],
                    created_by=req.get("created_by", "fragment_validator"),
                )
                return (idx, result)
            except Exception as e:
                logger.error(f"Request {idx} failed: {e}")
                return (idx, None)

        # Execute in parallel
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = {
                executor.submit(register_one, i, req): i
                for i, req in enumerate(requests_list)
            }

            completed = 0
            for future in as_completed(futures):
                idx, result = future.result()
                results[idx] = result
                completed += 1

                if result:
                    action = result.get("action", "unknown")
                    if action == "create_new":
                        stats["created"] += 1
                    elif action == "link_existing":
                        stats["existing"] += 1
                    if result.get("conflicts"):
                        stats["conflicts"] += 1
                else:
                    stats["errors"] += 1

                # Progress logging every 50 subjects
                if completed % 50 == 0 or completed == len(requests_list):
                    logger.info(
                        f"Progress: {completed}/{len(requests_list)} subjects processed "
                        f"(created={stats['created']}, existing={stats['existing']}, "
                        f"conflicts={stats['conflicts']}, errors={stats['errors']})"
                    )

        logger.info(
            f"✓ Parallel registration complete: {completed}/{len(requests_list)} subjects processed "
            f"(created={stats['created']}, existing={stats['existing']}, "
            f"conflicts={stats['conflicts']}, errors={stats['errors']})"
        )

        return results
