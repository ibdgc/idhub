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
        self.headers = {"x-api-key": self.api_key}

    def register_batch(
        self, requests_list: List[Dict], batch_size: int = 100, timeout: int = 60
    ) -> List[Dict]:
        """
        Register multiple subject IDs in batches

        Each request should contain:
        - center_id: int
        - local_subject_id: str
        - identifier_type: str
        - registration_year: Optional[str] (ISO format)
        - control: bool

        Args:
            requests_list: List of registration requests
            batch_size: Number of records per batch
            timeout: Request timeout in seconds

        Returns:
            List of registration responses
        """
        results = []
        total_batches = (len(requests_list) + batch_size - 1) // batch_size

        logger.info(
            f"Processing {len(requests_list)} records in {total_batches} batches "
            f"(batch_size={batch_size})"
        )

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(requests_list))
            batch = requests_list[start_idx:end_idx]

            try:
                response = requests.post(
                    f"{self.service_url}/register/batch",
                    json={"requests": batch},
                    headers=self.headers,
                    timeout=timeout,
                )
                response.raise_for_status()
                batch_results = response.json()
                results.extend(batch_results)

                logger.info(
                    f"Batch {batch_num + 1}/{total_batches}: "
                    f"Processed {len(batch_results)} records"
                )

            except requests.exceptions.RequestException as e:
                logger.error(f"Batch {batch_num + 1} failed: {e}")
                raise

        # Log summary
        actions = {}
        for r in results:
            action = r.get("action", "unknown")
            actions[action] = actions.get(action, 0) + 1

        logger.info(f"Batch registration complete. Actions: {actions}")
        return results
