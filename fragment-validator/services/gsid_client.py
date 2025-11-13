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

    def register_batch(
        self, requests_list: List[Dict], batch_size: int = 100, timeout: int = 60
    ) -> List[Dict]:
        """Register multiple subject IDs in batches"""
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
                    f"{self.service_url}/register/batch",  # ← Changed from /register/batch/multi-candidate
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

            except requests.exceptions.Timeout:
                logger.error(f"Batch {batch_num + 1} timed out after {timeout}s")
                raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Batch {batch_num + 1} failed: {e}")
                raise

        return results

    def get_subject(self, gsid: str) -> Dict:
        """Get subject details by GSID"""
        try:
            response = requests.get(
                f"{self.service_url}/subjects/{gsid}",
                headers=self.headers,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch subject {gsid}: {e}")
            raise

    def get_flagged_subjects(self, limit: int = 100) -> List[Dict]:
        """Get subjects flagged for review"""
        try:
            response = requests.get(
                f"{self.service_url}/review/flagged",
                params={"limit": limit},
                headers=self.headers,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch flagged subjects: {e}")
            raise
