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
                    f"{self.service_url}/register/batch",
                    json={"requests": batch},
                    headers=self.headers,
                    timeout=timeout,
                )
                response.raise_for_status()
                batch_results = response.json()
                results.extend(batch_results)

                # Log progress: every 10 batches OR last batch
                if (batch_num + 1) % 10 == 0 or batch_num == total_batches - 1:
                    progress_pct = (end_idx / len(requests_list)) * 100
                    logger.info(
                        f"Batch {batch_num + 1}/{total_batches} complete: "
                        f"{end_idx}/{len(requests_list)} records ({progress_pct:.1f}%)"
                    )

            except Exception as e:
                logger.error(f"Batch {batch_num + 1}/{total_batches} failed: {e}")
                raise

        logger.info(f"✓ Completed all {total_batches} batches ({len(results)} records)")
        return results

    def register_single(self, request: Dict) -> Dict:
        """Register a single subject ID"""
        response = requests.post(
            f"{self.service_url}/register",
            json=request,
            headers=self.headers,
        )
        response.raise_for_status()
        return response.json()
