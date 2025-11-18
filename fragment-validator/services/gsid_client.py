# fragment-validator/services/gsid_client.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import requests

logger = logging.getLogger(__name__)


class GSIDClient:
    """Client for GSID service API with optimized batch processing"""

    def __init__(self, service_url: str, api_key: str):
        self.base_url = service_url.rstrip("/")
        self.api_key = api_key
        self.headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
        }

        # Create session with connection pooling
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=30,
            max_retries=requests.adapters.Retry(
                total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504]
            ),
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        logger.info(f"GSID client initialized (base_url={self.base_url})")

    def register_subject(
        self,
        identifiers: List[Dict[str, str]],
        center_id: int,
        created_by: str = "fragment_validator",
    ) -> Dict:
        """
        Register a subject with the GSID service.

        Args:
            identifiers: List of dicts with keys:
                - local_subject_id: The identifier value
                - identifier_type: Type (e.g., "primary", "consortium_id", "alternate")
            center_id: Center ID
            created_by: Source identifier

        Returns:
            Registration result with gsid, action, etc.
        """
        payload = {
            "identifiers": identifiers,
            "center_id": center_id,
            "created_by": created_by,
        }

        try:
            response = self.session.post(
                f"{self.base_url}/register/subject",
                json=payload,
                headers=self.headers,
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()

            if result.get("conflicts"):
                logger.debug(
                    f"GSID conflict: {result['gsid']} (conflicts: {result['conflicts']})"
                )

            return result

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to register subject {identifiers[0]['local_subject_id']}: {e}"
            )
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise

    def _register_single_request(self, request_data: Dict, index: int) -> tuple:
        """
        Register a single subject (for parallel execution).

        Returns: (index, result_dict)
        """
        try:
            # Build identifiers list with proper structure
            identifiers = [
                {
                    "local_subject_id": request_data["local_subject_id"],
                    "identifier_type": request_data.get("primary_type", "primary"),
                }
            ]

            # Add alternate identifiers
            if request_data.get("alternate_ids"):
                for alt_id in request_data["alternate_ids"]:
                    identifiers.append(
                        {
                            "local_subject_id": alt_id,
                            "identifier_type": "alternate",
                        }
                    )

            result = self.register_subject(
                identifiers=identifiers,
                center_id=request_data["center_id"],
                created_by=request_data.get("created_by", "fragment_validator"),
            )
            return (index, result)
        except Exception as e:
            logger.error(f"Failed to register subject at index {index}: {e}")
            raise

    def register_batch(
        self,
        requests_list: List[Dict],
        batch_size: int = 50,
        timeout: int = 120,
    ) -> List[Dict]:
        """
        Register multiple subjects using parallel requests to /register/subject.

        Since there's no /register/batch endpoint, we use ThreadPoolExecutor
        to make parallel calls to /register/subject for better performance.

        Args:
            requests_list: List of dicts with keys:
                - local_subject_id (required)
                - center_id (required)
                - primary_type (optional, default "primary")
                - alternate_ids (optional list)
                - created_by (optional)
            batch_size: Number of parallel requests (default 50)
            timeout: Not used (kept for API compatibility)

        Returns:
            List of results matching input order
        """
        if not requests_list:
            return []

        total_requests = len(requests_list)
        logger.info(f"Starting parallel registration of {total_requests} subjects...")
        logger.info(f"Parallel workers: {batch_size}")

        # Results array (preserve order)
        results = [None] * total_requests

        # Statistics
        stats = {
            "created": 0,
            "existing": 0,
            "conflicts": 0,
            "errors": 0,
        }

        # Process in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(self._register_single_request, req, i): i
                for i, req in enumerate(requests_list)
            }

            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_index):
                completed += 1
                index = future_to_index[future]

                try:
                    idx, result = future.result()
                    results[idx] = result

                    # Update statistics
                    action = result.get("action", "unknown")
                    if action == "create_new":
                        stats["created"] += 1
                    elif action == "link_existing":
                        stats["existing"] += 1

                    if result.get("conflicts"):
                        stats["conflicts"] += 1

                    # Log progress every 500 subjects
                    if completed % 500 == 0 or completed == total_requests:
                        logger.info(
                            f"Progress: {completed}/{total_requests} subjects processed "
                            f"(created={stats['created']}, existing={stats['existing']}, "
                            f"conflicts={stats['conflicts']}, errors={stats['errors']})"
                        )

                except Exception as e:
                    stats["errors"] += 1
                    # Only log first 10 errors to avoid spam
                    if stats["errors"] <= 10:
                        logger.error(f"Failed to process subject at index {index}: {e}")
                    elif stats["errors"] == 11:
                        logger.error(
                            "Suppressing further error messages (too many failures)..."
                        )
                    # Continue processing other subjects

        # Check for any failed registrations
        failed_count = sum(1 for r in results if r is None)
        if failed_count > 0:
            logger.warning(f"⚠ {failed_count} subjects failed to register")

        logger.info(
            f"✓ Parallel registration complete: "
            f"{total_requests - failed_count}/{total_requests} subjects processed "
            f"(created={stats['created']}, existing={stats['existing']}, "
            f"conflicts={stats['conflicts']}, errors={stats['errors']})"
        )

        return results

    def close(self):
        """Close the session"""
        self.session.close()
