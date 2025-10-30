import logging
import time
from typing import Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class REDCapClient:
    def __init__(self, project_config: dict):
        """Initialize REDCap client for a specific project"""
        self.project_config = project_config
        self.project_key = project_config.get("key")
        self.project_name = project_config.get("name")
        self.redcap_project_id = project_config.get("redcap_project_id")

        # Get API URL - use project-specific or fall back to global
        self.api_url = project_config.get("redcap_api_url")
        if not self.api_url:
            # Fall back to global REDCAP_API_URL from environment
            from core.config import settings

            self.api_url = settings.REDCAP_API_URL

        self.api_token = project_config.get("api_token")

        if not self.api_url:
            raise ValueError(f"Project {self.project_key}: redcap_api_url is required")
        if not self.api_token:
            raise ValueError(f"Project {self.project_key}: api_token is required")

        # Create session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,  # 2, 4, 8 seconds
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        logger.info(
            f"Initialized REDCap client for project: {self.project_name} "
            f"(REDCap ID: {self.redcap_project_id}, "
            f"Key: {self.project_key}, "
            f"Token: {self.api_token[:4]}...{self.api_token[-4:]})"
        )

    def fetch_records_batch(
        self, batch_size: int, offset: int, timeout: int = 120
    ) -> List[Dict]:
        """Fetch a batch of records from REDCap with pagination"""
        payload = {
            "token": self.api_token,
            "content": "record",
            "format": "json",
            "type": "flat",
            "rawOrLabel": "raw",
            "rawOrLabelHeaders": "raw",
            "exportCheckboxLabel": "false",
            "exportSurveyFields": "false",
            "exportDataAccessGroups": "true",  # Important for center resolution
            "returnFormat": "json",
        }

        max_retries = 3
        retry_delay = 5  # Start with 5 seconds

        for attempt in range(max_retries):
            try:
                logger.debug(
                    f"[{self.project_key}] Fetching records "
                    f"(batch_size={batch_size}, offset={offset}, timeout={timeout}s, "
                    f"attempt={attempt + 1}/{max_retries})"
                )

                response = self.session.post(
                    self.api_url, data=payload, timeout=timeout
                )
                response.raise_for_status()
                all_records = response.json()

                # Manual pagination (REDCap doesn't support offset/limit in API)
                paginated_records = all_records[offset : offset + batch_size]

                logger.info(
                    f"[{self.project_key}] Fetched {len(paginated_records)} records "
                    f"(offset={offset}, total={len(all_records)})"
                )

                return paginated_records

            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"[{self.project_key}] Request timed out "
                        f"(attempt {attempt + 1}/{max_retries}). "
                        f"Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    timeout = min(timeout * 1.5, 300)  # Increase timeout, max 5 min
                else:
                    logger.error(
                        f"[{self.project_key}] Failed to fetch records "
                        f"after {max_retries} attempts: {e}"
                    )
                    raise

            except requests.exceptions.RequestException as e:
                logger.error(f"[{self.project_key}] Failed to fetch records: {e}")
                raise

        return []

    def get_project_info(self) -> Dict:
        """Get project metadata"""
        payload = {
            "token": self.api_token,
            "content": "project",
            "format": "json",
            "returnFormat": "json",
        }

        try:
            response = self.session.post(self.api_url, data=payload, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch project info: {e}")
            raise

    def get_metadata(self) -> List[Dict]:
        """Get field metadata (data dictionary)"""
        payload = {
            "token": self.api_token,
            "content": "metadata",
            "format": "json",
            "returnFormat": "json",
        }

        try:
            response = self.session.post(self.api_url, data=payload, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch metadata: {e}")
            raise
