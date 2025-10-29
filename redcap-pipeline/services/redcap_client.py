import logging
import time
from typing import Dict, List

import requests
from core.config import ProjectConfig
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class REDCapClient:
    def __init__(self, project_config: ProjectConfig):
        self.project_config = project_config
        self.api_url = (
            project_config.redcap_api_url
        )  # Changed from api_url to redcap_api_url
        self.api_token = project_config.api_token

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
            f"Initialized REDCap client for project: {project_config.project_name} "
            f"(REDCap ID: {project_config.redcap_project_id}, "
            f"Key: {project_config.project_key}, "
            f"Token: {project_config.api_token[:4]}...{project_config.api_token[-4:]})"
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
                    f"[{self.project_config.project_key}] Fetching records "
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
                    f"[{self.project_config.project_key}] Fetched {len(paginated_records)} records "
                    f"(offset={offset}, total={len(all_records)})"
                )
                return paginated_records

            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"[{self.project_config.project_key}] Request timed out "
                        f"(attempt {attempt + 1}/{max_retries}). "
                        f"Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    timeout = min(timeout * 1.5, 300)  # Increase timeout, max 5 min
                else:
                    logger.error(
                        f"[{self.project_config.project_key}] Failed to fetch records "
                        f"after {max_retries} attempts: {e}"
                    )
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"[{self.project_config.project_key}] Failed to fetch records: {e}"
                )
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
