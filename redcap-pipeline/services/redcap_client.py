import logging
from typing import Any, Dict, List, Optional

import requests
from core.config import ProjectConfig

logger = logging.getLogger(__name__)


class REDCapClient:
    def __init__(self, project_config: ProjectConfig):
        self.project_config = project_config
        self.api_url = project_config.redcap_api_url
        self.api_token = project_config.api_token
        self.project_id = project_config.redcap_project_id

        # Mask token for logging
        masked_token = (
            f"{self.api_token[:4]}...{self.api_token[-4:]}"
            if len(self.api_token) >= 8
            else "****"
        )

        logger.info(
            f"Initialized REDCap client for project: {project_config.project_name} "
            f"(REDCap ID: {self.project_id}, Key: {project_config.project_key}, "
            f"Token: {masked_token})"
        )

    def fetch_records_batch(
        self, batch_size: int, offset: int = 0
    ) -> List[Dict[str, Any]]:
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
            "exportDataAccessGroups": "false",
            "returnFormat": "json",
        }

        try:
            response = requests.post(self.api_url, data=payload, timeout=30)
            response.raise_for_status()
            all_records = response.json()

            # Apply pagination
            paginated_records = all_records[offset : offset + batch_size]

            logger.info(
                f"[{self.project_config.project_key}] Fetched {len(paginated_records)} records "
                f"(offset={offset}, total={len(all_records)})"
            )

            return paginated_records

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch records from REDCap: {e}")
            raise

    def fetch_metadata(self) -> List[Dict[str, Any]]:
        """Fetch project metadata (data dictionary)"""
        payload = {
            "token": self.api_token,
            "content": "metadata",
            "format": "json",
            "returnFormat": "json",
        }

        try:
            response = requests.post(self.api_url, data=payload, timeout=30)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch metadata from REDCap: {e}")
            raise
