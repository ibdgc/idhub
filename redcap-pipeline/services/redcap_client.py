import logging
from typing import Any, Dict, List, Optional

import requests
from core.config import ProjectConfig, settings

logger = logging.getLogger(__name__)


class REDCapClient:
    def __init__(self, project_config: Optional[ProjectConfig] = None):
        """
        Initialize REDCap client

        Args:
            project_config: Project-specific configuration. If None, uses legacy env vars.
        """
        if project_config:
            self.api_url = settings.REDCAP_API_URL
            self.api_token = project_config.api_token
            self.project_id = project_config.redcap_project_id
            self.project_key = project_config.project_key
            self.project_name = project_config.project_name
        else:
            # Legacy mode
            self.api_url = settings.REDCAP_API_URL
            self.api_token = settings.REDCAP_API_TOKEN
            self.project_id = settings.REDCAP_PROJECT_ID
            self.project_key = "default"
            self.project_name = "Default Project"

        # Validate configuration
        if not self.api_url:
            raise ValueError("REDCAP_API_URL not configured")

        if not self.api_token:
            raise ValueError(f"API token not configured for project {self.project_key}")

        # Mask token for logging
        masked_token = (
            f"{self.api_token[:4]}...{self.api_token[-4:]}"
            if len(self.api_token) > 8
            else "***"
        )

        self.session = requests.Session()
        logger.info(
            f"Initialized REDCap client for project: {self.project_name} "
            f"(REDCap ID: {self.project_id}, Key: {self.project_key}, Token: {masked_token})"
        )

    def fetch_records_batch(
        self, batch_size: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch records in batches with pagination"""
        payload = {
            "token": self.api_token,
            "content": "record",
            "format": "json",
            "type": "flat",
            "rawOrLabel": "raw",
            "rawOrLabelHeaders": "raw",
            "exportCheckboxLabel": "false",
            "exportSurveyFields": "false",
            "exportDataAccessGroups": "true",
            "returnFormat": "json",
        }

        try:
            response = self.session.post(self.api_url, data=payload, timeout=60)

            # Log response details for debugging
            if response.status_code != 200:
                logger.error(
                    f"[{self.project_key}] REDCap API error: "
                    f"Status {response.status_code}, "
                    f"Response: {response.text[:500]}"
                )

            response.raise_for_status()
            all_records = response.json()

            # Apply pagination
            paginated = all_records[offset : offset + batch_size]

            logger.info(
                f"[{self.project_key}] Fetched {len(paginated)} records "
                f"(offset={offset}, total={len(all_records)})"
            )

            return paginated

        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.project_key}] Failed to fetch REDCap records: {e}")
            raise

    def fetch_all_records(self) -> List[Dict[str, Any]]:
        """Fetch all records from the project"""
        payload = {
            "token": self.api_token,
            "content": "record",
            "format": "json",
            "type": "flat",
            "rawOrLabel": "raw",
            "rawOrLabelHeaders": "raw",
            "exportCheckboxLabel": "false",
            "exportSurveyFields": "false",
            "exportDataAccessGroups": "true",
            "returnFormat": "json",
        }

        try:
            response = self.session.post(self.api_url, data=payload, timeout=60)

            if response.status_code != 200:
                logger.error(
                    f"[{self.project_key}] REDCap API error: "
                    f"Status {response.status_code}, "
                    f"Response: {response.text[:500]}"
                )

            response.raise_for_status()
            records = response.json()

            logger.info(f"[{self.project_key}] Fetched {len(records)} total records")
            return records

        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.project_key}] Failed to fetch REDCap records: {e}")
            raise

    def get_project_info(self) -> Dict[str, Any]:
        """Get project metadata from REDCap"""
        payload = {
            "token": self.api_token,
            "content": "project",
            "format": "json",
            "returnFormat": "json",
        }

        try:
            response = self.session.post(self.api_url, data=payload, timeout=30)

            if response.status_code != 200:
                logger.error(
                    f"[{self.project_key}] REDCap API error: "
                    f"Status {response.status_code}, "
                    f"Response: {response.text[:500]}"
                )

            response.raise_for_status()
            info = response.json()

            logger.info(
                f"[{self.project_key}] Project info: "
                f"REDCap ID={info.get('project_id')}, "
                f"Title={info.get('project_title')}"
            )

            return info

        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.project_key}] Failed to fetch project info: {e}")
            raise
