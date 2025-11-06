import logging
from datetime import datetime
from typing import Dict, List, Optional

import requests
from core.config import settings

logger = logging.getLogger(__name__)


class LabKeyClient:
    """Client for LabKey API interactions"""

    def __init__(self):
        self.base_url = f"https://{settings.LABKEY_HOST}"
        self.project = settings.LABKEY_PROJECT
        self.schema = settings.LABKEY_SCHEMA
        self.headers = {
            "Authorization": f"Bearer {settings.LABKEY_API_KEY}",
            "Content-Type": "application/json",
        }

    def get_sample_info(self, sample_ids: List[str]) -> Dict[str, Dict]:
        """
        Query LabKey for sample status and date information

        Args:
            sample_ids: List of sample IDs to query

        Returns:
            Dict mapping sample_id to {status, date}
        """
        if not sample_ids:
            return {}

        # Build LabKey SQL query
        sample_id_list = "', '".join(sample_ids)
        sql = f"""
        SELECT 
            sample_id,
            status,
            date
        FROM {self.schema}.samples
        WHERE sample_id IN ('{sample_id_list}')
        """

        endpoint = f"{self.base_url}/query/{self.project}/executeSql.api"
        payload = {"schemaName": self.schema, "sql": sql}

        try:
            response = requests.post(
                endpoint, headers=self.headers, json=payload, timeout=30
            )
            response.raise_for_status()

            data = response.json()
            rows = data.get("rows", [])

            # Convert to dict keyed by sample_id
            result = {}
            for row in rows:
                sample_id = row.get("sample_id")
                if sample_id:
                    result[sample_id] = {
                        "status": row.get("status"),
                        "date": self._parse_date(row.get("date")),
                    }

            logger.info(f"Retrieved LabKey data for {len(result)} samples")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"LabKey API error: {e}")
            raise

    def _parse_date(self, date_value) -> Optional[datetime]:
        """Parse date from LabKey response"""
        if not date_value:
            return None

        try:
            # Handle various date formats
            if isinstance(date_value, str):
                # Try ISO format first
                return datetime.fromisoformat(date_value.replace("Z", "+00:00"))
            elif isinstance(date_value, (int, float)):
                # Unix timestamp
                return datetime.fromtimestamp(date_value / 1000)
            return None
        except Exception as e:
            logger.warning(f"Could not parse date '{date_value}': {e}")
            return None
