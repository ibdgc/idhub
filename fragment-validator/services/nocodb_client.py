# fragment-validator/services/nocodb_client.py
import logging
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class NocoDBClient:
    """Client for NocoDB API interactions"""

    def __init__(self, url: str, token: str, base_id: Optional[str] = None):
        self.url = url.rstrip("/")
        self.token = token
        self._base_id_cache = base_id
        self._table_id_cache: Dict[str, str] = {}
        self.headers = {"xc-token": self.token}

    def _get_base_id(self) -> str:
        """Get base ID (auto-detect if not provided, cached after first call)"""
        if self._base_id_cache:
            return self._base_id_cache

        url = f"{self.url}/api/v2/meta/bases"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        bases = response.json().get("list", [])
        if not bases:
            raise ValueError("No NocoDB bases found")

        base_id = bases[0]["id"]
        base_title = bases[0].get("title", "Unknown")
        self._base_id_cache = base_id
        logger.info(f"Auto-detected NocoDB base: '{base_title}' (ID: {base_id})")
        return base_id

    def get_table_id(self, table_name: str) -> str:
        """Get table ID by name (cached after first lookup)"""
        if table_name in self._table_id_cache:
            return self._table_id_cache[table_name]

        base_id = self._get_base_id()
        url = f"{self.url}/api/v2/meta/bases/{base_id}/tables"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        tables = response.json().get("list", [])
        table = next((t for t in tables if t["table_name"] == table_name), None)

        if not table:
            raise ValueError(f"Table '{table_name}' not found in NocoDB base")

        table_id = table["id"]
        self._table_id_cache[table_name] = table_id
        logger.info(f"Found table '{table_name}' (ID: {table_id})")
        return table_id

    def get_table_metadata(self, table_name: str) -> dict:
        """Get full table metadata including columns"""
        table_id = self.get_table_id(table_name)
        url = f"{self.url}/api/v2/meta/tables/{table_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_all_records(self, table_name: str, limit: int = 1000) -> List[dict]:
        """Fetch all records from a table with pagination"""
        table_id = self.get_table_id(table_name)
        records_url = f"{self.url}/api/v2/tables/{table_id}/records"

        all_records = []
        offset = 0

        while True:
            response = requests.get(
                records_url,
                headers=self.headers,
                params={"limit": limit, "offset": offset},
            )
            response.raise_for_status()
            data = response.json()
            records = data.get("list", [])

            if not records:
                break

            all_records.extend(records)
            offset += limit

            page_info = data.get("pageInfo", {})
            if page_info.get("isLastPage", True):
                break

        return all_records

    def load_local_id_cache(self) -> Dict[tuple, str]:
        """Pre-load all local_subject_ids into memory for fast lookups"""
        logger.info("Loading local_subject_ids cache from NocoDB...")
        cache = {}

        try:
            records = self.get_all_records("local_subject_ids")

            for record in records:
                key = (
                    record["center_id"],
                    record["local_subject_id"],
                    record["identifier_type"],
                )
                cache[key] = record["global_subject_id"]

            logger.info(f"Loaded {len(cache)} unique local IDs into cache")
            return cache
        except Exception as e:
            logger.error(f"Failed to load local ID cache: {e}")
            raise
