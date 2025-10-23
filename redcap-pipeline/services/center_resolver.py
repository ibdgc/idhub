# redcap-pipeline/services/center_resolver.py
import logging
from difflib import SequenceMatcher
from typing import Dict, Optional

from core.config import settings
from core.database import db_manager

logger = logging.getLogger(__name__)


class CenterResolver:
    def __init__(self):
        self.center_cache: Dict[str, int] = {}
        self._load_centers()

    def _load_centers(self):
        """Load centers from database into cache"""
        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                cursor.execute("SELECT center_id, name FROM centers")
                for row in cursor.fetchall():
                    self.center_cache[row["name"].lower()] = row["center_id"]
        logger.info(f"Loaded {len(self.center_cache)} centers into cache")

    def normalize_center_name(self, raw_name: str) -> str:
        """Normalize center name using aliases"""
        normalized = raw_name.lower().strip().replace(" ", "_")
        return settings.CENTER_ALIASES.get(normalized, raw_name)

    def resolve_center_id(self, center_name: str, fuzzy: bool = True) -> Optional[int]:
        """Resolve center name to center_id"""
        if not center_name:
            return None

        normalized = self.normalize_center_name(center_name)
        lookup_key = normalized.lower()

        # Exact match
        if lookup_key in self.center_cache:
            return self.center_cache[lookup_key]

        # Fuzzy match
        if fuzzy:
            best_match = None
            best_ratio = 0.0

            for cached_name, center_id in self.center_cache.items():
                ratio = SequenceMatcher(None, lookup_key, cached_name).ratio()
                if ratio > best_ratio and ratio > 0.8:
                    best_ratio = ratio
                    best_match = center_id

            if best_match:
                logger.info(
                    f"Fuzzy matched '{center_name}' with ratio {best_ratio:.2f}"
                )
                return best_match

        logger.warning(f"Could not resolve center: {center_name}")
        return None
