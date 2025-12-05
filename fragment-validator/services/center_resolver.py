# fragment-validator/services/center_resolver.py
import logging
from difflib import SequenceMatcher
from typing import Optional

from core.config import settings
from .nocodb_client import NocoDBClient

logger = logging.getLogger(__name__)


class CenterResolver:
    def __init__(self, nocodb_client: NocoDBClient):
        self.nocodb_client = nocodb_client
        self.center_cache = {}
        self.alias_map = settings.CENTER_ALIASES
        self.fuzzy_threshold = settings.FUZZY_MATCH_THRESHOLD
        self._load_centers()

    # ──────────────────────────────────────────────────────────────────────
    # Cache helpers
    # ──────────────────────────────────────────────────────────────────────
    def _load_centers(self):
        """Load all centers into in-memory cache from NocoDB."""
        try:
            centers = self.nocodb_client.get_all_records("centers")

            for center in centers:
                # store both by ID and by normalised name
                self.center_cache[center["center_id"]] = center["name"]
                self.center_cache[center["name"].lower()] = center["center_id"]

            logger.info(f"Loaded {len(centers)} centers into cache")
        except Exception as e:
            logger.error(f"Failed to load centers from NocoDB: {e}")
            raise

    # ──────────────────────────────────────────────────────────────────────
    # Normalisation / matching utilities
    # ──────────────────────────────────────────────────────────────────────
    def normalize_name(self, name: str) -> str:
        return name.lower().strip().replace("_", " ")

    def resolve_alias(self, center_name: str) -> Optional[str]:
        """Return canonical name if `center_name` is an alias."""
        normalized = self.normalize_name(center_name)
        alias_map_norm = {k.lower(): v for k, v in self.alias_map.items()}

        if normalized in alias_map_norm:
            canonical = alias_map_norm[normalized]
            logger.info(f"Alias matched '{center_name}' -> '{canonical}'")
            return canonical

        if center_name in self.alias_map:  # original case
            canonical = self.alias_map[center_name]
            logger.info(f"Alias matched '{center_name}' -> '{canonical}'")
            return canonical

        return None

    def fuzzy_match(self, center_name: str) -> Optional[str]:
        """Return best fuzzy match above threshold (or None)."""
        best_match = None
        best_score = 0.0
        normalized_input = self.normalize_name(center_name)

        center_names = [v for v in self.center_cache.values() if isinstance(v, str)]
        for existing in center_names:
            score = SequenceMatcher(
                None, normalized_input, self.normalize_name(existing)
            ).ratio()
            if score > best_score:
                best_score = score
                best_match = existing

        if best_score >= self.fuzzy_threshold:
            logger.info(
                f"Fuzzy matched '{center_name}' -> '{best_match}' (score: {best_score:.2f})"
            )
            return best_match

        logger.warning(
            f"No fuzzy match found for '{center_name}' (best score: {best_score:.2f})"
        )
        return None

    # ──────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────
    def resolve_center(self, center_name: str) -> int:
        """
        Resolve a free-form center name to center_id.
        Resolution order:
            1. alias map
            2. exact match
            3. fuzzy match
        """
        # 1) alias
        canonical = self.resolve_alias(center_name)
        if canonical:
            center_name = canonical

        # 2) exact match
        normalized = self.normalize_name(center_name)
        if normalized in self.center_cache:
            return self.center_cache[normalized]
        if center_name.lower() in self.center_cache:
            return self.center_cache[center_name.lower()]

        # 3) fuzzy
        fuzzy = self.fuzzy_match(center_name)
        if fuzzy:
            return self.center_cache[fuzzy.lower()]

        raise ValueError(f"Could not resolve center name: '{center_name}'")

