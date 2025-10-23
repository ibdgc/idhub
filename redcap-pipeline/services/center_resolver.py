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
                    # Store both original and lowercase for matching
                    self.center_cache[row["name"].lower()] = row["center_id"]
        logger.info(f"Loaded {len(self.center_cache)} centers into cache")

    def normalize_center_name(self, raw_name: str) -> str:
        """Normalize center name using aliases"""
        if not raw_name:
            return "Unknown"

        normalized = raw_name.lower().strip().replace(" ", "_").replace("-", "_")

        # Check if there's an alias mapping
        if normalized in settings.CENTER_ALIASES:
            canonical = settings.CENTER_ALIASES[normalized]
            logger.info(f"Alias matched '{raw_name}' -> '{canonical}'")
            return canonical

        return raw_name

    def _fuzzy_match_center(
        self, input_name: str, threshold: float = 0.7
    ) -> Optional[int]:
        """
        Fuzzy match center name using string similarity
        Returns center_id if match found above threshold, None otherwise
        """
        if not input_name:
            return None

        # Normalize input
        input_normalized = input_name.lower().replace("_", "-").replace(" ", "-")

        best_match_name = None
        best_match_id = None
        best_score = 0.0

        for center_name_lower, center_id in self.center_cache.items():
            center_normalized = center_name_lower.replace("_", "-").replace(" ", "-")

            # Calculate similarity ratio
            score = SequenceMatcher(None, input_normalized, center_normalized).ratio()

            if score > best_score:
                best_score = score
                best_match_name = center_name_lower
                best_match_id = center_id

        if best_score >= threshold:
            logger.info(
                f"Fuzzy matched '{input_name}' -> '{best_match_name}' (score: {best_score:.2f})"
            )
            return best_match_id

        logger.warning(
            f"No fuzzy match found for '{input_name}' (best score: {best_score:.2f})"
        )
        return None

    def resolve_center_id(self, center_name: str, fuzzy: bool = True) -> Optional[int]:
        """
        Resolve center name to center_id with alias lookup and fuzzy matching

        Args:
            center_name: Raw center name from REDCap (e.g., redcap_data_access_group)
            fuzzy: Enable fuzzy matching if exact match fails

        Returns:
            center_id if found, None otherwise
        """
        if not center_name:
            logger.warning("Empty center name provided")
            return None

        # Step 1: Apply alias normalization
        normalized_name = self.normalize_center_name(center_name)

        # Step 2: Try exact match (case-insensitive)
        center_id = self.center_cache.get(normalized_name.lower())
        if center_id:
            logger.debug(f"Exact match: '{center_name}' -> center_id {center_id}")
            return center_id

        # Step 3: Try fuzzy matching if enabled
        if fuzzy:
            center_id = self._fuzzy_match_center(normalized_name, threshold=0.7)
            if center_id:
                return center_id

        # Step 4: No match found
        logger.warning(f"Could not resolve center: '{center_name}'")
        return None

    def get_or_create_center(self, center_name: str) -> int:
        """
        Get center_id or create new center if not found

        Args:
            center_name: Raw center name from REDCap

        Returns:
            center_id (always returns a valid ID)
        """
        # Try to resolve existing center
        center_id = self.resolve_center_id(center_name, fuzzy=True)
        if center_id:
            return center_id

        # Create new center
        normalized_name = self.normalize_center_name(center_name)
        logger.warning(f"Creating new center: '{normalized_name}'")

        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                cursor.execute(
                    """
                    INSERT INTO centers (name, investigator, country, consortium)
                    VALUES (%s, %s, %s, %s)
                    RETURNING center_id
                    """,
                    (normalized_name, "Unknown", "Unknown", "Unknown"),
                )
                result = cursor.fetchone()
                conn.commit()

                # Update cache
                self.center_cache[normalized_name.lower()] = result["center_id"]

                logger.info(
                    f"Created new center '{normalized_name}' with ID {result['center_id']}"
                )
                return result["center_id"]
