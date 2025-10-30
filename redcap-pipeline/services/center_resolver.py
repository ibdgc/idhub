import logging
from difflib import SequenceMatcher
from typing import Dict, Optional

from core.config import settings
from core.database import get_db_connection, return_db_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class CenterResolver:
    def __init__(self):
        self.center_cache: Dict[str, int] = {}
        self.center_names: Dict[int, str] = {}
        self._load_centers()

    def _load_centers(self):
        """Load centers from database into cache"""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT center_id, name FROM centers")
                for row in cursor.fetchall():
                    self.center_cache[row["name"].lower()] = row["center_id"]
                    self.center_names[row["center_id"]] = row["name"]
            logger.info(f"Loaded {len(self.center_cache)} centers into cache")
        except Exception as e:
            logger.error(f"Failed to load centers: {e}")
            raise
        finally:
            if conn:
                return_db_connection(conn)

    def normalize_center_name(self, raw_name: str) -> str:
        """Normalize center name using aliases"""
        if not raw_name:
            return "Unknown"

        normalized = raw_name.lower().strip().replace(" ", "_").replace("-", "_")

        # Check aliases
        if normalized in settings.CENTER_ALIASES:
            canonical = settings.CENTER_ALIASES[normalized]
            logger.info(f"Alias matched '{raw_name}' -> '{canonical}'")
            return canonical

        return raw_name

    def _fuzzy_match_center(
        self, input_name: str, threshold: float = 0.7
    ) -> Optional[int]:
        """Fuzzy match center name using string similarity"""
        if not input_name:
            return None

        input_normalized = input_name.lower().replace("_", "-").replace(" ", "-")
        best_match_id = None
        best_match_name = None
        best_score = 0.0

        for center_name_db, center_id in self.center_cache.items():
            center_normalized = center_name_db.replace("_", "-").replace(" ", "-")
            score = SequenceMatcher(None, input_normalized, center_normalized).ratio()

            if score > best_score:
                best_score = score
                best_match_id = center_id
                best_match_name = self.center_names[center_id]

        if best_score >= threshold:
            logger.info(
                f"Fuzzy matched '{input_name}' -> '{best_match_name}' "
                f"(score: {best_score:.2f})"
            )
            return best_match_id

        logger.warning(
            f"No fuzzy match found for '{input_name}' (best score: {best_score:.2f})"
        )
        return None

    def resolve_center_id(self, center_name: str, fuzzy: bool = True) -> Optional[int]:
        """Resolve center name to center_id with optional fuzzy matching"""
        normalized = self.normalize_center_name(center_name)

        # Try exact match
        center_id = self.center_cache.get(normalized.lower())
        if center_id:
            return center_id

        # Try fuzzy matching if enabled
        if fuzzy:
            return self._fuzzy_match_center(normalized, threshold=0.7)

        return None

    def get_or_create_center(self, center_name: str) -> int:
        """Get center_id with alias lookup, fuzzy matching, or create if no match"""
        # Try to resolve existing center
        center_id = self.resolve_center_id(center_name, fuzzy=True)
        if center_id:
            return center_id

        # No match - create new center
        normalized = self.normalize_center_name(center_name)
        logger.warning(f"Creating new center: '{normalized}'")

        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    INSERT INTO centers (name, investigator, country, consortium)
                    VALUES (%s, %s, %s, %s)
                    RETURNING center_id
                    """,
                    (normalized, "Unknown", "Unknown", "Unknown"),
                )
                result = cursor.fetchone()
                center_id = result["center_id"]

                # Update cache
                self.center_cache[normalized.lower()] = center_id
                self.center_names[center_id] = normalized

                conn.commit()
                logger.info(f"✓ Created new center: {normalized} (ID: {center_id})")
                return center_id

        except Exception as e:
            logger.error(f"Failed to create center '{normalized}': {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                return_db_connection(conn)

