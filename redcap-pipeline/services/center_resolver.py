import logging
from difflib import SequenceMatcher
from typing import Optional

import psycopg2.extras
from core.config import settings
from core.database import get_db_connection

logger = logging.getLogger(__name__)


class CenterResolver:
    def __init__(self):
        self.center_cache = {}
        self.alias_map = settings.CENTER_ALIASES
        self.fuzzy_threshold = settings.FUZZY_MATCH_THRESHOLD
        self._load_centers()

    def _load_centers(self):
        """Load all centers into memory cache"""
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("SELECT center_id, name FROM centers")
                centers = cursor.fetchall()
                for center in centers:
                    # Store both by ID and by normalized name
                    self.center_cache[center["center_id"]] = center["name"]
                    self.center_cache[center["name"].lower()] = center["center_id"]
            conn.close()
            logger.info(f"Loaded {len(centers)} centers into cache")
        except Exception as e:
            logger.error(f"Failed to load centers: {e}")
            raise

    def normalize_name(self, name: str) -> str:
        """Normalize center name for matching"""
        return name.lower().strip().replace("_", " ")

    def resolve_alias(self, center_name: str) -> Optional[str]:
        """Check if center name matches a known alias"""
        normalized = self.normalize_name(center_name)

        # Check exact alias match
        if normalized in {k.lower(): v for k, v in self.alias_map.items()}:
            canonical = self.alias_map.get(
                normalized, self.alias_map.get(center_name, None)
            )
            if canonical:
                logger.info(f"Alias matched '{center_name}' -> '{canonical}'")
                return canonical

        # Check if the alias map has the original case
        if center_name in self.alias_map:
            canonical = self.alias_map[center_name]
            logger.info(f"Alias matched '{center_name}' -> '{canonical}'")
            return canonical

        return None

    def fuzzy_match(self, center_name: str) -> Optional[str]:
        """Find best fuzzy match from existing centers"""
        best_match = None
        best_score = 0.0

        normalized_input = self.normalize_name(center_name)

        # Only compare against center names (strings, not IDs)
        center_names = [v for k, v in self.center_cache.items() if isinstance(v, str)]

        for existing_name in center_names:
            normalized_existing = self.normalize_name(existing_name)
            score = SequenceMatcher(None, normalized_input, normalized_existing).ratio()

            if score > best_score:
                best_score = score
                best_match = existing_name

        if best_score >= self.fuzzy_threshold:
            logger.info(
                f"Fuzzy matched '{center_name}' -> '{best_match}' (score: {best_score:.2f})"
            )
            return best_match
        else:
            logger.warning(
                f"No fuzzy match found for '{center_name}' (best score: {best_score:.2f})"
            )
            return None

    def get_or_create_center(self, center_name: str) -> int:
        """
        Resolve center name to center_id, creating new center if needed

        Resolution order:
        1. Check alias map
        2. Check exact match in cache
        3. Try fuzzy match (if threshold met)
        4. Create new center
        """
        # Step 1: Check alias map
        canonical_name = self.resolve_alias(center_name)
        if canonical_name:
            center_name = canonical_name

        # Step 2: Check exact match in cache
        normalized = self.normalize_name(center_name)
        if normalized in self.center_cache:
            return self.center_cache[normalized]

        # Also check original case
        if center_name.lower() in self.center_cache:
            return self.center_cache[center_name.lower()]

        # Step 3: Try fuzzy match
        fuzzy_match = self.fuzzy_match(center_name)
        if fuzzy_match:
            return self.center_cache[fuzzy_match.lower()]

        # Step 4: Create new center
        logger.warning(f"Creating new center: '{center_name}'")
        return self._create_center(center_name)

    def _create_center(self, center_name: str) -> int:
        """Create a new center in the database"""
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # Match your schema: name, investigator are required
                cursor.execute(
                    """
                    INSERT INTO centers (name, investigator, country, consortium)
                    VALUES (%s, %s, %s, %s)
                    RETURNING center_id
                    """,
                    (center_name, "Unknown", None, None),
                )
                center_id = cursor.fetchone()[0]
                conn.commit()

                # Update cache
                self.center_cache[center_id] = center_name
                self.center_cache[center_name.lower()] = center_id

                logger.info(f"✓ Created new center: {center_name} (ID: {center_id})")
                return center_id

        except psycopg2.errors.UniqueViolation as e:
            conn.rollback()
            logger.error(f"Failed to create center '{center_name}': {e}")

            # Reload cache in case another process created it
            self._load_centers()

            # Try one more time to find it
            normalized = self.normalize_name(center_name)
            if normalized in self.center_cache:
                logger.info(f"Found center in cache after reload: {center_name}")
                return self.center_cache[normalized]

            raise
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create center '{center_name}': {e}")
            raise
        finally:
            conn.close()
