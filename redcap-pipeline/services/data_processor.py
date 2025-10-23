import logging
from typing import Any, Dict, List

from core.config import settings
from core.database import db_manager

from .center_resolver import CenterResolver
from .gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, center_resolver: CenterResolver, gsid_client: GSIDClient):
        self.center_resolver = center_resolver
        self.gsid_client = gsid_client
        self.field_mappings = settings.load_field_mappings()

    def process_records(self, records: List[Dict[str, Any]]):
        """Process REDCap records"""
        for record in records:
            # Get center - use get_or_create_center to match original behavior
            center_name = record.get("redcap_data_access_group")
            center_id = self.center_resolver.get_or_create_center(
                center_name or "Unknown"
            )

            if not center_id:
                logger.warning(
                    f"Skipping record - no center: {record.get('record_id')}"
                )
                continue

            # Check if subject exists
            subject_id = self._get_or_create_subject(record, center_id)

            # Process specimens
            self._process_specimens(record, subject_id)

    def _get_or_create_subject(self, record: Dict[str, Any], center_id: int) -> int:
        """Get existing subject or create new one"""
        record_id = record.get("record_id")

        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                # Check if subject exists
                cursor.execute(
                    "SELECT subject_id FROM subjects WHERE center_id = %s AND local_id = %s",
                    (center_id, record_id),
                )
                result = cursor.fetchone()

                if result:
                    return result["subject_id"]

                # Create new subject
                cursor.execute(
                    """
                    INSERT INTO subjects (center_id, local_id, control)
                    VALUES (%s, %s, %s)
                    RETURNING subject_id
                    """,
                    (center_id, record_id, record.get("control", False)),
                )
                result = cursor.fetchone()
                conn.commit()
                return result["subject_id"]

    def _process_specimens(self, record: Dict[str, Any], subject_id: int):
        """Process specimen data from record"""
        specimen_mappings = [
            m
            for m in self.field_mappings["mappings"]
            if m.get("target_table") == "specimen"
        ]

        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                for mapping in specimen_mappings:
                    source_field = mapping["source_field"]
                    sample_id = record.get(source_field)

                    if not sample_id:
                        continue

                    sample_type = mapping.get("sample_type", "unknown")

                    # Check if specimen exists
                    cursor.execute(
                        "SELECT 1 FROM specimen WHERE subject_id = %s AND sample_id = %s",
                        (subject_id, sample_id),
                    )

                    if not cursor.fetchone():
                        cursor.execute(
                            """
                            INSERT INTO specimen (subject_id, sample_id, sample_type, created_at)
                            VALUES (%s, %s, %s, NOW())
                            """,
                            (subject_id, sample_id, sample_type),
                        )
                        logger.debug(
                            f"Created specimen {sample_id} for subject {subject_id}"
                        )
