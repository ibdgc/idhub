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
            global_subject_id = self._get_or_create_subject(record, center_id)

            # Process specimens
            self._process_specimens(record, global_subject_id)

    def _get_or_create_subject(self, record: Dict[str, Any], center_id: int) -> str:
        """Get existing subject or create new one"""
        record_id = record.get("record_id")

        with db_manager.get_connection() as conn:
            with db_manager.get_cursor(conn) as cursor:
                # Check if subject exists via local_subject_ids table
                cursor.execute(
                    """
                    SELECT global_subject_id 
                    FROM local_subject_ids 
                    WHERE center_id = %s AND local_subject_id = %s
                    """,
                    (center_id, record_id),
                )
                result = cursor.fetchone()

                if result:
                    return result["global_subject_id"]

                # Generate new GSID
                gsids = self.gsid_client.generate_gsids(1)
                global_subject_id = gsids[0]

                # Create new subject
                cursor.execute(
                    """
                    INSERT INTO subjects (global_subject_id, center_id, control)
                    VALUES (%s, %s, %s)
                    """,
                    (global_subject_id, center_id, record.get("control", False)),
                )

                # Create local_subject_id mapping
                cursor.execute(
                    """
                    INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (center_id, record_id, "primary", global_subject_id),
                )

                conn.commit()
                logger.info(
                    f"Created new subject {global_subject_id} for record {record_id}"
                )
                return global_subject_id

    def _process_specimens(self, record: Dict[str, Any], global_subject_id: str):
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
                        "SELECT 1 FROM specimen WHERE global_subject_id = %s AND sample_id = %s",
                        (global_subject_id, sample_id),
                    )

                    if not cursor.fetchone():
                        cursor.execute(
                            """
                            INSERT INTO specimen (global_subject_id, sample_id, sample_type, created_at)
                            VALUES (%s, %s, %s, NOW())
                            """,
                            (global_subject_id, sample_id, sample_type),
                        )
                        logger.debug(
                            f"Created specimen {sample_id} for subject {global_subject_id}"
                        )
