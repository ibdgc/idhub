# redcap-pipeline/services/data_processor.py
import logging
from typing import Any, Dict, Optional

import psycopg2
from core.database import get_db_connection, get_db_cursor

from services.center_resolver import CenterResolver
from services.gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, center_resolver: CenterResolver, gsid_client: GSIDClient):
        self.center_resolver = center_resolver
        self.gsid_client = gsid_client

    def process_records(self, records: list[Dict[str, Any]]) -> None:
        """Process REDCap records and insert into database"""
        with get_db_connection() as conn:
            with get_db_cursor(conn) as cursor:
                for record in records:
                    try:
                        self._process_record(cursor, record)
                        conn.commit()  # Commit after each record
                    except Exception as e:
                        conn.rollback()  # Rollback on error
                        logger.error(
                            f"Error processing record {record.get('record_id')}: {e}"
                        )
                        raise

    def _process_record(self, cursor, record: Dict[str, Any]) -> None:
        """Process a single record"""
        # Resolve center
        center_name = record.get("redcap_data_access_group", "")
        center_id = self.center_resolver.resolve_center(center_name)

        if not center_id:
            logger.warning(f"Skipping record - no center: {record.get('record_id')}")
            return

        # Get or create subject
        global_subject_id = self._get_or_create_subject(cursor, record, center_id)

        logger.info(
            f"Processed record {record.get('record_id')} -> GSID: {global_subject_id}"
        )

    def _get_or_create_subject(
        self, cursor, record: Dict[str, Any], center_id: int
    ) -> str:
        """Get existing subject or create new one with GSID"""
        record_id = record.get("record_id")

        # Check if subject already exists via local_subject_ids
        cursor.execute(
            """
            SELECT global_subject_id 
            FROM local_subject_ids 
            WHERE center_id = %s AND local_subject_id = %s AND identifier_type = 'primary'
            """,
            (center_id, record_id),
        )
        result = cursor.fetchone()

        if result:
            logger.info(f"Found existing subject: {result['global_subject_id']}")
            return result["global_subject_id"]

        # Subject doesn't exist - generate new GSID
        logger.info(f"Creating new subject for record {record_id}")
        gsids = self.gsid_client.generate_gsids(1)
        gsid = gsids[0]

        # Update the reserved GSID with actual subject data
        cursor.execute(
            """
            UPDATE subjects
            SET center_id = %s, updated_at = NOW()
            WHERE global_subject_id = %s
            RETURNING global_subject_id
            """,
            (center_id, gsid),
        )
        result = cursor.fetchone()

        if not result:
            # This shouldn't happen - the GSID service should have reserved it
            logger.error(f"GSID {gsid} was not found in database after reservation")
            raise Exception(f"GSID {gsid} not found - reservation failed")

        # Create local_subject_id mapping
        cursor.execute(
            """
            INSERT INTO local_subject_ids 
            (center_id, local_subject_id, identifier_type, global_subject_id)
            VALUES (%s, %s, 'primary', %s)
            ON CONFLICT (center_id, local_subject_id, identifier_type) DO NOTHING
            """,
            (center_id, record_id, gsid),
        )

        logger.info(f"Created new subject {gsid} for record {record_id}")
        return gsid
