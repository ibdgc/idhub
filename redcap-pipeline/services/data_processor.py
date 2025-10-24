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
                    except Exception as e:
                        logger.error(
                            f"Error processing record {record.get('record_id')}: {e}"
                        )
                        raise

    def _process_record(self, cursor, record: Dict[str, Any]) -> None:
        """Process a single record"""
        # Resolve center
        center_name = record.get("redcap_data_access_group", "")
        center_id = self.center_resolver.resolve_center(center_name)

        # Get or create subject
        global_subject_id = self._get_or_create_subject(cursor, record, center_id)

        logger.info(
            f"Processed record {record.get('record_id')} -> GSID: {global_subject_id}"
        )

    def _get_or_create_subject(
        self, cursor, record: Dict[str, Any], center_id: int
    ) -> str:
        """Get existing subject or create new one with GSID"""
        consortium_id = record.get("consortium_id")
        local_id = record.get("local_patient_id")

        # Try to find existing subject by identifiers
        if consortium_id:
            cursor.execute(
                "SELECT global_subject_id FROM subjects WHERE consortium_id = %s",
                (consortium_id,),
            )
            result = cursor.fetchone()
            if result:
                return result["global_subject_id"]

        if local_id and center_id:
            cursor.execute(
                "SELECT global_subject_id FROM subjects WHERE local_id = %s AND center_id = %s",
                (local_id, center_id),
            )
            result = cursor.fetchone()
            if result:
                return result["global_subject_id"]

        # Subject doesn't exist, generate new GSID
        gsids = self.gsid_client.generate_gsids(1)
        gsid = gsids[0]

        # Update the reserved GSID record with actual subject data
        cursor.execute(
            """
            UPDATE subjects
            SET 
                center_id = %s,
                consortium_id = %s,
                local_id = %s,
                updated_at = NOW()
            WHERE global_subject_id = %s
            RETURNING global_subject_id
            """,
            (center_id, consortium_id, local_id, gsid),
        )
        result = cursor.fetchone()

        if result:
            return result["global_subject_id"]
        else:
            # Fallback: if UPDATE didn't find the record, INSERT it
            # This shouldn't happen but handles edge cases
            cursor.execute(
                """
                INSERT INTO subjects (
                    global_subject_id, 
                    center_id, 
                    consortium_id, 
                    local_id,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                RETURNING global_subject_id
                """,
                (gsid, center_id, consortium_id, local_id),
            )
            result = cursor.fetchone()
            return result["global_subject_id"]
