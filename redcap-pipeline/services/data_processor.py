import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.database import Database

from services.s3_client import S3Client

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(
        self,
        db: Database,
        s3_client: S3Client,
        project_key: str,
        project_config: Optional[Any] = None,
    ):
        self.db = db
        self.s3_client = s3_client
        self.project_key = project_key  # This will be "gap", "legacy_samples", etc.
        self.project_config = project_config

        # Load field mappings
        if project_config:
            self.field_mappings = project_config.load_field_mappings()
            self.project_name = project_config.name  # e.g., "GAP"
        else:
            from core.config import settings

            self.field_mappings = settings.load_field_mappings()
            self.project_name = "default"

    def insert_samples(self, gsid: str, samples: List[Dict[str, Any]]) -> bool:
        """Insert sample records into the database"""
        if not samples:
            logger.warning(f"[{self.project_key}] No samples to insert for {gsid}")
            return True

        try:
            query = """
                INSERT INTO specimen (
                    specimen_id, global_subject_id, sample_type, 
                    redcap_event, project, collection_date, 
                    storage_location, notes, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (specimen_id) 
                DO UPDATE SET
                    sample_type = EXCLUDED.sample_type,
                    redcap_event = EXCLUDED.redcap_event,
                    project = EXCLUDED.project,
                    collection_date = EXCLUDED.collection_date,
                    storage_location = EXCLUDED.storage_location,
                    notes = EXCLUDED.notes,
                    updated_at = EXCLUDED.updated_at
            """

            now = datetime.utcnow()

            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    for sample in samples:
                        cursor.execute(
                            query,
                            (
                                sample["specimen_id"],
                                gsid,
                                sample.get("sample_type"),
                                sample.get("redcap_event"),
                                self.project_name,  # Use project name from config (e.g., "GAP")
                                sample.get("collection_date"),
                                sample.get("storage_location"),
                                sample.get("notes"),
                                now,
                                now,
                            ),
                        )
                    conn.commit()

            logger.info(
                f"[{self.project_key}] Inserted {len(samples)} samples for GSID {gsid} (project: {self.project_name})"
            )
            return True

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error inserting samples for {gsid}: {e}"
            )
            return False

    def create_fragment(self, gsid: str, record: Dict[str, Any]) -> Optional[Dict]:
        """Create a curated data fragment from REDCap record"""
        try:
            fragment = {
                "gsid": gsid,
                "project_key": self.project_key,
                "project_name": self.project_name,
                "source": "redcap",
                "created_at": datetime.utcnow().isoformat(),
                "data": {},
            }

            # Map fields according to configuration
            for section, fields in self.field_mappings.items():
                fragment["data"][section] = {}
                for field_name, redcap_field in fields.items():
                    if redcap_field in record:
                        value = record[redcap_field]
                        # Only include non-empty values
                        if value not in [None, "", "NA", "N/A"]:
                            fragment["data"][section][field_name] = value

            return fragment

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error creating fragment for {gsid}: {e}"
            )
            return None

    def process_record(
        self, record: Dict[str, Any], gsid: str, samples: List[Dict[str, Any]]
    ) -> bool:
        """Process a single REDCap record"""
        try:
            # Insert samples
            if not self.insert_samples(gsid, samples):
                return False

            # Create and upload fragment
            fragment = self.create_fragment(gsid, record)
            if fragment:
                success = self.s3_client.upload_fragment(gsid, fragment)
                if not success:
                    logger.warning(
                        f"[{self.project_key}] Failed to upload fragment for {gsid}"
                    )
                    # Don't fail the whole record if S3 upload fails
                    return True

            return True

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error processing record "
                f"{record.get('record_id', 'unknown')}: {e}"
            )
            return False
