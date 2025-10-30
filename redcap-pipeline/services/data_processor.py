import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.database import get_db_connection

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, project_config: dict):
        """Initialize DataProcessor with project configuration"""
        self.project_config = project_config
        self.project_key = project_config.get("key")
        self.project_name = project_config.get("name")
        self.field_mappings = self.load_field_mappings()

    def load_field_mappings(self) -> Dict:
        """Load field mappings from configuration file"""
        import json
        from pathlib import Path

        mapping_file = self.project_config.get("field_mappings")
        if not mapping_file:
            logger.warning(f"[{self.project_key}] No field mappings configured")
            return {}

        mapping_path = Path(__file__).parent.parent / "config" / mapping_file

        if not mapping_path.exists():
            logger.warning(
                f"[{self.project_key}] Field mappings file not found: {mapping_path}"
            )
            return {}

        with open(mapping_path) as f:
            mappings = json.load(f)

        logger.info(f"[{self.project_key}] Loaded field mappings from {mapping_file}")
        return mappings

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
            conn = get_db_connection()
            try:
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
            finally:
                conn.close()

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
            # Insert samples into database
            if not self.insert_samples(gsid, samples):
                return False

            # Fragment creation and S3 upload is handled by the pipeline
            return True

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error processing record "
                f"{record.get('record_id', 'unknown')}: {e}"
            )
            return False
