import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.database import get_db_connection, return_db_connection

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
            return {"mappings": [], "transformations": {}}

        mapping_path = Path(__file__).parent.parent / "config" / mapping_file
        if not mapping_path.exists():
            logger.warning(
                f"[{self.project_key}] Field mappings file not found: {mapping_path}"
            )
            return {"mappings": [], "transformations": {}}

        with open(mapping_path) as f:
            mappings = json.load(f)

        logger.info(f"[{self.project_key}] Loaded field mappings from {mapping_file}")
        return mappings

    def extract_specimens_from_record(
        self, record: Dict[str, Any], gsid: str
    ) -> List[Dict[str, Any]]:
        """Extract specimen records from REDCap record using field mappings"""
        specimens = []
        mappings = self.field_mappings.get("mappings", [])

        # Get specimen mappings only
        specimen_mappings = [m for m in mappings if m.get("target_table") == "specimen"]

        for mapping in specimen_mappings:
            source_field = mapping.get("source_field")
            sample_type = mapping.get("sample_type", "unknown")

            # Get value from record
            value = record.get(source_field)

            # Only include non-empty values
            if value and value not in ["", "NA", "N/A", "null"]:
                specimen = {
                    "sample_id": str(value).strip(),
                    "global_subject_id": gsid,
                    "sample_type": sample_type,
                    "redcap_event": record.get("redcap_event_name", ""),
                    "project": self.project_name,
                    "collection_date": None,  # Could be mapped if available
                    "storage_location": None,  # Could be mapped if available
                    "notes": f"Source field: {source_field}",
                }
                specimens.append(specimen)

        logger.info(
            f"[{self.project_key}] Extracted {len(specimens)} specimens from record"
        )
        return specimens

    def insert_samples(self, specimens: List[Dict[str, Any]]) -> bool:
        """Insert specimen records into the database"""
        if not specimens:
            logger.info(f"[{self.project_key}] No specimens to insert")
            return True

        conn = None
        try:
            query = """
                INSERT INTO specimen (
                    sample_id, global_subject_id, sample_type, 
                    redcap_event, project, collection_date,
                    storage_location, notes, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_id) 
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

            with conn.cursor() as cursor:
                for specimen in specimens:
                    cursor.execute(
                        query,
                        (
                            specimen["sample_id"],
                            specimen["global_subject_id"],
                            specimen["sample_type"],
                            specimen.get("redcap_event"),
                            specimen.get("project"),
                            specimen.get("collection_date"),
                            specimen.get("storage_location"),
                            specimen.get("notes"),
                            now,
                            now,
                        ),
                    )
                conn.commit()

            logger.info(
                f"[{self.project_key}] Inserted {len(specimens)} specimens "
                f"for GSID {specimens[0]['global_subject_id']}"
            )
            return True

        except Exception as e:
            logger.error(f"[{self.project_key}] Error inserting specimens: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                return_db_connection(conn)

    def create_fragment(self, gsid: str, record: Dict[str, Any]) -> Optional[Dict]:
        """Create a curated data fragment from REDCap record using field mappings"""
        try:
            fragment = {
                "gsid": gsid,
                "project_key": self.project_key,
                "project_name": self.project_name,
                "source": "redcap",
                "record_id": record.get("record_id"),
                "created_at": datetime.utcnow().isoformat(),
                "data": {},
            }

            mappings = self.field_mappings.get("mappings", [])

            if not mappings:
                logger.warning(
                    f"[{self.project_key}] No mappings found for fragment creation"
                )
                return fragment

            # Group data by target table
            for mapping in mappings:
                target_table = mapping.get("target_table")
                source_field = mapping.get("source_field")
                target_field = mapping.get("target_field")

                if not all([target_table, source_field, target_field]):
                    continue

                # Get value from record
                value = record.get(source_field)

                # Skip empty values
                if value in [None, "", "NA", "N/A", "null"]:
                    continue

                # Initialize table section if needed
                if target_table not in fragment["data"]:
                    fragment["data"][target_table] = []

                # For specimen table, include sample_type
                if target_table == "specimen":
                    sample_type = mapping.get("sample_type", "unknown")
                    fragment["data"][target_table].append(
                        {
                            "sample_id": str(value).strip(),
                            "sample_type": sample_type,
                            "source_field": source_field,
                        }
                    )
                else:
                    # For other tables, just store field->value mapping
                    fragment["data"][target_table].append(
                        {target_field: value, "source_field": source_field}
                    )

            return fragment

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error creating fragment for {gsid}: {e}",
                exc_info=True,
            )
            return None

    def process_record(self, record: Dict[str, Any], gsid: str) -> bool:
        """Process a single REDCap record - extract specimens and insert"""
        try:
            # Extract specimens from record using field mappings
            specimens = self.extract_specimens_from_record(record, gsid)

            # Insert specimens into database
            if not self.insert_samples(specimens):
                return False

            logger.info(
                f"[{self.project_key}] Successfully processed record "
                f"{record.get('record_id')} for GSID {gsid}"
            )
            return True

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error processing record "
                f"{record.get('record_id', 'unknown')}: {e}"
            )
            return False
