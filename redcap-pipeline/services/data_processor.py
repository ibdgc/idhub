import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.database import get_db_connection, return_db_connection
from psycopg2.extras import RealDictCursor

from services.center_resolver import CenterResolver
from services.gsid_client import GSIDClient
from services.s3_uploader import S3Uploader

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, project_config: dict):
        """Initialize DataProcessor with project configuration"""
        self.project_config = project_config
        self.project_key = project_config.get("key")
        self.project_name = project_config.get("name")
        self.field_mappings = self.load_field_mappings()

        # Initialize dependencies
        self.center_resolver = CenterResolver()
        self.gsid_client = GSIDClient()
        self.s3_uploader = S3Uploader()

    def load_field_mappings(self) -> Dict:
        """Load field mappings from configuration file"""
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
            config = json.load(f)

        logger.info(f"[{self.project_key}] Loaded field mappings from {mapping_file}")
        return config

    def transform_value(self, field_name: str, value: Any) -> Any:
        """Apply transformations to field values"""
        transformations = self.field_mappings.get("transformations", {})

        if field_name not in transformations:
            return value

        transform = transformations[field_name]

        if transform["type"] == "extract_year":
            if not value:
                return None
            return value.split("-")[0] if "-" in str(value) else value
        elif transform["type"] == "boolean":
            if value in transform["true_values"]:
                return True
            elif value in transform["false_values"]:
                return False
            return None

        return value

    def extract_local_ids(self, record: Dict, center_id: int) -> List[Dict]:
        """Extract all local identifiers from record using field mappings"""
        identifiers = []
        mappings = self.field_mappings.get("mappings", [])

        # Get all local_subject_ids mappings
        local_id_mappings = [
            m for m in mappings if m.get("target_table") == "local_subject_ids"
        ]

        for mapping in local_id_mappings:
            source_field = mapping.get("source_field")
            value = record.get(source_field)

            if value and value not in ["", "NA", "N/A", "null"]:
                identifiers.append(
                    {
                        "center_id": center_id,
                        "local_subject_id": str(value).strip(),
                        "identifier_type": source_field,
                    }
                )

        return identifiers

    def register_all_local_ids(self, gsid: str, identifiers: List[Dict]):
        """Register all local IDs for a subject, flag conflicts for review"""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for identifier in identifiers:
                    # Check if this local_id already exists with a different GSID
                    cur.execute(
                        """
                        SELECT global_subject_id, identifier_type
                        FROM local_subject_ids
                        WHERE center_id = %s AND local_subject_id = %s
                        """,
                        (identifier["center_id"], identifier["local_subject_id"]),
                    )
                    existing = cur.fetchone()

                    if existing and existing["global_subject_id"] != gsid:
                        # CONFLICT: Same local_id points to different GSID
                        logger.warning(
                            f"[{self.project_key}] CONFLICT: {identifier['local_subject_id']} "
                            f"already linked to {existing['global_subject_id']}, "
                            f"attempting to link to {gsid}"
                        )

                        # Flag BOTH subjects for review
                        cur.execute(
                            """
                            UPDATE subjects
                            SET flagged_for_review = TRUE,
                                review_notes = COALESCE(review_notes || E'\n', '') || %s
                            WHERE global_subject_id IN (%s, %s)
                            """,
                            (
                                f"[{datetime.utcnow().isoformat()}] Duplicate local_id conflict: "
                                f"{identifier['local_subject_id']} (type: {identifier['identifier_type']}) "
                                f"linked to both {gsid} and {existing['global_subject_id']}",
                                gsid,
                                existing["global_subject_id"],
                            ),
                        )

                        # Log to identity_resolutions
                        cur.execute(
                            """
                            INSERT INTO identity_resolutions
                            (input_center_id, input_local_id, matched_gsid, action,
                             match_strategy, confidence_score, requires_review, review_reason, created_by)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                identifier["center_id"],
                                identifier["local_subject_id"],
                                gsid,
                                "conflict_detected",
                                "duplicate_local_id",
                                0.0,
                                True,
                                f"Local ID already exists for GSID {existing['global_subject_id']}",
                                "redcap_pipeline",
                            ),
                        )
                        # Skip inserting this conflicting ID
                        continue
                    elif existing and existing["global_subject_id"] == gsid:
                        # Already linked correctly, skip
                        logger.debug(
                            f"[{self.project_key}] ID {identifier['local_subject_id']} "
                            f"already linked to {gsid}"
                        )
                        continue

                    # No conflict - insert new mapping
                    cur.execute(
                        """
                        INSERT INTO local_subject_ids
                        (center_id, local_subject_id, identifier_type, global_subject_id)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            identifier["center_id"],
                            identifier["local_subject_id"],
                            identifier["identifier_type"],
                            gsid,
                        ),
                    )
                    logger.info(
                        f"[{self.project_key}] Linked {identifier['identifier_type']}="
                        f"{identifier['local_subject_id']} -> {gsid}"
                    )

                conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(
                f"[{self.project_key}] Error registering local IDs for {gsid}: {e}"
            )
            raise
        finally:
            if conn:
                return_db_connection(conn)

    def insert_samples(self, record: Dict, gsid: str):
        """Insert sample records into database using field mappings"""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                mappings = self.field_mappings.get("mappings", [])

                # Process all specimen mappings from config
                for mapping in mappings:
                    if mapping.get("target_table") == "specimen":
                        source_field = mapping["source_field"]
                        sample_type = mapping.get("sample_type", "unknown")

                        if record.get(source_field):
                            cur.execute(
                                """
                                INSERT INTO specimen (sample_id, global_subject_id, sample_type, redcap_event, project)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (sample_id) DO UPDATE SET
                                    sample_type = EXCLUDED.sample_type,
                                    redcap_event = EXCLUDED.redcap_event,
                                    project = EXCLUDED.project
                                """,
                                (
                                    record[source_field],
                                    gsid,
                                    sample_type,
                                    record.get("redcap_event_name"),
                                    self.project_name,
                                ),
                            )
                            logger.debug(
                                f"[{self.project_key}] Inserted specimen: "
                                f"{record[source_field]} (type: {sample_type})"
                            )

                # Family linkage
                if record.get("family_id"):
                    cur.execute(
                        """
                        INSERT INTO family (family_id)
                        VALUES (%s)
                        ON CONFLICT (family_id) DO NOTHING
                        """,
                        (record["family_id"],),
                    )
                    cur.execute(
                        """
                        UPDATE subjects
                        SET family_id = %s
                        WHERE global_subject_id = %s
                        """,
                        (record["family_id"], gsid),
                    )
                    logger.debug(
                        f"[{self.project_key}] Linked family_id {record['family_id']} to {gsid}"
                    )

                conn.commit()
                logger.info(f"[{self.project_key}] Inserted samples for GSID {gsid}")

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(
                f"[{self.project_key}] Error inserting samples for {gsid}: {e}"
            )
            raise
        finally:
            if conn:
                return_db_connection(conn)

    def create_curated_fragment(self, record: Dict, gsid: str, center_id: int) -> Dict:
        """Create curated data fragment (PHI-free)"""
        fragment = {
            "gsid": gsid,
            "center_id": center_id,
            "project_key": self.project_key,
            "project_name": self.project_name,
            "samples": {},
            "family": {},
            "metadata": {
                "source": "redcap",
                "pipeline_version": "2.0",
                "processed_at": datetime.utcnow().isoformat(),
            },
        }

        mappings = self.field_mappings.get("mappings", [])

        # Group specimens by type from mappings
        specimen_types = {}
        for mapping in mappings:
            if mapping.get("target_table") == "specimen":
                source_field = mapping["source_field"]
                sample_type = mapping.get("sample_type", "unknown")

                if record.get(source_field):
                    if sample_type not in specimen_types:
                        specimen_types[sample_type] = []
                    specimen_types[sample_type].append(record[source_field])

        # Add to fragment
        for sample_type, sample_ids in specimen_types.items():
            if len(sample_ids) == 1:
                fragment["samples"][sample_type] = sample_ids[0]
            else:
                fragment["samples"][sample_type] = sample_ids

        # Add family info if present
        if record.get("family_id"):
            fragment["family"]["family_id"] = record["family_id"]

        return fragment

    def process_record(self, record: Dict) -> Dict:
        """Process single REDCap record with conflict detection"""
        try:
            # Get center
            center_name = record.get("redcap_data_access_group", "Unknown")
            center_id = self.center_resolver.get_or_create_center(center_name)

            # Extract primary local ID for GSID registration
            local_subject_id = record.get("consortium_id") or record.get("local_id")
            if not local_subject_id:
                raise ValueError(
                    f"No local_subject_id found in record: {record.get('record_id')}"
                )

            # Get transformed values
            registration_date = record.get("registration_date")
            registration_year = self.transform_value(
                "registration_date", registration_date
            )
            control = self.transform_value("control", record.get("control", "0"))

            # Register subject with GSID service (primary ID only)
            gsid_result = self.gsid_client.register_subject(
                {
                    "center_id": center_id,
                    "local_subject_id": local_subject_id,
                    "registration_year": registration_year,
                    "control": control,
                    "created_by": "redcap_pipeline",
                }
            )
            gsid = gsid_result["gsid"]

            identifier_type = (
                "consortium_id" if record.get("consortium_id") else "local_id"
            )
            logger.info(
                f"[{self.project_key}] Registered {local_subject_id} ({identifier_type}) -> "
                f"GSID {gsid} ({gsid_result['action']})"
            )

            # Extract and register ALL local IDs (with conflict detection)
            all_identifiers = self.extract_local_ids(record, center_id)
            if all_identifiers:
                self.register_all_local_ids(gsid, all_identifiers)

            # Insert samples
            self.insert_samples(record, gsid)

            # Create and upload fragment
            fragment = self.create_curated_fragment(record, gsid, center_id)
            self.s3_uploader.upload_fragment(fragment, self.project_key, gsid)

            return {"status": "success", "gsid": gsid}

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error processing record "
                f"{record.get('record_id')}: {e}"
            )
            return {
                "status": "error",
                "error": str(e),
                "record_id": record.get("record_id"),
            }
