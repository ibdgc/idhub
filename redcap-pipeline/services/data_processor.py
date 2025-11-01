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

        # Cache subject ID fields from mappings
        self.subject_id_fields = self.get_subject_id_fields()
        logger.info(f"[{self.project_key}] Subject ID fields: {self.subject_id_fields}")

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

    def get_subject_id_fields(self) -> List[str]:
        """Extract subject ID field names from mappings"""
        mappings = self.field_mappings.get("mappings", [])

        # Find all fields mapped to local_subject_ids
        id_fields = [
            m["source_field"]
            for m in mappings
            if m.get("target_table") == "local_subject_ids"
        ]

        return id_fields

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

    def extract_subject_ids(self, record: Dict) -> List[Dict[str, str]]:
        """Extract all available subject IDs from record based on field mappings"""
        subject_ids = []

        for field_name in self.subject_id_fields:
            value = record.get(field_name)
            if value and value not in ["", "NA", "N/A", "null", "NULL"]:
                subject_ids.append(
                    {
                        "identifier_type": field_name,
                        "local_subject_id": str(value).strip(),
                    }
                )

        return subject_ids

    def resolve_subject_ids(
        self,
        subject_ids: List[Dict[str, str]],
        center_id: int,
    ) -> Dict[str, Any]:
        """
        Register/attach a *set* of local subject IDs to **one** and only one GSID.

        Strategy
        --------
        1. Register the first ID in the list – this either returns an existing GSID
           (if the ID is already known) or creates a new GSID.
        2. For every remaining ID call the GSID service again *with the GSID we
           just got* so the service attaches the alias instead of creating a new
           subject.
        3. Collect/return metadata that downstream code expects.
        """
        if not subject_ids:
            raise ValueError("No valid subject IDs found in record")

        # --- step 1 : primary registration ------------------------------------
        primary_id = subject_ids[0]
        primary_result = self.gsid_client.register_subject(
            center_id=center_id,
            local_subject_id=primary_id["local_subject_id"],
            identifier_type=primary_id["identifier_type"],
            created_by="redcap_pipeline",
        )

        gsid = primary_result["gsid"]
        primary_action = primary_result["action"]

        # --- step 2 : attach secondary IDs ------------------------------------
        for id_info in subject_ids[1:]:
            try:
                # We re-use the same endpoint but supply the GSID so the service
                # links instead of spawns a new subject record.
                payload = {
                    "center_id": center_id,
                    "local_subject_id": id_info["local_subject_id"],
                    "identifier_type": id_info["identifier_type"],
                    "created_by": "redcap_pipeline",
                    "gsid": gsid,  # <-- adjust if your API uses a different key
                }
                # Use the lower-level session so we can freely craft the payload
                resp = self.gsid_client.session.post(
                    f"{self.gsid_client.base_url}/register",
                    json=payload,
                    timeout=30,
                )
                resp.raise_for_status()
            except Exception as e:
                logger.warning(
                    f"[{self.project_key}] Unable to attach "
                    f"{id_info['local_subject_id']} to GSID {gsid}: {e}"
                )

        # --- step 3 : build return structure ----------------------------------
        result = {
            "gsid": gsid,
            "action": primary_action,
            "identifier_type": primary_id["identifier_type"],
            "local_subject_id": primary_id["local_subject_id"],
            "conflict": False,  # we guarantee a single GSID
        }

        return result

    def register_all_local_ids(
        self, gsid: str, subject_ids: List[Dict[str, str]], center_id: int
    ):
        """Register all local IDs for a subject, flag conflicts for review"""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for id_info in subject_ids:
                    local_id = id_info["local_subject_id"]
                    id_type = id_info["identifier_type"]

                    # Check if this local_id already exists with a different GSID
                    cur.execute(
                        """
                        SELECT global_subject_id, identifier_type
                        FROM local_subject_ids
                        WHERE center_id = %s AND local_subject_id = %s
                        """,
                        (center_id, local_id),
                    )
                    existing = cur.fetchone()

                    if existing and existing["global_subject_id"] != gsid:
                        # CONFLICT: Same local_id points to different GSID
                        logger.warning(
                            f"[{self.project_key}] CONFLICT: {local_id} "
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
                                f"{local_id} (type: {id_type}) "
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
                                center_id,
                                local_id,
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
                            f"[{self.project_key}] ID {local_id} already linked to {gsid}"
                        )
                        continue

                    # No conflict - insert new mapping
                    cur.execute(
                        """
                        INSERT INTO local_subject_ids
                        (center_id, local_subject_id, identifier_type, global_subject_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (center_id, local_subject_id) DO UPDATE SET
                            identifier_type = EXCLUDED.identifier_type,
                            global_subject_id = EXCLUDED.global_subject_id
                        """,
                        (center_id, local_id, id_type, gsid),
                    )
                    logger.info(
                        f"[{self.project_key}] Linked {id_type}={local_id} -> {gsid}"
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
        """Process single REDCap record with multi-ID resolution and conflict detection"""
        try:
            # Get center
            center_name = record.get("redcap_data_access_group", "Unknown")
            center_id = self.center_resolver.get_or_create_center(center_name)

            # Extract ALL subject IDs from field mappings
            subject_ids = self.extract_subject_ids(record)

            if not subject_ids:
                raise ValueError(
                    f"No subject IDs found in record {record.get('record_id')}. "
                    f"Expected fields: {self.subject_id_fields}"
                )

            # Format IDs for logging
            id_list = ", ".join(
                [f"{s['identifier_type']}={s['local_subject_id']}" for s in subject_ids]
            )
            logger.debug(
                f"[{self.project_key}] Found {len(subject_ids)} subject ID(s): {id_list}"
            )

            # Resolve against GSID service (checks all IDs for existing matches)
            resolution = self.resolve_subject_ids(subject_ids, center_id)
            gsid = resolution["gsid"]

            # Log resolution
            log_msg = (
                f"[{self.project_key}] Resolved to GSID {gsid} "
                f"(action: {resolution['action']}, primary: {resolution['identifier_type']}="
                f"{resolution['local_subject_id']})"
            )
            if resolution.get("conflict"):
                log_msg += f" ⚠️ CONFLICT: Multiple GSIDs found: {resolution['conflicting_gsids']}"
                logger.warning(log_msg)
            else:
                logger.info(log_msg)

            # Register ALL local IDs (with conflict detection at DB level)
            self.register_all_local_ids(gsid, subject_ids, center_id)

            # Insert samples
            self.insert_samples(record, gsid)

            # Create and upload fragment
            fragment = self.create_curated_fragment(record, gsid, center_id)
            self.s3_uploader.upload_fragment(fragment, self.project_key, gsid)

            result = {
                "status": "success",
                "gsid": gsid,
                "action": resolution["action"],
            }

            if resolution.get("conflict"):
                result["conflict"] = True
                result["conflicting_gsids"] = resolution["conflicting_gsids"]

            return result

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error processing record "
                f"{record.get('record_id')}: {e}",
                exc_info=True,
            )
            return {
                "status": "error",
                "error": str(e),
                "record_id": record.get("record_id"),
            }
