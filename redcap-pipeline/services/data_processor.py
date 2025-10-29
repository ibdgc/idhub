import logging
from datetime import datetime
from typing import Any, Dict, List

from core.config import ProjectConfig, settings
from core.database import get_db_connection, return_db_connection
from psycopg2.extras import RealDictCursor

from services.center_resolver import CenterResolver
from services.gsid_client import GSIDClient

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(
        self,
        gsid_client: GSIDClient,
        center_resolver: CenterResolver,
        project_config: ProjectConfig = None,
    ):
        self.gsid_client = gsid_client
        self.center_resolver = center_resolver
        self.project_config = project_config

        # Use project_key for logging/tracking, redcap_project_id for REDCap API
        if project_config:
            self.project_key = project_config.project_key
            self.redcap_project_id = project_config.redcap_project_id
            self.project_name = project_config.project_name
        else:
            self.project_key = "default"
            self.redcap_project_id = settings.REDCAP_PROJECT_ID
            self.project_name = "Default Project"

    def extract_local_ids(self, record: Dict[str, Any], center_id: int) -> List[Dict]:
        """Extract all available local identifiers from record"""
        identifiers = []

        # Primary identifiers
        if record.get("consortium_id"):
            identifiers.append(
                {
                    "center_id": center_id,
                    "local_subject_id": record["consortium_id"],
                    "identifier_type": "consortium_id",
                }
            )

        if record.get("local_id"):
            identifiers.append(
                {
                    "center_id": center_id,
                    "local_subject_id": record["local_id"],
                    "identifier_type": "local_id",
                }
            )

        # Additional identifiers that might exist
        if record.get("subject_id"):
            identifiers.append(
                {
                    "center_id": center_id,
                    "local_subject_id": record["subject_id"],
                    "identifier_type": "subject_id",
                }
            )

        if record.get("patient_id"):
            identifiers.append(
                {
                    "center_id": center_id,
                    "local_subject_id": record["patient_id"],
                    "identifier_type": "patient_id",
                }
            )

        return identifiers

    def register_all_local_ids(self, gsid: str, identifiers: List[Dict]):
        """Register all local IDs for a subject, flag conflicts for review"""
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for identifier in identifiers:
                    # Check if this local_id already exists with a different GSID
                    cursor.execute(
                        """
                        SELECT global_subject_id, identifier_type
                        FROM local_subject_ids
                        WHERE center_id = %s AND local_subject_id = %s
                        """,
                        (identifier["center_id"], identifier["local_subject_id"]),
                    )
                    existing = cursor.fetchone()

                    if existing and existing["global_subject_id"] != gsid:
                        # CONFLICT: Same local_id points to different GSID
                        logger.warning(
                            f"[{self.project_key}] CONFLICT: {identifier['local_subject_id']} already linked to "
                            f"{existing['global_subject_id']}, attempting to link to {gsid}"
                        )

                        # Flag BOTH subjects for review
                        cursor.execute(
                            """
                            UPDATE subjects
                            SET flagged_for_review = TRUE,
                                review_notes = COALESCE(review_notes || E'\n', '') || %s
                            WHERE global_subject_id IN (%s, %s)
                            """,
                            (
                                f"[{datetime.utcnow().isoformat()}] [{self.project_name} (REDCap ID: {self.redcap_project_id})] "
                                f"Duplicate local_id conflict: {identifier['local_subject_id']} "
                                f"(type: {identifier['identifier_type']}) linked to both {gsid} and {existing['global_subject_id']}",
                                gsid,
                                existing["global_subject_id"],
                            ),
                        )

                        # Log to identity_resolutions
                        cursor.execute(
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
                                f"[{self.project_name}] Local ID already exists for GSID {existing['global_subject_id']}",
                                f"redcap_{self.project_key}",
                            ),
                        )

                        # Skip inserting this conflicting ID
                        continue

                    elif existing and existing["global_subject_id"] == gsid:
                        # Already linked correctly, skip
                        logger.debug(
                            f"[{self.project_key}] ID {identifier['local_subject_id']} already linked to {gsid}"
                        )
                        continue

                    # No conflict - insert new mapping
                    cursor.execute(
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
                        f"[{self.project_key}] Linked {identifier['identifier_type']}={identifier['local_subject_id']} -> {gsid}"
                    )

            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(
                f"[{self.project_key}] Error registering local IDs for {gsid}: {e}"
            )
            raise
        finally:
            return_db_connection(conn)

    def transform_value(self, field_name: str, value: Any) -> Any:
        """Apply transformations to field values"""
        if self.project_config:
            field_mappings = self.project_config.load_field_mappings()
        else:
            field_mappings = settings.load_field_mappings()

        transformations = field_mappings.get("transformations", {})

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

    def register_subject(self, record: Dict[str, Any]) -> tuple[str, int]:
        """Register subject with primary identifier"""
        center_name = record.get("redcap_data_access_group", "Unknown")
        center_id = self.center_resolver.get_or_create_center(center_name)

        # Determine primary identifier
        local_subject_id = record.get("consortium_id") or record.get("local_id")
        identifier_type = "consortium_id" if record.get("consortium_id") else "local_id"

        if not local_subject_id:
            raise ValueError(
                f"[{self.project_key}] No local_subject_id found in record: {record.get('record_id')}"
            )

        # Transform values
        registration_date = record.get("registration_date")
        registration_year = self.transform_value("registration_date", registration_date)
        control = self.transform_value("control", record.get("control", "0"))

        # Register with GSID service
        result = self.gsid_client.register_subject(
            center_id=center_id,
            local_subject_id=local_subject_id,
            identifier_type=identifier_type,
            registration_year=registration_year,
            control=control,
            created_by=f"redcap_{self.project_key}",
        )

        return result["gsid"], center_id

    def _process_specimens(self, record: Dict[str, Any], gsid: str):
        """Process and insert specimen records using field mappings"""
        if self.project_config:
            field_mappings = self.project_config.load_field_mappings()
        else:
            field_mappings = settings.load_field_mappings()

        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Process all specimen mappings from config
                for mapping in field_mappings.get("mappings", []):
                    if mapping.get("target_table") == "specimen":
                        source_field = mapping["source_field"]
                        sample_type = mapping.get("sample_type")

                        if record.get(source_field):
                            cursor.execute(
                                """
                                INSERT INTO specimen (sample_id, global_subject_id, sample_type, redcap_event, redcap_project_id)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (sample_id) DO UPDATE SET
                                    sample_type = EXCLUDED.sample_type,
                                    redcap_event = EXCLUDED.redcap_event,
                                    redcap_project_id = EXCLUDED.redcap_project_id
                                """,
                                (
                                    record[source_field],
                                    gsid,
                                    sample_type,
                                    record.get("redcap_event_name"),
                                    self.redcap_project_id,
                                ),
                            )
                            logger.debug(
                                f"[{self.project_key}] Inserted specimen: {record[source_field]} (type: {sample_type})"
                            )

                # Handle family linkage
                if record.get("family_id"):
                    cursor.execute(
                        """
                        INSERT INTO family (family_id)
                        VALUES (%s)
                        ON CONFLICT (family_id) DO NOTHING
                        """,
                        (record["family_id"],),
                    )

                    cursor.execute(
                        """
                        UPDATE subjects
                        SET family_id = %s
                        WHERE global_subject_id = %s
                        """,
                        (record["family_id"], gsid),
                    )

                logger.info(f"[{self.project_key}] Inserted samples for GSID {gsid}")

            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(
                f"[{self.project_key}] Error inserting samples for {gsid}: {e}"
            )
            raise
        finally:
            return_db_connection(conn)

    def process_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process single REDCap record with conflict detection"""
        try:
            # Register subject with primary ID
            gsid, center_id = self.register_subject(record)

            # Extract and register ALL local IDs (with conflict detection)
            all_identifiers = self.extract_local_ids(record, center_id)
            if all_identifiers:
                self.register_all_local_ids(gsid, all_identifiers)

            # Insert samples
            self._process_specimens(record, gsid)

            return {
                "status": "success",
                "gsid": gsid,
                "project_key": self.project_key,
                "redcap_project_id": self.redcap_project_id,
            }

        except Exception as e:
            logger.error(
                f"[{self.project_key}] Error processing record {record.get('record_id')}: {str(e)}"
            )
            return {
                "status": "error",
                "error": str(e),
                "record_id": record.get("record_id"),
                "project_key": self.project_key,
                "redcap_project_id": self.redcap_project_id,
            }

    def process_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process multiple records"""
        results = []
        for record in records:
            result = self.process_record(record)
            results.append(result)
        return results
