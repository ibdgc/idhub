import json
import logging
from datetime import date, datetime
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

    def extract_registration_year(self, record: Dict) -> Optional[date]:
        """
        Extract and convert registration_year from REDCap record.
        Always normalizes to January 1st of the year.
        Looks for field mapped to 'registration_year' in target_field.
        """
        # Find the registration_year field from mappings
        reg_field = None
        for mapping in self.field_mappings.get("mappings", []):
            if (
                mapping.get("target_table") == "subjects"
                and mapping.get("target_field") == "registration_year"
            ):
                reg_field = mapping.get("source_field")
                break

        if not reg_field:
            logger.debug(f"[{self.project_key}] No registration_year field mapped")
            return None

        reg_value = record.get(reg_field)

        if not reg_value:
            return None

        logger.debug(
            f"[{self.project_key}] Found registration field '{reg_field}' = '{reg_value}'"
        )

        year = None

        # If it's already a date object, extract year
        if isinstance(reg_value, date):
            year = reg_value.year

        # If it's a datetime object, extract year
        elif isinstance(reg_value, datetime):
            year = reg_value.year

        # If it's a string
        elif isinstance(reg_value, str):
            reg_value = reg_value.strip()

            if not reg_value:
                return None

            try:
                # Try full date format (YYYY-MM-DD) - extract year only
                if len(reg_value) >= 10 and "-" in reg_value:
                    year_str = reg_value.split("-")[0]
                    year = int(year_str)

                # Try just year (YYYY)
                elif len(reg_value) == 4 and reg_value.isdigit():
                    year = int(reg_value)

                # Try parsing with various formats - extract year only
                else:
                    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]:
                        try:
                            parsed_date = datetime.strptime(reg_value, fmt)
                            year = parsed_date.year
                            break
                        except ValueError:
                            continue

            except (ValueError, IndexError) as e:
                logger.warning(
                    f"[{self.project_key}] Could not parse registration_date '{reg_value}': {e}"
                )
                return None

        # If it's an integer year
        elif isinstance(reg_value, int):
            year = reg_value

        # Validate year and convert to January 1st
        if year and 1900 <= year <= 2100:
            normalized_date = date(year, 1, 1)
            logger.debug(
                f"[{self.project_key}] Normalized '{reg_value}' to {normalized_date}"
            )
            return normalized_date

        logger.warning(
            f"[{self.project_key}] Invalid year extracted: {year} from {reg_value}"
        )
        return None

    def extract_control_status(self, record: Dict) -> bool:
        """
        Extract control status from REDCap record.
        Looks for field mapped to 'control' in target_field.
        """
        # Find the control field from mappings
        control_field = None
        for mapping in self.field_mappings.get("mappings", []):
            if (
                mapping.get("target_table") == "subjects"
                and mapping.get("target_field") == "control"
            ):
                control_field = mapping.get("source_field")
                break

        if not control_field:
            return False

        control_value = record.get(control_field, False)

        # Handle various representations of boolean
        if isinstance(control_value, bool):
            return control_value
        if isinstance(control_value, str):
            return control_value.lower() in ["1", "true", "yes", "y"]
        if isinstance(control_value, int):
            return control_value == 1

        return False

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
        record: Dict,
    ) -> Dict[str, Any]:
        """Resolve which GSID this set of local-subject-IDs belongs to"""

        if not subject_ids:
            raise ValueError("No valid subject IDs found in record")

        registration_year = self.extract_registration_year(record)
        control = self.extract_control_status(record)

        conn = get_db_connection()
        gsids_by_center: Dict[int, Dict[str, str]] = {}  # center_id → {local_id: gsid}

        logger.debug(
            f"[{self.project_key}] Checking {len(subject_ids)} IDs for center_id={center_id}"
        )

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for id_info in subject_ids:
                    local_id = id_info["local_subject_id"]

                    # Check for this ID across ALL centers
                    cur.execute(
                        """
                        SELECT global_subject_id, center_id
                        FROM local_subject_ids
                        WHERE local_subject_id = %s
                        ORDER BY center_id
                        """,
                        (local_id,),
                    )
                    rows = cur.fetchall()

                    for row in rows:
                        found_center = row["center_id"]
                        found_gsid = row["global_subject_id"]

                        if found_center not in gsids_by_center:
                            gsids_by_center[found_center] = {}
                        gsids_by_center[found_center][local_id] = found_gsid

                        logger.debug(
                            f"[{self.project_key}] Found: {local_id} → {found_gsid} "
                            f"(center_id={found_center})"
                        )
        finally:
            return_db_connection(conn)

        # Decision logic
        exact_match_gsids = gsids_by_center.get(center_id, {})
        unknown_center_gsids = gsids_by_center.get(1, {})  # center_id=1 is "Unknown"
        all_other_gsids = {
            gsid
            for cid, gsids in gsids_by_center.items()
            if cid not in [center_id, 1]
            for gsid in gsids.values()
        }

        # Case 1: Exact center match exists
        if exact_match_gsids:
            unique_gsids = list(set(exact_match_gsids.values()))

            if len(unique_gsids) == 1:
                logger.info(
                    f"[{self.project_key}] Exact center match: {unique_gsids[0]}"
                )
                return self._build_response(
                    gsid=unique_gsids[0],
                    action="link_existing",
                    subject_ids=subject_ids,
                    exact_match_gsids=exact_match_gsids,
                    registration_year=registration_year,
                    control=control,
                )
            else:
                logger.warning(
                    f"[{self.project_key}] Conflict within same center: {unique_gsids}"
                )
                return self._build_conflict_response(
                    unique_gsids, subject_ids, registration_year, control
                )

        # Case 2: Unknown center exists, incoming has known center → upgrade
        elif unknown_center_gsids and center_id != 1:
            unique_gsids = list(set(unknown_center_gsids.values()))

            if len(unique_gsids) == 1:
                gsid = unique_gsids[0]
                logger.info(
                    f"[{self.project_key}] Upgrading {gsid} from Unknown (center=1) "
                    f"to center={center_id}"
                )

                # Update center_id in GSID service
                try:
                    self.gsid_client.update_subject_center(
                        gsid=gsid, new_center_id=center_id
                    )
                except Exception as e:
                    logger.error(f"Failed to update center for {gsid}: {e}")

                return self._build_response(
                    gsid=gsid,
                    action="link_existing_upgraded_center",
                    subject_ids=subject_ids,
                    exact_match_gsids=unknown_center_gsids,
                    registration_year=registration_year,
                    control=control,
                )
            else:
                logger.warning(
                    f"[{self.project_key}] Conflict in Unknown center: {unique_gsids}"
                )
                return self._build_conflict_response(
                    unique_gsids, subject_ids, registration_year, control
                )

        # Case 3: Match exists but in different (non-Unknown) center → conflict
        elif all_other_gsids:
            logger.error(
                f"[{self.project_key}] Cross-center conflict: "
                f"ID exists in other centers: {gsids_by_center}"
            )
            return self._build_conflict_response(
                list(all_other_gsids), subject_ids, registration_year, control
            )

        # Case 4: No match anywhere → create new
        else:
            logger.info(f"[{self.project_key}] No existing GSID found - creating new")

            # Use multi-candidate registration if we have multiple IDs
            if len(subject_ids) > 1:
                result = self.gsid_client.register_multi_candidate(
                    center_id=center_id,
                    candidate_ids=subject_ids,
                    registration_year=registration_year,
                    control=control,
                    created_by=self.project_key,
                )
            else:
                # Single ID registration
                primary_id = subject_ids[0]
                result = self.gsid_client.register_subject(
                    center_id=center_id,
                    local_subject_id=primary_id["local_subject_id"],
                    identifier_type=primary_id["identifier_type"],
                    registration_year=registration_year,
                    control=control,
                    created_by=self.project_key,
                )

            return {
                "gsid": result["gsid"],
                "action": result["action"],
                "identifier_type": subject_ids[0]["identifier_type"],
                "local_subject_id": subject_ids[0]["local_subject_id"],
                "conflict": False,
                "conflicting_gsids": None,
                "registration_year": registration_year,
                "control": control,
                "match_strategy": result.get("match_strategy"),
                "confidence": result.get("confidence"),
            }

    def _build_response(
        self,
        gsid: str,
        action: str,
        subject_ids: List[Dict[str, str]],
        exact_match_gsids: Dict[str, str],
        registration_year: Optional[date],
        control: bool,
    ) -> Dict[str, Any]:
        """Build standard response"""
        any_local_id = next(iter(exact_match_gsids))
        identifier_type = next(
            i["identifier_type"]
            for i in subject_ids
            if i["local_subject_id"] == any_local_id
        )

        return {
            "gsid": gsid,
            "action": action,
            "identifier_type": identifier_type,
            "local_subject_id": any_local_id,
            "conflict": False,
            "conflicting_gsids": None,
            "registration_year": registration_year,
            "control": control,
        }

    def _build_conflict_response(
        self,
        unique_gsids: List[str],
        subject_ids: List[Dict[str, str]],
        registration_year: Optional[date],
        control: bool,
    ) -> Dict[str, Any]:
        """Build conflict response"""
        return {
            "gsid": unique_gsids[0],
            "action": "conflict_detected",
            "identifier_type": subject_ids[0]["identifier_type"],
            "local_subject_id": subject_ids[0]["local_subject_id"],
            "conflict": True,
            "conflicting_gsids": unique_gsids,
            "registration_year": registration_year,
            "control": control,
        }

    def register_all_local_ids(
        self, gsid: str, subject_ids: List[Dict[str, str]], center_id: int
    ):
        """
        Register every local-subject-ID in `subject_ids` for the given GSID,
        logging any conflicts to the identity_resolutions table.
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for id_info in subject_ids:
                    id_type = id_info["identifier_type"]
                    local_id = id_info["local_subject_id"]
                    if not local_id or local_id.strip() == "":
                        continue

                    # ‒‒‒ conflict check ‒‒‒
                    cursor.execute(
                        """
                        SELECT global_subject_id
                        FROM   local_subject_ids
                        WHERE  center_id = %s AND local_subject_id = %s
                        """,
                        (center_id, local_id),
                    )
                    existing = cursor.fetchone()

                    if existing and existing["global_subject_id"] != gsid:
                        logger.warning(
                            f"[{self.project_key}] Conflict: {local_id} already "
                            f"linked to {existing['global_subject_id']} – skipping"
                        )
                        cursor.execute(
                            """
                            INSERT INTO identity_resolutions (
                                input_center_id,
                                input_local_id,
                                matched_gsid,
                                action,
                                match_strategy,
                                confidence_score,
                                requires_review,
                                review_reason,
                                created_by
                            )
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
                                f"Local ID already exists for GSID "
                                f"{existing['global_subject_id']}",
                                "redcap_pipeline",
                            ),
                        )
                        continue

                    if existing:
                        logger.debug(
                            f"[{self.project_key}] Local ID {local_id} already linked to {gsid}"
                        )
                        continue

                    # ‒‒‒ insert new mapping (NO created_by column) ‒‒‒
                    cursor.execute(
                        """
                        INSERT INTO local_subject_ids (
                            global_subject_id,
                            center_id,
                            local_subject_id,
                            identifier_type
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (gsid, center_id, local_id, id_type),
                    )
                    logger.info(
                        f"[{self.project_key}] Registered {id_type}={local_id} → {gsid}"
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
                            # Extract optional fields from mapping
                            region_location = mapping.get("region_location")
                            year_collected = None
                            sample_available = True

                            # Try to extract year from record if available
                            if record.get("year_collected"):
                                year_collected = self.transform_value(
                                    "year_collected", record.get("year_collected")
                                )

                            cur.execute(
                                """
                                INSERT INTO specimen (
                                    sample_id, 
                                    global_subject_id, 
                                    sample_type, 
                                    redcap_event, 
                                    project,
                                    region_location,
                                    year_collected,
                                    sample_available
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (sample_id) DO UPDATE SET
                                    global_subject_id = EXCLUDED.global_subject_id,
                                    sample_type = EXCLUDED.sample_type,
                                    redcap_event = EXCLUDED.redcap_event,
                                    project = EXCLUDED.project,
                                    region_location = EXCLUDED.region_location,
                                    year_collected = EXCLUDED.year_collected,
                                    sample_available = EXCLUDED.sample_available
                                """,
                                (
                                    record[source_field],
                                    gsid,
                                    sample_type,
                                    record.get("redcap_event_name"),
                                    self.project_name,
                                    region_location,
                                    year_collected,
                                    sample_available,
                                ),
                            )
                            logger.debug(
                                f"[{self.project_key}] Inserted specimen: "
                                f"{record[source_field]} (type: {sample_type}, "
                                f"region: {region_location})"
                            )

                    # Process all sequence mappings from config
                    elif mapping.get("target_table") == "sequence":
                        source_field = mapping["source_field"]
                        sample_type = mapping.get("sample_type", "unknown")

                        if record.get(source_field):
                            # Extract batch and vcf_sample_id if present in record
                            batch = record.get("batch") or record.get(
                                "sequencing_batch"
                            )
                            vcf_sample_id = record.get("vcf_sample_id") or record.get(
                                "vcf_id"
                            )

                            cur.execute(
                                """
                                INSERT INTO sequence (sample_id, global_subject_id, sample_type, batch, vcf_sample_id)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (sample_id) DO UPDATE SET
                                    global_subject_id = EXCLUDED.global_subject_id,
                                    sample_type = EXCLUDED.sample_type,
                                    batch = EXCLUDED.batch,
                                    vcf_sample_id = EXCLUDED.vcf_sample_id
                                """,
                                (
                                    record[source_field],
                                    gsid,
                                    sample_type,
                                    batch,
                                    vcf_sample_id,
                                ),
                            )
                            logger.debug(
                                f"[{self.project_key}] Inserted sequence: "
                                f"{record[source_field]} (type: {sample_type}, batch: {batch})"
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
            "sequences": {},
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
        sequence_types = {}

        for mapping in mappings:
            if mapping.get("target_table") == "specimen":
                source_field = mapping["source_field"]
                sample_type = mapping.get("sample_type", "unknown")

                if record.get(source_field):
                    if sample_type not in specimen_types:
                        specimen_types[sample_type] = []
                    specimen_types[sample_type].append(record[source_field])

            elif mapping.get("target_table") == "sequence":
                source_field = mapping["source_field"]
                sample_type = mapping.get("sample_type", "unknown")

                if record.get(source_field):
                    if sample_type not in sequence_types:
                        sequence_types[sample_type] = []
                    sequence_types[sample_type].append(record[source_field])

        # Add specimens to fragment
        for sample_type, sample_ids in specimen_types.items():
            if len(sample_ids) == 1:
                fragment["samples"][sample_type] = sample_ids[0]
            else:
                fragment["samples"][sample_type] = sample_ids

        # Add sequences to fragment
        for sample_type, sample_ids in sequence_types.items():
            if len(sample_ids) == 1:
                fragment["sequences"][sample_type] = sample_ids[0]
            else:
                fragment["sequences"][sample_type] = sample_ids

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
            resolution = self.resolve_subject_ids(subject_ids, center_id, record)
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
