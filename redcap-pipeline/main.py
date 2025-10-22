# redcap-pipeline/main.py

import json
import logging
import os
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

import boto3
import psycopg2
import requests
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

CENTER_ALIASES = {
    "mount_sinai": "MSSM",
    "mount_sinai_ny": "MSSM",
    "mount-sinai": "MSSM",
    "mt_sinai": "MSSM",
    "cedars_sinai": "Cedars-Sinai",
    "cedars-sinai": "Cedars-Sinai",
    "university_of_chicago": "University of Chicago",
    "uchicago": "University of Chicago",
    "u_chicago": "University of Chicago",
    "johns_hopkins": "Johns Hopkins",
    "jhu": "Johns Hopkins",
    "mass_general": "Massachusetts General Hospital",
    "mgh": "Massachusetts General Hospital",
    "pitt": "Pittsburgh",
    "upitt": "Pittsburgh",
    "university_of_pittsburgh": "Pittsburgh",
}


class REDCapPipeline:
    def __init__(self):
        self.redcap_url = os.getenv("REDCAP_API_URL")
        self.redcap_token = os.getenv("REDCAP_API_TOKEN")
        self.redcap_project_id = os.getenv("REDCAP_PROJECT_ID", "16894")
        self.gsid_service_url = os.getenv(
            "GSID_SERVICE_URL", "http://gsid-service:8000"
        )
        self.s3_bucket = os.getenv("S3_BUCKET", "idhub-curated-fragments")

        self.s3_client = boto3.client("s3")

        with open("config/field_mappings.json") as f:
            config = json.load(f)
            self.mappings = config["mappings"]
            self.transformations = config.get("transformations", {})

        # Store DB config but don't create pool yet
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }
        self.db_pool = None
        self._centers_cache = None

    def _load_centers_cache(self):
        """Load all centers into memory for fuzzy matching"""
        if self._centers_cache is not None:
            return

        conn = self.get_db_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT center_id, name FROM centers")
            self._centers_cache = cur.fetchall()
            logger.info(f"Loaded {len(self._centers_cache)} centers into cache")
        finally:
            self.return_db_connection(conn)

    def _fuzzy_match_center(
        self, input_name: str, threshold: float = 0.6
    ) -> Optional[int]:
        """
        Fuzzy match center name using string similarity
        Returns center_id if match found above threshold, None otherwise
        """
        self._load_centers_cache()

        if not input_name:
            return None

        # Normalize input
        input_normalized = input_name.lower().replace("_", "-").replace(" ", "-")

        best_match = None
        best_score = 0.0

        for center in self._centers_cache:
            center_normalized = (
                center["name"].lower().replace("_", "-").replace(" ", "-")
            )

            # Calculate similarity ratio
            score = SequenceMatcher(None, input_normalized, center_normalized).ratio()

            if score > best_score:
                best_score = score
                best_match = center

        if best_score >= threshold:
            logger.info(
                f"Fuzzy matched '{input_name}' -> '{best_match['name']}' (score: {best_score:.2f})"
            )
            return best_match["center_id"]

        logger.warning(
            f"No fuzzy match found for '{input_name}' (best score: {best_score:.2f})"
        )
        return None

    def _normalize_center_name(self, name: str) -> str:
        """Normalize and check aliases"""
        if not name:
            return "Unknown"

        normalized = name.lower().replace(" ", "_").replace("-", "_")

        # Check aliases first
        if normalized in CENTER_ALIASES:
            canonical = CENTER_ALIASES[normalized]
            logger.info(f"Alias matched '{name}' -> '{canonical}'")
            return canonical

        return name

    def get_or_create_center(self, center_name: str) -> int:
        """Get center_id with alias lookup, fuzzy matching, create if no match"""
        # Apply alias normalization first
        center_name = self._normalize_center_name(center_name)

        conn = self.get_db_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            # Try exact match
            cur.execute("SELECT center_id FROM centers WHERE name = %s", (center_name,))
            result = cur.fetchone()
            if result:
                return result["center_id"]

            # Try fuzzy match
            fuzzy_center_id = self._fuzzy_match_center(center_name, threshold=0.7)
            if fuzzy_center_id:
                return fuzzy_center_id

            # No match - create new center
            logger.warning(f"Creating new center: '{center_name}'")
            cur.execute(
                """
                INSERT INTO centers (name, investigator, country, consortium)
                VALUES (%s, %s, %s, %s)
                RETURNING center_id
                """,
                (center_name, "Unknown", "Unknown", "Unknown"),
            )

            result = cur.fetchone()
            conn.commit()

            self._centers_cache = None

            return result["center_id"]

        finally:
            self.return_db_connection(conn)

    def ensure_pool(self):
        """Lazy initialization of connection pool"""
        if self.db_pool is None:
            logger.info("Initializing database connection pool...")
            try:
                self.db_pool = psycopg2.pool.SimpleConnectionPool(
                    1, 10, **self.db_config
                )
            except Exception as e:
                logger.error(f"Failed to create connection pool: {e}")
                raise  # Don't retry, fail fast

    def get_db_connection(self):
        """Get connection from pool"""
        self.ensure_pool()
        return self.db_pool.getconn()

    def return_db_connection(self, conn):
        """Return connection to pool"""
        if self.db_pool:
            self.db_pool.putconn(conn)

    def fetch_redcap_records_batch(
        self, batch_size: int = 100, offset: int = 0
    ) -> List[Dict]:
        """Fetch records in batches"""
        logger.info(f"Fetching batch: offset={offset}, limit={batch_size}")

        payload = {
            "token": self.redcap_token,
            "content": "record",
            "format": "json",
            "type": "flat",
            "rawOrLabel": "raw",
            "exportDataAccessGroups": "true",
        }

        try:
            response = requests.post(self.redcap_url, data=payload, timeout=30)
            response.raise_for_status()

            all_records = response.json()

            return all_records[offset : offset + batch_size]

        except requests.exceptions.Timeout:
            logger.error("REDCap API timeout")
            return []
        except Exception as e:
            logger.error(f"REDCap API error: {str(e)}")
            return []

    def transform_value(self, field_name: str, value: Any) -> Any:
        """Apply transformations to field values"""
        if field_name not in self.transformations:
            return value

        transform = self.transformations[field_name]

        if transform["type"] == "extract_year":
            if not value:
                return None
            return value.split("-")[0] if "-" in value else value

        elif transform["type"] == "boolean":
            if value in transform["true_values"]:
                return True
            elif value in transform["false_values"]:
                return False
            return None

        return value

    def extract_local_ids(self, record: Dict, center_id: int) -> List[Dict]:
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
        conn = self.get_db_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)

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
                        f"CONFLICT: {identifier['local_subject_id']} already linked to "
                        f"{existing['global_subject_id']}, attempting to link to {gsid}"
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
                        f"ID {identifier['local_subject_id']} already linked to {gsid}"
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
                    f"Linked {identifier['identifier_type']}={identifier['local_subject_id']} -> {gsid}"
                )

            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Error registering local IDs for {gsid}: {e}")
            raise
        finally:
            self.return_db_connection(conn)

    def register_subject(self, record: Dict) -> tuple[str, int]:
        """Register subject with primary identifier"""
        center_name = record.get("redcap_data_access_group", "Unknown")
        center_id = self.get_or_create_center(center_name)

        local_subject_id = record.get("consortium_id") or record.get("local_id")

        if not local_subject_id:
            raise ValueError(
                f"No local_subject_id found in record: {record.get('record_id')}"
            )

        registration_date = record.get("registration_date")
        registration_year = self.transform_value("registration_date", registration_date)
        control = self.transform_value("control", record.get("control", "0"))

        payload = {
            "center_id": center_id,
            "local_subject_id": local_subject_id,
            "registration_year": registration_year,
            "control": control,
            "created_by": "redcap_pipeline",
        }

        # Add API key header
        headers = {"x-api-key": os.getenv("GSID_API_KEY")}

        response = requests.post(
            f"{self.gsid_service_url}/register", json=payload, headers=headers
        )
        response.raise_for_status()

        result = response.json()

        identifier_type = "consortium_id" if record.get("consortium_id") else "local_id"
        logger.info(
            f"Registered {local_subject_id} ({identifier_type}) -> GSID {result['gsid']} ({result['action']})"
        )

        return result["gsid"], center_id

    def process_record(self, record: Dict):
        """Process single REDCap record with conflict detection"""
        try:
            # Register subject with primary ID
            gsid, center_id = self.register_subject(record)

            # Extract and register ALL local IDs (with conflict detection)
            all_identifiers = self.extract_local_ids(record, center_id)
            if all_identifiers:
                self.register_all_local_ids(gsid, all_identifiers)

            # Insert samples
            self.insert_samples(record, gsid)

            # Create and upload fragment
            fragment = self.create_curated_fragment(record, gsid)
            self.upload_to_s3(fragment, gsid)

            return {"status": "success", "gsid": gsid}

        except Exception as e:
            logger.error(f"Error processing record {record.get('record_id')}: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "record_id": record.get("record_id"),
            }

    def upload_to_s3(self, fragment: Dict, gsid: str):
        """Upload curated fragment to S3"""
        key = f"subjects/{gsid}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=json.dumps(fragment, indent=2),
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )

        logger.info(f"Uploaded fragment to s3://{self.s3_bucket}/{key}")

    def insert_samples(self, record: Dict, gsid: str):
        """Insert sample records into database using field mappings"""
        conn = self.get_db_connection()
        try:
            cur = conn.cursor()

            # Process all specimen mappings from config
            for mapping in self.mappings:
                if mapping.get("target_table") == "specimen":
                    source_field = mapping["source_field"]
                    sample_type = mapping.get("sample_type")

                    if record.get(source_field):
                        cur.execute(
                            """
                            INSERT INTO specimen (sample_id, global_subject_id, sample_type, redcap_event)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (sample_id) DO UPDATE SET
                                sample_type = EXCLUDED.sample_type,
                                redcap_event = EXCLUDED.redcap_event
                            """,
                            (
                                record[source_field],
                                gsid,
                                sample_type,
                                record.get("redcap_event_name"),
                            ),
                        )
                        logger.debug(
                            f"Inserted specimen: {record[source_field]} (type: {sample_type})"
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

            conn.commit()
            logger.info(f"Inserted samples for GSID {gsid}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting samples for {gsid}: {e}")
            raise
        finally:
            self.return_db_connection(conn)

    def create_curated_fragment(self, record: Dict, gsid: str) -> Dict:
        """Create curated data fragment (PHI-free)"""
        fragment = {
            "gsid": gsid,
            "center_id": self.get_or_create_center(
                record.get("redcap_data_access_group", "Unknown")
            ),
            "samples": {},
            "family": {},
            "metadata": {
                "source": "redcap",
                "pipeline_version": "1.0",
                "processed_at": datetime.utcnow().isoformat(),
            },
        }

        # Group specimens by type from mappings
        specimen_types = {}
        for mapping in self.mappings:
            if mapping.get("target_table") == "specimen":
                source_field = mapping["source_field"]
                sample_type = mapping.get("sample_type")

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

        if record.get("family_id"):
            fragment["family"]["family_id"] = record["family_id"]

        return fragment

    def run(self):
        """Execute pipeline with batch processing"""
        logger.info("Starting REDCap pipeline (batch mode)...")

        batch_size = 50
        offset = 0
        total_success = 0
        total_errors = 0

        try:
            while True:
                records = self.fetch_redcap_records_batch(batch_size, offset)

                if not records:
                    logger.info("No more records to process")
                    break

                logger.info(f"Processing {len(records)} records...")

                for record in records:
                    result = self.process_record(record)
                    if result["status"] == "success":
                        total_success += 1
                    else:
                        total_errors += 1

                offset += batch_size
                del records

            logger.info(
                f"Pipeline complete: {total_success} success, {total_errors} errors"
            )

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            # Close all connections in pool
            if self.db_pool:
                self.db_pool.closeall()


if __name__ == "__main__":
    pipeline = REDCapPipeline()
    pipeline.run()
