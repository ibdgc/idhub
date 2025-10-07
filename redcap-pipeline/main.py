# redcap-pipeline/main.py

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

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



class REDCapPipeline:
    def __init__(self):
        self.redcap_url = os.getenv('REDCAP_API_URL')
        self.redcap_token = os.getenv('REDCAP_API_TOKEN')
        self.redcap_project_id = os.getenv('REDCAP_PROJECT_ID', '16894')
        self.gsid_service_url = os.getenv('GSID_SERVICE_URL', 'http://gsid-service:8000')
        self.s3_bucket = os.getenv('S3_BUCKET', 'idhub-curated-fragments')

        self.s3_client = boto3.client('s3')

        with open('config/field_mappings.json') as f:
            config = json.load(f)
            self.mappings = config['mappings']
            self.transformations = config.get('transformations', {})

        # Store DB config but don't create pool yet
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.db_pool = None

    def ensure_pool(self):
        """Lazy initialization of connection pool"""
        if self.db_pool is None:
            logger.info("Initializing database connection pool...")
            try:
                self.db_pool = psycopg2.pool.SimpleConnectionPool(
                    1, 10,
                    **self.db_config
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

    def get_or_create_center(self, center_name: str) -> int:
        """Get center_id or create if doesn't exist"""
        conn = self.get_db_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            cur.execute("SELECT center_id FROM centers WHERE name = %s", (center_name,))
            result = cur.fetchone()

            if result:
                return result["center_id"]

            cur.execute(
                """
                INSERT INTO centers (name, investigator, country, consortium)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (center_id) DO NOTHING
                RETURNING center_id
            """,
                (center_name, "Unknown", "Unknown", "IBD"),
            )

            result = cur.fetchone()
            conn.commit()

            if result:
                return result["center_id"]

            cur.execute("SELECT center_id FROM centers WHERE name = %s", (center_name,))
            result = cur.fetchone()
            return result["center_id"]

        finally:
            self.return_db_connection(conn)

    def register_subject(self, record: Dict) -> str:
        """Register subject and get GSID"""
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

        response = requests.post(f"{self.gsid_service_url}/register", json=payload)
        response.raise_for_status()

        result = response.json()
        logger.info(
            f"Registered {local_subject_id} -> GSID {result['gsid']} ({result['action']})"
        )

        return result["gsid"]

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

        if record.get("sample_id"):
            fragment["samples"]["dna"] = record["sample_id"]
        if record.get("dna_id"):
            fragment["samples"]["dna"] = record["dna_id"]
        if record.get("dna_blood_id"):
            fragment["samples"]["blood"] = record["dna_blood_id"]

        if record.get("family_id"):
            fragment["family"]["family_id"] = record["family_id"]

        return fragment

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

    def process_record(self, record: Dict):
        """Process single REDCap record with isolated transaction"""
        try:
            gsid = self.register_subject(record)
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
                    if result['status'] == 'success':
                        total_success += 1
                    else:
                        total_errors += 1

                offset += batch_size
                del records

            logger.info(f"Pipeline complete: {total_success} success, {total_errors} errors")

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
