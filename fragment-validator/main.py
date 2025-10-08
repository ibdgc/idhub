import os
import sys
import argparse
import pandas as pd
import numpy as np
import boto3
import psycopg2
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
import logging
import json
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FragmentValidator:
    def __init__(self, db_config: dict, s3_bucket: str, gsid_service_url: str):
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.gsid_service_url = gsid_service_url
        self.s3_client = boto3.client('s3')
        self.local_id_cache = {}
        self._load_local_id_cache()

    def _load_local_id_cache(self):
        """Pre-load all local_subject_ids into memory for fast lookups"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT center_id, local_subject_id, identifier_type, global_subject_id
                FROM local_subject_ids
            """)

            for center_id, local_id, id_type, gsid in cursor.fetchall():
                key = (center_id, local_id, id_type)
                self.local_id_cache[key] = gsid

            cursor.close()
            conn.close()

            logger.info(f"Loaded {len(self.local_id_cache)} unique local IDs into cache")

        except Exception as e:
            logger.error(f"Failed to load local ID cache: {e}")
            raise

    def process_incoming_file(
        self,
        table_name: str,
        s3_key: str,
        mapping_config: dict,
        source_name: str,
        auto_approve: bool = False
    ) -> dict:
        """Process incoming fragment file through validation pipeline"""

        logger.info(f"Processing {s3_key} for table {table_name}")

        # Load raw data
        raw_data = self._load_from_s3(f"s3://{self.s3_bucket}/{s3_key}")

        # Apply mapping
        mapped_data = self._apply_mapping(raw_data, mapping_config)

        # Validate schema
        validation_errors = self._validate_schema(mapped_data, table_name)

        # Resolve subject IDs
        resolution_results = self._resolve_subject_ids(
            mapped_data, 
            mapping_config.get('subject_id_candidates', []),
            mapping_config.get('center_id_field')
        )

        # Generate batch ID
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Prepare report
        report = {
            'batch_id': batch_id,
            'table_name': table_name,
            'source_name': source_name,
            'timestamp': datetime.now().isoformat(),
            'input_file': s3_key,
            'row_count': len(mapped_data),
            'validation_errors': validation_errors,
            'resolution_summary': resolution_results['summary'],
            'warnings': resolution_results['warnings'],
            'auto_approved': auto_approve
        }

        # Check if validation passed
        if validation_errors:
            report['status'] = 'FAILED'
            logger.error(f"✗ Validation failed with {len(validation_errors)} errors")
            self._print_summary(report)
            return report

        # Add resolved GSIDs to data
        mapped_data['global_subject_id'] = resolution_results['gsids']

        # Write outputs to staging
        self._write_staging_outputs(
            batch_id,
            table_name,
            mapped_data,
            resolution_results['local_id_records'],
            report
        )

        report['status'] = 'VALIDATED'
        report['staging_location'] = f"s3://{self.s3_bucket}/staging/validated/{batch_id}/"

        logger.info(f"✓ Validation complete: {batch_id}")
        self._print_summary(report)

        return report

    def _apply_mapping(self, raw_data: pd.DataFrame, mapping_config: dict) -> pd.DataFrame:
        """Apply field mapping from config"""

        field_map = mapping_config.get('field_mapping', {})
        mapped_data = pd.DataFrame()

        for target_field, source_field in field_map.items():
            if source_field in raw_data.columns:
                mapped_data[target_field] = raw_data[source_field]
            else:
                logger.warning(f"Source field '{source_field}' not found in input data")
                mapped_data[target_field] = None

        return mapped_data

    def _validate_schema(self, data: pd.DataFrame, table_name: str) -> List[dict]:
        """Validate data against target table schema"""

        errors = []

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Get table schema
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name != 'global_subject_id'
                ORDER BY ordinal_position
            """, (table_name,))

            schema = cursor.fetchall()
            cursor.close()
            conn.close()

            # Check required columns exist
            for col_name, data_type, is_nullable in schema:
                if col_name not in data.columns:
                    if is_nullable == 'NO':
                        errors.append({
                            'type': 'missing_required_column',
                            'column': col_name,
                            'message': f"Required column '{col_name}' not found in data"
                        })

            # Check for null values in NOT NULL columns
            for col_name, data_type, is_nullable in schema:
                if col_name in data.columns and is_nullable == 'NO':
                    null_count = data[col_name].isna().sum()
                    if null_count > 0:
                        errors.append({
                            'type': 'null_in_required_column',
                            'column': col_name,
                            'null_count': int(null_count),
                            'message': f"Column '{col_name}' has {null_count} null values but is NOT NULL"
                        })

        except Exception as e:
            errors.append({
                'type': 'schema_validation_error',
                'message': str(e)
            })

        return errors

    def _resolve_subject_ids(
        self, 
        data: pd.DataFrame, 
        candidate_fields: List[str],
        center_id_field: Optional[str] = None
    ) -> dict:
        """Resolve local subject IDs to GSIDs, minting new ones as needed"""

        gsids = []
        local_id_records = []
        warnings = []
        stats = {
            'existing_matches': 0,
            'new_gsids_minted': 0,
            'unknown_center_used': 0,
            'center_promoted': 0
        }

        for idx, row in data.iterrows():
            center_id = int(row[center_id_field]) if center_id_field and pd.notna(row.get(center_id_field)) else 0

            if center_id == 0:
                stats['unknown_center_used'] += 1

            # Try to find existing GSID from candidate fields
            found_gsid = None
            matched_local_id = None
            matched_id_type = None

            for field in candidate_fields:
                if field in row and pd.notna(row[field]):
                    local_id = str(row[field])
                    id_type = field  # Use field name as identifier_type

                    # Check cache for existing mapping
                    cache_key = (center_id, local_id, id_type)
                    if cache_key in self.local_id_cache:
                        found_gsid = self.local_id_cache[cache_key]
                        matched_local_id = local_id
                        matched_id_type = id_type
                        stats['existing_matches'] += 1
                        break

                    # Also check with center_id=0 (unknown) for potential promotion
                    unknown_key = (0, local_id, id_type)
                    if center_id != 0 and unknown_key in self.local_id_cache:
                        found_gsid = self.local_id_cache[unknown_key]
                        matched_local_id = local_id
                        matched_id_type = id_type
                        stats['center_promoted'] += 1

                        # Record promotion in local_id_records
                        local_id_records.append({
                            'center_id': center_id,
                            'local_subject_id': local_id,
                            'identifier_type': id_type,
                            'global_subject_id': found_gsid,
                            'action': 'promote_center'
                        })

                        # Update cache
                        self.local_id_cache[(center_id, local_id, id_type)] = found_gsid
                        break

            # If no existing GSID found, mint new one
            if not found_gsid:
                found_gsid = self._mint_new_gsid()
                stats['new_gsids_minted'] += 1

                # Record all candidate IDs for this new subject
                for field in candidate_fields:
                    if field in row and pd.notna(row[field]):
                        local_id = str(row[field])
                        id_type = field

                        local_id_records.append({
                            'center_id': center_id,
                            'local_subject_id': local_id,
                            'identifier_type': id_type,
                            'global_subject_id': found_gsid,
                            'action': 'new'
                        })

                        # Update cache
                        cache_key = (center_id, local_id, id_type)
                        self.local_id_cache[cache_key] = found_gsid

            gsids.append(found_gsid)

        # Generate warnings
        if stats['unknown_center_used'] > 0:
            warnings.append(f"{stats['unknown_center_used']} records used center_id=0 (Unknown)")

        if stats['center_promoted'] > 0:
            warnings.append(f"{stats['center_promoted']} records promoted from Unknown to known center")

        return {
            'gsids': gsids,
            'local_id_records': local_id_records,
            'summary': stats,
            'warnings': warnings
        }

    def _mint_new_gsid(self) -> str:
        """Request new GSID from gsid-service"""
        try:
            # Use /register endpoint with minimal payload
            payload = {
                "center_id": 0,  # Unknown center
                "local_subject_id": f"temp_{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
                "created_by": "fragment_validator"
            }
            response = requests.post(f"{self.gsid_service_url}/register", json=payload)
            response.raise_for_status()
            return response.json()['gsid']
        except Exception as e:
            logger.error(f"Failed to mint GSID: {e}")
            raise

    def _write_staging_outputs(
        self,
        batch_id: str,
        table_name: str,
        data: pd.DataFrame,
        local_id_records: List[dict],
        report: dict
    ):
        """Write validated data and metadata to staging area"""

        staging_prefix = f"staging/validated/{batch_id}"

        # Write main table data
        table_csv = data.to_csv(index=False)
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=f"{staging_prefix}/{table_name}.csv",
            Body=table_csv
        )

        # Write local_subject_ids records
        if local_id_records:
            local_ids_df = pd.DataFrame(local_id_records)
            local_ids_csv = local_ids_df.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=f"{staging_prefix}/local_subject_ids.csv",
                Body=local_ids_csv
            )

        # Write validation report
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=f"{staging_prefix}/validation_report.json",
            Body=json.dumps(report, indent=2)
        )

        logger.info(f"Staging outputs written to s3://{self.s3_bucket}/{staging_prefix}/")

    def _load_from_s3(self, s3_path: str) -> pd.DataFrame:
        """Load CSV from S3"""
        bucket, key = s3_path.replace('s3://', '').split('/', 1)
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'])

    def _print_summary(self, report: dict):
        """Print validation summary"""
        print("\n" + "="*70)
        print(f"VALIDATION SUMMARY - {report['batch_id']}")
        print("="*70)
        print(f"Table: {report['table_name']}")
        print(f"Source: {report['source_name']}")
        print(f"Rows: {report['row_count']}")
        print(f"Status: {report['status']}")
    
        if report['status'] == 'VALIDATED':
            stats = report['resolution_summary']
            print(f"\nSubject Resolution:")
            print(f"  - Existing matches: {stats['existing_matches']}")
            print(f"  - New GSIDs minted: {stats['new_gsids_minted']}")
            print(f"  - Unknown center used: {stats['unknown_center_used']}")
            print(f"  - Centers promoted: {stats['center_promoted']}")
    
            if report['warnings']:
                print(f"\nWarnings:")
                for warning in report['warnings']:
                    print(f"  ⚠ {warning}")
    
            print(f"\nStaging: {report['staging_location']}")
        else:
            print(f"\nValidation Errors ({len(report['validation_errors'])}):")
            for error in report['validation_errors']:
                print(f"  ✗ [{error['type']}] {error['message']}")
                if 'column' in error:
                    print(f"    Column: {error['column']}")
                if 'null_count' in error:
                    print(f"    Null count: {error['null_count']}")
    
        print("="*70 + "\n")

def main():
    parser = argparse.ArgumentParser(description='Validate and stage data fragments')
    parser.add_argument('table_name', help='Target database table name')
    parser.add_argument('s3_key', help='S3 key of input file (relative to bucket)')
    parser.add_argument('mapping_config', help='Path to mapping config JSON file')
    parser.add_argument('--source', required=True, help='Source system name')
    parser.add_argument('--auto-approve', action='store_true', help='Auto-approve for loading')

    args = parser.parse_args()

    # Load mapping config
    with open(args.mapping_config, 'r') as f:
        mapping_config = json.load(f)

    # Database config
    db_config = {
        'host': os.getenv('DB_HOST', 'idhub_db'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

    s3_bucket = os.getenv('S3_BUCKET', 'idhub-curated-fragments')
    gsid_service_url = os.getenv('GSID_SERVICE_URL', 'http://gsid_service:8000')

    try:
        validator = FragmentValidator(db_config, s3_bucket, gsid_service_url)
        report = validator.process_incoming_file(
            args.table_name,
            args.s3_key,
            mapping_config,
            args.source,
            args.auto_approve
        )

        if report['status'] == 'FAILED':
            logger.error("✗ Validation failed")
            sys.exit(1)
        else:
            logger.info("✓ Validation successful")
            sys.exit(0)

    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
