import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError
from core.config import settings

logger = logging.getLogger(__name__)


class S3Uploader:
    def __init__(self):
        self.s3_client = boto3.client("s3", region_name=settings.AWS_DEFAULT_REGION)
        self.bucket = settings.S3_BUCKET

    def create_curated_fragment(
        self,
        record: Dict[str, Any],
        gsid: str,
        center_id: int,
        project_key: str = "default",
        redcap_project_id: str = None,
    ) -> Dict[str, Any]:
        """Create curated data fragment (PHI-free)"""
        fragment = {
            "gsid": gsid,
            "center_id": center_id,
            "samples": {},
            "family": {},
            "metadata": {
                "source": "redcap",
                "project_key": project_key,
                "redcap_project_id": redcap_project_id,
                "pipeline_version": "1.0",
                "processed_at": datetime.utcnow().isoformat(),
            },
        }

        # Group specimens by type from mappings
        specimen_types = {}
        field_mappings = settings.load_field_mappings()

        for mapping in field_mappings.get("mappings", []):
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

    def upload_fragment(
        self,
        record: Dict[str, Any],
        gsid: str,
        center_id: int,
        project_key: str = "default",
        redcap_project_id: str = None,
    ) -> str:
        """Create and upload curated fragment to S3"""
        fragment = self.create_curated_fragment(
            record, gsid, center_id, project_key, redcap_project_id
        )

        # Include project_key in S3 path for organization
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"subjects/{gsid}/redcap_{project_key}_{timestamp}.json"

        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(fragment, indent=2),
                ContentType="application/json",
                ServerSideEncryption="AES256",
                Metadata={
                    "project_key": project_key,
                    "redcap_project_id": redcap_project_id or "unknown",
                    "gsid": gsid,
                    "center_id": str(center_id),
                },
            )
            logger.info(
                f"[{project_key}] Uploaded fragment to s3://{self.bucket}/{key}"
            )
            return key
        except ClientError as e:
            logger.error(f"[{project_key}] Failed to upload fragment to S3: {e}")
            raise

    def upload_batch_summary(
        self,
        results: List[Dict[str, Any]],
        batch_id: str,
        project_key: str = "default",
        redcap_project_id: str = None,
    ) -> str:
        """Upload batch processing summary"""
        key = f"batches/{project_key}/{batch_id}/summary.json"

        summary = {
            "batch_id": batch_id,
            "project_key": project_key,
            "redcap_project_id": redcap_project_id,
            "processed_at": datetime.utcnow().isoformat(),
            "total_records": len(results),
            "successful": sum(1 for r in results if r["status"] == "success"),
            "failed": sum(1 for r in results if r["status"] == "error"),
            "results": results,
        }

        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(summary, indent=2),
                ContentType="application/json",
                ServerSideEncryption="AES256",
                Metadata={
                    "project_key": project_key,
                    "redcap_project_id": redcap_project_id or "unknown",
                    "batch_id": batch_id,
                },
            )
            logger.info(
                f"[{project_key}] Uploaded batch summary to s3://{self.bucket}/{key}"
            )
            return key
        except ClientError as e:
            logger.error(f"[{project_key}] Failed to upload batch summary: {e}")
            raise
