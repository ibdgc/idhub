# Fragment Validator

Data validation and staging service for fragment files.

## Overview

This service:

- Validates fragment data files against schemas
- Resolves GSIDs for subjects
- Stages validated data to S3
- Reports validation results to NocoDB

## Supported Tables

- Blood
- LCL (Lymphoblastoid Cell Lines)
- Specimen
- DNA
- RNA

## Configuration

Required environment variables:

- `S3_BUCKET` - S3 bucket for staging
- `GSID_SERVICE_URL` - GSID service endpoint
- `NOCODB_URL` - NocoDB instance URL
- `NOCODB_API_TOKEN` - NocoDB API token

## Running Tests

```bash
pytest

```
