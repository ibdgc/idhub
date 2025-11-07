# Table Loader

Database loader service for validated fragment data.

## Overview

This service:

- Loads validated data from S3 staging
- Inserts/updates database tables
- Handles data transformations
- Reports load status

## Supported Tables

- Blood
- LCL
- Specimen
- DNA
- RNA
- Subject metadata

## Configuration

Required environment variables:

- `DB_HOST` - Database host
- `DB_NAME` - Database name
- `DB_USER` - Database user
- `DB_PASSWORD` - Database password
- `S3_BUCKET` - S3 bucket for staging

## Running Tests

```bash
pytest

```
