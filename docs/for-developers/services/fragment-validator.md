# Fragment Validator Documentation

## Overview

The Fragment Validator is a critical data pipeline service responsible for ensuring the quality, integrity, and consistency of data before it is staged for loading into the IDhub database. It acts as a gatekeeper for manually uploaded data and can also be used to re-validate data from other pipelines.

It processes input files (like CSVs or Excel spreadsheets), validates each record against a series of configurable rules, resolves subject identifiers, and generates standardized, validated "fragments" that are placed into a validation queue for the final loading step.

## Architecture

```mermaid
graph TB
    subgraph "Input"
        INPUT[CSV / Excel File]
    end

    subgraph "Fragment Validator Service"
        MAIN[main.py - CLI Entrypoint]
        CONFIG[Configuration Loader]

        subgraph "Validation Pipeline"
            PARSE[1. File Parser]
            MAP[2. Field Mapper]
            SCHEMA[3. Schema Validator]
            GSID[4. GSID Resolver]
            BIZ[5. Business Rule Validator]
        end

        subgraph "Output Processing"
            FRAG[6. Fragment Generator]
            S3[7. S3 Uploader]
            QUEUE[8. Queue Inserter]
        end

        subgraph "External Dependencies"
            GSID_SVC[GSID Service]
            DB[(PostgreSQL)]
            S3_BUCKET[(S3 Bucket)]
        end
    end

    INPUT --> MAIN
    MAIN --> CONFIG
    CONFIG -- table_configs.json --> SCHEMA & BIZ
    CONFIG -- field_mapping.json --> MAP

    MAIN --> PARSE
    PARSE --> MAP
    MAP --> SCHEMA
    SCHEMA --> GSID

    GSID -- Resolve/Generate --> GSID_SVC
    GSID --> BIZ

    BIZ --> FRAG
    FRAG --> S3
    S3 --> S3_BUCKET
    S3 --> QUEUE
    QUEUE -- Insert record --> DB

    style S3_BUCKET fill:#FF9800
    style DB fill:#4CAF50
    style GSID_SVC fill:#9C27B0
```

## Key Features

-   **Schema Validation**: Ensures each record conforms to the expected data types, required fields, and constraints.
-   **Subject ID Resolution**: Integrates with the GSID Service to resolve local subject identifiers to a Global Subject ID (GSID), a critical step for data integration.
-gaps**Conflict Detection**: Identifies records that might conflict with existing data, such as `center_mismatch` or `subject_id_mismatch`.
-   **Update Detection**: Compares incoming data against existing records to determine if it's a new record or an update.
-   **Configurable Mappings**: Uses JSON configuration files to define field mappings, validation rules, and table-specific logic, making the service adaptable to new data types.
-   **Standardized Output**: Generates consistent JSON "fragments" that are staged in S3, providing a clear audit trail and a uniform input format for the Table Loader service.

## Configuration

The Fragment Validator relies on several configuration files to direct its behavior.

### Table Configuration (`config/table_configs.json`)

This file defines the schema, validation rules, and natural keys for each data table.

```json
{
  "lcl": {
    "schema": {
      "type": "object",
      "properties": {
        "consortium_id": {"type": "string"},
        "center_id": {"type": "integer"},
        "knumber": {"type": "string"},
        "niddk_no": {"type": "string"},
        "passage_number": {"type": "integer"},
        "freeze_date": {"type": "string", "format": "date"}
      },
      "required": ["consortium_id", "knumber", "niddk_no"]
    },
    "natural_key": ["global_subject_id", "niddk_no"]
  },
  "genotype": {
    "schema": {
        "type": "object",
        "properties": {
            "genotype_id": {"type": "string"},
            "genotyping_project": {"type": "string"}
        },
        "required": ["genotype_id"]
    },
    "natural_key": ["global_subject_id", "genotype_id"]
  }
}
```

### Field Mapping (`config/*_mapping.json`)

Each data source or type has its own mapping file to translate source column names to the target schema fields.

```json
{
  "field_mapping": {
    "consortium_id": "Subject ID",
    "knumber": "K Number",
    "niddk_no": "NIDDK",
    "passage_number": "Passage",
    "freeze_date": "Frozen Date"
  },
  "subject_id_candidates": ["consortium_id"],
  "center_id_field": "center_name",
  "transformations": {
      "freeze_date": "date",
      "passage_number": "integer"
  }
}
```

-   `field_mapping`: Maps target fields (keys) to source file column names (values).
-   `subject_id_candidates`: A list of columns to try, in order, to resolve the subject's GSID.
-   `center_id_field`: The column containing the center's name or ID.
-   `transformations`: Specifies type conversions to be applied to a field's value.

### Environment Variables

```bash
# GSID Service
GSID_SERVICE_URL=http://localhost:8000
GSID_API_KEY=your_gsid_api_key

# AWS (for S3 and Queue)
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_ENDPOINT_URL=http://localhost:4566 # For local development with LocalStack
S3_BUCKET=idhub-dev-fragments

# Database (for queue insertion)
DATABASE_URL=postgresql://user:password@host:port/database

LOG_LEVEL=INFO
```

## Validation Workflow

The service executes a pipeline of validation and processing steps for each record in an input file.

1.  **Parse Input File**: The input file (CSV/Excel) is parsed row by row.
2.  **Apply Field Mappings**: Each row is converted into a dictionary, with keys transformed from source column names to target field names based on the mapping configuration.
3.  **Schema Validation**: The record is validated against its JSON Schema definition from `table_configs.json`. This checks for:
    -   Presence of `required` fields.
    -   Correct `type` for each field (string, integer, etc.).
    -   Correct `format` for specific fields (e.g., date, email).
4.  **Subject ID (GSID) Resolution**:
    -   The validator extracts potential local IDs from the fields listed in `subject_id_candidates`.
    -   It calls the GSID Service to resolve the local ID to a GSID.
    -   If the ID cannot be resolved, the service may be configured to attempt to generate a new GSID or flag the record as a conflict.
    -   The resolved/generated GSID is added to the record.
5.  **Business Rule Validation**: More complex, domain-specific rules are checked. This can include:
    -   **Conflict Detection**: Checking for potential duplicates or conflicting information (e.g., a subject ID linked to a different center).
    -   **Update Detection**: Comparing the record against an existing version in the database to see if it's a true update.
6.  **Fragment Generation**: If a record passes all validation steps, a standardized JSON "fragment" is created. This fragment contains the validated data, the resolved GSID, and metadata about the ingestion process (source, batch ID, timestamp).
7.  **S3 Upload**: The JSON fragment is uploaded to a designated path in the S3 staging bucket.
8.  **Queue Insertion**: A new entry is inserted into the `validation_queue` table in the database. This entry points to the fragment's S3 location and sets its status to `pending`, making it available for the Table Loader service.

## Error Handling

If a record fails any validation step, it is not queued for loading. Instead, a detailed error report is generated.

-   **Error Output**: The service produces a summary file (e.g., `validation_errors.json`) that lists each failed record, its original line number, and a description of each validation error.
-   **Example Error Report**:

```json
[
  {
    "record_number": 5,
    "data": {"consortium_id": "SUBJ-005", "niddk_no": null},
    "errors": [
      {
        "field": "niddk_no",
        "error": "Missing required field"
      }
    ]
  },
  {
    "record_number": 12,
    "data": {"consortium_id": "SUBJ-012", "passage_number": "eight"},
    "errors": [
      {
        "field": "passage_number",
        "error": "Invalid type: expected integer, got string"
      }
    ]
  }
]
```

This report allows data curators to quickly identify and correct issues in the source data file.

## Usage

The Fragment Validator is typically run as a command-line tool.

### Command-Line Arguments

```bash
python main.py \
  --table-name <table_name> \
  --input-file <path_to_input_file> \
  --mapping-config <path_to_mapping_config> \
  --source "<source_description>" \
  --batch-id <optional_batch_id> \
  --dry-run
```

-   `--table-name`: The target database table (e.g., `lcl`, `genotype`).
-   `--input-file`: Path to the source CSV or Excel file.
-   `--mapping-config`: Path to the field mapping JSON file.
-   `--source`: A string describing the data source (e.g., "Manual upload by Curator X").
-   `--batch-id`: (Optional) A specific ID for the batch. If omitted, one is generated.
-   `--dry-run`: (Optional) Runs the entire validation process without uploading to S3 or creating a queue entry. Useful for testing.

### Example

```bash
# Validate an LCL data file
python main.py \
  --table-name lcl \
  --input-file /path/to/lcl_data.csv \
  --mapping-config config/lcl_mapping.json \
  --source "LCL inventory update 2025-12-05"
```

## Testing

The service includes a comprehensive test suite.

```bash
# Navigate to the service directory
cd fragment-validator

# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
pytest

# Run tests with coverage report
pytest --cov=. --cov-report=html
```

## Related Documentation

-   [System Architecture](../architecture/overview.md)
-   [Data Flow](../architecture/data-flow.md)
-   [GSID Service](./gsid-service.md)
-   [Table Loader Service](./table-loader.md)

