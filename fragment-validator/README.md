# Fragment Validator

Data validation and staging service for curated fragment files in the IDhub platform.

## Overview

The Fragment Validator service validates data fragments against target table schemas, resolves subject identifiers to GSIDs, and stages validated data for loading into the database. It acts as a quality gate between raw data sources and the production database.

### Key Features

- **Schema Validation**: Validates data against NocoDB table metadata
- **GSID Resolution**: Automatic subject ID resolution via GSID service
- **Field Mapping**: Flexible JSON-based field mapping configurations
- **S3 Integration**: Reads from and writes to S3 staging buckets
- **NocoDB Integration**: Reports validation results and manages approval workflow
- **Multi-Table Support**: Handles LCL, specimen, DNA, RNA, and other sample tables
- **Batch Processing**: Efficient processing of large datasets
- **Validation Reports**: Detailed error and warning reports
- **Auto-Approval**: Optional automatic approval for trusted sources

## Architecture

```
fragment-validator/
├── config/
│   ├── table_schemas.json           # Table schema definitions
│   ├── lcl_mapping.json             # LCL field mappings
│   ├── specimen_mapping.json        # Specimen field mappings
│   ├── dna_mapping.json             # DNA field mappings
│   └── rna_mapping.json             # RNA field mappings
├── core/
│   ├── __init__.py
│   └── config.py                    # Configuration helpers
├── services/
│   ├── __init__.py
│   ├── validator.py                 # Main validation orchestrator
│   ├── schema_validator.py          # Schema validation logic
│   ├── field_mapper.py              # Field mapping transformer
│   ├── subject_id_resolver.py       # GSID resolution
│   ├── gsid_client.py               # GSID service client
│   ├── nocodb_client.py             # NocoDB API client
│   └── s3_client.py                 # S3 operations
├── tests/
│   ├── conftest.py                  # Pytest fixtures
│   ├── test_validator.py
│   ├── test_schema_validator.py
│   ├── test_field_mapper.py
│   ├── test_subject_id_resolver.py
│   └── test_integration.py
├── main.py                          # CLI entry point
├── requirements.txt
├── requirements-test.txt
├── Dockerfile
├── Dockerfile.test
├── pytest.ini
└── .coveragerc
```

## Validation Flow

```
┌─────────────────┐
│  Input CSV File │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Load Raw Data  │ ← Read CSV into DataFrame
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apply Mapping  │ ← FieldMapper (source → target fields)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Validate Schema │ ← SchemaValidator (check against NocoDB)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Resolve GSIDs  │ ← SubjectIDResolver (register subjects)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Stage to S3    │ ← S3Client (upload validated fragment)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Report Results  │ → NocoDB (validation_queue table)
└─────────────────┘
```

## Configuration

### Environment Variables

| Variable                | Description           | Required | Default                    |
| ----------------------- | --------------------- | -------- | -------------------------- |
| `S3_BUCKET`             | S3 bucket for staging | Yes      | `idhub-curated-fragments`  |
| `AWS_ACCESS_KEY_ID`     | AWS access key        | Yes      | -                          |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key        | Yes      | -                          |
| `AWS_DEFAULT_REGION`    | AWS region            | No       | `us-east-1`                |
| `GSID_SERVICE_URL`      | GSID service endpoint | Yes      | `http://gsid-service:8000` |
| `GSID_API_KEY`          | GSID service API key  | Yes      | -                          |
| `NOCODB_URL`            | NocoDB instance URL   | Yes      | -                          |
| `NOCODB_API_TOKEN`      | NocoDB API token      | Yes      | -                          |
| `NOCODB_BASE_ID`        | NocoDB base ID        | No       | Auto-detected              |

### Example `.env` File

```bash
# S3 Configuration
S3_BUCKET=idhub-curated-fragments
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=us-east-1

# GSID Service
GSID_SERVICE_URL=http://gsid-service:8000
GSID_API_KEY=your-secure-api-key

# NocoDB
NOCODB_URL=https://nocodb.idhub.ibdgc.org
NOCODB_API_TOKEN=your_nocodb_token
NOCODB_BASE_ID=base_xxxxxxxxxxxxx
```

## Mapping Configuration

### Field Mapping Structure

Each table has a JSON mapping configuration defining how source fields map to target schema:

```json
{
  "field_mapping": {
    "target_field_1": "source_field_1",
    "target_field_2": "source_field_2"
  },
  "subject_id_candidates": ["consortium_id", "local_id", "subject_id"],
  "center_id_field": "center_name",
  "default_center_id": 1,
  "exclude_from_load": ["consortium_id", "center_id"]
}
```

**Configuration Fields**:

- `field_mapping`: Direct field name mappings (target → source)
- `subject_id_candidates`: List of fields to try for subject identification (in order)
- `center_id_field`: Field containing center identifier (optional)
- `default_center_id`: Fallback center ID if not specified
- `exclude_from_load`: Fields to exclude from final database load

### Example: LCL Mapping (`config/lcl_mapping.json`)

```json
{
  "field_mapping": {
    "knumber": "knumber",
    "niddk_no": "niddk_no",
    "cell_line_name": "cell_line_name",
    "passage_number": "passage_number",
    "freeze_date": "freeze_date",
    "vial_count": "vial_count",
    "location": "storage_location",
    "notes": "comments"
  },
  "subject_id_candidates": ["consortium_id", "subject_id"],
  "center_id_field": null,
  "default_center_id": 1,
  "exclude_from_load": ["consortium_id", "center_id"]
}
```

## Usage

### Command Line Interface

```bash
# Validate local CSV file
python main.py \
  --table-name lcl \
  --input-file /path/to/lcl_data.csv \
  --mapping-config config/lcl_mapping.json \
  --source "LabKey Export" \
  --auto-approve

# Without auto-approval (manual review required)
python main.py \
  --table-name lcl \
  --input-file /path/to/lcl_samples.csv \
  --mapping-config config/lcl_mapping.json \
  --source "Legacy ID DB"
```

**Arguments**:

- `--table-name`: Target database table name (e.g., `lcl`, `specimen`)
- `--input-file`: Path to input CSV file
- `--mapping-config`: Path to field mapping JSON configuration
- `--source`: Source system identifier (for audit trail)
- `--auto-approve`: Automatically approve for loading (optional)

### Docker Compose

```bash
# Run validation
docker-compose run --rm fragment-validator python main.py \
  --table-name lcl \
  --input-file /data/lcl_export.csv \
  --mapping-config config/lcl_mapping.json \
  --source "LabKey"

# Mount local file
docker-compose run --rm \
  -v /local/path/data.csv:/app/data/data.csv \
  fragment-validator python main.py \
  --table-name specimen \
  --input-file /app/data/data.csv \
  --mapping-config config/specimen_mapping.json \
  --source "Manual Upload"
```

### Programmatic Usage

```python
from services import FragmentValidator, S3Client, NocoDBClient, GSIDClient
import os

# Initialize clients
s3_client = S3Client(bucket=os.getenv("S3_BUCKET"))
nocodb_client = NocoDBClient(
    url=os.getenv("NOCODB_URL"),
    token=os.getenv("NOCODB_API_TOKEN")
)
gsid_client = GSIDClient(
    service_url=os.getenv("GSID_SERVICE_URL"),
    api_key=os.getenv("GSID_API_KEY")
)

# Create validator
validator = FragmentValidator(s3_client, nocodb_client, gsid_client)

# Process file
result = validator.process_local_file(
    table_name="lcl",
    local_file_path="/path/to/data.csv",
    mapping_config={"field_mapping": {...}, ...},
    source_name="LabKey Export",
    auto_approve=False
)

print(f"Validation: {result['validation_status']}")
print(f"Records processed: {result['total_records']}")
print(f"Errors: {len(result['errors'])}")
```

## Components

### 1. Schema Validator (`services/schema_validator.py`)

Validates data against NocoDB table metadata.

**Features**:

- Fetches live schema from NocoDB
- Validates required fields
- Checks data types
- Validates field constraints
- Skips system/auto-generated columns

**System Columns (Auto-Skipped)**:

- `created_at`
- `updated_at`
- `global_subject_id` (resolved during processing)
- `Id`

**Example**:

```python
from services.schema_validator import SchemaValidator

validator = SchemaValidator(nocodb_client)
result = validator.validate(dataframe, table_name="lcl")

if result.is_valid:
    print("✓ Schema validation passed")
else:
    for error in result.errors:
        print(f"✗ {error['field']}: {error['message']}")
```

**Validation Result**:

```python
@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[dict]      # [{"field": "...", "message": "...", "row": 5}]
    warnings: List[str]     # ["Missing optional field: notes"]
```

### 2. Field Mapper (`services/field_mapper.py`)

Transforms source data to target schema using mapping configuration.

**Features**:

- Direct field name mapping
- Auto-inclusion of subject ID candidates
- Auto-inclusion of center ID field
- Handles missing source fields gracefully
- Preserves unmapped fields for reference

**Example**:

```python
from services.field_mapper import FieldMapper

mapped_data = FieldMapper.apply_mapping(
    raw_data=source_df,
    field_mapping={"knumber": "k_number", "niddk_no": "niddk"},
    subject_id_candidates=["consortium_id"],
    center_id_field="center_name"
)
```

**Mapping Logic**:

1. Map explicitly defined fields from `field_mapping`
2. Auto-include all `subject_id_candidates` fields
3. Auto-include `center_id_field` if specified
4. Log warnings for missing source fields
5. Return DataFrame with target schema columns

### 3. Subject ID Resolver (`services/subject_id_resolver.py`)

Resolves subject identifiers to GSIDs using the GSID service.

**Features**:

- Tries multiple candidate fields in order
- Batch registration for efficiency
- Handles center ID resolution
- Tracks resolution statistics
- Generates local ID records for database

**Example**:

```python
from services.subject_id_resolver import SubjectIDResolver

resolver = SubjectIDResolver(gsid_client)
result = resolver.resolve_batch(
    data=dataframe,
    candidate_fields=["consortium_id", "local_id"],
    center_id_field="center_name",
    default_center_id=1,
    created_by="fragment_validator"
)

print(f"Resolved {result['summary']['resolved']} GSIDs")
print(f"New subjects: {result['summary']['new_subjects']}")
print(f"Existing subjects: {result['summary']['existing_subjects']}")
```

**Resolution Result**:

```python
{
    "gsids": ["GSID-...", "GSID-...", ...],
    "local_id_records": [
        {
            "gsid": "GSID-...",
            "center_id": 1,
            "local_subject_id": "SUBJ001",
            "identifier_type": "primary"
        },
        ...
    ],
    "summary": {
        "total": 100,
        "resolved": 98,
        "new_subjects": 45,
        "existing_subjects": 53,
        "failed": 2
    },
    "warnings": ["Row 5: No valid subject ID found"]
}
```

### 4. GSID Client (`services/gsid_client.py`)

Communicates with GSID service for subject registration.

**Features**:

- Batch registration (configurable batch size)
- Automatic retry on transient failures
- Timeout handling
- Session management
- Detailed logging

**Example**:

```python
from services.gsid_client import GSIDClient

client = GSIDClient(
    service_url="http://gsid-service:8000",
    api_key="your-api-key"
)

# Batch registration
requests = [
    {
        "center_id": 1,
        "local_subject_id": "SUBJ001",
        "identifier_type": "primary"
    },
    {
        "center_id": 1,
        "local_subject_id": "SUBJ002",
        "identifier_type": "primary"
    }
]

results = client.register_batch(
    requests_list=requests,
    batch_size=100,
    timeout=60
)

for result in results:
    print(f"{result['local_subject_id']} → {result['gsid']}")
```

### 5. NocoDB Client (`services/nocodb_client.py`)

Manages NocoDB API interactions for schema and validation queue.

**Features**:

- Auto-detect base ID
- Fetch table metadata
- Load local ID cache
- Create validation queue records
- Update record status

**Example**:

```python
from services.nocodb_client import NocoDBClient

client = NocoDBClient(
    url="https://nocodb.idhub.ibdgc.org",
    token="your_token",
    base_id="base_xxxxx"  # Optional, auto-detected if not provided
)

# Get table metadata
metadata = client.get_table_metadata("lcl")
print(f"Table has {len(metadata['columns'])} columns")

# Load local ID cache
cache = client.load_local_id_cache()
print(f"Cached {len(cache)} local IDs")

# Create validation queue record
queue_id = client.create_validation_queue_record(
    table_name="lcl",
    source="LabKey",
    s3_key="staging/lcl/batch_20240115.json",
    total_records=100,
    validation_status="pending_review"
)
```

### 6. S3 Client (`services/s3_client.py`)

Handles S3 operations for staging and archiving fragments.

**Features**:

- Upload validated fragments
- Download fragments for processing
- Move fragments between folders
- List fragments by status
- Server-side encryption

**S3 Folder Structure**:

```
s3://idhub-curated-fragments/
├── staging/                    # Validated, pending approval
│   ├── lcl/
│   │   ├── batch_20240115_103000.json
│   │   └── batch_20240116_143000.json
│   └── specimen/
├── approved/                   # Approved for loading
│   └── lcl/
└── loaded/                     # Successfully loaded
│   └── lcl/
```

**Example**:

```python
from services.s3_client import S3Client

client = S3Client(bucket="idhub-curated-fragments")

# Upload fragment
s3_key = client.upload_fragment(
    data=validated_data,
    table_name="lcl",
    source="LabKey",
    metadata={"total_records": 100, "validation_status": "passed"}
)
print(f"Uploaded to: {s3_key}")

# Download fragment
fragment = client.download_fragment(s3_key)

# Mark as loaded (move to loaded/ folder)
client.mark_fragment_as_loaded(s3_key)
```

### 7. Fragment Validator (`services/validator.py`)

Main orchestrator coordinating all validation steps.

**Validation Pipeline**:

1. Load raw CSV data
2. Apply field mapping
3. Validate against schema
4. Resolve subject GSIDs
5. Add GSID column to data
6. Stage to S3
7. Report to NocoDB validation queue

**Example**:

```python
from services.validator import FragmentValidator

validator = FragmentValidator(s3_client, nocodb_client, gsid_client)

result = validator.process_local_file(
    table_name="lcl",
    local_file_path="/data/lcl_export.csv",
    mapping_config={
        "field_mapping": {"knumber": "k_number"},
        "subject_id_candidates": ["consortium_id"],
        "center_id_field": None,
        "default_center_id": 1,
        "exclude_from_load": ["consortium_id"]
    },
    source_name="LabKey Export",
    auto_approve=False
)
```

**Result Structure**:

```python
{
    "validation_status": "passed",  # or "failed"
    "total_records": 100,
    "valid_records": 98,
    "errors": [
        {
            "row": 5,
            "field": "knumber",
            "message": "Required field missing"
        }
    ],
    "warnings": [
        "Row 10: Optional field 'notes' is empty"
    ],
    "gsid_resolution": {
        "resolved": 98,
        "new_subjects": 45,
        "existing_subjects": 53,
        "failed": 2
    },
    "s3_key": "staging/lcl/batch_20240115_103000.json",
    "nocodb_queue_id": 123,
    "approved_for_load": false
}
```

## Supported Tables

### Sample Tables

| Table      | Description               | Key Fields                                        |
| ---------- | ------------------------- | ------------------------------------------------- |
| `lcl`      | Lymphoblastoid cell lines | knumber, niddk_no, cell_line_name, passage_number |
| `specimen` | General specimen tracking | specimen_id, specimen_type, collection_date       |
| `dna`      | DNA extraction records    | dna_id, extraction_date, concentration, quality   |
| `rna`      | RNA extraction records    | rna_id, extraction_date, rin_score, concentration |

### Adding New Tables

1. **Create mapping configuration**:

   ```bash
   cp config/lcl_mapping.json config/new_table_mapping.json
   # Edit field mappings
   ```

2. **Add table schema to NocoDB**:

   - Create table in NocoDB with required columns
   - Set data types and constraints
   - Add to appropriate base

3. **Test validation**:
   ```bash
   python main.py \
     --table-name new_table \
     --input-file test_data.csv \
     --mapping-config config/new_table_mapping.json \
     --source "Test"
   ```

## Development

### Local Setup

```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-test.txt

# Set environment variables
cp .env.example .env
# Edit .env with your configuration

# Run validator
python main.py \
  --table-name lcl \
  --input-file test_data.csv \
  --mapping-config config/lcl_mapping.json \
  --source "Test"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/test_validator.py -v

# Run with markers
pytest -m unit
pytest -m integration
```

### Test Coverage

Current coverage: **~82%**

Coverage reports:

- Terminal: `pytest --cov=. --cov-report=term-missing`
- HTML: `htmlcov/index.html`
- XML: `coverage.xml`
- JUnit: `test-reports/junit.xml`

### Docker Development

```bash
# Build image
docker build -t fragment-validator:latest .

# Run container
docker run --rm \
  -v /local/data:/app/data \
  -e GSID_API_KEY=your_key \
  -e NOCODB_API_TOKEN=your_token \
  fragment-validator:latest \
  python main.py \
    --table-name lcl \
    --input-file /app/data/lcl.csv \
    --mapping-config config/lcl_mapping.json \
    --source "Test"

# Run tests in Docker
docker build -f Dockerfile.test -t fragment-validator:test .
docker run fragment-validator:test
```

## Output Examples

### Successful Validation

```
2024-01-15 10:30:00 - INFO - Starting validation for table: lcl
2024-01-15 10:30:01 - INFO - Loaded 100 records from /data/lcl_export.csv
2024-01-15 10:30:02 - INFO - Applied field mapping: 8 fields mapped
2024-01-15 10:30:03 - INFO - Schema validation: PASSED
2024-01-15 10:30:04 - INFO - Resolving subject IDs...
2024-01-15 10:30:10 - INFO - GSID resolution: 98/100 resolved (45 new, 53 existing)
2024-01-15 10:30:11 - INFO - Uploaded fragment to s3://bucket/staging/lcl/batch_20240115_103000.json
2024-01-15 10:30:12 - INFO - Created validation queue record: ID=123
2024-01-15 10:30:12 - INFO - ✓ Validation completed successfully
2024-01-15 10:30:12 - INFO - Status: passed (98/100 valid records)
```

### Validation with Errors

```
2024-01-15 10:30:00 - INFO - Starting validation for table: lcl
2024-01-15 10:30:01 - INFO - Loaded 50 records from /data/lcl_samples.csv
2024-01-15 10:30:02 - WARNING - Missing source field: draw_time (mapped to collection_time)
2024-01-15 10:30:03 - ERROR - Schema validation: FAILED
2024-01-15 10:30:03 - ERROR - Row 5: Required field 'sample_id' is missing
2024-01-15 10:30:03 - ERROR - Row 12: Invalid data type for 'volume_ml' (expected numeric)
2024-01-15 10:30:03 - ERROR - Row 23: Required field 'collection_date' is missing
2024-01-15 10:30:03 - INFO - ✗ Validation failed with 3 errors
2024-01-15 10:30:03 - INFO - Please correct errors and resubmit
```

### GSID Resolution Warnings

```
2024-01-15 10:30:05 - INFO - Resolving subject IDs...
2024-01-15 10:30:06 - WARNING - Row 5: No valid subject ID found in candidates
2024-01-15 10:30:07 - WARNING - Row 18: Subject ID 'SUBJ999' not found in GSID service
2024-01-15 10:30:10 - INFO - GSID resolution: 48/50 resolved (20 new, 28 existing, 2 failed)
2024-01-15 10:30:10 - WARNING - 2 records could not be resolved - check subject IDs
```

## Monitoring & Troubleshooting

### Query Validation Queue

```sql
-- Recent validations
SELECT
    id,
    table_name,
    source,
    validation_status,
    total_records,
    valid_records,
    approved_for_load,
    created_at
FROM validation_queue
ORDER BY created_at DESC
LIMIT 20;

-- Pending approvals
SELECT *
FROM validation_queue
WHERE validation_status = 'passed'
  AND approved_for_load = FALSE
ORDER BY created_at;

-- Failed validations
SELECT
    table_name,
    source,
    error_summary,
    created_at
FROM validation_queue
WHERE validation_status = 'failed'
ORDER BY created_at DESC;

-- Success rate by table
SELECT
    table_name,
    COUNT(*) as total_validations,
    SUM(CASE WHEN validation_status = 'passed' THEN 1 ELSE 0 END) as passed,
    ROUND(100.0 * SUM(CASE WHEN validation_status = 'passed' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM validation_queue
WHERE created_at > NOW() - INTERVAL '30 days'
GROUP BY table_name;
```

### Common Issues

**Issue**: `Schema validation failed: Required field missing`

```bash
# Solution: Check field mapping configuration
# Ensure all required fields are mapped
cat config/lcl_mapping.json | jq '.field_mapping'

# Check NocoDB table schema
curl -H "xc-token: $NOCODB_API_TOKEN" \
  "$NOCODB_URL/api/v2/meta/tables/$TABLE_ID"
```

**Issue**: `GSID resolution failed: No valid subject ID found`

```bash
# Solution: Verify subject_id_candidates in mapping config
# Check that at least one candidate field exists in source data
head -1 input.csv  # Check column names

# Update mapping config with correct candidate fields
{
  "subject_id_candidates": ["consortium_id", "local_id", "subject_id"]
}
```

**Issue**: `S3 upload failed: Access Denied`

```bash
# Solution: Verify AWS credentials and bucket permissions
aws s3 ls s3://$S3_BUCKET/staging/ --profile your-profile

# Check IAM policy
aws iam get-user-policy --user-name your-user --policy-name S3Access
```

**Issue**: `NocoDB API error: 401 Unauthorized`

```bash
# Solution: Verify NocoDB token is valid
curl -H "xc-token: $NOCODB_API_TOKEN" \
  "$NOCODB_URL/api/v2/meta/bases"

# Regenerate token in NocoDB if expired
```

**Issue**: `Field mapping error: Source field not found`

```bash
# Solution: Check CSV column names match mapping config
head -1 input.csv

# Update mapping config or CSV headers
# Mapping config uses exact case-sensitive field names
```

## Performance

### Benchmarks

- **CSV Loading**: ~1 second per 10,000 rows
- **Field Mapping**: ~0.5 seconds per 10,000 rows
- **Schema Validation**: ~2 seconds per 10,000 rows
- **GSID Resolution**: ~5 seconds per 100 subjects (batched)
- **S3 Upload**: ~2 seconds per 10MB file
- **Total Throughput**: ~1,000-2,000 rows/second

### Optimization Tips

1. **Batch Size**: GSID resolution uses batches of 100 by default

   ```python
   # Adjust in services/gsid_client.py
   results = client.register_batch(requests, batch_size=200)
   ```

2. **Parallel Processing**: Process multiple files in parallel

   ```bash
   python main.py --table-name lcl --input-file file1.csv --source "Batch1" &
   python main.py --table-name lcl --input-file file2.csv --source "Batch2" &
   wait
   ```

3. **Pre-cache Local IDs**: Load local ID cache once for multiple validations
   ```python
   # Cache is loaded once during FragmentValidator initialization
   validator = FragmentValidator(s3_client, nocodb_client, gsid_client)
   # Reuse validator
   ```
