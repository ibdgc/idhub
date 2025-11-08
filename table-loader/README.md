# Table Loader

Database loader service for validated fragment data in the IDhub platform.

## Overview

The Table Loader service loads validated data fragments from S3 staging into PostgreSQL database tables. It acts as the final step in the data pipeline, transforming staged fragments into production database records with proper data types, constraints, and relationships.

### Key Features

- **Batch Loading**: Efficient bulk insert operations with configurable batch sizes
- **Load Strategies**: Support for INSERT and UPSERT operations
- **Data Transformation**: Type conversion, null handling, and data cleaning
- **S3 Integration**: Reads validated fragments from S3 staging area
- **Transaction Management**: Atomic operations with rollback on failure
- **Dry-Run Mode**: Preview load operations without committing changes
- **Load Tracking**: Comprehensive logging and status tracking
- **Fragment Archiving**: Automatic movement of loaded fragments to archive
- **Multi-Table Support**: Handles all sample tables (blood, LCL, specimen, DNA, RNA, etc.)

## Architecture

```
table-loader/
├── core/
│   ├── __init__.py
│   ├── config.py                    # Configuration settings
│   └── database.py                  # Connection pool management
├── services/
│   ├── __init__.py
│   ├── loader.py                    # Main loader orchestrator
│   ├── load_strategy.py             # INSERT/UPSERT strategies
│   ├── data_transformer.py          # Data type transformations
│   └── s3_client.py                 # S3 operations
├── tests/
│   ├── conftest.py                  # Pytest fixtures
│   ├── test_loader.py
│   ├── test_load_strategy.py
│   ├── test_data_transformer.py
│   ├── test_config.py
│   └── test_main.py
├── logs/                            # Load execution logs
├── main.py                          # CLI entry point
├── requirements.txt
├── requirements-test.txt
├── Dockerfile
├── Dockerfile.test
├── pytest.ini
└── .coveragerc
```

## Load Flow

```
┌─────────────────┐
│  S3 Staging     │ ← Validated fragments from fragment-validator
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ List Fragments  │ ← S3Client (by batch_id)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Download JSON   │ ← S3Client (parse fragment)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transform Data  │ ← DataTransformer (type conversion)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Select Strategy │ ← LoadStrategy (INSERT vs UPSERT)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Execute Load    │ ← Database (bulk insert with transaction)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Archive Fragment│ ← S3Client (move to loaded/)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Update Status   │ → validation_queue (mark as loaded)
└─────────────────┘
```

## Configuration

### Environment Variables

| Variable                | Description              | Required | Default                   |
| ----------------------- | ------------------------ | -------- | ------------------------- |
| `DB_HOST`               | PostgreSQL host          | Yes      | `idhub_db`                |
| `DB_NAME`               | Database name            | Yes      | `idhub`                   |
| `DB_USER`               | Database user            | Yes      | `idhub_user`              |
| `DB_PASSWORD`           | Database password        | Yes      | -                         |
| `DB_PORT`               | Database port            | No       | `5432`                    |
| `S3_BUCKET`             | S3 bucket for fragments  | Yes      | `idhub-curated-fragments` |
| `AWS_ACCESS_KEY_ID`     | AWS access key           | Yes      | -                         |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key           | Yes      | -                         |
| `AWS_DEFAULT_REGION`    | AWS region               | No       | `us-east-1`               |
| `BATCH_SIZE`            | Records per batch insert | No       | `1000`                    |
| `MAX_RETRIES`           | Max retry attempts       | No       | `3`                       |

### Example `.env` File

```bash
# Database Configuration
DB_HOST=idhub_db
DB_NAME=idhub
DB_USER=idhub_user
DB_PASSWORD=your_secure_password
DB_PORT=5432

# S3 Configuration
S3_BUCKET=idhub-curated-fragments
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=us-east-1

# Load Configuration
BATCH_SIZE=1000
MAX_RETRIES=3
```

## Usage

### Command Line Interface

```bash
# Preview load (dry-run)
python main.py --batch-id batch_20240115_103000

# Execute load (with approval)
python main.py --batch-id batch_20240115_103000 --approve

# Load specific table only
python main.py --batch-id batch_20240115_103000 --table lcl --approve

# Get help
python main.py --help
```

**Arguments**:

- `--batch-id`: Batch identifier (matches S3 folder structure)
- `--approve`: Execute load (without this flag, runs in dry-run mode)
- `--table`: Load specific table only (optional)

### Docker Compose

```bash
# Preview load
docker-compose run --rm table-loader python main.py \
  --batch-id batch_20240115_103000

# Execute load
docker-compose run --rm table-loader python main.py \
  --batch-id batch_20240115_103000 \
  --approve

# View logs
docker-compose logs table-loader -f
```

## Components

### 1. Table Loader (`services/loader.py`)

Main orchestrator coordinating the load process.

**Features**:

- Discovers fragments by batch ID
- Selects appropriate load strategy per table
- Manages transaction boundaries
- Tracks load statistics
- Updates validation queue status
- Archives loaded fragments

**Example**:

```python
from services.loader import TableLoader

loader = TableLoader()

# Preview load
preview = loader.preview_load(batch_id="batch_20240115_103000")
print(f"Tables to load: {list(preview.keys())}")
for table, info in preview.items():
    print(f"  {table}: {info['record_count']} records")

# Execute load
result = loader.execute_load(
    batch_id="batch_20240115_103000",
    dry_run=False
)
print(f"Status: {result['status']}")
print(f"Tables loaded: {len(result['tables'])}")
```

**Load Result Structure**:

```python
{
    "batch_id": "batch_20240115_103000",
    "status": "success",  # or "partial_success", "failed"
    "tables": {
        "lcl": {
            "status": "success",
            "records_loaded": 100,
            "records_failed": 0,
            "load_time_seconds": 2.5,
            "s3_key": "staging/validated/batch_20240115_103000/lcl.json"
        },
        "blood": {
            "status": "success",
            "records_loaded": 250,
            "records_failed": 0,
            "load_time_seconds": 5.2,
            "s3_key": "staging/validated/batch_20240115_103000/blood.json"
        }
    },
    "total_records": 350,
    "total_time_seconds": 7.8,
    "errors": []
}
```

### 2. Load Strategies (`services/load_strategy.py`)

Implements different loading strategies based on table requirements.

#### Standard Load Strategy (INSERT)

Used for most sample tables where records are append-only.

**Features**:

- Bulk INSERT operations
- Batch processing for large datasets
- Duplicate detection (logs warnings)
- Transaction rollback on error

**Example**:

```python
from services.load_strategy import StandardLoadStrategy

strategy = StandardLoadStrategy(
    table_name="blood",
    exclude_fields={"Id", "created_at", "updated_at"}
)

result = strategy.load(
    fragment=dataframe,
    dry_run=False
)
```

#### Upsert Load Strategy (INSERT ... ON CONFLICT UPDATE)

Used for tables that require updates to existing records (e.g., `subject` table).

**Features**:

- INSERT with ON CONFLICT DO UPDATE
- Configurable conflict columns
- Selective field updates
- Preserves created_at timestamps

**Example**:

```python
from services.load_strategy import UpsertLoadStrategy

strategy = UpsertLoadStrategy(
    table_name="subject",
    conflict_columns=["global_subject_id"],
    exclude_fields={"Id", "created_at"}
)

result = strategy.load(
    fragment=dataframe,
    dry_run=False
)
```

**Upsert SQL Example**:

```sql
INSERT INTO subject (global_subject_id, center_id, local_subject_id, ...)
VALUES (%s, %s, %s, ...)
ON CONFLICT (global_subject_id)
DO UPDATE SET
    center_id = EXCLUDED.center_id,
    local_subject_id = EXCLUDED.local_subject_id,
    updated_at = CURRENT_TIMESTAMP
WHERE subject.updated_at < EXCLUDED.updated_at;
```

### 3. Data Transformer (`services/data_transformer.py`)

Transforms fragment data for database insertion.

**Features**:

- Type conversion (string → int, float, date, boolean)
- Null value handling
- Field exclusion (system columns)
- Data validation
- Error collection

**Transformations**:

- **Dates**: ISO format strings → `datetime.date`
- **Timestamps**: ISO format strings → `datetime.datetime`
- **Booleans**: Various formats → `True`/`False`
- **Numeric**: String numbers → `int`/`float`
- **Nulls**: Empty strings, "NULL", "NA" → `None`

**Example**:

```python
from services.data_transformer import DataTransformer

transformer = DataTransformer(
    table_name="blood",
    exclude_fields={"Id", "created_at", "updated_at"}
)

# Transform DataFrame
records = transformer.transform_records(fragment_df)

# Transform dict
records = transformer.transform_records({
    "records": [
        {"sample_id": "S001", "volume_ml": "5.5", "collection_date": "2024-01-15"},
        {"sample_id": "S002", "volume_ml": "7.2", "collection_date": "2024-01-16"}
    ]
})

# Result
[
    {"sample_id": "S001", "volume_ml": 5.5, "collection_date": date(2024, 1, 15)},
    {"sample_id": "S002", "volume_ml": 7.2, "collection_date": date(2024, 1, 16)}
]
```

**Type Detection**:

```python
def _infer_type(self, value: Any) -> Any:
    """Infer and convert data type"""
    if value is None or value == "":
        return None

    # Try boolean
    if str(value).lower() in ("true", "false", "yes", "no", "1", "0"):
        return str(value).lower() in ("true", "yes", "1")

    # Try integer
    try:
        return int(value)
    except (ValueError, TypeError):
        pass

    # Try float
    try:
        return float(value)
    except (ValueError, TypeError):
        pass

    # Try date (YYYY-MM-DD)
    if isinstance(value, str) and len(value) == 10 and value[4] == "-":
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            pass

    # Return as string
    return str(value)
```

### 4. S3 Client (`services/s3_client.py`)

Manages S3 operations for fragment retrieval and archiving.

**Features**:

- List fragments by batch ID
- Download and parse JSON fragments
- Move fragments between folders
- Upload load results
- Error handling and retry

**S3 Folder Structure**:

```
s3://idhub-curated-fragments/
├── staging/
│   └── validated/
│       └── batch_20240115_103000/
│           ├── lcl.json
│           ├── blood.json
│           └── specimen.json
├── loaded/
│   └── batch_20240115_103000/
│       ├── lcl.json
│       ├── blood.json
│       └── specimen.json
└── failed/
    └── batch_20240115_103000/
        └── error_log.json
```

**Example**:

```python
from services.s3_client import S3Client

client = S3Client(bucket="idhub-curated-fragments")

# List batch fragments
fragments = client.list_batch_fragments("batch_20240115_103000")
for fragment in fragments:
    print(f"{fragment['table']}: {fragment['key']}")

# Download fragment
data = client.download_fragment("staging/validated/batch_20240115_103000/lcl.json")
print(f"Records: {len(data['records'])}")

# Mark as loaded (move to loaded/)
client.mark_fragment_as_loaded(
    "staging/validated/batch_20240115_103000/lcl.json"
)
```

### 5. Database Manager (`core/database.py`)

Manages PostgreSQL connection pool and transactions.

**Features**:

- Thread-safe connection pooling
- Context manager for transactions
- Automatic commit/rollback
- Connection health checks
- Lazy initialization

**Example**:

```python
from core.database import db_manager

# Get connection from pool
with db_manager.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM blood")
        count = cur.fetchone()[0]
        print(f"Blood samples: {count}")
    # Auto-commit on success

# Execute with transaction
with db_manager.transaction() as cur:
    cur.execute(
        "INSERT INTO blood (sample_id, volume_ml) VALUES (%s, %s)",
        ("S001", 5.5)
    )
    # Auto-commit on exit, rollback on exception
```

**Connection Pool Configuration**:

```python
# In core/database.py
self.pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=2,      # Minimum connections
    maxconn=10,     # Maximum connections
    host=settings.DB_HOST,
    database=settings.DB_NAME,
    user=settings.DB_USER,
    password=settings.DB_PASSWORD,
    port=settings.DB_PORT
)
```

## Load Strategies by Table

### Tables Using Standard INSERT

Most sample tables use standard INSERT strategy:

- `lcl` - Lymphoblastoid cell lines
- `specimen` - General specimen tracking
- `dna` - DNA extraction records
- `rna` - RNA extraction records
- `serum` - Serum samples
- `plasma` - Plasma samples
- `stool` - Stool samples
- `tissue` - Tissue samples

**Characteristics**:

- Append-only records
- No updates to existing records
- Unique constraints on sample IDs
- Duplicate detection logs warnings

### Tables Using UPSERT

Tables that require updates to existing records:

- `subject` - Subject master records
  - **Conflict Column**: `global_subject_id`
  - **Update Logic**: Update demographics, clinical data if newer

**Characteristics**:

- Records may be updated over time
- Conflict resolution on primary key
- Preserves created_at timestamp
- Updates updated_at timestamp

### Adding New Tables

To add support for a new table:

1. **Determine Load Strategy**:

   ```python
   # In services/loader.py
   UPSERT_TABLES = {"subject", "new_table"}  # Add if upsert needed
   ```

2. **Configure Conflict Columns** (if upsert):

   ```python
   # In services/load_strategy.py
   if table_name == "new_table":
       conflict_columns = ["unique_field_1", "unique_field_2"]
   ```

3. **Define Excluded Fields**:

   ```python
   exclude_fields = {
       "Id",           # Auto-increment primary key
       "created_at",   # Auto-generated timestamp
       "updated_at"    # Auto-updated timestamp
   }
   ```

4. **Test Load**:
   ```bash
   python main.py --batch-id test_batch --table new_table
   ```

## Batch Processing

### Batch Identification

Batches are identified by timestamp-based IDs:

**Format**: `batch_YYYYMMDD_HHMMSS`

**Example**: `batch_20240115_103000`

### Batch Discovery

The loader discovers batches from S3 folder structure:

```python
# List all batches
batches = s3_client.list_objects_v2(
    Bucket=bucket,
    Prefix="staging/validated/",
    Delimiter="/"
)

# Extract batch IDs
batch_ids = [
    prefix.split("/")[-2]
    for prefix in batches.get("CommonPrefixes", [])
]
```

### Batch Validation

Before loading, the loader validates:

1. **Batch exists in S3**: Folder `staging/validated/{batch_id}/` exists
2. **Contains fragments**: At least one `.json` file present
3. **Approved for load**: `validation_queue` record has `approved_for_load=TRUE`
4. **Not already loaded**: `loaded_at` is NULL

### Batch Loading

Load process per batch:

1. **List fragments** in batch folder
2. **For each fragment**:
   - Download JSON from S3
   - Transform data types
   - Select load strategy
   - Execute database load
   - Archive fragment to `loaded/`
3. **Update validation queue** with results
4. **Generate summary report**

## Database Schema

### Validation Queue Integration

The loader updates the `validation_queue` table to track load status:

```sql
-- Mark batch as loading
UPDATE validation_queue
SET validation_status = 'loading',
    updated_at = CURRENT_TIMESTAMP
WHERE batch_id = 'batch_20240115_103000'
  AND approved_for_load = TRUE;

-- Mark batch as loaded
UPDATE validation_queue
SET validation_status = 'loaded',
    loaded_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE batch_id = 'batch_20240115_103000';

-- Mark batch as failed
UPDATE validation_queue
SET validation_status = 'load_failed',
    error_summary = 'Database constraint violation',
    updated_at = CURRENT_TIMESTAMP
WHERE batch_id = 'batch_20240115_103000';
```

### Load Audit Log

Track all load operations:

TODO implement this with identity resolution table

```sql
CREATE TABLE load_audit_log (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    records_loaded INTEGER,
    records_failed INTEGER,
    load_time_seconds DECIMAL(10,2),
    status VARCHAR(50),
    error_message TEXT,
    loaded_by VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_batch_id (batch_id),
    INDEX idx_table_name (table_name),
    INDEX idx_status (status)
);
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

# Run loader (dry-run)
python main.py --batch-id batch_20240115_103000

# Execute load
python main.py --batch-id batch_20240115_103000 --approve
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/test_loader.py -v

# Run specific test
pytest tests/test_load_strategy.py::TestStandardLoadStrategy::test_load_success -v

# Run with markers
pytest -m unit
pytest -m integration
```

### Test Coverage

Current coverage: **~78%**

Coverage reports:

- Terminal: `pytest --cov=. --cov-report=term-missing`
- HTML: `htmlcov/index.html`
- XML: `coverage.xml`
- JUnit: `test-reports/junit.xml`

### Docker Development

```bash
# Build image
docker build -t table-loader:latest .

# Run container (dry-run)
docker run --rm \
  -e DB_PASSWORD=your_password \
  -e AWS_ACCESS_KEY_ID=your_key \
  table-loader:latest \
  python main.py --batch-id batch_20240115_103000

# Run container (execute load)
docker run --rm \
  -e DB_PASSWORD=your_password \
  -e AWS_ACCESS_KEY_ID=your_key \
  table-loader:latest \
  python main.py --batch-id batch_20240115_103000 --approve

# Run tests in Docker
docker build -f Dockerfile.test -t table-loader:test .
docker run table-loader:test
```

## Output Examples

### Dry-Run (Preview)

```
2024-01-15 10:30:00 - INFO - Starting table loader (DRY-RUN mode)
2024-01-15 10:30:01 - INFO - Batch ID: batch_20240115_103000
2024-01-15 10:30:02 - INFO - Found 3 fragments to load:
2024-01-15 10:30:02 - INFO -   - lcl: 100 records
2024-01-15 10:30:02 - INFO -   - blood: 250 records
2024-01-15 10:30:02 - INFO -   - specimen: 75 records
2024-01-15 10:30:03 - INFO -
2024-01-15 10:30:03 - INFO - === PREVIEW: lcl ===
2024-01-15 10:30:03 - INFO - Strategy: StandardLoadStrategy
2024-01-15 10:30:03 - INFO - Records: 100
2024-01-15 10:30:03 - INFO - Fields: knumber, niddk_no, cell_line_name, passage_number, freeze_date
2024-01-15 10:30:03 - INFO - Sample record: {'knumber': 'K12345', 'niddk_no': 'ND67890', ...}
2024-01-15 10:30:04 - INFO -
2024-01-15 10:30:04 - INFO - === PREVIEW: blood ===
2024-01-15 10:30:04 - INFO - Strategy: StandardLoadStrategy
2024-01-15 10:30:04 - INFO - Records: 250
2024-01-15 10:30:04 - INFO - Fields: sample_id, collection_date, tube_type, volume_ml
2024-01-15 10:30:04 - INFO - Sample record: {'sample_id': 'BLD001', 'volume_ml': 5.5, ...}
2024-01-15 10:30:05 - INFO -
2024-01-15 10:30:05 - INFO - Total records to load: 425
2024-01-15 10:30:05 - INFO - Run with --approve to execute load
```

### Successful Load

```
2024-01-15 10:35:00 - INFO - Starting table loader (EXECUTE mode)
2024-01-15 10:35:01 - INFO - Batch ID: batch_20240115_103000
2024-01-15 10:35:02 - INFO - Loading table: lcl
2024-01-15 10:35:03 - INFO - Transformed 100 records
2024-01-15 10:35:04 - INFO - Executing bulk insert (batch_size=1000)
2024-01-15 10:35:05 - INFO - ✓ Loaded 100 records to lcl (2.5s)
2024-01-15 10:35:06 - INFO - Archived fragment to loaded/batch_20240115_103000/lcl.json
2024-01-15 10:35:07 - INFO -
2024-01-15 10:35:07 - INFO - Loading table: blood
2024-01-15 10:35:08 - INFO - Transformed 250 records
2024-01-15 10:35:09 - INFO - Executing bulk insert (batch_size=1000)
2024-01-15 10:35:11 - INFO - ✓ Loaded 250 records to blood (4.2s)
2024-01-15 10:35:12 - INFO - Archived fragment to loaded/batch_20240115_103000/blood.json
2024-01-15 10:35:13 - INFO -
2024-01-15 10:35:13 - INFO - === LOAD SUMMARY ===
2024-01-15 10:35:13 - INFO - Status: SUCCESS
2024-01-15 10:35:13 - INFO - Tables loaded: 3
2024-01-15 10:35:13 - INFO - Total records: 425
2024-01-15 10:35:13 - INFO - Total time: 13.2s
2024-01-15 10:35:13 - INFO - Updated validation_queue: loaded_at = 2024-01-15 10:35:13
```

### Load with Errors

```
2024-01-15 10:40:00 - INFO - Starting table loader (EXECUTE mode)
2024-01-15 10:40:01 - INFO - Batch ID: batch_20240115_143000
2024-01-15 10:40:02 - INFO - Loading table: blood
2024-01-15 10:40:03 - INFO - Transformed 150 records
2024-01-15 10:40:04 - INFO - Executing bulk insert (batch_size=1000)
2024-01-15 10:40:05 - ERROR - Database error: duplicate key value violates unique constraint "blood_sample_id_key"
2024-01-15 10:40:05 - ERROR - Detail: Key (sample_id)=(BLD123) already exists.
2024-01-15 10:40:05 - INFO - Rolling back transaction
2024-01-15 10:40:06 - ERROR - ✗ Failed to load blood: Duplicate sample_id
2024-01-15 10:40:07 - INFO -
2024-01-15 10:40:07 - INFO - === LOAD SUMMARY ===
2024-01-15 10:40:07 - INFO - Status: FAILED
2024-01-15 10:40:07 - INFO - Tables loaded: 0
2024-01-15 10:40:07 - INFO - Tables failed: 1
2024-01-15 10:40:07 - INFO - Error: Duplicate sample_id in blood table
2024-01-15 10:40:07 - INFO - Updated validation_queue: status = load_failed
```

## Monitoring & Troubleshooting

### Query Load Status

```sql
-- Recent loads
SELECT
    batch_id,
    table_name,
    records_loaded,
    load_time_seconds,
    status,
    loaded_at
FROM load_audit_log
ORDER BY loaded_at DESC
LIMIT 20;

-- Failed loads
SELECT
    batch_id,
    table_name,
    error_message,
    loaded_at
FROM load_audit_log
WHERE status = 'failed'
ORDER BY loaded_at DESC;

-- Load performance by table
SELECT
    table_name,
    COUNT(*) as load_count,
    SUM(records_loaded) as total_records,
    AVG(load_time_seconds) as avg_time,
    AVG(records_loaded / load_time_seconds) as avg_records_per_sec
FROM load_audit_log
WHERE status = 'success'
  AND loaded_at > NOW() - INTERVAL '30 days'
GROUP BY table_name
ORDER BY total_records DESC;

-- Pending loads (approved but not loaded)
SELECT
    vq.batch_id,
    vq.table_name,
    vq.total_records,
    vq.approved_at,
    vq.approved_by
FROM validation_queue vq
WHERE vq.approved_for_load = TRUE
  AND vq.loaded_at IS NULL
ORDER BY vq.approved_at;
```

### Common Issues

**Issue**: `Database error: duplicate key value violates unique constraint`

```bash
# Solution 1: Check for duplicate records in source data
SELECT sample_id, COUNT(*)
FROM staging_table
GROUP BY sample_id
HAVING COUNT(*) > 1;

# Solution 2: Check if records already exist in database
SELECT sample_id FROM blood WHERE sample_id IN ('BLD123', 'BLD456');

# Solution 3: Use UPSERT strategy if updates are intended
# Update services/loader.py to add table to UPSERT_TABLES
```

**Issue**: `Connection pool exhausted`

```bash
# Solution: Increase pool size in core/database.py
# Or reduce concurrent load operations
maxconn=20  # Increase from default 10
```

**Issue**: `S3 fragment not found`

```bash
# Solution

```
