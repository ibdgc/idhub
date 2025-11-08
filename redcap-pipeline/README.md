# REDCap Pipeline

Automated data integration pipeline for extracting, transforming, and loading research data from REDCap projects into the IDhub platform.

## Overview

The REDCap Pipeline service provides automated synchronization of subject data from multiple REDCap projects. It handles data extraction, center resolution, GSID registration, field mapping, and fragment staging to S3.

### Key Features

- **Multi-Project Support**: Manage multiple REDCap projects with independent configurations
- **Automated GSID Registration**: Automatic subject registration with the GSID service
- **Intelligent Center Resolution**: Fuzzy matching and alias resolution for research centers
- **Field Mapping**: Flexible JSON-based field mapping configurations per project
- **S3 Fragment Staging**: Curated data fragments uploaded to S3 for downstream processing
- **Batch Processing**: Efficient batch processing with configurable batch sizes
- **Continuous & Manual Modes**: Support for both continuous integration and manual runs
- **Comprehensive Logging**: Detailed audit trail of all pipeline operations

## Architecture

```
redcap-pipeline/
├── config/
│   ├── projects.json                    # Project configurations
│   ├── gap_field_mappings.json         # GAP project field mappings
│   ├── cd_ileal_field_mappings.json    # CD Ileal project mappings
│   └── uc_demarc_field_mappings.json   # UC Demarc project mappings
├── core/
│   ├── __init__.py
│   ├── config.py                        # Environment configuration
│   └── database.py                      # PostgreSQL connection pool
├── services/
│   ├── __init__.py
│   ├── pipeline.py                      # Main pipeline orchestration
│   ├── redcap_client.py                # REDCap API client
│   ├── gsid_client.py                  # GSID service client
│   ├── center_resolver.py              # Center name resolution
│   ├── data_processor.py               # Data transformation
│   └── s3_uploader.py                  # S3 fragment upload
├── tests/
│   ├── conftest.py                      # Pytest fixtures
│   ├── test_pipeline.py
│   ├── test_redcap_client.py
│   ├── test_center_resolver.py
│   └── test_data_processor.py
├── logs/                                # Pipeline execution logs
├── main.py                              # CLI entry point
├── requirements.txt
├── requirements-test.txt
├── Dockerfile
├── Dockerfile.test
├── pytest.ini
└── .coveragerc
```

## Pipeline Flow

```
┌─────────────────┐
│  REDCap Project │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Extract Records │ ← REDCapClient
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Resolve Center  │ ← CenterResolver (fuzzy matching)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Register GSID   │ ← GSIDClient (identity resolution)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transform Data  │ ← DataProcessor (field mapping)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Upload to S3    │ ← S3Uploader (curated fragments)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Log Results     │ → Database audit log
└─────────────────┘
```

## Configuration

### Environment Variables

| Variable                     | Description                     | Required | Default                    |
| ---------------------------- | ------------------------------- | -------- | -------------------------- |
| `REDCAP_API_URL`             | REDCap API base URL             | Yes      | -                          |
| `REDCAP_API_TOKEN_GAP`       | API token for GAP project       | Yes\*    | -                          |
| `REDCAP_API_TOKEN_CD_ILEAL`  | API token for CD Ileal project  | Yes\*    | -                          |
| `REDCAP_API_TOKEN_UC_DEMARC` | API token for UC Demarc project | Yes\*    | -                          |
| `GSID_SERVICE_URL`           | GSID service endpoint           | Yes      | `http://gsid-service:8000` |
| `GSID_API_KEY`               | GSID service API key            | Yes      | -                          |
| `DB_HOST`                    | PostgreSQL host                 | Yes      | `idhub_db`                 |
| `DB_NAME`                    | Database name                   | Yes      | `idhub`                    |
| `DB_USER`                    | Database user                   | Yes      | `idhub_user`               |
| `DB_PASSWORD`                | Database password               | Yes      | -                          |
| `DB_PORT`                    | Database port                   | No       | `5432`                     |
| `S3_BUCKET`                  | S3 bucket for fragments         | Yes      | `idhub-curated-fragments`  |
| `AWS_ACCESS_KEY_ID`          | AWS access key                  | Yes      | -                          |
| `AWS_SECRET_ACCESS_KEY`      | AWS secret key                  | Yes      | -                          |
| `AWS_DEFAULT_REGION`         | AWS region                      | No       | `us-east-1`                |

\*Required only for enabled projects

### Example `.env` File

```bash
# REDCap Configuration
REDCAP_API_URL=https://redcap.example.edu/api/
REDCAP_API_TOKEN_GAP=your_gap_token_here
REDCAP_API_TOKEN_CD_ILEAL=your_cd_ileal_token_here
REDCAP_API_TOKEN_UC_DEMARC=your_uc_demarc_token_here

# GSID Service
GSID_SERVICE_URL=http://gsid-service:8000
GSID_API_KEY=your-secure-api-key

# Database
DB_HOST=idhub_db
DB_NAME=idhub
DB_USER=idhub_user
DB_PASSWORD=your_db_password
DB_PORT=5432

# AWS S3
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=idhub-curated-fragments
```

## Project Configuration

### `config/projects.json`

Define multiple REDCap projects with independent configurations:

```json
{
  "projects": {
    "gap": {
      "name": "GAP",
      "redcap_project_id": "16894",
      "api_token": "${REDCAP_API_TOKEN_GAP}",
      "field_mappings": "gap_field_mappings.json",
      "schedule": "continuous",
      "batch_size": 50,
      "enabled": true,
      "description": "Main biobank project - continuous integration"
    },
    "cd_ileal": {
      "name": "cd_ileal",
      "redcap_project_id": "16899",
      "api_token": "${REDCAP_API_TOKEN_CD_ILEAL}",
      "field_mappings": "cd_ileal_field_mappings.json",
      "schedule": "manual",
      "batch_size": 50,
      "enabled": true,
      "description": "Legacy sample collection - manual integration"
    }
  }
}
```

**Configuration Fields**:

- `name`: Project display name
- `redcap_project_id`: REDCap project ID
- `api_token`: Environment variable reference for API token
- `field_mappings`: JSON file with field mapping configuration
- `schedule`: `continuous` (automated) or `manual` (on-demand)
- `batch_size`: Number of records to process per batch
- `enabled`: Enable/disable project processing
- `description`: Human-readable description

### Field Mapping Configuration

Example `gap_field_mappings.json`:

TODO update this example

```json

```

## Usage

### Command Line Interface

```bash
# Run pipeline for specific project
python main.py --project gap

# Run pipeline for all enabled projects
python main.py --all

# Get help
python main.py --help
```

### Docker Compose

```bash
# Run as one-off job
docker-compose run --rm redcap-pipeline python main.py --project gap

# Run all projects
docker-compose run --rm redcap-pipeline python main.py --all

# View logs
docker-compose logs redcap-pipeline -f
```

## Components

### 1. REDCap Client (`services/redcap_client.py`)

Handles REDCap API interactions with retry logic and rate limiting.

**Features**:

- Automatic retry with exponential backoff
- Connection pooling
- API token resolution from environment variables
- Batch record fetching
- Error handling and logging

**Example**:

```python
from services.redcap_client import REDCapClient

client = REDCapClient(project_config)
records = client.fetch_records(batch_size=50)
```

### 2. Center Resolver (`services/center_resolver.py`)

Resolves research center names using exact matching, aliases, and fuzzy matching.

**Features**:

- In-memory center cache
- Configurable alias mapping
- Fuzzy string matching (SequenceMatcher)
- Confidence scoring
- Fallback to manual review

**Example**:

```python
from services.center_resolver import CenterResolver

resolver = CenterResolver()
center_id = resolver.resolve("Mount Sinai")  # Returns center_id for MSSM
```

**Alias Configuration** (in `core/config.py`):

```python
CENTER_ALIASES = {
    "mount_sinai": "MSSM",
    "mount_sinai_ny": "MSSM",
    "mount-sinai": "MSSM",
    "mt_sinai": "MSSM",
    # ... more aliases
}
```

### 3. GSID Client (`services/gsid_client.py`)

Communicates with GSID service for subject registration.

**Features**:

- Subject registration with identity resolution
- Batch registration support
- Automatic retry on failures
- Session management with keep-alive

**Example**:

```python
from services.gsid_client import GSIDClient

client = GSIDClient()
result = client.register_subject(
    center_id=1,
    local_subject_id="SUBJ001",
    identifier_type="primary"
)
# Returns: {"gsid": "GSID-...", "action": "create_new", ...}
```

### 4. Data Processor (`services/data_processor.py`)

Transforms REDCap data using field mappings and applies transformations.

**Features**:

- JSON-based field mapping
- Type transformations (date, map, numeric)
- Nested field structures
- Validation and error handling

**Example**:

```python
from services.data_processor import DataProcessor

processor = DataProcessor(project_config)
fragment = processor.transform_record(redcap_record, gsid)
```

### 5. S3 Uploader (`services/s3_uploader.py`)

Uploads curated data fragments to S3 for downstream processing.

**Features**:

- Server-side encryption (AES256)
- Organized folder structure by GSID
- Timestamped filenames
- Metadata tagging
- Error handling and retry

**S3 Structure**:

```
s3://idhub-curated-fragments/
└── subjects/
    └── GSID-A1B2C3D4E5F6G7H8/
        ├── gap_20240115_103000.json
        ├── gap_20240116_143000.json
        └── cd_ileal_20240117_093000.json
```

**Example**:

```python
from services.s3_uploader import S3Uploader

uploader = S3Uploader()
s3_key = uploader.upload_fragment(
    fragment=data,
    project_key="gap",
    gsid="GSID-A1B2C3D4E5F6G7H8"
)
```

### 6. Pipeline Orchestrator (`services/pipeline.py`)

Main orchestration logic coordinating all components.

**Pipeline Steps**:

1. Fetch records from REDCap (batched)
2. For each record:
   - Resolve center name to center_id
   - Register subject with GSID service
   - Transform data using field mappings
   - Upload fragment to S3
   - Log results to database
3. Generate summary report

**Example**:

```python
from services.pipeline import REDCapPipeline

pipeline = REDCapPipeline(project_config)
results = pipeline.run(batch_size=50)
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

# Run pipeline
python main.py --project gap
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/test_pipeline.py -v

# Run with markers
pytest -m unit
pytest -m integration
```

### Test Coverage

Current coverage: **~80%**

Coverage reports:

- Terminal: `pytest --cov=. --cov-report=term-missing`
- HTML: `htmlcov/index.html`
- XML: `coverage.xml`
- JUnit: `test-reports/junit.xml`

### Docker Development

```bash
# Build image
docker build -t redcap-pipeline:latest .

# Run container
docker run --rm \
  -e REDCAP_API_URL=https://redcap.example.edu/api/ \
  -e REDCAP_API_TOKEN_GAP=your_token \
  -e GSID_API_KEY=your_key \
  redcap-pipeline:latest \
  python main.py --project gap

# Run tests in Docker
docker build -f Dockerfile.test -t redcap-pipeline:test .
docker run redcap-pipeline:test
```

## Output Examples

### Successful Run

```
2024-01-15 10:30:00 - INFO - Starting REDCap pipeline for project: gap
2024-01-15 10:30:01 - INFO - Fetched 150 records from REDCap
2024-01-15 10:30:02 - INFO - Processing batch 1/3 (50 records)
2024-01-15 10:30:05 - INFO - Resolved center: Mount Sinai → MSSM (center_id=1)
2024-01-15 10:30:06 - INFO - Registered GSID: GSID-A1B2C3D4E5F6G7H8 (action=create_new)
2024-01-15 10:30:07 - INFO - Uploaded fragment to s3://bucket/subjects/GSID-.../gap_20240115_103000.json
2024-01-15 10:35:00 - INFO - Pipeline completed successfully
2024-01-15 10:35:00 - INFO - Summary: 150 processed, 148 success, 2 errors
```

### Error Handling

```
2024-01-15 10:30:10 - WARNING - Center not found: "Unknown Hospital" (record_id=SUBJ999)
2024-01-15 10:30:10 - ERROR - Failed to process record SUBJ999: Center resolution failed
```

## Monitoring & Logging

### Log Files

Logs are written to `logs/pipeline.log` and stdout:

```bash
# View recent logs
tail -f logs/pipeline.log

# Search for errors
grep ERROR logs/pipeline.log

# View specific project logs
grep "project: gap" logs/pipeline.log
```

### Database Monitoring

Query sync log for pipeline status:

```sql
-- Recent pipeline runs
SELECT project_key, status, COUNT(*) as count, MAX(created_at) as last_run
FROM redcap_sync_log
GROUP BY project_key, status
ORDER BY last_run DESC;

-- Failed records
SELECT record_id, center_id, error_message, created_at
FROM redcap_sync_log
WHERE status != 'success'
ORDER BY created_at DESC
LIMIT 50;

-- Success rate by project
SELECT
    project_key,
    COUNT(*) as total,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
    ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM redcap_sync_log
WHERE created_at > NOW() - INTERVAL '7 days'
GROUP BY project_key;
```

## Troubleshooting

### Common Issues

**Issue**: `REDCap API error: 403 Forbidden`

```bash
# Solution: Verify API token is correct
echo $REDCAP_API_TOKEN_GAP
# Check token has correct permissions in REDCap project
```

**Issue**: `Center not found: "XYZ Hospital"`

```bash
# Solution 1: Add alias to CENTER_ALIASES in core/config.py
# Solution 2: Add center to database
INSERT INTO centers (name, abbreviation) VALUES ('XYZ Hospital', 'XYZ');
```

**Issue**: `GSID service connection refused`

```bash
# Solution: Verify GSID service is running
docker ps | grep gsid-service
curl http://gsid-service:8000/health
```

**Issue**: `S3 upload failed: Access Denied`

```bash
# Solution: Verify AWS credentials and bucket permissions
aws s3 ls s3://$S3_BUCKET --profile your-profile
# Check IAM policy allows s3:PutObject
```

**Issue**: `Database connection pool exhausted`

```bash
# Solution: Reduce batch_size in project config
# Or increase pool size in core/database.py
```

## Performance

### Benchmarks

- **REDCap Fetch**: ~2-5 seconds per 100 records
- **Center Resolution**: ~1ms per record (cached)
- **GSID Registration**: ~50ms per subject
- **Data Transformation**: ~5ms per record
- **S3 Upload**: ~100ms per fragment
- **Total Throughput**: ~10-15 records/second

### Optimization Tips

1. **Batch Size**: Adjust `batch_size` based on record complexity

   - Simple records: 100-200
   - Complex records: 25-50

2. **Parallel Processing**: For large datasets, run multiple projects in parallel

   ```bash
   docker-compose run -d redcap-pipeline python main.py --project gap &
   docker-compose run -d redcap-pipeline python main.py --project cd_ileal &
   ```

3. **Database Connection Pool**: Increase pool size for high-volume processing

   ```python
   # In core/database.py
   db_pool = pool.SimpleConnectionPool(minconn=5, maxconn=20, ...)
   ```

4. **Incremental Sync**: Track last sync timestamp to fetch only new/updated records
   ```python
   # Future enhancement: Add last_sync_date to project config
   records = client.fetch_records(modified_since="2024-01-15")
   ```

## Security

### API Token Management

- Store tokens in environment variables, never in code
- Use separate tokens per project for access control
- Rotate tokens periodically (every 90 days recommended)
- Audit token usage in REDCap

### Data Security

- All S3 uploads use server-side encryption (AES256)
- Database connections use SSL/TLS in production
- PHI/PII data is pseudonymized with GSIDs
- Access logs maintained for audit compliance

### AWS IAM Policy

Minimum required permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:PutObjectAcl"],
      "Resource": "arn:aws:s3:::idhub-curated-fragments/*"
    }
  ]
}
```

## Adding New Projects

### Step-by-Step Guide

1. **Create Field Mapping Configuration**

   ```bash
   cp config/gap_field_mappings.json config/new_project_field_mappings.json
   # Edit field mappings for your project
   ```

2. **Add Project to `config/projects.json`**

   ```json
   {
     "new_project": {
       "name": "New Project",
       "redcap_project_id": "12345",
       "api_token": "${REDCAP_API_TOKEN_NEW_PROJECT}",
       "field_mappings": "new_project_field_mappings.json",
       "schedule": "manual",
       "batch_size": 50,
       "enabled": true,
       "description": "Description of new project"
     }
   }
   ```

3. **Set Environment Variable**

   ```bash
   export REDCAP_API_TOKEN_NEW_PROJECT="your_token_here"
   ```

4. **Test Pipeline**

   ```bash
   python main.py --project new_project
   ```

5. **Enable Continuous Integration** (optional)

   ```bash
   # Update schedule in projects.json
   "schedule": "continuous"

   # Add cron job
   0 * * * * docker-compose run --rm redcap-pipeline python main.py --project new_project
   ```

## Integration with IDhub Platform

### Upstream Dependencies

- **GSID Service**: Subject registration and identity resolution
- **Database**: Center lookup and sync logging
- **REDCap**: Source data extraction

### Downstream Consumers

- **Fragment Validator**: Validates staged S3 fragments
- **Table Loader**: Loads validated fragments into database tables
- **NocoDB**: Displays pipeline results and errors

### Data Flow

```
REDCap → Pipeline → S3 Fragments → Validator → Loader → Database → NocoDB
                         ↓
                    GSID Service
```

## Contributing

### Code Style

- Follow PEP 8 guidelines
- Use type hints for function signatures
- Write docstrings for all public methods
- Maintain test coverage above 75%

### Adding New Transformations

Example: Add custom transformation type

```python
# In services/data_processor.py

def apply_transformation(self, value, transform_config):
    transform_type = transform_config.get("type")

    if transform_type == "custom_type":
        # Your custom logic here
        return self._custom_transform(value, transform_config)

    # ... existing transformations
```

### Pull Request Process

1. Create feature branch from `main`
2. Write tests for new functionality
3. Update field mapping examples if needed
4. Ensure all tests pass: `pytest`
5. Update this README if adding features
6. Submit PR with clear description

## Support

For issues or questions:

- **GitHub Issues**: https://github.com/ibdgc/idhub/issues
- **Documentation**: See root `/README.md` for platform overview
- **Related Services**:
  - `/gsid-service` - Subject ID generation
  - `/fragment-validator` - Fragment validation
  - `/table-loader` - Database loading

## Future Enhancements

- [ ] Incremental sync (fetch only new/updated records)
- [ ] Parallel batch processing
- [ ] Real-time webhook integration
- [ ] Advanced data quality checks
- [ ] Automated center resolution learning
- [ ] GraphQL API for pipeline status
- [ ] Prometheus metrics export
- [ ] Dead letter queue for failed records

## License

[Add license information]

## Changelog

### v1.0.0 (2024-01-15)

- Initial release
- Multi-project support
- Center resolution with fuzzy matching
- GSID integration
- S3 fragment staging
- Comprehensive logging and error handling
