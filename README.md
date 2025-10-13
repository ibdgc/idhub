Here's the updated README section for fragment ingestion:

```markdown
# IBDGC IDhub

Project and Dataset Intersection Through Globally-Resolved Subject Identifiers

## Overview

**IDhub** is a centralized identity hub for managing biomedical research subjects and samples across multiple research centers. It provides:

- **Global Subject IDs (GSIDs)**: 12-character ULID-inspired unique identifiers
- **Identity Resolution**: Automatic matching and conflict detection for subject registration
- **REDCap Integration**: Automated data ingestion pipeline
- **Fragment Validation & Loading**: Curated sample data ingestion from S3
- **Multi-Center Support**: 81+ pre-configured research centers with fuzzy matching
- **Sample Tracking**: DNA, blood, and 12+ sample types linked to subjects
- **NocoDB Frontend**: User-friendly interface for data management

## Architecture
```

Internet → Route53/DNS → EC2 (Elastic IP)
↓
Nginx (Port 80/443)
├─→ NocoDB (idhub.ibdgc.org)
└─→ GSID API (api.idhub.ibdgc.org)
↓
┌─────────────┴─────────────┐
↓ ↓
NocoDB Container GSID Service (FastAPI)
↓ ↓
NocoDB PostgreSQL idHub PostgreSQL
↑
│
┌─────────────────────┴─────────────────────┐
↓ ↓
REDCap Pipeline (GitHub Actions) Fragment Ingestion Pipeline
↓ ↓
AWS S3 (Curated Fragments) Fragment Validator → Table Loader

```

## Infrastructure

- **Platform**: AWS EC2 t3.small (2 vCPU, 2GB RAM)
- **OS**: Amazon Linux 2023
- **Domains**:
  - `idhub.ibdgc.org` (NocoDB frontend)
  - `api.idhub.ibdgc.org` (GSID registration API)
- **SSL**: Let's Encrypt with automatic renewal
- **Container Orchestration**: Docker Compose

## Core Components

### 1. GSID Registration Service (FastAPI)

- **Port**: 8000 (internal), 443 (external via nginx)
- **Purpose**: Subject registration and identity resolution
- **Features**:
  - ULID-inspired 12-character GSID generation (`TTTTTTRRRRRR`)
  - Exact match, alias matching, and fuzzy matching
  - Conflict detection and review flagging
  - Audit trail via `identity_resolutions` table

### 2. IDhub Database (PostgreSQL 15)

- **Port**: 5432 (localhost only)
- **Schema**:
  - `subjects`: Core subject records with GSIDs
  - `local_subject_ids`: Multiple local IDs per subject with conflict detection
  - `centers`: 81 research centers (pre-seeded)
  - `family`: Family/pedigree relationships
  - `dna`, `blood`, `wgs`, `immunochip`, `lcl`, etc.: Sample tables
  - `identity_resolutions`: Audit log for all registration events

### 3. REDCap Integration Pipeline

- **Execution**: GitHub Actions (scheduled/manual)
- **Features**:
  - Batch processing (50 records/batch) to manage memory
  - Connection pooling for database efficiency
  - Center name normalization with alias mapping
  - Fuzzy matching for center names (70% threshold)
  - Multiple local ID extraction and registration
  - Conflict detection and review flagging
  - PHI-free fragment upload to S3
  - Comprehensive logging

### 4. Fragment Ingestion Pipeline

Two-stage process for loading curated sample data from S3 into PostgreSQL:

#### Stage 1: Fragment Validator

- **Purpose**: Validate and prepare sample data for loading
- **Execution**: Manual via Docker or GitHub Actions
- **Features**:
  - Schema validation against database tables
  - GSID resolution via GSID service API
  - Local ID extraction and mapping
  - Duplicate detection
  - Generates validated CSV + metadata
  - Outputs to S3 `staging/validated/`

#### Stage 2: Table Loader

- **Purpose**: Load validated data directly into PostgreSQL
- **Execution**: Manual via Docker or GitHub Actions
- **Features**:
  - Direct PostgreSQL insertion (bypasses NocoDB API)
  - Dynamic schema detection
  - Automatic column filtering
  - Conflict detection for duplicate local IDs
  - Dry-run mode by default (requires `--approve` flag)
  - Transactional with automatic rollback on errors
  - Batch processing with `execute_values`

### 5. NocoDB Frontend

- **Port**: 8080 (internal), 443 (external)
- **Purpose**: User interface for data viewing and management
- **Database**: Separate PostgreSQL instance
- **Note**: Automatically syncs with idHub PostgreSQL data

### 6. Nginx Reverse Proxy

- **Ports**: 80 (HTTP redirect), 443 (HTTPS)
- **Features**:
  - TLS 1.2/1.3 with strong cipher suites
  - HSTS, security headers
  - WebSocket support for NocoDB
  - Certbot webroot authentication

## Directory Structure

```

/opt/idhub/
├── docker-compose.yml # Service orchestration
├── .env # Environment variables (secrets)
├── backup-idhub.sh # Automated backup script
├── sync-certs.sh # SSL certificate sync script
├── nginx/
│ ├── nginx.conf # Main nginx config
│ └── conf.d/
│ ├── nocodb.conf # NocoDB site config
│ └── gsid-api.conf # API site config
├── database/
│ ├── init-scripts/
│ │ ├── 01-schema.sql # Database schema
│ │ └── 02-seed_data.sql # Center seed data
│ └── migrations/
│ └── 02-switch-to-ulid.sql # ULID migration
├── gsid-service/
│ ├── Dockerfile
│ ├── main.py # FastAPI application
│ ├── requirements.txt
│ └── config.py
├── redcap-pipeline/
│ ├── Dockerfile
│ ├── main.py # Pipeline orchestration
│ ├── requirements.txt
│ └── config/
│ └── field_mappings.json # REDCap field mappings
├── fragment-validator/
│ ├── Dockerfile
│ ├── main.py # Validation logic
│ ├── requirements.txt
│ ├── logs/
│ └── config/
│ └── table_schemas.json # Sample table definitions
├── table-loader/
│ ├── Dockerfile
│ ├── main.py # PostgreSQL loader
│ ├── requirements.txt
│ └── logs/
└── backups/ # Automated backups (30-day retention)
├── nocodb/
└── idhub/

````

## Fragment Ingestion Workflow

### Overview

The fragment ingestion process loads curated sample data from S3 into the idHub database. It consists of two stages: validation and loading.

### Prerequisites

- Sample data CSV file with required columns:
  - Primary key column (e.g., `niddk_no`, `sample_id`)
  - `global_subject_id` OR local identifier columns
  - Sample-specific metadata columns
- S3 bucket access configured
- Docker environment running

### Stage 1: Validate Fragment

**Purpose**: Validate sample data and resolve subject identities

```bash
# Run fragment validator
# Executed on an external local client
python fragment-validator/main.py \
  --source legacy_id_db \
  --table lcl \
  --input /path/to/data.csv
  fragment-validator/config/lcl_mapping.json \
````

**Parameters**:

- `--source`: Data source identifier (e.g., `legacy_id_db`, `external_lab`)
- `--table`: Target database table name (e.g., `lcl`, `blood`, `dna`)
- `--input`: Path to input CSV file

**Output** (uploaded to S3 `staging/validated/batch_YYYYMMDD_HHMMSS/`):

- `{table}.csv`: Validated sample records with resolved GSIDs
- `local_subject_ids.csv`: Local ID mappings with actions (`link_existing`, `create_new`)
- `validation_report.json`: Validation summary and statistics

**Validation Steps**:

1. Schema validation against target table
2. GSID resolution via GSID service API
3. Local ID extraction from multiple columns
4. Duplicate detection
5. Data type validation
6. NULL value checks for required fields

### Stage 2: Load Data

**Purpose**: Load validated data into PostgreSQL

#### Dry Run (Preview)

```bash
# Preview what will be loaded (default mode)
docker-compose run --rm table-loader python main.py \
  --batch-id batch_20251012_220934
```

**Output**:

```
DRY RUN: Loading batch batch_20251012_220934
Target table: lcl
Row count: 9341
Loaded 9341 sample records
Loaded 9333 local ID mappings
DRY RUN - No changes will be made
Would insert 156 new subjects
Would insert 9333 local ID mappings
Would upsert 9341 records to lcl
```

#### Execute Load

```bash
# Actually load the data (requires --approve flag)
docker-compose run --rm table-loader python main.py \
  --batch-id batch_20251012_220934 \
  --approve
```

**Output**:

```
Loading batch batch_20251012_220934
Target table: lcl
✓ Inserted 156 new subjects
✓ Inserted 9333 local ID mappings
✓ Upserted 9341 records to lcl
✓ Successfully loaded batch batch_20251012_220934
```

### Verify Load

```bash
# Check record count
docker-compose exec idhub_db psql -U idhub_user -d idhub -c \
  "SELECT COUNT(*) FROM lcl;"

# Check sample records
docker-compose exec idhub_db psql -U idhub_user -d idhub -c \
  "SELECT * FROM lcl LIMIT 5;"

# Check subject linkage
docker-compose exec idhub_db psql -U idhub_user -d idhub -c \
  "SELECT l.niddk_no, l.global_subject_id, s.center_id
   FROM lcl l
   JOIN subjects s ON l.global_subject_id = s.global_subject_id
   LIMIT 5;"

# Check local ID mappings
docker-compose exec idhub_db psql -U idhub_user -d idhub -c \
  "SELECT * FROM local_subject_ids
   WHERE identifier_type = 'niddk_no'
   LIMIT 5;"
```

### View in NocoDB

1. Navigate to https://idhub.ibdgc.org
2. Open the target table (e.g., `lcl`)
3. Verify records are visible
4. Check that `global_subject_id` links properly to `subjects` table

### Automated Execution (GitHub Actions)

**Fragment Validation** (`.github/workflows/fragment-validation.yml`):

```yaml
name: Fragment Validation

on:
  workflow_dispatch:
    inputs:
      source:
        description: "Data source identifier"
        required: true
      table:
        description: "Target table name"
        required: true
      s3_input_key:
        description: "S3 key for input CSV"
        required: true

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up SSH tunnel
        # ... SSH tunnel setup ...

      - name: Run fragment validator
        run: |
          cd fragment-validator
          python main.py \
            --source ${{ github.event.inputs.source }} \
            --table ${{ github.event.inputs.table }} \
            --s3-input s3://$S3_BUCKET/${{ github.event.inputs.s3_input_key }}
```

**Table Loading** (`.github/workflows/fragment-ingestion.yml`):

```yaml
name: Fragment Ingestion Pipeline

on:
  workflow_dispatch:
    inputs:
      batch_id:
        description: "Batch ID to load (e.g., batch_20251012_170035)"
        required: true
      dry_run:
        description: "Dry run mode (no changes)"
        type: boolean
        default: true

jobs:
  ingest:
    runs-on: ubuntu-latest
    steps:
      - name: Run table loader
        run: |
          cd table-loader
          python main.py \
            --batch-id ${{ github.event.inputs.batch_id }} \
            ${{ github.event.inputs.dry_run == 'false' && '--approve' || '' }}
```

### Conflict Resolution

If duplicate local IDs are detected during loading:

1. **Check flagged subjects**:

```bash
docker-compose exec idhub_db psql -U idhub_user -d idhub -c \
  "SELECT global_subject_id, review_notes
   FROM subjects
   WHERE flagged_for_review = TRUE;"
```

2. **Review identity resolutions**:

```bash
docker-compose exec idhub_db psql -U idhub_user -d idhub -c \
  "SELECT * FROM identity_resolutions
   WHERE requires_review = TRUE
   ORDER BY created_at DESC
   LIMIT 10;"
```

3. **Resolve conflicts**:
   - Investigate which GSID is correct
   - Update `local_subject_ids` to point to correct GSID
   - Mark subjects as resolved:

```bash
curl -X POST https://api.idhub.ibdgc.org/resolve-review/{gsid} \
  -H "X-API-Key: ${GSID_API_KEY}"
```

### Best Practices

1. **Always run dry-run first** to preview changes
2. **Validate batch IDs** exist in S3 before loading
3. **Check logs** for warnings about skipped columns or conflicts
4. **Verify record counts** match expectations after loading
5. **Monitor NocoDB** to ensure foreign key relationships display correctly
6. **Keep validation reports** for audit trail

### Troubleshooting

**Batch not found**:

```bash
# Check if batch exists in S3
aws s3 ls s3://idhub-curated-fragments/staging/validated/

# Check if already loaded
aws s3 ls s3://idhub-curated-fragments/staging/loaded/
```

**Column mismatch errors**:

- Loader automatically skips columns not in database schema
- Check logs for "Skipping columns not in table schema" warnings
- Update table schema if new columns are needed

**Foreign key constraint violations**:

- Ensure all `global_subject_id` values exist in `subjects` table
- Validator should create missing subjects automatically
- Check `local_subject_ids.csv` for `action = 'create_new'` entries

**Memory issues**:

- Fragment validator and table loader run with 256MB memory limit
- For large datasets (>50k rows), consider splitting into multiple batches
- Use `page_size=1000` in `execute_values` for batch inserts

## Key Features

### GSID Generation

- **Format**: 12-character Base32 string (Crockford alphabet)
- **Structure**: `TTTTTTRRRRRR` (6 timestamp + 6 random)
- **Collision Resistance**: Timestamp-based ordering + cryptographic randomness
- **Example**: `01HQXK8N9P2M`

### Identity Resolution Strategy

1. **Exact Match**: Check `local_subject_ids` for existing mapping
2. **Alias Match**: Check `subject_alias` table for known aliases
3. **Withdrawn Check**: Flag if matched subject is withdrawn
4. **Create New**: Generate new GSID if no match found
5. **Conflict Detection**: Flag when local_id maps to multiple GSIDs

### Center Matching

1. **Alias Lookup**: Pre-defined aliases (e.g., `mount_sinai` → `MSSM`)
2. **Exact Match**: Direct name comparison
3. **Fuzzy Match**: SequenceMatcher with 70% threshold
4. **Auto-Create**: New center created if no match (flagged in logs)

### Multi-ID Registration

- Extracts `consortium_id`, `local_id`, `subject_id`, `patient_id` from REDCap
- Registers all available IDs to same GSID
- Tracks `identifier_type` for each local ID
- Detects conflicts when ID already linked to different GSID
- Flags both subjects for manual review on conflict

## API Endpoints

### GSID Service (`api.idhub.ibdgc.org`)

**POST /register**

```json
{
  "center_id": 42,
  "local_subject_id": "ABC123",
  "identifier_type": "niddk_no",
  "registration_year": "2023",
  "control": false,
  "created_by": "fragment_validator"
}
```

**GET /review-queue**

- Returns subjects flagged for manual review

**POST /resolve-review/{gsid}**

- Mark review as resolved

**GET /health**

- Service health check

## Management Commands

### Service Management

```bash
cd /opt/idhub

# View status
docker-compose ps

# View logs
docker-compose logs -f gsid-service
docker-compose logs -f redcap-pipeline
docker-compose logs -f table-loader

# Restart services
docker-compose restart gsid-service

# Rebuild after code changes
docker-compose build table-loader
docker-compose build fragment-validator
```

### Database Management

```bash
# Access IDhub database
docker exec -it idhub_db psql -U idhub_user -d idhub

# Backup databases
./backup-idhub.sh

# Manual backup
docker exec idhub_db pg_dump -U idhub_user idhub | gzip > backup_$(date +%Y%m%d).sql.gz

# Restore database
gunzip < backup_20250107.sql.gz | docker exec -i idhub_db psql -U idhub_user -d idhub
```

### SSL Certificate Management

```bash
# Check certificate status
sudo certbot certificates

# Test renewal
sudo certbot renew --dry-run

# Sync certificates to containers
./sync-certs.sh

# Manual renewal (if needed)
sudo certbot renew --force-renewal
./sync-certs.sh
```

### REDCap Pipeline

```bash
# Run locally (testing only)
docker-compose --profile pipeline up redcap-pipeline

# Production runs via GitHub Actions
# Triggered: Manual dispatch or scheduled cron
```

### Fragment Ingestion

```bash
# Validate fragment
# Ran externally on a local client
python fragment-validator/main.py \
  --source legacy_id_db \
  --table lcl \
  --input /path/to/data.csv
  fragment-validator/config/lcl_mapping.json \

# Load validated batch (dry-run)
docker-compose run --rm table-loader python main.py \
  --batch-id batch_20251012_220934

# Load validated batch (execute)
docker-compose run --rm table-loader python main.py \
  --batch-id batch_20251012_220934 \
  --approve
```

## Configuration

### Environment Variables (.env)

```bash
# Domains
DOMAIN=idhub.ibdgc.org

# Database Passwords
NOCODB_DB_PASSWORD=<secure_password>
IDHUB_DB_PASSWORD=<secure_password>
JWT_SECRET=<secure_secret>

# REDCap Integration
REDCAP_API_URL=https://redcap.example.org/api/
REDCAP_API_TOKEN=<redcap_token>
REDCAP_PROJECT_ID=16894

# AWS S3
AWS_ACCESS_KEY_ID=<aws_key>
AWS_SECRET_ACCESS_KEY=<aws_secret>
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=idhub-curated-fragments

# idHub Database
IDHUB_DB_NAME=idhub
IDHUB_DB_USER=idhub_user

# GSID Service
GSID_API_KEY=<api_key>
```

### Center Aliases (redcap-pipeline/main.py)

```python
CENTER_ALIASES = {
    'mount_sinai': 'MSSM',
    'cedars_sinai': 'Cedars-Sinai',
    'johns_hopkins': 'Johns Hopkins',
    'mass_general': 'Massachusetts General Hospital',
    # ... add more as needed
}
```

## Database Schema Highlights

### subjects

- `global_subject_id` (VARCHAR(12), PK): ULID-based GSID
- `center_id` (INT, FK): Research center
- `registration_year` (DATE): Year only (e.g., 2023-01-01)
- `flagged_for_review` (BOOLEAN): Manual review required
- `family_id` (VARCHAR, FK): Family linkage

### local_subject_ids

- `center_id` + `local_subject_id` + `identifier_type` (Composite PK)
- `identifier_type` (VARCHAR): Source field name
- `global_subject_id` (VARCHAR(12), FK): Links to GSID

### identity_resolutions

- Audit trail for all registration attempts
- Tracks match strategy, confidence, review status
- Enables conflict analysis and data quality monitoring

### Sample Tables (lcl, blood, dna, etc.)

- Primary key varies by table (e.g., `niddk_no`, `sample_id`)
- `global_subject_id` (VARCHAR(12), FK): Links to subjects
- Sample-specific metadata columns
- `created_at`, `updated_at`: Automatic timestamps

## Security

- **HTTPS Only**: Enforced with HSTS headers
- **Database Isolation**: PostgreSQL not exposed to internet
- **SSH Tunnel**: GitHub Actions connects via SSH for pipeline
- **Secrets Management**: Environment variables, not in version control
- **PHI Protection**: No REDCap PHI stored; processed in-memory only
- **S3 Encryption**: Server-side AES256 encryption

## Monitoring & Logs

```bash
# Application logs
docker-compose logs --tail=100 -f

# Pipeline logs
tail -f redcap-pipeline/logs/pipeline.log
tail -f fragment-validator/logs/validator.log
tail -f table-loader/logs/loader.log

# Nginx access logs
docker exec nginx tail -f /var/log/nginx/access.log

# System logs
sudo journalctl -u docker -f
```

## Backup Strategy

- **Automated**: Daily backups via cron (30-day retention)
- **Scope**: Both NocoDB and idHub databases
- **Location**: `/opt/idhub/backups/`
- **Script**: `./backup-idhub.sh`

## Troubleshooting

### Pipeline OOM Errors

- Pipeline runs in GitHub Actions (not EC2) to avoid memory constraints
- Batch size: 50 records (configurable in `main.py`)
- Connection pooling: 1-10 connections

### Center Matching Issues

- Check `CENTER_ALIASES` dictionary for known variations
- Fuzzy matching threshold: 0.7 (70% similarity)
- New centers auto-created with "Unknown" metadata

### Conflict Resolution

- Check `/review-queue` API endpoint
- Review `identity_resolutions` table for audit trail
- Use `/resolve-review/{gsid}` to mark as resolved

### Fragment Loading Issues

- **Batch not found**: Check S3 paths in `staging/validated/` and `staging/loaded/`
- **Column mismatches**: Loader auto-skips extra columns; check logs for warnings
- **FK violations**: Ensure validator created all required subjects
- **Memory errors**: Split large datasets into smaller batches

## Development

### Local Testing

```bash
# Start core services
docker-compose up -d

# Test GSID service
curl https://api.idhub.ibdgc.org/health

# Run pipeline locally (not recommended for production)
docker-compose --profile pipeline up redcap-pipeline

# Test fragment validation
docker-compose run --rm fragment-validator python main.py \
  --source test \
  --table lcl \
  --input /path/to/test.csv

# Test table loading (dry-run)
docker-compose run --rm table-loader python main.py \
  --batch-id batch_YYYYMMDD_HHMMSS
```

### Schema Changes

1. Create migration SQL in `database/migrations/`
2. Apply manually: `docker exec -i idhub_db psql -U idhub_user -d idhub < migration.sql`
3. Update `database/init-scripts/01-schema.sql` for fresh deployments
4. Update `fragment-validator/config/table_schemas.json` if adding new sample tables

## Support & Resources

- **FastAPI Docs**: https://fastapi.tiangolo.com/
- **NocoDB Docs**: https://nocodb.com/docs
- **PostgreSQL Docs**: https://www.postgresql.org/docs/15/
- **REDCap API**: https://redcap.example.org/api/help/

## Notes

- Certificate auto-renews when <30 days remaining
- Elastic IP must remain allocated to maintain DNS
- GitHub Actions requires SSH key for database tunnel
- Registration year stored as DATE but only year is significant
- Family IDs displayed in NocoDB via Lookup field (known UI limitation)
- Fragment loader uses direct PostgreSQL insertion for reliability
- NocoDB automatically syncs with PostgreSQL data changes
- Always run dry-run mode first before executing loads

```
