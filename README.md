# IBDGC IDhub

Project and Dataset Intersection Through Subject-Sample Identifiers

## Overview

**IDhub** is a centralized identity hub for managing biomedical research subjects and samples across multiple research centers. It provides:

- **Global Subject IDs (GSIDs)**: 12-character ULID-inspired unique identifiers
- **Identity Resolution**: Automatic matching and conflict detection for subject registration
- **REDCap Integration**: Automated data ingestion pipeline
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
              ↓                           ↓
        NocoDB Container          GSID Service (FastAPI)
              ↓                           ↓
        NocoDB PostgreSQL         idHub PostgreSQL
                                          ↑
                                          │
                              REDCap Pipeline (GitHub Actions)
                                          ↓
                                    AWS S3 (Curated Fragments)
```

## Infrastructure

- **Platform**: AWS EC2 t3.micro (2 vCPU, 1GB RAM)
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
  - `dna`, `blood`, `wgs`, `immunochip`, etc.: Sample tables
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

### 4. NocoDB Frontend

- **Port**: 8080 (internal), 443 (external)
- **Purpose**: User interface for data viewing and management
- **Database**: Separate PostgreSQL instance

### 5. Nginx Reverse Proxy

- **Ports**: 80 (HTTP redirect), 443 (HTTPS)
- **Features**:
  - TLS 1.2/1.3 with strong cipher suites
  - HSTS, security headers
  - WebSocket support for NocoDB
  - Certbot webroot authentication

## Directory Structure

```
/opt/idhub/
├── docker-compose.yml              # Service orchestration
├── .env                            # Environment variables (secrets)
├── backup-idhub.sh                 # Automated backup script
├── sync-certs.sh                   # SSL certificate sync script
├── nginx/
│   ├── nginx.conf                  # Main nginx config
│   └── conf.d/
│       ├── nocodb.conf             # NocoDB site config
│       └── gsid-api.conf           # API site config
├── database/
│   ├── init-scripts/
│   │   ├── 01-schema.sql           # Database schema
│   │   └── 02-seed_data.sql        # Center seed data
│   └── migrations/
│       └── 02-switch-to-ulid.sql   # ULID migration
├── gsid-service/
│   ├── Dockerfile
│   ├── main.py                     # FastAPI application
│   ├── requirements.txt
│   └── config.py
├── redcap-pipeline/
│   ├── Dockerfile
│   ├── main.py                     # Pipeline orchestration
│   ├── requirements.txt
│   └── config/
│       └── field_mappings.json     # REDCap field mappings
└── backups/                        # Automated backups (30-day retention)
    ├── nocodb/
    └── idhub/
```

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
  "registration_year": "2023",
  "control": false,
  "created_by": "redcap_pipeline"
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

# Restart services
docker-compose restart gsid-service

# Rebuild after code changes
docker-compose up -d --build gsid-service
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

- `center_id` + `local_subject_id` (Composite PK)
- `identifier_type` (VARCHAR): Source field name
- `global_subject_id` (VARCHAR(12), FK): Links to GSID

### identity_resolutions

- Audit trail for all registration attempts
- Tracks match strategy, confidence, review status
- Enables conflict analysis and data quality monitoring

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

## Development

### Local Testing

```bash
# Start core services
docker-compose up -d

# Test GSID service
curl https://api.idhub.ibdgc.org/health

# Run pipeline locally (not recommended for production)
docker-compose --profile pipeline up redcap-pipeline
```

### Schema Changes

1. Create migration SQL in `database/migrations/`
2. Apply manually: `docker exec -i idhub_db psql -U idhub_user -d idhub < migration.sql`
3. Update `database/init-scripts/01-schema.sql` for fresh deployments

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

undefined
