# IDhub Platform

**Integrated ID Hub for the IBD Genetics Consortium**

A comprehensive data integration and management platform for multi-center inflammatory bowel disease (IBD) research, providing centralized subject identification, data harmonization, and quality-controlled data pipelines.

---

## Overview

IDhub is a cloud-native platform that orchestrates the collection, validation, and integration of research data from multiple sources (REDCap projects, manual uploads, external systems) into a unified PostgreSQL database with a NocoDB frontend. The platform ensures data quality through automated validation, provides global subject identification (GSID) for cross-study linkage, and maintains complete audit trails for regulatory compliance.

### Key Capabilities

- **Global Subject Identification**: Collision-resistant GSID generation with intelligent identity resolution
- **Automated Data Pipelines**: REDCap synchronization, fragment validation, and database loading
- **Multi-Environment Support**: Separate QA and Production environments with CI/CD automation
- **Data Quality Assurance**: Schema validation, type checking, and referential integrity enforcement
- **Audit & Compliance**: Complete tracking of data lineage and transformation history
- **Flexible Integration**: REST APIs, S3-based staging, and configurable field mappings

---

## Architecture

TODO make better architecture diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Data Sources                                   │
│  REDCap Projects  │  Manual Uploads  │  External Systems               │
└──────────┬──────────────────┬────────────────────┬─────────────────────┘
           │                  │                    │
           ▼                  ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Data Ingestion Layer                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │
│  │   REDCap     │  │   Fragment   │  │   Manual     │                 │
│  │   Pipeline   │  │   Validator  │  │   Import     │                 │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                 │
└─────────┼──────────────────┼──────────────────┼──────────────────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    S3 Staging (Validated Fragments)                     │
│              s3://idhub-curated-fragments/staging/validated/            │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       Table Loader Service                              │
│              (Batch Loading with Transaction Management)                │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Core Platform Services                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │
│  │    GSID      │  │  PostgreSQL  │  │   NocoDB     │                 │
│  │   Service    │  │   Database   │  │   Frontend   │                 │
│  │  (FastAPI)   │  │  (Primary)   │  │   (UI/API)   │                 │
│  └──────────────┘  └──────────────┘  └──────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
          │                  │                  │
          └──────────────────┴──────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Nginx Reverse  │
                    │      Proxy      │
                    │  (SSL/TLS)      │
                    └─────────────────┘
                             │
                             ▼
                    Internet (HTTPS)
```

---

## Core Services

### 1. [GSID Service](./gsid-service/README.md)

**Global Subject ID Generation & Identity Resolution**

- **Purpose**: Centralized subject identification across research centers
- **Technology**: FastAPI, PostgreSQL
- **Key Features**:
  - Cryptographic 16-character GSIDs, truncated ULID standard (`GSID-XXXXXXXXXXXXXXXX`)
  - Intelligent identity resolution (exact, alias, fuzzy matching)
  - Multi-center support with local ID mapping
  - Conflict detection and review workflow
  - Complete audit logging
- **API Endpoints**:
  - `POST /register` - Register subjects with identity resolution
  - `POST /register/batch` - Batch registration (up to 1000 subjects)
  - `GET /lookup/{gsid}` - Retrieve subject information
  - `GET /health` - Service health check

**[→ Full Documentation](./gsid-service/README.md)**

---

### 2. [REDCap Pipeline](./redcap-pipeline/README.md)

**Automated REDCap Data Integration**

- **Purpose**: Continuous synchronization of research data from REDCap projects
- **Technology**: Python, Boto3, REDCap API
- **Key Features**:
  - Multi-project support with independent configurations
  - Automated GSID registration with center resolution
  - Flexible JSON-based field mapping
  - S3 fragment staging for downstream processing
  - Batch processing with configurable sizes
  - Comprehensive error handling and logging
- **Execution**: GitHub Actions (scheduled daily at 2 AM UTC)
- **Supported Projects**: GAP, CD Ileal, UC Demarc (configurable)

**[→ Full Documentation](./redcap-pipeline/README.md)**

---

### 3. [Fragment Validator](./fragment-validator/README.md)

**Data Quality Assurance & Staging**

- **Purpose**: Validate data fragments against target schemas before database loading
- **Technology**: Python, Pandas, NocoDB API
- **Key Features**:
  - Schema validation against NocoDB table metadata
  - GSID resolution for subject identifiers
  - Field mapping and transformation
  - Data type validation and null handling
  - Detailed validation reports with errors/warnings
  - S3 staging of validated fragments
- **Usage**: CLI tool for manual validation or automated pipeline integration
- **Output**: Validated CSV fragments + JSON validation reports

**[→ Full Documentation](./fragment-validator/README.md)**

---

### 4. [Table Loader](./table-loader/README.md)

**Database Loading & Transaction Management**

- **Purpose**: Load validated fragments from S3 into PostgreSQL database tables
- **Technology**: Python, psycopg2, Boto3
- **Key Features**:
  - Batch loading with configurable batch sizes
  - Multiple load strategies (INSERT, UPSERT)
  - Data type conversion and cleaning
  - Transaction management with rollback on failure
  - Dry-run mode for preview
  - Automatic fragment archiving post-load
- **Execution**: GitHub Actions (manual workflow dispatch)
- **Safety**: Requires explicit approval flag for production loads

**[→ Full Documentation](./table-loader/README.md)**

---

### 5. [Nginx Reverse Proxy](./nginx/README.md)

**SSL Termination & Traffic Routing**

- **Purpose**: Entry point for all HTTP/HTTPS traffic
- **Technology**: Nginx, Let's Encrypt
- **Key Features**:
  - Automatic HTTPS with Let's Encrypt certificates
  - Environment-specific routing (QA/Production)
  - HTTP/2 support
  - Security headers (HSTS, CSP, X-Frame-Options)
  - Connection pooling and keepalive
  - Health check endpoints
- **Domains**:
  - Production: `idhub.ibdgc.org`, `api.idhub.ibdgc.org`
  - QA: `qa.idhub.ibdgc.org`, `api.qa.idhub.ibdgc.org`

**[→ Full Documentation](./nginx/README.md)**

---

## Automated Workflows (GitHub Actions)

### 1. **Deployment Pipeline** (`.github/workflows/deploy.yml`)

Automated deployment to QA and Production environments.

**Triggers**:

- Push to `qa` or `prod` branches
- Manual workflow dispatch

**Process**:

1. Environment detection (QA/Production)
2. SSH key setup and server connection
3. Environment configuration generation
4. Git pull latest code
5. Nginx configuration processing
6. Docker Compose service rebuild
7. Health checks (GSID service, NocoDB, Nginx)
8. Deployment verification

**Deployed Services**: GSID Service, NocoDB, Nginx, REDCap Pipeline, Fragment Validator, Table Loader

---

### 2. **REDCap Sync Pipeline** (`.github/workflows/redcap-sync.yml`)

Scheduled and on-demand REDCap data synchronization.

**Triggers**:

- Scheduled: Daily at 2 AM UTC
- Manual workflow dispatch (with project selection)

**Process**:

1. Environment selection (QA/Production)
2. SSH tunnel to database
3. Python environment setup
4. REDCap pipeline execution (all projects or specific project)
5. Log upload to artifacts

**Output**: Curated fragments uploaded to S3 staging area

---

### 3. **Fragment Ingestion Pipeline** (`.github/workflows/fragment-ingestion.yml`)

Manual workflow for loading validated fragments into the database.

**Triggers**: Manual workflow dispatch only

**Inputs**:

- `environment`: Target environment (QA/Production)
- `batch_id`: Batch identifier (format: `batch_YYYYMMDD_HHMMSS`)
- `dry_run`: Preview mode (default: true)

**Process**:

1. Batch ID validation
2. SSH tunnel to database
3. S3 batch verification
4. Python environment setup
5. Table loader execution (dry-run or live)
6. Load summary and log upload

**Safety Features**:

- Dry-run mode by default
- Explicit approval required for live loads
- Comprehensive validation before loading

---

### 4. **Test & Coverage** (`.github/workflows/test-and-coverage.yml`)

Automated testing and coverage reporting for all services.

**Triggers**:

- Push to `main`, `develop`, `prod`, `qa` branches
- Pull requests
- Manual workflow dispatch

**Process**:

1. Matrix build for all services (GSID, REDCap, Validator, Loader)
2. Docker test container build
3. Pytest execution with coverage
4. Coverage report generation (HTML, XML, JUnit)
5. Codecov upload
6. Artifact upload (coverage reports, test results)

**Coverage Targets**: 75%+ for all services

---

## Data Flow

TODO generate better data flow diagram

### Complete Pipeline Flow

```
┌─────────────────┐
│  REDCap Project │
└────────┬────────┘
         │ (1) Extract
         ▼
┌─────────────────┐
│ REDCap Pipeline │ ← GitHub Actions (Daily 2 AM UTC)
└────────┬────────┘
         │ (2) Transform & GSID Registration
         ▼
┌─────────────────┐
│  S3 Staging     │ ← s3://idhub-curated-fragments/staging/validated/
│  (Fragments)    │
└────────┬────────┘
         │ (3) Manual Approval
         ▼
┌─────────────────┐
│ Fragment        │ ← GitHub Actions (Manual Trigger)
│ Ingestion       │
└────────┬────────┘
         │ (4) Load to Database
         ▼
┌─────────────────┐
│  PostgreSQL     │
│  Database       │
└────────┬────────┘
         │ (5) Display
         ▼
┌─────────────────┐
│    NocoDB       │ ← https://idhub.ibdgc.org
│   (Frontend)    │
└─────────────────┘
```

---

## Infrastructure

### Deployment Architecture

- **Platform**: AWS EC2 t3.small (2 vCPU, 2GB RAM)
- **OS**: Amazon Linux 2023
- **Container Orchestration**: Docker Compose
- **SSL/TLS**: Let's Encrypt with automatic renewal
- **Environments**: QA and Production (separate EC2 instances)

### Database Schema

**Core Tables**:

- `subjects` - Subject records with GSIDs
- `centers` - Research center registry
- `identity_resolutions` - GSID resolution audit log
- `redcap_sync_log` - REDCap pipeline execution history
- Sample tables: `blood`, `lcl`, `dna`, `rna`, `specimen`, etc.

### AWS Resources

- **S3 Buckets**:
  - `idhub-curated-fragments` (Production)
  - `idhub-curated-fragments-qa` (QA)
- **IAM**: Service-specific roles with least-privilege access
- **Secrets Manager**: API keys, database credentials (via GitHub Secrets)

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- AWS CLI (for S3 access)
- Python 3.11+ (for local development)
- PostgreSQL client (for database access)

### Local Development Setup

```bash
# Clone repository
git clone https://github.com/ibdgc/idhub.git
cd idhub

# Create unified conda environment
conda env create -f environment.yml
conda activate idhub-dev

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Start core services
docker-compose up -d nocodb idhub_db gsid-service

# Verify services
docker-compose ps
curl http://localhost:8000/health  # GSID service
curl http://localhost:8080/api/v1/health  # NocoDB
```

### Running Individual Services

```bash
# REDCap Pipeline
cd redcap-pipeline
python main.py --project gap

# Fragment Validator
cd fragment-validator
python main.py --table-name blood --input-file data.csv --mapping-config config.json --source redcap

# Table Loader
cd table-loader
python main.py --batch-id batch_20240115_120000 --dry-run
```

### Running Tests

```bash
# All services
docker-compose -f docker-compose.test.yml up

# Specific service
docker-compose -f docker-compose.test.yml run --rm test-gsid
docker-compose -f docker-compose.test.yml run --rm test-redcap
docker-compose -f docker-compose.test.yml run --rm test-validator
docker-compose -f docker-compose.test.yml run --rm test-loader

# Local pytest
cd gsid-service
pytest --cov=. --cov-report=html
```

---

## Deployment

### QA Environment

```bash
# Push to qa branch triggers automatic deployment
git checkout qa
git merge develop
git push origin qa

# Or manual deployment via GitHub Actions
# Actions → Deploy to Environment → Run workflow → Select "qa"
```

### Production Environment

```bash
# Push to prod branch triggers automatic deployment
git checkout prod
git merge main
git push origin prod

# Or manual deployment via GitHub Actions
# Actions → Deploy to Environment → Run workflow → Select "prod"
```

### Manual Fragment Loading

```bash
# Via GitHub Actions
# Actions → Fragment Ingestion Pipeline → Run workflow
# - Environment: qa/prod
# - Batch ID: batch_20240115_120000
# - Dry Run: true (preview) or false (live load)
```

---

## Monitoring & Operations

### Health Checks

```bash
# GSID Service
curl https://api.idhub.ibdgc.org/health

# NocoDB
curl https://idhub.ibdgc.org/api/v1/health

# Nginx
curl -I https://idhub.ibdgc.org
```

### Logs

```bash
# Docker logs
docker-compose logs -f gsid-service
docker-compose logs -f nocodb
docker-compose logs -f nginx

# GitHub Actions logs
# Available in Actions tab for each workflow run

# Service-specific logs
tail -f redcap-pipeline/logs/pipeline.log
tail -f fragment-validator/logs/validator.log
tail -f table-loader/logs/loader.log
```

### Database Queries

TODO expand query examples

```sql
-- Recent GSID registrations
SELECT * FROM subjects ORDER BY created_at DESC LIMIT 50;

-- REDCap sync status
SELECT project_key, status, COUNT(*)
FROM redcap_sync_log
GROUP BY project_key, status;

-- Identity resolution conflicts
SELECT * FROM identity_resolutions
WHERE action = 'review_required'
ORDER BY created_at DESC;
```

---

## Security

### Authentication & Authorization

- **GSID API**: API key authentication (`X-API-Key` header)
- **NocoDB**: JWT-based authentication
- **GitHub Actions**: Environment-specific secrets
- **Database**: SSL/TLS connections in production

### Data Protection

- **Encryption at Rest**: S3 server-side encryption (AES256)
- **Encryption in Transit**: TLS 1.2/1.3 for all HTTPS traffic
- **PHI/PII**: Pseudonymized with GSIDs
- **Access Control**: Role-based access in NocoDB
- **Audit Logging**: Complete tracking of all data operations

### Secrets Management

All sensitive credentials stored in GitHub Secrets:

- Database passwords
- API keys (GSID, REDCap)
- AWS credentials
- SSH keys for deployment

---

## Contributing

### Development Workflow

1. Create feature branch from `develop`
2. Implement changes with tests
3. Ensure tests pass: `pytest`
4. Update relevant README files
5. Submit pull request to `develop`
6. After review, merge to `develop`
7. Deploy to QA for testing
8. Merge to `main` and `prod` for production release

### Code Standards

- **Python**: PEP 8, type hints, docstrings
- **Testing**: 75%+ coverage required
- **Documentation**: Update READMEs for new features
- **Commits**: Conventional commit messages

### Adding New Services

1. Create service directory with standard structure
2. Add Dockerfile and requirements.txt
3. Update docker-compose.yml
4. Add test configuration to docker-compose.test.yml
5. Create comprehensive README.md
6. Add GitHub Actions workflow if needed

---

## Troubleshooting

### Common Issues

**Issue**: GSID service connection refused

```bash
# Check service status
docker-compose ps gsid-service
docker-compose logs gsid-service

# Restart service
docker-compose restart gsid-service
```

**Issue**: Database connection failed

```bash
# Verify database is running
docker-compose ps idhub_db

# Check connection
psql -h localhost -U idhub_user -d idhub
```

**Issue**: S3 access denied

```bash
# Verify AWS credentials
aws s3 ls s3://idhub-curated-fragments

# Check IAM permissions
aws iam get-user
```

**Issue**: Deployment failed

```bash
# Check GitHub Actions logs
# SSH into server and check Docker logs
ssh user@server
cd /opt/idhub
docker-compose logs --tail=100
```

---

## Support & Resources

- **GitHub Issues**: https://github.com/ibdgc/idhub/issues
- **Documentation**: Service-specific READMEs in each directory
- **Platform Team**: Contact via GitHub or institutional email

---

## License

[Add license information]

---

## Changelog

### v1.0.0 (2024-01-15)

- Initial platform release
- GSID service with identity resolution
- REDCap pipeline with multi-project support
- Fragment validator with schema validation
- Table loader with transaction management
- Nginx reverse proxy with SSL/TLS
- GitHub Actions CI/CD pipelines
- Comprehensive documentation

---
