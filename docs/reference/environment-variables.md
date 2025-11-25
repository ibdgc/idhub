# Environment Variables Reference

## Overview

This document provides a comprehensive reference for all environment variables used across the IDhub platform services.

## Table of Contents

- [Common Variables](#common-variables)
- [Database Configuration](#database-configuration)
- [GSID Service](#gsid-service)
- [NocoDB](#nocodb)
- [REDCap Pipeline](#redcap-pipeline)
- [Fragment Validator](#fragment-validator)
- [Table Loader](#table-loader)
- [LabKey Sync](#labkey-sync)
- [Nginx](#nginx)
- [AWS Configuration](#aws-configuration)
- [Monitoring & Logging](#monitoring--logging)

---

## Common Variables

Variables used across multiple services.

### Environment Selection

| Variable      | Description            | Required | Default       | Valid Values                                    |
| ------------- | ---------------------- | -------- | ------------- | ----------------------------------------------- |
| `ENVIRONMENT` | Deployment environment | No       | `development` | `development`, `qa`, `production`               |
| `DEBUG`       | Enable debug mode      | No       | `false`       | `true`, `false`                                 |
| `LOG_LEVEL`   | Logging verbosity      | No       | `INFO`        | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

**Example:**

```bash
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO
```

---

## Database Configuration

PostgreSQL database connection settings.

### Connection Parameters

| Variable      | Description       | Required | Default      | Example                       |
| ------------- | ----------------- | -------- | ------------ | ----------------------------- |
| `DB_HOST`     | Database hostname | Yes      | `idhub_db`   | `localhost`, `db.example.com` |
| `DB_PORT`     | Database port     | No       | `5432`       | `5432`                        |
| `DB_NAME`     | Database name     | Yes      | `idhub`      | `idhub`, `idhub_qa`           |
| `DB_USER`     | Database username | Yes      | `idhub_user` | `idhub_user`                  |
| `DB_PASSWORD` | Database password | Yes      | -            | `secure_password_here`        |
| `DB_SCHEMA`   | Database schema   | No       | `public`     | `public`, `idhub_schema`      |

**Example:**

```bash
DB_HOST=idhub_db
DB_PORT=5432
DB_NAME=idhub
DB_USER=idhub_user
DB_PASSWORD=your_secure_password_here
DB_SCHEMA=public
```

### Connection Pool Settings

| Variable          | Description                       | Required | Default | Range           |
| ----------------- | --------------------------------- | -------- | ------- | --------------- |
| `DB_POOL_SIZE`    | Connection pool size              | No       | `20`    | `5-100`         |
| `DB_MAX_OVERFLOW` | Max overflow connections          | No       | `10`    | `0-50`          |
| `DB_POOL_TIMEOUT` | Pool timeout (seconds)            | No       | `30`    | `10-300`        |
| `DB_POOL_RECYCLE` | Connection recycle time (seconds) | No       | `3600`  | `300-7200`      |
| `DB_ECHO`         | Echo SQL statements               | No       | `false` | `true`, `false` |

**Example:**

```bash
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=10
DB_POOL_TIMEOUT=30
DB_POOL_RECYCLE=3600
DB_ECHO=false
```

### SSL Configuration

| Variable           | Description             | Required | Default  | Valid Values                                                        |
| ------------------ | ----------------------- | -------- | -------- | ------------------------------------------------------------------- |
| `DB_SSL_MODE`      | SSL mode                | No       | `prefer` | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `DB_SSL_CERT`      | Client certificate path | No       | -        | `/path/to/client-cert.pem`                                          |
| `DB_SSL_KEY`       | Client key path         | No       | -        | `/path/to/client-key.pem`                                           |
| `DB_SSL_ROOT_CERT` | Root certificate path   | No       | -        | `/path/to/root-cert.pem`                                            |

**Example:**

```bash
DB_SSL_MODE=require
DB_SSL_CERT=/etc/ssl/certs/client-cert.pem
DB_SSL_KEY=/etc/ssl/private/client-key.pem
DB_SSL_ROOT_CERT=/etc/ssl/certs/root-cert.pem
```

---

## GSID Service

Global Subject ID service configuration.

### Service Configuration

| Variable           | Description            | Required | Default                 | Example                               |
| ------------------ | ---------------------- | -------- | ----------------------- | ------------------------------------- |
| `GSID_API_KEY`     | API authentication key | Yes      | -                       | `your-secure-random-key-min-32-chars` |
| `GSID_SERVICE_URL` | Service URL            | No       | `http://localhost:8000` | `https://api.idhub.ibdgc.org`         |
| `GSID_PREFIX`      | GSID prefix            | No       | `GSID`                  | `GSID`, `IBD`                         |
| `GSID_LENGTH`      | GSID length            | No       | `13`                    | `10-20`                               |

**Example:**

```bash
GSID_API_KEY=your-secure-random-key-min-32-chars-long
GSID_SERVICE_URL=http://gsid-service:8000
GSID_PREFIX=GSID
GSID_LENGTH=13
```

### API Settings

| Variable          | Description               | Required | Default | Range     |
| ----------------- | ------------------------- | -------- | ------- | --------- |
| `API_RATE_LIMIT`  | Requests per minute       | No       | `100`   | `10-1000` |
| `API_TIMEOUT`     | Request timeout (seconds) | No       | `30`    | `5-300`   |
| `API_MAX_RETRIES` | Max retry attempts        | No       | `3`     | `0-10`    |
| `API_RETRY_DELAY` | Retry delay (seconds)     | No       | `1`     | `1-60`    |

**Example:**

```bash
API_RATE_LIMIT=100
API_TIMEOUT=30
API_MAX_RETRIES=3
API_RETRY_DELAY=1
```

### ULID Configuration

| Variable               | Description     | Required | Default       | Notes         |
| ---------------------- | --------------- | -------- | ------------- | ------------- |
| `ULID_TIMESTAMP_BITS`  | Timestamp bits  | No       | `48`          | Do not change |
| `ULID_RANDOMNESS_BITS` | Randomness bits | No       | `80`          | Do not change |
| `ULID_ENCODING`        | Encoding scheme | No       | `Crockford32` | Do not change |

**Example:**

```bash
ULID_TIMESTAMP_BITS=48
ULID_RANDOMNESS_BITS=80
ULID_ENCODING=Crockford32
```

---

## NocoDB

NocoDB configuration variables.

### Service Configuration

| Variable             | Description | Required | Default                 | Example                      |
| -------------------- | ----------- | -------- | ----------------------- | ---------------------------- |
| `NOCODB_URL`         | NocoDB URL  | Yes      | `http://localhost:8080` | `https://idhub.ibdgc.org`    |
| `NOCODB_API_TOKEN`   | API token   | Yes      | -                       | `your_nocodb_api_token`      |
| `NC_AUTH_JWT_SECRET` | JWT secret  | Yes      | -                       | `random-secret-min-32-chars` |

**Example:**

```bash
NOCODB_URL=http://nocodb:8080
NOCODB_API_TOKEN=your_nocodb_api_token_here
NC_AUTH_JWT_SECRET=your-random-jwt-secret-min-32-chars
```

### Database Connection

| Variable       | Description                   | Required | Default | Notes                      |
| -------------- | ----------------------------- | -------- | ------- | -------------------------- |
| `NC_DB`        | Database connection string    | Yes      | -       | Uses PostgreSQL connection |
| `DATABASE_URL` | Alternative connection string | No       | -       | Same as NC_DB              |

**Example:**

```bash
NC_DB=pg://idhub_user:password@idhub_db:5432/idhub
```

### Application Settings

| Variable            | Description       | Required | Default                 | Valid Values    |
| ------------------- | ----------------- | -------- | ----------------------- | --------------- |
| `NC_PUBLIC_URL`     | Public URL        | No       | `http://localhost:8080` | Full URL        |
| `NC_DISABLE_TELE`   | Disable telemetry | No       | `true`                  | `true`, `false` |
| `NC_ADMIN_EMAIL`    | Admin email       | No       | -                       | Valid email     |
| `NC_ADMIN_PASSWORD` | Admin password    | No       | -                       | Secure password |

**Example:**

```bash
NC_PUBLIC_URL=https://idhub.ibdgc.org
NC_DISABLE_TELE=true
NC_ADMIN_EMAIL=admin@example.com
NC_ADMIN_PASSWORD=secure_admin_password
```

---

## REDCap Pipeline

REDCap data pipeline configuration.

### REDCap API Configuration

| Variable                     | Description         | Required | Default | Example                           |
| ---------------------------- | ------------------- | -------- | ------- | --------------------------------- |
| `REDCAP_API_URL`             | REDCap API endpoint | Yes      | -       | `https://redcap.example.edu/api/` |
| `REDCAP_API_TOKEN_GAP`       | GAP project token   | Yes\*    | -       | `your_gap_api_token`              |
| `REDCAP_API_TOKEN_UC_DEMARC` | UC DEMARC token     | Yes\*    | -       | `your_uc_demarc_token`            |
| `REDCAP_API_TOKEN_CCFA`      | CCFA token          | Yes\*    | -       | `your_ccfa_token`                 |
| `REDCAP_API_TOKEN_NIDDK`     | NIDDK token         | Yes\*    | -       | `your_niddk_token`                |

\*At least one project token required

**Example:**

```bash
REDCAP_API_URL=https://redcap.example.edu/api/
REDCAP_API_TOKEN_GAP=your_gap_api_token_here
REDCAP_API_TOKEN_UC_DEMARC=your_uc_demarc_token_here
REDCAP_API_TOKEN_CCFA=your_ccfa_token_here
REDCAP_API_TOKEN_NIDDK=your_niddk_token_here
```

### Pipeline Settings

| Variable         | Description                     | Required | Default | Range    |
| ---------------- | ------------------------------- | -------- | ------- | -------- |
| `BATCH_SIZE`     | Records per batch               | No       | `50`    | `10-500` |
| `MAX_WORKERS`    | Parallel workers                | No       | `4`     | `1-10`   |
| `RETRY_ATTEMPTS` | Retry failed records            | No       | `3`     | `0-10`   |
| `RETRY_DELAY`    | Delay between retries (seconds) | No       | `5`     | `1-60`   |

**Example:**

```bash
BATCH_SIZE=50
MAX_WORKERS=4
RETRY_ATTEMPTS=3
RETRY_DELAY=5
```

### Field Mapping

| Variable                | Description           | Required | Default   | Example                 |
| ----------------------- | --------------------- | -------- | --------- | ----------------------- |
| `FIELD_MAPPINGS_DIR`    | Mappings directory    | No       | `config/` | `/app/config/mappings/` |
| `DEFAULT_CENTER_ID`     | Default center ID     | No       | `0`       | `1`, `2`, `3`           |
| `FUZZY_MATCH_THRESHOLD` | Fuzzy match threshold | No       | `0.8`     | `0.0-1.0`               |

**Example:**

```bash
FIELD_MAPPINGS_DIR=/app/config/mappings
DEFAULT_CENTER_ID=0
FUZZY_MATCH_THRESHOLD=0.8
```

### Data Validation

| Variable               | Description                   | Required | Default | Valid Values    |
| ---------------------- | ----------------------------- | -------- | ------- | --------------- |
| `VALIDATE_DATES`       | Validate date formats         | No       | `true`  | `true`, `false` |
| `VALIDATE_EMAILS`      | Validate email formats        | No       | `true`  | `true`, `false` |
| `ALLOW_MISSING_FIELDS` | Allow missing optional fields | No       | `true`  | `true`, `false` |
| `STRICT_MODE`          | Strict validation mode        | No       | `false` | `true`, `false` |

**Example:**

```bash
VALIDATE_DATES=true
VALIDATE_EMAILS=true
ALLOW_MISSING_FIELDS=true
STRICT_MODE=false
```

---

## Fragment Validator

Fragment validation service configuration.

### Service Configuration

| Variable               | Description        | Required | Default         | Example                |
| ---------------------- | ------------------ | -------- | --------------- | ---------------------- |
| `VALIDATOR_MODE`       | Validation mode    | No       | `strict`        | `strict`, `permissive` |
| `VALIDATION_RULES_DIR` | Rules directory    | No       | `config/rules/` | `/app/config/rules/`   |
| `MAX_FILE_SIZE_MB`     | Max file size (MB) | No       | `100`           | `10-1000`              |

**Example:**

```bash
VALIDATOR_MODE=strict
VALIDATION_RULES_DIR=/app/config/rules
MAX_FILE_SIZE_MB=100
```

### Validation Settings

| Variable             | Description           | Required | Default | Valid Values    |
| -------------------- | --------------------- | -------- | ------- | --------------- |
| `CHECK_DUPLICATES`   | Check for duplicates  | No       | `true`  | `true`, `false` |
| `CHECK_FOREIGN_KEYS` | Validate foreign keys | No       | `true`  | `true`, `false` |
| `CHECK_DATA_TYPES`   | Validate data types   | No       | `true`  | `true`, `false` |
| `CHECK_CONSTRAINTS`  | Check constraints     | No       | `true`  | `true`, `false` |

**Example:**

```bash
CHECK_DUPLICATES=true
CHECK_FOREIGN_KEYS=true
CHECK_DATA_TYPES=true
CHECK_CONSTRAINTS=true
```

### Error Handling

| Variable              | Description                | Required | Default | Valid Values            |
| --------------------- | -------------------------- | -------- | ------- | ----------------------- |
| `FAIL_ON_ERROR`       | Fail on first error        | No       | `false` | `true`, `false`         |
| `MAX_ERRORS`          | Max errors before stopping | No       | `100`   | `0-10000` (0=unlimited) |
| `ERROR_REPORT_FORMAT` | Error report format        | No       | `json`  | `json`, `csv`, `html`   |

**Example:**

```bash
FAIL_ON_ERROR=false
MAX_ERRORS=100
ERROR_REPORT_FORMAT=json
```

---

## Table Loader

Table loader service configuration.

### Loader Settings

| Variable              | Description            | Required | Default | Range           |
| --------------------- | ---------------------- | -------- | ------- | --------------- |
| `LOAD_BATCH_SIZE`     | Records per batch      | No       | `100`   | `10-1000`       |
| `LOAD_TIMEOUT`        | Load timeout (seconds) | No       | `300`   | `60-3600`       |
| `PARALLEL_LOADS`      | Parallel table loads   | No       | `false` | `true`, `false` |
| `MAX_PARALLEL_TABLES` | Max parallel tables    | No       | `3`     | `1-10`          |

**Example:**

```bash
LOAD_BATCH_SIZE=100
LOAD_TIMEOUT=300
PARALLEL_LOADS=false
MAX_PARALLEL_TABLES=3
```

### Transaction Settings

| Variable                | Description       | Required | Default          | Valid Values                                                            |
| ----------------------- | ----------------- | -------- | ---------------- | ----------------------------------------------------------------------- |
| `USE_TRANSACTIONS`      | Use transactions  | No       | `true`           | `true`, `false`                                                         |
| `TRANSACTION_ISOLATION` | Isolation level   | No       | `READ COMMITTED` | `READ UNCOMMITTED`, `READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE` |
| `ROLLBACK_ON_ERROR`     | Rollback on error | No       | `true`           | `true`, `false`                                                         |

**Example:**

```bash
USE_TRANSACTIONS=true
TRANSACTION_ISOLATION=READ COMMITTED
ROLLBACK_ON_ERROR=true
```

### Conflict Resolution

| Variable                  | Description             | Required | Default  | Valid Values                        |
| ------------------------- | ----------------------- | -------- | -------- | ----------------------------------- |
| `CONFLICT_STRATEGY`       | Conflict resolution     | No       | `upsert` | `upsert`, `skip`, `error`, `update` |
| `UPDATE_IMMUTABLE_FIELDS` | Update immutable fields | No       | `false`  | `true`, `false`                     |
| `PRESERVE_TIMESTAMPS`     | Preserve timestamps     | No       | `true`   | `true`, `false`                     |

**Example:**

```bash
CONFLICT_STRATEGY=upsert
UPDATE_IMMUTABLE_FIELDS=false
PRESERVE_TIMESTAMPS=true
```

---

## LabKey Sync

LabKey synchronization service configuration.

### LabKey Connection

| Variable          | Description       | Required | Default | Example                      |
| ----------------- | ----------------- | -------- | ------- | ---------------------------- |
| `LABKEY_URL`      | LabKey server URL | Yes      | -       | `https://labkey.example.edu` |
| `LABKEY_USERNAME` | LabKey username   | Yes      | -       | `api_user`                   |
| `LABKEY_PASSWORD` | LabKey password   | Yes      | -       | `secure_password`            |
| `LABKEY_PROJECT`  | LabKey project    | Yes      | -       | `IBD_Biobank`                |
| `LABKEY_FOLDER`   | LabKey folder     | No       | `/`     | `/Samples`                   |

**Example:**

```bash
LABKEY_URL=https://labkey.example.edu
LABKEY_USERNAME=api_user
LABKEY_PASSWORD=secure_password_here
LABKEY_PROJECT=IBD_Biobank
LABKEY_FOLDER=/Samples
```

### Sync Settings

| Variable            | Description             | Required | Default | Range           |
| ------------------- | ----------------------- | -------- | ------- | --------------- |
| `SYNC_INTERVAL`     | Sync interval (minutes) | No       | `60`    | `5-1440`        |
| `SYNC_BATCH_SIZE`   | Records per sync        | No       | `100`   | `10-1000`       |
| `SYNC_FULL_REFRESH` | Full refresh mode       | No       | `false` | `true`, `false` |
| `SYNC_INCREMENTAL`  | Incremental sync        | No       | `true`  | `true`, `false` |

**Example:**

```bash
SYNC_INTERVAL=60
SYNC_BATCH_SIZE=100
SYNC_FULL_REFRESH=false
SYNC_INCREMENTAL=true
```

### Schema Mapping

| Variable         | Description         | Required | Default                      | Example          |
| ---------------- | ------------------- | -------- | ---------------------------- | ---------------- |
| `LABKEY_SCHEMA`  | LabKey schema       | No       | `study`                      | `study`, `lists` |
| `LABKEY_QUERY`   | LabKey query        | No       | -                            | `Specimens`      |
| `MAPPING_CONFIG` | Mapping config file | No       | `config/labkey_mapping.json` | Path to config   |

**Example:**

```bash
LABKEY_SCHEMA=study
LABKEY_QUERY=Specimens
MAPPING_CONFIG=/app/config/labkey_mapping.json
```

---

## Nginx

Nginx reverse proxy configuration.

### Server Configuration

| Variable         | Description     | Required | Default     | Example           |
| ---------------- | --------------- | -------- | ----------- | ----------------- |
| `NGINX_HOST`     | Server hostname | No       | `localhost` | `idhub.ibdgc.org` |
| `NGINX_PORT`     | HTTP port       | No       | `80`        | `80`, `8080`      |
| `NGINX_SSL_PORT` | HTTPS port      | No       | `443`       | `443`, `8443`     |

**Example:**

```bash
NGINX_HOST=idhub.ibdgc.org
NGINX_PORT=80
NGINX_SSL_PORT=443
```

### SSL Configuration

| Variable              | Description          | Required | Default            | Example                   |
| --------------------- | -------------------- | -------- | ------------------ | ------------------------- |
| `SSL_CERTIFICATE`     | SSL certificate path | No       | -                  | `/etc/nginx/ssl/cert.pem` |
| `SSL_CERTIFICATE_KEY` | SSL key path         | No       | -                  | `/etc/nginx/ssl/key.pem`  |
| `SSL_PROTOCOLS`       | SSL protocols        | No       | `TLSv1.2 TLSv1.3`  | Space-separated           |
| `SSL_CIPHERS`         | SSL ciphers          | No       | `HIGH:!aNULL:!MD5` | Cipher string             |

**Example:**

```bash
SSL_CERTIFICATE=/etc/nginx/ssl/cert.pem
SSL_CERTIFICATE_KEY=/etc/nginx/ssl/key.pem
SSL_PROTOCOLS=TLSv1.2 TLSv1.3
SSL_CIPHERS=HIGH:!aNULL:!MD5
```

### Proxy Settings

| Variable                | Description               | Required | Default | Range    |
| ----------------------- | ------------------------- | -------- | ------- | -------- |
| `PROXY_CONNECT_TIMEOUT` | Connect timeout (seconds) | No       | `60`    | `10-300` |
| `PROXY_SEND_TIMEOUT`    | Send timeout (seconds)    | No       | `60`    | `10-300` |
| `PROXY_READ_TIMEOUT`    | Read timeout (seconds)    | No       | `60`    | `10-300` |
| `PROXY_BUFFER_SIZE`     | Buffer size               | No       | `4k`    | `4k-64k` |

**Example:**

```bash
PROXY_CONNECT_TIMEOUT=60
PROXY_SEND_TIMEOUT=60
PROXY_READ_TIMEOUT=60
PROXY_BUFFER_SIZE=4k
```

---

## AWS Configuration

AWS service configuration.

### Credentials

| Variable                | Description      | Required | Default   | Example                                    |
| ----------------------- | ---------------- | -------- | --------- | ------------------------------------------ |
| `AWS_ACCESS_KEY_ID`     | AWS access key   | Yes\*    | -         | `AKIAIOSFODNN7EXAMPLE`                     |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key   | Yes\*    | -         | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `AWS_SESSION_TOKEN`     | Session token    | No       | -         | For temporary credentials                  |
| `AWS_PROFILE`           | AWS profile name | No       | `default` | `idhub`, `production`                      |

\*Required unless using IAM roles

**Example:**

```bash
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_PROFILE=idhub
```

### S3 Configuration

| Variable          | Description       | Required | Default     | Example                    |
| ----------------- | ----------------- | -------- | ----------- | -------------------------- |
| `S3_BUCKET`       | S3 bucket name    | Yes      | -           | `idhub-curated-fragments`  |
| `S3_REGION`       | AWS region        | No       | `us-east-1` | `us-east-1`, `us-west-2`   |
| `S3_PREFIX`       | Object key prefix | No       | -           | `validated/`               |
| `S3_ENDPOINT_URL` | Custom endpoint   | No       | -           | For S3-compatible services |

**Example:**

```bash
S3_BUCKET=idhub-curated-fragments
S3_REGION=us-east-1
S3_PREFIX=validated/
```

### S3 Upload Settings

| Variable                    | Description    | Required | Default    | Valid Values                         |
| --------------------------- | -------------- | -------- | ---------- | ------------------------------------ |
| `S3_STORAGE_CLASS`          | Storage class  | No       | `STANDARD` | `STANDARD`, `STANDARD_IA`, `GLACIER` |
| `S3_SERVER_SIDE_ENCRYPTION` | Encryption     | No       | `AES256`   | `AES256`, `aws:kms`                  |
| `S3_ACL`                    | Access control | No       | `private`  | `private`, `public-read`             |

**Example:**

```bash
S3_STORAGE_CLASS=STANDARD_IA
S3_SERVER_SIDE_ENCRYPTION=AES256
S3_ACL=private
```

---

## Monitoring & Logging

Monitoring and logging configuration.

### Logging Configuration

| Variable           | Description       | Required | Default | Valid Values                                    |
| ------------------ | ----------------- | -------- | ------- | ----------------------------------------------- |
| `LOG_LEVEL`        | Log level         | No       | `INFO`  | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `LOG_FORMAT`       | Log format        | No       | `json`  | `json`, `text`                                  |
| `LOG_FILE`         | Log file path     | No       | -       | `/var/log/idhub/app.log`                        |
| `LOG_MAX_SIZE_MB`  | Max log size (MB) | No       | `100`   | `10-1000`                                       |
| `LOG_BACKUP_COUNT` | Log backup count  | No       | `5`     | `1-30`                                          |

**Example:**

```bash
LOG_LEVEL=INFO
LOG_FORMAT=json
LOG_FILE=/var/log/idhub/app.log
LOG_MAX_SIZE_MB=100
LOG_BACKUP_COUNT=5
```

### Metrics & Monitoring

| Variable              | Description         | Required | Default    | Example         |
| --------------------- | ------------------- | -------- | ---------- | --------------- |
| `METRICS_ENABLED`     | Enable metrics      | No       | `true`     | `true`, `false` |
| `METRICS_PORT`        | Metrics port        | No       | `9090`     | `9090`          |
| `PROMETHEUS_ENDPOINT` | Prometheus endpoint | No       | `/metrics` | `/metrics`      |

**Example:**

```bash
METRICS_ENABLED=true
METRICS_PORT=9090
PROMETHEUS_ENDPOINT=/metrics
```

### Alerting

| Variable                | Description     | Required | Default | Example                       |
| ----------------------- | --------------- | -------- | ------- | ----------------------------- |
| `SLACK_WEBHOOK_URL`     | Slack webhook   | No       | -       | `https://hooks.slack.com/...` |
| `ALERT_EMAIL`           | Alert email     | No       | -       | `alerts@example.com`          |
| `ALERT_THRESHOLD_ERROR` | Error threshold | No       | `10`    | `1-100`                       |

**Example:**

```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
ALERT_EMAIL=alerts@example.com
ALERT_THRESHOLD_ERROR=10
```

---

## Environment-Specific Examples

### Development Environment

```bash:.env.development
# Environment
ENVIRONMENT=development
DEBUG=true
LOG_LEVEL=DEBUG

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=idhub_dev
DB_USER=idhub_user
DB_PASSWORD=dev_password
DB_POOL_SIZE=5

# GSID Service
GSID_API_KEY=dev-api-key-min-32-chars-long-here
GSID_SERVICE_URL=http://localhost:8000

# NocoDB
NOCODB_URL=http://localhost:8080
NOCODB_API_TOKEN=dev_nocodb_token
NC_AUTH_JWT_SECRET=dev-jwt-secret-min-32-chars

# REDCap
REDCAP_API_URL=https://redcap-dev.example.edu/api/
REDCAP_API_TOKEN_GAP=dev_gap_token

# AWS
S3_BUCKET=idhub-dev-fragments
AWS_PROFILE=idhub-dev

# Monitoring
METRICS_ENABLED=true
LOG_FORMAT=text
```

### QA Environment

```bash:.env.qa
# Environment
ENVIRONMENT=qa
DEBUG=false
LOG_LEVEL=INFO

# Database
DB_HOST=idhub-db-qa.example.com
DB_PORT=5432
DB_NAME=idhub_qa
DB_USER=idhub_user
DB_PASSWORD=${DB_PASSWORD_QA}
DB_POOL_SIZE=20
DB_SSL_MODE=require

# GSID Service
GSID_API_KEY=${GSID_API_KEY_QA}
GSID_SERVICE_URL=https://api.qa.idhub.ibdgc.org

# NocoDB
NOCODB_URL=https://qa.idhub.ibdgc.org
NOCODB_API_TOKEN=${NOCODB_API_TOKEN_QA}
NC_AUTH_JWT_SECRET=${NC_JWT_SECRET_QA}

# REDCap
REDCAP_API_URL=https://redcap.example.edu/api/
REDCAP_API_TOKEN_GAP=${REDCAP_TOKEN_GAP_QA}

# AWS
S3_BUCKET=idhub-curated-fragments-qa
S3_REGION=us-east-1
S3_STORAGE_CLASS=STANDARD_IA

# Monitoring
METRICS_ENABLED=true
SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_QA}
LOG_FORMAT=json
```

### Production Environment

```bash:.env.production
# Environment
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=WARNING

# Database
DB_HOST=idhub-db-prod.example.com
DB_PORT=5432
DB_NAME=idhub
DB_USER=idhub_user
DB_PASSWORD=${DB_PASSWORD_PROD}
DB_POOL_SIZE=50
DB_MAX_OVERFLOW=20
DB_SSL_MODE=verify-full
DB_SSL_CERT=/etc/ssl/certs/client-cert.pem
DB_SSL_KEY=/etc/ssl/private/client-key.pem
DB_SSL_ROOT_CERT=/etc/ssl/certs/root-cert.pem

# GSID Service
GSID_API_KEY=${GSID_API_KEY_PROD}
GSID_SERVICE_URL=https://api.idhub.ibdgc.org

# NocoDB
NOCODB_URL=https://idhub.ibdgc.org
NOCODB_API_TOKEN=${NOCODB_API_TOKEN_PROD}
NC_AUTH_JWT_SECRET=${NC_JWT_SECRET_PROD}
NC_DISABLE_TELE=true

# REDCap
REDCAP_API_URL=https://redcap.example.edu/api/
REDCAP_API_TOKEN_GAP=${REDCAP_TOKEN_GAP_PROD}
REDCAP_API_TOKEN_UC_DEMARC=${REDCAP_TOKEN_UC_DEMARC_PROD}
REDCAP_API_TOKEN_CCFA=${REDCAP_TOKEN_CCFA_PROD}
REDCAP_API_TOKEN_NIDDK=${REDCAP_TOKEN_NIDDK_PROD}

# AWS
S3_BUCKET=idhub-curated-fragments
S3_REGION=us-east-1
S3_STORAGE_CLASS=STANDARD_IA
S3_SERVER_SIDE_ENCRYPTION=aws:kms

# Nginx
NGINX_HOST=idhub.ibdgc.org
SSL_CERTIFICATE=/etc/nginx/ssl/cert.pem
SSL_CERTIFICATE_KEY=/etc/nginx/ssl/key.pem

# Monitoring
METRICS_ENABLED=true
SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_PROD}
ALERT_EMAIL=alerts@ibdgc.org
LOG_FORMAT=json
LOG_FILE=/var/log/idhub/app.log
```

---

## Security Best Practices

### Secret Management

1. **Never commit secrets to version control**

   ```bash
   # Add to .gitignore
   .env
   .env.*
   *.pem
   *.key
   ```

2. **Use environment-specific secrets**

   ```bash
   # Development
   GSID_API_KEY=dev-key-for-local-testing-only

   # Production (from secrets manager)
   GSID_API_KEY=${GSID_API_KEY_FROM_SECRETS_MANAGER}
   ```

3. **Rotate secrets regularly**

   ```bash
   # Generate new API key
   openssl rand -base64 32

   # Update in all environments
   # Update in secrets manager
   # Restart services
   ```

4. **Use secrets managers**

   ```bash
   # AWS Secrets Manager
   aws secretsmanager get-secret-value \
     --secret-id idhub/production/db-password \
     --query SecretString \
     --output text

   # GitHub Secrets (for CI/CD)
   # Set in repository settings
   ```

### Validation

```bash:scripts/validate_env.sh
#!/bin/bash
# Validate environment variables

set -e

echo "Validating environment variables..."

# Required variables
REQUIRED_VARS=(
    "DB_HOST"
    "DB_NAME"

```
