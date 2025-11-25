# Configuration Reference

## Overview

This guide provides a comprehensive reference for all configuration options across the IDhub platform, including environment variables, configuration files, and service-specific settings.

## Table of Contents

- [Environment Variables](#environment-variables)
- [Service Configuration](#service-configuration)
- [Database Configuration](#database-configuration)
- [API Configuration](#api-configuration)
- [Security Configuration](#security-configuration)
- [Logging Configuration](#logging-configuration)
- [Integration Configuration](#integration-configuration)
- [Performance Tuning](#performance-tuning)

---

## Environment Variables

### Global Settings

| Variable      | Description            | Required | Default       | Example                             |
| ------------- | ---------------------- | -------- | ------------- | ----------------------------------- |
| `ENVIRONMENT` | Deployment environment | No       | `development` | `production`, `qa`, `development`   |
| `DEBUG`       | Enable debug mode      | No       | `false`       | `true`, `false`                     |
| `LOG_LEVEL`   | Logging level          | No       | `INFO`        | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `TZ`          | Timezone               | No       | `UTC`         | `America/New_York`, `UTC`           |

### Database Configuration

| Variable          | Description              | Required | Default      | Example                               |
| ----------------- | ------------------------ | -------- | ------------ | ------------------------------------- |
| `DB_HOST`         | PostgreSQL host          | Yes      | `localhost`  | `idhub_db`, `postgres.example.com`    |
| `DB_PORT`         | PostgreSQL port          | No       | `5432`       | `5432`                                |
| `DB_NAME`         | Database name            | Yes      | `idhub`      | `idhub`, `idhub_qa`                   |
| `DB_USER`         | Database user            | Yes      | `idhub_user` | `idhub_user`                          |
| `DB_PASSWORD`     | Database password        | Yes      | -            | `secure_password_here`                |
| `DB_POOL_SIZE`    | Connection pool size     | No       | `20`         | `10`, `50`                            |
| `DB_MAX_OVERFLOW` | Max overflow connections | No       | `10`         | `5`, `20`                             |
| `DB_POOL_TIMEOUT` | Pool timeout (seconds)   | No       | `30`         | `30`, `60`                            |
| `DB_POOL_RECYCLE` | Connection recycle time  | No       | `3600`       | `1800`, `3600`                        |
| `DATABASE_URL`    | Full database URL        | No       | -            | `postgresql://user:pass@host:5432/db` |

**Example Database URL**:

```bash
DATABASE_URL=postgresql://idhub_user:password@localhost:5432/idhub
```

### GSID Service Configuration

| Variable                | Description            | Required | Default     | Example                       |
| ----------------------- | ---------------------- | -------- | ----------- | ----------------------------- |
| `GSID_SERVICE_URL`      | GSID service URL       | Yes      | -           | `https://api.idhub.ibdgc.org` |
| `GSID_API_KEY`          | API authentication key | Yes      | -           | `gsid_live_abc123...`         |
| `SECRET_KEY`            | Application secret key | Yes      | -           | `your-secret-key-here`        |
| `GSID_PREFIX`           | GSID prefix            | No       | `01HQ`      | `01HQ`, `01HP`                |
| `GSID_LENGTH`           | GSID length            | No       | `11`        | `11`, `13`                    |
| `API_KEY_HEADER`        | API key header name    | No       | `X-API-Key` | `X-API-Key`, `Authorization`  |
| `RATE_LIMIT_ENABLED`    | Enable rate limiting   | No       | `true`      | `true`, `false`               |
| `RATE_LIMIT_PER_MINUTE` | Requests per minute    | No       | `100`       | `60`, `200`                   |

### REDCap Configuration

| Variable                     | Description            | Required | Default | Example                           |
| ---------------------------- | ---------------------- | -------- | ------- | --------------------------------- |
| `REDCAP_API_URL`             | REDCap API endpoint    | Yes      | -       | `https://redcap.example.edu/api/` |
| `REDCAP_API_TOKEN_<PROJECT>` | Project-specific token | Yes      | -       | `ABCD1234567890...`               |
| `REDCAP_BATCH_SIZE`          | Records per batch      | No       | `50`    | `10`, `100`                       |
| `REDCAP_TIMEOUT`             | API timeout (seconds)  | No       | `30`    | `30`, `60`                        |
| `REDCAP_RETRY_ATTEMPTS`      | Retry attempts         | No       | `3`     | `3`, `5`                          |
| `REDCAP_RETRY_DELAY`         | Retry delay (seconds)  | No       | `5`     | `5`, `10`                         |

**Project-Specific Tokens**:

```bash
REDCAP_API_TOKEN_GAP=your_gap_token_here
REDCAP_API_TOKEN_UC_DEMARC=your_uc_demarc_token_here
REDCAP_API_TOKEN_PROTECT=your_protect_token_here
```

### NocoDB Configuration

| Variable             | Description         | Required | Default | Example                             |
| -------------------- | ------------------- | -------- | ------- | ----------------------------------- |
| `NOCODB_URL`         | NocoDB base URL     | Yes      | -       | `https://idhub.ibdgc.org`           |
| `NOCODB_API_TOKEN`   | NocoDB API token    | Yes      | -       | `your-nocodb-token`                 |
| `NC_DB`              | Database connection | Yes      | -       | `pg://host:5432?u=user&p=pass&d=db` |
| `NC_AUTH_JWT_SECRET` | JWT secret          | Yes      | -       | `your-jwt-secret`                   |
| `NC_PUBLIC_URL`      | Public URL          | Yes      | -       | `https://idhub.ibdgc.org`           |
| `NC_DISABLE_TELE`    | Disable telemetry   | No       | `true`  | `true`, `false`                     |
| `NC_ADMIN_EMAIL`     | Admin email         | No       | -       | `admin@ibdgc.org`                   |
| `NC_ADMIN_PASSWORD`  | Admin password      | No       | -       | `secure-password`                   |

**NocoDB Database Connection Format**:

```bash
NC_DB=pg://postgres:5432?u=idhub_user&p=password&d=nocodb
```

### AWS/S3 Configuration

| Variable                | Description      | Required | Default      | Example                              |
| ----------------------- | ---------------- | -------- | ------------ | ------------------------------------ |
| `AWS_ACCESS_KEY_ID`     | AWS access key   | Yes      | -            | `AKIAIOSFODNN7EXAMPLE`               |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key   | Yes      | -            | `wJalrXUtnFEMI/K7MDENG/...`          |
| `AWS_REGION`            | AWS region       | No       | `us-east-1`  | `us-east-1`, `us-west-2`             |
| `AWS_ENDPOINT_URL`      | Custom endpoint  | No       | -            | `http://localhost:4566` (LocalStack) |
| `S3_BUCKET`             | S3 bucket name   | Yes      | -            | `idhub-curated-fragments`            |
| `S3_PREFIX`             | S3 key prefix    | No       | `fragments/` | `fragments/`, `data/`                |
| `S3_REGION`             | S3 bucket region | No       | `us-east-1`  | `us-east-1`                          |

### Redis Configuration

| Variable                | Description           | Required | Default     | Example                         |
| ----------------------- | --------------------- | -------- | ----------- | ------------------------------- |
| `REDIS_HOST`            | Redis host            | No       | `localhost` | `redis`, `redis.example.com`    |
| `REDIS_PORT`            | Redis port            | No       | `6379`      | `6379`                          |
| `REDIS_PASSWORD`        | Redis password        | No       | -           | `redis-password`                |
| `REDIS_DB`              | Redis database number | No       | `0`         | `0`, `1`                        |
| `REDIS_URL`             | Full Redis URL        | No       | -           | `redis://:password@host:6379/0` |
| `REDIS_MAX_CONNECTIONS` | Max connections       | No       | `50`        | `50`, `100`                     |
| `REDIS_SOCKET_TIMEOUT`  | Socket timeout        | No       | `5`         | `5`, `10`                       |

### Monitoring & Observability

| Variable                    | Description        | Required | Default      | Example                     |
| --------------------------- | ------------------ | -------- | ------------ | --------------------------- |
| `SENTRY_DSN`                | Sentry DSN         | No       | -            | `https://...@sentry.io/...` |
| `SENTRY_ENVIRONMENT`        | Sentry environment | No       | `production` | `production`, `qa`          |
| `SENTRY_TRACES_SAMPLE_RATE` | Trace sample rate  | No       | `0.1`        | `0.1`, `1.0`                |
| `PROMETHEUS_ENABLED`        | Enable Prometheus  | No       | `false`      | `true`, `false`             |
| `PROMETHEUS_PORT`           | Prometheus port    | No       | `9090`       | `9090`                      |

---

## Service Configuration

### GSID Service

#### Application Configuration

```yaml:gsid-service/config/production.yml
# Production configuration for GSID Service

server:
  host: 0.0.0.0
  port: 8000
  workers: 4
  reload: false
  access_log: true
  proxy_headers: true
  forwarded_allow_ips: "*"

database:
  pool_size: 20
  max_overflow: 10
  pool_timeout: 30
  pool_recycle: 3600
  echo: false
  pool_pre_ping: true

redis:
  max_connections: 50
  socket_timeout: 5
  socket_connect_timeout: 5
  decode_responses: true
  retry_on_timeout: true

security:
  api_key_header: X-API-Key
  api_key_prefix: gsid_live_
  min_api_key_length: 32

  rate_limit:
    enabled: true
    requests_per_minute: 100
    burst_size: 20

  cors:
    enabled: true
    origins:
      - https://idhub.ibdgc.org
      - https://api.idhub.ibdgc.org
    allow_credentials: true
    allow_methods:
      - GET
      - POST
      - PUT
      - DELETE
    allow_headers:
      - "*"

logging:
  level: INFO
  format: json
  handlers:
    - console
    - file
  file:
    path: /app/logs/gsid-service.log
    max_bytes: 10485760  # 10MB
    backup_count: 10
    rotation: daily

gsid:
  prefix: "01HQ"
  length: 11
  alphabet: "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

cache:
  enabled: true
  ttl: 3600  # 1 hour
  max_size: 10000
```

#### Development Configuration

```yaml:gsid-service/config/development.yml
# Development configuration

server:
  host: 0.0.0.0
  port: 8000
  workers: 1
  reload: true
  access_log: true

database:
  pool_size: 5
  max_overflow: 5
  echo: true  # Log SQL queries

security:
  api_key_prefix: gsid_test_
  rate_limit:
    enabled: false
  cors:
    enabled: true
    origins:
      - http://localhost:3000
      - http://localhost:8080

logging:
  level: DEBUG
  format: text
  handlers:
    - console

cache:
  enabled: false
```

### REDCap Pipeline

#### Projects Configuration

```json:redcap-pipeline/config/projects.json
{
  "projects": {
    "gap": {
      "name": "GAP",
      "redcap_project_id": "16894",
      "api_token_env": "REDCAP_API_TOKEN_GAP",
      "field_mappings": "gap_field_mappings.json",
      "schedule": "continuous",
      "batch_size": 50,
      "enabled": true,
      "description": "Main biobank project",
      "validation": {
        "required_fields": ["consortium_id", "sample_id"],
        "unique_fields": ["sample_id"],
        "date_fields": ["date_collected", "date_received"]
      },
      "retry": {
        "max_attempts": 3,
        "delay_seconds": 5,
        "backoff_multiplier": 2
      }
    },
    "uc_demarc": {
      "name": "UC-DEMARC",
      "redcap_project_id": "17234",
      "api_token_env": "REDCAP_API_TOKEN_UC_DEMARC",
      "field_mappings": "uc_demarc_field_mappings.json",
      "schedule": "daily",
      "batch_size": 100,
      "enabled": true,
      "description": "UC-DEMARC study",
      "validation": {
        "required_fields": ["subject_id", "sample_id"],
        "unique_fields": ["sample_id"]
      }
    },
    "protect": {
      "name": "PROTECT",
      "redcap_project_id": "18456",
      "api_token_env": "REDCAP_API_TOKEN_PROTECT",
      "field_mappings": "protect_field_mappings.json",
      "schedule": "weekly",
      "batch_size": 25,
      "enabled": false,
      "description": "PROTECT study (currently disabled)"
    }
  },
  "global_settings": {
    "default_batch_size": 50,
    "max_batch_size": 200,
    "api_timeout": 30,
    "max_retries": 3,
    "log_retention_days": 90
  }
}
```

#### Field Mappings

```json:redcap-pipeline/config/gap_field_mappings.json
{
  "subject_fields": {
    "consortium_id": {
      "redcap_field": "consortium_id",
      "target_field": "local_subject_id",
      "required": true,
      "validation": "^[A-Z0-9]{6,12}$"
    },
    "center_id": {
      "redcap_field": "center_id",
      "target_field": "center_id",
      "required": true,
      "type": "integer"
    }
  },

  "sample_fields": {
    "sample_id": {
      "redcap_field": "sample_id",
      "target_field": "sample_id",
      "required": true,
      "unique": true
    },
    "sample_type": {
      "redcap_field": "sample_type",
      "target_field": "sample_type",
      "required": true,
      "allowed_values": ["whole_blood", "plasma", "serum", "dna", "rna"]
    },
    "date_collected": {
      "redcap_field": "collection_date",
      "target_field": "date_collected",
      "type": "date",
      "format": "%Y-%m-%d"
    },
    "volume_ml": {
      "redcap_field": "volume",
      "target_field": "volume_ml",
      "type": "float",
      "min": 0,
      "max": 1000
    }
  },

  "metadata_fields": {
    "notes": {
      "redcap_field": "comments",
      "target_field": "notes",
      "type": "text"
    },
    "quality_flag": {
      "redcap_field": "qc_flag",
      "target_field": "quality_flag",
      "type": "boolean"
    }
  },

  "transformations": {
    "sample_type": {
      "type": "mapping",
      "map": {
        "1": "whole_blood",
        "2": "plasma",
        "3": "serum",
        "4": "dna",
        "5": "rna"
      }
    },
    "quality_flag": {
      "type": "boolean",
      "true_values": ["1", "yes", "true"],
      "false_values": ["0", "no", "false"]
    }
  }
}
```

### Fragment Validator

#### Table Configurations

```json:fragment-validator/config/table_configs.json
{
  "blood": {
    "natural_key": ["global_subject_id", "sample_id"],
    "immutable_fields": ["created_at", "created_by"],
    "update_strategy": "upsert",
    "validation": {
      "required_fields": ["global_subject_id", "sample_id", "sample_type"],
      "unique_fields": ["sample_id"],
      "date_fields": ["date_collected", "date_received"],
      "numeric_fields": ["volume_ml", "aliquot_count"]
    },
    "allowed_values": {
      "sample_type": ["whole_blood", "plasma", "serum", "buffy_coat"],
      "storage_location": ["freezer_a", "freezer_b", "freezer_c"]
    }
  },

  "dna": {
    "natural_key": ["global_subject_id", "sample_id"],
    "immutable_fields": ["created_at", "created_by"],
    "update_strategy": "upsert",
    "validation": {
      "required_fields": ["global_subject_id", "sample_id", "extraction_method"],
      "numeric_fields": ["concentration_ng_ul", "volume_ul", "a260_280"]
    },
    "allowed_values": {
      "extraction_method": ["qiagen", "phenol_chloroform", "salting_out"],
      "quality_grade": ["A", "B", "C", "F"]
    }
  },

  "rna": {
    "natural_key": ["global_subject_id", "sample_id"],
    "immutable_fields": ["created_at", "created_by"],
    "update_strategy": "upsert",
    "validation": {
      "required_fields": ["global_subject_id", "sample_id", "extraction_method"],
      "numeric_fields": ["concentration_ng_ul", "rin_score"]
    },
    "constraints": {
      "rin_score": {
        "min": 0,
        "max": 10
      }
    }
  },

  "lcl": {
    "natural_key": ["global_subject_id", "niddk_no"],
    "immutable_fields": ["created_at"],
    "update_strategy": "upsert",
    "validation": {
      "required_fields": ["global_subject_id", "niddk_no"],
      "unique_fields": ["niddk_no"]
    }
  },

  "specimen": {
    "natural_key": ["sample_id"],
    "immutable_fields": ["created_at"],
    "update_strategy": "upsert",
    "validation": {
      "required_fields": ["sample_id", "specimen_type"],
      "unique_fields": ["sample_id"]
    }
  },

  "subjects": {
    "natural_key": ["global_subject_id"],
    "immutable_fields": ["created_at", "global_subject_id"],
    "update_strategy": "update_only",
    "validation": {
      "required_fields": ["global_subject_id"]
    }
  },

  "local_subject_ids": {
    "natural_key": ["center_id", "local_subject_id", "identifier_type"],
    "immutable_fields": ["created_at"],
    "update_strategy": "upsert",
    "validation": {
      "required_fields": ["center_id", "local_subject_id", "identifier_type", "global_subject_id"]
    }
  }
}
```

#### Mapping Configurations

```json:fragment-validator/config/blood_mapping.json
{
  "field_mapping": {
    "sample_id": "sample_id",
    "sample_type": "sample_type",
    "date_collected": "date_collected",
    "date_received": "date_received",
    "volume_ml": "volume_ml",
    "aliquot_count": "aliquot_count",
    "storage_location": "storage_location",
    "notes": "notes"
  },

  "subject_id_candidates": [
    "consortium_id",
    "global_subject_id",
    "subject_id"
  ],

  "center_id_field": "center_id",
  "default_center_id": 0,

  "exclude_from_load": [
    "consortium_id",
    "center_id",
    "temp_field"
  ],

  "transformations": {
    "date_collected": {
      "type": "date",
      "input_format": "%m/%d/%Y",
      "output_format": "%Y-%m-%d"
    },
    "volume_ml": {
      "type": "numeric",
      "round": 2
    }
  }
}
```

### Table Loader

#### Loader Configuration

```yaml:table-loader/config/loader.yml
# Table loader configuration

batch_processing:
  default_batch_size: 100
  max_batch_size: 1000
  commit_interval: 50

error_handling:
  max_errors_per_batch: 10
  continue_on_error: true
  error_log_path: logs/load_errors.log

validation:
  strict_mode: true
  validate_foreign_keys: true
  validate_constraints: true

performance:
  use_bulk_insert: true
  disable_triggers: false
  parallel_loads: false

logging:
  level: INFO
  log_sql: false
  log_validation: true

retry:
  max_attempts: 3
  delay_seconds: 5
  exponential_backoff: true
```

---

## Database Configuration

### PostgreSQL Settings

```ini:postgresql.conf
# Connection Settings
listen_addresses = '*'
port = 5432
max_connections = 100
superuser_reserved_connections = 3

# Memory Settings
shared_buffers = 256MB
effective_cache_size = 1GB
maintenance_work_mem = 64MB
work_mem = 16MB

# Write Ahead Log
wal_level = replica
max_wal_size = 1GB
min_wal_size = 80MB
checkpoint_completion_target = 0.9

# Query Planning
random_page_cost = 1.1
effective_io_concurrency = 200

# Logging
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_rotation_age = 1d
log_rotation_size = 100MB
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_min_duration_statement = 1000
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on

# Performance
shared_preload_libraries = 'pg_stat_statements'
track_activity_query_size = 2048
pg_stat_statements.track = all
```

### Connection Pooling (PgBouncer)

```ini:pgbouncer.ini
[databases]
idhub = host=localhost port=5432 dbname=idhub

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt
admin_users = postgres
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
min_pool_size = 10
reserve_pool_size = 5
reserve_pool_timeout = 5
max_db_connections = 100
max_user_connections = 100
server_idle_timeout = 600
server_lifetime = 3600
server_connect_timeout = 15
query_timeout = 0
query_wait_timeout = 120
client_idle_timeout = 0
client_login_timeout = 60
log_connections = 1
log_disconnections = 1
log_pooler_errors = 1
```

---

## API Configuration

### Rate Limiting

```python:core/rate_limit.py
"""Rate limiting configuration"""

from typing import Dict

RATE_LIMITS: Dict[str, Dict[str, int]] = {
    "default": {
        "requests_per_minute": 100,
        "requests_per_hour": 5000,
        "requests_per_day": 100000,
        "burst_size": 20,
    },

    "authenticated": {
        "requests_per_minute": 200,
        "requests_per_hour": 10000,
        "requests_per_day": 200000,
        "burst_size": 50,
    },

    "admin": {
        "requests_per_minute": 1000,
        "requests_per_hour": 50000,
        "requests_per_day": 1000000,
        "burst_size": 100,
    },

    "batch_operations": {
        "requests_per_minute": 10,
        "requests_per_hour": 100,
        "requests_per_day": 1000,
        "burst_size": 5,
    },
}

# Endpoint-specific limits
ENDPOINT_LIMITS: Dict[str, str] = {
    "/subjects": "authenticated",
    "/subjects/batch": "batch_operations",
    "/health": "default",
    "/admin/*": "admin",
}
```

### CORS Configuration

```python:core/cors.py
"""CORS configuration"""

from typing import List

# Production CORS settings
CORS_ORIGINS: List[str] = [
    "https://idhub.ibdgc.org",
    "https://api.idhub.ibdgc.org",
    "https://qa.idhub.ibdgc.org",
    "https://api.qa.idhub.ibdgc.org",
]

CORS_ALLOW_CREDENTIALS = True

CORS_ALLOW_METHODS = [
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "OPTIONS",
]

CORS_ALLOW_HEADERS = [
    "Accept",
    "Accept-Language",
    "Content-Type",
    "Authorization",
    "X-API-Key",
    "X-Request-ID",
]

CORS_EXPOSE_HEADERS = [
    "X-Request-ID",
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "X-RateLimit-Reset",
]

CORS_MAX_AGE = 600  # 10 minutes
```

---

## Security Configuration

### API Key Management

```python:core/security.py
"""Security configuration"""

import secrets
from typing import Dict

# API Key settings
API_KEY_PREFIX = "gsid_live_"
API_KEY_TEST_PREFIX = "gsid_test_"
API_KEY_LENGTH = 64  # bytes (128 hex characters)
API_KEY_HEADER = "X-API-Key"

# Password requirements
PASSWORD_MIN_LENGTH = 12
PASSWORD_REQUIRE_UPPERCASE = True
PASSWORD_REQUIRE_LOWERCASE = True
PASSWORD_REQUIRE_DIGITS = True
PASSWORD_REQUIRE_SPECIAL = True

# Session settings
SESSION_TIMEOUT = 3600  # 1 hour
SESSION_ABSOLUTE_TIMEOUT = 28800  # 8 hours
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

# JWT settings
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60
JWT_REFRESH_TOKEN_EXPIRE_DAYS = 7

def generate_api_key(prefix: str = API_KEY_PREFIX) -> str:
    """Generate secure API key"""
    random_part = secrets.token_hex(API_KEY_LENGTH)
    return f"{prefix}{random_part}"
```

### SSL/TLS Configuration

```nginx:nginx/snippets/ssl-params.conf
# SSL/TLS Configuration

# SSL Protocols
ssl_protocols TLSv1.2 TLSv1.3;
ssl_prefer_server_ciphers on;

# SSL Ciphers
ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384';

# SSL Session
ssl_session_timeout 1d;
ssl_session_cache shared:SSL:50m;
ssl_session_tickets off;

# OCSP Stapling
ssl_stapling on;
ssl_stapling_verify on;
resolver 8.8.8.8 8.8.4.4 valid=300s;
resolver_timeout 5s;

# Security Headers
add_header Strict-Transport-Security "max-age=63072000; includeSubDomains; preload" always;
add_header X-Frame-Options "SAMEORIGIN" always;
add_header X-Content-Type-Options "nosniff" always;
add_header X-XSS-Protection "1; mode=block" always;
add_header Referrer-Policy "no-referrer-when-downgrade" always;
```

---

## Logging Configuration

### Structured Logging

```python:core/logging_config.py
"""Logging configuration"""

import logging
import sys
from pathlib import Path
from typing import Dict, Any
import json

class JSONFormatter(logging.Formatter):
    """JSON log formatter"""

    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id

        if hasattr(record, "user_id"):
            log_data["user_id"] = record.user_id

        return json.dumps(log_data)


def setup_logging(
    service_name: str,
    log_level: str = "INFO",
    log_format: str = "json",
    log_file: str = None,
):
    """Setup logging configuration"""

    # Create logs directory
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))

    # Remove existing handlers
    root_logger.handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level))

    if log_format == "json":
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(
            logging.

```
