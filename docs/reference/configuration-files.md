# Configuration Files Reference

## Overview

This document provides a comprehensive reference for all configuration files used across the IDhub platform services.

## Table of Contents

- [Project Configuration](#project-configuration)
- [Field Mappings](#field-mappings)
- [Table Configurations](#table-configurations)
- [Validation Rules](#validation-rules)
- [Docker Configuration](#docker-configuration)
- [Nginx Configuration](#nginx-configuration)
- [Database Schema](#database-schema)
- [GitHub Actions](#github-actions)

---

## Project Configuration

### REDCap Projects Configuration

**File:** `/redcap-pipeline/config/projects.json`

Defines REDCap project configurations for the pipeline.

#### Structure

```json
{
  "projects": {
    "<project_key>": {
      "name": "string",
      "redcap_project_id": "string",
      "api_token": "string",
      "field_mappings": "string",
      "schedule": "string",
      "batch_size": number,
      "enabled": boolean,
      "description": "string",
      "center_id": number,
      "validation_rules": "string"
    }
  }
}
```

#### Fields

| Field               | Type    | Required | Description                 | Example                               |
| ------------------- | ------- | -------- | --------------------------- | ------------------------------------- |
| `name`              | string  | Yes      | Human-readable project name | `"GAP"`                               |
| `redcap_project_id` | string  | Yes      | REDCap project ID           | `"16894"`                             |
| `api_token`         | string  | No\*     | API token (or use env var)  | `"your_token"`                        |
| `field_mappings`    | string  | Yes      | Field mapping config file   | `"gap_field_mappings.json"`           |
| `schedule`          | string  | No       | Sync schedule               | `"continuous"`, `"daily"`, `"weekly"` |
| `batch_size`        | number  | No       | Records per batch           | `50`                                  |
| `enabled`           | boolean | No       | Enable/disable project      | `true`                                |
| `description`       | string  | No       | Project description         | `"Main biobank project"`              |
| `center_id`         | number  | No       | Default center ID           | `1`                                   |
| `validation_rules`  | string  | No       | Validation rules file       | `"gap_validation.json"`               |

\*API token can be provided via environment variable `REDCAP_API_TOKEN_<PROJECT_KEY>`

#### Example

```json:/redcap-pipeline/config/projects.json
{
  "projects": {
    "gap": {
      "name": "GAP",
      "redcap_project_id": "16894",
      "field_mappings": "gap_field_mappings.json",
      "schedule": "continuous",
      "batch_size": 50,
      "enabled": true,
      "description": "Main biobank project",
      "center_id": 1,
      "validation_rules": "gap_validation.json"
    },
    "uc_demarc": {
      "name": "UC DEMARC",
      "redcap_project_id": "12345",
      "field_mappings": "uc_demarc_field_mappings.json",
      "schedule": "daily",
      "batch_size": 100,
      "enabled": true,
      "description": "UC DEMARC study",
      "center_id": 2
    },
    "ccfa": {
      "name": "CCFA",
      "redcap_project_id": "67890",
      "field_mappings": "ccfa_field_mappings.json",
      "schedule": "weekly",
      "batch_size": 25,
      "enabled": false,
      "description": "CCFA registry (currently disabled)",
      "center_id": 3
    }
  }
}
```

#### Usage

```python
import json
from pathlib import Path

# Load projects configuration
config_path = Path("config/projects.json")
with open(config_path) as f:
    config = json.load(f)

# Get specific project
gap_config = config["projects"]["gap"]
print(f"Project: {gap_config['name']}")
print(f"REDCap ID: {gap_config['redcap_project_id']}")

# Get enabled projects only
enabled_projects = {
    key: proj for key, proj in config["projects"].items()
    if proj.get("enabled", True)
}
```

---

## Field Mappings

### Field Mapping Configuration

**Location:** `/redcap-pipeline/config/field_mappings/`

Maps REDCap fields to IDhub database fields.

#### Structure

```json
{
  "field_mapping": {
    "<target_field>": "<source_field>"
  },
  "subject_id_candidates": ["field1", "field2"],
  "center_id_field": "string",
  "default_center_id": number,
  "exclude_from_load": ["field1", "field2"],
  "transformations": {
    "<field>": {
      "type": "string",
      "params": {}
    }
  },
  "validation": {
    "<field>": {
      "required": boolean,
      "type": "string",
      "pattern": "string",
      "min": number,
      "max": number
    }
  }
}
```

#### Fields

| Field                   | Type   | Required | Description                             |
| ----------------------- | ------ | -------- | --------------------------------------- |
| `field_mapping`         | object | Yes      | Source to target field mappings         |
| `subject_id_candidates` | array  | Yes      | Fields to use for subject ID resolution |
| `center_id_field`       | string | No       | Field containing center ID              |
| `default_center_id`     | number | No       | Default center ID if not specified      |
| `exclude_from_load`     | array  | No       | Fields to exclude from database load    |
| `transformations`       | object | No       | Field transformation rules              |
| `validation`            | object | No       | Field validation rules                  |

#### Example: Blood Samples

```json:/redcap-pipeline/config/field_mappings/gap_blood_field_mappings.json
{
  "field_mapping": {
    "sample_id": "blood_sample_id",
    "sample_type": "blood_type",
    "date_collected": "collection_date",
    "time_collected": "collection_time",
    "volume_ml": "blood_volume",
    "collection_site": "site_code",
    "fasting_status": "fasting",
    "anticoagulant": "anticoag_type",
    "processing_notes": "proc_notes",
    "storage_location": "freezer_location",
    "aliquot_count": "num_aliquots"
  },
  "subject_id_candidates": [
    "consortium_id",
    "local_subject_id",
    "mrn"
  ],
  "center_id_field": "center_code",
  "default_center_id": 1,
  "exclude_from_load": [
    "consortium_id",
    "center_code",
    "redcap_event_name",
    "redcap_repeat_instrument",
    "redcap_repeat_instance"
  ],
  "transformations": {
    "date_collected": {
      "type": "date",
      "params": {
        "input_format": "%Y-%m-%d",
        "output_format": "%Y-%m-%d"
      }
    },
    "volume_ml": {
      "type": "numeric",
      "params": {
        "decimal_places": 2
      }
    },
    "fasting_status": {
      "type": "boolean",
      "params": {
        "true_values": ["1", "yes", "true"],
        "false_values": ["0", "no", "false"]
      }
    }
  },
  "validation": {
    "sample_id": {
      "required": true,
      "type": "string",
      "pattern": "^[A-Z0-9]{8,20}$"
    },
    "date_collected": {
      "required": true,
      "type": "date",
      "min": "2000-01-01",
      "max": "today"
    },
    "volume_ml": {
      "required": false,
      "type": "number",
      "min": 0,
      "max": 100
    }
  }
}
```

#### Example: DNA Samples

```json:/redcap-pipeline/config/field_mappings/gap_dna_field_mappings.json
{
  "field_mapping": {
    "sample_id": "dna_sample_id",
    "parent_sample_id": "blood_sample_id",
    "extraction_date": "dna_extraction_date",
    "extraction_method": "extraction_protocol",
    "concentration_ng_ul": "dna_concentration",
    "volume_ul": "dna_volume",
    "a260_280_ratio": "purity_260_280",
    "a260_230_ratio": "purity_260_230",
    "quality_score": "dna_quality",
    "storage_location": "dna_freezer_location",
    "extraction_notes": "dna_notes"
  },
  "subject_id_candidates": [
    "consortium_id",
    "local_subject_id"
  ],
  "center_id_field": null,
  "default_center_id": 1,
  "exclude_from_load": [
    "consortium_id",
    "redcap_event_name"
  ],
  "transformations": {
    "extraction_date": {
      "type": "date",
      "params": {
        "input_format": "%Y-%m-%d"
      }
    },
    "concentration_ng_ul": {
      "type": "numeric",
      "params": {
        "decimal_places": 2
      }
    },
    "a260_280_ratio": {
      "type": "numeric",
      "params": {
        "decimal_places": 3
      }
    }
  },
  "validation": {
    "sample_id": {
      "required": true,
      "type": "string"
    },
    "concentration_ng_ul": {
      "required": false,
      "type": "number",
      "min": 0,
      "max": 10000
    },
    "a260_280_ratio": {
      "required": false,
      "type": "number",
      "min": 1.0,
      "max": 3.0
    }
  }
}
```

#### Example: LCL Samples

```json:/fragment-validator/config/lcl_mapping.json
{
  "field_mapping": {
    "knumber": "knumber",
    "niddk_no": "niddk_no",
    "cell_line_id": "lcl_id",
    "passage_number": "passage",
    "vial_count": "num_vials",
    "freeze_date": "date_frozen",
    "viability_percent": "viability",
    "storage_location": "freezer_location",
    "notes": "lcl_notes"
  },
  "subject_id_candidates": [
    "consortium_id"
  ],
  "center_id_field": null,
  "default_center_id": 1,
  "exclude_from_load": [
    "consortium_id",
    "center_id"
  ],
  "transformations": {
    "freeze_date": {
      "type": "date"
    },
    "viability_percent": {
      "type": "numeric",
      "params": {
        "decimal_places": 1
      }
    }
  },
  "validation": {
    "knumber": {
      "required": true,
      "type": "string",
      "pattern": "^K[0-9]{6}$"
    },
    "niddk_no": {
      "required": true,
      "type": "string"
    },
    "viability_percent": {
      "required": false,
      "type": "number",
      "min": 0,
      "max": 100
    }
  }
}
```

#### Transformation Types

| Type        | Description          | Parameters                      | Example                 |
| ----------- | -------------------- | ------------------------------- | ----------------------- |
| `date`      | Date formatting      | `input_format`, `output_format` | `"2024-01-15"`          |
| `datetime`  | DateTime formatting  | `input_format`, `output_format` | `"2024-01-15 14:30:00"` |
| `numeric`   | Number formatting    | `decimal_places`                | `123.45`                |
| `boolean`   | Boolean conversion   | `true_values`, `false_values`   | `true`                  |
| `uppercase` | Convert to uppercase | -                               | `"ABC"`                 |
| `lowercase` | Convert to lowercase | -                               | `"abc"`                 |
| `trim`      | Trim whitespace      | -                               | `"text"`                |
| `replace`   | Replace text         | `pattern`, `replacement`        | -                       |

---

## Table Configurations

### Table Configuration

**File:** `/fragment-validator/config/table_configs.json`

Defines table-specific settings for validation and loading.

#### Structure

```json
{
  "<table_name>": {
    "natural_key": ["field1", "field2"],
    "immutable_fields": ["field1", "field2"],
    "update_strategy": "string",
    "required_fields": ["field1", "field2"],
    "unique_constraints": [
      ["field1", "field2"]
    ],
    "foreign_keys": {
      "field": {
        "table": "string",
        "column": "string"
      }
    },
    "indexes": [
      {
        "name": "string",
        "columns": ["field1", "field2"],
        "unique": boolean
      }
    ]
  }
}
```

#### Fields

| Field                | Type   | Required | Description                                        |
| -------------------- | ------ | -------- | -------------------------------------------------- |
| `natural_key`        | array  | Yes      | Fields that uniquely identify a record             |
| `immutable_fields`   | array  | No       | Fields that cannot be updated                      |
| `update_strategy`    | string | No       | How to handle conflicts: `upsert`, `skip`, `error` |
| `required_fields`    | array  | No       | Fields that must have values                       |
| `unique_constraints` | array  | No       | Unique constraint definitions                      |
| `foreign_keys`       | object | No       | Foreign key relationships                          |
| `indexes`            | array  | No       | Index definitions                                  |

#### Example

```json:/fragment-validator/config/table_configs.json
{
  "blood": {
    "natural_key": ["global_subject_id", "sample_id"],
    "immutable_fields": ["created_at", "created_by"],
    "update_strategy": "upsert",
    "required_fields": ["sample_id", "global_subject_id", "sample_type"],
    "unique_constraints": [
      ["sample_id"]
    ],
    "foreign_keys": {
      "global_subject_id": {
        "table": "subjects",
        "column": "global_subject_id"
      }
    },
    "indexes": [
      {
        "name": "idx_blood_sample_id",
        "columns": ["sample_id"],
        "unique": true
      },
      {
        "name": "idx_blood_subject_date",
        "columns": ["global_subject_id", "date_collected"],
        "unique": false
      }
    ]
  },
  "dna": {
    "natural_key": ["global_subject_id", "sample_id"],
    "immutable_fields": ["created_at", "created_by"],
    "update_strategy": "upsert",
    "required_fields": ["sample_id", "global_subject_id"],
    "unique_constraints": [
      ["sample_id"]
    ],
    "foreign_keys": {
      "global_subject_id": {
        "table": "subjects",
        "column": "global_subject_id"
      },
      "parent_sample_id": {
        "table": "blood",
        "column": "sample_id"
      }
    }
  },
  "rna": {
    "natural_key": ["global_subject_id", "sample_id"],
    "immutable_fields": ["created_at", "created_by"],
    "update_strategy": "upsert",
    "required_fields": ["sample_id", "global_subject_id"],
    "unique_constraints": [
      ["sample_id"]
    ],
    "foreign_keys": {
      "global_subject_id": {
        "table": "subjects",
        "column": "global_subject_id"
      }
    }
  },
  "lcl": {
    "natural_key": ["global_subject_id", "niddk_no"],
    "immutable_fields": ["created_at"],
    "update_strategy": "upsert",
    "required_fields": ["global_subject_id", "knumber", "niddk_no"],
    "unique_constraints": [
      ["knumber"],
      ["niddk_no"]
    ],
    "foreign_keys": {
      "global_subject_id": {
        "table": "subjects",
        "column": "global_subject_id"
      }
    }
  },
  "specimen": {
    "natural_key": ["sample_id"],
    "immutable_fields": ["created_at"],
    "update_strategy": "upsert",
    "required_fields": ["sample_id", "specimen_type"],
    "unique_constraints": [
      ["sample_id"]
    ]
  },
  "local_subject_ids": {
    "natural_key": ["center_id", "local_subject_id", "identifier_type"],
    "immutable_fields": ["created_at"],
    "update_strategy": "upsert",
    "required_fields": ["center_id", "local_subject_id", "global_subject_id"],
    "unique_constraints": [
      ["center_id", "local_subject_id", "identifier_type"]
    ],
    "foreign_keys": {
      "global_subject_id": {
        "table": "subjects",
        "column": "global_subject_id"
      }
    },
    "indexes": [
      {
        "name": "idx_local_ids_gsid",
        "columns": ["global_subject_id"],
        "unique": false
      },
      {
        "name": "idx_local_ids_lookup",
        "columns": ["center_id", "local_subject_id"],
        "unique": false
      }
    ]
  },
  "subjects": {
    "natural_key": ["global_subject_id"],
    "immutable_fields": ["global_subject_id", "created_at"],
    "update_strategy": "skip",
    "required_fields": ["global_subject_id"],
    "unique_constraints": [
      ["global_subject_id"]
    ],
    "indexes": [
      {
        "name": "idx_subjects_gsid",
        "columns": ["global_subject_id"],
        "unique": true
      }
    ]
  }
}
```

#### Usage

```python
import json
from pathlib import Path

# Load table configuration
config_path = Path("config/table_configs.json")
with open(config_path) as f:
    table_configs = json.load(f)

# Get configuration for specific table
blood_config = table_configs.get("blood", {})
natural_key = blood_config.get("natural_key", ["id"])
immutable_fields = blood_config.get("immutable_fields", [])
update_strategy = blood_config.get("update_strategy", "upsert")

print(f"Natural key: {natural_key}")
print(f"Immutable fields: {immutable_fields}")
print(f"Update strategy: {update_strategy}")
```

---

## Validation Rules

### Validation Rules Configuration

**Location:** `/fragment-validator/config/validation_rules/`

Defines validation rules for data quality checks.

#### Structure

```json
{
  "table": "string",
  "rules": [
    {
      "name": "string",
      "type": "string",
      "field": "string",
      "severity": "string",
      "params": {}
    }
  ],
  "cross_field_rules": [
    {
      "name": "string",
      "type": "string",
      "fields": ["field1", "field2"],
      "severity": "string",
      "params": {}
    }
  ]
}
```

#### Rule Types

| Type          | Description            | Parameters             | Example                      |
| ------------- | ---------------------- | ---------------------- | ---------------------------- |
| `required`    | Field must have value  | -                      | Non-null check               |
| `pattern`     | Match regex pattern    | `pattern`              | Email format                 |
| `range`       | Value within range     | `min`, `max`           | Age 0-120                    |
| `length`      | String length          | `min`, `max`           | Name 1-100 chars             |
| `enum`        | Value in list          | `values`               | Status in [active, inactive] |
| `date_range`  | Date within range      | `min_date`, `max_date` | Collection date              |
| `foreign_key` | Reference exists       | `table`, `column`      | Valid subject ID             |
| `unique`      | Value is unique        | `scope`                | Unique sample ID             |
| `conditional` | Conditional validation | `condition`, `rule`    | If X then Y                  |

#### Severity Levels

- `error`: Validation failure prevents loading
- `warning`: Validation failure logged but allows loading
- `info`: Informational message only

#### Example: Blood Validation

```json:/fragment-validator/config/validation_rules/blood_validation.json
{
  "table": "blood",
  "rules": [
    {
      "name": "sample_id_required",
      "type": "required",
      "field": "sample_id",
      "severity": "error"
    },
    {
      "name": "sample_id_format",
      "type": "pattern",
      "field": "sample_id",
      "severity": "error",
      "params": {
        "pattern": "^[A-Z0-9]{8,20}$",
        "message": "Sample ID must be 8-20 alphanumeric characters"
      }
    },
    {
      "name": "volume_range",
      "type": "range",
      "field": "volume_ml",
      "severity": "warning",
      "params": {
        "min": 0,
        "max": 100,
        "message": "Blood volume should be between 0-100 mL"
      }
    },
    {
      "name": "collection_date_range",
      "type": "date_range",
      "field": "date_collected",
      "severity": "error",
      "params": {
        "min_date": "2000-01-01",
        "max_date": "today",
        "message": "Collection date must be between 2000 and today"
      }
    },
    {
      "name": "sample_type_enum",
      "type": "enum",
      "field": "sample_type",
      "severity": "error",
      "params": {
        "values": ["whole_blood", "plasma", "serum", "buffy_coat"],
        "message": "Invalid sample type"
      }
    },
    {
      "name": "subject_exists",
      "type": "foreign_key",
      "field": "global_subject_id",
      "severity": "error",
      "params": {
        "table": "subjects",
        "column": "global_subject_id",
        "message": "Subject ID does not exist"
      }
    },
    {
      "name": "sample_id_unique",
      "type": "unique",
      "field": "sample_id",
      "severity": "error",
      "params": {
        "message": "Duplicate sample ID"
      }
    }
  ],
  "cross_field_rules": [
    {
      "name": "time_requires_date",
      "type": "conditional",
      "fields": ["time_collected", "date_collected"],
      "severity": "warning",
      "params": {
        "condition": "time_collected IS NOT NULL",
        "rule": "date_collected IS NOT NULL",
        "message": "Collection time requires collection date"
      }
    },
    {
      "name": "fasting_time_check",
      "type": "conditional",
      "fields": ["fasting_status", "fasting_hours"],
      "severity": "warning",
      "params": {
        "condition": "fasting_status = true",
        "rule": "fasting_hours >= 8",
        "message": "Fasting samples should have >= 8 hours fasting time"
      }
    }
  ]
}
```

#### Example: DNA Validation

```json:/fragment-validator/config/validation_rules/dna_validation.json
{
  "table": "dna",
  "rules": [
    {
      "name": "sample_id_required",
      "type": "required",
      "field": "sample_id",
      "severity": "error"
    },
    {
      "name": "concentration_range",
      "type": "range",
      "field": "concentration_ng_ul",
      "severity": "warning",
      "params": {
        "min": 0,
        "max": 10000,
        "message": "DNA concentration should be 0-10000 ng/µL"
      }
    },
    {
      "name": "purity_260_280_range",
      "type": "range",
      "field": "a260_280_ratio",
      "severity": "warning",
      "params": {
        "min": 1.0,
        "max": 3.0,
        "message": "260/280 ratio should be 1.0-3.0"
      }
    },
    {
      "name": "purity_260_230_range",
      "type": "range",
      "field": "a260_230_ratio",
      "severity": "warning",
      "params": {
        "min": 1.0,
        "max": 3.0,
        "message": "260/230 ratio should be 1.0-3.0"
      }
    },
    {
      "name": "parent_sample_exists",
      "type": "foreign_key",
      "field": "parent_sample_id",
      "severity": "warning",
      "params": {
        "table": "blood",
        "column": "sample_id",
        "message": "Parent blood sample not found"
      }
    }
  ],
  "cross_field_rules": [
    {
      "name": "quality_purity_check",
      "type": "conditional",
      "fields": ["quality_score", "a260_280_ratio"],
      "severity": "info",
      "params": {
        "condition": "quality_score = 'high'",
        "rule": "a260_280_ratio BETWEEN 1.7 AND 2.0",
        "message": "High quality DNA typically has 260/280 ratio 1.7-2.0"
      }
    }
  ]
}
```

#### Usage

```python
import json
from pathlib import Path

# Load validation rules
rules_path = Path("config/validation_rules/blood_validation.json")
with open(rules_path) as f:
    validation_config = json.load(f)

# Apply rules
for rule in validation_config["rules"]:
    rule_name = rule["name"]
    rule_type = rule["type"]
    field = rule["field"]
    severity = rule["severity"]

    # Apply validation logic based on rule type
    if rule_type == "required":
        # Check if field has value
        pass
    elif rule_type == "pattern":
        # Check if field matches pattern
        pattern = rule["params"]["pattern"]
        pass
    # ... etc
```

---

## Docker Configuration

### Docker Compose

**File:** `/docker-compose.yml`

Main Docker Compose configuration for all services.

#### Structure

```yaml
version: "3.8"

services:
  <service_name>:
    image: string
    build:
      context: string
      dockerfile: string
      args:
        KEY: value
    container_name: string
    environment:
      KEY: value
    env_file:
      - path
    ports:
      - "host:container"
    volumes:
      - "source:target"
    depends_on:
      - service
    networks:
      - network_name
    restart: policy
    healthcheck:
      test: command
      interval: duration
      timeout: duration
      retries: number

networks:
  <network_name>:
    driver: string

volumes:
  <volume_name>:
    driver: string
```

#### Example

```yaml:docker-compose.yml
version: "3.8"

services:
  # PostgreSQL Database
  idhub_db:
    image: postgres:15-alpine
    container_name: idhub_db
    environment:
      POSTGRES_DB: ${DB_NAME:-idhub}
      POSTGRES_USER: ${DB_USER:-idhub_user}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --locale=en_US.UTF-8"
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./database/init:/docker-entrypoint-initdb.d
    networks:
      - idhub_network
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${DB_USER:-idhub_user}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # GSID Service
  gsid-service:
    build:
      context: ./gsid-service
      dockerfile: Dockerfile
    container_name: gsid-service
    environment:
      DB_HOST: idhub_db
      DB_PORT: 5432
      DB_NAME: ${DB_NAME:-idhub}
      DB_USER: ${DB_USER:-idhub_user}
      DB_PASSWORD: ${DB_PASSWORD}
      GSID_API_KEY: ${GSID_API_KEY}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
    ports:
      - "8000:8000"
    depends_on:
      idhub_db:
        condition: service_healthy
    networks:
      - idhub_network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # NocoDB
  nocodb:
    image: nocodb/nocodb:latest
    container_name: nocodb
    environment:
      NC_DB: "pg://idhub_db:5432?u=${DB_USER}&p=${DB_PASSWORD}&d=${DB_NAME}"
      NC_AUTH_JWT_SECRET: ${NC_AUTH_JWT_SECRET}
      NC_PUBLIC_URL: ${NOCODB_URL:-http://localhost:8080}
      NC_DISABLE_TELE: "true"
    ports:
      - "8080:8080"
    volumes:
      - nocodb_data:/usr/app/data
    depends_on:
      idhub_db:
        condition: service_healthy
    networks:
      - idhub_network
    restart: unless-stopped

  # Nginx Reverse Proxy
  nginx:
    image: nginx:alpine
    container_name: nginx
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./nginx/ssl:/etc/nginx/ssl:ro
      - nginx_logs:/var/log/nginx
    depends_on:
      - gsid-service
      - nocodb
    networks:
      - idhub_network
    restart: unless-stopped

  # REDCap Pipeline (on-demand)
  redcap-pipeline:
    build:
      context: ./redcap-pipeline

```
