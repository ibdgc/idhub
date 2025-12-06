# System Architecture

## Overview

The IBDGC Integrated Data Hub (IDhub) is a microservices-based data integration platform designed to centralize biobank and clinical data from multiple sources while maintaining data quality, provenance, and subject identity consistency.

## Architecture Principles

### 1. **Separation of Concerns**

Each service has a single, well-defined responsibility:

-   **GSID Service**: Subject identity management
-   **REDCap Pipeline**: Data extraction and transformation
-   **Fragment Validator**: Data quality validation
-   **Table Loader**: Database persistence
-   **Nginx**: Routing and SSL termination

### 2. **Staged Data Pipeline**

Data flows through distinct stages with validation gates:

```
Source → Extract → Stage → Validate → Queue → Load → Database
```

Each stage can fail independently without affecting others, enabling retry logic and error recovery.

### 3. **Immutable Staging**

Data fragments in S3 are immutable once created, providing:

-   Complete audit trail
-   Ability to replay pipelines
-   Source of truth for debugging
-   Disaster recovery capability

### 4. **Natural Key Strategy**

Records are identified by business keys (natural keys) rather than database IDs, enabling:

-   Idempotent operations
-   Cross-system reconciliation
-   Intelligent upserts
-   Data deduplication

## High-Level Architecture

```mermaid
graph TB
    subgraph "External Systems"
        RC[REDCap Projects]
        LK[LabKey]
        MU[Manual Uploads]
    end

    subgraph "Ingestion Services"
        RCP[REDCap Pipeline<br/>Python Service]
        FV[Fragment Validator<br/>Python Service]
    end

    subgraph "Storage Layer"
        S3[(S3 Bucket<br/>Curated Fragments)]
        VQ[(PostgreSQL<br/>Validation Queue)]
    end

    subgraph "Loading Services"
        TL[Table Loader<br/>Python Service]
        GS[GSID Service<br/>FastAPI]
    end

    subgraph "Data Layer"
        DB[(PostgreSQL<br/>Main Database)]
    end

    subgraph "Access Layer"
        NX[Nginx<br/>Reverse Proxy]
        NC[NocoDB<br/>Web UI]
        API[REST API]
    end

    subgraph "Orchestration"
        GHA[GitHub Actions<br/>Workflows]
    end

    RC -->|API| RCP
    LK -->|Export| FV
    MU -->|Upload| FV

    RCP -->|Upload| S3
    FV -->|Upload| S3

    S3 -->|Read| FV
    FV -->|Insert| VQ

    VQ -->|Read| TL
    TL -->|Upsert| DB

    GS <-->|Query/Create| DB
    FV -->|Resolve GSID| GS
    TL -->|Resolve GSID| GS

    DB -->|Query| NC
    DB -->|Query| API

    NX -->|Proxy| NC
    NX -->|Proxy| GS
    NX -->|Proxy| API

    GHA -.->|Trigger| RCP
    GHA -.->|Trigger| FV
    GHA -.->|Trigger| TL

    style S3 fill:#FF9800
    style VQ fill:#2196F3
    style DB fill:#4CAF50
    style GS fill:#9C27B0
```

## Component Architecture

### GSID Service

**Purpose**: Centralized global subject ID management

**Technology**: FastAPI (Python), PostgreSQL

**Key Features**:

-   GSID generation (Custom format)
-   Local ID to GSID resolution
-   Fuzzy matching for subject identification
-   RESTful API with authentication

```mermaid
graph LR
    A[Client Request] --> B[FastAPI Router]
    B --> C{Endpoint}
    C -->|/generate| D[Generate GSID]
    C -->|/resolve| E[Resolve Local ID]
    C -->|/batch| F[Batch Operations]

    D --> G[Database]
    E --> G
    F --> G

    G --> H[Return Response]
```

**Database Tables**:

-   `subjects`: Core subject records with GSID
-   `local_subject_ids`: Mapping of local IDs to GSIDs

[Detailed documentation →](../services/gsid-service.md)

### REDCap Pipeline

**Purpose**: Extract and transform data from REDCap projects

**Technology**: Python, REDCap API, S3

**Key Features**:

-   Multi-project support
-   Incremental extraction
-   Field mapping and transformation
-   Fragment generation

```mermaid
graph TB
    A[REDCap API] --> B[Extract Records]
    B --> C[Apply Field Mappings]
    C --> D[Transform Data]
    D --> E[Generate Fragments]
    E --> F[Upload to S3]
    F --> G[Update Metadata]
```

**Configuration**:

-   `config/projects.json`: Project definitions
-   `config/*_field_mappings.json`: Field mapping rules

[Detailed documentation →](../services/redcap-pipeline.md)

### Fragment Validator

**Purpose**: Validate data quality before database loading

**Technology**: Python, S3, PostgreSQL

**Key Features**:

-   Schema validation
-   GSID resolution
-   Business rule validation
-   Duplicate detection

```mermaid
graph TB
    A[S3 Fragment] --> B[Load Fragment]
    B --> C[Schema Validation]
    C --> D{Valid?}
    D -->|No| E[Reject]
    D -->|Yes| F[Resolve GSID]
    F --> G{GSID Found?}
    G -->|No| H[Create Subject]
    G -->|Yes| I[Continue]
    H --> I
    I --> J[Business Rules]
    J --> K{Valid?}
    K -->|No| E
    K -->|Yes| L[Queue for Loading]

    E --> M[Log Error]
    L --> N[Validation Queue]
```

**Validation Steps**:

1.  **Schema Validation**: Field types, required fields
2.  **GSID Resolution**: Map local IDs to GSIDs
3.  **Business Rules**: Domain-specific validation
4.  **Duplicate Detection**: Check for existing records

[Detailed documentation →](../services/fragment-validator.md)

### Table Loader

**Purpose**: Load validated data into database with update strategy

**Technology**: Python, PostgreSQL

**Key Features**:

-   Natural key-based upserts
-   Immutable field protection
-   Batch processing
-   Transaction management

```mermaid
graph TB
    A[Validation Queue] --> B[Read Batch]
    B --> C[Group by Table]
    C --> D[For Each Record]
    D --> E{Natural Key Exists?}
    E -->|No| F[INSERT]
    E -->|Yes| G{Immutable Changed?}
    G -->|Yes| H[Reject]
    G -->|No| I{Data Changed?}
    I -->|No| J[Skip]
    I -->|Yes| K[UPDATE]

    F --> L[Commit]
    K --> L
    H --> M[Log Error]
    J --> N[Log Skip]
    L --> O[Mark as Loaded]
```

**Configuration**:

-   `config/table_configs.json`: Natural keys, immutable fields

[Detailed documentation →](../services/table-loader.md)

### Nginx Proxy

**Purpose**: Reverse proxy, SSL termination, routing

**Technology**: Nginx

**Key Features**:

-   SSL/TLS termination
-   Request routing
-   Rate limiting
-   Static file serving

```mermaid
graph LR
    A[Client] -->|HTTPS| B[Nginx]
    B -->|/| C[NocoDB]
    B -->|/api/gsid| D[GSID Service]
    B -->|/api/data| E[Data API]

    style B fill:#4CAF50
```

[Detailed documentation →](../services/nginx.md)

## Data Flow Architecture

### End-to-End Data Flow

```mermaid
sequenceDiagram
    participant SRC as Data Source
    participant EXT as Extractor
    participant S3 as S3 Staging
    participant VAL as Validator
    participant GSID as GSID Service
    participant QUEUE as Validation Queue
    participant LOAD as Loader
    participant DB as Database

    SRC->>EXT: 1. Extract data
    EXT->>EXT: 2. Transform & map
    EXT->>S3: 3. Upload fragment

    Note over S3: Fragment stored immutably

    S3->>VAL: 4. Process fragment
    VAL->>VAL: 5. Schema validation

    VAL->>GSID: 6. Resolve GSID
    GSID->>GSID: 7. Lookup/create
    GSID-->>VAL: 8. Return GSID

    VAL->>VAL: 9. Business rules
    VAL->>QUEUE: 10. Queue validated data

    Note over QUEUE: Awaiting batch load

    QUEUE->>LOAD: 11. Read batch
    LOAD->>LOAD: 12. Apply update strategy
    LOAD->>DB: 13. Upsert records
    LOAD->>QUEUE: 14. Mark as loaded
```

[Detailed data flow →](data-flow.md)

## Storage Architecture

### S3 Structure

```
s3://idhub-curated-fragments/
├── redcap/
│   ├── gap/
│   │   ├── batch_20240115_100000/
│   │   │   ├── lcl/
│   │   │   │   ├── fragment_001.json
│   │   │   │   ├── fragment_002.json
│   │   │   │   └── ...
│   │   │   ├── genotype/
│   │   │   ├── sequence/
│   │   │   └── metadata.json
│   │   └── batch_20240116_100000/
│   └── uc_demarc/
├── labkey/
│   └── export_20240115/
└── manual/
    └── upload_20240115_143000/
```

**Key Characteristics**:

-   Organized by source and project
-   Batch-based organization
-   Immutable once written
-   Metadata files for tracking

### Database Schema

```mermaid
erDiagram
    subjects ||--o{ local_subject_ids : has
    subjects ||--o{ lcl : has
    subjects ||--o{ genotype : "has"
    subjects ||--o{ sequence : "has"
    subjects ||--o{ specimen : has

    subjects {
        uuid id PK
        string gsid UK
        string sex
        string diagnosis
        timestamp created_at
    }

    local_subject_ids {
        uuid id PK
        uuid subject_id FK
        int center_id
        string local_subject_id
        string identifier_type
        timestamp created_at
    }

    lcl {
        uuid id PK
        uuid subject_id FK
        string global_subject_id
        string niddk_no
        string knumber
        int passage_number
        string cell_line_status
        timestamp created_at
    }

    genotype {
        uuid id PK
        uuid subject_id FK
        string global_subject_id
        string genotype_id
        string genotyping_project
        string genotyping_barcode
        timestamp created_at
    }

    sequence {
        uuid id PK
        uuid subject_id FK
        string global_subject_id
        string sample_id
        string sample_type
        string vcf_sample_id
        timestamp created_at
    }

    specimen {
        uuid id PK
        string sample_id UK
        string sample_type
        string storage_location
        timestamp collection_date
        timestamp created_at
    }
```

[Detailed schema documentation →](database-schema.md)

## Security Architecture

### Authentication & Authorization

```mermaid
graph TB
    A[Client Request] --> B{Has API Key?}
    B -->|No| C[401 Unauthorized]
    B -->|Yes| D{Valid Key?}
    D -->|No| C
    D -->|Yes| E{Has Permission?}
    E -->|No| F[403 Forbidden]
    E -->|Yes| G[Process Request]
```

**Security Layers**:

1.  **Network Security**

    -   SSL/TLS encryption (Let's Encrypt)
    -   Nginx reverse proxy
    -   Firewall rules

2.  **Application Security**

    -   API key authentication
    -   Environment-based secrets
    -   Input validation

3.  **Database Security**

    -   Connection pooling
    -   Prepared statements
    -   Role-based access

4.  **Data Security**
    -   Encrypted at rest (S3, RDS)
    -   Encrypted in transit (HTTPS)
    -   Audit logging

[Detailed security documentation →](../security-guide.md)

## Deployment Architecture

### Environment Structure

```mermaid
graph TB
    subgraph "Production"
        P_APP[Application Services]
        P_DB[(Production DB)]
        P_S3[(Production S3)]
    end

    subgraph "QA"
        Q_APP[Application Services]
        Q_DB[(QA DB)]
        Q_S3[(QA S3)]
    end

    subgraph "Development"
        D_APP[Application Services]
        D_DB[(Local DB)]
        D_S3[(Local S3/MinIO)]
    end

    GH[GitHub Actions] -.->|Deploy| P_APP
    GH -.->|Deploy| Q_APP

    DEV[Developers] -->|Test| D_APP
    DEV -->|PR| GH
```

**Environments**:

| Environment     | Purpose              | Database         | S3 Bucket                    |
| --------------- | -------------------- | ---------------- | ---------------------------- |
| **Development** | Local development    | Local PostgreSQL | Local MinIO                  |
| **QA**          | Testing & validation | QA RDS           | `idhub-curated-fragments-qa` |
| **Production**  | Live system          | Production RDS   | `idhub-curated-fragments`    |

### Deployment Process

```mermaid
graph LR
    A[Code Push] --> B[GitHub Actions]
    B --> C{Branch?}
    C -->|main| D[Deploy to QA]
    C -->|release| E[Deploy to Prod]
    D --> F[Run Tests]
    F --> G{Tests Pass?}
    G -->|Yes| H[Deploy Services]
    G -->|No| I[Rollback]
    E --> J[Manual Approval]
    J --> H
```

[Detailed deployment documentation →](../deployment-guide.md)

## Scalability Considerations

### Current Scale

-   **Subjects**: ~50,000
-   **LCL Lines**: ~30,000
-   **Genotypes**: ~40,000
-   **Sequences**: ~20,000
-   **Daily Ingestion**: ~1,000 records

### Scaling Strategies

**Horizontal Scaling**:

-   Multiple validator instances
-   Multiple loader instances
-   Load balancing via Nginx

**Vertical Scaling**:

-   Database connection pooling
-   Batch processing optimization
-   Query optimization

**Data Partitioning**:

-   S3 partitioning by date/source
-   Database table partitioning (future)
-   Archive old validation queue records

## Monitoring & Observability

### Metrics

```mermaid
graph TB
    A[Application Metrics] --> D[Monitoring Dashboard]
    B[Database Metrics] --> D
    C[Infrastructure Metrics] --> D

    A --> A1[Request Rate]
    A --> A2[Error Rate]
    A --> A3[Processing Time]

    B --> B1[Query Performance]
    B --> B2[Connection Pool]
    B --> B3[Table Sizes]

    C --> C1[CPU Usage]
    C --> C2[Memory Usage]
    C --> C3[Disk I/O]
```

**Key Metrics**:

-   Pipeline success/failure rates
-   GSID resolution performance
-   Database load times
-   Validation queue depth
-   API response times

[Detailed monitoring documentation →](../operations-monitoring.md)

## Technology Stack

### Languages & Frameworks

| Component          | Technology      | Version      |
| ------------------ | --------------- | ------------ |
| GSID Service       | Python, FastAPI | 3.11, 0.104+ |
| REDCap Pipeline    | Python          | 3.11         |
| Fragment Validator | Python          | 3.11         |
| Table Loader       | Python          | 3.11         |
| Database           | PostgreSQL      | 15+          |
| Web UI             | NocoDB          | Latest       |
| Proxy              | Nginx           | 1.24+        |

### Key Libraries

-   **Database**: `asyncpg`, `psycopg2`
-   **API**: `fastapi`, `uvicorn`, `pydantic`
-   **AWS**: `boto3`
-   **Testing**: `pytest`, `pytest-asyncio`
-   **Validation**: `jsonschema`, `pydantic`
-   **ETL**: `pandas`, `openpyxl`

## Related Documentation

-   [Data Flow Details](data-flow.md)
-   [Database Schema](database-schema.md)
-   [Security Model](../security-guide.md)
-   [Update Strategy](update-strategy.md)