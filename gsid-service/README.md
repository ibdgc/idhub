# GSID Service

Global Subject ID (GSID) generation and management service for the IBD Genetics Consortium IDhub platform.

## Overview

The GSID Service provides centralized subject identification and identity resolution across multiple research centers. It generates collision-resistant global identifiers and manages the mapping between local subject IDs and global GSIDs.

### Key Features

- **GSID Generation**: Cryptographically secure 16-character identifiers with `GSID-` prefix
- **Identity Resolution**: Automatic detection and linking of duplicate subject registrations
- **Multi-Center Support**: Handles subjects across different research centers with local ID mapping
- **API Key Authentication**: Secure access control for all endpoints
- **Audit Logging**: Complete tracking of all identity resolution decisions

## Architecture

```
gsid-service/
├── api/
│   ├── __init__.py
│   ├── models.py          # Pydantic request/response models
│   └── routes.py          # FastAPI endpoint definitions
├── core/
│   ├── __init__.py
│   ├── config.py          # Environment configuration
│   ├── database.py        # PostgreSQL connection management
│   └── security.py        # API key verification
├── services/
│   ├── __init__.py
│   ├── gsid_generator.py  # GSID generation logic
│   └── identity_resolution.py  # Duplicate detection
├── tests/
│   ├── conftest.py        # Pytest fixtures
│   ├── test_api_complete.py
│   ├── test_database.py
│   ├── test_gsid_format.py
│   └── test_integration.py
├── main.py                # FastAPI application entry point
├── requirements.txt
├── requirements-test.txt
├── Dockerfile
├── Dockerfile.test
├── pytest.ini
└── .coveragerc
```

## API Endpoints

### `POST /register`

Register or link a subject with automatic identity resolution.

**Authentication**: Requires `x-api-key` header

**Request Body**:

```json
{
  "center_id": 1,
  "local_subject_id": "SUBJ001",
  "identifier_type": "primary",
  "registration_year": "2024-01-15",
  "control": false,
  "created_by": "system"
}
```

**Response**:

```json
{
  "gsid": "GSID-A1B2C3D4E5F6G7H8",
  "action": "create_new",
  "matched_gsid": null,
  "confidence": null,
  "resolution_log_id": 123
}
```

**Actions**:

- `create_new`: New GSID created
- `link_existing`: Linked to existing GSID (duplicate detected)
- `already_linked`: Subject already registered

### `POST /batch-register`

Register multiple subjects in a single request.

**Request Body**:

```json
{
  "subjects": [
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
}
```

### `GET /lookup/{gsid}`

Retrieve subject information by GSID.

**Response**:

```json
{
  "gsid": "GSID-A1B2C3D4E5F6G7H8",
  "center_id": 1,
  "local_subject_id": "SUBJ001",
  "identifier_type": "primary",
  "created_at": "2024-01-15T10:30:00Z"
}
```

### `GET /health`

Health check endpoint (no authentication required).

**Response**:

```json
{
  "status": "healthy",
  "service": "gsid-service",
  "version": "1.0.0"
}
```

## Configuration

### Environment Variables

| Variable       | Description            | Required | Default      |
| -------------- | ---------------------- | -------- | ------------ |
| `DB_HOST`      | PostgreSQL host        | Yes      | `idhub_db`   |
| `DB_NAME`      | Database name          | Yes      | `idhub`      |
| `DB_USER`      | Database user          | Yes      | `idhub_user` |
| `DB_PASSWORD`  | Database password      | Yes      | -            |
| `DB_PORT`      | Database port          | No       | `5432`       |
| `GSID_API_KEY` | API authentication key | Yes      | -            |
| `LOG_LEVEL`    | Logging level          | No       | `INFO`       |

### Example `.env` File

```bash
DB_HOST=idhub_db
DB_NAME=idhub
DB_USER=idhub_user
DB_PASSWORD=your_secure_password
DB_PORT=5432
GSID_API_KEY=your-secure-random-key-here-min-32-chars
LOG_LEVEL=INFO
```

## GSID Format Specification

GSIDs follow a standardized format (truncated ULID):

- **Prefix**: `GSID-`
- **ID Length**: 16 characters (uppercase alphanumeric)
- **Total Length**: 21 characters
- **Character Set**: A-Z, 0-9 (excludes ambiguous characters: O, 0, I, 1)
- **Example**: `GSID-A7K9M2P4R6T8W3X5`

### Generation Algorithm

1. Generate 12 random bytes using `secrets.token_bytes()`
2. Encode to base32 (uppercase)
3. Remove padding and ambiguous characters
4. Truncate to 16 characters
5. Prepend `GSID-` prefix

This provides ~2^80 possible combinations with collision resistance.

## Identity Resolution

The service automatically detects potential duplicate registrations using:

1. **Exact Match**: Same `center_id` + `local_subject_id`
2. **Fuzzy Match**: Similar identifiers within same center (future enhancement)
3. **Cross-Center**: Potential matches across centers (logged for review)

### Resolution Actions

- **Create New**: No match found, new GSID assigned
- **Link Existing**: High-confidence match, link to existing GSID
- **Already Linked**: Subject previously registered

All resolution decisions are logged in `identity_resolution_log` table for audit.

## Database Schema

### `subjects` Table

```sql
CREATE TABLE subjects (
    gsid VARCHAR(21) PRIMARY KEY,
    center_id INTEGER NOT NULL,
    local_subject_id VARCHAR(255) NOT NULL,
    identifier_type VARCHAR(50) DEFAULT 'primary',
    registration_year DATE,
    control BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    UNIQUE(center_id, local_subject_id, identifier_type)
);
```

### `identity_resolution_log` Table

```sql
CREATE TABLE identity_resolution_log (
    id SERIAL PRIMARY KEY,
    center_id INTEGER NOT NULL,
    local_subject_id VARCHAR(255) NOT NULL,
    gsid VARCHAR(21),
    action VARCHAR(50),
    matched_gsid VARCHAR(21),
    confidence DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

# Run service
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/test_api_complete.py -v

# Run with markers
pytest -m unit
pytest -m integration
```

### Test Coverage

Current coverage: **~85%**

Coverage reports are generated in:

- Terminal: `--cov-report=term-missing`
- HTML: `htmlcov/index.html`
- XML: `coverage.xml`
- JUnit: `test-reports/junit.xml`

### Docker Development

```bash
# Build image
docker build -t gsid-service:latest .

# Run container
docker run -p 8000:8000 \
  -e DB_HOST=host.docker.internal \
  -e DB_PASSWORD=your_password \
  -e GSID_API_KEY=your_api_key \
  gsid-service:latest

# Run tests in Docker
docker build -f Dockerfile.test -t gsid-service:test .
docker run gsid-service:test
```

## Testing

### Test Structure

- **`test_api_complete.py`**: API endpoint tests (19 tests)
- **`test_database.py`**: Database connection and cursor management
- **`test_gsid_format.py`**: GSID format validation
- **`test_integration.py`**: End-to-end integration tests

### Example Test

```python
def test_register_new_subject(client, mock_db_connection):
    """Test registering a new subject"""
    response = client.post(
        "/register",
        json={
            "center_id": 1,
            "local_subject_id": "SUBJ001",
            "identifier_type": "primary"
        },
        headers={"x-api-key": "test-api-key-12345"}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["action"] == "create_new"
    assert data["gsid"].startswith("GSID-")
```

## Deployment

### Production Deployment

The service is deployed via Docker Compose as part of the IDhub platform:

```yaml
gsid-service:
  build: ./gsid-service
  container_name: gsid-service
  restart: unless-stopped
  environment:
    - DB_HOST=${DB_HOST}
    - DB_NAME=${DB_NAME}
    - DB_USER=${DB_USER}
    - DB_PASSWORD=${IDHUB_DB_PASSWORD}
    - GSID_API_KEY=${GSID_API_KEY}
  depends_on:
    - idhub_db
  networks:
    - idhub_network
```

### Health Monitoring

```bash
# Check service health
curl https://api.idhub.ibdgc.org/health

# Expected response
{"status":"healthy","service":"gsid-service","version":"1.0.0"}
```

### Logging

Logs are written to stdout/stderr and captured by Docker:

```bash
# View logs
docker logs gsid-service -f

# View last 100 lines
docker logs gsid-service --tail 100
```

## Security

### API Key Management

- API keys should be **minimum 32 characters**
- Use cryptographically secure random generation
- Rotate keys periodically
- Never commit keys to version control

```bash
# Generate secure API key
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### Database Security

- Use strong passwords (minimum 16 characters)
- Restrict database access to service network only
- Enable SSL/TLS for database connections in production
- Regular backups with encryption

## Troubleshooting

### Common Issues

**Issue**: `500 Internal Server Error - API key not configured`

```bash
# Solution: Set GSID_API_KEY environment variable
export GSID_API_KEY="your-secure-key"
```

**Issue**: `Connection refused to database`

```bash
# Solution: Verify database is running and accessible
docker ps | grep idhub_db
docker logs idhub_db
```

**Issue**: `403 Forbidden - Invalid API key`

```bash
# Solution: Verify API key matches in both service and client
echo $GSID_API_KEY
```

## Performance

### Benchmarks

- **GSID Generation**: ~0.1ms per ID
- **Subject Registration**: ~50ms (including DB write)
- **Batch Registration**: ~5ms per subject (100 subjects)
- **Lookup**: ~10ms (indexed query)

### Optimization Tips

- Use batch registration for bulk imports
- Enable database connection pooling
- Cache frequently accessed lookups (future enhancement)

## Contributing

### Code Style

- Follow PEP 8 guidelines
- Use type hints for all functions
- Write docstrings for public APIs
- Maintain test coverage above 80%

### Pull Request Process

1. Create feature branch from `main`
2. Write tests for new functionality
3. Ensure all tests pass: `pytest`
4. Update documentation
5. Submit PR with clear description

## Support

For issues or questions:

- **GitHub Issues**: https://github.com/ibdgc/idhub/issues
- **Documentation**: See root `/README.md` for platform overview
- **Related Services**: See `/fragment-validator`, `/table-loader`, `/redcap-pipeline`

## License

[Add license information]

## Changelog

### v1.0.0 (2024-01-15)

- Initial release
- GSID generation and registration
- Identity resolution
- API key authentication
- Comprehensive test suite
