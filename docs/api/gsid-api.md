# GSID Service API Reference

## Overview

The GSID (Global Subject ID) Service provides a RESTful API for managing global subject identifiers, local subject ID mappings, and subject metadata across the IDhub platform.

**Base URL**: `https://api.idhub.ibdgc.org`

**API Version**: v1

**Authentication**: API Key (Header-based)

## Authentication

All API requests require authentication via API key in the request header:

```http
X-API-Key: your-api-key-here
```

See [Authentication Guide](authentication.md) for details.

## API Endpoints

### Health Check

#### GET /health

Check service health and status.

**Authentication**: Not required

**Response**:

```json
{
  "status": "healthy",
  "service": "gsid-service",
  "version": "1.0.0",
  "timestamp": "2024-01-15T14:30:22Z",
  "database": "connected",
  "uptime_seconds": 86400
}
```

**Status Codes**:

- `200 OK`: Service is healthy
- `503 Service Unavailable`: Service is unhealthy

**Example**:

```bash
curl https://api.idhub.ibdgc.org/health
```

---

### Subject Management

#### POST /subjects

Create a new subject with a global subject ID.

**Authentication**: Required

**Request Body**:

```json
{
  "center_id": 1,
  "local_subject_id": "SUBJ001",
  "identifier_type": "mrn",
  "metadata": {
    "enrollment_date": "2024-01-15",
    "study": "IBD-001",
    "site": "Site A"
  }
}
```

**Parameters**:

| Field              | Type    | Required | Description                              |
| ------------------ | ------- | -------- | ---------------------------------------- |
| `center_id`        | integer | Yes      | Center/institution identifier            |
| `local_subject_id` | string  | Yes      | Local subject identifier                 |
| `identifier_type`  | string  | Yes      | Type of identifier (mrn, study_id, etc.) |
| `metadata`         | object  | No       | Additional subject metadata              |

**Response** (201 Created):

```json
{
  "global_subject_id": "01HQXYZ123ABC",
  "center_id": 1,
  "local_subject_id": "SUBJ001",
  "identifier_type": "mrn",
  "created_at": "2024-01-15T14:30:22Z",
  "metadata": {
    "enrollment_date": "2024-01-15",
    "study": "IBD-001",
    "site": "Site A"
  }
}
```

**Error Responses**:

```json
// 400 Bad Request - Invalid input
{
  "detail": "Invalid center_id: must be a positive integer"
}

// 409 Conflict - Subject already exists
{
  "detail": "Subject already exists",
  "existing_gsid": "01HQXYZ123ABC"
}

// 401 Unauthorized - Missing/invalid API key
{
  "detail": "Invalid or missing API key"
}
```

**Status Codes**:

- `201 Created`: Subject created successfully
- `400 Bad Request`: Invalid request data
- `401 Unauthorized`: Authentication failed
- `409 Conflict`: Subject already exists

**Example**:

```bash
curl -X POST https://api.idhub.ibdgc.org/subjects \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "center_id": 1,
    "local_subject_id": "SUBJ001",
    "identifier_type": "mrn"
  }'
```

```python
import requests

response = requests.post(
    "https://api.idhub.ibdgc.org/subjects",
    headers={"X-API-Key": "your-api-key"},
    json={
        "center_id": 1,
        "local_subject_id": "SUBJ001",
        "identifier_type": "mrn"
    }
)

if response.status_code == 201:
    gsid = response.json()["global_subject_id"]
    print(f"Created GSID: {gsid}")
```

---

#### GET /subjects/{global_subject_id}

Retrieve subject information by global subject ID.

**Authentication**: Required

**Path Parameters**:

| Parameter           | Type   | Description                             |
| ------------------- | ------ | --------------------------------------- |
| `global_subject_id` | string | Global subject identifier (ULID format) |

**Response** (200 OK):

```json
{
  "global_subject_id": "01HQXYZ123ABC",
  "local_identifiers": [
    {
      "center_id": 1,
      "local_subject_id": "SUBJ001",
      "identifier_type": "mrn",
      "created_at": "2024-01-15T14:30:22Z"
    },
    {
      "center_id": 1,
      "local_subject_id": "STUDY-001",
      "identifier_type": "study_id",
      "created_at": "2024-01-16T10:15:00Z"
    }
  ],
  "metadata": {
    "enrollment_date": "2024-01-15",
    "study": "IBD-001"
  },
  "created_at": "2024-01-15T14:30:22Z",
  "updated_at": "2024-01-16T10:15:00Z"
}
```

**Error Responses**:

```json
// 404 Not Found
{
  "detail": "Subject not found"
}

// 400 Bad Request - Invalid GSID format
{
  "detail": "Invalid global_subject_id format"
}
```

**Status Codes**:

- `200 OK`: Subject found
- `400 Bad Request`: Invalid GSID format
- `401 Unauthorized`: Authentication failed
- `404 Not Found`: Subject not found

**Example**:

```bash
curl https://api.idhub.ibdgc.org/subjects/01HQXYZ123ABC \
  -H "X-API-Key: your-api-key"
```

---

#### GET /subjects

Search for subjects by local identifier.

**Authentication**: Required

**Query Parameters**:

| Parameter          | Type    | Required | Description                       |
| ------------------ | ------- | -------- | --------------------------------- |
| `center_id`        | integer | Yes      | Center identifier                 |
| `local_subject_id` | string  | Yes      | Local subject identifier          |
| `identifier_type`  | string  | No       | Type of identifier (default: any) |

**Response** (200 OK):

```json
{
  "results": [
    {
      "global_subject_id": "01HQXYZ123ABC",
      "center_id": 1,
      "local_subject_id": "SUBJ001",
      "identifier_type": "mrn",
      "created_at": "2024-01-15T14:30:22Z"
    }
  ],
  "count": 1
}
```

**Empty Results**:

```json
{
  "results": [],
  "count": 0
}
```

**Status Codes**:

- `200 OK`: Search completed (may return empty results)
- `400 Bad Request`: Missing required parameters
- `401 Unauthorized`: Authentication failed

**Example**:

```bash
# Search by center and local ID
curl "https://api.idhub.ibdgc.org/subjects?center_id=1&local_subject_id=SUBJ001" \
  -H "X-API-Key: your-api-key"

# Search with specific identifier type
curl "https://api.idhub.ibdgc.org/subjects?center_id=1&local_subject_id=SUBJ001&identifier_type=mrn" \
  -H "X-API-Key: your-api-key"
```

---

#### PATCH /subjects/{global_subject_id}

Update subject metadata.

**Authentication**: Required

**Path Parameters**:

| Parameter           | Type   | Description               |
| ------------------- | ------ | ------------------------- |
| `global_subject_id` | string | Global subject identifier |

**Request Body**:

```json
{
  "metadata": {
    "enrollment_date": "2024-01-15",
    "study": "IBD-002",
    "status": "active"
  }
}
```

**Response** (200 OK):

```json
{
  "global_subject_id": "01HQXYZ123ABC",
  "metadata": {
    "enrollment_date": "2024-01-15",
    "study": "IBD-002",
    "status": "active"
  },
  "updated_at": "2024-01-16T11:20:00Z"
}
```

**Status Codes**:

- `200 OK`: Subject updated
- `400 Bad Request`: Invalid metadata
- `401 Unauthorized`: Authentication failed
- `404 Not Found`: Subject not found

**Example**:

```bash
curl -X PATCH https://api.idhub.ibdgc.org/subjects/01HQXYZ123ABC \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "status": "active"
    }
  }'
```

---

### Local Identifier Management

#### POST /subjects/{global_subject_id}/identifiers

Add a new local identifier to an existing subject.

**Authentication**: Required

**Path Parameters**:

| Parameter           | Type   | Description               |
| ------------------- | ------ | ------------------------- |
| `global_subject_id` | string | Global subject identifier |

**Request Body**:

```json
{
  "center_id": 1,
  "local_subject_id": "STUDY-001",
  "identifier_type": "study_id"
}
```

**Response** (201 Created):

```json
{
  "global_subject_id": "01HQXYZ123ABC",
  "center_id": 1,
  "local_subject_id": "STUDY-001",
  "identifier_type": "study_id",
  "created_at": "2024-01-16T10:15:00Z"
}
```

**Error Responses**:

```json
// 409 Conflict - Identifier already exists
{
  "detail": "Local identifier already exists",
  "existing_mapping": {
    "global_subject_id": "01HQXYZ123ABC",
    "center_id": 1,
    "local_subject_id": "STUDY-001",
    "identifier_type": "study_id"
  }
}

// 409 Conflict - Identifier mapped to different subject
{
  "detail": "Local identifier already mapped to different subject",
  "existing_gsid": "01HQABC456DEF"
}
```

**Status Codes**:

- `201 Created`: Identifier added
- `400 Bad Request`: Invalid input
- `401 Unauthorized`: Authentication failed
- `404 Not Found`: Subject not found
- `409 Conflict`: Identifier already exists

**Example**:

```bash
curl -X POST https://api.idhub.ibdgc.org/subjects/01HQXYZ123ABC/identifiers \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "center_id": 1,
    "local_subject_id": "STUDY-001",
    "identifier_type": "study_id"
  }'
```

---

#### GET /subjects/{global_subject_id}/identifiers

Get all local identifiers for a subject.

**Authentication**: Required

**Response** (200 OK):

```json
{
  "global_subject_id": "01HQXYZ123ABC",
  "identifiers": [
    {
      "center_id": 1,
      "local_subject_id": "SUBJ001",
      "identifier_type": "mrn",
      "created_at": "2024-01-15T14:30:22Z"
    },
    {
      "center_id": 1,
      "local_subject_id": "STUDY-001",
      "identifier_type": "study_id",
      "created_at": "2024-01-16T10:15:00Z"
    }
  ],
  "count": 2
}
```

**Status Codes**:

- `200 OK`: Identifiers retrieved
- `401 Unauthorized`: Authentication failed
- `404 Not Found`: Subject not found

---

#### DELETE /subjects/{global_subject_id}/identifiers

Remove a local identifier from a subject.

**Authentication**: Required

**Request Body**:

```json
{
  "center_id": 1,
  "local_subject_id": "STUDY-001",
  "identifier_type": "study_id"
}
```

**Response** (204 No Content):

No response body.

**Status Codes**:

- `204 No Content`: Identifier removed
- `400 Bad Request`: Invalid input
- `401 Unauthorized`: Authentication failed
- `404 Not Found`: Subject or identifier not found

**Example**:

```bash
curl -X DELETE https://api.idhub.ibdgc.org/subjects/01HQXYZ123ABC/identifiers \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "center_id": 1,
    "local_subject_id": "STUDY-001",
    "identifier_type": "study_id"
  }'
```

---

### Batch Operations

#### POST /subjects/batch

Create or retrieve multiple subjects in a single request.

**Authentication**: Required

**Request Body**:

```json
{
  "subjects": [
    {
      "center_id": 1,
      "local_subject_id": "SUBJ001",
      "identifier_type": "mrn"
    },
    {
      "center_id": 1,
      "local_subject_id": "SUBJ002",
      "identifier_type": "mrn"
    },
    {
      "center_id": 2,
      "local_subject_id": "PATIENT-A",
      "identifier_type": "study_id"
    }
  ],
  "create_if_missing": true
}
```

**Parameters**:

| Field               | Type    | Required | Description                                       |
| ------------------- | ------- | -------- | ------------------------------------------------- |
| `subjects`          | array   | Yes      | List of subject identifiers                       |
| `create_if_missing` | boolean | No       | Create new subjects if not found (default: false) |

**Response** (200 OK):

```json
{
  "results": [
    {
      "center_id": 1,
      "local_subject_id": "SUBJ001",
      "identifier_type": "mrn",
      "global_subject_id": "01HQXYZ123ABC",
      "status": "existing"
    },
    {
      "center_id": 1,
      "local_subject_id": "SUBJ002",
      "identifier_type": "mrn",
      "global_subject_id": "01HQXYZ456DEF",
      "status": "created"
    },
    {
      "center_id": 2,
      "local_subject_id": "PATIENT-A",
      "identifier_type": "study_id",
      "global_subject_id": "01HQXYZ789GHI",
      "status": "created"
    }
  ],
  "summary": {
    "total": 3,
    "created": 2,
    "existing": 1,
    "errors": 0
  }
}
```

**With Errors**:

```json
{
  "results": [
    {
      "center_id": 1,
      "local_subject_id": "SUBJ001",
      "identifier_type": "mrn",
      "global_subject_id": "01HQXYZ123ABC",
      "status": "existing"
    },
    {
      "center_id": 1,
      "local_subject_id": "INVALID",
      "identifier_type": "mrn",
      "status": "error",
      "error": "Invalid local_subject_id format"
    }
  ],
  "summary": {
    "total": 2,
    "created": 0,
    "existing": 1,
    "errors": 1
  }
}
```

**Status Codes**:

- `200 OK`: Batch processed (check individual results for errors)
- `400 Bad Request`: Invalid request format
- `401 Unauthorized`: Authentication failed

**Example**:

```bash
curl -X POST https://api.idhub.ibdgc.org/subjects/batch \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "subjects": [
      {"center_id": 1, "local_subject_id": "SUBJ001", "identifier_type": "mrn"},
      {"center_id": 1, "local_subject_id": "SUBJ002", "identifier_type": "mrn"}
    ],
    "create_if_missing": true
  }'
```

```python
import requests

response = requests.post(
    "https://api.idhub.ibdgc.org/subjects/batch",
    headers={"X-API-Key": "your-api-key"},
    json={
        "subjects": [
            {"center_id": 1, "local_subject_id": "SUBJ001", "identifier_type": "mrn"},
            {"center_id": 1, "local_subject_id": "SUBJ002", "identifier_type": "mrn"}
        ],
        "create_if_missing": True
    }
)

results = response.json()
for result in results["results"]:
    print(f"{result['local_subject_id']}: {result['global_subject_id']} ({result['status']})")
```

---

#### POST /subjects/resolve

Resolve multiple local identifiers to global subject IDs.

**Authentication**: Required

**Request Body**:

```json
{
  "identifiers": [
    {
      "center_id": 1,
      "local_subject_id": "SUBJ001",
      "identifier_type": "mrn"
    },
    {
      "center_id": 1,
      "local_subject_id": "SUBJ002",
      "identifier_type": "mrn"
    }
  ]
}
```

**Response** (200 OK):

```json
{
  "results": [
    {
      "center_id": 1,
      "local_subject_id": "SUBJ001",
      "identifier_type": "mrn",
      "global_subject_id": "01HQXYZ123ABC",
      "found": true
    },
    {
      "center_id": 1,
      "local_subject_id": "SUBJ002",
      "identifier_type": "mrn",
      "global_subject_id": null,
      "found": false
    }
  ],
  "summary": {
    "total": 2,
    "found": 1,
    "not_found": 1
  }
}
```

**Status Codes**:

- `200 OK`: Resolution completed
- `400 Bad Request`: Invalid request format
- `401 Unauthorized`: Authentication failed

---

### Center Management

#### GET /centers

List all registered centers.

**Authentication**: Required

**Response** (200 OK):

```json
{
  "centers": [
    {
      "center_id": 1,
      "name": "Cedars-Sinai Medical Center",
      "code": "CSMC",
      "active": true,
      "created_at": "2024-01-01T00:00:00Z"
    },
    {
      "center_id": 2,
      "name": "University of Chicago",
      "code": "UCHICAGO",
      "active": true,
      "created_at": "2024-01-01T00:00:00Z"
    }
  ],
  "count": 2
}
```

**Status Codes**:

- `200 OK`: Centers retrieved
- `401 Unauthorized`: Authentication failed

---

#### GET /centers/{center_id}

Get information about a specific center.

**Authentication**: Required

**Response** (200 OK):

```json
{
  "center_id": 1,
  "name": "Cedars-Sinai Medical Center",
  "code": "CSMC",
  "active": true,
  "metadata": {
    "address": "8700 Beverly Blvd, Los Angeles, CA 90048",
    "contact": "research@csmc.edu"
  },
  "created_at": "2024-01-01T00:00:00Z",
  "subject_count": 1523
}
```

**Status Codes**:

- `200 OK`: Center found
- `401 Unauthorized`: Authentication failed
- `404 Not Found`: Center not found

---

### Statistics and Reporting

#### GET /stats

Get overall system statistics.

**Authentication**: Required

**Response** (200 OK):

```json
{
  "total_subjects": 15234,
  "total_identifiers": 23456,
  "centers": 12,
  "subjects_by_center": {
    "1": 5234,
    "2": 3456,
    "3": 2345
  },
  "identifiers_by_type": {
    "mrn": 15234,
    "study_id": 6789,
    "consortium_id": 1433
  },
  "recent_activity": {
    "subjects_created_today": 45,
    "subjects_created_this_week": 234,
    "subjects_created_this_month": 1023
  },
  "timestamp": "2024-01-15T14:30:22Z"
}
```

**Status Codes**:

- `200 OK`: Statistics retrieved
- `401 Unauthorized`: Authentication failed

---

#### GET /stats/centers/{center_id}

Get statistics for a specific center.

**Authentication**: Required

**Response** (200 OK):

```json
{
  "center_id": 1,
  "center_name": "Cedars-Sinai Medical Center",
  "total_subjects": 5234,
  "total_identifiers": 7856,
  "identifiers_by_type": {
    "mrn": 5234,
    "study_id": 2345,
    "consortium_id": 277
  },
  "recent_activity": {
    "subjects_created_today": 12,
    "subjects_created_this_week": 67,
    "subjects_created_this_month": 289
  },
  "timestamp": "2024-01-15T14:30:22Z"
}
```

**Status Codes**:

- `200 OK`: Statistics retrieved
- `401 Unauthorized`: Authentication failed
- `404 Not Found`: Center not found

---

## Data Models

### Subject

```json
{
  "global_subject_id": "string (ULID)",
  "local_identifiers": [
    {
      "center_id": "integer",
      "local_subject_id": "string",
      "identifier_type": "string",
      "created_at": "datetime (ISO 8601)"
    }
  ],
  "metadata": "object (JSON)",
  "created_at": "datetime (ISO 8601)",
  "updated_at": "datetime (ISO 8601)"
}
```

### Local Identifier

```json
{
  "center_id": "integer",
  "local_subject_id": "string",
  "identifier_type": "string",
  "global_subject_id": "string (ULID)",
  "created_at": "datetime (ISO 8601)"
}
```

### Center

```json
{
  "center_id": "integer",
  "name": "string",
  "code": "string",
  "active": "boolean",
  "metadata": "object (JSON)",
  "created_at": "datetime (ISO 8601)"
}
```

---

## Error Handling

### Error Response Format

All errors follow a consistent format:

```json
{
  "detail": "Error message describing what went wrong",
  "error_code": "SPECIFIC_ERROR_CODE",
  "timestamp": "2024-01-15T14:30:22Z"
}
```

### Common Error Codes

| HTTP Status | Error Code            | Description                     |
| ----------- | --------------------- | ------------------------------- |
| 400         | `INVALID_INPUT`       | Request validation failed       |
| 401         | `UNAUTHORIZED`        | Missing or invalid API key      |
| 403         | `FORBIDDEN`           | Insufficient permissions        |
| 404         | `NOT_FOUND`           | Resource not found              |
| 409         | `CONFLICT`            | Resource already exists         |
| 422         | `VALIDATION_ERROR`    | Data validation failed          |
| 429         | `RATE_LIMIT_EXCEEDED` | Too many requests               |
| 500         | `INTERNAL_ERROR`      | Server error                    |
| 503         | `SERVICE_UNAVAILABLE` | Service temporarily unavailable |

### Validation Errors

```json
{
  "detail": "Validation error",
  "errors": [
    {
      "field": "center_id",
      "message": "must be a positive integer",
      "value": -1
    },
    {
      "field": "local_subject_id",
      "message": "cannot be empty",
      "value": ""
    }
  ]
}
```

---

## Rate Limiting

**Limits**:

- **Standard**: 100 requests per minute
- **Batch operations**: 10 requests per minute
- **Burst**: Up to 20 requests in 1 second

**Headers**:

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1705329600
```

**Rate Limit Exceeded Response** (429):

```json
{
  "detail": "Rate limit exceeded",
  "retry_after": 45,
  "limit": 100,
  "window": "1 minute"
}
```

---

## Pagination

For endpoints that return lists, pagination is supported:

**Query Parameters**:

| Parameter   | Type    | Default | Description                |
| ----------- | ------- | ------- | -------------------------- |
| `page`      | integer | 1       | Page number                |
| `page_size` | integer | 100     | Items per page (max: 1000) |

**Response**:

```json
{
  "results": [...],
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total_pages": 5,
    "total_items": 456,
    "has_next": true,
    "has_previous": false
  }
}
```

---

## Versioning

The API uses URL-based versioning:

```
https://api.idhub.ibdgc.org/v1/subjects
https://api.idhub.ibdgc.org/v2/subjects  (future)
```

**Current Version**: v1

**Deprecation Policy**: Versions are supported for at least 12 months after a new version is released.

---

## OpenAPI Specification

Interactive API documentation is available at:

- **Swagger UI**: https://api.idhub.ibdgc.org/docs
- **ReDoc**: https://api.idhub.ibdgc.org/redoc
- **OpenAPI JSON**: https://api.idhub.ibdgc.org/openapi.json

---

## SDK Examples

### Python

```python
import requests
from typing import Optional, Dict, List

class GSIDClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({'X-API-Key': api_key})

    def create_subject(
        self,
        center_id: int,
        local_subject_id: str,
        identifier_type: str,
        metadata: Optional[Dict] = None
    ) -> Dict:
        """Create a new subject"""
        response = self.session.post(
            f"{self.base_url}/subjects",
            json={
                "center_id": center_id,
                "local_subject_id": local_subject_id,
                "identifier_type": identifier_type,
                "metadata": metadata or {}
            }
        )
        response.raise_for_status()
        return response.json()

    def get_subject(self, global_subject_id: str) -> Dict:
        """Get subject by GSID"""
        response = self.session.get(
            f"{self.base_url}/subjects/{global_subject_id}"
        )
        response.raise_for_status()
        return response.json()

    def search_subject(
        self,
        center_id: int,
        local_subject_id: str,
        identifier_type: Optional[str] = None
    ) -> Optional[Dict]:
        """Search for subject by local identifier"""
        params = {
            "center_id": center_id,
            "local_subject_id": local_subject_id
        }
        if identifier_type:
            params["identifier_type"] = identifier_type

        response = self.session.get(
            f"{self.base_url}/subjects",
            params=params
        )
        response.raise_for_status()

        results = response.json()["results"]
        return results[0] if results else None

    def batch_resolve(
        self,
        identifiers: List[Dict],
        create_if_missing: bool = False
    ) -> Dict:
        """Resolve multiple identifiers"""
        response = self.session.post(
            f"{self.base_url}/subjects/batch",
            json={
                "subjects": identifiers,
                "create_if_missing": create_if_missing
            }
        )
        response.raise_for_status()
        return response.json()

# Usage
client = GSIDClient(
    base_url="https://api.idhub.ibdgc.org",
    api_key="your-api-key"
)

# Create subject
subject = client.create_subject(
    center_id=1,
    local_subject_id="SUBJ001",
    identifier_type="mrn"
)
print(f"Created GSID: {subject['global_subject_id']}")

# Search for subject
found = client.search_subject(
    center_id=1,
    local_subject_id="SUBJ001"
)
if found:
    print(f"Found GSID: {found['global_subject_id']}")
```

### JavaScript/Node.js

```javascript
const axios = require('axios');

class GSIDClient {
  constructor(baseURL, apiKey) {
    this.client = axios.create({
      baseURL: baseURL,
      headers: {
        'X-API-Key': apiKey,
        'Content-Type': 'application/json'
      }
    });
  }

  async createSubject(centerId, localSubjectId, identifierType, metadata = {}) {
    const response = await this.client.post('/subjects', {
      center_id: centerId,
      local_subject_id: localSubjectId,
      identifier_type: identifierType,
      metadata: metadata
    });
    return response.data;
  }

  async getSubject(globalSubjectId) {
    const response = await this.client.get(`/subjects/${globalSubjectId}`);
    return response.data;
  }

  async searchSubject(centerId, localSubjectId, identifierType = null) {
    const params = {
      center_id: centerId,
      local_subject_id: localSubjectId
    };
    if (identifierType) {
      params.identifier_type = identifierType;
    }

    const response = await this.client.get('/subjects', { params });
    const results =

```
