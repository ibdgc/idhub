# GSID Service API Reference

This document provides a comprehensive reference for the Global Subject ID (GSID) Service API. It includes endpoint details, request/response schemas, and design principles.

## API Design Principles (Rubric)

The GSID Service API adheres to a set of design principles to ensure consistency, predictability, and ease of use.

| Principle             | Guideline                                                                                                             |
| --------------------- | --------------------------------------------------------------------------------------------------------------------- |
| **Data Format**       | All data is sent and received as `application/json`.                                                                  |
| **Endpoint Naming**   | Endpoints use plural nouns for resources and follow RESTful conventions (e.g., `GET /subjects/{gsid}`).                 |
| **Authentication**    | All protected endpoints require a valid API key sent in the `X-API-Key` HTTP header.                                  |
| **HTTP Verbs**        | Standard HTTP verbs are used: `GET` for retrieval, `POST` for creation/actions, `PUT`/`PATCH` for updates.              |
| **Status Codes**      | Standard HTTP status codes are used to indicate success or failure (e.g., `200` OK, `201` Created, `404` Not Found). |
| **Error Responses**   | Errors return a standardized JSON object: `{"detail": "Error message"}`.                                              |
| **Idempotency**       | `GET`, `PUT`, `DELETE` operations are idempotent. `POST` may not be.                                                    |
| **Case Convention**   | `snake_case` is used for all JSON fields in requests and responses.                                                   |

## Authentication

To authenticate with the API, you must include your API key in the `X-API-Key` header with every request to a protected endpoint.

```bash
curl -X POST "https://api.idhub.ibdgc.org/api/gsid/resolve" \
  -H "X-API-Key: your-secret-api-key" \
  -H "Content-Type: application/json" \
  -d '{ ... }'
```

Failure to provide a valid key will result in a `401 Unauthorized` error.

## Endpoint Reference

---

### Health Check

Service health and version information.

`GET /health`

**Authentication**: None

**Responses**:
- `200 OK`

```json
{
  "status": "healthy",
  "service": "gsid-service",
  "version": "1.0.0",
  "database": "connected"
}
```

---

### Generate GSID

Generate a new Global Subject ID (GSID) and create a new subject record if one doesn't already exist for the given local identifier.

`POST /api/gsid/generate`

**Authentication**: Required

**Request Body**:

| Field             | Type      | Required | Description                                 |
| ----------------- | --------- | -------- | ------------------------------------------- |
| `center_id`       | `integer` | Yes      | The ID of the center providing the local ID. |
| `local_subject_id`| `string`  | Yes      | The subject identifier from the source system. |
| `identifier_type` | `string`  | Yes      | The type of local ID (e.g., `consortium_id`). |
| `metadata`        | `object`  | No       | Optional subject metadata (e.g., `sex`, `diagnosis`). |

**Example Request**:
```bash
curl -X POST "https://api.idhub.ibdgc.org/api/gsid/generate" \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "center_id": 1,
    "local_subject_id": "GAP-001",
    "identifier_type": "consortium_id",
    "metadata": {
      "sex": "F",
      "diagnosis": "CD"
    }
  }'
```

**Responses**:
- `201 Created`: If a new subject and GSID were successfully created.
- `409 Conflict`: If a subject with that `local_subject_id` already exists.

**Example Response (`201 Created`)**:
```json
{
  "gsid": "01HQXYZ123ABCDEF456789",
  "subject_id": "550e8400-e29b-41d4-a716-446655440000",
  "local_subject_id": "GAP-001",
  "center_id": 1,
  "identifier_type": "consortium_id",
  "created_at": "2024-01-15T10:00:00Z"
}
```

---

### Resolve GSID

Resolve a local subject identifier from a specific center to its corresponding Global Subject ID (GSID).

`POST /api/gsid/resolve`

**Authentication**: Required

**Request Body**:

| Field             | Type      | Required | Description                                 |
| ----------------- | --------- | -------- | ------------------------------------------- |
| `center_id`       | `integer` | Yes      | The ID of the center providing the local ID. |
| `local_subject_id`| `string`  | Yes      | The subject identifier from the source system. |
| `identifier_type` | `string`  | No       | The type of local ID.                       |

**Example Request**:
```bash
curl -X POST "https://api.idhub.ibdgc.org/api/gsid/resolve" \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "center_id": 1,
    "local_subject_id": "GAP-001"
  }'
```

**Responses**:
- `200 OK`: If the identifier is found.
- `404 Not Found`: If no subject is found for the given identifier.

**Example Response (`200 OK`)**:
```json
{
  "gsid": "01HQXYZ123ABCDEF456789",
  "subject_id": "550e8400-e29b-41d4-a716-446655440000",
  "local_subject_id": "GAP-001",
  "center_id": 1,
  "identifier_type": "consortium_id",
  "found": true
}
```

**Example Response (`404 Not Found`)**:
```json
{
  "gsid": null,
  "found": false,
  "message": "No subject found for center_id=1, local_subject_id=GAP-001"
}
```

---

### Batch Resolve GSIDs

Resolve multiple local identifiers in a single request for efficiency.

`POST /api/gsid/batch/resolve`

**Authentication**: Required

**Request Body**:
- An object containing an `identifiers` key, which is an array of identifier objects. Each object has the same schema as the request for the single `/resolve` endpoint.

**Example Request**:
```python
import requests

requests.post(
    "https://api.idhub.ibdgc.org/api/gsid/batch/resolve",
    headers={"X-API-Key": "your-api-key"},
    json={
        "identifiers": [
            {"center_id": 1, "local_subject_id": "GAP-001"},
            {"center_id": 1, "local_subject_id": "GAP-002"},
            {"center_id": 2, "local_subject_id": "CSMC-101"}
        ]
    }
)
```

**Responses**:
- `200 OK`

**Example Response (`200 OK`)**:
```json
{
  "results": [
    {
      "local_subject_id": "GAP-001",
      "gsid": "01HQXYZ123ABCDEF456789",
      "found": true
    },
    {
      "local_subject_id": "GAP-002",
      "gsid": "01HQXYZ456GHIJKL789012",
      "found": true
    },
    {
      "local_subject_id": "CSMC-101",
      "gsid": null,
      "found": false
    }
  ],
  "total": 3,
  "found": 2,
  "not_found": 1
}
```

---

### Get Subject by GSID

Retrieve a full subject record, including all known local identifiers, by their GSID.

`GET /api/gsid/subjects/{gsid}`

**Authentication**: Required

**Path Parameters**:
- `{gsid}`: The Global Subject ID of the subject to retrieve.

**Example Request**:
```bash
curl "https://api.idhub.ibdgc.org/api/gsid/subjects/01HQXYZ123ABCDEF456789" \
  -H "X-API-Key: your-api-key"
```

**Responses**:
- `200 OK`: If the subject is found.
- `404 Not Found`: If no subject with that GSID exists.

**Example Response (`200 OK`)**:
```json
{
  "gsid": "01HQXYZ123ABCDEF456789",
  "subject_id": "550e8400-e29b-41d4-a716-446655440000",
  "sex": "F",
  "diagnosis": "CD",
  "age_at_diagnosis": 25,
  "local_identifiers": [
    {
      "center_id": 1,
      "local_subject_id": "GAP-001",
      "identifier_type": "consortium_id"
    },
    {
      "center_id": 1,
      "local_subject_id": "MRN-123456",
      "identifier_type": "mrn"
    }
  ],
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-20T14:30:00Z"
}
```

## Error Handling

The API uses standard HTTP status codes to indicate the outcome of a request. When an error occurs, the response body will contain a JSON object with a `detail` key explaining the error.

| Status Code | Meaning             | When It Occurs                                              |
| ----------- | ------------------- | ----------------------------------------------------------- |
| `400`       | Bad Request         | The request body is malformed or missing required fields.   |
| `401`       | Unauthorized        | The `X-API-Key` header is missing or the key is invalid.    |
| `404`       | Not Found           | The requested resource (e.g., a subject) does not exist.    |
| `409`       | Conflict            | The resource you tried to create already exists.            |
| `422`       | Unprocessable Entity| The request body is syntactically correct but semantically invalid. |
| `500`       | Internal Server Error | An unexpected error occurred on the server.                 |