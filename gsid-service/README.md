# GSID Service

Global Subject ID (GSID) generation and management service.

## Overview

This service provides:

- GSID generation with collision-resistant identifiers
- Subject registration and identity resolution
- API for GSID lookup and management

## API Endpoints

- `POST /generate` - Generate new GSIDs
- `POST /register` - Register a subject
- `GET /lookup/{gsid}` - Lookup subject information
- `GET /health` - Health check

## Configuration

Required environment variables:

- `DB_HOST` - Database host
- `DB_NAME` - Database name
- `DB_USER` - Database user
- `DB_PASSWORD` - Database password
- `GSID_API_KEY` - API key for authentication

## Running Tests

```bash
pytest
```
