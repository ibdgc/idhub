# Development Guide

## Overview

This guide covers setting up a local development environment for the IDhub platform, including all services, tools, and best practices for contributing to the project.

## Table of Contents

-   [Prerequisites](#prerequisites)
-   [Local Environment Setup](#local-environment-setup)
-   [Service Development](#service-development)
-   [Database Development](#database-development)
-   [Testing](#testing)
-   [Code Quality](#code-quality)
-   [Git Workflow](#git-workflow)
-   [Debugging](#debugging)
-   [Contributing](#contributing)

---

## Prerequisites

### Required Software

```bash
# Python 3.11+
python3 --version

# Node.js 18+ (for NocoDB)
node --version
npm --version

# Docker & Docker Compose
docker --version
docker-compose --version

# PostgreSQL client
psql --version

# Git
git --version
```

### Install Development Tools

**macOS**:

```bash
# Install Homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install tools
brew install python@3.11 node postgresql git docker
brew install --cask docker
```

**Ubuntu/Debian**:

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python
sudo apt install -y python3.11 python3.11-venv python3-pip

# Install Node.js
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs

# Install PostgreSQL client
sudo apt install -y postgresql-client

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

**Windows (WSL2)**:

```bash
# Install WSL2
wsl --install

# Follow Ubuntu instructions above in WSL2 terminal
```

### IDE Setup

**VS Code** (Recommended):

```bash
# Install VS Code
# Download from: https://code.visualstudio.com/

# Install extensions
code --install-extension ms-python.python
code --install-extension ms-python.vscode-pylance
code --install-extension ms-python.black-formatter
code --install-extension ms-azuretools.vscode-docker
code --install-extension eamodio.gitlens
code --install-extension GitHub.copilot
```

**VS Code Settings**:

```json:.vscode/settings.json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/venv/bin/python",
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": false,
  "python.linting.flake8Enabled": true,
  "python.formatting.provider": "black",
  "python.formatting.blackArgs": ["--line-length", "88"],
  "editor.formatOnSave": true,
  "editor.codeActionsOnSave": {
    "source.organizeImports": true
  },
  "files.exclude": {
    "**/__pycache__": true,
    "**/*.pyc": true,
    "**/.pytest_cache": true,
    "**/venv": true
  },
  "[python]": {
    "editor.rulers": [88],
    "editor.tabSize": 4
  },
  "[javascript]": {
    "editor.defaultFormatter": "esbenp.prettier-vscode",
    "editor.tabSize": 2
  },
  "[json]": {
    "editor.defaultFormatter": "esbenp.prettier-vscode",
    "editor.tabSize": 2
  }
}
```

---

## Local Environment Setup

### Clone Repository

```bash
# Clone repository
git clone https://github.com/ibdgc/idhub.git
cd idhub

# Create development branch
git checkout -b feature/your-feature-name
```

### Environment Configuration

```bash
# Copy example environment file
cp .env.example .env.development

# Edit environment variables
vim .env.development
```

**Development Environment Variables**:

```bash:.env.development
# Environment
ENVIRONMENT=development
DEBUG=true

# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=idhub_dev
POSTGRES_USER=idhub_dev
POSTGRES_PASSWORD=dev_password

# GSID Service
GSID_SERVICE_URL=http://localhost:8000
GSID_API_KEY=gsid_test_dev_key_12345678901234567890
SECRET_KEY=dev_secret_key_not_for_production

# NocoDB
NC_DB=pg://localhost:5432?u=idhub_dev&p=dev_password&d=nocodb_dev
NC_AUTH_JWT_SECRET=dev_jwt_secret
NC_PUBLIC_URL=http://localhost:8080

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# AWS (LocalStack for local S3)
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_ENDPOINT_URL=http://localhost:4566
S3_BUCKET=idhub-dev-fragments

# Logging
LOG_LEVEL=DEBUG
```

### Docker Development Environment

```yaml:docker-compose.dev.yml
version: "3.8"

services:
  # PostgreSQL
  postgres_dev:
    image: postgres:15-alpine
    container_name: idhub_postgres_dev
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: idhub_dev
      POSTGRES_USER: idhub_dev
      POSTGRES_PASSWORD: dev_password
    volumes:
      - postgres_dev_data:/var/lib/postgresql/data
      - ./database/init:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U idhub_dev"]
      interval: 5s
      timeout: 5s
      retries: 5

  # Redis
  redis_dev:
    image: redis:7-alpine
    container_name: idhub_redis_dev
    ports:
      - "6379:6379"
    volumes:
      - redis_dev_data:/data

  # LocalStack (for S3)
  localstack:
    image: localstack/localstack:latest
    container_name: idhub_localstack
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3
      - DEBUG=1
      - DATA_DIR=/tmp/localstack/data
    volumes:
      - localstack_data:/tmp/localstack

  # Adminer (Database UI)
  adminer:
    image: adminer:latest
    container_name: idhub_adminer
    ports:
      - "8081:8080"
    environment:
      ADMINER_DEFAULT_SERVER: postgres_dev

volumes:
  postgres_dev_data:
  redis_dev_data:
  localstack_data:
```

```bash
# Start development services
docker-compose -f docker-compose.dev.yml up -d

# View logs
docker-compose -f docker-compose.dev.yml logs -f

# Stop services
docker-compose -f docker-compose.dev.yml down
```

---

## Service Development

### GSID Service

#### Setup

```bash
# Navigate to service directory
cd gsid-service

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install
```

#### Run Development Server

```bash
# Load environment variables
export $(cat ../.env.development | xargs)

# Run database migrations
alembic upgrade head

# Start development server with auto-reload
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Or use the run script
python run_dev.py
```

**Development Server Script**:

```python:gsid-service/run_dev.py
#!/usr/bin/env python3
"""Development server runner with auto-reload"""

import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv("../.env.development")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=".", "../core"],
        log_level="debug",
    )
```

#### API Documentation

```bash
# Access interactive API docs
open http://localhost:8000/docs

# Access ReDoc
open http://localhost:8000/redoc
```

#### Database Migrations

```bash
# Create new migration
alembic revision --autogenerate -m "Add new table"

# Apply migrations
alembic upgrade head

# Rollback migration
alembic downgrade -1

# View migration history
alembic history

# View current version
alembic current
```

### REDCap Pipeline

#### Setup

```bash
cd redcap-pipeline

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-test.txt
```

#### Run Pipeline

```bash
# Load environment
export $(cat ../.env.development | xargs)

# Run for specific project
python main.py --project gap

# Run with dry-run mode
python main.py --project gap --dry-run

# Run all enabled projects
python main.py --all

# Run with custom batch size
python main.py --project gap --batch-size 10
```

#### Debug Mode

```python:redcap-pipeline/debug.py
#!/usr/bin/env python3
"""Debug script for REDCap pipeline"""

import logging
from dotenv import load_dotenv
from services.pipeline import REDCapPipeline
from core.config import settings

# Load environment
load_dotenv("../.env.development")

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Run pipeline with debugging
pipeline = REDCapPipeline(
    project_key="gap",
    redcap_api_url=settings.REDCAP_API_URL,
    api_token=settings.REDCAP_API_TOKEN_GAP,
    gsid_service_url=settings.GSID_SERVICE_URL,
    gsid_api_key=settings.GSID_API_KEY,
)

# Process single record for debugging
result = pipeline.process_single_record(record_id="1")
print(f"Result: {result}")
```

### Fragment Validator

#### Setup

```bash
cd fragment-validator

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-test.txt
```

#### Run Validator

```bash
# Validate local file
python main.py \
  --table-name lcl \
  --input-file tests/fixtures/lcl_sample.csv \
  --mapping-config config/lcl_mapping.json \
  --source "Development Test"

# Use LocalStack for S3
export AWS_ENDPOINT_URL=http://localhost:4566
python main.py \
  --table-name blood \
  --input-file tests/fixtures/blood_sample.csv \
  --mapping-config config/blood_mapping.json \
  --source "LocalStack Test"
```

### Table Loader

#### Setup

```bash
cd table-loader

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-test.txt
```

#### Run Loader

```bash
# Dry-run mode (default)
python main.py --batch-id batch_20240115_140000 --dry-run

# Live load (use with caution)
python main.py --batch-id batch_20240115_140000

# Load specific table
python main.py --batch-id batch_20240115_140000 --table lcl
```

---

## Database Development

### Local Database Setup

```bash
# Create development database
createdb -h localhost -U idhub_dev idhub_dev

# Or using Docker
docker exec -it idhub_postgres_dev psql -U idhub_dev -c "CREATE DATABASE idhub_dev;"

# Run migrations
cd gsid-service
alembic upgrade head
```

### Database Seeding

```python:scripts/seed_dev_data.py
#!/usr/bin/env python3
"""Seed development database with test data"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "gsid-service"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.center import Center
from models.subject import Subject
from models.api_key import APIKey
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://idhub_dev:dev_password@localhost:5432/idhub_dev")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def seed_centers():
    """Seed test centers"""
    session = SessionLocal()

    centers = [
        {"center_id": 1, "name": "Test Center 1", "code": "TC1"},
        {"center_id": 2, "name": "Test Center 2", "code": "TC2"},
        {"center_id": 99, "name": "Development Center", "code": "DEV"},
    ]

    for center_data in centers:
        center = Center(**center_data)
        session.merge(center)

    session.commit()
    session.close()
    print(f"✓ Seeded {len(centers)} centers")

def seed_subjects():
    """Seed test subjects"""
    session = SessionLocal()

    subjects = [
        {
            "global_subject_id": "01HQTEST001",
            "center_id": 1,
            "local_subject_id": "SUBJ001",
            "identifier_type": "mrn"
        },
        {
            "global_subject_id": "01HQTEST002",
            "center_id": 1,
            "local_subject_id": "SUBJ002",
            "identifier_type": "mrn"
        },
    ]

    for subject_data in subjects:
        subject = Subject(**subject_data)
        session.merge(subject)

    session.commit()
    session.close()
    print(f"✓ Seeded {len(subjects)} subjects")

def seed_api_keys():
    """Seed development API keys"""
    session = SessionLocal()

    api_key = APIKey(
        key_name="dev-key",
        api_key="gsid_test_dev_key_12345678901234567890",
        description="Development API key",
        created_by="dev",
        is_active=True
    )

    session.merge(api_key)
    session.commit()
    session.close()
    print("✓ Seeded API key: gsid_test_dev_key_12345678901234567890")

if __name__ == "__main__":
    print("Seeding development database...")
    seed_centers()
    seed_subjects()
    seed_api_keys()
    print("✓ Development database seeded")
```

```bash
# Run seed script
python scripts/seed_dev_data.py
```

### Database Tools

```bash
# Connect to database
psql -h localhost -U idhub_dev -d idhub_dev

# Or using Docker
docker exec -it idhub_postgres_dev psql -U idhub_dev -d idhub_dev

# Useful queries
# List all tables
\dt

# Describe table
\d subjects

# View data
SELECT * FROM subjects LIMIT 10;

# Reset database
DROP DATABASE idhub_dev;
CREATE DATABASE idhub_dev;
```

### Database GUI Tools

**Adminer** (included in docker-compose.dev.yml):

```bash
# Access Adminer
open http://localhost:8081

# Login credentials:
# System: PostgreSQL
# Server: postgres_dev
# Username: idhub_dev
# Password: dev_password
# Database: idhub_dev
```

**pgAdmin** (alternative):

```bash
# Run pgAdmin in Docker
docker run -d \
  --name pgadmin_dev \
  -p 5050:80 \
  -e PGADMIN_DEFAULT_EMAIL=admin@idhub.local \
  -e PGADMIN_DEFAULT_PASSWORD=admin \
  dpage/pgadmin4

# Access pgAdmin
open http://localhost:5050
```

---

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html --cov-report=term

# Run specific test file
pytest tests/test_pipeline.py -v

# Run specific test
pytest tests/test_pipeline.py::TestPipeline::test_process_record -v

# Run tests matching pattern
pytest -k "test_subject" -v

# Run with markers
pytest -m unit
pytest -m integration
pytest -m "not slow"

# Run in parallel
pytest -n auto

# Stop on first failure
pytest -x

# Show print statements
pytest -s
```

### Writing Tests

**Unit Test Example**:

```python:tests/test_gsid_service.py
"""Unit tests for GSID service"""

import pytest
from unittest.mock import Mock, patch
from services.gsid_client import GSIDClient

class TestGSIDClient:
    """Test GSID client functionality"""

    @pytest.fixture
    def gsid_client(self):
        """Create GSID client instance"""
        return GSIDClient(
            service_url="http://localhost:8000",
            api_key="test_key"
        )

    def test_get_or_create_subject_existing(self, gsid_client, requests_mock):
        """Test getting existing subject"""
        # Mock API response
        requests_mock.post(
            "http://localhost:8000/subjects",
            json={"global_subject_id": "01HQTEST001", "created": False}
        )

        # Call method
        result = gsid_client.get_or_create_subject(
            center_id=1,
            local_subject_id="SUBJ001",
            identifier_type="mrn"
        )

        # Assertions
        assert result["global_subject_id"] == "01HQTEST001"
        assert result["created"] is False

    def test_get_or_create_subject_new(self, gsid_client, requests_mock):
        """Test creating new subject"""
        requests_mock.post(
            "http://localhost:8000/subjects",
            json={"global_subject_id": "01HQTEST002", "created": True}
        )

        result = gsid_client.get_or_create_subject(
            center_id=1,
            local_subject_id="SUBJ002",
            identifier_type="mrn"
        )

        assert result["global_subject_id"] == "01HQTEST002"
        assert result["created"] is True

    def test_api_error_handling(self, gsid_client, requests_mock):
        """Test API error handling"""
        requests_mock.post(
            "http://localhost:8000/subjects",
            status_code=500,
            json={"detail": "Internal server error"}
        )

        with pytest.raises(Exception) as exc_info:
            gsid_client.get_or_create_subject(
                center_id=1,
                local_subject_id="SUBJ003",
                identifier_type="mrn"
            )

        assert "500" in str(exc_info.value)
```

**Integration Test Example**:

```python:tests/integration/test_pipeline_integration.py
"""Integration tests for REDCap pipeline"""

import pytest
from services.pipeline import REDCapPipeline
from core.database import get_db_session

@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests requiring database"""

    @pytest.fixture(autouse=True)
    def setup_database(self, db_session):
        """Setup test database"""
        # Database is set up by conftest.py
        yield
        # Cleanup handled by conftest.py

    def test_end_to_end_pipeline(self, sample_redcap_data):
        """Test complete pipeline flow"""
        pipeline = REDCapPipeline(
            project_key="test",
            redcap_api_url="http://test.redcap.edu/api/",
            api_token="test_token",
            gsid_service_url="http://localhost:8000",
            gsid_api_key="test_key",
        )

        # Process records
        results = pipeline.run(batch_size=10)

        # Verify results
        assert results["total_processed"] > 0
        assert results["successful"] > 0
        assert results["errors"] == 0

        # Verify database state
        session = get_db_session()
        subjects = session.query(Subject).all()
        assert len(subjects) > 0
```

### Test Fixtures

```python:tests/conftest.py
"""Shared test fixtures"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from core.database import Base
import tempfile
import os

@pytest.fixture(scope="session")
def test_database():
    """Create test database"""
    # Use in-memory SQLite for tests
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)

@pytest.fixture
def db_session(test_database):
    """Create database session for test"""
    Session = sessionmaker(bind=test_database)
    session = Session()
    yield session
    session.rollback()
    session.close()

@pytest.fixture
def sample_redcap_data():
    """Sample REDCap data for testing"""
    return [
        {
            "record_id": "1",
            "consortium_id": "SUBJ001",
            "sample_id": "BLOOD001",
            "sample_type": "whole_blood",
        },
        {
            "record_id": "2",
            "consortium_id": "SUBJ002",
            "sample_id": "BLOOD002",
            "sample_type": "plasma",
        },
    ]

@pytest.fixture
def temp_csv_file(sample_redcap_data):
    """Create temporary CSV file"""
    import csv

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        if sample_redcap_data:
            writer = csv.DictWriter(f, fieldnames=sample_redcap_data[0].keys())
            writer.writeheader()
            writer.writerows(sample_redcap_data)

        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)
```

### Coverage Reports

```bash
# Generate HTML coverage report
pytest --cov=. --cov-report=html

# Open report
open htmlcov/index.html

# Generate XML report (for CI)
pytest --cov=. --cov-report=xml

# Show missing lines
pytest --cov=. --cov-report=term-missing
```

---

## Code Quality

### Linting

```bash
# Install linting tools
pip install flake8 black isort mypy

# Run flake8
flake8 . --max-line-length=88 --exclude=venv,__pycache__

# Run black (formatter)
black . --line-length=88

# Run isort (import sorter)
isort . --profile=black

# Run mypy (type checker)
mypy . --ignore-missing-imports
```

### Pre-commit Hooks

```yaml:.pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
      - id: check-json
      - id: check-merge-conflict
      - id: detect-private-key

  - repo: https://github.com/psf/black
    rev: 23.12.1
    hooks:
      - id: black
        language_version: python3.11
        args: [--line-length=88]

  - repo: https://github.com/pycqa/isort
    rev: 5.13.2
    hooks:
      - id: isort
        args: [--profile=black]

  - repo: https://github.com/pycqa/flake8
    rev: 7.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=88, --extend-ignore=E203]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.8.0
    hooks:
      - id: mypy
        additional_dependencies: [types-all]
        args: [--ignore-missing-imports]
```

```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files

# Update hooks
pre-commit autoupdate
```

### Code Style Guide

**Python Style** (PEP 8 + Black):

```python
# Good
def process_subject_data(
    subject_id: str,
    center_id: int,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    """
    Process subject data and return results.

    Args:
        subject_id: Global subject identifier
        center_id: Center identifier
        metadata: Additional metadata

    Returns:
        Dictionary containing processed results

    Raises:
        ValueError: If subject_id is invalid
    """
    if not subject_id:
        raise ValueError("subject_id cannot be empty")

    result = {
        "subject_id": subject_id,
        "center_id": center_id,
        "processed": True,
    }

    return result


# Bad
def processSubjectData(subjectId,centerId,metadata):
    if not subjectId:raise ValueError("Invalid")
    result={"subject_id":subjectId,"center_id":centerId}
    return result
```

---

## Git Workflow

### Branch Strategy

```bash
# Main branches
main          # Production-ready code
develop       # Integration branch for features

# Feature branches
feature/add-new-endpoint
feature/improve-validation
bugfix/fix-gsid-generation
hotfix/critical-security-patch
```

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```bash
# Format
<type>(<scope>): <subject>

<body>

<footer>

# Types
feat:     # New feature
fix:      # Bug fix
docs:     # Documentation
style:    # Formatting
refactor: # Code restructuring
test:     # Adding tests
chore:    # Maintenance

# Examples
feat(gsid): add batch subject creation endpoint

Implement new endpoint for creating multiple subjects in a single request.
Includes validation and error handling for batch operations.

Closes #123

fix(pipeline): handle missing consortium_id gracefully

Previously, missing consortium_id would cause pipeline to crash.
Now logs warning and skips record.

Fixes #456

docs(api): update authentication guide

Add examples for Python, JavaScript, and cURL.
Include rate limiting information.
```

### Pull Request Process

```bash
# 1. Create feature branch
git checkout -b feature/your-feature

# 2. Make changes and commit
git add .
git commit -m "feat(scope): description"

# 3. Push to remote
git push origin feature/your-feature

# 4. Create pull request on GitHub
# - Fill out PR template
# - Request reviews
# - Link related issues

# 5. Address review comments
git add .
git commit -m "fix: address review comments"
git push

# 6. Merge after approval
# - Squash and merge (preferred)
# - Delete branch after merge
```

**Pull Request Template**:

```markdown:.github/pull_request_template.md
## Description

Brief description of changes

## Type of Change

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed

## Checklist

- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex code
- [ ] Documentation updated
- [ ] No new warnings generated
- [ ] Tests pass locally

## Related Issues

Closes #(issue number)

## Screenshots (if applicable)

## Additional Notes
```

---

## Debugging

### VS Code Debug Configuration

```json:.vscode/launch.json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "GSID Service",
      "type": "python",
      "request": "launch",
      "module": "uvicorn",
      "args": [
        "main:app",
        "--reload",
        "--host",
        "0.0.0.0",
        "--port",
        "8000"
      ],
      "cwd": "${workspaceFolder}/gsid-service",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/gsid-service"
      },
      "envFile": "${workspaceFolder}/.env.development",
      "console": "integratedTerminal"
    },
    {
      "name": "REDCap Pipeline",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/redcap-pipeline/main.py",
      "args": [
        "--project",
        "gap",
        "--dry-run"
      ],
      "cwd": "${workspaceFolder}/redcap-pipeline",
      "envFile": "${workspaceFolder}/.env.development",
      "console": "integratedTerminal"
    },
    {
      "name": "Pytest: Current File",
      "type": "python",
      "request": "launch",
      "module": "pytest",
      "args": [
        "${file}",
        "-v",
        "-s"
      ],
      "console": "integratedTerminal",
      "justMyCode": false
    }
  ]
}
```

### Logging Configuration

```python:core/logging_config.py
"""Logging configuration for development"""

import logging
import sys
from pathlib import Path

def setup_logging(service_name: str, log_level: str = "DEBUG"):
    """Setup logging for development"""

    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            # Console handler
            logging.StreamHandler(sys.stdout),
            # File handler
            logging.FileHandler(log_dir / f"{service_name}.log"),
        ]
    )

    # Set third-party loggers to WARNING
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    logger = logging.

```

```