# Deployment Guide

## Overview

This guide covers deploying the IDhub platform across different environments (development, QA, production) using Docker Compose and manual deployment methods.

## Table of Contents

-   [Prerequisites](#prerequisites)
-   [Environment Setup](#environment-setup)
-   [Docker Deployment](#docker-deployment)
-   [Manual Deployment](#manual-deployment)
-   [Database Setup](#database-setup)
-   [SSL/TLS Configuration](#ssltls-configuration)
-   [Service Configuration](#service-configuration)
-   [Health Checks](#health-checks)
-   [Monitoring](#monitoring)
-   [Backup and Recovery](#backup-and-recovery)
-   [Troubleshooting](#troubleshooting)

---

## Prerequisites

### System Requirements

**Minimum**:

-   CPU: 4 cores
-   RAM: 8 GB
-   Disk: 100 GB SSD
-   OS: Ubuntu 20.04 LTS or later

**Recommended (Production)**:

-   CPU: 8+ cores
-   RAM: 16+ GB
-   Disk: 500 GB SSD
-   OS: Ubuntu 22.04 LTS

### Software Requirements

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installations
docker --version
docker-compose --version

# Install additional tools
sudo apt install -y git curl wget vim postgresql-client
```

### Network Requirements

**Firewall Rules**:

```bash
# Allow HTTP/HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Allow SSH (if needed)
sudo ufw allow 22/tcp

# Enable firewall
sudo ufw enable
```

**DNS Configuration**:

Ensure the following DNS records are configured:

| Record Type | Name                   | Value        |
| ----------- | ---------------------- | ------------ |
| A           | idhub.ibdgc.org        | Server IP    |
| A           | api.idhub.ibdgc.org    | Server IP    |
| A           | qa.idhub.ibdgc.org     | QA Server IP |
| A           | api.qa.idhub.ibdgc.org | QA Server IP |

---

## Environment Setup

### Directory Structure

```bash
# Create application directory
sudo mkdir -p /opt/idhub
sudo chown $USER:$USER /opt/idhub
cd /opt/idhub

# Create directory structure
mkdir -p {config,data,logs,backups,secrets,ssl}
mkdir -p data/{postgres,nocodb,redis,nginx_cache}
mkdir -p logs/{nginx,gsid-service,nocodb,postgres}
```

### Clone Repository

```bash
# Clone the repository
git clone https://github.com/ibdgc/idhub.git /opt/idhub
cd /opt/idhub

# Checkout appropriate branch
git checkout main  # For production
# git checkout develop  # For QA/development
```

### Environment Variables

Create environment files for each environment:

#### Production Environment

```bash:.env.production
# Environment
ENVIRONMENT=production
DEBUG=false

# Database
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=idhub
POSTGRES_USER=idhub_user
POSTGRES_PASSWORD=<generate-secure-password>

# GSID Service
GSID_SERVICE_URL=https://api.idhub.ibdgc.org
GSID_API_KEY=<generate-api-key>
SECRET_KEY=<generate-secret-key>

# NocoDB
NC_DB=pg://postgres:5432?u=idhub_user&p=<password>&d=nocodb
NC_AUTH_JWT_SECRET=<generate-jwt-secret>
NC_PUBLIC_URL=https://idhub.ibdgc.org

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=<generate-redis-password>

# Nginx
DOMAIN=idhub.ibdgc.org
API_DOMAIN=api.idhub.ibdgc.org
SSL_EMAIL=admin@ibdgc.org

# Monitoring
SENTRY_DSN=<sentry-dsn-if-using>
LOG_LEVEL=INFO
```

#### QA Environment

```bash:.env.qa
# Environment
ENVIRONMENT=qa
DEBUG=true

# Database
POSTGRES_HOST=postgres_qa
POSTGRES_PORT=5432
POSTGRES_DB=idhub_qa
POSTGRES_USER=idhub_qa_user
POSTGRES_PASSWORD=<generate-secure-password>

# GSID Service
GSID_SERVICE_URL=https://api.qa.idhub.ibdgc.org
GSID_API_KEY=<generate-api-key>
SECRET_KEY=<generate-secret-key>

# NocoDB
NC_DB=pg://postgres_qa:5432?u=idhub_qa_user&p=<password>&d=nocodb_qa
NC_AUTH_JWT_SECRET=<generate-jwt-secret>
NC_PUBLIC_URL=https://qa.idhub.ibdgc.org

# Redis
REDIS_HOST=redis_qa
REDIS_PORT=6379

# Nginx
DOMAIN=qa.idhub.ibdgc.org
API_DOMAIN=api.qa.idhub.ibdgc.org
SSL_EMAIL=admin@ibdgc.org

# Monitoring
LOG_LEVEL=DEBUG
```

### Generate Secrets

```bash
# Generate secure passwords and keys
python3 << 'EOF'
import secrets
import string

def generate_password(length=32):
    alphabet = string.ascii_letters + string.digits + string.punctuation
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def generate_key(length=64):
    return secrets.token_hex(length)

print("POSTGRES_PASSWORD:", generate_password())
print("SECRET_KEY:", generate_key())
print("NC_AUTH_JWT_SECRET:", generate_key())
print("REDIS_PASSWORD:", generate_password())
print("GSID_API_KEY:", "gsid_live_" + secrets.token_hex(32))
EOF
```

**Important**: Store these secrets securely (e.g., password manager, vault).

---

## Docker Deployment

### Production Docker Compose

```yaml:docker-compose.production.yml
version: '3.8'

services:
  # PostgreSQL Database
  postgres:
    image: postgres:15-alpine
    container_name: idhub_postgres
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      PGDATA: /var/lib/postgresql/data/pgdata
    volumes:
      - ./data/postgres:/var/lib/postgresql/data
      - ./backups:/backups
    networks:
      - idhub_network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Redis Cache
  redis:
    image: redis:7-alpine
    container_name: idhub_redis
    restart: unless-stopped
    command: redis-server --requirepass ${REDIS_PASSWORD}
    volumes:
      - ./data/redis:/data
    networks:
      - idhub_network
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  # GSID Service
  gsid-service:
    build:
      context: ./gsid-service
      dockerfile: Dockerfile
    container_name: idhub_gsid_service
    restart: unless-stopped
    environment:
      - ENVIRONMENT=${ENVIRONMENT}
      - DEBUG=${DEBUG}
      - DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
      - REDIS_URL=redis://:${REDIS_PASSWORD}@redis:6379/0
      - SECRET_KEY=${SECRET_KEY}
      - LOG_LEVEL=${LOG_LEVEL}
    volumes:
      - ./logs/gsid-service:/app/logs
      - ./config:/app/config:ro
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    networks:
      - idhub_network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # NocoDB
  nocodb:
    image: nocodb/nocodb:latest
    container_name: idhub_nocodb
    restart: unless-stopped
    environment:
      - NC_DB=${NC_DB}
      - NC_AUTH_JWT_SECRET=${NC_AUTH_JWT_SECRET}
      - NC_PUBLIC_URL=${NC_PUBLIC_URL}
      - NC_DISABLE_TELE=true
    volumes:
      - ./data/nocodb:/usr/app/data
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - idhub_network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/api/v1/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Nginx Reverse Proxy
  nginx:
    image: nginx:alpine
    container_name: idhub_nginx
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./nginx/conf.d:/etc/nginx/conf.d:ro
      - ./nginx/snippets:/etc/nginx/snippets:ro
      - ./ssl:/etc/letsencrypt:ro
      - ./data/nginx_cache:/var/cache/nginx
      - ./logs/nginx:/var/log/nginx
    depends_on:
      - gsid-service
      - nocodb
    networks:
      - idhub_network
    healthcheck:
      test: ["CMD", "nginx", "-t"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Certbot for SSL
  certbot:
    image: certbot/certbot
    container_name: idhub_certbot
    volumes:
      - ./ssl:/etc/letsencrypt
      - ./nginx/certbot:/var/www/certbot
    entrypoint: "/bin/sh -c \'trap exit TERM; while :; do certbot renew; sleep 12h & wait $${!}; done;\'"
    networks:
      - idhub_network

networks:
  idhub_network:
    driver: bridge

volumes:
  postgres_data:
  redis_data:
  nocodb_data:
  nginx_cache:
```

### Deploy with Docker Compose

```bash
# Navigate to project directory
cd /opt/idhub

# Copy environment file
cp .env.production .env

# Pull latest images
docker-compose -f docker-compose.production.yml pull

# Build custom images
docker-compose -f docker-compose.production.yml build

# Start services
docker-compose -f docker-compose.production.yml up -d

# View logs
docker-compose -f docker-compose.production.yml logs -f

# Check service status
docker-compose -f docker-compose.production.yml ps
```

### Initial SSL Setup

```bash
# Stop nginx temporarily
docker-compose -f docker-compose.production.yml stop nginx

# Obtain SSL certificates
docker run -it --rm \
  -v /opt/idhub/ssl:/etc/letsencrypt \
  -v /opt/idhub/nginx/certbot:/var/www/certbot \
  -p 80:80 \
  certbot/certbot certonly --standalone \
  -d idhub.ibdgc.org \
  -d api.idhub.ibdgc.org \
  --email admin@ibdgc.org \
  --agree-tos \
  --no-eff-email

# Start nginx
docker-compose -f docker-compose.production.yml start nginx
```

---

## Manual Deployment

### PostgreSQL Setup

```bash
# Install PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# Start and enable service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql << EOF
CREATE DATABASE idhub;
CREATE USER idhub_user WITH ENCRYPTED PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE idhub TO idhub_user;
\q
EOF

# Configure PostgreSQL
sudo vim /etc/postgresql/15/main/postgresql.conf
# Set: listen_addresses = 'localhost'
# Set: max_connections = 100

# Restart PostgreSQL
sudo systemctl restart postgresql
```

### GSID Service Setup

```bash
# Create virtual environment
cd /opt/idhub/gsid-service
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run database migrations
alembic upgrade head

# Create systemd service
sudo tee /etc/systemd/system/gsid-service.service > /dev/null << 'EOF'
[Unit]
Description=GSID Service
After=network.target postgresql.service

[Service]
Type=exec
User=idhub
Group=idhub
WorkingDirectory=/opt/idhub/gsid-service
Environment="PATH=/opt/idhub/gsid-service/venv/bin"
EnvironmentFile=/opt/idhub/.env.production
ExecStart=/opt/idhub/gsid-service/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Start service
sudo systemctl daemon-reload
sudo systemctl start gsid-service
sudo systemctl enable gsid-service
```

### NocoDB Setup

```bash
# Install Node.js
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs

# Install NocoDB globally
sudo npm install -g nocodb

# Create systemd service
sudo tee /etc/systemd/system/nocodb.service > /dev/null << 'EOF'
[Unit]
Description=NocoDB
After=network.target postgresql.service

[Service]
Type=simple
User=idhub
Group=idhub
WorkingDirectory=/opt/idhub/nocodb
EnvironmentFile=/opt/idhub/.env.production
ExecStart=/usr/bin/nocodb
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Start service
sudo systemctl daemon-reload
sudo systemctl start nocodb
sudo systemctl enable nocodb
```

### Nginx Setup

```bash
# Install Nginx
sudo apt install -y nginx

# Copy configuration
sudo cp /opt/idhub/nginx/nginx.conf /etc/nginx/nginx.conf
sudo cp /opt/idhub/nginx/conf.d/* /etc/nginx/conf.d/
sudo cp -r /opt/idhub/nginx/snippets /etc/nginx/

# Test configuration
sudo nginx -t

# Start Nginx
sudo systemctl start nginx
sudo systemctl enable nginx
```

---

## Database Setup

### Initialize Database

```bash
# Run migrations
cd /opt/idhub/gsid-service
source venv/bin/activate
alembic upgrade head

# Seed initial data
python scripts/seed_data.py
```

### Database Migration Script

```python:scripts/seed_data.py
#!/usr/bin/env python3
"""Seed initial data for IDhub"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from core.database import Base
from models.center import Center
from models.api_key import APIKey
import secrets

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def seed_centers():
    """Seed initial centers"""
    session = SessionLocal()

    centers = [
        {"center_id": 1, "name": "Cedars-Sinai Medical Center", "code": "CSMC"},
        {"center_id": 2, "name": "University of Chicago", "code": "UCHICAGO"},
        {"center_id": 3, "name": "University of Pittsburgh", "code": "PITT"},
        {"center_id": 4, "name": "Emory University", "code": "EMORY"},
    ]

    for center_data in centers:
        center = Center(**center_data)
        session.merge(center)

    session.commit()
    session.close()
    print(f"✓ Seeded {len(centers)} centers")

def create_admin_api_key():
    """Create initial admin API key"""
    session = SessionLocal()

    api_key = "gsid_live_" + secrets.token_hex(32)

    key = APIKey(
        key_name="admin-initial",
        api_key=api_key,
        description="Initial admin API key",
        created_by="system",
        is_active=True
    )

    session.add(key)
    session.commit()
    session.close()

    print(f"✓ Created admin API key: {api_key}")
    print("  IMPORTANT: Save this key securely!")

if __name__ == "__main__":
    print("Seeding database...")
    seed_centers()
    create_admin_api_key()
    print("✓ Database seeding complete")
```

```bash
# Run seed script
python scripts/seed_data.py
```

### Database Migration Script

```python:scripts/seed_data.py
#!/usr/bin/env python3
"""Seed initial data for IDhub"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from core.database import Base
from models.center import Center
from models.api_key import APIKey
import secrets

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def seed_centers():
    """Seed initial centers"""
    session = SessionLocal()

    centers = [
        {"center_id": 1, "name": "Cedars-Sinai Medical Center", "code": "CSMC"},
        {"center_id": 2, "name": "University of Chicago", "code": "UCHICAGO"},
        {"center_id": 3, "name": "University of Pittsburgh", "code": "PITT"},
        {"center_id": 4, "name": "Emory University", "code": "EMORY"},
    ]

    for center_data in centers:
        center = Center(**center_data)
        session.merge(center)

    session.commit()
    session.close()
    print(f"✓ Seeded {len(centers)} centers")

def create_admin_api_key():
    """Create initial admin API key"""
    session = SessionLocal()

    api_key = "gsid_live_" + secrets.token_hex(32)

    key = APIKey(
        key_name="admin-initial",
        api_key=api_key,
        description="Initial admin API key",
        created_by="system",
        is_active=True
    )

    session.add(key)
    session.commit()
    session.close()

    print(f"✓ Created admin API key: {api_key}")
    print("  IMPORTANT: Save this key securely!")

if __name__ == "__main__":
    print("Seeding database...")
    seed_centers()
    create_admin_api_key()
    print("✓ Database seeding complete")
```

```bash:scripts/backup_database.sh
#!/bin/bash
# Database backup script

set -e

# Configuration
BACKUP_DIR="/opt/idhub/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="idhub_backup_${TIMESTAMP}.sql.gz"
RETENTION_DAYS=30

# Load environment
source /opt/idhub/.env.production

# Create backup
echo "Creating backup: ${BACKUP_FILE}"
PGPASSWORD=${POSTGRES_PASSWORD} pg_dump \
  -h ${POSTGRES_HOST} \
  -U ${POSTGRES_USER} \
  -d ${POSTGRES_DB} \
  | gzip > "${BACKUP_DIR}/${BACKUP_FILE}"

# Verify backup
if [ -f "${BACKUP_DIR}/${BACKUP_FILE}" ]; then
    echo "✓ Backup created successfully"
    ls -lh "${BACKUP_DIR}/${BACKUP_FILE}"
else
    echo "✗ Backup failed"
    exit 1
fi

# Clean old backups
echo "Cleaning backups older than ${RETENTION_DAYS} days"
find "${BACKUP_DIR}" -name "idhub_backup_*.sql.gz" -mtime +${RETENTION_DAYS} -delete

# Upload to S3 (optional)
if [ -n "${AWS_S3_BUCKET}" ]; then
    echo "Uploading to S3..."
    aws s3 cp "${BACKUP_DIR}/${BACKUP_FILE}" "s3://${AWS_S3_BUCKET}/backups/"
fi

echo "✓ Backup complete"
```

```bash
# Make executable
chmod +x scripts/backup_database.sh

# Add to crontab (daily at 2 AM)
crontab -e
# Add: 0 2 * * * /opt/idhub/scripts/backup_database.sh >> /opt/idhub/logs/backup.log 2>&1
```

---

## SSL/TLS Configuration

### Let's Encrypt Setup

```bash
# Install Certbot
sudo apt install -y certbot python3-certbot-nginx

# Obtain certificates
sudo certbot certonly --nginx \
  -d idhub.ibdgc.org \
  -d api.idhub.ibdgc.org \
  --email admin@ibdgc.org \
  --agree-tos \
  --no-eff-email

# Test auto-renewal
sudo certbot renew --dry-run

# Auto-renewal is configured via systemd timer
sudo systemctl status certbot.timer
```

### Manual SSL Certificate

```bash
# Generate private key
openssl genrsa -out /opt/idhub/ssl/private.key 4096

# Generate CSR
openssl req -new -key /opt/idhub/ssl/private.key \
  -out /opt/idhub/ssl/request.csr \
  -subj "/C=US/ST=California/L=Los Angeles/O=IBDGC/CN=idhub.ibdgc.org"

# Submit CSR to CA and receive certificate
# Place certificate in /opt/idhub/ssl/certificate.crt
# Place CA bundle in /opt/idhub/ssl/ca_bundle.crt

# Update Nginx configuration
sudo vim /etc/nginx/conf.d/ssl.conf
# ssl_certificate /opt/idhub/ssl/certificate.crt;
# ssl_certificate_key /opt/idhub/ssl/private.key;
```

---

## Service Configuration

### GSID Service Configuration

```yaml:gsid-service/config/production.yml
# Production configuration for GSID Service

server:
  host: 0.0.0.0
  port: 8000
  workers: 4
  reload: false

database:
  pool_size: 20
  max_overflow: 10
  pool_timeout: 30
  pool_recycle: 3600

redis:
  max_connections: 50
  socket_timeout: 5
  socket_connect_timeout: 5

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

security:
  api_key_header: X-API-Key
  rate_limit:
    enabled: true
    requests_per_minute: 100
  cors:
    enabled: true
    origins:
      - https://idhub.ibdgc.org
      - https://api.idhub.ibdgc.org
```

### NocoDB Configuration

```bash
# NocoDB environment variables
NC_DB=pg://postgres:5432?u=idhub_user&p=password&d=nocodb
NC_AUTH_JWT_SECRET=your-jwt-secret
NC_PUBLIC_URL=https://idhub.ibdgc.org
NC_DISABLE_TELE=true
NC_ADMIN_EMAIL=admin@ibdgc.org
NC_ADMIN_PASSWORD=secure-admin-password
```

---

## Health Checks

### Service Health Check Script

```bash:scripts/health_check.sh
#!/bin/bash
# Health check script for all services

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m # No Color

# Configuration
GSID_URL="https://api.idhub.ibdgc.org/health"
NOCODB_URL="https://idhub.ibdgc.org"
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"

echo "=== IDhub Health Check ==="
echo

# Check GSID Service
echo -n "GSID Service: "
if curl -sf "${GSID_URL}" > /dev/null; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
    exit 1
fi

# Check NocoDB
echo -n "NocoDB: "
if curl -sf "${NOCODB_URL}" > /dev/null; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
    exit 1
fi

# Check PostgreSQL
echo -n "PostgreSQL: "
if pg_isready -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
    exit 1
fi

# Check Nginx
echo -n "Nginx: "
if systemctl is-active --quiet nginx; then
    echo -e "${GREEN}✓ Running${NC}"
else
    echo -e "${RED}✗ Stopped${NC}"
    exit 1
fi

# Check disk space
echo -n "Disk Space: "
DISK_USAGE=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "${DISK_USAGE}" -lt 80 ]; then
    echo -e "${GREEN}✓ ${DISK_USAGE}% used${NC}"
elif [ "${DISK_USAGE}" -lt 90 ]; then
    echo -e "${YELLOW}⚠ ${DISK_USAGE}% used${NC}"
else
    echo -e "${RED}✗ ${DISK_USAGE}% used${NC}"
fi

echo
echo "=== All checks passed ==="
```

```bash
# Make executable
chmod +x scripts/health_check.sh

# Run health check
./scripts/health_check.sh

# Add to monitoring (every 5 minutes)
crontab -e
# Add: */5 * * * * /opt/idhub/scripts/health_check.sh >> /opt/idhub/logs/health.log 2>&1
```

---

## Monitoring

### Prometheus Configuration

```yaml:monitoring/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'gsid-service'
    static_configs:
      - targets: ['gsid-service:8000']
    metrics_path: '/metrics'

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: 'nginx'
    static_configs:
      - targets: ['nginx-exporter:9113']

  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']
```

### Grafana Dashboard

```json:monitoring/grafana/dashboards/idhub.json
{
  "dashboard": {
    "title": "IDhub Monitoring",
    "panels": [
      {
        "title": "API Request Rate",
        "targets": [
          {
            "expr": "rate(http_requests_total[5m])"
          }
        ]
      },
      {
        "title": "Database Connections",
        "targets": [
          {
            "expr": "pg_stat_database_numbackends"
          }
        ]
      },
      {
        "title": "Response Time",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))"
          }
        ]
      }
    ]
  }
}
```

---

## Backup and Recovery

### Automated Backup

```bash:scripts/automated_backup.sh
#!/bin/bash
# Automated backup with rotation and S3 upload

set -e

# Configuration
BACKUP_DIR="/opt/idhub/backups"
S3_BUCKET="idhub-backups"
RETENTION_DAYS=30
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Load environment
source /opt/idhub/.env.production

# Create backup directory
mkdir -p "${BACKUP_DIR}"

# Backup PostgreSQL
echo "Backing up PostgreSQL..."
PGPASSWORD=${POSTGRES_PASSWORD} pg_dump \
  -h ${POSTGRES_HOST} \
  -U ${POSTGRES_USER} \
  -d ${POSTGRES_DB} \
  -F c \
  -f "${BACKUP_DIR}/postgres_${TIMESTAMP}.dump"

# Backup NocoDB data
echo "Backing up NocoDB data..."
tar -czf "${BACKUP_DIR}/nocodb_${TIMESTAMP}.tar.gz" \
  -C /opt/idhub/data nocodb

# Backup configuration
echo "Backing up configuration..."
tar -czf "${BACKUP_DIR}/config_${TIMESTAMP}.tar.gz" \
  -C /opt/idhub config nginx

# Upload to S3
if command -v aws &> /dev/null; then
    echo "Uploading to S3..."
    aws s3 sync "${BACKUP_DIR}" "s3://${S3_BUCKET}/backups/" \
      --exclude "*" \
      --include "*_${TIMESTAMP}.*"
fi

# Clean old backups
echo "Cleaning old backups..."
find "${BACKUP_DIR}" -type f -mtime +${RETENTION_DAYS} -delete

echo "✓ Backup complete"
```

### Recovery Procedure

```bash:scripts/restore_backup.sh
#!/bin/bash
# Restore from backup

set -e

# Check arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 <backup_timestamp>"
    echo "Example: $0 20240115_140000"
    exit 1
fi

TIMESTAMP=$1
BACKUP_DIR="/opt/idhub/backups"

# Load environment
source /opt/idhub/.env.production

# Stop services
echo "Stopping services..."
docker-compose -f docker-compose.production.yml stop

# Restore PostgreSQL
echo "Restoring PostgreSQL..."
PGPASSWORD=${POSTGRES_PASSWORD} pg_restore \
  -h ${POSTGRES_HOST} \
  -U ${POSTGRES_USER} \
  -d ${POSTGRES_DB} \
  --clean \
  --if-exists \
  "${BACKUP_DIR}/postgres_${TIMESTAMP}.dump"

# Restore NocoDB data
echo "Restoring NocoDB data..."
rm -rf /opt/idhub/data/nocodb/*
tar -xzf "${BACKUP_DIR}/nocodb_${TIMESTAMP}.tar.gz" \
  -C /opt/idhub/data

# Restore configuration
echo "Restoring configuration..."
tar -xzf "${BACKUP_DIR}/config_${TIMESTAMP}.tar.gz" \
  -C /opt/idhub

# Start services
echo "Starting services..."
docker-compose -f docker-compose.production.yml up -d

echo "✓ Restore complete"
```

---

## Troubleshooting

### Common Issues

#### Service Won't Start

```bash
# Check logs
docker-compose -f docker-compose.production.yml logs gsid-service

# Check service status
docker-compose -f docker-compose.production.yml ps

# Restart service
docker-compose -f docker-compose.production.yml restart gsid-service
```

#### Database Connection Issues

```bash
# Test database connection
docker exec -it idhub_postgres psql -U idhub_user -d idhub

# Check database logs
docker logs idhub_postgres

# Verify credentials
cat .env.production | grep POSTGRES
```

#### SSL Certificate Issues

```bash
# Check certificate validity
openssl x509 -in /opt/idhub/ssl/fullchain.pem -text -noout

# Renew certificate
sudo certbot renew --force-renewal

# Test SSL configuration
openssl s_client -connect idhub.ibdgc.org:443
```

#### High Memory Usage

```bash
# Check container stats
docker stats

# Restart services
docker-compose -f docker
```
