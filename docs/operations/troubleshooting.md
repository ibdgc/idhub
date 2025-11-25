# Troubleshooting Guide

## Overview

This guide provides solutions to common issues encountered in the IDhub platform, including diagnostic steps, resolution procedures, and preventive measures.

## Table of Contents

- [Quick Diagnostics](#quick-diagnostics)
- [Service Issues](#service-issues)
- [Database Issues](#database-issues)
- [Pipeline Issues](#pipeline-issues)
- [Network & Connectivity](#network--connectivity)
- [Performance Issues](#performance-issues)
- [Data Issues](#data-issues)
- [Authentication & Authorization](#authentication--authorization)
- [Emergency Procedures](#emergency-procedures)

---

## Quick Diagnostics

### Health Check Script

```bash:scripts/health_check.sh
#!/bin/bash
# Quick health check for IDhub platform

set -e

echo "========================================="
echo "IDhub Platform Health Check"
echo "========================================="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_service() {
    local service=$1
    local url=$2

    echo -n "Checking $service... "

    if curl -sf "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        return 1
    fi
}

check_docker_container() {
    local container=$1

    echo -n "Checking container $container... "

    if docker ps --filter "name=$container" --filter "status=running" | grep -q "$container"; then
        echo -e "${GREEN}✓ Running${NC}"
        return 0
    else
        echo -e "${RED}✗ Not Running${NC}"
        return 1
    fi
}

check_database() {
    echo -n "Checking database connection... "

    if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Connected${NC}"
        return 0
    else
        echo -e "${RED}✗ Connection Failed${NC}"
        return 1
    fi
}

# Check Docker containers
echo "=== Docker Containers ==="
check_docker_container "idhub_db"
check_docker_container "nocodb"
check_docker_container "gsid-service"
check_docker_container "nginx"
echo ""

# Check services
echo "=== Services ==="
check_service "NocoDB" "http://localhost:8080/api/v1/health"
check_service "GSID Service" "http://localhost:8000/health"
check_service "Nginx" "http://localhost:80"
echo ""

# Check database
echo "=== Database ==="
check_database
echo ""

# Check disk space
echo "=== Disk Space ==="
df -h | grep -E "Filesystem|/dev/|docker"
echo ""

# Check memory
echo "=== Memory Usage ==="
free -h
echo ""

# Check Docker stats
echo "=== Docker Resource Usage ==="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"
echo ""

# Check logs for errors
echo "=== Recent Errors ==="
echo "Checking last 100 log lines for errors..."
docker logs --tail 100 gsid-service 2>&1 | grep -i error | tail -5 || echo "No errors found"
docker logs --tail 100 nocodb 2>&1 | grep -i error | tail -5 || echo "No errors found"
echo ""

echo "========================================="
echo "Health check complete"
echo "========================================="
```

### Service Status Check

```bash:scripts/check_services.sh
#!/bin/bash
# Check status of all IDhub services

echo "=== Service Status Check ==="
echo ""

# Function to check HTTP endpoint
check_http() {
    local name=$1
    local url=$2
    local expected_code=${3:-200}

    response=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)

    if [ "$response" = "$expected_code" ]; then
        echo "✓ $name: OK (HTTP $response)"
    else
        echo "✗ $name: FAILED (HTTP $response, expected $expected_code)"
    fi
}

# Function to check database
check_db() {
    if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "SELECT version();" > /dev/null 2>&1; then
        echo "✓ PostgreSQL: Connected"

        # Check database size
        size=$(PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));")
        echo "  Database size: $size"

        # Check active connections
        connections=$(PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT count(*) FROM pg_stat_activity;")
        echo "  Active connections: $connections"
    else
        echo "✗ PostgreSQL: Connection failed"
    fi
}

# Function to check S3
check_s3() {
    if aws s3 ls s3://$S3_BUCKET > /dev/null 2>&1; then
        echo "✓ S3 Bucket: Accessible"

        # Count objects
        count=$(aws s3 ls s3://$S3_BUCKET --recursive | wc -l)
        echo "  Object count: $count"
    else
        echo "✗ S3 Bucket: Not accessible"
    fi
}

# Check services
check_http "NocoDB" "http://localhost:8080/api/v1/health"
check_http "GSID Service" "http://localhost:8000/health"
check_http "GSID Service API" "http://localhost:8000/api/v1/health"
check_http "Nginx" "http://localhost:80"

echo ""
check_db

echo ""
check_s3

echo ""
echo "=== Docker Container Status ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

---

## Service Issues

### GSID Service Not Starting

**Symptoms:**

- Container exits immediately
- Health check fails
- API returns 502/503 errors

**Diagnostic Steps:**

```bash
# Check container logs
docker logs gsid-service --tail 100

# Check container status
docker ps -a | grep gsid-service

# Inspect container
docker inspect gsid-service

# Check environment variables
docker exec gsid-service env | grep -E "DB_|GSID_"
```

**Common Causes & Solutions:**

#### 1. Database Connection Failed

```bash
# Test database connection
docker exec gsid-service python -c "
import asyncpg
import asyncio

async def test():
    try:
        conn = await asyncpg.connect(
            host='idhub_db',
            port=5432,
            user='idhub_user',
            password='your_password',
            database='idhub'
        )
        print('✓ Database connection successful')
        await conn.close()
    except Exception as e:
        print(f'✗ Database connection failed: {e}')

asyncio.run(test())
"
```

**Solution:**

```bash
# Check database is running
docker ps | grep idhub_db

# Restart database
docker restart idhub_db

# Wait for database to be ready
sleep 10

# Restart GSID service
docker restart gsid-service
```

#### 2. Missing Environment Variables

```bash
# Check required variables
docker exec gsid-service env | grep -E "DB_HOST|DB_NAME|DB_USER|DB_PASSWORD|GSID_API_KEY"
```

**Solution:**

```bash
# Update .env file
cat >> .env << EOF
DB_HOST=idhub_db
DB_NAME=idhub
DB_USER=idhub_user
DB_PASSWORD=your_secure_password
GSID_API_KEY=your_api_key_min_32_chars
EOF

# Recreate container
docker-compose up -d gsid-service
```

#### 3. Port Conflict

```bash
# Check if port 8000 is in use
netstat -tuln | grep 8000
lsof -i :8000
```

**Solution:**

```bash
# Kill process using port
kill -9 $(lsof -t -i:8000)

# Or change port in docker-compose.yml
# ports:
#   - "8001:8000"

docker-compose up -d gsid-service
```

#### 4. Database Schema Not Initialized

```bash
# Check if tables exist
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "\dt"
```

**Solution:**

```bash
# Run migrations
cd gsid-service
alembic upgrade head

# Or reinitialize database
docker-compose down
docker volume rm idhub_postgres_data
docker-compose up -d
```

### NocoDB Issues

#### NocoDB Not Accessible

**Diagnostic Steps:**

```bash
# Check container
docker logs nocodb --tail 100

# Check port binding
docker port nocodb

# Test connection
curl -v http://localhost:8080/api/v1/health
```

**Solutions:**

```bash
# Restart NocoDB
docker restart nocodb

# Check database connection
docker exec nocodb nc -zv idhub_db 5432

# Clear NocoDB cache
docker exec nocodb rm -rf /usr/app/data/nc/noco.db

# Restart with fresh state
docker-compose down nocodb
docker-compose up -d nocodb
```

#### NocoDB Performance Issues

```bash
# Check resource usage
docker stats nocodb --no-stream

# Check database connections
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    application_name,
    COUNT(*) as connection_count,
    state
FROM pg_stat_activity
WHERE application_name LIKE '%nocodb%'
GROUP BY application_name, state;
"

# Increase memory limit
# In docker-compose.yml:
# services:
#   nocodb:
#     deploy:
#       resources:
#         limits:
#           memory: 2G
```

### Nginx Issues

#### 502 Bad Gateway

**Diagnostic Steps:**

```bash
# Check nginx logs
docker logs nginx --tail 100

# Check upstream services
curl http://localhost:8080/api/v1/health  # NocoDB
curl http://localhost:8000/health         # GSID Service

# Test nginx config
docker exec nginx nginx -t
```

**Solutions:**

```bash
# Restart upstream services
docker restart nocodb gsid-service

# Wait for services to be ready
sleep 10

# Restart nginx
docker restart nginx

# Check nginx error log
docker exec nginx cat /var/log/nginx/error.log | tail -50
```

#### SSL Certificate Issues

```bash
# Check certificate expiry
docker exec nginx openssl x509 -in /etc/nginx/ssl/cert.pem -noout -dates

# Renew Let's Encrypt certificate
docker exec nginx certbot renew

# Reload nginx
docker exec nginx nginx -s reload
```

---

## Database Issues

### Connection Pool Exhausted

**Symptoms:**

- "too many connections" errors
- Slow query performance
- Service timeouts

**Diagnostic Steps:**

```sql
-- Check current connections
SELECT
    COUNT(*) as total_connections,
    MAX(max_conn) as max_connections
FROM (
    SELECT setting::int as max_conn FROM pg_settings WHERE name = 'max_connections'
) mc
CROSS JOIN pg_stat_activity;

-- Check connections by application
SELECT
    application_name,
    state,
    COUNT(*) as count
FROM pg_stat_activity
GROUP BY application_name, state
ORDER BY count DESC;

-- Check idle connections
SELECT
    pid,
    usename,
    application_name,
    state,
    NOW() - state_change as idle_duration
FROM pg_stat_activity
WHERE state = 'idle'
    AND NOW() - state_change > INTERVAL '5 minutes'
ORDER BY idle_duration DESC;
```

**Solutions:**

```sql
-- Kill idle connections
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'idle'
    AND NOW() - state_change > INTERVAL '10 minutes'
    AND pid != pg_backend_pid();

-- Increase max connections (requires restart)
ALTER SYSTEM SET max_connections = 200;
-- Then restart PostgreSQL

-- Configure connection pooling in application
-- In .env:
-- DB_POOL_SIZE=20
-- DB_MAX_OVERFLOW=10
-- DB_POOL_TIMEOUT=30
```

### Slow Queries

**Diagnostic Steps:**

```sql
-- Enable query logging
ALTER SYSTEM SET log_min_duration_statement = 1000; -- Log queries > 1 second
SELECT pg_reload_conf();

-- Find slow queries
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    max_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 20;

-- Check for missing indexes
SELECT
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan,
    seq_tup_read / seq_scan as avg_seq_tup
FROM pg_stat_user_tables
WHERE seq_scan > 0
ORDER BY seq_tup_read DESC
LIMIT 20;

-- Check for table bloat
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    n_dead_tup,
    n_live_tup,
    ROUND(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_pct
FROM pg_stat_user_tables
WHERE n_live_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 20;
```

**Solutions:**

```sql
-- Add missing indexes
CREATE INDEX CONCURRENTLY idx_subjects_gsid ON subjects(global_subject_id);
CREATE INDEX CONCURRENTLY idx_blood_gsid ON blood(global_subject_id);
CREATE INDEX CONCURRENTLY idx_dna_gsid ON dna(global_subject_id);

-- Vacuum bloated tables
VACUUM ANALYZE subjects;
VACUUM ANALYZE blood;
VACUUM ANALYZE dna;

-- For severe bloat, use VACUUM FULL (requires table lock)
VACUUM FULL subjects;

-- Update statistics
ANALYZE;
```

### Replication Lag

**Diagnostic Steps:**

```sql
-- Check replication status
SELECT
    client_addr,
    state,
    sync_state,
    pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as send_lag,
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as replay_lag
FROM pg_stat_replication;

-- Check replication slots
SELECT
    slot_name,
    slot_type,
    active,
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as lag_bytes
FROM pg_replication_slots;
```

**Solutions:**

```bash
# Check network connectivity
ping replica-host

# Check replica server load
ssh replica-host "top -bn1 | head -20"

# Increase WAL sender processes
# On primary:
ALTER SYSTEM SET max_wal_senders = 10;
SELECT pg_reload_conf();

# Check for long-running queries on replica
ssh replica-host "psql -c \"SELECT pid, query_start, query FROM pg_stat_activity WHERE state != 'idle' ORDER BY query_start;\""
```

### Database Locks

**Diagnostic Steps:**

```sql
-- Check for blocking locks
SELECT
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS blocking_statement,
    NOW() - blocked_activity.query_start as blocked_duration
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;

-- Check lock types
SELECT
    locktype,
    mode,
    COUNT(*) as count
FROM pg_locks
GROUP BY locktype, mode
ORDER BY count DESC;
```

**Solutions:**

```sql
-- Kill blocking query (use with caution)
SELECT pg_terminate_backend(blocking_pid);

-- Cancel query instead of killing
SELECT pg_cancel_backend(blocking_pid);

-- Set statement timeout to prevent long locks
ALTER DATABASE idhub SET statement_timeout = '30s';

-- Set lock timeout
ALTER DATABASE idhub SET lock_timeout = '10s';
```

---

## Pipeline Issues

### REDCap Pipeline Failures

#### API Connection Errors

**Diagnostic Steps:**

```bash
# Test REDCap API connectivity
curl -X POST https://redcap.example.edu/api/ \
  -d token=YOUR_TOKEN \
  -d content=version

# Check API token
docker exec redcap-pipeline python -c "
import os
print('API Token length:', len(os.getenv('REDCAP_API_TOKEN_GAP', '')))
print('API URL:', os.getenv('REDCAP_API_URL', 'NOT SET'))
"

# Check logs
docker logs redcap-pipeline --tail 100 | grep -i error
```

**Solutions:**

```bash
# Verify API token
# In .env file, ensure token is correct and has proper permissions

# Test with curl
curl -X POST $REDCAP_API_URL \
  -d token=$REDCAP_API_TOKEN_GAP \
  -d content=record \
  -d format=json \
  -d type=flat \
  -d records=1

# Check network connectivity
docker exec redcap-pipeline ping -c 3 redcap.example.edu

# Check SSL certificates
docker exec redcap-pipeline curl -v https://redcap.example.edu/api/
```

#### GSID Resolution Failures

**Diagnostic Steps:**

```bash
# Check GSID service connectivity
docker exec redcap-pipeline curl http://gsid-service:8000/health

# Test GSID resolution
docker exec redcap-pipeline python -c "
import requests
import os

response = requests.post(
    'http://gsid-service:8000/api/v1/resolve',
    headers={'X-API-Key': os.getenv('GSID_API_KEY')},
    json={
        'center_id': 1,
        'local_subject_id': 'TEST001',
        'identifier_type': 'mrn'
    }
)
print(response.status_code, response.json())
"

# Check logs
docker logs gsid-service | grep -i resolve
```

**Solutions:**

```bash
# Verify GSID API key
docker exec redcap-pipeline env | grep GSID_API_KEY

# Restart GSID service
docker restart gsid-service

# Check database connectivity from pipeline
docker exec redcap-pipeline python -c "
import asyncpg
import asyncio

async def test():
    conn = await asyncpg.connect(
        host='idhub_db',
        database='idhub',
        user='idhub_user',
        password='$DB_PASSWORD'
    )
    result = await conn.fetchval('SELECT COUNT(*) FROM subjects')
    print(f'Subject count: {result}')
    await conn.close()

asyncio.run(test())
"
```

#### S3 Upload Failures

**Diagnostic Steps:**

```bash
# Check AWS credentials
docker exec redcap-pipeline aws sts get-caller-identity

# Test S3 access
docker exec redcap-pipeline aws s3 ls s3://$S3_BUCKET/

# Check bucket permissions
docker exec redcap-pipeline aws s3api get-bucket-acl --bucket $S3_BUCKET
```

**Solutions:**

```bash
# Verify AWS credentials
docker exec redcap-pipeline env | grep AWS

# Test upload
echo "test" | docker exec -i redcap-pipeline aws s3 cp - s3://$S3_BUCKET/test.txt

# Check IAM permissions
aws iam get-user-policy --user-name idhub-pipeline --policy-name s3-access

# Update credentials
# In .env:
# AWS_ACCESS_KEY_ID=your_key
# AWS_SECRET_ACCESS_KEY=your_secret

docker-compose up -d redcap-pipeline
```

### Fragment Validator Issues

#### Validation Errors

**Diagnostic Steps:**

```bash
# Check validation logs
docker logs fragment-validator --tail 100

# List failed validations
aws s3 ls s3://$S3_BUCKET/validation_errors/

# Download error report
aws s3 cp s3://$S3_BUCKET/validation_errors/latest.json -
```

**Common Validation Errors:**

```python
# Missing required fields
{
    "error": "Missing required field: global_subject_id",
    "record": {...},
    "table": "blood"
}
# Solution: Check field mapping configuration

# Invalid data type
{
    "error": "Invalid date format for field: date_collected",
    "value": "2024-13-45",
    "expected": "YYYY-MM-DD"
}
# Solution: Fix data transformation in pipeline

# Duplicate records
{
    "error": "Duplicate natural key",
    "key": {"global_subject_id": "GSID123", "sample_id": "S001"},
    "table": "blood"
}
# Solution: Check for duplicate source records

# Foreign key violation
{
    "error": "Referenced subject not found",
    "global_subject_id": "GSID999",
    "table": "blood"
}
# Solution: Ensure subject exists before loading samples
```

**Solutions:**

```bash
# Re-run validation with verbose logging
docker run --rm \
  -e LOG_LEVEL=DEBUG \
  -v $(pwd)/data:/data \
  fragment-validator python main.py \
  --table-name blood \
  --input-file /data/blood.csv \
  --mapping-config config/blood_mapping.json

# Fix mapping configuration
vim fragment-validator/config/blood_mapping.json

# Validate mapping config
docker exec fragment-validator python -c "
import json
with open('config/blood_mapping.json') as f:
    config = json.load(f)
    print('Field mappings:', config.get('field_mapping'))
    print('Subject ID candidates:', config.get('subject_id_candidates'))
"
```

### Table Loader Issues

#### Load Failures

**Diagnostic Steps:**

```bash
# Check loader logs
docker logs table-loader --tail 100

# Check validation queue status
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    batch_id,
    table_name,
    status,
    COUNT(*) as count
FROM validation_queue
WHERE batch_id = 'batch_20240115_140000'
GROUP BY batch_id, table_name, status;
"

# Check for errors
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    table_name,
    error_message,
    COUNT(*) as error_count
FROM validation_queue
WHERE batch_id = 'batch_20240115_140000'
    AND status = 'error'
GROUP BY table_name, error_message;
"
```

**Solutions:**

```bash
# Retry failed records
docker exec table-loader python scripts/retry_failed.py \
  --batch-id batch_20240115_140000

# Reset batch status
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
UPDATE validation_queue
SET status = 'pending', error_message = NULL
WHERE batch_id = 'batch_20240115_140000'
    AND status = 'error';
"

# Re-run loader
docker exec table-loader python main.py \
  --batch-id batch_20240115_140000

# Check for constraint violations
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT conname, contype, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'blood'::regclass;
"
```

---

## Network & Connectivity

### Docker Network Issues

**Diagnostic Steps:**

```bash
# List networks
docker network ls

# Inspect network
docker network inspect idhub_default

# Check container connectivity
docker exec gsid-service ping -c 3 idhub_db
docker exec gsid-service nc -zv idhub_db 5432

# Check DNS resolution
docker exec gsid-service nslookup idhub_db
```

**Solutions:**

```bash
# Recreate network
docker-compose down
docker network prune -f
docker-compose up -d

# Add container to network
docker network connect idhub_default container_name

# Check firewall rules
sudo iptables -L -n | grep DOCKER

# Restart Docker daemon
sudo systemctl restart docker
```

### SSH Tunnel Issues

**Diagnostic Steps:**

```bash
# Check tunnel status
ps aux | grep ssh | grep 5432

# Test tunnel
nc -zv localhost 5432

# Check SSH connection
ssh -v bastion-host

# Check port forwarding
netstat -tuln | grep 5432
```

**Solutions:**

```bash
# Kill existing tunnels
pkill -f "ssh.*5432"

# Recreate tunnel
ssh -f -N -L 5432:db-host:5432 user@bastion-host

# Use autossh for persistent tunnel
autossh -M 0 -f -N -L 5432:db-host:5432 user@bastion-host

# Add to SSH config
cat >> ~/.ssh/config << EOF
Host bastion
    HostName bastion-host
    User your-user
    LocalForward 5432 db-host:5432
    ServerAliveInterval 60
    ServerAliveCountMax 3
EOF

ssh -f -N bastion
```

---

## Performance Issues

### High CPU Usage

**Diagnostic Steps:**

```bash
# Check container CPU usage
docker stats --no-stream

# Find CPU-intensive processes
docker exec container_name top -bn1

# Check database queries
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    pid,
    usename,
    query_start,
    state,
    LEFT(query, 100) as query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY query_start;
"
```

**Solutions:**

```bash
# Limit container CPU
# In docker-compose.yml:
# services:
#   service_name:
#     deploy:
#       resources:
#         limits:
#           cpus: '2.0'

# Kill expensive queries
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state != 'idle'
    AND NOW() - query_start > INTERVAL '5 minutes';
"

# Optimize queries
# Add indexes, rewrite queries, use EXPLAIN ANALYZE
```

### High Memory Usage

**Diagnostic Steps:**

```bash
# Check memory usage
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Check system memory
free -h

# Check for memory leaks
docker exec container_name ps aux --sort=-%mem | head -20
```

**Solutions:**

```bash
# Limit container memory
# In docker-compose.yml:
# services:
#   service_name:
#     deploy:
#       resources:
#         limits:
#           memory: 2G

# Restart container to clear memory
docker restart container_name

# Clear caches
docker exec idhub_db psql -U idhub_user -d idhub -c "
SELECT pg_stat_reset();
"

# Tune PostgreSQL memory settings
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET maintenance_work_mem = '128MB';
SELECT pg_reload_conf();
```

### Disk Space Issues

**Diagnostic Steps:**

```bash
# Check disk usage
df -h

# Check Docker disk usage
docker system df

# Find large files
du -sh /var/lib/docker/* | sort -h

# Check database size
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    pg_size_pretty(pg_database_size('idhub')) as db_size;
"
```

**Solutions:**

```bash
# Clean Docker resources
docker system prune -a --volumes -f

# Clean old logs
find /var/log -name "*.log" -mtime +30 -delete
docker exec container_name find /var/log -name "*.log" -mtime +7 -delete

# Vacuum database
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
VACUUM FULL;
"

# Archive old S3 objects
aws s3 sync s3://$S3_BUCKET/validated/ s3://$S3_BUCKET/archive/ \
  --exclude "*" --include "batch_2023*"

aws s3 rm s3://$S3_BUCKET/validated/ --recursive --exclude "*" --include "batch_2023*"
```

---

## Data Issues

### Missing GSIDs

**Diagnostic Steps:**

```sql
-- Find records without GSIDs
SELECT COUNT(*)
FROM blood
WHERE global_subject_id IS NULL;

-- Find orphaned records
SELECT b.*
FROM blood b
LEFT JOIN subjects s ON b.global_subject_id = s.global_subject_id
WHERE s.global_subject_id IS NULL
LIMIT 10;
```

**Solutions:**

```sql
-- Generate missing GSIDs
INSERT INTO subjects (global_subject_id, created_at)
SELECT DISTINCT
    generate_gsid() as global_subject_id,
    NOW() as created_at
FROM blood
WHERE global_subject_id IS NULL
ON CONFLICT DO NOTHING;

-- Update records with new GSIDs
UPDATE blood b
SET global_subject_id = s.global_subject_id
FROM subjects s
WHERE b.local_subject_id = s.local_subject_id
    AND b.center_id = s.center_id
    AND b.global_subject_id IS NULL;
```

### Duplicate Records

**Diagnostic Steps:**

```sql
-- Find duplicates in blood table
SELECT
    global_subject_id,
    sample_id,
    COUNT(*) as duplicate_count
FROM blood
GROUP BY global_subject_id, sample_id
HAVING COUNT(*) > 1;

-- Find duplicate subjects
SELECT
    center_id,
    local_subject_id,
    COUNT(*) as duplicate_count
FROM local_subject_ids
GROUP BY center_id, local_subject_id
HAVING COUNT(*) > 1;
```

**Solutions:**

```sql
-- Remove duplicates, keeping most recent

```
