# Maintenance Tasks Guide

## Overview

This guide covers routine maintenance tasks, backup procedures, database optimization, and system updates for the IDhub platform.

## Table of Contents

- [Routine Maintenance](#routine-maintenance)
- [Backup & Recovery](#backup--recovery)
- [Database Maintenance](#database-maintenance)
- [System Updates](#system-updates)
- [Security Maintenance](#security-maintenance)
- [Performance Optimization](#performance-optimization)
- [Data Archival](#data-archival)
- [Monitoring & Cleanup](#monitoring--cleanup)

---

## Routine Maintenance

### Daily Tasks

#### Health Checks

```bash:scripts/daily_health_check.sh
#!/bin/bash
# Daily health check script

set -e

LOG_DIR="/var/log/idhub/maintenance"
DATE=$(date +%Y%m%d)
LOG_FILE="$LOG_DIR/health_check_$DATE.log"

mkdir -p "$LOG_DIR"

echo "=========================================" | tee -a "$LOG_FILE"
echo "Daily Health Check - $(date)" | tee -a "$LOG_FILE"
echo "=========================================" | tee -a "$LOG_FILE"

# Check all services are running
echo -e "\n=== Service Status ===" | tee -a "$LOG_FILE"
docker ps --format "table {{.Names}}\t{{.Status}}" | tee -a "$LOG_FILE"

# Check disk space
echo -e "\n=== Disk Space ===" | tee -a "$LOG_FILE"
df -h | grep -E "Filesystem|/dev/" | tee -a "$LOG_FILE"

# Alert if disk usage > 80%
DISK_USAGE=$(df / | tail -1 | awk '{print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 80 ]; then
    echo "WARNING: Disk usage is ${DISK_USAGE}%" | tee -a "$LOG_FILE"
    # Send alert
    curl -X POST "$SLACK_WEBHOOK_URL" \
        -H 'Content-Type: application/json' \
        -d "{\"text\":\"⚠️ Disk usage alert: ${DISK_USAGE}% on $(hostname)\"}"
fi

# Check database connections
echo -e "\n=== Database Connections ===" | tee -a "$LOG_FILE"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -t -c "
SELECT
    COUNT(*) as total_connections,
    COUNT(*) FILTER (WHERE state = 'active') as active,
    COUNT(*) FILTER (WHERE state = 'idle') as idle
FROM pg_stat_activity;
" | tee -a "$LOG_FILE"

# Check for long-running queries
echo -e "\n=== Long Running Queries ===" | tee -a "$LOG_FILE"
LONG_QUERIES=$(PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -t -c "
SELECT COUNT(*)
FROM pg_stat_activity
WHERE state != 'idle'
    AND NOW() - query_start > INTERVAL '10 minutes';
")

if [ "$LONG_QUERIES" -gt 0 ]; then
    echo "WARNING: $LONG_QUERIES long-running queries detected" | tee -a "$LOG_FILE"
    PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
    SELECT
        pid,
        usename,
        NOW() - query_start as duration,
        LEFT(query, 100) as query
    FROM pg_stat_activity
    WHERE state != 'idle'
        AND NOW() - query_start > INTERVAL '10 minutes';
    " | tee -a "$LOG_FILE"
fi

# Check error logs
echo -e "\n=== Recent Errors ===" | tee -a "$LOG_FILE"
docker logs --since 24h gsid-service 2>&1 | grep -i error | tail -10 | tee -a "$LOG_FILE"
docker logs --since 24h nocodb 2>&1 | grep -i error | tail -10 | tee -a "$LOG_FILE"

# Check backup status
echo -e "\n=== Backup Status ===" | tee -a "$LOG_FILE"
LATEST_BACKUP=$(aws s3 ls s3://idhub-backups/database/ | tail -1)
echo "Latest backup: $LATEST_BACKUP" | tee -a "$LOG_FILE"

# Check SSL certificate expiry
echo -e "\n=== SSL Certificate ===" | tee -a "$LOG_FILE"
CERT_EXPIRY=$(docker exec nginx openssl x509 -in /etc/nginx/ssl/cert.pem -noout -enddate 2>/dev/null || echo "Certificate not found")
echo "$CERT_EXPIRY" | tee -a "$LOG_FILE"

echo -e "\n=========================================" | tee -a "$LOG_FILE"
echo "Health check complete" | tee -a "$LOG_FILE"
echo "=========================================" | tee -a "$LOG_FILE"

# Cleanup old logs (keep 30 days)
find "$LOG_DIR" -name "health_check_*.log" -mtime +30 -delete
```

#### Log Rotation

```bash:scripts/rotate_logs.sh
#!/bin/bash
# Rotate application logs

set -e

LOG_DIR="/var/log/idhub"
ARCHIVE_DIR="$LOG_DIR/archive"
DATE=$(date +%Y%m%d)

mkdir -p "$ARCHIVE_DIR"

# Rotate Docker container logs
for container in gsid-service nocodb redcap-pipeline fragment-validator table-loader; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        echo "Rotating logs for $container..."

        # Save current logs
        docker logs "$container" > "$ARCHIVE_DIR/${container}_${DATE}.log" 2>&1

        # Truncate logs (requires Docker restart)
        # docker restart "$container"
    fi
done

# Compress old logs
find "$ARCHIVE_DIR" -name "*.log" -mtime +1 -exec gzip {} \;

# Delete logs older than 90 days
find "$ARCHIVE_DIR" -name "*.log.gz" -mtime +90 -delete

echo "Log rotation complete"
```

### Weekly Tasks

#### Database Statistics Update

```bash:scripts/weekly_db_maintenance.sh
#!/bin/bash
# Weekly database maintenance

set -e

echo "========================================="
echo "Weekly Database Maintenance - $(date)"
echo "========================================="

# Update statistics
echo "Updating database statistics..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "ANALYZE VERBOSE;"

# Check for bloat
echo -e "\n=== Table Bloat Check ==="
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    n_dead_tup,
    n_live_tup,
    ROUND(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_pct
FROM pg_stat_user_tables
WHERE n_live_tup > 0
    AND n_dead_tup > 1000
ORDER BY dead_pct DESC
LIMIT 10;
"

# Vacuum tables with high bloat
echo -e "\n=== Vacuuming Bloated Tables ==="
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT schemaname, tablename
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 1000
            AND ROUND(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 2) > 10
    LOOP
        RAISE NOTICE 'Vacuuming %.%', r.schemaname, r.tablename;
        EXECUTE format('VACUUM ANALYZE %I.%I', r.schemaname, r.tablename);
    END LOOP;
END;
\$\$;
"

# Check index usage
echo -e "\n=== Unused Indexes ==="
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_scan
FROM pg_stat_user_indexes
WHERE idx_scan = 0
    AND indexrelname NOT LIKE '%_pkey'
    AND pg_relation_size(indexrelid) > 1048576  -- > 1MB
ORDER BY pg_relation_size(indexrelid) DESC;
"

# Reindex if needed
echo -e "\n=== Reindexing Large Tables ==="
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
REINDEX TABLE CONCURRENTLY subjects;
REINDEX TABLE CONCURRENTLY blood;
REINDEX TABLE CONCURRENTLY dna;
REINDEX TABLE CONCURRENTLY rna;
"

echo -e "\n========================================="
echo "Database maintenance complete"
echo "========================================="
```

#### Security Audit

```bash:scripts/weekly_security_audit.sh
#!/bin/bash
# Weekly security audit

set -e

AUDIT_LOG="/var/log/idhub/security_audit_$(date +%Y%m%d).log"

echo "=========================================" | tee "$AUDIT_LOG"
echo "Security Audit - $(date)" | tee -a "$AUDIT_LOG"
echo "=========================================" | tee -a "$AUDIT_LOG"

# Check for failed login attempts
echo -e "\n=== Failed Login Attempts ===" | tee -a "$AUDIT_LOG"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    username,
    COUNT(*) as failed_attempts,
    MAX(attempted_at) as last_attempt
FROM audit_log
WHERE event_type = 'failed_login'
    AND attempted_at > NOW() - INTERVAL '7 days'
GROUP BY username
HAVING COUNT(*) > 5
ORDER BY failed_attempts DESC;
" | tee -a "$AUDIT_LOG"

# Check for unauthorized API access
echo -e "\n=== Unauthorized API Access ===" | tee -a "$AUDIT_LOG"
docker logs --since 7d gsid-service 2>&1 | grep -i "unauthorized\|forbidden" | tail -20 | tee -a "$AUDIT_LOG"

# Check SSL certificate
echo -e "\n=== SSL Certificate Status ===" | tee -a "$AUDIT_LOG"
docker exec nginx openssl x509 -in /etc/nginx/ssl/cert.pem -noout -dates 2>/dev/null | tee -a "$AUDIT_LOG"

# Check for exposed secrets
echo -e "\n=== Environment Variable Check ===" | tee -a "$AUDIT_LOG"
for container in gsid-service nocodb redcap-pipeline; do
    echo "Checking $container..." | tee -a "$AUDIT_LOG"
    docker exec "$container" env | grep -E "PASSWORD|SECRET|KEY|TOKEN" | sed 's/=.*/=***/' | tee -a "$AUDIT_LOG"
done

# Check Docker image vulnerabilities
echo -e "\n=== Docker Image Vulnerabilities ===" | tee -a "$AUDIT_LOG"
docker images --format "{{.Repository}}:{{.Tag}}" | grep idhub | while read image; do
    echo "Scanning $image..." | tee -a "$AUDIT_LOG"
    # trivy image "$image" --severity HIGH,CRITICAL | tee -a "$AUDIT_LOG"
done

# Check file permissions
echo -e "\n=== Sensitive File Permissions ===" | tee -a "$AUDIT_LOG"
find . -name ".env*" -o -name "*.pem" -o -name "*.key" | while read file; do
    ls -l "$file" | tee -a "$AUDIT_LOG"
done

echo -e "\n=========================================" | tee -a "$AUDIT_LOG"
echo "Security audit complete" | tee -a "$AUDIT_LOG"
echo "=========================================" | tee -a "$AUDIT_LOG"
```

### Monthly Tasks

#### Database Backup Verification

```bash:scripts/monthly_backup_verification.sh
#!/bin/bash
# Monthly backup verification

set -e

echo "========================================="
echo "Backup Verification - $(date)"
echo "========================================="

# List recent backups
echo -e "\n=== Recent Backups ==="
aws s3 ls s3://idhub-backups/database/ --recursive | tail -10

# Download latest backup
LATEST_BACKUP=$(aws s3 ls s3://idhub-backups/database/ | tail -1 | awk '{print $4}')
echo -e "\n=== Downloading Latest Backup ==="
echo "Backup: $LATEST_BACKUP"
aws s3 cp "s3://idhub-backups/database/$LATEST_BACKUP" /tmp/

# Verify backup integrity
echo -e "\n=== Verifying Backup Integrity ==="
if gunzip -t "/tmp/$LATEST_BACKUP" 2>/dev/null; then
    echo "✓ Backup file is valid"
else
    echo "✗ Backup file is corrupted"
    exit 1
fi

# Test restore to temporary database
echo -e "\n=== Testing Restore ==="
TEMP_DB="idhub_restore_test_$(date +%Y%m%d)"

# Create temporary database
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d postgres -c "
DROP DATABASE IF EXISTS $TEMP_DB;
CREATE DATABASE $TEMP_DB;
"

# Restore backup
gunzip -c "/tmp/$LATEST_BACKUP" | PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d "$TEMP_DB"

# Verify data
echo -e "\n=== Verifying Restored Data ==="
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d "$TEMP_DB" -c "
SELECT
    'subjects' as table_name, COUNT(*) as row_count FROM subjects
UNION ALL
SELECT 'blood', COUNT(*) FROM blood
UNION ALL
SELECT 'dna', COUNT(*) FROM dna
UNION ALL
SELECT 'rna', COUNT(*) FROM rna;
"

# Cleanup
echo -e "\n=== Cleanup ==="
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d postgres -c "DROP DATABASE $TEMP_DB;"
rm "/tmp/$LATEST_BACKUP"

echo -e "\n========================================="
echo "Backup verification complete"
echo "========================================="
```

#### Performance Review

```bash:scripts/monthly_performance_review.sh
#!/bin/bash
# Monthly performance review

set -e

REPORT_FILE="/var/log/idhub/performance_report_$(date +%Y%m).log"

echo "=========================================" | tee "$REPORT_FILE"
echo "Monthly Performance Review - $(date)" | tee -a "$REPORT_FILE"
echo "=========================================" | tee -a "$REPORT_FILE"

# Database size growth
echo -e "\n=== Database Size Growth ===" | tee -a "$REPORT_FILE"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;
" | tee -a "$REPORT_FILE"

# Query performance
echo -e "\n=== Slowest Queries (Last 30 Days) ===" | tee -a "$REPORT_FILE"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    LEFT(query, 100) as query,
    calls,
    ROUND(total_exec_time::numeric, 2) as total_time_ms,
    ROUND(mean_exec_time::numeric, 2) as avg_time_ms,
    ROUND(max_exec_time::numeric, 2) as max_time_ms
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
" | tee -a "$REPORT_FILE"

# Cache hit ratio
echo -e "\n=== Cache Hit Ratio ===" | tee -a "$REPORT_FILE"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    'cache_hit_ratio' as metric,
    ROUND((blks_hit::float / NULLIF(blks_hit + blks_read, 0) * 100)::numeric, 2) as percentage
FROM pg_stat_database
WHERE datname = 'idhub';
" | tee -a "$REPORT_FILE"

# Index usage
echo -e "\n=== Index Usage Statistics ===" | tee -a "$REPORT_FILE"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    schemaname,
    tablename,
    COUNT(*) as index_count,
    SUM(idx_scan) as total_scans,
    pg_size_pretty(SUM(pg_relation_size(indexrelid))) as total_index_size
FROM pg_stat_user_indexes
GROUP BY schemaname, tablename
ORDER BY total_scans DESC
LIMIT 10;
" | tee -a "$REPORT_FILE"

# Connection statistics
echo -e "\n=== Connection Statistics ===" | tee -a "$REPORT_FILE"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "
SELECT
    application_name,
    state,
    COUNT(*) as connection_count,
    MAX(NOW() - state_change) as max_idle_time
FROM pg_stat_activity
GROUP BY application_name, state
ORDER BY connection_count DESC;
" | tee -a "$REPORT_FILE"

# Docker resource usage trends
echo -e "\n=== Docker Resource Usage ===" | tee -a "$REPORT_FILE"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}" | tee -a "$REPORT_FILE"

echo -e "\n=========================================" | tee -a "$REPORT_FILE"
echo "Performance review complete" | tee -a "$REPORT_FILE"
echo "=========================================" | tee -a "$REPORT_FILE"
```

---

## Backup & Recovery

### Automated Backup Script

```bash:scripts/backup_database.sh
#!/bin/bash
# Automated database backup script

set -e

# Configuration
BACKUP_DIR="/var/backups/idhub"
S3_BUCKET="idhub-backups"
RETENTION_DAYS=30
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="idhub_backup_${DATE}.sql.gz"

# Create backup directory
mkdir -p "$BACKUP_DIR"

echo "========================================="
echo "Database Backup - $(date)"
echo "========================================="

# Pre-backup checks
echo "Checking database connection..."
if ! PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -c "SELECT 1;" > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to database"
    exit 1
fi

# Get database size
DB_SIZE=$(PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -t -c "SELECT pg_size_pretty(pg_database_size('idhub'));")
echo "Database size: $DB_SIZE"

# Create backup
echo "Creating backup: $BACKUP_FILE"
PGPASSWORD=$DB_PASSWORD pg_dump \
    -h localhost \
    -U idhub_user \
    -d idhub \
    --format=custom \
    --compress=9 \
    --verbose \
    --file="$BACKUP_DIR/$BACKUP_FILE.custom" \
    2>&1 | tee "$BACKUP_DIR/backup_${DATE}.log"

# Also create plain SQL backup for easier inspection
PGPASSWORD=$DB_PASSWORD pg_dump \
    -h localhost \
    -U idhub_user \
    -d idhub \
    --format=plain \
    | gzip > "$BACKUP_DIR/$BACKUP_FILE"

# Verify backup
echo "Verifying backup integrity..."
if gunzip -t "$BACKUP_DIR/$BACKUP_FILE" 2>/dev/null; then
    echo "✓ Backup verified successfully"
else
    echo "✗ Backup verification failed"
    exit 1
fi

# Get backup size
BACKUP_SIZE=$(du -h "$BACKUP_DIR/$BACKUP_FILE" | cut -f1)
echo "Backup size: $BACKUP_SIZE"

# Upload to S3
echo "Uploading to S3..."
aws s3 cp "$BACKUP_DIR/$BACKUP_FILE" "s3://$S3_BUCKET/database/$BACKUP_FILE" \
    --storage-class STANDARD_IA \
    --metadata "source=automated,date=$DATE,size=$DB_SIZE"

aws s3 cp "$BACKUP_DIR/$BACKUP_FILE.custom" "s3://$S3_BUCKET/database/$BACKUP_FILE.custom" \
    --storage-class STANDARD_IA

# Verify S3 upload
if aws s3 ls "s3://$S3_BUCKET/database/$BACKUP_FILE" > /dev/null 2>&1; then
    echo "✓ Backup uploaded to S3 successfully"
else
    echo "✗ S3 upload failed"
    exit 1
fi

# Cleanup old local backups
echo "Cleaning up old local backups..."
find "$BACKUP_DIR" -name "idhub_backup_*.sql.gz*" -mtime +7 -delete
find "$BACKUP_DIR" -name "backup_*.log" -mtime +7 -delete

# Cleanup old S3 backups
echo "Cleaning up old S3 backups..."
aws s3 ls "s3://$S3_BUCKET/database/" | while read -r line; do
    createDate=$(echo "$line" | awk '{print $1" "$2}')
    createDate=$(date -d "$createDate" +%s)
    olderThan=$(date -d "$RETENTION_DAYS days ago" +%s)

    if [[ $createDate -lt $olderThan ]]; then
        fileName=$(echo "$line" | awk '{print $4}')
        if [[ $fileName != "" ]]; then
            echo "Deleting old backup: $fileName"
            aws s3 rm "s3://$S3_BUCKET/database/$fileName"
        fi
    fi
done

# Create backup manifest
cat > "$BACKUP_DIR/manifest_${DATE}.json" << EOF
{
    "backup_date": "$(date -Iseconds)",
    "database": "idhub",
    "backup_file": "$BACKUP_FILE",
    "database_size": "$DB_SIZE",
    "backup_size": "$BACKUP_SIZE",
    "s3_location": "s3://$S3_BUCKET/database/$BACKUP_FILE",
    "retention_days": $RETENTION_DAYS,
    "tables": $(PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub -t -c "SELECT json_agg(tablename) FROM pg_tables WHERE schemaname = 'public';")
}
EOF

aws s3 cp "$BACKUP_DIR/manifest_${DATE}.json" "s3://$S3_BUCKET/manifests/manifest_${DATE}.json"

echo "========================================="
echo "Backup complete: $BACKUP_FILE"
echo "S3 location: s3://$S3_BUCKET/database/$BACKUP_FILE"
echo "========================================="

# Send notification
curl -X POST "$SLACK_WEBHOOK_URL" \
    -H 'Content-Type: application/json' \
    -d "{\"text\":\"✓ Database backup completed successfully\n• File: $BACKUP_FILE\n• Size: $BACKUP_SIZE\n• Database: $DB_SIZE\"}"
```

### Restore Procedure

```bash:scripts/restore_database.sh
#!/bin/bash
# Database restore script

set -e

# Check arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <backup_file> [target_database]"
    echo "Example: $0 idhub_backup_20240115_120000.sql.gz idhub_restore"
    exit 1
fi

BACKUP_FILE=$1
TARGET_DB=${2:-idhub}
TEMP_DIR="/tmp/idhub_restore_$$"

echo "========================================="
echo "Database Restore"
echo "========================================="
echo "Backup file: $BACKUP_FILE"
echo "Target database: $TARGET_DB"
echo ""

# Confirmation
read -p "This will OVERWRITE the database '$TARGET_DB'. Continue? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Restore cancelled"
    exit 0
fi

mkdir -p "$TEMP_DIR"

# Download from S3 if needed
if [[ $BACKUP_FILE == s3://* ]]; then
    echo "Downloading backup from S3..."
    aws s3 cp "$BACKUP_FILE" "$TEMP_DIR/backup.sql.gz"
    BACKUP_FILE="$TEMP_DIR/backup.sql.gz"
elif [[ ! -f $BACKUP_FILE ]]; then
    echo "Searching for backup in S3..."
    aws s3 cp "s3://idhub-backups/database/$BACKUP_FILE" "$TEMP_DIR/backup.sql.gz"
    BACKUP_FILE="$TEMP_DIR/backup.sql.gz"
fi

# Verify backup file
echo "Verifying backup file..."
if ! gunzip -t "$BACKUP_FILE" 2>/dev/null; then
    echo "ERROR: Backup file is corrupted"
    exit 1
fi

# Stop services
echo "Stopping services..."
docker-compose stop gsid-service nocodb redcap-pipeline fragment-validator table-loader

# Terminate active connections
echo "Terminating active connections..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d postgres -c "
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '$TARGET_DB'
    AND pid != pg_backend_pid();
"

# Drop and recreate database
echo "Recreating database..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d postgres -c "
DROP DATABASE IF EXISTS ${TARGET_DB}_old;
ALTER DATABASE $TARGET_DB RENAME TO ${TARGET_DB}_old;
CREATE DATABASE $TARGET_DB;
"

# Restore backup
echo "Restoring backup..."
gunzip -c "$BACKUP_FILE" | PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d "$TARGET_DB" 2>&1 | tee "$TEMP_DIR/restore.log"

# Verify restore
echo "Verifying restore..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d "$TARGET_DB" -c "
SELECT
    'subjects' as table_name, COUNT(*) as row_count FROM subjects
UNION ALL
SELECT 'blood', COUNT(*) FROM blood
UNION ALL
SELECT 'dna', COUNT(*) FROM dna
UNION ALL
SELECT 'rna', COUNT(*) FROM rna
UNION ALL
SELECT 'local_subject_ids', COUNT(*) FROM local_subject_ids;
"

# Update statistics
echo "Updating statistics..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d "$TARGET_DB" -c "ANALYZE;"

# Restart services
echo "Restarting services..."
docker-compose up -d

# Wait for services
echo "Waiting for services to start..."
sleep 10

# Health check
echo "Running health check..."
./scripts/health_check.sh

# Cleanup
rm -rf "$TEMP_DIR"

echo "========================================="
echo "Restore complete"
echo "Old database backed up as: ${TARGET_DB}_old"
echo "To remove old database: DROP DATABASE ${TARGET_DB}_old;"
echo "========================================="

# Send notification
curl -X POST "$SLACK_WEBHOOK_URL" \
    -H 'Content-Type: application/json' \
    -d "{\"text\":\"✓ Database restored successfully\n• Backup: $BACKUP_FILE\n• Target: $TARGET_DB\"}"
```

### Point-in-Time Recovery

```bash:scripts/pitr_restore.sh
#!/bin/bash
# Point-in-time recovery script

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <base_backup> <target_time>"
    echo "Example: $0 idhub_backup_20240115_120000.sql.gz '2024-01-15 14:30:00'"
    exit 1
fi

BASE_BACKUP=$1
TARGET_TIME=$2

echo "========================================="
echo "Point-in-Time Recovery"
echo "========================================="
echo "Base backup: $BASE_BACKUP"
echo "Target time: $TARGET_TIME"
echo ""

# Restore base backup
./scripts/restore_database.sh "$BASE_BACKUP" idhub_pitr

# Apply WAL files up to target time
echo "Applying WAL files..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub_pitr -c "
SELECT pg_wal_replay_resume();
"

# Wait for recovery to target time
echo "Waiting for recovery to complete..."
while true; do
    RECOVERY_TIME=$(PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub_pitr -t -c "
    SELECT pg_last_xact_replay_timestamp();
    ")

    if [[ "$RECOVERY_TIME" > "$TARGET_TIME" ]]; then
        break
    fi

    echo "Current recovery time: $RECOVERY_TIME"
    sleep 5
done

# Promote to primary
echo "Promoting to primary..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U idhub_user -d idhub_pitr -c "
SELECT pg_promote();
"

echo "========================================="
echo "Point-in-time recovery complete"
echo "Database: idhub_pitr"
echo "Recovery time: $RECOVERY_TIME"
echo "========================================="
```

---

## Database Maintenance

### Vacuum and Analyze

```bash:scripts/vacuum_database.sh
#!/bin/bash
# Comprehensive vacuum and analyze script

set -e

echo "========================================="
echo "Database Vacuum and Analyze - $(date)"
echo "========================================="

# Check for bloat before vacuum
echo -e "\n=== Table Bloat Before Vacuum ==="
PGPASSWORD=$DB_PASSWORD ps

```
