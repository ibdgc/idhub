# Monitoring & Logging Guide

## Overview

This guide covers monitoring, logging, alerting, and observability practices for the IDhub platform.

## Table of Contents

-   [Monitoring Architecture](#monitoring-architecture)
-   [Application Monitoring](#application-monitoring)
-   [Database Monitoring](#database-monitoring)
-   [Infrastructure Monitoring](#infrastructure-monitoring)
-   [Log Management](#log-management)
-   [Alerting](#alerting)
-   [Dashboards](#dashboards)
-   [Performance Metrics](#performance-metrics)
-   [Health Checks](#health-checks)

---

## Monitoring Architecture

### Monitoring Stack

```
┌─────────────────────────────────────────────────────────────┐
│                    Monitoring Architecture                   │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Application  │  │  Database    │  │Infrastructure│      │
│  │   Metrics    │  │   Metrics    │  │   Metrics    │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │               │
│         └──────────────────┼──────────────────┘               │
│                            │                                  │
│                    ┌───────▼────────┐                        │
│                    │  Prometheus    │                        │
│                    │   (Metrics)    │                        │
│                    └───────┬────────┘                        │
│                            │                                  │
│         ┌──────────────────┼──────────────────┐              │
│         │                  │                  │               │
│  ┌──────▼───────┐  ┌──────▼───────┐  ┌──────▼───────┐      │
│  │   Grafana    │  │ AlertManager │  │    Loki      │      │
│  │(Dashboards)  │  │   (Alerts)   │  │    (Logs)    │      │
│  └───────┘  └──────┬───────┘  └───────┘      │
│                            │                                  │
│                    ┌───────▼────────┐                        │
│                    │  Notification  │                        │
│                    │   (Slack/Email)│                        │
│                    └────────────────┘                        │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Components

| Component         | Purpose                       | Port | URL                   |
| ----------------- | ----------------------------- | ---- | --------------------- |
| Prometheus        | Metrics collection & storage  | 9090 | http://localhost:9090 |
| Grafana           | Visualization & dashboards    | 3001 | http://localhost:3001 |
| Loki              | Log aggregation               | 3100 | http://localhost:3100 |
| AlertManager      | Alert routing & notifications | 9093 | http://localhost:9093 |
| Node Exporter     | System metrics                | 9100 | http://localhost:9100 |
| Postgres Exporter | Database metrics              | 9187 | http://localhost:9187 |

---

## Application Monitoring

### GSID Service Metrics

#### Prometheus Configuration

```yaml:monitoring/prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: "idhub-production"
    environment: "production"

# Alerting configuration
alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - alertmanager:9093

# Load rules
rule_files:
  - "alerts/*.yml"

# Scrape configurations
scrape_configs:
  # GSID Service
  - job_name: 'gsid-service'
    static_configs:
      - targets: ['gsid-service:8000']
    metrics_path: '/metrics'
    scrape_interval: 10s

  # REDCap Pipeline
  - job_name: 'redcap-pipeline'
    static_configs:
      - targets: ['redcap-pipeline:9091']
    metrics_path: '/metrics'

  # Fragment Validator
  - job_name: 'fragment-validator'
    static_configs:
      - targets: ['fragment-validator:9092']
    metrics_path: '/metrics'

  # Table Loader
  - job_name: 'table-loader'
    static_configs:
      - targets: ['table-loader:9093']
    metrics_path: '/metrics'

  # PostgreSQL
  - job_name: 'postgresql'
    static_configs:
      - targets: ['postgres-exporter:9187']

  # Node metrics
  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']

  # NocoDB
  - job_name: 'nocodb'
    static_configs:
      - targets: ['nocodb:8080']
    metrics_path: '/api/v1/health'

  # Redis
  - job_name: 'redis'
    static_configs:
      - targets: ['redis-exporter:9121']
```

#### Application Metrics Instrumentation

```python:gsid-service/core/metrics.py
"""Prometheus metrics for GSID service"""

from prometheus_client import Counter, Histogram, Gauge, Info
import time
from functools import wraps

# Application info
app_info = Info('gsid_service', 'GSID Service information')
app_info.info({
    'version': '1.0.0',
    'environment': 'production'
})

# Request metrics
request_count = Counter(
    'gsid_requests_total',
    'Total number of requests',
    ['method', 'endpoint', 'status']
)

request_duration = Histogram(
    'gsid_request_duration_seconds',
    'Request duration in seconds',
    ['method', 'endpoint'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# GSID generation metrics
gsid_generated = Counter(
    'gsid_generated_total',
    'Total number of GSIDs generated',
    ['source']
)

gsid_generation_duration = Histogram(
    'gsid_generation_duration_seconds',
    'GSID generation duration',
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
)

# Subject lookup metrics
subject_lookups = Counter(
    'gsid_subject_lookups_total',
    'Total subject lookups',
    ['result']  # hit, miss, error
)

subject_lookup_duration = Histogram(
    'gsid_subject_lookup_duration_seconds',
    'Subject lookup duration',
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0]
)

# Database metrics
db_connections = Gauge(
    'gsid_db_connections',
    'Number of database connections',
    ['state']  # active, idle
)

db_query_duration = Histogram(
    'gsid_db_query_duration_seconds',
    'Database query duration',
    ['query_type'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
)

# Cache metrics
cache_hits = Counter(
    'gsid_cache_hits_total',
    'Total cache hits',
    ['cache_type']
)

cache_misses = Counter(
    'gsid_cache_misses_total',
    'Total cache misses',
    ['cache_type']
)

# Error metrics
errors = Counter(
    'gsid_errors_total',
    'Total errors',
    ['error_type', 'endpoint']
)

# Active requests
active_requests = Gauge(
    'gsid_active_requests',
    'Number of active requests'
)


def track_request_metrics(func):
    """Decorator to track request metrics"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        method = kwargs.get('method', 'UNKNOWN')
        endpoint = kwargs.get('endpoint', 'UNKNOWN')

        active_requests.inc()
        start_time = time.time()

        try:
            result = await func(*args, **kwargs)
            status = kwargs.get('status', 200)
            request_count.labels(method=method, endpoint=endpoint, status=status).inc()
            return result
        except Exception as e:
            errors.labels(error_type=type(e).__name__, endpoint=endpoint).inc()
            raise
        finally:
            duration = time.time() - start_time
            request_duration.labels(method=method, endpoint=endpoint).observe(duration)
            active_requests.dec()

    return wrapper


def track_gsid_generation(source: str):
    """Track GSID generation"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                gsid_generated.labels(source=source).inc()
                return result
            finally:
                duration = time.time() - start_time
                gsid_generation_duration.observe(duration)
        return wrapper
    return decorator


def track_subject_lookup(func):
    """Track subject lookup metrics"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            if result:
                subject_lookups.labels(result='hit').inc()
            else:
                subject_lookups.labels(result='miss').inc()
            return result
        except Exception as e:
            subject_lookups.labels(result='error').inc()
            raise
        finally:
            duration = time.time() - start_time
            subject_lookup_duration.observe(duration)
    return wrapper


def track_db_query(query_type: str):
    """Track database query metrics"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                duration = time.time() - start_time
                db_query_duration.labels(query_type=query_type).observe(duration)
        return wrapper
    return decorator
```

#### Metrics Endpoint

```python:gsid-service/api/metrics.py
"""Metrics endpoint for Prometheus"""

from fastapi import APIRouter
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

router = APIRouter()


@router.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
```

### REDCap Pipeline Metrics

```python:redcap-pipeline/core/metrics.py
"""Metrics for REDCap pipeline"""

from prometheus_client import Counter, Histogram, Gauge, Summary

# Pipeline execution metrics
pipeline_runs = Counter(
    'redcap_pipeline_runs_total',
    'Total pipeline runs',
    ['project', 'status']  # success, failure, partial
)

pipeline_duration = Histogram(
    'redcap_pipeline_duration_seconds',
    'Pipeline execution duration',
    ['project'],
    buckets=[60, 300, 600, 1800, 3600, 7200]  # 1min to 2hrs
)

# Record processing metrics
records_fetched = Counter(
    'redcap_records_fetched_total',
    'Total records fetched from REDCap',
    ['project']
)

records_processed = Counter(
    'redcap_records_processed_total',
    'Total records processed',
    ['project', 'status']  # success, error, skipped
)

records_uploaded = Counter(
    'redcap_records_uploaded_total',
    'Total records uploaded to S3',
    ['project', 'table']
)

# API metrics
redcap_api_calls = Counter(
    'redcap_api_calls_total',
    'Total REDCap API calls',
    ['project', 'status']
)

redcap_api_duration = Histogram(
    'redcap_api_call_duration_seconds',
    'REDCap API call duration',
    ['project'],
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
)

# GSID resolution metrics
gsid_resolutions = Counter(
    'redcap_gsid_resolutions_total',
    'Total GSID resolutions',
    ['project', 'result']  # found, created, error
)

# Batch metrics
batch_size = Gauge(
    'redcap_current_batch_size',
    'Current batch size',
    ['project']
)

batches_processed = Counter(
    'redcap_batches_processed_total',
    'Total batches processed',
    ['project']
)

# Error metrics
validation_errors = Counter(
    'redcap_validation_errors_total',
    'Total validation errors',
    ['project', 'error_type']
)

transformation_errors = Counter(
    'redcap_transformation_errors_total',
    'Total transformation errors',
    ['project', 'field']
)
```

---

## Database Monitoring

### PostgreSQL Metrics

#### Postgres Exporter Configuration

```yaml:monitoring/postgres-exporter/queries.yml
# Custom PostgreSQL queries for monitoring

# Table sizes
pg_table_size:
  query: |
    SELECT
      schemaname,
      tablename,
      pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
    FROM pg_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
  metrics:
    - schemaname:
        usage: "LABEL"
        description: "Schema name"
    - tablename:
        usage: "LABEL"
        description: "Table name"
    - size_bytes:
        usage: "GAUGE"
        description: "Table size in bytes"

# Row counts
pg_table_rows:
  query: |
    SELECT
      schemaname,
      tablename,
      n_live_tup as row_count
    FROM pg_stat_user_tables
  metrics:
    - schemaname:
        usage: "LABEL"
    - tablename:
        usage: "LABEL"
    - row_count:
        usage: "GAUGE"
        description: "Approximate row count"

# Index usage
pg_index_usage:
  query: |
    SELECT
      schemaname,
      tablename,
      indexname,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch
    FROM pg_stat_user_indexes
  metrics:
    - schemaname:
        usage: "LABEL"
    - tablename:
        usage: "LABEL"
    - indexname:
        usage: "LABEL"
    - idx_scan:
        usage: "COUNTER"
        description: "Number of index scans"
    - idx_tup_read:
        usage: "COUNTER"
        description: "Tuples read from index"
    - idx_tup_fetch:
        usage: "COUNTER"
        description: "Tuples fetched from table"

# Slow queries
pg_slow_queries:
  query: |
    SELECT
      COUNT(*) as slow_query_count
    FROM pg_stat_statements
    WHERE mean_exec_time > 1000  # queries slower than 1 second
  metrics:
    - slow_query_count:
        usage: "GAUGE"
        description: "Number of slow queries"

# Connection states
pg_connection_states:
  query: |
    SELECT
      state,
      COUNT(*) as connection_count
    FROM pg_stat_activity
    WHERE state IS NOT NULL
    GROUP BY state
  metrics:
    - state:
        usage: "LABEL"
    - connection_count:
        usage: "GAUGE"
        description: "Connections by state"

# Database size
pg_database_size:
  query: |
    SELECT
      datname,
      pg_database_size(datname) as size_bytes
    FROM pg_database
    WHERE datname NOT IN ('template0', 'template1')
  metrics:
    - datname:
        usage: "LABEL"
    - size_bytes:
        usage: "GAUGE"
        description: "Database size in bytes"

# Replication lag
pg_replication_lag:
  query: |
    SELECT
      client_addr,
      EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())) as lag_seconds
    FROM pg_stat_replication
  metrics:
    - client_addr:
        usage: "LABEL"
    - lag_seconds:
        usage: "GAUGE"
        description: "Replication lag in seconds"

# Vacuum and analyze stats
pg_vacuum_stats:
  query: |
    SELECT
      schemaname,
      tablename,
      last_vacuum,
      last_autovacuum,
      last_analyze,
      last_autoanalyze,
      n_dead_tup
    FROM pg_stat_user_tables
  metrics:
    - schemaname:
        usage: "LABEL"
    - tablename:
        usage: "LABEL"
    - n_dead_tup:
        usage: "GAUGE"
        description: "Dead tuples count"

# Lock monitoring
pg_locks:
  query: |
    SELECT
      mode,
      COUNT(*) as lock_count
    FROM pg_locks
    GROUP BY mode
  metrics:
    - mode:
        usage: "LABEL"
    - lock_count:
        usage: "GAUGE"
        description: "Locks by mode"

# Cache hit ratio
pg_cache_hit_ratio:
  query: |
    SELECT
      'cache_hit_ratio' as metric,
      CASE
        WHEN (blks_hit + blks_read) = 0 THEN 0
        ELSE (blks_hit::float / (blks_hit + blks_read))
      END as ratio
    FROM pg_stat_database
    WHERE datname = current_database()
  metrics:
    - metric:
        usage: "LABEL"
    - ratio:
        usage: "GAUGE"
        description: "Cache hit ratio (0-1)"
```

### Database Performance Queries

```sql:monitoring/queries/performance.sql
-- Active queries
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    query_start,
    NOW() - query_start as duration,
    LEFT(query, 100) as query_preview
FROM pg_stat_activity
WHERE state != 'idle'
    AND query NOT LIKE '%pg_stat_activity%'
ORDER BY query_start;

-- Long-running queries (> 5 minutes)
SELECT
    pid,
    usename,
    NOW() - query_start as duration,
    state,
    query
FROM pg_stat_activity
WHERE state != 'idle'
    AND NOW() - query_start > INTERVAL '5 minutes'
ORDER BY duration DESC;

-- Blocking queries
SELECT
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS blocking_statement
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;

-- Table bloat
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    n_dead_tup,
    n_live_tup,
    ROUND(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_tuple_percent
FROM pg_stat_user_tables
WHERE n_live_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 20;

-- Unused indexes
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE idx_scan = 0
    AND indexrelname NOT LIKE '%_pkey'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Most expensive queries
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    max_exec_time,
    stddev_exec_time,
    rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;

-- Connection pool status
SELECT
    state,
    COUNT(*) as count,
    MAX(NOW() - state_change) as max_age
FROM pg_stat_activity
GROUP BY state;

-- Database size growth
SELECT
    datname,
    pg_size_pretty(pg_database_size(datname)) as size,
    pg_database_size(datname) as size_bytes
FROM pg_database
WHERE datname NOT IN ('template0', 'template1')
ORDER BY pg_database_size(datname) DESC;
```

---

## Infrastructure Monitoring

### Docker Container Metrics

```yaml:monitoring/docker-compose.monitoring.yml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - ./prometheus/alerts:/etc/prometheus/alerts
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=30d'
      - '--web.console.libraries=/usr/share/prometheus/console_libraries'
      - '--web.console.templates=/usr/share/prometheus/consoles'
    ports:
      - "9090:9090"
    networks:
      - monitoring
    restart: unless-stopped

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/dashboards:/var/lib/grafana/dashboards
      - grafana_data:/var/lib/grafana
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD}
      - GF_USERS_ALLOW_SIGN_UP=false
      - GF_SERVER_ROOT_URL=https://monitoring.idhub.ibdgc.org
      - GF_INSTALL_PLUGINS=grafana-piechart-panel
    ports:
      - "3001:3000"
    networks:
      - monitoring
    restart: unless-stopped
    depends_on:
      - prometheus

  loki:
    image: grafana/loki:latest
    container_name: loki
    volumes:
      - ./loki/loki-config.yml:/etc/loki/local-config.yaml
      - loki_data:/loki
    ports:
      - "3100:3100"
    command: -config.file=/etc/loki/local-config.yaml
    networks:
      - monitoring
    restart: unless-stopped

  promtail:
    image: grafana/promtail:latest
    container_name: promtail
    volumes:
      - ./promtail/promtail-config.yml:/etc/promtail/config.yml
      - /var/log:/var/log:ro
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
    command: -config.file=/etc/promtail/config.yml
    networks:
      - monitoring
    restart: unless-stopped
    depends_on:
      - loki

  alertmanager:
    image: prom/alertmanager:latest
    container_name: alertmanager
    volumes:
      - ./alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml
      - alertmanager_data:/alertmanager
    command:
      - '--config.file=/etc/alertmanager/alertmanager.yml'
      - '--storage.path=/alertmanager'
    ports:
      - "9093:9093"
    networks:
      - monitoring
    restart: unless-stopped

  node-exporter:
    image: prom/node-exporter:latest
    container_name: node-exporter
    command:
      - '--path.procfs=/host/proc'
      - '--path.sysfs=/host/sys'
      - '--path.rootfs=/rootfs'
      - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    ports:
      - "9100:9100"
    networks:
      - monitoring
    restart: unless-stopped

  postgres-exporter:
    image: prometheuscommunity/postgres-exporter:latest
    container_name: postgres-exporter
    environment:
      DATA_SOURCE_NAME: "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5432/${DB_NAME}?sslmode=disable"
      PG_EXPORTER_EXTEND_QUERY_PATH: "/etc/postgres_exporter/queries.yml"
    volumes:
      - ./postgres-exporter/queries.yml:/etc/postgres_exporter/queries.yml:ro
    ports:
      - "9187:9187"
    networks:
      - monitoring
    restart: unless-stopped

  redis-exporter:
    image: oliver006/redis_exporter:latest
    container_name: redis-exporter
    environment:
      REDIS_ADDR: "redis:6379"
      REDIS_PASSWORD: "${REDIS_PASSWORD}"
    ports:
      - "9121:9121"
    networks:
      - monitoring
    restart: unless-stopped

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:latest
    container_name: cadvisor
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
      - /dev/disk/:/dev/disk:ro
    ports:
      - "8080:8080"
    networks:
      - monitoring
    restart: unless-stopped
    privileged: true
    devices:
      - /dev/kmsg

networks:
  monitoring:
    driver: bridge

volumes:
  prometheus_data:
  grafana_data:
  loki_data:
  alertmanager_data:
```

### System Metrics Collection

```bash:monitoring/scripts/collect_metrics.sh
#!/bin/bash
# System metrics collection script

set -e

METRICS_DIR="/var/log/idhub/metrics"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$METRICS_DIR"

# CPU usage
echo "=== CPU Usage ===" > "$METRICS_DIR/cpu_$TIMESTAMP.txt"
top -bn1 | head -20 >> "$METRICS_DIR/cpu_$TIMESTAMP.txt"

# Memory usage
echo "=== Memory Usage ===" > "$METRICS_DIR/memory_$TIMESTAMP.txt"
free -h >> "$METRICS_DIR/memory_$TIMESTAMP.txt"
echo "" >> "$METRICS_DIR/memory_$TIMESTAMP.txt"
ps aux --sort=-%mem | head -20 >> "$METRICS_DIR/memory_$TIMESTAMP.txt"

# Disk usage
echo "=== Disk Usage ===" > "$METRICS_DIR/disk_$TIMESTAMP.txt"
df -h >> "$METRICS_DIR/disk_$TIMESTAMP.txt"
echo "" >> "$METRICS_DIR/disk_$TIMESTAMP.txt"
du -sh /var/lib/docker/* 2>/dev/null >> "$METRICS_DIR/disk_$TIMESTAMP.txt" || true

# Docker stats
echo "=== Docker Stats ===" > "$METRICS_DIR/docker_$TIMESTAMP.txt"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" >> "$METRICS_DIR/docker_$TIMESTAMP.txt"

# Network connections
echo "=== Network Connections ===" > "$METRICS_DIR/network_$TIMESTAMP.txt"
netstat -an | grep ESTABLISHED | wc -l >> "$METRICS_DIR/network_$TIMESTAMP.txt"
ss -s >> "$METRICS_DIR/network_$TIMESTAMP.txt"

# Cleanup old metrics (keep last 7 days)
find "$METRICS_DIR" -name "*.txt" -mtime +7 -delete

echo "Metrics collected: $METRICS_DIR/*_$TIMESTAMP.txt"
```

---

## Log Management

### Loki Configuration

```yaml:monitoring/loki/loki-config.yml
auth_enabled: false

server:
  http_listen_port: 3100
  grpc_listen_port: 9096

common:
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: 2023-01-01
      store: boltdb-shipper
      object_store: filesystem
      schema: v11
      index:
        prefix: index_
        period: 24h

ruler:
  alertmanager_url: http://alertmanager:9093

limits_config:
  retention_period: 744h  # 31 days
  ingestion_rate_mb: 10
  ingestion_burst_size_mb: 20
  max_query_length: 721h  # 30 days
  max_query_parallelism: 32

chunk_store_config:
  max_look_back_period: 744h

table_manager:
  retention_deletes_enabled: true
  retention_period: 744h
```
