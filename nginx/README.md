# Nginx Reverse Proxy

Nginx configuration for the IDhub platform, providing SSL termination, reverse proxy, and load balancing for all services.

## Overview

The Nginx service acts as the entry point for all HTTP/HTTPS traffic to the IDhub platform. It handles SSL/TLS termination, routes requests to appropriate backend services, and provides security headers and rate limiting.

### Key Features

- **SSL/TLS Termination**: Automatic HTTPS with Let's Encrypt certificates
- **Reverse Proxy**: Routes traffic to NocoDB and GSID service
- **Environment-Specific Configs**: Separate configurations for QA and Production
- **HTTP/2 Support**: Modern protocol support for improved performance
- **Security Headers**: HSTS, X-Frame-Options, CSP, etc.
- **Health Check Endpoints**: Public health checks for monitoring
- **Connection Pooling**: Keepalive connections to backends
- **Gzip Compression**: Automatic compression for text-based responses
- **Rate Limiting**: Protection against abuse (optional)
- **Access Logging**: Detailed request logging for audit and debugging

## Architecture

```
nginx/
├── nginx.conf                      # Main Nginx configuration
├── conf.d/
│   ├── qa/                         # QA environment configs
│   │   ├── nocodb.conf            # NocoDB reverse proxy (qa.idhub.ibdgc.org)
│   │   └── gsid-api.conf          # GSID API reverse proxy (api.qa.idhub.ibdgc.org)
│   └── prod/                       # Production environment configs
│       ├── nocodb.conf            # NocoDB reverse proxy (idhub.ibdgc.org)
│       └── gsid-api.conf          # GSID API reverse proxy (api.idhub.ibdgc.org)
└── README.md                       # This file
```

## Request Flow

```
┌─────────────────┐
│  Internet       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Nginx :80/443  │ ← SSL Termination, Routing
└────────┬────────┘
         │
         ├─────────────────────────────────┐
         │                                 │
         ▼                                 ▼
┌─────────────────┐              ┌─────────────────┐
│  NocoDB :8080   │              │ GSID API :8000  │
│  (nocodb)       │              │ (gsid-service)  │
└─────────────────┘              └─────────────────┘
```

## Domain Mapping

### Production Environment

| Domain                | Backend Service     | Purpose              |
| --------------------- | ------------------- | -------------------- |
| `idhub.ibdgc.org`     | `nocodb:8080`       | NocoDB web interface |
| `api.idhub.ibdgc.org` | `gsid-service:8000` | GSID REST API        |

### QA Environment

| Domain                   | Backend Service     | Purpose                   |
| ------------------------ | ------------------- | ------------------------- |
| `qa.idhub.ibdgc.org`     | `nocodb:8080`       | NocoDB web interface (QA) |
| `api.qa.idhub.ibdgc.org` | `gsid-service:8000` | GSID REST API (QA)        |

## Configuration Files

### Main Configuration (`nginx.conf`)

Global Nginx settings applied to all virtual hosts.

```nginx
user nginx;
worker_processes auto;                    # Auto-detect CPU cores
error_log /var/log/nginx/error.log warn;
pid /var/run/nginx.pid;

events {
    worker_connections 1024;              # Max connections per worker
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    # Logging format
    log_format main '$remote_addr - $remote_user [$time_local] "$request" '
                    '$status $body_bytes_sent "$http_referer" '
                    '"$http_user_agent" "$http_x_forwarded_for"';

    access_log /var/log/nginx/access.log main;

    # Performance settings
    sendfile on;                          # Efficient file transfers
    tcp_nopush on;                        # Send headers in one packet
    tcp_nodelay on;                       # Don't buffer data
    keepalive_timeout 65;                 # Keep connections alive
    types_hash_max_size 2048;
    client_max_body_size 50M;             # Max upload size

    # Compression
    gzip on;
    gzip_vary on;
    gzip_proxied any;
    gzip_comp_level 6;
    gzip_types text/plain text/css text/xml text/javascript
               application/json application/javascript application/xml+rss;

    # Include virtual host configs
    include /etc/nginx/conf.d/*.conf;
}
```

**Key Settings**:

- `worker_processes auto`: Automatically scales to available CPU cores
- `client_max_body_size 50M`: Allows large file uploads (adjust as needed)
- `gzip on`: Compresses responses to reduce bandwidth
- `keepalive_timeout 65`: Reuses connections for better performance

### NocoDB Configuration

**Production** (`conf.d/prod/nocodb.conf`):

```nginx
upstream nocodb_backend {
    server nocodb:8080;
    keepalive 32;                         # Connection pool
}

# HTTP → HTTPS redirect
server {
    listen 80;
    server_name idhub.ibdgc.org;

    # Let's Encrypt challenge
    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }

    # Redirect all other traffic to HTTPS
    location / {
        return 301 https://$host$request_uri;
    }
}

# HTTPS server
server {
    listen 443 ssl;
    http2 on;
    server_name idhub.ibdgc.org;

    # SSL certificates (Let's Encrypt)
    ssl_certificate /etc/letsencrypt/live/idhub.ibdgc.org/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/idhub.ibdgc.org/privkey.pem;

    # SSL session cache
    ssl_session_timeout 1d;
    ssl_session_cache shared:SSL:50m;
    ssl_session_tickets off;

    # Modern SSL configuration
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384';
    ssl_prefer_server_ciphers off;

    # Security headers
    add_header Strict-Transport-Security "max-age=63072000" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;

    # Proxy to NocoDB
    location / {
        proxy_pass http://nocodb_backend;
        proxy_http_version 1.1;

        # Headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;

        # Buffering
        proxy_buffering off;
        proxy_request_buffering off;
    }
}
```

**QA** (`conf.d/qa/nocodb.conf`):

Same structure as production, but with:

- `server_name qa.idhub.ibdgc.org;`
- `ssl_certificate /etc/letsencrypt/live/qa.idhub.ibdgc.org/fullchain.pem;`

### GSID API Configuration

**Production** (`conf.d/prod/gsid-api.conf`):

```nginx
upstream gsid_backend {
    server gsid-service:8000;
    keepalive 32;
}

# HTTP → HTTPS redirect
server {
    listen 80;
    server_name api.idhub.ibdgc.org;

    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }

    location / {
        return 301 https://$host$request_uri;
    }
}

# HTTPS server
server {
    listen 443 ssl;
    http2 on;
    server_name api.idhub.ibdgc.org;

    ssl_certificate /etc/letsencrypt/live/idhub.ibdgc.org/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/idhub.ibdgc.org/privkey.pem;

    ssl_session_timeout 1d;
    ssl_session_cache shared:SSL:50m;
    ssl_session_tickets off;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384';
    ssl_prefer_server_ciphers off;

    add_header Strict-Transport-Security "max-age=63072000" always;

    # Public health check (no auth required)
    location /health {
        proxy_pass http://gsid_backend/health;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        access_log off;                   # Don't log health checks
    }

    # API endpoints (require authentication)
    location / {
        proxy_pass http://gsid_backend;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # API-specific timeouts
        proxy_connect_timeout 30s;
        proxy_send_timeout 30s;
        proxy_read_timeout 30s;

        # Rate limiting (optional)
        # limit_req zone=api_limit burst=20 nodelay;
    }
}
```

**QA** (`conf.d/qa/gsid-api.conf`):

Same structure as production, but with:

- `server_name api.qa.idhub.ibdgc.org;`
- `ssl_certificate /etc/letsencrypt/live/qa.idhub.ibdgc.org/fullchain.pem;`

## SSL/TLS Configuration

### Let's Encrypt Certificates

Certificates are managed via Certbot and mounted into the Nginx container.

**Certificate Locations**:

```
/etc/letsencrypt/live/idhub.ibdgc.org/
├── fullchain.pem          # Certificate + intermediate chain
├── privkey.pem            # Private key
├── cert.pem               # Certificate only
└── chain.pem              # Intermediate chain only

/etc/letsencrypt/live/qa.idhub.ibdgc.org/
├── fullchain.pem
├── privkey.pem
├── cert.pem
└── chain.pem
```

**Docker Compose Volume Mount**:

```yaml
services:
  nginx:
    volumes:
      - /etc/letsencrypt:/etc/letsencrypt:ro
      - /var/www/certbot:/var/www/certbot:ro
```

### SSL Best Practices

**Protocols**:

- ✅ TLSv1.2, TLSv1.3
- ❌ SSLv3, TLSv1.0, TLSv1.1 (deprecated/insecure)

**Cipher Suites**:

```nginx
ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384';
```

- Forward secrecy (ECDHE)
- Authenticated encryption (GCM)
- Modern algorithms (AES-128/256)

**Session Resumption**:

```nginx
ssl_session_timeout 1d;
ssl_session_cache shared:SSL:50m;
ssl_session_tickets off;
```

- Improves performance for returning clients
- Shared cache across workers
- Session tickets disabled for better security

**HSTS (HTTP Strict Transport Security)**:

```nginx
add_header Strict-Transport-Security "max-age=63072000" always;
```

- Forces HTTPS for 2 years
- Prevents downgrade attacks
- Preload eligible (can submit to browsers)

### Certificate Renewal

Certificates auto-renew via Certbot cron job on the host:

```bash
# Check certificate expiry
certbot certificates

# Manual renewal (if needed)
certbot renew --dry-run
certbot renew

# Reload Nginx after renewal
docker-compose exec nginx nginx -s reload
```

**Automated Renewal** (crontab on host):

```cron
# Renew certificates daily at 2 AM
0 2 * * * certbot renew --quiet --post-hook "docker-compose -f /path/to/idhub/docker-compose.yml exec nginx nginx -s reload"
```

## Security Headers

### Implemented Headers

| Header                      | Value              | Purpose                 |
| --------------------------- | ------------------ | ----------------------- |
| `Strict-Transport-Security` | `max-age=63072000` | Force HTTPS for 2 years |
| `X-Frame-Options`           | `SAMEORIGIN`       | Prevent clickjacking    |
| `X-Content-Type-Options`    | `nosniff`          | Prevent MIME sniffing   |
| `X-XSS-Protection`          | `1; mode=block`    | Enable XSS filter       |

### Optional Headers

**Content Security Policy** (add if needed):

```nginx
add_header Content-Security-Policy "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline';" always;
```

**Referrer Policy**:

```nginx
add_header Referrer-Policy "strict-origin-when-cross-origin" always;
```

**Permissions Policy**:

```nginx
add_header Permissions-Policy "geolocation=(), microphone=(), camera=()" always;
```

## Rate Limiting (Optional)

Protect against abuse and DDoS attacks.

### Configuration

Add to `http` block in `nginx.conf`:

```nginx
http {
    # Define rate limit zones
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;
    limit_req_zone $binary_remote_addr zone=web_limit:10m rate=30r/s;

    # Connection limits
    limit_conn_zone $binary_remote_addr zone=conn_limit:10m;

    # ... rest of config
}
```

### Apply to Locations

**API Rate Limiting**:

```nginx
location /api/ {
    limit_req zone=api_limit burst=20 nodelay;
    limit_conn conn_limit 10;

    proxy_pass http://gsid_backend;
    # ... rest of proxy config
}
```

**Web Rate Limiting**:

```nginx
location / {
    limit_req zone=web_limit burst=50 nodelay;
    limit_conn conn_limit 20;

    proxy_pass http://nocodb_backend;
    # ... rest of proxy config
}
```

**Parameters**:

- `rate=10r/s`: 10 requests per second average
- `burst=20`: Allow bursts up to 20 requests
- `nodelay`: Process burst requests immediately
- `limit_conn 10`: Max 10 concurrent connections per IP

### Whitelist IPs

Exclude trusted IPs from rate limiting:

```nginx
geo $limit {
    default 1;
    10.0.0.0/8 0;           # Internal network
    192.168.1.100 0;        # Admin IP
}

map $limit $limit_key {
    0 "";
    1 $binary_remote_addr;
}

limit_req_zone $limit_key zone=api_limit:10m rate=10r/s;
```

## Logging

### Access Logs

**Format**:

```nginx
log_format main '$remote_addr - $remote_user [$time_local] "$request" '
                '$status $body_bytes_sent "$http_referer" '
                '"$http_user_agent" "$http_x_forwarded_for"';
```

**Example Log Entry**:

```
192.168.1.100 - - [15/Jan/2024:10:30:45 +0000] "GET /api/health HTTP/2.0" 200 15 "-" "curl/7.68.0" "-"
```

**Log Locations**:

- Access: `/var/log/nginx/access.log`
- Error: `/var/log/nginx/error.log`

### Disable Logging for Health Checks

```nginx
location /health {
    access_log off;
    proxy_pass http://backend;
}
```

### Custom Log Format for APIs

```nginx
log_format api_log '$remote_addr - $remote_user [$time_local] '
                   '"$request" $status $body_bytes_sent '
                   '"$http_referer" "$http_user_agent" '
                   'rt=$request_time uct="$upstream_connect_time" '
                   'uht="$upstream_header_time" urt="$upstream_response_time"';

server {
    access_log /var/log/nginx/api_access.log api_log;
    # ...
}
```

### Log Rotation

Logs are automatically rotated by Docker or logrotate on the host.

**Logrotate Configuration** (`/etc/logrotate.d/nginx`):

```
/var/log/nginx/*.log {
    daily
    missingok
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 nginx adm
    sharedscripts
    postrotate
        docker-compose -f /path/to/idhub/docker-compose.yml exec nginx nginx -s reopen
    endscript
}
```

## Monitoring

### Health Checks

**NocoDB Health**:

```bash
curl -I https://idhub.ibdgc.org/api/v1/health
# Expected: HTTP/2 200
```

**GSID API Health**:

```bash
curl https://api.idhub.ibdgc.org/health
# Expected: {"status": "healthy"}
```

### Nginx Status

Enable stub_status module for monitoring:

```nginx
server {
    listen 8080;
    server_name localhost;

    location /nginx_status {
        stub_status;
        access_log off;
        allow 127.0.0.1;
        deny all;
    }
}
```

**Query Status**:

```bash
docker-compose exec nginx curl http://localhost:8080/nginx_status

# Output:
# Active connections: 5
# server accepts handled requests
#  1000 1000 5000
# Reading: 0 Writing: 1 Waiting: 4
```

## Deployment

### Environment Selection

The `docker-compose.yml` uses the `ENVIRONMENT` variable to select configs:

```yaml
services:
  nginx:
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./nginx/conf.d/${ENVIRONMENT:-qa}:/etc/nginx/conf.d:ro
```

**Deploy QA**:

```bash
export ENVIRONMENT=qa
docker-compose up -d nginx
```

**Deploy Production**:

```bash
export ENVIRONMENT=prod
docker-compose up -d nginx
```

### Initial Setup

1. **Obtain SSL Certificates**:

   ```bash
   # Install Certbot on host
   sudo apt-get install certbot

   # Obtain certificates (interactive)
   sudo certbot certonly --standalone -d idhub.ibdgc.org -d api.idhub.ibdgc.org
   sudo certbot certonly --standalone -d qa.idhub.ibdgc.org -d api.qa.idhub.ibdgc.org
   ```

2. **Start Nginx**:

   ```bash
   docker-compose up -d nginx
   ```

3. **Verify Configuration**:

   ```bash
   docker-compose exec nginx nginx -t
   ```

4. **Check Logs**:
   ```bash
   docker-compose logs nginx -f
   ```

### Configuration Reload

After changing configuration files:

```bash
# Test configuration
docker-compose exec nginx nginx -t

# Reload (graceful, no downtime)
docker-compose exec nginx nginx -s reload

# Or restart container
docker-compose restart nginx
```

### SSL Certificate Renewal

```bash
# Dry run
sudo certbot renew --dry-run

# Actual renewal
sudo certbot renew

# Reload Nginx
docker-compose exec nginx nginx -s reload
```

## Troubleshooting

### Common Issues

**Issue**: `502 Bad Gateway`

```bash
# Check backend service is running
docker-compose ps nocodb gsid-service

# Check backend logs
docker-compose logs nocodb
docker-compose logs gsid-service

# Check Nginx can reach backend
docker-compose exec nginx ping nocodb
docker-compose exec nginx ping gsid-service

# Check Nginx error log
docker-compose logs nginx | grep error
```

**Issue**: `SSL certificate problem`

```bash
# Verify certificate files exist
docker-compose exec nginx ls -la /etc/letsencrypt/live/idhub.ibdgc.org/

# Check certificate expiry
docker-compose exec nginx openssl x509 -in /etc/letsencrypt/live/idhub.ibdgc.org/fullchain.pem -noout -dates

# Renew if expired
sudo certbot renew
docker-compose exec nginx nginx -s reload
```

**Issue**: `Connection timeout`

```bash
# Check proxy timeouts in config
docker-compose exec nginx grep -r "proxy_.*_timeout" /etc/nginx/

# Increase timeouts if needed
proxy_connect_timeout 120s;
proxy_send_timeout 120s;
proxy_read_timeout 120s;
```

**Issue**: `413 Request Entity Too Large`

```bash
# Increase client_max_body_size in nginx.conf
client_max_body_size 100M;

# Reload Nginx
docker-compose exec nginx nginx -s reload
```

**Issue**: `Too many redirects`

```bash
# Check X-Forwarded-Proto header is set
proxy_set_header X-Forwarded-Proto $scheme;

# Verify backend respects X-Forwarded-Proto
# (NocoDB should automatically handle this)
```

### Debug Mode

Enable debug logging temporarily:

```nginx
error_log /var/log/nginx/error.log debug;
```

```bash
# Reload and check logs
docker-compose exec nginx nginx -s reload
docker-compose logs nginx -f
```

### Test Configuration

```bash
# Syntax check
docker-compose exec nginx nginx -t

# Dump configuration
docker-compose exec nginx nginx -T

# Check loaded modules
docker-compose exec nginx nginx -V
```

### SSL Testing

**Test SSL Configuration**:

```bash
# Using OpenSSL
openssl s_client -connect idhub.ibdgc.org:443 -servername idhub.ibdgc.org

# Using SSL Labs (web-based)
# https://www.ssllabs.com/ssltest/analyze.html?d=idhub.ibdgc.org
```

**Expected SSL Labs Grade**: A or A+

## Performance Tuning

### Worker Processes

```nginx
# Auto-detect CPU cores (recommended)
worker_processes auto;

# Or set manually
worker_processes 4;
```

### Worker Connections

```nginx
events {
    worker_connections 2048;    # Increase for high traffic
    use epoll;                  # Linux-specific optimization
}
```

### Keepalive Connections

**Client Keepalive**:

```nginx
keepalive_timeout 65;
keepalive_requests 100;
```

**Upstream Keepalive**:

```nginx
upstream backend {
    server backend:8080;
    keepalive 64;               # Connection pool size
    keepalive_requests 100;
    keepalive_timeout 60s;
}

location / {
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_pass http://backend;
}
```

### Caching (Optional)

**Proxy Cache**:

```nginx
http {
    proxy_cache_path /var/cache/nginx levels=1:2 keys_zone=api_cache:10m max_size=1g inactive=60m;

    server {
        location /api/static/ {
            proxy_cache api_cache;
            proxy_cache_valid 200 60m;
            proxy_cache_use_stale error timeout updating;
            proxy_pass http://backend;
        }
    }
}
```

### Buffer Sizes

```nginx
client_body_buffer_size 128k;
client_max_body_size 50m;
client_header_buffer_size 1k;
large_client_header_buffers 4 16k;
```

## Security Checklist

- [x] HTTPS enforced (HTTP → HTTPS redirect)
- [x] Modern TLS protocols only (TLSv1.2, TLSv1.3)
- [x] Strong cipher suites
- [x] HSTS header enabled
- [x] X-Frame-Options set
- [x] X-Content-Type-Options set
- [x] X-XSS-Protection enabled
- [x] SSL session tickets disabled
- [x] Certificate auto-renewal configured
- [ ] Rate limiting enabled (optional)
- [ ] IP whitelisting for admin endpoints (optional)
- [ ] WAF/ModSecurity enabled (optional)
- [ ] DDoS protection (Cloudflare/AWS Shield) (optional)

## Maintenance

### Backup

**Configuration Backup**:

```bash
# Backup Nginx configs
tar -czf nginx-config-$(date +%Y%m%d).tar.gz nginx/

# Backup to S3
aws s3 cp nginx-config-$(date +%Y%m%d).tar.gz s3://idhub-backups/nginx/
```

**Certificate Backup**:

```bash
# Backup Let's Encrypt certificates
sudo tar -czf letsencrypt-$(date +%Y%m%d).tar.gz /etc/letsencrypt/

# Store securely (encrypted)
gpg --encrypt --recipient admin@ibdgc.org letsencrypt-$(date +%Y%m%d).tar.gz
```

## References

- **Nginx Documentation**: https://nginx.org/en/docs/
- **Mozilla SSL Configuration Generator**: https://ssl-config.mozilla.org/
- **SSL Labs**: https://www.ssllabs.com/ssltest/
- **Let's Encrypt**: https://letsencrypt.org/docs/
- **Security Headers**: https://securityheaders.com/

## Support

For issues or questions:

- Check logs: `docker-compose logs nginx -f`
- Test config: `docker-compose exec nginx nginx -t`
- Review this README
- Contact platform team

---

**Last Updated**: 2024-01-15
**Nginx Version**: 1.25 (Alpine)
**Maintained By**: IDhub Platform Team
