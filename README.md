# NocoDB Self-Hosted Deployment on AWS EC2

Production-ready NocoDB installation with Docker, Nginx reverse proxy, and automatic SSL certificate management.

## Architecture

```
Internet → Route53/DNS → EC2 (Elastic IP)
                           ↓
                         Nginx (Port 80/443)
                           ↓
                         NocoDB Container (Port 8080)
                           ↓
                         PostgreSQL Container (Port 5432)
```

## Infrastructure

- **Platform**: AWS EC2
- **Instance Type**: t3.small (2 vCPU, 2GB RAM)
- **OS**: Amazon Linux 2023
- **Domain**: idhub.ibdgc.org
- **SSL**: Let's Encrypt (auto-renewal via certbot)

## Components

### Services
- **NocoDB**: Latest version (Docker)
- **PostgreSQL**: 15-alpine (Docker)
- **Nginx**: Alpine (Docker)
- **Certbot**: Installed on host for SSL management

## Directory Structure

```
~/nocodb/
├── docker-compose.yml          # Container orchestration
├── .env                        # Environment variables (secrets)
├── nginx/
│   ├── nginx.conf             # Main nginx configuration
│   └── conf.d/
│       └── nocodb.conf        # Site-specific configuration
└── setup-info.txt             # Deployment documentation

/etc/letsencrypt/              # SSL certificates (host)
└── live/idhub.ibdgc.org/
    ├── fullchain.pem
    └── privkey.pem

/var/www/certbot/              # Certbot webroot (host)
```

## Installation Summary

### 1. EC2 Setup
- Launched t3.small instance with Amazon Linux 2023
- Allocated and associated Elastic IP
- Configured Security Group (ports 22, 80, 443)

### 2. DNS Configuration
- Created A record: idhub.ibdgc.org → Elastic IP

### 3. Software Installation
```bash
sudo yum update -y
sudo yum install -y docker certbot
sudo systemctl enable --now docker
sudo usermod -a -G docker ec2-user

# Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### 4. SSL Certificate
```bash
# Obtained via certbot standalone
sudo certbot certonly --standalone -d idhub.ibdgc.org

# Configured for webroot renewal
sudo nano /etc/letsencrypt/renewal/idhub.ibdgc.org.conf
# Changed authenticator to webroot
# Added webroot_path = /var/www/certbot
```

### 5. Application Deployment
```bash
cd ~/nocodb
# Created docker-compose.yml, nginx configs, .env
docker-compose up -d
```

## Configuration Files

### docker-compose.yml
See `docker-compose.yml` in this directory.

Key environment variables:
- `NC_DB`: PostgreSQL connection string
- `NC_AUTH_JWT_SECRET`: JWT signing secret
- `NC_PUBLIC_URL`: Public URL (https://idhub.ibdgc.org)

### Nginx Configuration
- Main config: `nginx/nginx.conf`
- Site config: `nginx/conf.d/nocodb.conf`

## Management Commands

### Service Management
```bash
cd ~/nocodb

# View status
docker-compose ps

# View logs
docker-compose logs -f
docker-compose logs -f nocodb
docker-compose logs -f nginx

# Restart services
docker-compose restart
docker-compose restart nocodb

# Stop services
docker-compose down

# Start services
docker-compose up -d

# Update NocoDB
docker-compose pull nocodb
docker-compose up -d nocodb
```

### SSL Certificate Management
```bash
# Check certificate status
sudo certbot certificates

# Test renewal
sudo certbot renew --dry-run

# Force renewal (if needed)
sudo certbot renew --force-renewal

# Check auto-renewal timer
sudo systemctl status certbot-renew.timer
sudo systemctl list-timers | grep certbot
```

### Database Management
```bash
# Access PostgreSQL
docker-compose exec db psql -U nocodb -d nocodb

# Backup database
docker-compose exec db pg_dump -U nocodb nocodb | gzip > backup_$(date +%Y%m%d).sql.gz

# Restore database
gunzip < backup_20251001.sql.gz | docker-compose exec -T db psql -U nocodb -d nocodb
```

### Nginx Management
```bash
# Test configuration
docker-compose exec nginx nginx -t

# Reload configuration
docker-compose exec nginx nginx -s reload

# View access logs
docker-compose exec nginx tail -f /var/log/nginx/access.log

# View error logs
docker-compose exec nginx tail -f /var/log/nginx/error.log
```

## Backup Strategy

### Automated Backups
Backup script located at `~/backup-nocodb.sh`.

Manual backup:
```bash
# Backup database
docker-compose exec -T db pg_dump -U nocodb nocodb | gzip > ~/backups/db_$(date +%Y%m%d_%H%M%S).sql.gz

# Backup NocoDB data volume
docker run --rm \
    -v nocodb_nocodb_data:/data \
    -v ~/backups:/backup \
    alpine tar czf /backup/nocodb_data_$(date +%Y%m%d_%H%M%S).tar.gz -C /data .

# Backup configuration
tar czf ~/backups/config_$(date +%Y%m%d_%H%M%S).tar.gz -C ~/nocodb .env docker-compose.yml nginx/
```

## Monitoring

### Health Checks
```bash
# Check service health
docker-compose ps

# Check NocoDB health endpoint
curl https://idhub.ibdgc.org/api/v1/health

# Check SSL certificate expiry
sudo certbot certificates
```

### Logs
```bash
# Application logs
docker-compose logs --tail=100 -f

# System logs
sudo journalctl -u docker -f

# Certbot logs
sudo tail -f /var/log/letsencrypt/letsencrypt.log
```

## Security

### Implemented Security Measures
- HTTPS enforced with HSTS
- Modern TLS protocols (1.2, 1.3)
- Strong cipher suites
- Security headers (X-Frame-Options, CSP, etc.)
- Secrets stored in .env file (not in version control)
- Database not exposed to internet
- Regular security updates via yum

### Security Group Rules
```
Inbound:
- SSH (22): Your IP only
- HTTP (80): 0.0.0.0/0
- HTTPS (443): 0.0.0.0/0

Outbound:
- All traffic: 0.0.0.0/0
```

## Maintenance

### Regular Tasks
- **Daily**: Automated via certbot timer
  - SSL certificate renewal check
  
- **Weekly**: Manual
  - Review logs for errors
  - Check disk space usage
  
- **Monthly**: Manual
  - Update Docker images
  - Review and rotate backups
  - Security updates: `sudo yum update -y`

## Support & Resources

- **NocoDB Documentation**: https://nocodb.com/docs
- **NocoDB GitHub**: https://github.com/nocodb/nocodb
- **Let's Encrypt**: https://letsencrypt.org/docs/
- **Nginx Documentation**: https://nginx.org/en/docs/

## Notes

- Certificate auto-renews when <30 days remaining
- Certbot runs twice daily via systemd timer
- All data persists in Docker volumes
- .env file contains sensitive credentials (keep secure)
- Elastic IP must remain allocated to maintain DNS

