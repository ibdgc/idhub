#!/bin/bash
# backup-idhub.sh

BACKUP_DIR="/opt/idhub/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$BACKUP_DIR"/{nocodb,idhub}

# Backup NocoDB
docker exec nocodb_db pg_dump -U nocodb nocodb | gzip >"$BACKUP_DIR/nocodb/db_$TIMESTAMP.sql.gz"
docker cp nocodb:/usr/app/data "$BACKUP_DIR/nocodb/data_$TIMESTAMP"
tar -czf "$BACKUP_DIR/nocodb/data_$TIMESTAMP.tar.gz" -C "$BACKUP_DIR/nocodb" "data_$TIMESTAMP"
rm -rf "$BACKUP_DIR/nocodb/data_$TIMESTAMP"

# Backup idHub
docker exec idhub_db pg_dump -U idhub_user idhub | gzip >"$BACKUP_DIR/idhub/db_$TIMESTAMP.sql.gz"

# Cleanup old backups (keep 7 days)
find "$BACKUP_DIR" -type f -mtime +30 -delete

echo "Backup completed: $TIMESTAMP"
