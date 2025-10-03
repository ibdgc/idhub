#!/bin/bash
sudo cp -rL /etc/letsencrypt/live/idhub.ibdgc.org/* /opt/idhub/ssl/live/idhub.ibdgc.org/
sudo cp -rL /etc/letsencrypt/archive/idhub.ibdgc.org/* /opt/idhub/ssl/archive/idhub.ibdgc.org/
sudo chown -R ec2-user:ec2-user /opt/idhub/ssl/
docker-compose -f /opt/idhub/docker-compose.yml exec nginx nginx -s reload
