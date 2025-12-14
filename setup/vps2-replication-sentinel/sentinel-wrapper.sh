#!/bin/sh

echo "Waiting for redis-master to be resolvable..."

until getent hosts redis-master > /dev/null 2>&1; do
    echo "  redis-master not yet resolvable, waiting..."
    sleep 2
done

MASTER_IP=$(getent hosts redis-master | awk '{ print $1 }')
echo " redis-master resolved to: $MASTER_IP"

cat > /tmp/sentinel-runtime.conf << EOF
port 26379
dir /data

sentinel monitor mymaster $MASTER_IP 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel parallel-syncs mymaster 1
sentinel failover-timeout mymaster 10000

sentinel announce-ip $(hostname -i)
sentinel announce-port 26379
EOF

echo "Starting Redis Sentinel with runtime config..."
exec redis-sentinel /tmp/sentinel-runtime.conf
