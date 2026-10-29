#!/bin/sh
set -eu

LOCAL_IP="$(hostname -I | awk '{print $1}')"

if [ -z "$LOCAL_IP" ]; then
    echo "ERROR: unable to determine local IP" >&2
    exit 1
fi

export LOCAL_IP
export SIP_DOMAIN="$LOCAL_IP"

echo "RING2SIP: detected local IP: $LOCAL_IP"
echo "RING2SIP: SIP_DOMAIN=$SIP_DOMAIN"

exec supervisord -c /etc/supervisor/conf.d/supervisord.conf
