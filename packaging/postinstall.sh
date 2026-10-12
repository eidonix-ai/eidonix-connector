#!/bin/sh
set -eu
if command -v useradd >/dev/null 2>&1; then
  getent group cletus-connector >/dev/null || groupadd --system cletus-connector
  getent passwd cletus-connector >/dev/null || useradd --system --gid cletus-connector --home-dir /nonexistent --shell /usr/sbin/nologin cletus-connector
else
  # Alpine (busybox): no shadow utilities.
  getent group cletus-connector >/dev/null || addgroup -S cletus-connector
  getent passwd cletus-connector >/dev/null || adduser -S -D -H -h /nonexistent -s /sbin/nologin -G cletus-connector cletus-connector
fi
install -d -m 0750 -o root -g cletus-connector /etc/cletus-connector
install -d -m 0750 -o cletus-connector -g cletus-connector /var/log/cletus-connector
if command -v systemctl >/dev/null 2>&1; then systemctl daemon-reload || true; fi
# Installation deliberately leaves the service disabled and stopped.
