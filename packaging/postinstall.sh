#!/bin/sh
set -eu
if command -v useradd >/dev/null 2>&1; then
  getent group eidonix-connector >/dev/null || groupadd --system eidonix-connector
  getent passwd eidonix-connector >/dev/null || useradd --system --gid eidonix-connector --home-dir /nonexistent --shell /usr/sbin/nologin eidonix-connector
else
  # Alpine (busybox): no shadow utilities.
  getent group eidonix-connector >/dev/null || addgroup -S eidonix-connector
  getent passwd eidonix-connector >/dev/null || adduser -S -D -H -h /nonexistent -s /sbin/nologin -G eidonix-connector eidonix-connector
fi
install -d -m 0750 -o root -g eidonix-connector /etc/eidonix-connector
install -d -m 0750 -o eidonix-connector -g eidonix-connector /var/log/eidonix-connector
if command -v systemctl >/dev/null 2>&1; then systemctl daemon-reload || true; fi
# Installation deliberately leaves the service disabled and stopped.
