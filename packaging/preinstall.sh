#!/bin/sh
# Create the service account before files are unpacked, so the
# configuration can be installed as root:eidonix-connector 0640.
set -eu
if command -v useradd >/dev/null 2>&1; then
  getent group eidonix-connector >/dev/null || groupadd --system eidonix-connector
  getent passwd eidonix-connector >/dev/null || useradd --system --gid eidonix-connector --home-dir /nonexistent --shell /usr/sbin/nologin eidonix-connector
else
  # Alpine (busybox): no shadow utilities.
  getent group eidonix-connector >/dev/null || addgroup -S eidonix-connector
  getent passwd eidonix-connector >/dev/null || adduser -S -D -H -h /nonexistent -s /sbin/nologin -G eidonix-connector eidonix-connector
fi
