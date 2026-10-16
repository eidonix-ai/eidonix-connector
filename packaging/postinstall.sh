#!/bin/sh
set -eu
install -d -m 0750 -o root -g eidonix-connector /etc/eidonix-connector
install -d -m 0750 -o eidonix-connector -g eidonix-connector /var/log/eidonix-connector
# The configuration holds secrets: root-owned, readable by the service group only.
# Fix a fresh install whose package manager could not apply the group; leave an
# owner's own choice of group alone.
config=/etc/eidonix-connector/connector.yaml
if [ -f "$config" ] && [ "$(stat -c %G "$config")" = root ]; then
  chown root:eidonix-connector "$config"
  chmod 0640 "$config"
fi
if command -v systemctl >/dev/null 2>&1; then systemctl daemon-reload || true; fi
# Installation deliberately leaves the service disabled and stopped:
# fill in /etc/eidonix-connector/connector.yaml, run eidonix-connector check, then enable it.
