#!/bin/sh
set -eu
format=$1
case "$format" in deb) apt-get update; apt-get install -y curl iproute2 /tmp/connector.deb;;rpm) dnf install -y curl iproute /tmp/connector.rpm;;esac
! systemctl is-active --quiet cletus-connector
! systemctl is-enabled --quiet cletus-connector
getent passwd cletus-connector
stat /var/log/cletus-connector
systemd-analyze verify /usr/lib/systemd/system/cletus-connector.service
systemd-analyze security cletus-connector.service --no-pager
# Verify that the kernel implements the actual IP filter (not just the unit property).
output=$(systemd-run --quiet --wait --pipe --unit=cletus-egress-probe \
  -p IPAddressDeny=any -p NoNewPrivileges=yes -p CapabilityBoundingSet= \
  /usr/bin/curl --verbose --noproxy '*' --connect-timeout 2 http://192.0.2.1:80 2>&1) && { echo 'FAIL: denied destination connected'; exit 1; }
printf '%s\n' "$output"
printf '%s' "$output" | grep -Ei 'Operation not permitted|Permission denied' >/dev/null || { echo 'FAIL: egress probe did not prove kernel denial'; exit 1; }
cletus-connector version
chmod 0755 /tmp/test-gateway
/tmp/test-gateway --init --state /etc/cletus-connector/dev-test --upstream http://127.0.0.1:8000
chgrp cletus-connector /etc/cletus-connector/dev-test /etc/cletus-connector/dev-test/token /etc/cletus-connector/dev-test/tls.pem
chmod 0750 /etc/cletus-connector/dev-test
chmod 0640 /etc/cletus-connector/dev-test/token /etc/cletus-connector/dev-test/tls.pem
sed 's|^audit_log = .*|audit_log = "/var/log/cletus-connector/audit.jsonl"|' /etc/cletus-connector/dev-test/config.toml > /etc/cletus-connector/config.toml
chown root:cletus-connector /etc/cletus-connector/config.toml
chmod 0640 /etc/cletus-connector/config.toml
mkdir -p /etc/systemd/system/cletus-connector.service.d
cletus-connector print-systemd-network > /etc/systemd/system/cletus-connector.service.d/network.conf
/tmp/test-gateway --state /etc/cletus-connector/dev-test >/tmp/gateway.log 2>&1 &
gateway_pid=$!
trap 'kill "$gateway_pid" 2>/dev/null || true; systemctl stop cletus-connector' EXIT
systemctl daemon-reload
systemctl start cletus-connector
sleep 3
pid=$(systemctl show --value -p MainPID cletus-connector)
[ "$pid" -gt 0 ]
grep -Eq '^CapEff:[[:space:]]+0000000000000000$' "/proc/$pid/status"
grep -Eq '^NoNewPrivs:[[:space:]]+1$' "/proc/$pid/status"
! ss -ltnp | grep "pid=$pid,"
systemctl reload cletus-connector
systemctl is-active --quiet cletus-connector
journalctl -u cletus-connector --no-pager
