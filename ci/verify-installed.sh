#!/bin/sh
set -eu
format=$1
case "$format" in deb) apt-get update; apt-get install -y curl iproute2 /tmp/connector.deb;;rpm) dnf install -y curl iproute /tmp/connector.rpm;;esac
! systemctl is-active --quiet eidonix-connector
! systemctl is-enabled --quiet eidonix-connector
getent passwd eidonix-connector
stat /var/log/eidonix-connector
systemd-analyze verify /usr/lib/systemd/system/eidonix-connector.service
systemd-analyze security eidonix-connector.service --no-pager
# Verify that the kernel implements the actual IP filter (not just the unit property).
output=$(systemd-run --quiet --wait --pipe --unit=eidonix-egress-probe \
  -p IPAddressDeny=any -p NoNewPrivileges=yes -p CapabilityBoundingSet= \
  /usr/bin/curl --verbose --noproxy '*' --connect-timeout 2 http://192.0.2.1:80 2>&1) && { echo 'FAIL: denied destination connected'; exit 1; }
printf '%s\n' "$output"
printf '%s' "$output" | grep -Ei 'Operation not permitted|Permission denied' >/dev/null || { echo 'FAIL: egress probe did not prove kernel denial'; exit 1; }
eidonix-connector version
chmod 0755 /tmp/test-gateway
/tmp/test-gateway --init --state /etc/eidonix-connector/dev-test --upstream http://127.0.0.1:8000
chgrp eidonix-connector /etc/eidonix-connector/dev-test /etc/eidonix-connector/dev-test/token /etc/eidonix-connector/dev-test/tls.pem
chmod 0750 /etc/eidonix-connector/dev-test
chmod 0640 /etc/eidonix-connector/dev-test/token /etc/eidonix-connector/dev-test/tls.pem
sed 's|^audit_log = .*|audit_log = "/var/log/eidonix-connector/audit.jsonl"|' /etc/eidonix-connector/dev-test/config.toml > /etc/eidonix-connector/config.toml
chown root:eidonix-connector /etc/eidonix-connector/config.toml
chmod 0640 /etc/eidonix-connector/config.toml
mkdir -p /etc/systemd/system/eidonix-connector.service.d
eidonix-connector print-systemd-network > /etc/systemd/system/eidonix-connector.service.d/network.conf
/tmp/test-gateway --state /etc/eidonix-connector/dev-test >/tmp/gateway.log 2>&1 &
gateway_pid=$!
trap 'kill "$gateway_pid" 2>/dev/null || true; systemctl stop eidonix-connector' EXIT
systemctl daemon-reload
systemctl start eidonix-connector
sleep 3
pid=$(systemctl show --value -p MainPID eidonix-connector)
[ "$pid" -gt 0 ]
grep -Eq '^CapEff:[[:space:]]+0000000000000000$' "/proc/$pid/status"
grep -Eq '^NoNewPrivs:[[:space:]]+1$' "/proc/$pid/status"
! ss -ltnp | grep "pid=$pid,"
systemctl reload eidonix-connector
systemctl is-active --quiet eidonix-connector
journalctl -u eidonix-connector --no-pager
