#!/bin/sh
# Runs inside a disposable systemd VM after the package is pushed to /tmp.
set -eu
format=$1
case "$format" in deb) apt-get update; apt-get install -y curl iproute2 /tmp/connector.deb;;rpm) dnf install -y curl iproute /tmp/connector.rpm;;esac
! systemctl is-active --quiet eidonix-connector
! systemctl is-enabled --quiet eidonix-connector
getent passwd eidonix-connector
stat -c '%a %U:%G %n' /etc/eidonix-connector /etc/eidonix-connector/connector.yaml /var/log/eidonix-connector
[ "$(stat -c '%a %U:%G' /etc/eidonix-connector/connector.yaml)" = "640 root:eidonix-connector" ]
systemd-analyze verify /usr/lib/systemd/system/eidonix-connector.service
systemd-analyze security eidonix-connector.service --no-pager
eidonix-connector version
# The blank template must be refused until it is filled in.
! su -s /bin/sh eidonix-connector -c 'eidonix-connector check'
# Development gateway on loopback; its generated file replaces the template.
chmod 0755 /tmp/test-gateway
/tmp/test-gateway --init --state /root/dev-test --upstream 127.0.0.1:8000
sed 's|^access-log: .*|access-log: /var/log/eidonix-connector/access.log|' /root/dev-test/connector.yaml > /etc/eidonix-connector/connector.yaml
chown root:eidonix-connector /etc/eidonix-connector/connector.yaml
chmod 0640 /etc/eidonix-connector/connector.yaml
su -s /bin/sh eidonix-connector -c 'eidonix-connector check'
/tmp/test-gateway --state /root/dev-test >/tmp/gateway.log 2>&1 &
gateway_pid=$!
trap 'kill "$gateway_pid" 2>/dev/null || true; systemctl stop eidonix-connector' EXIT
systemctl start eidonix-connector
sleep 3
pid=$(systemctl show --value -p MainPID eidonix-connector)
[ "$pid" -gt 0 ]
grep -Eq '^CapEff:[[:space:]]+0000000000000000$' "/proc/$pid/status"
grep -Eq '^NoNewPrivs:[[:space:]]+1$' "/proc/$pid/status"
! ss -ltnp | grep "pid=$pid,"
curl -fsS --cacert /root/dev-test/tls.pem -H "Authorization: Bearer $(cat /root/dev-test/token)" https://127.0.0.1:8443/status | grep -q '"sessions":1'
systemctl reload eidonix-connector
systemctl is-active --quiet eidonix-connector
journalctl -u eidonix-connector --no-pager
