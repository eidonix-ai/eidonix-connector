#!/bin/sh
set -eu
date -u +%FT%TZ
cat /etc/os-release
apt-get update
apt-get install -y ca-certificates systemd procps /src/dist/eidonix-connector_0.1.0-dev_arm64.deb
getent passwd eidonix-connector
getent group eidonix-connector
stat -c '%a %U %G %n' /etc/eidonix-connector /var/log/eidonix-connector
[ "$(stat -c '%a' /var/log/eidonix-connector)" = 750 ]
[ ! -e /etc/systemd/system/multi-user.target.wants/eidonix-connector.service ]
eidonix-connector version
sha256sum /usr/bin/eidonix-connector
! eidonix-connector run --config /does/not/exist 2>/tmp/root-refusal
cat /tmp/root-refusal
grep -q 'refusing to run as root' /tmp/root-refusal
systemd-analyze verify /usr/lib/systemd/system/eidonix-connector.service
printf '%s\n' 'PASS package installation/user/directories/root refusal/unit syntax/no enable symlink'
printf '%s\n' 'NOT RUN real systemd runtime, kernel BPF egress, capabilities/listeners under running service'
