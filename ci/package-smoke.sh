#!/bin/sh
set -eu
date -u +%FT%TZ
cat /etc/os-release
apt-get update
apt-get install -y ca-certificates systemd procps /src/dist/cletus-connector_0.1.0-dev_arm64.deb
getent passwd cletus-connector
getent group cletus-connector
stat -c '%a %U %G %n' /etc/cletus-connector /var/log/cletus-connector
[ "$(stat -c '%a' /var/log/cletus-connector)" = 750 ]
[ ! -e /etc/systemd/system/multi-user.target.wants/cletus-connector.service ]
cletus-connector version
sha256sum /usr/bin/cletus-connector
! cletus-connector run --config /does/not/exist 2>/tmp/root-refusal
cat /tmp/root-refusal
grep -q 'refusing to run as root' /tmp/root-refusal
systemd-analyze verify /usr/lib/systemd/system/cletus-connector.service
printf '%s\n' 'PASS package installation/user/directories/root refusal/unit syntax/no enable symlink'
printf '%s\n' 'NOT RUN real systemd runtime, kernel BPF egress, capabilities/listeners under running service'
