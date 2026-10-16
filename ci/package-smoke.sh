#!/bin/sh
# Package installation smoke test inside a disposable Debian/Ubuntu container
# (no running systemd). Feed a tar archive with this script and dist/ on stdin.
set -eu
date -u +%FT%TZ
cat /etc/os-release
apt-get update
apt-get install -y ca-certificates procps /src/dist/eidonix-connector_*_arm64.deb
getent passwd eidonix-connector
getent group eidonix-connector
stat -c '%a %U %G %n' /etc/eidonix-connector /etc/eidonix-connector/connector.yaml /var/log/eidonix-connector
[ "$(stat -c '%a' /var/log/eidonix-connector)" = 750 ]
[ "$(stat -c '%a %U:%G' /etc/eidonix-connector/connector.yaml)" = "640 root:eidonix-connector" ]
[ ! -e /etc/systemd/system/multi-user.target.wants/eidonix-connector.service ]
eidonix-connector version
sha256sum /usr/bin/eidonix-connector
! eidonix-connector run --config /does/not/exist 2>/tmp/root-refusal
cat /tmp/root-refusal
grep -q 'refusing to run as root' /tmp/root-refusal
! su -s /bin/sh eidonix-connector -c 'eidonix-connector check' 2>/tmp/template
grep -q 'token is not a connection token' /tmp/template
printf '%s\n' 'PASS package installation/user/config ownership/root refusal/template refused/no enable symlink'
printf '%s\n' 'NOT RUN real systemd runtime, capabilities/listeners under running service'
