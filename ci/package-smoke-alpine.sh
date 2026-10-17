#!/bin/sh
# Package installation smoke test inside a disposable Alpine container (no
# running OpenRC). Feed a tar archive with this script and dist/ on stdin.
set -eu
date -u +%FT%TZ
cat /etc/os-release
apk add --allow-untrusted /src/dist/eidonix-connector_*_"$(apk --print-arch | sed 's/aarch64/arm64/; s/x86_64/amd64/')".apk
getent passwd eidonix-connector
stat -c '%a %U:%G %n' /etc/eidonix-connector /etc/eidonix-connector/connector.yaml /var/log/eidonix-connector
[ "$(stat -c '%a %U:%G' /etc/eidonix-connector/connector.yaml)" = "640 root:eidonix-connector" ]
[ -x /etc/init.d/eidonix-connector ]
eidonix-connector version
sha256sum /usr/bin/eidonix-connector
! eidonix-connector run 2>/tmp/root-refusal
grep -q 'refusing to run as root' /tmp/root-refusal
! su -s /bin/sh eidonix-connector -c 'eidonix-connector check' 2>/tmp/template
cat /tmp/template
grep -q "must be the connector's name in Eidonix Setup" /tmp/template
chmod 0644 /etc/eidonix-connector/connector.yaml
! su -s /bin/sh eidonix-connector -c 'eidonix-connector check' 2>/tmp/mode
cat /tmp/mode
grep -q 'Fix: chown root:eidonix-connector /etc/eidonix-connector/connector.yaml && chmod 0640 /etc/eidonix-connector/connector.yaml' /tmp/mode
printf '%s\n' 'PASS apk installation/user/config ownership/root refusal/template refused/permission gate'
printf '%s\n' 'NOT RUN OpenRC runtime'
