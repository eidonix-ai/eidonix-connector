#!/bin/sh
# Run inside a disposable container with /work mounted tmpfs (rw,exec,dev).
# Feed a tar archive containing ci/package-smoke.sh and the arm64 deb on stdin.
set -eu
printf '%s\n' 'Environment: disposable container; tmpfs-backed chroot; no running systemd'
mkdir -p /work/root
tar -C / --exclude=./work --exclude=./proc --exclude=./sys --exclude=./dev -cf /work/base.tar .
tar -C /work/root -xf /work/base.tar
rm /work/base.tar
mkdir -p /work/root/dev /work/root/proc /work/root/sys /work/root/src
cp -a /dev/null /dev/zero /dev/random /dev/urandom /work/root/dev/
tar -xf - -C /work/root/src
chmod -R a-w /work/root/src
chroot /work/root sh /src/ci/package-smoke.sh
