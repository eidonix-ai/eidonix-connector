#!/bin/sh
# Requires an Incus host with VM support and the images remote configured.
set -eu
command -v incus >/dev/null
: "${VERSION:?set package VERSION}"
ARCH=${ARCH:-amd64}
mkdir -p ci/evidence
for distro in ubuntu/22.04 ubuntu/24.04 ubuntu/26.04 debian/12 debian/13 fedora/41 rocky/9 rocky/10 almalinux/9 almalinux/10; do
  name="connector-$(echo "$distro" | tr / -)-$ARCH"
  log="ci/evidence/$name.log"
  image="images:$distro/$ARCH"
  echo "RUN $image $(date -u +%FT%TZ)" > "$log"
  (
    incus launch "$image" "$name" --vm
    trap 'incus delete -f "$name" >/dev/null 2>&1 || true' EXIT HUP INT TERM
    incus exec "$name" -- sh -c 'timeout 180 sh -c "until systemctl is-system-running --quiet || systemctl is-system-running | grep -q degraded; do sleep 2; done"'
    case "$distro" in ubuntu/*|debian/*) format=deb;;*)format=rpm;;esac
    package=$(find dist -name "*${ARCH}*.$format" -print | head -1)
    [ -n "$package" ]
    incus file push "$package" "$name/tmp/connector.$format"
    incus file push "dist/test-gateway-linux-$ARCH" "$name/tmp/test-gateway"
    incus file push ci/verify-installed.sh "$name/tmp/verify-installed.sh"
    incus exec "$name" -- sh /tmp/verify-installed.sh "$format"
  ) >> "$log" 2>&1 || { echo "FAILED: $image; see $log" >&2; exit 1; }
  echo "PASS $image" >> "$log"
done
