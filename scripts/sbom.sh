#!/bin/sh
set -eu
command -v syft >/dev/null
for arch in amd64 arm64; do syft "file:dist/cletus-connector-linux-$arch" -o "cyclonedx-json=dist/cletus-connector-linux-$arch.cdx.json"; done
