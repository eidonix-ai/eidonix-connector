#!/bin/sh
set -eu
: "${VERSION:?set release VERSION}"
case "$VERSION" in *[!a-zA-Z0-9.+_-]*) echo 'invalid package version' >&2; exit 1;; esac
command -v nfpm >/dev/null
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-0}
export SOURCE_DATE_EPOCH
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT HUP INT TERM
for ARCH in amd64 arm64; do
  sed 's/${ARCH}/'"$ARCH"'/g; s/${VERSION}/'"$VERSION"'/g' packaging/nfpm.yaml > "$rendered"
  for format in deb rpm; do nfpm package --config "$rendered" --packager "$format" --target "dist/cletus-connector_${VERSION}_${ARCH}.$format"; done
done
