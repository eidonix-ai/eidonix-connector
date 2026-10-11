#!/bin/sh
set -eu
VERSION=${VERSION:-dev}
GO_VERSION=go1.27.2
[ "$(go version | awk '{print $3}')" = "$GO_VERSION" ] || { echo "build requires $GO_VERSION" >&2; exit 1; }
mkdir -p dist
for arch in amd64 arm64; do
  CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -buildvcs=false -ldflags "-s -w -buildid= -X main.version=$VERSION" -o "dist/cletus-connector-linux-$arch" ./cmd/cletus-connector
  CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -buildvcs=false -ldflags "-s -w -buildid=" -o "dist/test-gateway-linux-$arch" ./cmd/test-gateway
done
