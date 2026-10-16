#!/bin/sh
set -eu
VERSION=${VERSION:-0.2.0}
GO_VERSION=go1.27.2
[ "$(go version | awk '{print $3}')" = "$GO_VERSION" ] || { echo "build requires $GO_VERSION" >&2; exit 1; }
mkdir -p dist
for arch in amd64 arm64; do
  CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -buildvcs=false -ldflags "-s -w -buildid= -X main.version=$VERSION" -o "dist/eidonix-connector-linux-$arch" ./cmd/eidonix-connector
  CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -buildvcs=false -ldflags "-s -w -buildid=" -o "dist/test-gateway-linux-$arch" ./cmd/test-gateway
done
