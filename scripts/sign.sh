#!/bin/sh
set -eu
: "${MINISIGN_SECRET_KEY:?set path to an operator-controlled release key}"
command -v minisign >/dev/null
if command -v sha256sum >/dev/null; then sum='sha256sum'; else sum='shasum -a 256'; fi
(cd dist && $sum eidonix-connector* > SHA256SUMS)
minisign -Sm dist/SHA256SUMS -s "$MINISIGN_SECRET_KEY"
