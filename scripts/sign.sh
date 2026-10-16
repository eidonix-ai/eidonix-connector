#!/bin/sh
set -eu
: "${MINISIGN_SECRET_KEY:?set path to an operator-controlled release key}"
command -v minisign >/dev/null
(cd dist && sha256sum eidonix-connector* > SHA256SUMS)
minisign -Sm dist/SHA256SUMS -s "$MINISIGN_SECRET_KEY"
