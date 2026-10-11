#!/bin/sh
set -eu
: "${IMAGE_DIGEST:?set immutable image reference registry/name@sha256:digest}"
: "${COSIGN_KEY:?set operator image-signing key or KMS reference}"
command -v cosign >/dev/null
cosign sign --key "$COSIGN_KEY" "$IMAGE_DIGEST"
