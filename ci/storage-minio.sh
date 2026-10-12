#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
work=$(mktemp -d)
name="connector-storage-test-$$"
image=${MINIO_TEST_IMAGE:-quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e}
case "$image" in *@sha256:*) ;; *) printf 'MINIO_TEST_IMAGE must use an immutable digest\n' >&2; exit 1;; esac
cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
mkdir "$work/certs"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$work/certs/private.key" -out "$work/certs/public.crt" \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' >/dev/null 2>&1
# These credentials exist only for this isolated test container.
access=$(openssl rand -hex 12)
secret=$(openssl rand -hex 24)
printf 'MINIO_ROOT_USER=%s\nMINIO_ROOT_PASSWORD=%s\n' "$access" "$secret" > "$work/env"
chmod 600 "$work/env" "$work/certs/private.key"
docker create --name "$name" --env-file "$work/env" \
  -p 127.0.0.1::9000 --tmpfs /data:rw,size=512m \
  "$image" \
  server /data --certs-dir /certs >/dev/null
docker cp "$work/certs/." "$name:/certs" >/dev/null
docker start "$name" >/dev/null
port=$(docker port "$name" 9000/tcp | sed 's/.*://')
export CONNECTOR_TEST_S3_ENDPOINT="https://127.0.0.1:$port"
export CONNECTOR_TEST_S3_CA="$work/certs/public.crt"
export CONNECTOR_TEST_S3_ACCESS="$access"
export CONNECTOR_TEST_S3_SECRET="$secret"
for attempt in $(seq 1 60); do
  if curl -fsS --cacert "$CONNECTOR_TEST_S3_CA" "$CONNECTOR_TEST_S3_ENDPOINT/minio/health/ready" >/dev/null 2>&1; then
    go test -race ./internal/storage -run TestDocker -count=1 -timeout=5m
    exit
  fi
  sleep 1
done
docker logs "$name" 2>&1 | sed "s/$access/[test-user]/g; s/$secret/[test-secret]/g" >&2
printf 'MinIO did not become ready\n' >&2
exit 1
