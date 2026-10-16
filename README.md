# Eidonix Connector

An outbound Linux connector for inference and bring-your-own S3-compatible object storage. One local TOML file (`/etc/eidonix-connector/config.toml`, one `[[routes]]` entry per connected service) fixes upstream destinations, methods, paths, headers, model-name aliases and resource limits. The cloud opens inference and typed storage streams over one authenticated WebSocket/yamux session; it cannot update local configuration or execute commands.

Storage exposes only put, get, head, list, delete and same-prefix copy. Endpoint, bucket, credentials and encryption stay under local operator control. Uploads commit only after byte counts, SHA-256 and stream completion are verified. Run the TLS MinIO acceptance suite with `./ci/storage-minio.sh`; see the image configuration note in [verification evidence](ci/evidence/README.md).

Module: `github.com/eidonix-ai/eidonix-connector`. License: Apache-2.0.

```sh
go test ./...
make build
./dist/eidonix-connector-linux-amd64 version
```

Builds require Go 1.27.2 and target Linux amd64 and arm64, packaged as .deb, .rpm and .apk (Alpine, OpenRC). Read [installation](docs/INSTALL.md), [configuration](docs/CONFIG.md), [security](docs/SECURITY.md), and [protocol](docs/PROTOCOL.md), and [implementation status](docs/IMPLEMENTATION.md).

This repository is the standalone connector and a development test gateway. It does not implement Cletus production gateway integration, database/setup UI, tenant enforcement, or rollout. No production server identity key or release signing key is supplied. Examples fail validation until a verified operator key is installed.

Release tooling generates deb/rpm packages, CycloneDX SBOMs and a minisign-signed checksum manifest. Signing requires an operator key; no signed release or distro certification is claimed. See [verification evidence](ci/evidence/README.md). The container uses distroless nonroot, with builder and runtime images pinned by digest.

## Local test gateway

Start an OpenAI-compatible upstream on loopback, then create development-only credentials:

```sh
go run ./cmd/test-gateway --init --state /tmp/connector-dev --upstream http://127.0.0.1:8000
go run ./cmd/test-gateway --state /tmp/connector-dev
# In another terminal (ensure local credential ownership satisfies secret checks):
go run ./cmd/eidonix-connector run --config /tmp/connector-dev/config.toml
curl --cacert /tmp/connector-dev/tls.pem \
  -H "Authorization: Bearer $(cat /tmp/connector-dev/token)" \
  -H 'Content-Type: application/json' \
  -d '{"model":"test","messages":[{"role":"user","content":"hello"}]}' \
  https://127.0.0.1/routes/chat/v1/chat/completions
```

The gateway binds loopback port 443. Linux may require a privileged launcher for that port; the connector must remain unprivileged. Root diagnostics require explicit `--allow-root`. Never reuse development credentials in production.
