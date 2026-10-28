# Eidonix connector

Two programs:

| Program | Runs at | Does |
| --- | --- | --- |
| `eidonix-connector` | each site | Dials out to the hub and offers the services its file lists (below). |
| `eidonix-connect` | one cloud host | The hub: accepts connector sessions and makes every connected service reachable on one local port, as `http://connect:8080/<site>/<service>/…` ([hub guide](docs/HUB.md)). |

```
application ──HTTP──▶ eidonix-connect ◀══ WSS, outbound from the site ══ eidonix-connector ──▶ service
```

The Eidonix connector lets an organization share chosen services on its own
hardware (GPU model servers, OCR, conversion, voice, S3-compatible storage)
with Eidonix, without opening any inbound port. It makes one outbound
connection to Eidonix and forwards only the requests its configuration file
allows.

One file, `/etc/eidonix-connector/connector.yaml`, says:

1. **what it shares**: each service by name, with its address and the exact
   requests allowed (`POST /v1/*`, `GET /v1/models`, …), and nothing else;
2. **the rules for them**: which models may be used, how many requests at
   once, how many per minute;
3. **where it may connect**: out to Eidonix only (checked against Eidonix's
   public signing keys before anything is sent), and on to only the machines
   its services name;
4. **what used it**: a local access log of every request, naming the
   organization and user Eidonix reported, so it matches Eidonix's usage
   records.

```yaml
site: example-gpu-site               # the connector's name in Eidonix Setup
upstream:
  url: https://connect.eidonix.com
  token: cxn_…                       # issued in Eidonix Setup
  public-keys: [BASE64_KEY_FROM_SETUP]

services:
  gpu1:
    address: 10.0.0.33:8000
    allowed-endpoints: [POST /v1/*, GET /v1/models]
    models: [Qwen/Qwen3-32B]         # only these models may be used
    health: /health
    max-concurrent-requests: 32
```

`eidonix-connector check` explains the file back in sentences:

```
Site example-gpu-site (must match the connector's name in Eidonix Setup).
Connects to upstream connect.eidonix.com (1 public key).
Shares 1 service and can reach only this machine:
  gpu1  http  10.0.0.33:8000  POST /v1/*, GET /v1/models   models: Qwen/Qwen3-32B   health: GET /health   max concurrent: 32
Logs every request to /var/log/eidonix-connector/access.log (never contents).
File permissions: ok (0640 root:eidonix-connector). Secrets set: token.
```

## Commands

| Command | |
| --- | --- |
| `eidonix-connector run [--config PATH]` | Connect and share the configured services. SIGHUP reloads the file without dropping running requests; SIGINT/SIGTERM drain for up to `--drain-timeout` (default 10m), a second one stops at once; SIGUSR1 reopens the access log. |
| `eidonix-connector status [--file PATH] [--json]` | Show whether the running connector is connected, each service's health and load, and its requests and errors in the last hour. |
| `eidonix-connector check [--config PATH]` | Validate the file and explain it. `--redacted` prints the file with every secret replaced by `•••• (set)`, for an auditor or a support ticket. `--write-probe` also tests writing to storage. |
| `eidonix-connector init [PATH]` | Write the commented blank configuration. |
| `eidonix-connector generate-key` | Print a new storage `encryption-key`. |
| `eidonix-connector convert --site NAME OLD` | Convert a v0.2 (or v0.1) configuration to v0.3 (see [installation](docs/INSTALL.md#upgrading-from-v02)). |
| `eidonix-connector storage move-to-root SERVICE --from FOLDER/` | Move a storage service's files from a v0.2 folder to the top of its bucket (`--dry-run` to preview). |
| `eidonix-connector version` | |

## The hub

`eidonix-connect` holds no database: one file,
`/etc/eidonix-connect/connect.yaml`, lists the sites that may connect, each
with its token (and optionally which applications may use it). Applications
on the hub host then use connected services by name: HTTP and SSE streamed
both ways, WebSocket, a small storage API for `kind: s3` services, and a
catalog at `/v1/catalog`. Go applications can use the
[`client`](client) package.

| Command | |
| --- | --- |
| `eidonix-connect serve [--config PATH]` | Run the hub; file changes apply by themselves; SIGUSR1 reopens the access log. |
| `eidonix-connect check [--config PATH]` | Validate the file and explain it (prints the server's public key, never a secret). |
| `eidonix-connect status [--url URL] [--healthz]` | Sites, sessions and services as a table. |
| `eidonix-connect token` / `server-key` | Print a new site token / server key. |
| `eidonix-connect version` | |

The container image and compose file are in [`deploy/connect`](deploy/connect).

## Documentation

- [Hub](docs/HUB.md): installing and running `eidonix-connect`, its file, logs and the local API; [design](docs/HUB-DESIGN.md).
- [Installation](docs/INSTALL.md): Alpine (OpenRC), Debian/Ubuntu and RHEL-family (systemd), Docker.
- [Configuration](docs/CONFIG.md): every setting, how requests are matched and models checked.
- [Security](docs/SECURITY.md): the trust model and what the connector does and does not protect.
- [Protocol](docs/PROTOCOL.md): the v3 wire protocol (`protocol/` is the reference implementation).
- [Changelog](CHANGELOG.md).

## Building

Go 1.27.3. `make` builds static Linux amd64 and arm64 binaries (connector
and hub) and the connector's
`.deb`, `.rpm` and `.apk` packages into `dist/` (packaging needs
[nfpm](https://nfpm.goreleaser.com/)); `make test` runs `go vet` and the
race-enabled test suite.

This repository also contains `cmd/test-gateway`, a loopback-only development
peer that speaks the same protocol (for trying a connector without a hub):

```sh
go run ./cmd/test-gateway --init --state /tmp/connector-dev --upstream 127.0.0.1:8000
go run ./cmd/test-gateway --state /tmp/connector-dev        # listens on https://127.0.0.1:8443
go run ./cmd/eidonix-connector run --config /tmp/connector-dev/connector.yaml
curl --cacert /tmp/connector-dev/tls.pem \
  -H "Authorization: Bearer $(cat /tmp/connector-dev/token)" \
  -H 'Content-Type: application/json' \
  -d '{"model":"test","messages":[{"role":"user","content":"hello"}]}' \
  https://127.0.0.1:8443/services/chat/v1/chat/completions
```

Never reuse development credentials in production. Production server keys
and the release signing keys are held by Eidonix and are never part of this
repository; only the release public keys are, in [`keys/`](keys) (see
[verifying a release](docs/INSTALL.md#verify-the-release)).

Module `github.com/eidonix-ai/eidonix-connector`. License: Apache-2.0.
