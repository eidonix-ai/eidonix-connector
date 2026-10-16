# Eidonix connector

The Eidonix connector lets an organization share chosen services on its own
hardware (GPU model servers, OCR, conversion, voice, S3-compatible storage)
with Eidonix, without opening any inbound port. It makes one outbound
connection to Eidonix and forwards only the requests its configuration file
allows.

One file, `/etc/eidonix-connector/connector.yaml`, says:

1. **what it shares**: the services, grouped by type (`chat`, `embeddings`,
   `rerank`, `transcription`, …), and nothing else;
2. **the rules for them**: which models may be used, how many requests at
   once, how many per minute;
3. **where it may connect**: out to Eidonix only (checked against Eidonix's
   public signing keys before anything is sent), and on to only the machines
   its services name;
4. **what used it**: a local access log of every request, naming the
   organization and user Eidonix reported, so it matches Eidonix's usage
   records.

```yaml
upstream:
  url: https://connect.eidonix.com
  token: cxn_…                       # issued in Eidonix Setup
  public_keys: [BASE64_KEY_FROM_SETUP]

chat:
  - name: gpu1
    url: http://10.0.0.33:8000
    models: [Qwen/Qwen3-32B]         # only these models may be used
    max_concurrent_requests: 32
```

`eidonix-connector check` explains the file back in sentences:

```
Connects to upstream connect.eidonix.com (1 public key).
Shares 1 service and can reach only this machine:
  chat  gpu1  10.0.0.33:8000  models: Qwen/Qwen3-32B   max concurrent: 32
Not shared: anthropic, embeddings, rerank, transcription, speech, image, document, conversion, realtime, storage, custom.
Logs every request to /var/log/eidonix-connector/access.log (never contents).
File permissions: ok (0640 root:eidonix-connector). Secrets set: token.
```

## Commands

| Command | |
| --- | --- |
| `eidonix-connector run [--config PATH]` | Connect and share the configured services. SIGHUP reloads the file; SIGUSR1 reopens the access log. |
| `eidonix-connector check [--config PATH]` | Validate the file and explain it. `--redacted` prints the file with every secret replaced by `•••• (set)`, for an auditor or a support ticket. `--write-probe` also tests writing to storage. |
| `eidonix-connector init [PATH]` | Write the commented blank configuration. |
| `eidonix-connector generate-key` | Print a new storage `encryption_key`. |
| `eidonix-connector convert OLD.toml` | Convert a v0.1 configuration (see [installation](docs/INSTALL.md#upgrading-from-v01)). |
| `eidonix-connector version` | |

## Documentation

- [Installation](docs/INSTALL.md): Alpine (OpenRC), Debian/Ubuntu and RHEL-family (systemd), Docker.
- [Configuration](docs/CONFIG.md): every setting and exactly which requests each type accepts.
- [Security](docs/SECURITY.md): the trust model and what the connector does and does not protect.
- [Protocol](docs/PROTOCOL.md): the v2 wire protocol (`protocol/` is the reference implementation).
- [Changelog](CHANGELOG.md).

## Building

Go 1.27.2. `make` builds static Linux amd64 and arm64 binaries and the
`.deb`, `.rpm` and `.apk` packages into `dist/` (packaging needs
[nfpm](https://nfpm.goreleaser.com/)); `make test` runs `go vet` and the
race-enabled test suite.

This repository also contains `cmd/test-gateway`, a loopback-only development
peer that speaks the same protocol:

```sh
go run ./cmd/test-gateway --init --state /tmp/connector-dev --upstream http://127.0.0.1:8000
go run ./cmd/test-gateway --state /tmp/connector-dev        # listens on https://127.0.0.1:8443
go run ./cmd/eidonix-connector run --config /tmp/connector-dev/connector.yaml
curl --cacert /tmp/connector-dev/tls.pem \
  -H "Authorization: Bearer $(cat /tmp/connector-dev/token)" \
  -H 'Content-Type: application/json' \
  -d '{"model":"test","messages":[{"role":"user","content":"hello"}]}' \
  https://127.0.0.1:8443/services/chat/v1/chat/completions
```

Never reuse development credentials in production. No production server key
or release signing key is part of this repository.

Module `github.com/eidonix-ai/eidonix-connector`. License: Apache-2.0.
