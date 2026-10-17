# Changelog

## 0.3.0 (unreleased)

**Breaking: sites and services instead of types; protocol v3.** Convert a
v0.2 file with `eidonix-connector convert --site <name in Setup>
/etc/eidonix-connector/connector.yaml` and review the notes it prints (see
[installation](docs/INSTALL.md#upgrading-from-v02)).

- **The connector only exposes resources.** Services are listed by name
  under `services:`, each with an `address` (`host:port` for plain HTTP or
  `https://host:port`) and the `allowed-endpoints` it accepts
  (`METHOD /path`, a final `/*` matching everything below; `GET`, `POST`,
  `PUT`, `DELETE`, `WEBSOCKET`). Gone: the 12 service types and their request
  tables and content-type rules, `api_path` rewriting, `models: any`
  discovery, the connector's own `GET /v1/models` answer and `urls:` load
  spreading.
- **`site`**: the connector's name in Setup, sent in hello; Eidonix closes a
  session whose site does not match (`site_mismatch`).
- **Keys are lowercase with dashes** (`public-keys`, `api-key`,
  `max-concurrent-requests`, `access-log`, …); `proxy_url` is `proxy`.
- **Models** are a list or `<name on the server>: {alias: <name Eidonix
  uses>}`. With models set, POST/PUT bodies must be JSON or multipart and
  their model is checked and rewritten; without models, bodies are not read.
- **Health** is `GET <health>` every 15 s when set, otherwise a TCP
  reachability check.
- Defaults: 16 concurrent requests, 5 minute idle timeout, no rate limit,
  100 MiB requests. `Anthropic-Version` and `Anthropic-Beta` are forwarded to
  any service.
- **Storage** is `kind: s3` and uses the whole bucket (no `prefix`);
  `encryption` is `none` (default), `sse-s3` or `sse-c`; `read-only` refuses
  put, delete and copy. `sse-kms`, `kms_key_id`, `allow_http` (now the address
  scheme) and `max_read_per_hour` are gone.
- **`eidonix-connector storage move-to-root SERVICE --from FOLDER/`** moves a
  v0.2 folder's files to the top of the bucket server-side (SSE-C key from the
  file), verifies every copy, then removes the originals; it never
  overwrites. `--dry-run` previews.
- **Protocol v3** (`eidonix-connector.v3`): `hello.site`; services carry
  `kind` (`http`|`s3`), `endpoints` and `models` instead of `type` and
  `models_any`; storage advertises `read_only`. The identity signature covers
  the v3 subprotocol name. Everything else is unchanged from v2.
- The access log's `type` field is now `kind`.

## 0.2.0

**Breaking: new name, new configuration format, new protocol version.**
Convert an existing configuration with
`eidonix-connector convert /etc/cletus-connector/config.toml` and review the
notes it prints (see [installation](docs/INSTALL.md#upgrading-from-v01)).

- Renamed from cletus-connector to **eidonix-connector**: module
  `github.com/eidonix-ai/eidonix-connector`, binary, packages, service and
  account `eidonix-connector`, paths under `/etc/eidonix-connector` and
  `/var/log/eidonix-connector`.
- **One YAML file**, `/etc/eidonix-connector/connector.yaml`, with secrets
  inline (no token or secret files). Services are grouped by type. Unknown
  or misspelt settings stop the connector with the line and service. The
  file must be root-owned and unreadable by other users; the connector says
  how to fix it.
- **Fixed request list per type** (`chat`, `anthropic`, `embeddings`,
  `rerank`, `transcription`, `speech`, `image`, `document`, `conversion`,
  `realtime`, `storage`, `custom`). Adds requests Cletus uses that v0.1
  lacked (`POST /v1/responses`, `GET /v1/audio/voices`, `GET /v1/config`);
  drops flavors and their vendor endpoints, the three path lists,
  `content_types`, `balance`, health settings, `allow_cidrs`, `tls.ca`,
  `health_listen` and `report_location`. `api_path` maps the API prefix
  for servers such as Infinity.
- **Models restrict as well as rename**: a list or map is an allowlist
  (`model_not_allowed`, `model_required`); `models: any` must be written
  out. Allowlisted services answer `GET /v1/models` themselves.
- **Per-service limits** are enforced and announced:
  `max_concurrent_requests` (`busy`) and `max_requests_per_minute`
  (`rate_limited`).
- **Reach is enforced in the process only**: the upstream, `proxy_url` and
  each service's address. The systemd network drop-in generator
  (`print-systemd-network`) and the unit's `IPAddressDeny` are gone; there is
  no implicit localhost. The web proxy is `upstream.proxy_url`, no longer
  `HTTPS_PROXY`.
- **Protocol v2** (`eidonix-connector.v2`): the server proves its identity
  before the token is sent (review item M5, see [security](docs/SECURITY.md)),
  key ids are derived from the keys, services are advertised with their
  type, models and limit, an emptied model list is sent explicitly, and every
  stream carries the organization, user key and request id.
- **Access log** (`access.log`, was `audit.jsonl`) records the organization,
  user key and request id Eidonix reported for every request.
- Storage: `encryption` defaults to `sse-c`; `none` is accepted without an
  extra flag. Credentials and key are inline (`access_key`, `secret_key`,
  `encryption_key`); internal defaults replace part size, operation rate,
  lifetimes and path-style settings.
- Commands: `check` (explains the file; `--redacted`), `init`,
  `generate-key`, `convert`. `check-config` and `print-systemd-network` are
  removed.
- The development gateway listens on `127.0.0.1:8443` and generates a YAML
  configuration.

## 0.1.0

First standalone release as cletus-connector: outbound WebSocket/yamux
session with Ed25519 server identity, TOML routes with profiles and flavors,
model aliases, pinned egress, connector-side object storage, deb/rpm/apk
packaging.
