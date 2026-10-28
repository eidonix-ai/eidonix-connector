# Changelog

## 0.5.2 (2026-10-28)

**Built with Go 1.27.3.** Go 1.27.2's `crypto/x509` still trusted some
compromised intermediate certificates (GO-2026-6809), reachable from the
connector's TLS verification. `govulncheck` now reports nothing reachable.

**Signed releases.** Each release publishes `SHA256SUMS` signed with
minisign, `.apk` packages signed with an apk key (so Alpine installs no
longer need `--allow-untrusted`), and a CycloneDX SBOM per binary. The public
keys are in `keys/`; [INSTALL.md](docs/INSTALL.md#verify-the-release) shows
how to verify.

**Documentation.** IMPLEMENTATION.md and the README describe the production
hub, revocation and signed releases as they are, instead of the pre-hub state.

## 0.5.1 (2026-10-21)

### Connector

**`X-Affinity-Key` is forwarded.** Applications may send an opaque
conversation hash so a load balancer in front of several replicas keeps a
conversation on the replica that holds its prefix cache (haproxy:
`balance hdr(X-Affinity-Key)` with `hash-type consistent`). It carries no
credential; the connector passes it through unchanged.

## 0.5.0 (2026-10-21)

### Connector

**Machines.** One service name for a host that runs several servers, each on
its own port: `address` is the host and `ports` lists each server's settings
by port number ([docs/CONFIG.md](docs/CONFIG.md#machines)). Callers name the
machine; a request goes to the only port that allows it, or to the port that
lists the model it names. `GET /v1/models` shared by several ports is
answered with their models. A model may be on only one port, and ports that
allow a request in common must list their models; `check` refuses a file
that breaks either rule. Health and limits are per port: a port that is down
refuses its own requests while the others keep working. Files without
machines are unchanged.

**Health has three states.** A machine announces its ports as `parts` (what
each serves and whether it is healthy, no addresses); `status` and the hub
show a service **ok**, **degraded** (some ports down) or **down**. `status`'s
HEALTH column replaces HEALTHY.

### Protocol

Services and status services may carry `parts`; peers that predate it
ignore it.

### Hub

The catalog gives each service `health` (`ok`, `degraded`, `down`) and a
machine's `parts`, merged across the site's sessions.

## 0.4.0 (2026-10-19)

### Hub

**The hub, `eidonix-connect`.** A second program, for the cloud host:
it accepts connector sessions (protocol v3, unchanged) and makes every
connected service reachable on one local port, by site and service name
([docs/HUB.md](docs/HUB.md), [design](docs/HUB-DESIGN.md)).

- **One file**, `/etc/eidonix-connect/connect.yaml`: `public`, `local`,
  `server-key`, `access-log` and `sites` (each with `token`, optional
  `next-token`, `disabled`, `applications`, and per-service
  `services.<name>.applications`). Strict validation with the line at fault;
  a permission gate; checked every second and applied without a restart
  (removing or disabling a site, or rotating its token away, ends its
  sessions at once); a bad file keeps the last good one.
- **Local API**: `/<site>/<service>/<path>` for HTTP (streamed both ways,
  SSE flushed per write, cancelled at the site when the caller goes away)
  and WebSocket; a storage API for `kind: s3` services (put, get with one
  range and `If-Match`, head, delete, copy, list); `/v1/catalog` and
  `/v1/catalog/<site>`; `/healthz`. Errors are
  `{"error":{"code","message"}}` with `X-Eidonix-Error`.
- **Labels**: `X-Eidonix-Application` and `X-Eidonix-For` go to the
  connector in the stream header (`application`, `for`) and into both access
  logs; a site or service with an `applications` list refuses other
  applications (`not_allowed`).
- **Sessions**: the production gateway's session handling (identity proof
  before the token, site check, v3 limits, per-address and per-token rate
  limits), several sessions per site with load spread across them, each
  service's concurrency enforced at the hub (`busy`, 503, `Retry-After: 1`),
  `draining` honoured, and a draining connector's `busy` refusal retried once
  on another session.
- **Commands**: `serve`, `check`, `status` (a table from the catalog;
  `--healthz` for health checks), `token`, `server-key`, `version`.
- **Access log**: one JSON line per request (application, for, site,
  service, kind, method, path or storage op, status, bytes, duration,
  request id, error; never bodies, queries or keys); SIGUSR1 reopens it.
- **Container**: `deploy/connect/Dockerfile` (distroless, uid 65532) and
  `deploy/connect/compose.yaml` (service `connect` on the external
  `eidonix-connect`, `eidonix-edge` and `eidonix-connect-admin` networks, no
  published ports).
- **`client`**: a Go package for applications using the local API
  (service URLs, labels, catalog, storage, typed errors).
- **Protocol docs**: the `draining` control message and the `application`
  and `for` stream header fields (added in the protocol package earlier).

### Connector

Compatible with v0.3 files and gateways (protocol v3 is unchanged; a gateway
that does not know `draining` ignores it).

- **Reload without dropping traffic.** SIGHUP loads and validates the file (a
  rejected file keeps the running configuration), opens a new session with
  the new services and, once it is established, drains the old session: it
  sends a `draining` control message, answers any stream that still arrives
  with `busy` (never forwarded to a service), lets running streams finish and
  closes, after at most the longest `idle-timeout` of the old file (15
  minutes at most). If the new session is not up within a minute the old one
  keeps serving and the log says why. The last hour's counters carry over.
- **Graceful stop.** SIGINT/SIGTERM drain the same way, bounded by the new
  `run --drain-timeout` (default 10m); a second signal stops at once. The
  systemd unit now has `TimeoutStopSec=11min` and the OpenRC script waits 11
  minutes before KILL, so a restart after an upgrade no longer cuts long
  generations. Docker: use `--stop-timeout 660`.
- **`eidonix-connector status`** (and `--json`, `--file`): connected or not,
  the hub and since when, the last connection error, sessions draining, and
  each service's health, requests in flight, limit, and requests and errors in
  the last hour. The running connector writes
  `/run/eidonix-connector/status.json` (`status-file:` in the file; atomic;
  every 15 seconds and on every session change; no secrets).
- **Access log** gains `application` and `for`, the labels the calling
  application sets (storage lines too). The legacy `org_*` and `user_key_id`
  fields stay for older gateways. The `auth_ok` display name is logged as `hub`.
- Wording: examples, templates and docs say "the name applications use"
  instead of naming one application. The example storage bucket is
  `my-bucket`.

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
