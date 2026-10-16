# Configuration

The connector reads one file, `/etc/eidonix-connector/connector.yaml` (or
`--config PATH`). It holds everything the connector needs: the upstream it
connects out to, the services it shares and their secrets. Nothing it needs
lives anywhere else, and Eidonix cannot change it.

`eidonix-connector init` writes the commented blank file
([examples/connector.yaml](../examples/connector.yaml)); a filled example is
[examples/gpu-server.yaml](../examples/gpu-server.yaml). After every edit run
`eidonix-connector check`, then `systemctl reload eidonix-connector` (or
`rc-service eidonix-connector reload`). A reload that fails validation keeps
the running configuration.

## Rules

- **Unknown or misspelt settings stop the connector**, naming the line, the
  service and the settings expected there, for example
  `line 9: unknown setting "max_concurrency" in chat service "cn3" (expected one of: name, url, urls, api_path, models, max_concurrent_requests, max_requests_per_minute, idle_timeout, api_key)`.
  Every other problem also names its line and service.
- **Services are grouped by type.** A type section may be absent, empty
  (`chat:`), `[]` or `null`; a file with no services at all is valid: the
  connector connects and shares nothing.
- **Names** are 1-40 lowercase letters, digits and `-`, starting with a letter
  or digit, and **unique across the whole file**: the name is what Eidonix
  addresses.
- **Durations** use Go's forms: `90s`, `15m`, `1h` (at most `24h`).
- **Sizes** are bytes, or `KB`/`MB`/`GB` (×1000) or `KiB`/`MiB`/`GiB` (×1024).
- **The file is a secret.** It must be a regular file owned by root (or, when
  the connector does not run as root, by the user running it), with no access
  for other users and no group write: `0640 root:eidonix-connector` (as
  packaged) or `0600`. Otherwise `run` and `check` refuse and print the exact
  fix, for example
  `Fix: chown root:eidonix-connector /etc/eidonix-connector/connector.yaml && chmod 0640 /etc/eidonix-connector/connector.yaml`.
- **Secrets** (`token`, `api_key`, `access_key`, `secret_key`,
  `encryption_key`) are never logged or printed. `check` lists which are set;
  `check --redacted` prints the whole file, comments kept, with each secret
  replaced by `•••• (set)`.

## Top level

| Setting | Meaning |
| --- | --- |
| `name` | Optional label for your own reference. Not used by the protocol; the connector's real name is set in Eidonix Setup. |
| `upstream.url` | The server to connect out to: `https://host` or `https://host:port`, no path. Only changes if Eidonix says so. |
| `upstream.token` | The connection token issued in Setup (`cxn_…`). Secret. It is sent only after the server has proved it holds one of `public_keys`. |
| `upstream.public_keys` | One or two base64 Ed25519 public keys (two only during a key rotation). The connector refuses to talk to a server that cannot sign with one of them. Public; safe to show. |
| `upstream.proxy_url` | Optional `http://host:port` (or `https://`) web proxy for the upstream connection only. Services are never reached through it. |
| `upstream.ca_certificate` | Optional PEM certificate added to the system's trusted roots for the upstream connection (for networks that inspect TLS with their own CA). |
| `chat`, `anthropic`, `embeddings`, `rerank`, `transcription`, `speech`, `image`, `document`, `conversion`, `realtime`, `storage`, `custom` | One section per type, each a list of services. |
| `access_log` | Absolute path of the request log. Default `/var/log/eidonix-connector/access.log`. |

## Services

| Setting | Meaning |
| --- | --- |
| `name` | Required. Unique in the file. |
| `url` | Required (or `urls`). Where the service runs: `http://host:port` or `https://host:port`, no path, query or credentials. Its host:port is the only machine this service may reach. |
| `urls` | A list instead of `url`: requests go to the address with the fewest requests in flight; an address that fails is skipped for 30 seconds. |
| `api_path` | Optional. Replaces the type's API prefix on the way to the server: `api_path: /` sends `POST /v1/embeddings` to `POST /embeddings` (Infinity-style servers); `api_path: /api/v1` sends it to `/api/v1/embeddings`. `/` or a path without a trailing `/`. Not for `custom` or `storage`. |
| `models` | Required on types with models (table below), not allowed on the others. A list (`[a, b]`) allows exactly those names. A map (`name: server-name`) allows the left names and rewrites them to the right ones on the way to the server. `any` allows whatever the server offers, written out on purpose. |
| `max_concurrent_requests` | Most requests running at once, enforced by the connector (`busy`) and announced to Eidonix. Default by type. |
| `max_requests_per_minute` | Optional rate limit (`rate_limited` beyond it). |
| `idle_timeout` | Stop a request that sends nothing for this long. A streaming response that keeps sending is never cut off. Default by type. |
| `api_key` | Optional key the connector sends to the service as `Authorization: Bearer …`. It is the only credential the connector sends to a service; a caller's own `Authorization` header is never forwarded. Secret. |
| `paths` | `custom` only, required: the exact requests allowed, e.g. `["POST /v1/run", "WEBSOCKET /v1/stream"]`. Methods: `GET`, `POST`, `PUT`, `WEBSOCKET`. |

### What each type accepts

Each type accepts exactly the requests below (as Eidonix sends them) and
nothing else; anything else gets `403 path_not_allowed` without reaching the
server. Queries are dropped from `GET` requests and refused on others.

| Type | Requests | API prefix | Models | Default max concurrent | Default idle | Request limit | Content types |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `chat` | `POST /v1/chat/completions`, `POST /v1/responses`, `GET /v1/models` | `/v1` | yes | 32 | 15m | 16 MiB | JSON |
| `anthropic` | `POST /v1/messages`, `POST /v1/messages/count_tokens`, `GET /v1/models` | `/v1` | yes | 32 | 15m | 16 MiB | JSON |
| `embeddings` | `POST /v1/embeddings`, `GET /v1/models` | `/v1` | yes | 32 | 2m | 16 MiB | JSON |
| `rerank` | `POST /v1/rerank`, `GET /v1/models` | `/v1` | yes | 32 | 2m | 16 MiB | JSON |
| `transcription` | `POST /v1/audio/transcriptions`, `GET /v1/models` | `/v1` | yes | 8 | 5m | 50 MiB | multipart |
| `speech` | `POST /v1/audio/speech`, `GET /v1/audio/voices` | `/v1` | yes | 8 | 2m | 16 MiB | JSON |
| `image` | `POST /v1/images/generations`, `POST /v1/images/edits` | `/v1` | yes | 2 | 10m | 16 MiB | JSON, multipart |
| `document` | `POST /layout-parsing` | none | no | 4 | 10m | 64 MiB | JSON |
| `conversion` | `POST /convert`, `POST /extract` | none | no | 4 | 10m | 100 MiB | multipart, octet-stream, PDF, text |
| `realtime` | `WEBSOCKET /v1/stream`, `GET /v1/config` | `/v1` | no | 4 | 5m (no traffic) | 16 MiB | JSON |
| `custom` | exactly `paths` | — | no | 32 | 2m | 16 MiB | JSON, multipart, octet-stream, text |
| `storage` | typed storage operations only (below) | — | no | 16 | 5m | — | — |

Responses are limited to 256 MiB and WebSocket messages to 4 MiB. Request
headers forwarded to a service are `Content-Type`, `Accept` and
`X-Request-Id` (plus `Anthropic-Version` and `Anthropic-Beta` for
`anthropic`); response headers returned are `Content-Type`, `Cache-Control`,
`Retry-After` and `X-Request-Id`. Redirects are never followed.

**Models.** With a list or map, a request naming any other model gets
`403 model_not_allowed` and is never forwarded (for uploads, nothing is sent
until the model field has been accepted). A request with no model is allowed
only when the list has exactly one entry, and is given that model; otherwise
`400 model_required`. Only the top-level `model` of a JSON body or the
`model` field of a multipart form is read or rewritten; a body with two
`model` fields, or one spelt in another case (`Model`), is refused
(`400 invalid_request`). `GET /v1/models` is answered by the connector from
the configured names. With `any`, requests pass unchanged and the advertised
list is what the server's own `GET /v1/models` reports.

**Health.** Every 15 seconds the connector requests `GET /health` at the
root of each address. Any response below 500 counts as healthy; no response,
a timeout or a 5xx marks the address unhealthy for 30 seconds.

## Storage

A `storage` service keeps an organization's files in its own S3-compatible
storage. Eidonix never receives the credentials or the encryption key: it can
only ask the connector to put, get, head, list, delete or copy a file inside
the one `bucket` and `prefix`, and the connector does that with its own
client. Bucket administration, policies, ACLs and presigned links cannot be
expressed.

| Setting | Meaning |
| --- | --- |
| `name` | Required, unique in the file. |
| `endpoint` | Required. `https://host[:port]`; `http://` only with `allow_http: true`, and never with `sse-c`. Its host is the only storage machine the connector may reach. Path-style addressing is used unless the host ends in `amazonaws.com`. |
| `bucket` | Required. Must already exist; the connector never creates or configures buckets. |
| `prefix` | Required folder every file lives under, ending in `/` (e.g. `cletus/`). |
| `region` | Default `us-east-1`. |
| `access_key`, `secret_key` | Required storage credentials. Secret. |
| `encryption` | The owner's choice. `sse-c` (default): files are encrypted with a key that never leaves this machine except to the storage service with each request. `sse-s3` / `sse-kms`: the storage encrypts. `none`: files are stored without encryption, for storage the organization already secures on its premises. `check` states the choice plainly. |
| `encryption_key` | `sse-c` only, required there: base64 of 32 random bytes (`eidonix-connector generate-key`). Secret. Keep a backup: losing it loses the files. |
| `kms_key_id` | `sse-kms` only, optional (the storage's default key otherwise). |
| `allow_http` | Default `false`. |
| `max_file_size` | Default and maximum 5 GiB. |
| `max_read_per_hour` | Optional cap on how much can be downloaded per hour (limits bulk export). |
| `max_concurrent_requests` | Default 16, at most 256. |
| `ca_certificate` | Optional PEM certificate added to the trusted roots for this endpoint. |

Internal defaults: 16 MiB upload parts, 200 operations per second, 5-minute
idle timeout, 6-hour upload lifetime, abandoned uploads cleaned after 24
hours. `check` also runs read-only checks against the bucket (`--write-probe`
adds a temporary write, read and delete under the reserved
`<prefix>.eidonix-connector/` folder).

## Where it may connect

The connector connects to exactly two kinds of place: the upstream (and
`proxy_url`, if set) and the addresses its services list. Hostnames are
resolved once at start and pinned: if one later resolves elsewhere, that
address is marked unhealthy until the connector restarts. Localhost is
reachable only if a service names it. There is no separate network policy
file; `check` lists every machine.

## Access log

One JSON line per request, never bodies. HTTP and WebSocket requests:

| Field | |
| --- | --- |
| `time`, `session` | When, and the upstream session id. |
| `service`, `type` | Which service. |
| `method`, `path`, `status` | The request as Eidonix sent it and the status returned. |
| `bytes_in`, `bytes_out`, `duration_ms` | Size and duration. |
| `model`, `upstream_model` | The model named, and the server's name when it was renamed. |
| `org_slug`, `org_id`, `user_key_id`, `request_id` | The organization, user key and request id Eidonix reported, matching its usage records. |
| `upstream_host` | Which address served it. |
| `error` | The error code, if the connector refused or ended the request. |

Storage lines carry `time`, `session`, `service`, `type`, `op`, `key_hash`
(a keyed hash, never the file name), `status` (`ok` or an error code),
`size`, `sha256_verified`, `duration_ms`, the same caller fields, and
`origin` (`upstream`, or `local` for the connector's own checks and
cleanup). SIGUSR1 reopens the file (logrotate does this).
