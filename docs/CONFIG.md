# Configuration

The connector reads one file, `/etc/eidonix-connector/connector.yaml` (or
`--config PATH`). It holds everything the connector needs: the site it
belongs to, the upstream it connects out to, the services it shares and their
secrets. Nothing it needs lives anywhere else, and Eidonix cannot change it.

The connector only exposes resources: an address, the requests allowed, and
optionally model names and limits. How Eidonix uses a service is decided in
Eidonix, not here.

`eidonix-connector init` writes the commented blank file
([examples/connector.yaml](../examples/connector.yaml)); filled examples are
[examples/gpu-server.yaml](../examples/gpu-server.yaml) and
[examples/plant-site.yaml](../examples/plant-site.yaml). After every edit run
`eidonix-connector check`, then `systemctl reload eidonix-connector` (or
`rc-service eidonix-connector reload`). A reload that fails validation keeps
the running configuration.

A v0.2 file (services grouped by type) is refused with the command that
converts it; see [installation](INSTALL.md#upgrading-from-v02).

## Rules

- **Unknown or misspelt settings stop the connector**, naming the line, the
  service and the settings expected there, for example
  `line 10: unknown setting "max_concurrency" in service "gpu1" (expected one of: kind, address, allowed-endpoints, models, health, api-key, max-concurrent-requests, max-requests-per-minute, idle-timeout)`.
  Every other problem also names its line and service.
- **Every key is lowercase with dashes.**
- **Services are listed by name** under `services:`. A file with no services
  is valid: the connector connects and shares nothing.
- **Names** are 1-40 lowercase letters, digits and `-`, starting with a letter
  or digit, and unique in the file: the name is what Eidonix addresses.
- **Durations** use Go's forms: `90s`, `15m`, `1h` (at most `24h`).
- **Sizes** are bytes, or `KB`/`MB`/`GB` (×1000) or `KiB`/`MiB`/`GiB` (×1024).
- **The file is a secret.** It must be a regular file owned by root (or, when
  the connector does not run as root, by the user running it), with no access
  for other users and no group write: `0640 root:eidonix-connector` (as
  packaged) or `0600`. Otherwise `run` and `check` refuse and print the exact
  fix, for example
  `Fix: chown root:eidonix-connector /etc/eidonix-connector/connector.yaml && chmod 0640 /etc/eidonix-connector/connector.yaml`.
- **Secrets** (`token`, `api-key`, `access-key`, `secret-key`,
  `encryption-key`) are never logged or printed. `check` lists which are set;
  `check --redacted` prints the whole file, comments kept, with each secret
  replaced by `•••• (set)`.

## Top level

| Setting | Meaning |
| --- | --- |
| `site` | Required. The connector's name in Eidonix Setup. Sent when connecting; Eidonix refuses the connection (`site_mismatch`) if it differs. |
| `upstream.url` | The server to connect out to: `https://host` or `https://host:port`, no path. Only changes if Eidonix says so. |
| `upstream.token` | The connection token issued in Setup (`cxn_…`). Secret. It is sent only after the server has proved it holds one of `public-keys`. |
| `upstream.public-keys` | One or two base64 Ed25519 public keys (two only during a key rotation). The connector refuses to talk to a server that cannot sign with one of them. Public; safe to show. |
| `upstream.proxy` | Optional `http://host:port` (or `https://`) web proxy for the upstream connection only. Services are never reached through it. |
| `upstream.ca-certificate` | Optional PEM certificate added to the system's trusted roots for the upstream connection (for networks that inspect TLS with their own CA). |
| `access-log` | Absolute path of the request log. Default `/var/log/eidonix-connector/access.log`. |
| `services` | The services, by name. |

## HTTP services

A service is `kind: http` unless it says `kind: s3`.

| Setting | Meaning |
| --- | --- |
| `address` | Required. `host:port` for plain HTTP, or `https://host:port` (port optional) for TLS. No path, query or credentials. It is the only machine this service may reach. |
| `allowed-endpoints` | Required. The requests allowed, as `METHOD /path`: `GET`, `POST`, `PUT`, `DELETE`, or `WEBSOCKET` for a WebSocket upgrade. A path is exact, or ends in `/*` to match every path one or more segments below it (`POST /v1/*` allows `POST /v1/chat/completions` but not `POST /v1`). `*` is allowed only as that whole last segment. At most 64 entries. |
| `models` | Optional. A list (`[a, b]`): only those names may be used. Or a map of `<name on the server>: {alias: <name Eidonix uses>}` (`alias` optional) to rename. Without `models` the connector does not read request bodies. |
| `health` | Optional path, e.g. `/health`. Every 15 seconds the connector sends `GET <health>`; any answer below 500 counts as healthy. Without it the connector checks only that the address accepts a connection. |
| `api-key` | Optional key the connector sends to the service as `Authorization: Bearer …`. It is the only credential the connector sends; a caller's own `Authorization` header is never forwarded. Secret. |
| `max-concurrent-requests` | Most requests running at once, enforced by the connector (`busy`) and announced to Eidonix. Default 16. |
| `max-requests-per-minute` | Optional rate limit (`rate_limited` beyond it). Default: none. |
| `idle-timeout` | How long a reply may go quiet before the request is stopped. A streaming response that keeps sending is never cut off. Default 5m. |

**Requests.** Before matching, request paths are checked strictly: no `%`,
`..`, `//`, backslash or control characters. A query is allowed on `GET` only;
it is not part of the match and is not forwarded. A request outside
`allowed-endpoints` gets `403 path_not_allowed` without reaching the server.
The request goes to the server at the same path; the connector never rewrites
paths. Requests are limited to 100 MiB, responses to 256 MiB and WebSocket
messages to 4 MiB. Request headers forwarded are `Content-Type`, `Accept`,
`X-Request-Id`, `Anthropic-Version` and `Anthropic-Beta` (the last two select
the Anthropic Messages API version and features; they carry no credentials);
response headers returned are `Content-Type`, `Cache-Control`, `Retry-After`
and `X-Request-Id`. Redirects are never followed.

**Models.** When `models` is set, a `POST` or `PUT` body must be JSON or a
multipart form (otherwise `403 content_type_not_allowed`), and its top-level
`model` (or the form's `model` field) must be one of the names Eidonix uses:
an alias, or the server name of an entry without one. It is rewritten to the
server's name; any other model gets `403 model_not_allowed` and is never
forwarded (for uploads, nothing is sent until the model field has been
accepted). A request with no model is allowed only when exactly one model is
listed, and is given that model; otherwise `400 model_required`. A body with
two `model` fields, or one spelt in another case (`Model`), is refused
(`400 invalid_request`). `GET` and `DELETE` requests to such a service may not
carry a body. `GET /v1/models`, if allowed, is answered by the server.

**Health.** A failed health check marks the service unhealthy for 30 seconds
(until the next check succeeds); meanwhile requests get
`502 upstream_unavailable`.

## Storage (s3 services)

A `kind: s3` service keeps an organization's files in its own S3-compatible
storage. Eidonix never receives the credentials or the encryption key: it can
only ask the connector to put, get, head, list, delete or copy a file inside
the one `bucket`, and the connector does that with its own client. The whole
bucket belongs to Eidonix. Bucket administration, policies, ACLs and
presigned links cannot be expressed.

| Setting | Meaning |
| --- | --- |
| `kind` | `s3`. |
| `address` | Required. `https://host[:port]`, or `host:port` for plain HTTP (never with `sse-c`). Its host is the only storage machine the connector may reach. Path-style addressing is used unless the host ends in `amazonaws.com`. |
| `bucket` | Required. Must already exist; the connector never creates or configures buckets. |
| `access-key`, `secret-key` | Required storage credentials. Secret. |
| `region` | Default `us-east-1`. |
| `encryption` | The owner's choice. `none` (default): files are stored without encryption, for storage the organization already secures. `sse-s3`: the storage encrypts. `sse-c`: files are encrypted with a key that never leaves this machine except to the storage service with each request. `check` states the choice plainly. |
| `encryption-key` | `sse-c` only, required there: base64 of 32 random bytes (`eidonix-connector generate-key`). Secret. Keep a backup: losing it loses the files. |
| `ca-certificate` | Optional PEM certificate added to the trusted roots for an `https://` address. |
| `read-only` | Default `false`. When `true`, put, delete and copy are refused (`forbidden`). |
| `max-file-size` | Default and maximum 5 GiB. |
| `max-concurrent-requests` | Default 16, at most 256. |

An s3 service may not set `allowed-endpoints`, `models`, `health`, `api-key`,
`max-requests-per-minute` or `idle-timeout`; an http service may not set the
s3 settings.

Internal defaults: 16 MiB upload parts, 200 operations per second, 5-minute
idle timeout, 6-hour upload lifetime, abandoned uploads cleaned after 24
hours. `check` also runs read-only checks against the bucket (`--write-probe`
adds a temporary write, read and delete under the reserved
`.eidonix-connector/` folder; skipped for a read-only service).

**Moving files out of a v0.2 folder.** v0.2 kept files under a folder (the
`prefix`); v0.3 uses the whole bucket. With the connector stopped,

```sh
eidonix-connector storage move-to-root <service> --from cletus/ [--dry-run]
```

copies every object under the folder to the same key at the top of the bucket
(a server-side copy, with the `encryption-key` for `sse-c`), checks that every
copy is there with the right size, then deletes the originals. It refuses,
changing nothing, if any destination key already exists, and removes its own
copies if a copy fails. `--dry-run` reports what it would move.

## Where it may connect

The connector connects to exactly two kinds of place: the upstream (and
`proxy`, if set) and the addresses its services list. Hostnames are resolved
once at start and pinned: if one later resolves elsewhere, the service is
marked unhealthy until the connector restarts. Localhost is reachable only if
a service names it. There is no separate network policy file; `check` lists
every machine.

## Access log

One JSON line per request, never bodies. HTTP and WebSocket requests:

| Field | |
| --- | --- |
| `time`, `session` | When, and the upstream session id. |
| `service`, `kind` | Which service. |
| `method`, `path`, `status` | The request as Eidonix sent it and the status returned. |
| `bytes_in`, `bytes_out`, `duration_ms` | Size and duration. |
| `model`, `upstream_model` | The model named, and the server's name when it was renamed. |
| `org_slug`, `org_id`, `user_key_id`, `request_id` | The organization, user key and request id Eidonix reported, matching its usage records. |
| `upstream_host` | Which address served it. |
| `error` | The error code, if the connector refused or ended the request. |

Storage lines carry `time`, `session`, `service`, `kind`, `op`, `key_hash`
(a keyed hash, never the file name), `status` (`ok` or an error code),
`size`, `sha256_verified`, `duration_ms`, the same caller fields, and
`origin` (`upstream`, or `local` for the connector's own checks and
cleanup). SIGUSR1 reopens the file (logrotate does this).
