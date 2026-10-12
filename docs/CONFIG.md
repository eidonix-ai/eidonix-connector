# Configuration

The connector is configured by one [TOML](https://toml.io) file. `run`, `check-config` and `print-systemd-network` use `/etc/cletus-connector/config.toml` by default; override with `--config`. `init --profile vllm --upstream http://192.0.2.20:8000` prints a starter TOML file; `--upstream` may be repeated for load balancing. `--profile generic` creates a generic chat route using only the upstream origins supplied by the operator. Add other services individually with their reviewed profile and port. Replace the server key placeholder before using it. `check-config` prints effective methods/paths, destinations and model aliases without upstream secret values.

Each connected service is one `[[routes]]` entry. Parsing is strict: unknown keys, values of the wrong type, redefined keys and malformed method/path pairs are configuration errors, so a mistyped option never silently falls back to a default. A complete file:

```toml
server = "https://gateway.example.com"
token_file = "/etc/cletus-connector/token"
audit_log = "/var/log/cletus-connector/audit.jsonl"

[server_keys]
"operator-key-id" = "BASE64_ED25519_PUBLIC_KEY"

[tls]
ca = "system"

[[routes]]
id = "rerank"
type = "rerank"
upstreams = ["http://192.0.2.20:8006"]
health = { path = "/health" }
extra_paths = [["GET", "/v1/models"]]

[routes.models]          # model aliases: the name the gateway sends = the name the upstream serves
"jina-reranker" = "jina-reranker-v3"
```

A `[routes.models]` (or `[routes.health]`, `[routes.upstream_headers]`, `[routes.encryption]`) table belongs to the `[[routes]]` entry above it, so keep each route's tables directly beneath its keys. The packaged example is installed as `/usr/share/doc/cletus-connector/config.example.toml`.

Top-level fields:

| Field | Meaning |
|---|---|
| `server` | HTTPS gateway origin, port 443 |
| `token_file` | Local bearer token file |
| `server_keys` | One or two key-ID to base64 Ed25519 public-key mappings |
| `tls.ca`, `tls.ca_file` | `system` or `file`, with local CA bundle for `file` |
| `audit_log` | Local JSONL file; defaults to `/var/log/cletus-connector/audit.jsonl` |
| `health_listen` | Empty by default; optional localhost address |
| `routes` | Locally authorized routes, one `[[routes]]` entry per service |

Route fields:

| Field | Meaning |
|---|---|
| `id`, `type`, `flavor` | Unique lowercase route ID, service profile and server flavor |
| `upstreams` | One or more HTTP/HTTPS base URLs with explicit port and no path |
| `balance` | `least_inflight` or `round_robin` |
| `health.path`, `health.interval` | Local health probe and interval |
| `max_concurrency` | Concurrent stream cap |
| `max_request_bytes`, `max_response_bytes` | Streaming byte caps, e.g. `16MiB`, `256MiB` |
| `idle_timeout` | No-progress timeout; no total generation duration limit |
| `rate_limit` | Per-route request rate |
| `max_websocket_message_bytes` | Per-message cap |
| `content_types` | Explicit accepted media types |
| `upstream_headers` | Local headers, including `${file:/absolute/secret}` substitutions |
| `models` | Model aliases: gateway model name → upstream model id (see below) |
| `methods_paths`, `extra_paths` | Explicit method/path pairs |
| `websocket_paths` | Exact locally authorized WebSocket paths |
| `allow_cidrs` | Restrict resolved upstream addresses |

Profiles cover `openai_chat`, `anthropic_messages`, `http` (explicit methods/paths required), `openai_embeddings`, `rerank`, `audio_transcription`, `audio_speech`, `image`, `document_ocr`, `document_conversion` and `realtime_ws`. Use `check-config` as the authoritative effective path listing for the selected flavor. vLLM, SGLang, Ollama, llama.cpp, go-llm-proxy, TEI, Infinity and LiteLLM use their own profile paths. Administration/model mutation endpoints remain excluded. Gotenberg URL-fetch conversion and Tika fileUrl headers must not be permitted. Live voice requires the operator's exact upstream path; none is guessed.

Upstream DNS is constrained by locally resolved addresses and optional CIDRs. The systemd drop-in must be regenerated when allowed addresses change. Cloud proxy environment variables apply only to the gateway connection; upstreams bypass proxy environment settings. HTTP redirects are returned rather than followed. Configured upstream credentials are injected locally; cloud-supplied authorization/cookies/forwarding headers are discarded.

For a fleet, list several upstream base URLs on one route or point the connector to a local load balancer. Example:

```toml
[[routes]]
id = "chat"
type = "openai_chat"
flavor = "vllm"
upstreams = ["http://192.0.2.20:8000", "http://192.0.2.21:8000"]
extra_paths = [["POST", "/v1/responses"]]
max_concurrency = 64

[routes.upstream_headers]
Authorization = "Bearer ${file:/etc/cletus-connector/secrets/inference-key}"
```

Do not add wildcard routes to solve compatibility failures. Add the required exact method/path after reviewing the upstream's behavior. Default limits and effective profile contents are displayed by `check-config` and defined in `internal/config` and `internal/profiles`.

The optional health endpoint exposes read-only `/health` status on the configured loopback listener; it does not accept administrative actions. Model discovery uses locally allowlisted `GET /v1/models` when available; advertised model IDs are suggestions, and production administrators must explicitly map them to service models.

## Model aliases

The gateway names models by its own catalogue (for example `jina-reranker`, `whisper`), while upstream servers serve their own ids (`jina-reranker-v3`, `/models/faster-whisper-large-v3-turbo`). A route's `[routes.models]` table translates between them; each key is the name the gateway sends and each value the id the upstream serves:

```toml
[[routes]]
id = "stt"
type = "audio_transcription"
upstreams = ["http://192.0.2.20:8007"]
health = { path = "/health" }

[routes.models]
"whisper" = "/models/faster-whisper-large-v3-turbo"
```

- Requests: in an `application/json` body, a top-level string `model` equal to an alias is replaced by its upstream id; every other byte of the body is kept. In a `multipart/form-data` body (audio transcription, image edits), a form field named `model` is replaced the same way; the form is re-encoded part by part as it streams, so file parts are not buffered, and the rewritten request is sent to the upstream with chunked transfer encoding. Nested `model` fields and other content are never rewritten.
- Names that are not aliases pass through unchanged and the upstream decides. Aliases are a naming convenience, not an access control: the gateway can still send an upstream id directly.
- On a route with aliases, a JSON body that is malformed, has trailing data or contains more than one top-level `model` key (compared after unescaping and case-insensitively), and a multipart body that cannot be parsed or has more than one `model` field, is rejected with 400 `invalid_request` instead of being forwarded. JSON bodies are buffered for this check, bounded by `max_request_bytes` and `max_concurrency`.
- `GET /v1/models` (where the route allows it): ids in an OpenAI-style `{"data":[{"id":...}]}` list that have aliases are listed under their aliases (one entry per alias when several aliases share an upstream id); other ids are unchanged. The models the connector advertises to the gateway use the same names. Before discovery succeeds, or when the route has no model list, the alias names are advertised.
- Response bodies other than the model list, including streamed (SSE) responses, are not rewritten and may carry the upstream id.
- Audit records of rewritten requests add `model` (the alias) and `upstream_model`. Bodies are still never logged.
- Rules: keys and values are 1–200 bytes of printable UTF-8 with no control characters, at most 64 aliases per route, and an alias may not map to itself. Aliases are accepted on `openai_chat`, `openai_embeddings`, `anthropic_messages`, `rerank`, `audio_transcription`, `audio_speech`, `image`, `realtime_ws` (its HTTP paths; WebSocket messages are not rewritten) and `http` routes, and rejected on `object_storage`, `document_ocr` and `document_conversion` routes.


## Object storage

An `object_storage` route exposes six operations: put, get (including ranges), head, list, delete and copy. It is not a raw S3 HTTP proxy. The endpoint, credentials, bucket, prefix and encryption policy come only from local configuration. Gateway requests supply relative object keys; they cannot change those local settings. Copy stays within the same bucket and prefix.

This example uses documentation hosts and a neutral bucket. Replace each angle-bracket path with an operator-selected local absolute path; no real credentials or encryption key are supplied.

```toml
[[routes]]
id = "documents"
type = "object_storage"
endpoint = "https://storage.example.com:9000"
region = "us-east-1"
bucket = "documentstore-example"
prefix = "documents/"
path_style = true
credentials_file = "<operator-selected-credentials-path>"
# tls_ca_file = "<operator-selected-ca-path>"
max_object_bytes = "5GiB"
part_size = "16MiB"
max_concurrency = 16
max_ops_per_second = 200
max_read_bytes_per_hour = 0
idle_timeout = "300s"
max_upload_lifetime = "6h"
abandoned_upload_age = "24h"
report_location = false

[routes.encryption]
mode = "sse-c"
key_file = "<operator-selected-key-path>"
```

| Field | Rule/default |
|---|---|
| `endpoint` | HTTP(S) origin without a path, user information, query or fragment; HTTPS recommended |
| `region` | Backend signing region; default `us-east-1` |
| `bucket` | Existing bucket; the connector never creates it |
| `prefix` | Required 1–256 UTF-8 bytes, relative, ending in `/` |
| `path_style` | Use path-style addressing when required by the backend; virtual-host addressing also requires bucket-host DNS/TLS |
| `credentials_file` | Root-owned 0600/0640 regular file: access key on line one, secret on line two |
| `encryption.mode` | `none`, `sse-s3`, `sse-kms` or `sse-c`; no encryption requires explicit `allow_unencrypted = true` |
| `encryption.key_file` | SSE-C only: root-owned 0600/0640 file containing base64 of exactly 32 bytes |
| `encryption.kms_key_id` | Required for SSE-KMS; separate backend/KMS permissions may be needed |
| `tls_ca_file` | Optional local CA bundle; TLS verification cannot be disabled |
| `allow_insecure_http` | Default false; opting in permits HTTP only for modes other than SSE-C and produces a warning |
| `allow_unencrypted` | Default false; required for `encryption.mode = "none"` |
| `allow_cidrs` | Optional restriction on resolved endpoint addresses |
| `max_object_bytes` | Default and hard maximum 5 GiB |
| `part_size` | Default 16 MiB; 5–64 MiB; one part buffered per active upload |
| `max_concurrency` | Default 16; 1–256 |
| `max_ops_per_second` | Default 200; positive rate |
| `max_read_bytes_per_hour` | Default 0 disables the budget; positive values cap bulk reads |
| `idle_timeout` | Default 300 seconds without progress |
| `max_upload_lifetime` | Default and hard maximum six hours; may be reduced; applies even while bytes arrive |
| `abandoned_upload_age` | Default and minimum 24 hours |
| `report_location` | Default false; true advertises bucket and prefix to the gateway |

Keep the SSE-C key backed up securely: losing it makes existing encrypted objects unreadable. SSE-C always requires HTTPS; `allow_insecure_http` cannot override that. Encryption mode and keys must match existing objects; a read-only check can detect a mismatch without changing them. Review backend-specific encryption defaults before use.

Keys must be valid UTF-8, relative and free of traversal components, repeated slashes, backslashes and control characters. A relative key is at most 1023 bytes; local prefix plus relative key must fit within 1024 bytes. The `.cletus-connector/` component is reserved for local probes and cannot be accessed through gateway operations. Lists return relative keys and omit reserved/out-of-prefix entries.

A positive hourly read budget reserves the announced read length before backend transfer. Failed or interrupted reads are conservatively charged; unused bytes are not refunded. Budgets and operation limits are local to each route runtime, reset with reload/restart, and are not shared across connector instances. Use aggregate monitoring for a fleet.

Uploads are verified before becoming visible: frame end marker, byte count, SHA-256 trailer and clean write-side EOF must agree. EOF must arrive within 30 seconds after the trailer, with no extra bytes. Failure or cancellation aborts multipart state and leaves any previous object unchanged. Buffer memory is approximately concurrency times part size, plus bounded protocol/client buffers. Local cleanup runs at startup and every six hours, skips this process's active uploads, and considers only uploads older than the configured age under the configured prefix. Keep clocks synchronized across instances and storage servers.

See [installation](INSTALL.md) for IAM permissions, lifecycle cleanup and the explicit write probe, and [security](SECURITY.md) for plaintext and encryption trust boundaries.

Configuration files must be regular files owned by root or the current user, with mode 0600 or 0640. Installations should use root ownership and the service group; current-user ownership supports local diagnostics. Validation leaves the input configuration unchanged; loading produces an independent normalized configuration and resolves local header secrets once.

Storage endpoints written as literal IP addresses require `path_style = true`; virtual-host addressing requires a DNS hostname.
