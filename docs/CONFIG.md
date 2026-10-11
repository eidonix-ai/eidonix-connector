# Configuration

`run`, `check-config` and `print-systemd-network` use `/etc/cletus-connector/config.yaml` by default; override with `--config`. `init --profile vllm --upstream http://192.0.2.20:8000` prints starter YAML; `--upstream` may be repeated for load balancing. `--profile cletus-gpu-node` creates eight example service routes using ports 8001/8002/8004/8006–8010 on each supplied host; review those local ports and add live voice manually with its confirmed path. Replace the server key placeholder before using it. `check-config` prints effective methods/paths and destinations without upstream secret values.

Top-level fields:

| Field | Meaning |
|---|---|
| `server` | HTTPS gateway origin, port 443 |
| `token_file` | Local bearer token file |
| `server_keys` | One or two key-ID to base64 Ed25519 public-key mappings |
| `tls.ca`, `tls.ca_file` | `system` or `file`, with local CA bundle for `file` |
| `audit_log` | Local JSONL file; defaults to `/var/log/cletus-connector/audit.jsonl` |
| `health_listen` | Empty by default; optional localhost address |
| `routes` | Locally authorized inference routes |

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
| `methods_paths`, `extra_paths` | Explicit method/path pairs |
| `websocket_paths` | Exact locally authorized WebSocket paths |
| `allow_cidrs` | Restrict resolved upstream addresses |

Profiles cover `openai_chat`, `anthropic_messages`, `http` (explicit methods/paths required), `openai_embeddings`, `rerank`, `audio_transcription`, `audio_speech`, `image`, `document_ocr`, `document_conversion` and `realtime_ws`. Use `check-config` as the authoritative effective path listing for the selected flavor. vLLM, SGLang, Ollama, llama.cpp, go-llm-proxy, TEI, Infinity and LiteLLM use their own profile paths. Administration/model mutation endpoints remain excluded. Gotenberg URL-fetch conversion and Tika fileUrl headers must not be permitted. Live voice requires the operator's exact upstream path; none is guessed.

Upstream DNS is constrained by locally resolved addresses and optional CIDRs. The systemd drop-in must be regenerated when allowed addresses change. Cloud proxy environment variables apply only to the gateway connection; upstreams bypass proxy environment settings. HTTP redirects are returned rather than followed. Configured upstream credentials are injected locally; cloud-supplied authorization/cookies/forwarding headers are discarded.

For a fleet, list several upstream base URLs on one route or point the connector to a local load balancer. Example:

```yaml
- id: chat
  type: openai_chat
  flavor: vllm
  upstreams: [http://192.0.2.20:8000, http://192.0.2.21:8000]
  extra_paths: [[POST, /v1/responses]]
  max_concurrency: 64
  upstream_headers:
    Authorization: 'Bearer ${file:/etc/cletus-connector/secrets/inference-key}'
```

Do not add wildcard routes to solve compatibility failures. Add the required exact method/path after reviewing the upstream's behavior. Default limits and effective profile contents are displayed by `check-config` and defined in `internal/config` and `internal/profiles`.

The optional health endpoint exposes read-only `/health` status on the configured loopback listener; it does not accept administrative actions. Model discovery uses locally allowlisted `GET /v1/models` when available; advertised model IDs are suggestions, and production administrators must explicitly map them to service models.
