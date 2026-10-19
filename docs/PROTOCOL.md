# Connector protocol, version 3

The Go `protocol` package defines `eidonix-connector.v3` and is the reference for every byte described here. It imports nothing from the connector, so the gateway vendors it unchanged. Capability strings version additional services independently: `inference/1` and `storage/1`. Capabilities describe the build, not which services happen to be configured. Additive JSON fields are ignored by older peers; breaking changes require a new protocol version. A v3 connector speaks only v3.

The gateway end is `eidonix-connect`, the hub in this repository ([HUB.md](HUB.md)); any other implementation follows the same rules.

v3 changes from v2: `hello` carries the connector's `site`; a service is advertised by `kind` (`http` or `s3`) with its allowed `endpoints` instead of one of twelve types, and `models_any` is gone; a storage service advertises `read_only` and has no folder prefix. Session mechanics, stream headers, storage operations and framing, and the closed error codes are unchanged.

## Connection and identity

The connector makes one outbound `wss://<host>[:port]/v1/connectors/session` connection (the `upstream.url` origin) with WebSocket subprotocol `eidonix-connector.v3`. Only this connection may use the configured `upstream.proxy`; upstream service calls never use a proxy. TLS uses system trust plus an optional configured CA certificate.

The upgrade request carries **no credential**:

- `X-Eidonix-Nonce`: 32 fresh random bytes, standard base64.
- `X-Eidonix-Instance`: a UUID generated at process start.

The gateway answers `101` with:

- `X-Eidonix-Server-Key-Id`: `"eidonix-" + hex(sha256(public key)[:4])`. The connector computes the id of each configured `public-keys` entry itself; owners never type ids.
- `X-Eidonix-Server-Time`: RFC3339.
- `X-Eidonix-Server-Signature`: standard base64 Ed25519 signature over these exact UTF-8 bytes, without a newline:

```
eidonix-connector.v3|<nonce>|<instance UUID>|<server time>
```

The connector checks the subprotocol, that the key id names one of its configured keys, that the server time is within five minutes of local time, and the signature. Any failure closes the WebSocket before multiplexing starts, so the token is never sent to a server that cannot prove it holds a configured key. `protocol/testdata/identity-vector.json` is a fixed signature vector.

Only then does the connector open the control stream and send the token:

```json
{"v":3,"type":"auth","token":"cxn_<id>_<secret>"}
```

The token is `cxn_` + a 16-character base32 id + `_` + 32 random secret bytes as unpadded base64url; verifiers store the SHA-256 of the decoded secret. The gateway answers `{"v":3,"type":"auth_ok","session_id":"…","org":"<display name>"}`, or `{"v":3,"type":"auth_failed","reason":"…"}` and closes. Then `hello` / `hello_ack` follow.

What the signature does and does not prove: it shows that a holder of a configured key answered this connection's fresh challenge, so a server without the key never sees the token. It is not bound to the TLS channel. A TLS-inspecting proxy that the host trusts (it has a CA in the host's trust store) can relay the challenge to the real gateway and then read or alter the session, including the token. See [security](SECURITY.md).

## Multiplexing and control

WebSocket binary messages carry a byte stream through yamux. Text transport messages are rejected. Binary messages are bounded to 256 KiB. Yamux uses 256 KiB stream windows, a 15-second keepalive and a 10-second connection write timeout. The connector opens stream 1 for control; the gateway opens request streams. Additional connector-initiated streams are protocol violations.

Control uses newline-delimited JSON, at most 64 KiB per line including the newline. Every message has `v:3` and `type`. Any other version terminates the session. Unknown message types are logged and ignored. Remote messages cannot alter local configuration.

| Direction | Type | Fields |
| --- | --- | --- |
| Connector → gateway | `auth` | `token` |
| Gateway → connector | `auth_ok` | `session_id`, `org` (display name) |
| Gateway → connector | `auth_failed` | `reason`; the gateway then closes |
| Connector → gateway | `hello` | `site`, `instance_id`, `version`, `os`, `arch`, `capabilities`, `services` |
| Gateway → connector | `hello_ack` | `accepted:[name]`, `rejected:[{name,reason}]`, `limits:{max_streams}` |
| Connector → gateway | `status` | `services`, `uptime_s`; every 15 seconds and whenever status changes |
| Connector → gateway | `draining` | No fields. The gateway opens no new streams on this session; the connector closes it once its streams finish. Gateways that predate it ignore it. |
| Gateway → connector | `ping` | Heartbeat; the gateway closes after three missed replies |
| Connector → gateway | `pong` | Reply to `ping` |
| Gateway → connector | `close` | `reason`; terminates the session |

`site` is the connector's name as configured on its machine. The gateway compares it with the connector's name in Setup; when they differ it sends `{"v":3,"type":"close","reason":"site_mismatch"}` instead of `hello_ack` and closes the session. The connector reports this as a configuration error and keeps retrying with backoff.

A `hello` service is:

```json
{"name":"gpu1","kind":"http","endpoints":["POST /v1/*","GET /v1/models"],"models":["model-a"],"max_concurrent_requests":32,"healthy":true}
```

`kind` is `http` (HTTP and WebSocket streams) or `s3` (storage streams); a gateway rejects any other kind. `endpoints` lists an `http` service's allowed requests exactly as configured: `METHOD /path`, where the method is `GET`, `POST`, `PUT`, `DELETE` or `WEBSOCKET` and a final `/*` matches one or more segments below the path. `models` is the list of names callers use, always present; empty means the connector does not check models for this service. `max_concurrent_requests` is the per-service limit the connector enforces; the gateway should enforce it per service as well. An `s3` service has no `endpoints` and adds:

```json
{"storage":{"max_key_bytes":1023,"max_object_bytes":5368709120,"encryption":"sse-c","read_only":false}}
```

Encryption is `none`, `sse-s3` or `sse-c`, the owner's choice. A `read_only` service refuses `put`, `delete` and `copy` with `forbidden`. A gateway rejects s3 services advertised without `storage/1` (`storage_capability_missing`) and never opens storage streams to such sessions.

A connector sends `draining` on its old session when it replaces it (reload, stop, upgrade): it first opens a new session, and once that has `auth_ok` and `hello_ack` the old one sends `draining`, keeps answering `ping` and sending `status`, finishes its streams and closes (a normal close, bounded by the connector). Both sessions carry the same `instance_id` (it is per process); gateways must not deduplicate or evict sessions by instance or site. A stream that still reaches a draining session is refused with `busy` (HTTP 429, or a storage `busy` result) without contacting the service, so the gateway may send it once more to another session. A gateway lists a draining session as such until it closes.

A `status` service is `{"name","healthy","in_flight","models"}`. `models` is **always** present: an empty array means the list is now empty, and the gateway must clear its copy. `protocol/testdata/control.jsonl` holds golden lines for every message type.

## Request headers and inference

Each request stream starts with one JSON line of at most 4 KiB including the newline:

```json
{"v":3,"service":"chat","kind":"http","request_id":"r1","application":"app-one","for":"account-7"}
```

`service` selects a locally configured service. It never selects a host, port, URL, bucket, filesystem path or command. `request_id` is the gateway's request id. `application` and `for` are optional labels the calling application set (on the hub, the `X-Eidonix-Application` and `X-Eidonix-For` headers): which application sent the request and whom it serves. The optional `org_id`, `org_slug` and `user_key_id` identify an organization and user key, for gateways that serve organizations; the hub does not send them. The connector does not authorise on any of these; it writes them to its access log so the owner's log matches the gateway's records line by line. Connectors that predate `application` and `for` ignore them. These fields are printable ASCII of at most 64 bytes. Unknown fields are ignored. Malformed or oversized headers close the stream without a response. A well-formed unknown `kind` receives this rejection line and then closes:

```json
{"v":3,"ok":false,"error":{"code":"unsupported_kind","message":"unsupported stream kind"}}
```

`kind:"http"` carries an HTTP/1.1 request and response. The connector canonicalises the origin-form path strictly (no `%`, `..`, `//`, backslash or control bytes; a query only on `GET`, where it is ignored and not forwarded), checks the method and path against the service's `endpoints`, checks the model when the service lists models, builds a new upstream request to the same path, forwards only `Content-Type`, `Accept`, `X-Request-Id`, `Anthropic-Version` and `Anthropic-Beta`, adds the configured `api-key` as `Authorization: Bearer` and streams the response as bytes arrive. It never follows upstream redirects or uses a proxy for upstream calls.

When a service lists models, a `POST` or `PUT` body must be `application/json` or `multipart/form-data` (otherwise `content_type_not_allowed`); its top-level `model` (JSON) or `model` form field (multipart) must be one of the names applications use and is rewritten to the server's name. A request without a model is refused with `model_required` unless exactly one model is listed, which is then filled in. `GET` and `DELETE` requests to such a service may not carry a body. Without models, bodies are not inspected. WebSocket messages are never inspected.

Connector-generated HTTP errors carry the code in `X-Eidonix-Error` and the body `<code>\n`:

| Status | Codes |
| --- | --- |
| 400 | `invalid_request`, `model_required` |
| 403 | `path_not_allowed`, `content_type_not_allowed`, `model_not_allowed` |
| 404 | `route_not_found` (no such service) |
| 413 | `request_too_large` |
| 429 | `busy` (the service's `max_concurrent_requests` is reached), `rate_limited` |
| 502 | `upstream_unavailable`, `upstream_error`, `response_too_large` |
| 504 | `upstream_timeout` |

`model_not_allowed` means the request named a model the service does not list; it is never forwarded. `model_required` means the request named no model and the service lists more than one. `upstream_unavailable` means the service's last health check failed. A gateway relays the status and code; `eidonix-connect` puts every connector refusal in its JSON error shape and reports `busy` as 503 ([HUB-DESIGN.md §4.6](HUB-DESIGN.md#46-errors-and-limits)).

`kind:"websocket"` starts with an HTTP GET upgrade request and response. Relay messages then consist of one type byte (1 text, 2 binary, 8 close), four unsigned big-endian length bytes and the payload, at most 4 MiB. Ping and pong are handled locally. HTTP or WebSocket streams naming an s3 service receive HTTP 403 `path_not_allowed`.

Inference streams use per-service idle timeouts with no total duration cap. Closing or resetting a stream cancels its upstream request. Session disconnection cancels active streams and reconnects with jittered exponential backoff from one to sixty seconds. Reconnections keep the destination addresses pinned at start.

## Storage requests

Storage streams use `kind:"storage"`. Their base header adds `op` and these operation-specific fields:

| Op | Fields | Request body |
| --- | --- | --- |
| `put` | `key`, required `size`, `content_type` | Framed object bytes, end marker, trailer, sender FIN |
| `get` | `key`, optional `range:[start,end]`, optional `if_match` | None |
| `head` | `key` | None |
| `list` | `prefix`, `recursive`, `max`, optional `token` | None |
| `delete` | `key` | None |
| `copy` | `src_key`, `dst_key` | None |

`service` matches `^[a-z0-9][a-z0-9-]{0,39}$`. For storage, `request_id` is 1–64 bytes from `[A-Za-z0-9._:-]`. Unknown operations or invalid field values receive `invalid_request`. A storage stream naming an http service receives `wrong_route_type`; an unknown service receives `route_not_found`. A read-only service answers `put`, `delete` and `copy` with `forbidden` before contacting storage.

All key-valued fields (`key`, `src_key`, `dst_key`, `prefix`, and list-result keys) are unpadded canonical base64url of the exact UTF-8 key bytes. Decoded relative keys are 1–1023 bytes; a list prefix may be empty. They cannot contain control characters, leading slash, double slashes, backslashes, dot segments or the reserved first segment `.eidonix-connector`. Keys name objects at the top of the service's bucket: the whole bucket belongs to Eidonix. Length failures return `key_too_long`; other key failures return `invalid_key`. The connector validates each key before contacting storage.

A list token is at most 1400 base64url characters and encodes the last returned relative key. The connector validates it and resumes after that key. Malformed or otherwise invalid tokens return `invalid_request`; decoded key-length violations return `key_too_long`. Tokens cannot widen the listed prefix.

Put `size` is −1 for unknown length, otherwise 0 through the service's object limit. `content_type` is at most 255 printable ASCII bytes and a valid media type; empty selects `application/octet-stream`. `if_match` is at most 256 printable ASCII bytes. Ranges are inclusive, with `0 ≤ start ≤ end`. List `max` is 1–1000; non-recursive lists include common prefixes as `dir:true,size:0` entries.

## Storage bodies and completion

Storage bodies use a framing format distinct from WebSocket relay messages:

```
[4-byte unsigned big-endian length N][N data bytes]
```

Data frames contain 1 through 1 MiB bytes. One zero-length frame marks the end. Immediately afterward comes one JSON trailer line of at most 256 bytes including newline:

```json
{"bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}
```

Both fields are mandatory. The checksum is 64 lowercase hexadecimal characters representing SHA-256 of exactly the framed bytes. Empty objects send only the marker and the empty SHA-256 trailer. Checksums detect corruption and truncation; they do not authenticate a compromised peer.

A receiver rejects oversized frames, missing markers, incomplete trailers, mismatched counts or checksums, and any byte after the trailer. It never reports clean EOF on a truncated body. For `get`, the expected body length is the full announced size, or `end−start+1` for a range. The result's `size` always describes the full object; ranged bounds must lie within it.

For `put`, verification also requires the sender's write-side EOF after the trailer. The sender calls `yamux.Stream.Close()` to send FIN and continues reading the result; yamux v0.1.2 permits reads after this half-close. The receiver allows at most 30 seconds for FIN. Any trailing byte is a protocol failure; an expired FIN wait maps to `timeout`. Neither case may commit an upload. Live callers of `NewStorageBodyReader` supply a stream read-deadline hook to enforce this bound.

For operations other than `put`, the sender keeps its write side open until it receives the result or finishes reading a get body. An early request-side FIN cancels those operations. A session failure also cancels all backend work. After a put FIN has been sent, a lost result has an uncertain outcome; the caller must check object state rather than assume rollback.

Only after marker, trailer, counts, checksum and FIN verification may the connector commit the object. A small object is committed from a local buffer; larger objects use explicitly managed multipart upload. A success result is sent only after storage confirms commit. A failure before commit aborts multipart state and preserves any existing object. After storage commits, a lost connection or acknowledgment may leave the object committed even though the sender saw no success; callers must account for that uncertain outcome. Put operations have a maximum six-hour lifetime, configurable lower, even if data continues arriving. Get streams have no total duration cap. Other silence is bounded by the storage idle timeout. The owning connector aborts an upload when its lifetime expires.

## Storage results and errors

Every operation returns one JSON result line with `v:3` and mandatory `ok`. Non-list lines are at most 4 KiB including newline; list lines are at most 1 MiB and 1000 objects.

| Op | Successful fields | Following data |
| --- | --- | --- |
| `put`, `copy` | `size`, `etag` | None |
| `get` | `size`, `content_type`, `etag`, `last_modified`, optional `range` | Framed body, end marker, trailer, EOF |
| `head` | `size`, `content_type`, `etag`, `last_modified` | None |
| `list` | `objects`, `truncated`, optional `next_token` | None |
| `delete` | None | None; idempotent |

List objects contain `key`, `size`, and `dir`, with optional bounded `etag`, `last_modified`, and `content_type` metadata. A truncated page requires a nonempty `next_token`; a complete page omits it. Empty pages still include `objects:[]` and `truncated:false`. Times use RFC3339 UTC. Result ETags and content types are bounded to 256 and 255 bytes. Object sizes never exceed 5 GiB or the local service limit.

Errors have the form `{"v":3,"ok":false,"error":{"code":"busy","message":"service is busy"}}`. The connector uses fixed per-code messages of at most 256 bytes, containing no backend text, keys, endpoints or credentials. Storage results use these codes from the closed set (the full set, including the HTTP codes above and `model_not_allowed` / `model_required`, is in `protocol/errors.go`):

`invalid_request`, `invalid_key`, `key_too_long`, `not_found`, `precondition_failed`, `too_large`, `size_mismatch`, `checksum_mismatch`, `forbidden`, `busy`, `rate_limited`, `backend_unavailable`, `backend_invalid`, `timeout`, `route_not_found`, `wrong_route_type`, `unsupported_kind`, `internal`.

Gateway HTTP mappings preserve these meanings:

| Codes | HTTP outcome |
| --- | --- |
| `invalid_key`, `key_too_long` | 400 |
| `not_found` | 404 |
| `precondition_failed` | 412 |
| `too_large` | 413 |
| `size_mismatch` | 400 for a short client upload; otherwise 502 |
| `checksum_mismatch`, `forbidden`, `backend_unavailable`, `backend_invalid`, `internal` | 502 |
| `busy` | Another eligible session, otherwise 503 |
| `rate_limited` | 503 with gateway-selected `Retry-After` |
| `timeout` | 504 |
| `invalid_request` | 500 and a gateway diagnostic |
| `route_not_found`, `wrong_route_type`, `unsupported_kind` | 502 and a gateway diagnostic |

A put error may arrive before the upload finishes; senders read results concurrently with writing and stop on errors. A get failure after its success line closes or resets the stream without an end marker. Receivers treat it as an incomplete download. An EOF or reset before a first result byte is a connector protocol failure and must not be retried on an incompatible session.

## Shared limits and implementation helpers

| Limit | Value |
| --- | --- |
| Header / non-list result line | 4 KiB including newline |
| List result line / entries | 1 MiB / 1000 |
| Storage data frame / trailer line | 1 MiB / 256 bytes |
| Object / key | 5 GiB / 1023 bytes |
| Multipart part / count | 5–64 MiB, default 16 MiB / at most 10,000 |
| Put lifetime / post-trailer FIN wait | At most 6 hours / 30 seconds |

`ReadStreamHeader` parses kind without dispatching it. `ValidateStorageHeader` applies field and key bounds. `ReadStorageResult` checks required fields, closed error codes and metadata bounds. `StorageBodyReader` verifies integrity and completion before returning EOF; `StorageBodyWriter.Finish` writes the marker and trailer but leaves FIN to the caller. The protocol package includes golden header fixtures, boundary and half-close tests, and fuzz targets for every storage parser. Application code remains responsible for service authorization, negotiated capabilities, S3 operations, resource accounting and cancellation.
