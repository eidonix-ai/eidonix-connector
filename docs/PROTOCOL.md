# Connector protocol

The shared Go `protocol` package defines `cletus-connector.v1`. Capability strings version additional services independently: `inference/1` and `storage/1`. Capability advertisement describes the build, rather than which routes happen to be configured. Additive JSON fields are ignored by older peers; breaking storage changes require a new storage capability version.

## Connection and identity

The connector makes one outbound `wss://<server>:443/v1/connectors/session` connection with WebSocket subprotocol `cletus-connector.v1`. Only this cloud connection uses `HTTPS_PROXY` and `NO_PROXY`. TLS uses system trust or a locally configured CA bundle. Application identity verification remains mandatory through a TLS inspection proxy.

The upgrade request sends:

- `Authorization: Bearer cxn_<id>_<secret>`: a 16-character base32 ID and 32 random secret bytes encoded as unpadded base64url. Token verifiers hash the decoded secret with SHA-256.
- `X-Cletus-Connector-Nonce`: 32 fresh random bytes in standard base64.
- `X-Cletus-Connector-Version`: the connector build version.
- `X-Cletus-Connector-Instance`: a UUID generated at process startup.

The server returns `X-Cletus-Server-Time` (RFC3339), `X-Cletus-Server-Key-Id`, and `X-Cletus-Server-Signature` (standard base64 Ed25519). The signature covers these exact UTF-8 bytes, without a newline:

```
cletus-connector.v1|<nonce>|<token id>|<instance UUID>|<server time>
```

The connector verifies the signature against its local public-key pins and rejects time skew exceeding five minutes. It verifies both the subprotocol and identity before starting multiplexing. Fresh nonces bind signatures to individual upgrade attempts.

The signature proves that a pinned signing key answered the fresh challenge. It does not bind the signature to the TLS channel or authenticate subsequent frames. A malicious trusted TLS inspection proxy could relay the challenge to the real server and then alter session traffic. Operators therefore trust their TLS inspection infrastructure; the pin does not provide end-to-end channel protection through a hostile proxy.

## Multiplexing and control

WebSocket binary messages carry a byte stream through yamux. Text transport messages are rejected. Binary messages are bounded to 256 KiB. Yamux uses 256 KiB stream windows, a 15-second keepalive and a 10-second connection write timeout. The connector opens stream 1 for control; the gateway opens request streams. Additional connector-initiated streams are protocol violations.

Control uses newline-delimited JSON, with at most 64 KiB including newline. Every message includes `v:1` and `type`. Unsupported versions terminate the session. Unknown message types are logged and ignored. Remote messages cannot alter local configuration.

| Direction | Type | Fields |
| --- | --- | --- |
| Connector → gateway | `hello` | `instance_id`, `version`, `os`, `arch`, `capabilities`, `routes` |
| Connector → gateway | `status` | `routes`, `uptime_s`; every 15 seconds and when status changes |
| Connector → gateway | `pong` | Reply to `ping` |
| Gateway → connector | `hello_ack` | `session_id`, `org` display name, `accepted_routes`, `rejected_routes:[{id,reason}]`, `limits:{max_streams}` |
| Gateway → connector | `ping` | Heartbeat; the gateway closes after three missed replies |
| Gateway → connector | `close` | `reason`; terminates the session |

Inference route advertisements contain `id`, `type`, `methods_paths`, optional `websocket_paths`, `max_concurrency`, `healthy`, and optional discovered `models`. Status routes contain `id`, `healthy`, `in_flight`, and optional `models`.

An `object_storage` route has empty `methods_paths` and adds:

```json
{"storage":{"max_key_bytes":1017,"max_object_bytes":5368709120,"encryption":"sse-c","location":"bucket/prefix/"}}
```

Encryption is `none`, `sse-s3`, `sse-kms` or `sse-c`. `location` is optional display metadata, omitted when reporting is disabled. It cannot select a destination. A gateway rejects storage routes advertised without `storage/1` using `storage_capability_missing`, and never opens storage streams to such sessions. It opens storage streams only for accepted storage routes. Request acceptance starts after `hello_ack`.

## Request headers and inference

Each request stream starts with one JSON line of at most 4 KiB including newline:

```json
{"v":1,"route":"chat","kind":"http","request_id":"r-1"}
```

The header selects a locally configured route. It never selects a host, port, URL, bucket, filesystem path or command. Unknown fields are ignored. Malformed or oversized headers close the stream without a response. A well-formed unknown `kind` receives this generic rejection line and then closes:

```json
{"v":1,"ok":false,"error":{"code":"unsupported_kind","message":"unsupported stream kind"}}
```

`kind:"http"` carries an HTTP/1.1 request and response. The connector validates the method and origin-form path against the local route, builds a new upstream request, filters headers, injects local authentication and streams the response as bytes arrive. It never follows upstream redirects or uses environment proxies for upstream calls. Connector-generated HTTP errors use status 400 (a request body the connector must rewrite but can't parse, `invalid_request`), 403, 404, 413, 429, 502 or 504 and `X-Cletus-Connector-Error`. Status codes are additive: a gateway relays any connector error status as-is.

`kind:"websocket"` starts with an HTTP GET upgrade request and response. Relay messages then consist of one type byte (1 text, 2 binary, 8 close), four unsigned big-endian length bytes and payload. The route limits message size; the default is 4 MiB. Ping and pong are handled locally. HTTP or WebSocket streams naming a storage route receive HTTP 403 `path_not_allowed`.

Inference streams use route-specific idle timeouts with no total duration cap. Closing or resetting a stream cancels its upstream. Session disconnection cancels active streams and reconnects with jittered exponential backoff from one to sixty seconds. Reconnections retain locally pinned destination addresses.

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

`route` matches `[a-z0-9-]{1,40}`. `request_id` is 1–64 bytes from `[A-Za-z0-9._:-]`. Unknown operations or invalid field values receive `invalid_request`. A storage stream naming an inference route receives `wrong_route_type`; an unknown route receives `route_not_found`.

All key-valued fields (`key`, `src_key`, `dst_key`, `prefix`, and list-result keys) are unpadded canonical base64url of the exact UTF-8 key bytes. Decoded relative keys are 1–1023 bytes; a list prefix may be empty. They cannot contain control characters, leading slash, double slashes, backslashes, dot segments or the reserved first segment `.cletus-connector`. The configured prefix plus relative key must fit within 1024 bytes. Length failures return `key_too_long`; other key failures return `invalid_key`. The connector validates each key before contacting storage.

A list token is at most 1400 base64url characters and encodes the last returned relative key. The connector validates it and resumes after that key under the configured prefix. Malformed or otherwise invalid tokens return `invalid_request`; decoded key-length violations return `key_too_long`. Tokens cannot widen the prefix.

Put `size` is −1 for unknown length, otherwise 0 through the route's object limit. `content_type` is at most 255 printable ASCII bytes and a valid media type; empty selects `application/octet-stream`. `if_match` is at most 256 printable ASCII bytes. Ranges are inclusive, with `0 ≤ start ≤ end`. List `max` is 1–1000; non-recursive lists include common prefixes as `dir:true,size:0` entries.

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

Only after marker, trailer, counts, checksum and FIN verification may the connector commit the object. A small object is committed from a local buffer; larger objects use explicitly managed multipart upload. A success result is sent only after storage confirms commit. A failure before commit aborts multipart state and preserves any existing object. After storage commits, a lost connection or acknowledgment may leave the object committed even though the sender saw no success; callers must account for that uncertain outcome. Put operations have a maximum six-hour lifetime, configurable lower, even if data continues arriving. Get streams have no total duration cap. Other silence is bounded by the route idle timeout. The owning connector aborts an upload when its lifetime expires.

## Storage results and errors

Every operation returns one JSON result line with `v:1` and mandatory `ok`. Non-list lines are at most 4 KiB including newline; list lines are at most 1 MiB and 1000 objects.

| Op | Successful fields | Following data |
| --- | --- | --- |
| `put`, `copy` | `size`, `etag` | None |
| `get` | `size`, `content_type`, `etag`, `last_modified`, optional `range` | Framed body, end marker, trailer, EOF |
| `head` | `size`, `content_type`, `etag`, `last_modified` | None |
| `list` | `objects`, `truncated`, optional `next_token` | None |
| `delete` | None | None; idempotent |

List objects contain `key`, `size`, and `dir`, with optional bounded `etag`, `last_modified`, and `content_type` metadata. A truncated page requires a nonempty `next_token`; a complete page omits it. Empty pages still include `objects:[]` and `truncated:false`. Times use RFC3339 UTC. Result ETags and content types are bounded to 256 and 255 bytes. Object sizes never exceed 5 GiB or the local route limit.

Errors have the form `{"v":1,"ok":false,"error":{"code":"busy","message":"storage route is busy"}}`. The connector uses fixed per-code messages of at most 256 bytes, containing no backend text, keys, endpoints or credentials. The closed code set is:

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
| Object / relative key / full key | 5 GiB / 1023 bytes / 1024 bytes |
| Configured prefix | 1–256 bytes |
| Multipart part / count | 5–64 MiB, default 16 MiB / at most 10,000 |
| Put lifetime / post-trailer FIN wait | At most 6 hours / 30 seconds |

`ReadStreamHeader` parses kind without dispatching it. `ValidateStorageHeader` applies field and key bounds. `ReadStorageResult` checks required fields, closed error codes and metadata bounds. `StorageBodyReader` verifies integrity and completion before returning EOF; `StorageBodyWriter.Finish` writes the marker and trailer but leaves FIN to the caller. The protocol package includes golden header fixtures, boundary and half-close tests, and fuzz targets for every storage parser. Application code remains responsible for route authorization, negotiated capabilities, S3 operations, resource accounting and cancellation.
