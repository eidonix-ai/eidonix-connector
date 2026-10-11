# Connector protocol v1

The connector opens one outbound `wss://<server>:443/v1/connectors/session` connection with subprotocol `cletus-connector.v1`. Binary messages carry a byte stream multiplexed with yamux: 256 KiB stream windows, 15 second keepalive, and 10 second connection write timeout. Text transport messages are rejected. Only the cloud connection uses environment proxies.

The upgrade has `Authorization: Bearer cxn_<id>_<secret>`, where id is 16 base32 characters and secret is 32 random bytes encoded with unpadded base64url. The server stores SHA-256 of the decoded secret. Upgrade request headers also carry `X-Cletus-Connector-Nonce` (32 random bytes, standard base64), `X-Cletus-Connector-Version`, and `X-Cletus-Connector-Instance` (a process UUID).

Before starting yamux, the connector verifies `X-Cletus-Server-Key-Id`, `X-Cletus-Server-Time` (RFC3339, within five minutes), and `X-Cletus-Server-Signature` (standard base64 Ed25519 signature). The signed UTF-8 payload is:

```
cletus-connector.v1|<nonce>|<token id>|<instance UUID>|<server time>
```

Pins come from local configuration. TLS certificate verification and application identity verification are independent. A fresh nonce prevents replay across upgrades.

The connector opens the first yamux stream for control. Control messages are newline-delimited JSON, capped at 64 KiB including newline, with `v:1` and `type`. The connector sends `hello` (instance_id, version, os, arch, routes), periodic `status` (routes and uptime_s), and `pong`. Route advertisements include id, type, methods_paths, websocket_paths, max_concurrency, healthy, and optional models. Status routes include id, healthy, in_flight, models. The server sends `hello_ack` (session_id, org display name, accepted_routes, rejected_routes, limits.max_streams), `ping`, and `close`. Unknown fields and message types are ignored; unsupported versions end the session. No control message mutates local configuration.

Request streams are accepted only after hello_ack. Each begins with a JSON line of at most 4 KiB including newline:

```json
{"v":1,"route":"chat","kind":"http","request_id":"request-1"}
```

The header names no destination. Unknown fields are ignored. Kind is `http` or `websocket`. HTTP requests and responses use HTTP/1.1 stream encoding. Routes, methods, paths, headers, concurrency, sizes and idle timeouts are governed exclusively by local configuration. Connector errors carry `X-Cletus-Connector-Error`.

WebSocket streams begin with an HTTP GET upgrade and response, then relay message records: one type byte (1 text, 2 binary, 8 close), four bytes unsigned big-endian payload length, and payload. Each route caps message length before allocation. Ping and pong stay local to each WebSocket endpoint.

Closing a session cancels active request contexts. Closing a request stream cancels its upstream. Idle timeouts bound silence; there is no total generation duration limit. Disconnections reconnect with jittered exponential backoff from one to sixty seconds.

The `protocol` Go package is the shared source of truth. Golden serialization, bounds, replay/time checks and parser fuzz seeds are tested locally. Full gateway resource and tenancy enforcement belongs to the gateway implementation.
