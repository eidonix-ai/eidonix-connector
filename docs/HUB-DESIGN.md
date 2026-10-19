# Eidonix Connect: the hub (design, draft for review)

Status: **approved 2026-10-19; built in v0.4.0** (`cmd/eidonix-connect`,
`internal/hub`, `client`). The operator guide is [HUB.md](HUB.md). Where
building it settled a detail, this document now says what the hub does.

## 1. Goal

Connect the hardware at satellite sites to one cloud host, cleanly and
observably, so any application on that host can use it.

| Program | Runs at | Does |
|---|---|---|
| `eidonix-connector` | each site | Dials out to the hub; offers the services in its file. Unchanged (v0.3, protocol v3). |
| `eidonix-connect` | the cloud host | Accepts connector sessions; makes every connected service reachable on that host. |

```
application ──HTTP──▶ eidonix-connect ◀══ WSS, outbound from the site ══ eidonix-connector ──▶ service
(on the hub host)     local port          public session port               (site)             (site)
```

No database, no admin service, no per-application credentials. A site's file
decides what the site offers; once connected, the host's applications may use
it.

## 2. What each end does

**Site (unchanged).** The connector dials out, checks the hub's identity
(Ed25519) before sending its token, and serves only its listed services: kind
`http` (HTTP and WebSocket) or `s3` (storage), each with allowed endpoints,
optional models and aliases, health check, limits and idle timeout. Storage
credentials and SSE-C keys stay at the site. Several connector instances may
serve one site; the hub spreads requests across them.

**Hub (new).** Holds the sessions and serves one local HTTP port on which
every connected service is reachable by name (§4), plus a catalog of what is
connected and its health.

## 3. Configuration: one file

`/etc/eidonix-connect/connect.yaml`, owned by root, mode 0600 (or 0640 with
the hub's group, `root:65532` for the container) — it holds the secrets, so
everything is visible in one place.

```yaml
public: :8443                 # connectors dial this (TLS in front: wss://connect.example.com)
local: :8080                  # applications on this host use this (a private Docker network); never published
server-key: <base64 Ed25519 private key>   # its public half goes in each connector's file
access-log: /var/log/eidonix-connect/access.log   # optional; this is the default

sites:
  site-a:
    token: cxn_9f3a1c_<secret>
  site-b:
    token: cxn_77e0aa_<secret>
    disabled: true            # keeps the entry, refuses its sessions
    # optional: only these applications may use site-b
    # applications: [app-one]
    # optional, per service: replaces the site's list for that service
    # services:
    #   files:
    #     applications: [app-two]
```

That is the whole file. Rules (`eidonix-connect check` enforces them; a bad
file is refused with the line at fault, and a running hub keeps the last good
one):

- a site name is what that site's connector has as `site:` (lowercase letters,
  digits, dashes, at most 40; `v1` and `healthz` are reserved for the local
  port's own paths); a session naming another site is refused;
- each site has one `token` (a second, `next-token`, may be listed during a
  rotation); `eidonix-connect token` prints a new random one; every token in
  the file is unique; `eidonix-connect server-key` prints a new `server-key`;
- the hub checks the file every second and applies a valid change at once;
  removing or disabling a site, or rotating away the token a session used,
  ends those sessions at once, in-flight requests included. `public` and
  `local` take effect at the next start.

`applications` is the only access setting, and it is optional. Without it
every application on the host may use the site. With it, only applications
that name themselves (§4.4) in the list may. A service may have its own list
under `services.<name>.applications`, which replaces the site's for that
service (so two applications sharing a site need not share its storage). A
service named there need not be offered by the site yet; `check` warns when
the connected site does not offer it.

## 4. Using a connected service

### 4.1 HTTP

```
<METHOD> http://<local>/<site>/<service>/<path>[?query]
```

goes to that service as `<METHOD> /<path>`. The site's connector applies its
allowed endpoints, models and limits. Bodies stream both ways without
buffering, server-sent events are flushed as they arrive, and closing the
request cancels it at the site. So an application configures a connected
service as an ordinary base URL, e.g. `http://connect:8080/site-a/chat/v1`.
The path and query are passed as sent (escaped); the connector decides
whether they are allowed. The response's `X-Request-Id` is the hub's request
id (the caller's own, when it is 1-64 characters of `[A-Za-z0-9._:-]`).

### 4.2 WebSocket

An upgrade to `/<site>/<service>/<path>` reaches the service's
`WEBSOCKET /path` endpoint; messages (text, binary, close) pass through
unchanged, at most 4 MiB each. A refusal before the upgrade is an ordinary
error response (§4.6).

### 4.3 Storage (`kind: s3`)

The key is the rest of the path after `/<site>/<service>/`, percent-decoded
(the hub reads the escaped path, so `%2F` stays part of a segment name and
never splits it). Keys follow the storage rules in PROTOCOL.md.

| Request | Operation | Success |
|---|---|---|
| `PUT …/<key>` — body; `Content-Length` (or chunked = unknown size); `Content-Type` stored | put | `200` `{"size": n, "etag": "…"}`, `ETag` header |
| `GET …/<key>` — optional `Range: bytes=a-b` or `bytes=a-` (one range), `If-Match: <etag>` | get | `200` (or `206` + `Content-Range`) body with `Content-Length`, `Content-Type`, `ETag`, `Last-Modified` |
| `HEAD …/<key>` | head | `200`, same headers, no body |
| `DELETE …/<key>` | delete | `204` (also for a missing key) |
| `POST …/<dst-key>?copy-from=<percent-encoded src key>` | copy | `200` `{"size": n, "etag": "…"}` |
| `GET /<site>/<service>/?list&prefix=<p>&max=<1..1000>[&recursive][&token=<t>]` | list | `200` `{"objects": [{"key","size","etag","last_modified","content_type","dir"}], "truncated": bool, "next_token": "…"}` |

`Last-Modified` in the JSON is RFC 3339; in headers, HTTP date. The `ETag`
header is quoted; JSON etags are not, and `If-Match` takes either. `max`
defaults to 1000; `next_token` is present only when `truncated` and is passed
back as `token` unchanged; `recursive` (or `recursive=true`) lists every key
below the prefix, otherwise keys below the next `/` are folded into
`"dir": true` entries. A truncated or corrupt `get` body ends the response
abnormally, never as a clean end. Storage errors use the closed set of
PROTOCOL.md with these statuses: `not_found` 404, `precondition_failed` 412,
`too_large` 413, `read_only` 403 (a write to a `read_only` service; refused by
the hub before contacting the site), `invalid_key` / `key_too_long` /
`invalid_request` 400, `method_not_allowed` 405 (other methods), `busy` and
`rate_limited` 503 with `Retry-After: 1`, `timeout` 504, anything else 502.
Encryption and limits are in the catalog entry (`storage`, §4.5).

### 4.4 Naming the application

An application may send `X-Eidonix-Application: <name>` (and, for its own
records, `X-Eidonix-For: <text>` — e.g. an account or user it serves). Both
appear in the hub's log and, sent on to the site, in the connector's access
log, so a site sees which application used its hardware. They are labels, not
credentials (except for a site or service with an `applications` list). Each
is at most 64 printable ASCII characters; an invalid label is dropped (the
request goes ahead without it).

### 4.5 Catalog

`GET /v1/catalog` (and `/v1/catalog/<site>`):

```json
{"sites": [{
  "name": "site-a", "online": true,
  "sessions": [{"instance": "…", "version": "0.3.0", "connected_at": "…", "remote_ip": "…"}],
  "services": [{
    "name": "chat", "kind": "http", "healthy": true,
    "endpoints": ["POST /v1/*", "GET /v1/models"],
    "models": ["model-a"], "max_concurrent_requests": 32, "in_flight": 3
  }, {
    "name": "files", "kind": "s3", "healthy": true,
    "storage": {"encryption": "sse-c", "read_only": false, "max_object_bytes": 5368709120, "max_key_bytes": 1023}
  }]
}]}
```

A session that sent `draining` shows `"draining": true` and gets no new
requests; it stays listed until it closes. A service offered by several
sessions is merged: healthy if any non-draining session reports it healthy;
`max_concurrent_requests` the largest of the non-draining sessions' (each
session enforces its own, so during a connector reload capacity briefly
doubles); `in_flight` (the hub's own count of requests in progress) summed.
`online` means at least one non-draining session. Every site in the file is
listed; a disabled one adds `"disabled": true`. `endpoints` and `models` are
left out when empty. `/v1/catalog/<site>` answers that site's object (not
wrapped in `sites`), or `unknown_site`.

Built from the live sessions (each connector reports every 15 seconds).
`GET /healthz` answers `ok` (cheap, unauthenticated; for health checks).

### 4.6 Errors and limits

Errors made by the hub or a connector are
`{"error": {"code": "...", "message": "..."}}` with the code repeated in
`X-Eidonix-Error`; a reply from the service itself passes through unchanged.

| Code | Status | Meaning |
|---|---|---|
| `unknown_site` / `unknown_service` | 404 | not in the file / not offered by any session of the site that takes new requests |
| `site_offline` | 503 | no live, non-draining session for the site (also a disabled site) |
| `service_unhealthy` | 503 | the site reports the service unhealthy |
| `busy` | 503 | the service is at its concurrency limit (`Retry-After: 1`); a connector's own `busy` is reported the same way |
| `not_allowed` | 403 | the site (or service) has an `applications` list without this application |
| `read_only`, `method_not_allowed`, `invalid_request` | 403, 405, 400 | storage API (§4.3) and malformed requests |
| `not_found` | 404 | storage: no such object; also any other path under `/v1/` |
| `connector_failed` | 502 | the session failed before the reply began |
| `response_too_large` | 502 | a reply over the hub's 512 MiB backstop |
| others (`path_not_allowed`, `model_not_allowed`, `model_required`, `content_type_not_allowed`, `request_too_large`, `rate_limited`, `upstream_unavailable`, `upstream_error`, `upstream_timeout`, …) | as in PROTOCOL.md | refused or failed at the site, in the same JSON shape |

A connector's `busy` refusal (a draining connector refuses new streams with
it before contacting the service) is retried once on another session of the
site; request bodies up to 1 MiB are kept for that.

No overall time limit: a reply may go quiet for the service's `idle-timeout`
(the hub's own backstop closes a stream silent both ways for 15 minutes, 10
for storage). Size limits are the site's. Hop-by-hop headers, `X-Eidonix-*`
headers (the two labels travel in the stream header instead) and credentials
(`Authorization`, `Cookie`, `X-Api-Key`, which the connector would drop) are
not forwarded; the connector applies its own header allowlist.

### 4.7 Go client

Go applications may use the `client` package
(`github.com/eidonix-ai/eidonix-connector/client`, standard library only):
`client.New(baseURL, application)`, `ServiceURL(site, service)` for base URLs,
`Transport` to label an SDK's requests, `Catalog` / `Site`, and
`Storage(site, service)` with `Put`, `Get`, `GetRange`, `Head`, `Delete`,
`Copy` and `List`; errors are `*client.Error{Status, Code, Message}` and
`client.WithFor(ctx, label)` sets `X-Eidonix-For`.

## 5. Observability

At the site (where the hardware is):

- **Access log** (exists): one JSON line per request — service, method, path,
  model, status, bytes, duration, request id, upstream host, error — gaining
  `application` and `for` from §4.4.
- **`eidonix-connector status`** (new): connected or not, to which hub, since
  when, the last connection error; each service's health, in-flight requests
  and limits; requests and errors in the last hour. The running connector
  writes this to `/run/eidonix-connector/status.json` every 15 seconds; the
  command prints it.

At the hub:

- the catalog (§4.5) and `eidonix-connect status` (the same, as a table);
- an access log with the same fields plus site.

## 6. Protocol

Protocol v3, unchanged. The hub adds two optional fields to the stream
header, `application` and `for`; connectors before v0.4 ignore unknown fields
(plain JSON decoding) and simply don't log them. The existing caller fields
(`org_id`, `org_slug`, `user_key_id`) stay in the protocol as optional and are
no longer sent. So deployed connectors keep working; upgrading only adds the
new log fields and `status`.

Comments, templates and docs here that say "the name Cletus uses" become "the
name applications use".

### 6.1 Connector v0.4 (optional for the hub, worth doing)

v0.3 works with the hub unchanged. v0.4 fixes what it lacks at the site end:

- **Reload without dropping traffic.** Today SIGHUP ends the session and every
  active stream (`cmd/eidonix-connector/main.go`, `run`). v0.4 opens a new
  session with the new file, then drains the old one: the old session sends a
  `draining` control message (the hub stops giving it new requests) and closes
  once its streams finish or the longest `idle-timeout` passes.
- **Stop and upgrade without dropping traffic.** SIGTERM drains the same way
  (bounded, default 10 minutes; a second SIGTERM stops at once), so a package
  upgrade doesn't cut long generations.
- **Observability.** `application` and `for` in the access log; the
  `status` command (§5).
- **Neutral wording.** `auth_ok`'s `org` (display name) is logged as the hub's
  name.

The hub treats `draining` as "no new requests for this session"; connectors
before v0.4 never send it.

A hub restart still ends in-flight requests (one instance, §9); applications
retry, and connectors reconnect within about a second.

## 7. Security, briefly

What protects a site is its own file (unchanged): the connector reaches only
the addresses and requests listed there, whoever sends them. The hub's local
port is bound to a private address on the hub host and never published; only
`public` is reachable from outside, and it accepts only connector sessions
(identity proof, token, site check, and v3's session limits). The file is
0600 because it holds the server key and site tokens.

## 8. Code layout

```
cmd/eidonix-connect/     serve, check, status, token, server-key, version
internal/hub/            sessions, routing, local API, catalog, file reload
client/                  Go client of the local API (§4.7)
protocol/                shared by both programs
deploy/connect/          container image and compose file
```

Container image plus static binary. Tests: per package, and an end-to-end
suite running the real connector and hub against fake HTTP, SSE, WebSocket and
S3 services.

## 9. Not in v1

More than one hub instance; applications on other hosts; per-application
concurrency shares; an S3-compatible front for storage.
