# Eidonix Connect: the hub (design, draft for review)

Status: **draft, 2026-10-19 — not approved, nothing built.**

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

`/etc/eidonix-connect/connect.yaml`, root-owned, mode 0600 — it holds the
secrets, so everything is visible in one place.

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
```

That is the whole file. Rules (`eidonix-connect check` enforces them; a bad
file is refused with the line at fault, and a running hub keeps the last good
one):

- a site name is what that site's connector has as `site:` (lowercase letters,
  digits, dashes, at most 40); a session naming another site is refused;
- each site has one `token` (a second, `next-token`, may be listed during a
  rotation); `eidonix-connect token` prints a new random one;
- the hub reloads the file within 5 seconds of a change; removing or
  disabling a site ends its sessions at once.

`applications` is the only access setting, and it is optional. Without it
every application on the host may use the site. With it, only applications
that name themselves (§4.4) in the list may.

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

### 4.2 WebSocket

An upgrade to `/<site>/<service>/<path>` reaches the service's
`WEBSOCKET /path` endpoint; frames pass through unchanged.

### 4.3 Storage (`kind: s3`)

The key is the rest of the path after `/<site>/<service>/`, percent-decoded
(the hub reads the escaped path, so `%2F` stays part of a segment name and
never splits it). Keys follow the storage rules in PROTOCOL.md.

| Request | Operation | Success |
|---|---|---|
| `PUT …/<key>` — body; `Content-Length` (or chunked = unknown size); `Content-Type` stored | put | `200` `{"size": n, "etag": "…"}`, `ETag` header |
| `GET …/<key>` — optional `Range: bytes=a-b` (one range), `If-Match: <etag>` | get | `200` (or `206` + `Content-Range`) body with `Content-Length`, `Content-Type`, `ETag`, `Last-Modified` |
| `HEAD …/<key>` | head | `200`, same headers, no body |
| `DELETE …/<key>` | delete | `204` |
| `POST …/<dst-key>?copy-from=<percent-encoded src key>` | copy | `200` `{"etag": "…"}` |
| `GET /<site>/<service>/?list&prefix=<p>&max=<1..1000>[&recursive][&token=<t>]` | list | `200` `{"objects": [{"key","size","etag","last_modified","content_type","dir"}], "truncated": bool, "next_token": "…"}` |

`Last-Modified` in the JSON is RFC 3339; in headers, HTTP date. Storage
errors use the closed set of PROTOCOL.md with these statuses: `not_found` 404,
`precondition_failed` 412, `too_large` 413, `read_only` 403, `invalid_key` /
`key_too_long` / `invalid_request` 400, anything else 502. Encryption and
limits are in the catalog entry (`storage`, §4.5).

### 4.4 Naming the application

An application may send `X-Eidonix-Application: <name>` (and, for its own
records, `X-Eidonix-For: <text>` — e.g. an account or user it serves). Both
appear in the hub's log and, sent on to the site, in the connector's access
log, so a site sees which application used its hardware. They are labels, not
credentials (except for a site with an `applications` list).

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
requests. A service offered by several sessions is merged (healthy if any
non-draining session reports it healthy; `in_flight` summed).

Built from the live sessions (each connector reports every 15 seconds).

### 4.6 Errors and limits

Errors made by the hub or a connector are
`{"error": {"code": "...", "message": "..."}}` with the code repeated in
`X-Eidonix-Error`; a reply from the service itself passes through unchanged.

| Code | Status | Meaning |
|---|---|---|
| `unknown_site` / `unknown_service` | 404 | not in the file / not offered by the site |
| `site_offline` | 503 | no live session for the site |
| `service_unhealthy` | 503 | the site reports the service unhealthy |
| `busy` | 503 | the service is at its concurrency limit (`Retry-After: 1`) |
| `not_allowed` | 403 | the site has an `applications` list without this application |
| others (`endpoint_not_allowed`, `model_not_allowed`, `too_large`, `rate_limited`, `upstream_unreachable`, `timeout`) | as in PROTOCOL.md | refused or failed at the site |

No overall time limit: a reply may go quiet for the service's `idle-timeout`.
Size limits are the site's. Hop-by-hop and `X-Eidonix-*` headers (other than
the two labels) are not forwarded; the connector applies its own header
allowlist.

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
cmd/eidonix-connect/     serve, check, status, token
internal/hub/            sessions, routing, local API, catalog, file reload
protocol/                shared by both programs
```

Container image plus static binary. Tests: per package, and an end-to-end
suite running the real connector and hub against fake HTTP, SSE, WebSocket and
S3 services.

## 9. Not in v1

More than one hub instance; applications on other hosts; per-application
concurrency shares; an S3-compatible front for storage.
