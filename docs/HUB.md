# eidonix-connect: the hub

`eidonix-connect` runs on one cloud host. Connectors at the sites dial out to
it; it makes every connected service reachable on one local port of that
host, by site and service name. There is no database and no admin service:
one file says which sites may connect, and each site's own file says what it
offers. The design record is [HUB-DESIGN.md](HUB-DESIGN.md); the wire
protocol is [PROTOCOL.md](PROTOCOL.md).

```
application ──HTTP──▶ eidonix-connect ◀══ WSS, outbound from the site ══ eidonix-connector ──▶ service
(on the hub host)     local port          public session port               (site)             (site)
```

## Install

The hub ships as a container image (`deploy/connect/Dockerfile`, a static
binary on distroless, running as uid/gid 65532) and as static Linux binaries
(`dist/eidonix-connect-linux-{amd64,arm64}` from `make build`).

With Docker Compose (`deploy/connect/compose.yaml`):

```sh
docker network create eidonix-connect          # applications
docker network create eidonix-edge             # the TLS front proxy
docker network create eidonix-connect-admin    # an admin UI that reads the catalog
sudo install -d -m 0750 /etc/eidonix-connect
docker run --rm eidonix-connect:0.4.0 server-key        # prints server-key; its public key to stderr
docker run --rm eidonix-connect:0.4.0 token             # one per site
sudoedit /etc/eidonix-connect/connect.yaml              # see below
sudo chown root:65532 /etc/eidonix-connect/connect.yaml
sudo chmod 0640 /etc/eidonix-connect/connect.yaml
docker compose -f deploy/connect/compose.yaml up -d --build
docker compose -f deploy/connect/compose.yaml exec connect eidonix-connect status
```

The service publishes no ports. Point the TLS front (on `eidonix-edge`) at
`connect:<public port>` for `wss://connect.example.com`, with WebSocket
upgrades passed through and no read timeout shorter than a few minutes
(connectors ping every 15 seconds). Applications on `eidonix-connect` use
`http://connect:<local port>`.

Without containers, run `eidonix-connect serve --config /etc/eidonix-connect/connect.yaml`
as an unprivileged user that can read the file, with `local` bound to a
private address.

## The file

`/etc/eidonix-connect/connect.yaml` (or `--config PATH`):

```yaml
public: :8443                 # connectors' sessions (behind TLS: wss://connect.example.com)
local: :8080                  # applications on this host; never publish it
server-key: <base64>          # from eidonix-connect server-key
access-log: /var/log/eidonix-connect/access.log   # optional (default); - for standard output

sites:
  site-a:
    token: cxn_…              # from eidonix-connect token
  site-b:
    token: cxn_…
    next-token: cxn_…         # optional: a second token during a rotation
    disabled: true            # optional: keep the entry, refuse its sessions
    applications: [app-one]   # optional: only these applications may use site-b
    services:                 # optional: per-service settings
      files:
        applications: [app-two]   # replaces the site's list for this service
```

| Setting | Meaning |
| --- | --- |
| `public` | Required. `[host]:port` connectors' sessions arrive on (`/v1/connectors/session`). Put TLS in front of it. |
| `local` | Required. `[host]:port` of the local API. Bind it to a private address; it has no authentication. |
| `server-key` | Required. Standard base64 of a 32-byte Ed25519 seed (what `server-key` prints) or a 64-byte private key. Its public key (shown by `check`) goes in every connector's `public-keys`. |
| `access-log` | Absolute path, or `-` for standard output. Default `/var/log/eidonix-connect/access.log`. |
| `sites.<name>` | One entry per site. The name is the `site:` in that site's connector file: 1-40 lowercase letters, digits or `-`. `v1` and `healthz` are reserved. |
| `token` | Required. The site's connection token, `cxn_<16 characters>_<43 characters>`. Every token in the file must be unique. |
| `next-token` | Optional second token accepted during a rotation. |
| `disabled` | `true` refuses the site's sessions (and ends live ones) but keeps the entry. |
| `applications` | Optional list of application names (the `X-Eidonix-Application` label) that may use the site. Without it every application on the host may. |
| `services.<name>.applications` | Optional list for one service; it replaces the site's list for that service. A service named here need not be offered by the site (yet); `check` warns when the connected site does not offer it. |

Rules: unknown or misspelt settings are refused with the line at fault, like
the connector's file. The file must be owned by root (or by the user running
the hub) with no access for other users and no group write: `0600`, or
`0640` with the hub's group (`root:65532` for the container).

`eidonix-connect check` validates the file and explains it, prints the
server's public key and never prints a secret.

### Changes take effect by themselves

The hub reads the file every second. A valid change applies at once:

- a new site can connect;
- removing a site, disabling it, or changing its tokens so the token a
  session used is no longer listed ends that site's sessions immediately,
  including their in-flight requests;
- `applications` changes apply to the next request;
- a new `server-key` is proved to new sessions (live ones continue);
  connectors must list the new public key first;
- a new `access-log` path is opened.

`public` and `local` take effect at the next start. A file that fails
validation is reported in the operational log and the last good one stays in
force.

### Rotating a site's token

1. `eidonix-connect token`; add it as `next-token`.
2. Put the new token in the site's connector file and reload the connector.
3. Make the new token the `token` and remove `next-token`. A session still
   using the old token is closed.

### Rotating the server key

1. Generate a key with `server-key`; add its public key to every connector's
   `public-keys` (connectors accept two).
2. Replace `server-key` in the hub's file. New sessions prove the new key.
3. Remove the old public key from the connectors.

## Commands

| Command | |
| --- | --- |
| `eidonix-connect serve [--config PATH]` | Run the hub. SIGINT/SIGTERM stop it; SIGUSR1 reopens the access log. |
| `eidonix-connect check [--config PATH]` | Validate the file and explain it. If the hub is running, warn about per-service settings for services the connected site does not offer. |
| `eidonix-connect status [--config PATH \| --url URL] [--healthz]` | Print sites, sessions and services as a table, from the running hub's catalog (URL derived from the file's `local`). `--healthz` only checks that the hub answers (the container healthcheck). |
| `eidonix-connect token` | Print a new site token. |
| `eidonix-connect server-key` | Print a new `server-key`; its public key goes to standard error. |
| `eidonix-connect version` | |

## Using connected services

Every service of every connected site is at
`http://<local>/<site>/<service>/<path>`: HTTP (streamed both ways, SSE
flushed as it arrives, cancelled at the site when the request is closed),
WebSocket, and for `kind: s3` services a small storage API. The catalog is
`GET /v1/catalog` (and `/v1/catalog/<site>`); `GET /healthz` answers `ok`.
The full contract, with every status and error code, is
[HUB-DESIGN.md §4](HUB-DESIGN.md#4-using-a-connected-service).

Go applications may use the [`client`](../client) package
(`github.com/eidonix-ai/eidonix-connector/client`) instead of hand-rolling the
API:

```go
c := client.New("http://connect:8080", "app-one")
base := c.ServiceURL("site-a", "chat") + "/v1"            // an OpenAI-style base URL
httpClient := &http.Client{Transport: c.Transport(nil)}   // labels every request
files := c.Storage("site-a", "files")
etag, err := files.Put(client.WithFor(ctx, "account-7"), "reports/q3.pdf", f, size, "application/pdf")
```

## Logs

- **Access log**: one JSON line per request with `time`, `application`,
  `for`, `site`, `service`, `kind` (`http`, `websocket` or `storage`),
  `method`, `path` (below `/<site>/<service>`, without the query), `op` (for
  storage; keys are never logged), `status`, `bytes_in`, `bytes_out`,
  `duration_ms`, `request_id`, `session` and `error`. Rotate it with
  logrotate's `copytruncate`, or rename it and send SIGUSR1
  (`docker compose kill -s USR1 connect`).
- **Operational log**: structured text on standard error: sessions up and
  down (site, session, instance, remote address, version), refusals and
  their reasons, reloads.

`status` and the catalog show each session's connector version, address and
connection time; a session that sent `draining` is shown as draining and gets
no new requests until it closes.

## Security

- Only `public` faces the network, behind TLS. The hub proves its identity
  first (a connector never sends its token to a server that cannot sign with
  a pinned key); a session must then present a token from the file and name
  the site that token belongs to. Upgrade attempts are
  rate-limited per address (the last `X-Forwarded-For` hop when the peer is
  the private front proxy) and per token.
- `local` has no authentication by design: anything that can reach it can
  use every site without an `applications` list. Keep it on a private network
  that only trusted applications join. The labels are not credentials, except
  for sites and services with an `applications` list.
- What protects a site is its own connector file: the connector reaches only
  the addresses and requests listed there, whoever asks.
- The hub strips hop-by-hop headers, `X-Eidonix-*` headers and credentials
  (`Authorization`, `Cookie`, `X-Api-Key`) from requests; the connector
  applies its own header allowlist and its own service credentials.
- The file holds the server key and every token; protect it and its backups.
