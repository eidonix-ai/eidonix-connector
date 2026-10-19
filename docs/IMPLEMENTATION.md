# Implementation status

Module `github.com/eidonix-ai/eidonix-connector`, Apache-2.0. Version 0.4.0.

## Implemented

- Protocol v3 (`protocol/`): bounded control, stream and storage framing;
  server identity proven before the token is sent; the site named in hello;
  services advertised by kind, endpoints and models; golden vectors including
  a signature vector; fuzz targets for every parser.
- One YAML configuration file (site, upstream, services by name) with strict
  parsing, line-numbered errors naming the service and a permission gate;
  `check` (and `--redacted`), `init`, `generate-key`, `convert --site` from
  v0.2 and v0.1, `storage move-to-root`.
- Per-service `allowed-endpoints` (exact or a final `/*`), model lists and
  aliases (JSON and multipart), per-service concurrency and rate limits,
  health probing by path or reachability.
- Egress pinned in process to the configured hosts; no proxy for service
  calls; redirects refused.
- Connector-side storage: six operations confined to one bucket,
  SSE-C/SSE-S3/none, read-only services, verified commits, abandoned-upload
  cleanup, read-only checks; `move-to-root` for v0.2 folders.
- Access log with caller metadata and the `application`/`for` labels; SIGHUP reload that opens a new session and drains the old one; SIGINT/SIGTERM drain; SIGUSR1 log reopen; `status` file and command.
- Static Linux amd64/arm64 builds and `.deb`/`.rpm`/`.apk` packages, systemd
  and OpenRC services, distroless image.
- The hub, `eidonix-connect` (`cmd/eidonix-connect`, `internal/hub`): one
  file with reload, sessions with draining, the local HTTP / SSE / WebSocket
  / storage API and catalog, access log, container image; the `client`
  package. Its end-to-end suite runs the real connector against it.
- `cmd/test-gateway`, a loopback development peer speaking v3 (not a
  production gateway, tenant boundary, revocation service or deployment
  target).

## Verified

The automated suite runs a service shaped like each server Eidonix uses
through a real TLS, WebSocket and yamux session against fake servers,
including storage against a recording S3 double, model refusals, path
refusals, a site mismatch, per-service `busy`, SSE streaming and cancellation, WebSocket relay, idle limits,
header and redirect attacks, and a server signing with an unlisted key
(which never receives the token). `ci/storage-minio.sh` runs the storage
suite, including `move-to-root` with SSE-C, against a real TLS MinIO. Recorded runs are under
[ci/evidence](../ci/evidence); only recorded runs count as evidence.

## Not yet verified

Real model-server compatibility, real document conversions, a long voice
session, the 45-minute soak
(`EIDONIX_SOAK_DURATION=45m go test ./internal/testgateway -run TestLongSSESoak -count=1 -timeout=50m`),
the production gateway, RPM and amd64 package installs, and systemd/OpenRC
runtime on target hosts. No production server key, release signing key or
published release exists.
