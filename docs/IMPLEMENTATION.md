# Implementation status

Module `github.com/eidonix-ai/eidonix-connector`, Apache-2.0. Version 0.2.0.

## Implemented

- Protocol v2 (`protocol/`): bounded control, stream and storage framing;
  server identity proven before the token is sent; golden vectors including a
  signature vector; fuzz targets for every parser.
- One YAML configuration file with strict parsing, line-numbered errors and a
  permission gate; `check` (and `--redacted`), `init`, `generate-key`,
  `convert` from v0.1.
- Fixed per-type request contracts, `api_path`, model allowlists (JSON and
  multipart), local model lists, per-service concurrency and rate limits,
  health probing and `models: any` discovery.
- Egress pinned in process to the configured hosts; no proxy for service
  calls; redirects refused.
- Connector-side storage: six operations confined to one bucket and prefix,
  SSE-C/SSE-S3/SSE-KMS/none, verified commits, read budgets, abandoned-upload
  cleanup, read-only checks.
- Access log with caller metadata; SIGHUP reload and SIGUSR1 log reopen.
- Static Linux amd64/arm64 builds and `.deb`/`.rpm`/`.apk` packages, systemd
  and OpenRC services, distroless image.
- `cmd/test-gateway`, a loopback development peer speaking v2 (not a
  production gateway, tenant boundary, revocation service or deployment
  target).

## Verified

The automated suite runs every service type through a real TLS, WebSocket
and yamux session against fake servers, including storage against a
recording S3 double, model refusals, path refusals, `api_path`, per-service
`busy`, SSE streaming and cancellation, WebSocket relay, idle limits,
header and redirect attacks, and a server signing with an unlisted key
(which never receives the token). `ci/storage-minio.sh` runs the storage
suite against a real TLS MinIO. Recorded runs are under
[ci/evidence](../ci/evidence); only recorded runs count as evidence.

## Not yet verified

Real model-server compatibility, real document conversions, a long voice
session, the 45-minute soak
(`EIDONIX_SOAK_DURATION=45m go test ./internal/testgateway -run TestLongSSESoak -count=1 -timeout=50m`),
the production gateway, RPM and amd64 package installs, and systemd/OpenRC
runtime on target hosts. No production server key, release signing key or
published release exists.
