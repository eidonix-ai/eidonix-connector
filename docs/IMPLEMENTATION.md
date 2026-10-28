# Implementation status

Module `github.com/eidonix-ai/eidonix-connector`, Apache-2.0. Version 0.5.2.

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
  health probing by path or reachability. Machines: one service for a host's
  servers, routed by port, with ok / degraded / down health.
- Egress pinned in process to the configured hosts; no proxy for service
  calls; redirects refused.
- Connector-side storage: six operations confined to one bucket,
  SSE-C/SSE-S3/none, read-only services, verified commits, abandoned-upload
  cleanup, read-only checks; `move-to-root` for v0.2 folders.
- Access log with caller metadata and the `application`/`for` labels; SIGHUP
  reload that opens a new session and drains the old one; SIGINT/SIGTERM
  drain; SIGUSR1 log reopen; `status` file and command.
- Static Linux amd64/arm64 builds and `.deb`/`.rpm`/`.apk` packages, systemd
  and OpenRC services, distroless image.
- The hub, `eidonix-connect` (`cmd/eidonix-connect`, `internal/hub`): the
  production peer. It holds the server key and one token per site in one
  file, with token rotation (`next-token`), revocation (remove the site or
  its token) and `disabled` sites, applied on reload and ending the affected
  live sessions; sessions with draining; the local HTTP / SSE / WebSocket /
  storage API and catalog; access log; container image; the `client`
  package. Its end-to-end suite runs the real connector against it.
- Signed releases (from 0.5.2): `SHA256SUMS` signed with minisign, `.apk`
  packages signed with an apk RSA key, a CycloneDX SBOM per binary; public
  keys in [`keys/`](../keys) ([verifying](INSTALL.md#verify-the-release)).
- `cmd/test-gateway`, a loopback development peer speaking v3 for trying a
  connector without a hub. It is not used in production.

Which organizations and users may use a service is decided by the
application behind the hub (for Cletus: Eidonix Setup), not by the
connector; see the [trust model](SECURITY.md#trust-model).

## Verified

The automated suite runs a service shaped like each server Eidonix uses
through a real TLS, WebSocket and yamux session against fake servers,
including storage against a recording S3 double, model refusals, path
refusals, a site mismatch, per-service `busy`, SSE streaming and
cancellation, WebSocket relay, idle limits, header and redirect attacks, and
a server signing with an unlisted key (which never receives the token).
`ci/storage-minio.sh` runs the storage suite, including `move-to-root` with
SSE-C, against a real TLS MinIO. CI runs gitleaks, govulncheck, the race
suite and the SBOM build on every push. Recorded runs are under
[ci/evidence](../ci/evidence).

In production since 2026-10-12 (hub since 2026-10-19): two sites on Alpine
(OpenRC, `.apk`) carrying chat and embedding models, rerank, OCR,
transcription, speech, image generation and S3 storage for Cletus and a
second application, upgraded together on each release.

## Not yet verified

Document-conversion servers through the connector, long voice sessions, the
45-minute soak
(`EIDONIX_SOAK_DURATION=45m go test ./internal/testgateway -run TestLongSSESoak -count=1 -timeout=50m`),
and `.deb`/`.rpm` installs with systemd on target hosts.
