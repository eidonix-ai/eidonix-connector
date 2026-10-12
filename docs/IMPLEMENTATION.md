# Implementation status

The agreed scope is the standalone connector and a local test gateway. Production Cletus integration is outside this iteration. Module path: `github.com/eidonix-ai/cletus-connector`, Apache-2.0.

## Implemented

- Shared v1 protocol, bounded control/stream/frame codecs, Ed25519 identity verification, golden fixtures and parser fuzz targets.
- HTTPS/WebSocket/yamux session, cloud-only proxy support, custom CA trust, nonce/time verification before streams, reconnect backoff, heartbeat and route status.
- Strict local TOML configuration (unknown keys rejected), all listed service profiles and flavors, exact path allowlists, administrative denylists, local secret injection, pinned IP:port dialing and DNS-change detection.
- Streaming HTTP and WebSocket relay, uploads and binary responses, byte/concurrency/rate/idle limits, cancellation, load balancing, health/model discovery, and body-free JSON audit records.
- Per-route model aliases: the top-level JSON `model` field or multipart `model` form field is translated from the gateway's name to the upstream id, `GET /v1/models` lists and advertised models show the aliases, and audit records note both names. Ambiguous or malformed bodies on alias routes are rejected; other response bodies are not rewritten.
- Connector-side object storage routes with six framed operations, prefix confinement, TLS and pinned endpoint dialing, SSE modes, bounded multipart uploads with explicit commit and EOF, local operation/read budgets, body-free key-hash audit records, read-only startup checks and abandoned-upload cleanup. Storage failures leave inference routes running.
- CLI commands, transactional local SIGHUP reload, optional loopback `/health`, systemd sandbox/drop-in generator, log rotation, Linux amd64/arm64 builds, deb/rpm packaging, distroless Dockerfile, CI and release tooling.
- Development-only TLS gateway and credential bootstrap. It requires the local token and listens on loopback. It is not a production registry, tenant boundary, revocation service or deployment target.

## Verified locally

The automated suite exercises all profiles through a real local TLS/WebSocket/yamux session with fake upstreams; multipart and binary forwarding; a 40 MiB OCR upload; SSE flush and cancellation; WebSocket text/binary relay; active streams longer than their idle limit; silence timeouts; request/response caps; concurrency rejection; path/header attacks; redirects; root refusal; model discovery; health listener handoff; DNS rebinding; and server identity through a TLS-inspecting CONNECT proxy.

The current storage implementation passes the full Go race suite and a real TLS MinIO integration run. That run exercises empty/small/multipart objects, ranged reads, SSE-C including rejected raw reads without its key, copy/list/delete, explicit check probes and cleanup, interrupted uploads without partial publication, preservation of existing objects, confinement, pagination and local budgets. The storage scope is connector-side protocol and S3-compatible backend support; production gateway dispatch, application clients and document workflows remain outside this implementation. Ceph and LocalStack compatibility have not been run.

Race, vet, fuzz, vulnerability, build and package results are recorded under [ci/evidence](../ci/evidence). Only the recorded runs constitute evidence. A short soak is not a 45-minute acceptance run.

## Deliberate restrictions and remaining release checks

- Inference prefix/wildcard routes are rejected; inference permits exact paths only. Storage routes use their separately enforced configured object prefix.
- Current storage image/package rebuilds and release verification are pending. Earlier image/package logs describe the source revision that produced them; they do not certify the storage changes.
- DNS changes never silently enlarge startup address pins. Review changed addresses, regenerate the systemd drop-in, and restart locally. The OS allowlist is IP-based; the application additionally checks ports.
- No production server public key, release signing key, published release or production deployment has been created. Operators must supply independently verified keys.
- Production gateway/setup UI, organization isolation, token revocation, service-client integration, metering and rollout are outside the agreed scope.
- Real model-server/container compatibility, real document conversions, a ten-minute voice session, the 45-minute soak, multi-instance failover against the production gateway, and the complete systemd/SELinux distro VM matrix still gate production acceptance.
- The test gateway accepts locally advertised routes for one configured token. It intentionally does not implement the production gateway's tenant authorization and service-specific policy.
- Upstream API behavior is a separate boundary: model/document servers may fetch URLs contained in request bodies. Isolate their egress; connector path restrictions do not sanitize arbitrary inference payloads.

Run the release soak explicitly:

```sh
CLETUS_SOAK_DURATION=45m go test ./internal/testgateway -run TestLongSSESoak -count=1 -timeout=50m
```
